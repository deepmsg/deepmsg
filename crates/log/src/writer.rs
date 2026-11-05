//! The writer thread: the only place that touches the file.
//!
//! One thread, one open file, one flush interval. Callers encode into a fixed
//! record and hand it over; this thread appends the newline, writes bytes, and
//! handles `flush`/`stop` control messages. It allocates its own write buffer
//! (that is allowed — ADR-0003 is about the calling thread), and it never
//! touches stdout or stderr.
//!
//! Writing is append-only from the process's point of view: the file is opened
//! with `append(true)`, so a restart continues the file instead of replacing
//! it. Rolling (the size limit and the old-file rotation) is the next slice;
//! this one writes the single `<component>-<node_id>.json` file.
//!
//! A write error is counted and the buffered bytes are dropped. A partial write
//! is possible on a full disk; the next successful line then joins the partial
//! one, which is the same degradation a line-oriented collector already has to
//! tolerate.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::config::Roll;
use crate::event::{Component, Event};
use crate::level::Level;
use crate::record::{Encoder, Record};

/// How long the writer waits before flushing an empty queue.
const FLUSH_INTERVAL: Duration = Duration::from_millis(200);

/// How often the writer reports the records the queue refused.
pub(crate) const REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// How many queued records one wake-up writes before it looks at control again.
const READ_BATCH: usize = 1024;

/// What the writer needs to encode its own `log.dropped` event.
pub(crate) struct ReportConfig {
    pub(crate) component: Component,
    pub(crate) node_id: String,
    pub(crate) clock: fn() -> i64,
}

/// What the handle can ask the writer to do besides write records.
pub(crate) enum Control {
    /// Write everything queued, flush, then acknowledge.
    Flush(Sender<()>),
    /// Drain, flush, and exit.
    Stop,
}

/// The writer thread and the channel that controls it.
pub(crate) struct Writer {
    pub(crate) control: Sender<Control>,
    pub(crate) join: JoinHandle<()>,
}

/// The current file and its rotation policy.
///
/// Rotation happens **before** a write that would take the file over the
/// limit, so the file never overshoots by a batch and the line that crosses
/// the limit is kept. A single write larger than the limit is written whole
/// and rotated before the next one.
///
/// The names are `base.json` (current), then `base.1.json`, `base.2.json`, …
/// up to `roll.files - 1`; every one still ends in `.json`, which is what the
/// collector's include glob matches. A crash between two renames leaves a gap;
/// that is accepted — the next start reads whatever is there.
pub(crate) struct RollingFile {
    base: PathBuf,
    file: File,
    bytes: u64,
    roll: Roll,
}

/// Open the current file for append, rotating first if it is already full.
pub(crate) fn open(path: &Path, roll: Roll) -> io::Result<RollingFile> {
    RollingFile::open(path.to_path_buf(), roll)
}

impl RollingFile {
    /// Open the current file for append, rotating first if it is already full.
    pub(crate) fn open(base: PathBuf, roll: Roll) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&base)?;
        let bytes = file.metadata()?.len();
        let mut rolling = Self {
            base,
            file,
            bytes,
            roll,
        };

        if rolling.bytes >= rolling.roll.bytes {
            rolling.rotate()?;
        }

        Ok(rolling)
    }

    /// Write bytes, rotating first if they would take the file over the limit.
    pub(crate) fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        let length = bytes.len() as u64;

        if self.bytes > 0 && self.bytes.saturating_add(length) > self.roll.bytes {
            self.rotate()?;
        }

        self.file.write_all(bytes)?;
        self.bytes += length;

        // A single write can be larger than the limit (the writer hands over a
        // batch). Rotate it out so the next write starts from an empty current
        // file; with one file there is nowhere to move it, and the next write
        // truncates instead.
        if self.bytes > self.roll.bytes && self.roll.files > 1 {
            self.rotate()?;
        }
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    fn rotate(&mut self) -> io::Result<()> {
        if self.roll.files <= 1 {
            // One file means "truncate in place": there is nowhere to move it.
            remove_if_exists(&self.base)?;
        } else {
            remove_if_exists(&numbered(&self.base, self.roll.files - 1))?;
            for index in (1..self.roll.files - 1).rev() {
                rename_if_exists(
                    &numbered(&self.base, index),
                    &numbered(&self.base, index + 1),
                )?;
            }
            rename_if_exists(&self.base, &numbered(&self.base, 1))?;
        }

        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.base)?;
        self.bytes = 0;
        Ok(())
    }
}

/// `base.3.json` for the third rotated file.
fn numbered(base: &Path, index: usize) -> PathBuf {
    let stem = base.file_stem().unwrap_or_default().to_string_lossy();
    let extension = base.extension().unwrap_or_default().to_string_lossy();
    base.with_file_name(format!("{stem}.{index}.{extension}"))
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn rename_if_exists(from: &Path, to: &Path) -> io::Result<()> {
    match fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Start the writer thread over an already-open file.
pub(crate) fn spawn(
    file: RollingFile,
    records: Receiver<Record>,
    io_errors: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    report: ReportConfig,
    report_interval: Duration,
) -> io::Result<Writer> {
    let (control, control_receiver) = mpsc::channel();
    let join = thread::Builder::new()
        .name("deepmsg-log-writer".to_owned())
        .spawn(move || {
            run(
                file,
                &records,
                &control_receiver,
                &io_errors,
                &dropped,
                &report,
                report_interval,
            );
        })?;

    Ok(Writer { control, join })
}

fn run(
    mut file: RollingFile,
    records: &Receiver<Record>,
    control: &Receiver<Control>,
    io_errors: &AtomicU64,
    dropped: &AtomicU64,
    report: &ReportConfig,
    report_interval: Duration,
) {
    let mut buffer = Vec::with_capacity(64 * 1024);
    // The queue is created empty and its counter starts at zero, so anything
    // already in it when this thread starts is a drop to report.
    let mut last_reported = 0;
    let mut last_report_at = Instant::now();
    // The writer has to wake often enough to notice a report is due, not only
    // often enough to flush.
    let wait = FLUSH_INTERVAL.min(report_interval);

    loop {
        let mut stop = false;
        while let Ok(message) = control.try_recv() {
            match message {
                Control::Flush(acknowledge) => {
                    drain(records, &mut buffer);
                    write(&mut file, &mut buffer, io_errors);
                    if file.flush().is_err() {
                        io_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    let _ = acknowledge.send(());
                }
                Control::Stop => stop = true,
            }
        }

        if stop {
            drain(records, &mut buffer);
            write(&mut file, &mut buffer, io_errors);
            report_dropped(&mut file, dropped, &mut last_reported, report, io_errors);
            if file.flush().is_err() {
                io_errors.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }

        match records.recv_timeout(wait) {
            Ok(record) => {
                append(&mut buffer, &record);
                for _ in 1..READ_BATCH {
                    match records.try_recv() {
                        Ok(record) => append(&mut buffer, &record),
                        Err(_) => break,
                    }
                }
                write(&mut file, &mut buffer, io_errors);
                report_if_due(
                    &mut file,
                    dropped,
                    &mut last_reported,
                    &mut last_report_at,
                    report,
                    io_errors,
                    report_interval,
                );
            }
            Err(RecvTimeoutError::Timeout) => {
                write(&mut file, &mut buffer, io_errors);
                if file.flush().is_err() {
                    io_errors.fetch_add(1, Ordering::Relaxed);
                }
                report_if_due(
                    &mut file,
                    dropped,
                    &mut last_reported,
                    &mut last_report_at,
                    report,
                    io_errors,
                    report_interval,
                );
            }
            Err(RecvTimeoutError::Disconnected) => {
                drain(records, &mut buffer);
                write(&mut file, &mut buffer, io_errors);
                report_dropped(&mut file, dropped, &mut last_reported, report, io_errors);
                if file.flush().is_err() {
                    io_errors.fetch_add(1, Ordering::Relaxed);
                }
                return;
            }
        }
    }
}

fn report_if_due(
    file: &mut RollingFile,
    dropped: &AtomicU64,
    last_reported: &mut u64,
    last_report_at: &mut Instant,
    report: &ReportConfig,
    io_errors: &AtomicU64,
    interval: Duration,
) {
    if last_report_at.elapsed() >= interval {
        report_dropped(file, dropped, last_reported, report, io_errors);
        *last_report_at = Instant::now();
    }
}

/// Write `log.dropped` for the records refused since the last report.
fn report_dropped(
    file: &mut RollingFile,
    dropped: &AtomicU64,
    last_reported: &mut u64,
    report: &ReportConfig,
    io_errors: &AtomicU64,
) {
    let current = dropped.load(Ordering::Relaxed);
    if current <= *last_reported {
        return;
    }

    let delta = current - *last_reported;
    let record = Encoder::begin(
        (report.clock)(),
        Level::Warn,
        report.component,
        &report.node_id,
        Event::LogDropped,
    )
    .field_u64("dropped", delta)
    .finish();

    if write_record(file, &record, io_errors) {
        *last_reported = current;
    }
}

fn write_record(file: &mut RollingFile, record: &Record, io_errors: &AtomicU64) -> bool {
    let mut bytes = Vec::with_capacity(record.as_bytes().len() + 1);
    bytes.extend_from_slice(record.as_bytes());
    bytes.push(b'\n');

    if file.write_all(&bytes).is_err() {
        io_errors.fetch_add(1, Ordering::Relaxed);
        return false;
    }
    true
}

fn append(buffer: &mut Vec<u8>, record: &Record) {
    buffer.extend_from_slice(record.as_bytes());
    buffer.push(b'\n');
}

/// Write what has accumulated; a failure is counted and the bytes are lost.
fn write(file: &mut RollingFile, buffer: &mut Vec<u8>, io_errors: &AtomicU64) {
    if buffer.is_empty() {
        return;
    }
    if file.write_all(buffer).is_err() {
        io_errors.fetch_add(1, Ordering::Relaxed);
    }
    buffer.clear();
}

fn drain(records: &Receiver<Record>, buffer: &mut Vec<u8>) {
    while let Ok(record) = records.try_recv() {
        append(buffer, &record);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use std::time::Duration;

    use super::{Control, ReportConfig, open, spawn};
    use crate::config::Roll;
    use crate::event::{Component, Event};
    use crate::level::Level;
    use crate::queue::Queue;
    use crate::record::Encoder;

    fn frozen() -> i64 {
        0
    }

    fn report() -> ReportConfig {
        ReportConfig {
            component: Component::Driver,
            node_id: "n".to_owned(),
            clock: frozen,
        }
    }

    fn wait_for(path: &Path, needle: &str) -> String {
        for _ in 0..400 {
            if let Ok(text) = fs::read_to_string(path) {
                if text.contains(needle) {
                    return text;
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("{needle} never appeared in {}", path.display());
    }

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            let suffix = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "deepmsg-log-writer-{}-{suffix}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("temp directory");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn record(message: &str) -> crate::record::Record {
        Encoder::begin(0, Level::Info, Component::Driver, "n", Event::RoleChange)
            .message(message)
            .finish()
    }

    #[test]
    fn records_are_appended_with_a_newline() {
        let directory = TempDir::new();
        let path = directory.0.join("driver-n.json");
        let file = open(&path, Roll::default()).unwrap();
        let (queue, receiver) = Queue::new(8);
        let io_errors = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let writer = spawn(
            file,
            receiver,
            Arc::clone(&io_errors),
            dropped,
            report(),
            std::time::Duration::from_secs(3600),
        )
        .unwrap();

        queue.push(record("one"));
        queue.push(record("two"));

        let (acknowledge, done) = std::sync::mpsc::channel();
        writer.control.send(Control::Flush(acknowledge)).unwrap();
        done.recv().unwrap();

        let text = fs::read_to_string(&path).unwrap();
        assert_eq!(2, text.lines().count());
        assert!(text.lines().next().unwrap().contains("\"msg\":\"one\""));
        assert!(text.lines().nth(1).unwrap().contains("\"msg\":\"two\""));
        assert_eq!(0, io_errors.load(std::sync::atomic::Ordering::Relaxed));

        writer.control.send(Control::Stop).unwrap();
        writer.join.join().unwrap();
    }

    #[test]
    fn a_second_open_appends_instead_of_replacing() {
        let directory = TempDir::new();
        let path = directory.0.join("driver-n.json");

        for message in ["first", "second"] {
            let file = open(&path, Roll::default()).unwrap();
            let (queue, receiver) = Queue::new(8);
            let io_errors = Arc::new(AtomicU64::new(0));
            let dropped = Arc::new(AtomicU64::new(0));
            let writer = spawn(
                file,
                receiver,
                io_errors,
                dropped,
                report(),
                std::time::Duration::from_secs(3600),
            )
            .unwrap();
            queue.push(record(message));
            writer.control.send(Control::Stop).unwrap();
            writer.join.join().unwrap();
        }

        let text = fs::read_to_string(&path).unwrap();
        assert_eq!(2, text.lines().count());
        assert!(text.lines().next().unwrap().contains("\"msg\":\"first\""));
        assert!(text.lines().nth(1).unwrap().contains("\"msg\":\"second\""));
    }

    fn index_of(line: &str) -> u32 {
        line.trim().parse().expect("a 100-byte test line")
    }

    fn line(index: u32) -> String {
        format!("{index:0>99}\n")
    }

    #[test]
    fn three_files_are_kept_and_the_oldest_lines_go() {
        let directory = TempDir::new();
        let base = directory.0.join("driver-n.json");
        let mut file = open(
            &base,
            Roll {
                bytes: 300,
                files: 3,
            },
        )
        .unwrap();

        for index in 0..20 {
            file.write_all(line(index).as_bytes()).unwrap();
        }
        file.flush().unwrap();
        drop(file);

        let mut names: Vec<String> = fs::read_dir(&directory.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(
            vec![
                "driver-n.1.json".to_owned(),
                "driver-n.2.json".to_owned(),
                "driver-n.json".to_owned()
            ],
            names
        );
        assert!(names.iter().all(|name| name.ends_with(".json")));

        let newest: Vec<u32> = fs::read_to_string(&base)
            .unwrap()
            .lines()
            .map(index_of)
            .collect();
        let middle: Vec<u32> = fs::read_to_string(directory.0.join("driver-n.1.json"))
            .unwrap()
            .lines()
            .map(index_of)
            .collect();
        let oldest: Vec<u32> = fs::read_to_string(directory.0.join("driver-n.2.json"))
            .unwrap()
            .lines()
            .map(index_of)
            .collect();

        assert_eq!(vec![18, 19], newest);
        assert_eq!(vec![15, 16, 17], middle);
        assert_eq!(vec![12, 13, 14], oldest);
        assert!(
            !oldest.contains(&0) && !oldest.contains(&11),
            "lines older than the retention window survived"
        );
    }

    #[test]
    fn a_file_already_at_the_limit_rotates_when_it_is_opened() {
        let directory = TempDir::new();
        let base = directory.0.join("driver-n.json");
        fs::write(&base, "x".repeat(300)).unwrap();

        let mut file = open(
            &base,
            Roll {
                bytes: 300,
                files: 3,
            },
        )
        .unwrap();
        file.write_all(b"new\n").unwrap();
        file.flush().unwrap();
        drop(file);

        assert_eq!(
            "x".repeat(300),
            fs::read_to_string(directory.0.join("driver-n.1.json")).unwrap()
        );
        assert_eq!("new\n", fs::read_to_string(&base).unwrap());
    }

    #[test]
    fn one_file_keeps_only_the_newest_line() {
        let directory = TempDir::new();
        let base = directory.0.join("driver-n.json");
        let mut file = open(
            &base,
            Roll {
                bytes: 300,
                files: 1,
            },
        )
        .unwrap();

        for index in 0..4 {
            file.write_all(line(index).as_bytes()).unwrap();
        }
        file.flush().unwrap();
        drop(file);

        let names: Vec<String> = fs::read_dir(&directory.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(vec!["driver-n.json".to_owned()], names);
        assert_eq!(
            vec![3],
            fs::read_to_string(&base)
                .unwrap()
                .lines()
                .map(index_of)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn dropped_records_are_reported_once() {
        let directory = TempDir::new();
        let path = directory.0.join("driver-n.json");
        let file = open(&path, Roll::default()).unwrap();
        let (_queue, receiver) = Queue::new(8);
        let io_errors = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let writer = spawn(
            file,
            receiver,
            Arc::clone(&io_errors),
            Arc::clone(&dropped),
            report(),
            Duration::from_millis(10),
        )
        .unwrap();

        dropped.fetch_add(5, Ordering::Relaxed);
        let text = wait_for(&path, "\"event\":\"log.dropped\"");
        assert_eq!(1, text.matches("\"event\":\"log.dropped\"").count());
        assert!(text.contains("\"dropped\":5"));
        assert!(text.contains("\"level\":\"warn\""));
        assert!(text.contains("\"component\":\"driver\""));
        assert!(text.contains("\"node_id\":\"n\""));

        // The next interval has nothing new to say.
        std::thread::sleep(Duration::from_millis(30));
        let text = fs::read_to_string(&path).unwrap();
        assert_eq!(1, text.matches("\"event\":\"log.dropped\"").count());

        writer.control.send(Control::Stop).unwrap();
        writer.join.join().unwrap();
    }

    #[test]
    fn stopping_reports_the_last_delta() {
        let directory = TempDir::new();
        let path = directory.0.join("driver-n.json");
        let file = open(&path, Roll::default()).unwrap();
        let (_queue, receiver) = Queue::new(8);
        let io_errors = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let writer = spawn(
            file,
            receiver,
            io_errors,
            Arc::clone(&dropped),
            report(),
            Duration::from_secs(3600),
        )
        .unwrap();

        dropped.fetch_add(3, Ordering::Relaxed);
        writer.control.send(Control::Stop).unwrap();
        writer.join.join().unwrap();

        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("\"event\":\"log.dropped\""));
        assert!(text.contains("\"dropped\":3"));
    }
}
