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

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::record::Record;

/// How long the writer waits before flushing an empty queue.
const FLUSH_INTERVAL: Duration = Duration::from_millis(200);

/// How many queued records one wake-up writes before it looks at control again.
const READ_BATCH: usize = 1024;

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

/// Open the log file for appending, creating it if it is not there.
pub(crate) fn open(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

/// Start the writer thread over an already-open file.
pub(crate) fn spawn(
    file: File,
    records: Receiver<Record>,
    io_errors: Arc<AtomicU64>,
) -> io::Result<Writer> {
    let (control, control_receiver) = mpsc::channel();
    let join = thread::Builder::new()
        .name("deepmsg-log-writer".to_owned())
        .spawn(move || run(file, &records, &control_receiver, &io_errors))?;

    Ok(Writer { control, join })
}

fn run(
    mut file: File,
    records: &Receiver<Record>,
    control: &Receiver<Control>,
    io_errors: &AtomicU64,
) {
    let mut buffer = Vec::with_capacity(64 * 1024);

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
            if file.flush().is_err() {
                io_errors.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }

        match records.recv_timeout(FLUSH_INTERVAL) {
            Ok(record) => {
                append(&mut buffer, &record);
                for _ in 1..READ_BATCH {
                    match records.try_recv() {
                        Ok(record) => append(&mut buffer, &record),
                        Err(_) => break,
                    }
                }
                write(&mut file, &mut buffer, io_errors);
            }
            Err(RecvTimeoutError::Timeout) => {
                write(&mut file, &mut buffer, io_errors);
                if file.flush().is_err() {
                    io_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                drain(records, &mut buffer);
                write(&mut file, &mut buffer, io_errors);
                if file.flush().is_err() {
                    io_errors.fetch_add(1, Ordering::Relaxed);
                }
                return;
            }
        }
    }
}

fn append(buffer: &mut Vec<u8>, record: &Record) {
    buffer.extend_from_slice(record.as_bytes());
    buffer.push(b'\n');
}

/// Write what has accumulated; a failure is counted and the bytes are lost.
fn write(file: &mut File, buffer: &mut Vec<u8>, io_errors: &AtomicU64) {
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
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, AtomicU64};

    use super::{Control, open, spawn};
    use crate::event::{Component, Event};
    use crate::level::Level;
    use crate::queue::Queue;
    use crate::record::Encoder;

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
        let file = open(&path).unwrap();
        let (queue, receiver) = Queue::new(8);
        let io_errors = Arc::new(AtomicU64::new(0));
        let writer = spawn(file, receiver, Arc::clone(&io_errors)).unwrap();

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
            let file = open(&path).unwrap();
            let (queue, receiver) = Queue::new(8);
            let io_errors = Arc::new(AtomicU64::new(0));
            let writer = spawn(file, receiver, io_errors).unwrap();
            queue.push(record(message));
            writer.control.send(Control::Stop).unwrap();
            writer.join.join().unwrap();
        }

        let text = fs::read_to_string(&path).unwrap();
        assert_eq!(2, text.lines().count());
        assert!(text.lines().next().unwrap().contains("\"msg\":\"first\""));
        assert!(text.lines().nth(1).unwrap().contains("\"msg\":\"second\""));
    }
}
