//! The facade: a configured logger, its writer handle, and the event builder.
//!
//! `open` builds an instance and does not touch process-global state;
//! `init_from_env` is the same thing plus installation as the process's global
//! logger, which is what the [`event`] function and the `event!` macro read.
//! Keeping the two apart is what makes tests parallel-safe: a test uses
//! `open`, so it never rewrites the environment or races another test for the
//! one global slot.
//!
//! A `Logger` is cheap to clone (two `Arc`s and a channel sender) and cloning
//! it does not allocate, so an [`EventBuilder`] can hold one and the caller
//! still pays nothing on the hot path.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::config::{Config, InitError, Pinner};
use crate::event::{Component, Event};
use crate::level::{COMPILED_MAX, Level};
use crate::queue::Queue;
use crate::record::Encoder;
use crate::writer::{self, Control};

static LOGGER: OnceLock<Logger> = OnceLock::new();

/// How long `flush` waits for the writer before giving up.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

/// The state shared by every clone of a logger.
struct Info {
    node_id: String,
    level: Level,
    component_levels: Vec<(Component, Level)>,
    clock: fn() -> i64,
}

impl Info {
    fn enabled(&self, level: Level, component: Component) -> bool {
        let ceiling = self
            .component_levels
            .iter()
            .find(|(candidate, _)| *candidate == component)
            .map(|(_, level)| *level)
            .unwrap_or(self.level);

        level.is_enabled(COMPILED_MAX) && level.is_enabled(ceiling)
    }
}

/// A cloneable handle to one writer and its queue.
#[derive(Clone)]
pub struct Logger {
    queue: Queue,
    io_errors: Arc<AtomicU64>,
    info: Arc<Info>,
}

impl Logger {
    /// Build an event for this logger, or a no-op builder if the level is off.
    pub fn event(&self, level: Level, component: Component, event: Event) -> EventBuilder {
        if !self.info.enabled(level, component) {
            return EventBuilder::disabled();
        }

        EventBuilder {
            logger: Some(self.clone()),
            encoder: Some(Encoder::begin(
                (self.info.clock)(),
                level,
                component,
                &self.info.node_id,
                event,
            )),
        }
    }

    /// Records the queue refused because it was full or the writer was gone.
    pub fn dropped_lines(&self) -> u64 {
        self.queue.dropped()
    }

    /// Writes or flushes that the platform refused.
    pub fn io_errors(&self) -> u64 {
        self.io_errors.load(Ordering::Relaxed)
    }
}

/// The process's logger and the thread writing its file.
pub struct Handle {
    logger: Logger,
    control: Sender<Control>,
    join: Mutex<Option<JoinHandle<()>>>,
    stopped: AtomicBool,
}

impl Handle {
    /// The logger, for a caller that wants to build events somewhere else.
    pub fn logger(&self) -> &Logger {
        &self.logger
    }

    /// Build an event for this instance.
    pub fn event(&self, level: Level, component: Component, event: Event) -> EventBuilder {
        self.logger.event(level, component, event)
    }

    /// Records the queue refused.
    pub fn dropped_lines(&self) -> u64 {
        self.logger.dropped_lines()
    }

    /// Writes or flushes that the platform refused.
    pub fn io_errors(&self) -> u64 {
        self.logger.io_errors()
    }

    /// Ask the writer to drain the queue, write, and flush.
    pub fn flush(&self) {
        let (acknowledge, done) = mpsc::channel();
        if self.control.send(Control::Flush(acknowledge)).is_ok() {
            let _ = done.recv_timeout(FLUSH_TIMEOUT);
        }
    }

    /// Drain, flush, and join the writer thread.
    pub fn shutdown(self) {
        self.stop();
        if let Some(join) = self
            .join
            .lock()
            .expect("the join mutex is never poisoned")
            .take()
        {
            let _ = join.join();
        }
    }

    fn stop(&self) {
        if !self.stopped.swap(true, Ordering::SeqCst) {
            let _ = self.control.send(Control::Stop);
        }
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Do not join here: a handle dropped during unwinding must not block.
        // `shutdown` is the explicit, blocking path.
        self.stop();
    }
}

/// Owns a [`Handle`] and shuts it down when dropped.
///
/// A binary's entry point has many `return` paths, and the last log line is
/// often the one written on the way out. `Handle`'s own `Drop` only signals the
/// writer; this guard joins it, so the queue is drained before the process
/// leaves. Put it in `main` where every path passes through it.
pub struct Guard(Option<Handle>);

impl Guard {
    /// Configure from the environment and install the process's logger.
    ///
    /// `pinner` is the binary's platform seam; it is set on the configuration
    /// before `open`, so a `DEEPMSG_LOG_CPU` without one is refused here.
    pub fn from_env(component: Component, pinner: Option<Pinner>) -> Result<Self, InitError> {
        let Some(mut config) = Config::from_env(component)? else {
            return Ok(Self(None));
        };
        config.pinner = pinner;
        Ok(Self(Some(open(config)?)))
    }

    /// Wrap an already-open handle.
    pub fn new(handle: Handle) -> Self {
        Self(Some(handle))
    }

    /// The handle, when logging is on.
    pub fn handle(&self) -> Option<&Handle> {
        self.0.as_ref()
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.shutdown();
        }
    }
}

/// Open a logger, creating its directory and file.
///
/// This does not install the logger globally; [`init_from_env`] does that.
pub fn open(config: Config) -> Result<Handle, InitError> {
    let pinning = match (config.cpu, config.pinner) {
        (Some(cpu), Some(pinner)) => Some((cpu, pinner)),
        (Some(_), None) => return Err(InitError::MissingPinner),
        (None, _) => None,
    };

    std::fs::create_dir_all(&config.dir).map_err(|error| InitError::Io {
        operation: "create the log directory",
        path: config.dir.clone(),
        message: error.to_string(),
    })?;

    let path = file_path(&config);
    let file = writer::open(&path, config.roll).map_err(|error| InitError::Io {
        operation: "open the log file",
        path: path.clone(),
        message: error.to_string(),
    })?;

    let (queue, receiver) = Queue::new(config.queue_capacity);
    let io_errors = Arc::new(AtomicU64::new(0));
    let writer = writer::spawn(
        file,
        receiver,
        Arc::clone(&io_errors),
        queue.dropped_handle(),
        writer::ReportConfig {
            component: config.component,
            node_id: config.node_id.clone(),
            clock: config.clock,
        },
        writer::REPORT_INTERVAL,
        pinning,
    )
    .map_err(|error| InitError::Io {
        operation: "start the log writer thread",
        path: path.clone(),
        message: error.to_string(),
    })?;

    let logger = Logger {
        queue,
        io_errors,
        info: Arc::new(Info {
            node_id: config.node_id,
            level: config.level,
            component_levels: config.component_levels,
            clock: config.clock,
        }),
    };

    Ok(Handle {
        logger,
        control: writer.control,
        join: Mutex::new(Some(writer.join)),
        stopped: AtomicBool::new(false),
    })
}

/// Configure from the environment and install as the process's logger.
///
/// `Ok(None)` means `DEEPMSG_LOG_DIR` was unset or empty: logging is off, and
/// nothing was created.
pub fn init_from_env(component: Component) -> Result<Option<Handle>, InitError> {
    let Some(config) = Config::from_env(component)? else {
        return Ok(None);
    };

    let handle = open(config)?;
    LOGGER
        .set(handle.logger.clone())
        .map_err(|_| InitError::AlreadyInstalled)?;
    Ok(Some(handle))
}

/// Build an event for the process's installed logger.
///
/// With no logger installed this is a no-op builder: tests, examples and
/// one-off runs pay a `OnceLock::get` and nothing else.
pub fn event(level: Level, component: Component, event: Event) -> EventBuilder {
    match LOGGER.get() {
        Some(logger) => logger.event(level, component, event),
        None => EventBuilder::disabled(),
    }
}

/// A partially built record.
///
/// The builder consumes itself so a whole line is one expression. When the
/// event is filtered out, both halves are `None` and every method is a no-op.
pub struct EventBuilder {
    logger: Option<Logger>,
    encoder: Option<Encoder>,
}

impl EventBuilder {
    fn disabled() -> Self {
        Self {
            logger: None,
            encoder: None,
        }
    }

    /// Set the human-readable message.
    pub fn message(mut self, text: &str) -> Self {
        self.encoder = self.encoder.map(|encoder| encoder.message(text));
        self
    }

    /// Set a string field.
    pub fn field_str(mut self, name: &'static str, value: &str) -> Self {
        self.encoder = self.encoder.map(|encoder| encoder.field_str(name, value));
        self
    }

    /// Set a signed integer field.
    pub fn field_i64(mut self, name: &'static str, value: i64) -> Self {
        self.encoder = self.encoder.map(|encoder| encoder.field_i64(name, value));
        self
    }

    /// Set an unsigned integer field.
    pub fn field_u64(mut self, name: &'static str, value: u64) -> Self {
        self.encoder = self.encoder.map(|encoder| encoder.field_u64(name, value));
        self
    }

    /// Set a boolean field.
    pub fn field_bool(mut self, name: &'static str, value: bool) -> Self {
        self.encoder = self.encoder.map(|encoder| encoder.field_bool(name, value));
        self
    }

    /// Finish the record and hand it to the queue.
    pub fn emit(self) {
        if let (Some(logger), Some(encoder)) = (self.logger, self.encoder) {
            logger.queue.push(encoder.finish());
        }
    }
}

fn file_path(config: &Config) -> PathBuf {
    config.dir.join(format!(
        "{}-{}.json",
        config.component.as_str(),
        safe_node_id(&config.node_id)
    ))
}

/// Make a node id safe as a filename component.
///
/// The JSON keeps the original string; only the filename is changed. This is
/// what stops a `node_id` containing `/` from becoming a directory.
fn safe_node_id(node_id: &str) -> String {
    node_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
    use std::sync::mpsc::Receiver;

    use super::{Guard, Handle, Logger, open};
    use crate::config::{Config, InitError, Roll};
    use crate::event::{Component, Event};
    use crate::level::Level;
    use crate::queue::Queue;
    use crate::record::Record;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            let suffix = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "deepmsg-log-facade-{}-{suffix}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("temp directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn frozen() -> i64 {
        0
    }

    fn config(directory: &Path, level: Level) -> Config {
        Config {
            dir: directory.to_path_buf(),
            node_id: "node-1".to_owned(),
            component: Component::Driver,
            level,
            component_levels: Vec::new(),
            roll: Roll::default(),
            queue_capacity: 64,
            cpu: None,
            pinner: None,
            clock: frozen,
        }
    }

    fn line_after_flush(handle: &Handle, directory: &Path) -> String {
        handle.flush();
        fs::read_to_string(directory.join("driver-node-1.json")).expect("log file")
    }

    #[test]
    fn an_event_reaches_the_file() {
        let directory = TempDir::new();
        let handle = open(config(directory.path(), Level::Info)).unwrap();

        handle
            .event(Level::Info, Component::Driver, Event::RoleChange)
            .message("hello")
            .field_u64("term", 7)
            .emit();

        let text = line_after_flush(&handle, directory.path());
        assert_eq!(1, text.lines().count());
        assert!(text.starts_with("{\"ts\":\"1970-01-01T00:00:00.000000000Z\""));
        assert!(text.contains("\"msg\":\"hello\""));
        assert!(text.contains("\"term\":7"));

        handle.shutdown();
    }

    #[test]
    fn a_level_below_the_ceiling_writes_nothing() {
        let directory = TempDir::new();
        let handle = open(config(directory.path(), Level::Off)).unwrap();

        handle
            .event(Level::Info, Component::Driver, Event::RoleChange)
            .message("not written")
            .emit();

        let text = line_after_flush(&handle, directory.path());
        assert!(text.is_empty());
        handle.shutdown();
    }

    #[test]
    fn a_second_open_appends() {
        let directory = TempDir::new();
        for message in ["first", "second"] {
            let handle = open(config(directory.path(), Level::Info)).unwrap();
            handle
                .event(Level::Info, Component::Driver, Event::RoleChange)
                .message(message)
                .emit();
            handle.shutdown();
        }

        let text = fs::read_to_string(directory.path().join("driver-node-1.json")).unwrap();
        assert_eq!(2, text.lines().count());
        assert!(text.lines().next().unwrap().contains("\"msg\":\"first\""));
        assert!(text.lines().nth(1).unwrap().contains("\"msg\":\"second\""));
    }

    #[test]
    fn a_node_id_with_a_slash_stays_in_the_directory() {
        let directory = TempDir::new();
        let mut config = config(directory.path(), Level::Info);
        config.node_id = "a/b".to_owned();
        let handle = open(config).unwrap();

        assert!(directory.path().join("driver-a_b.json").is_file());
        assert!(!directory.path().join("a").exists());
        handle.shutdown();
    }

    fn logger_without_writer(level: Level) -> (Logger, Receiver<Record>) {
        let (queue, receiver) = Queue::new(1);
        let logger = Logger {
            queue,
            io_errors: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            info: std::sync::Arc::new(super::Info {
                node_id: "n".to_owned(),
                level,
                component_levels: Vec::new(),
                clock: frozen,
            }),
        };
        (logger, receiver)
    }

    #[test]
    fn a_full_queue_counts_instead_of_blocking() {
        let (logger, receiver) = logger_without_writer(Level::Info);
        logger
            .event(Level::Info, Component::Driver, Event::RoleChange)
            .message("one")
            .emit();
        logger
            .event(Level::Info, Component::Driver, Event::RoleChange)
            .message("two")
            .emit();

        assert_eq!(1, logger.dropped_lines());
        assert!(receiver.try_recv().is_ok());
    }

    #[test]
    fn without_an_installed_logger_the_facade_is_a_no_op() {
        super::event(Level::Info, Component::Driver, Event::RoleChange)
            .message("nowhere")
            .emit();
    }

    #[test]
    fn the_configured_roll_is_the_one_the_writer_uses() {
        let directory = TempDir::new();
        let mut config = config(directory.path(), Level::Info);
        config.roll = Roll {
            bytes: 300,
            files: 2,
        };
        let handle = open(config).unwrap();

        for index in 0..20 {
            handle
                .event(Level::Info, Component::Driver, Event::RoleChange)
                .message("a message long enough to matter")
                .field_u64("index", index)
                .emit();
        }
        handle.shutdown();

        let mut names: Vec<String> = fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(
            vec![
                "driver-node-1.1.json".to_owned(),
                "driver-node-1.json".to_owned()
            ],
            names
        );
    }

    static PINNED_CPU: AtomicI32 = AtomicI32::new(-1);

    fn pin_to(cpu: i32) -> std::io::Result<()> {
        PINNED_CPU.store(cpu, Ordering::SeqCst);
        Ok(())
    }

    fn refuse_to_pin(_cpu: i32) -> std::io::Result<()> {
        Err(std::io::Error::other("no affinity for you"))
    }

    #[test]
    fn a_configured_cpu_reaches_the_writer_thread() {
        let directory = TempDir::new();
        let mut config = config(directory.path(), Level::Info);
        config.cpu = Some(7);
        config.pinner = Some(pin_to);
        let handle = open(config).unwrap();

        // The pinner runs before the writer's first control message.
        handle.flush();
        handle.shutdown();

        assert_eq!(7, PINNED_CPU.load(Ordering::SeqCst));
    }

    #[test]
    fn a_cpu_without_a_pinner_is_refused_before_anything_is_created() {
        let directory = TempDir::new();
        let target = directory.path().join("not-created");
        let mut config = config(&target, Level::Info);
        config.cpu = Some(3);
        config.pinner = None;

        assert_eq!(Err(InitError::MissingPinner), open(config).map(|_| ()));
        assert!(!target.exists(), "the refusal came after a side effect");
    }

    #[test]
    fn a_failing_pinner_is_counted_and_the_writer_keeps_running() {
        let directory = TempDir::new();
        let mut config = config(directory.path(), Level::Info);
        config.cpu = Some(0);
        config.pinner = Some(refuse_to_pin);
        let handle = open(config).unwrap();

        handle.flush();
        assert_eq!(1, handle.io_errors());
        handle.shutdown();
    }

    #[test]
    fn a_guard_shuts_the_writer_down_when_it_drops() {
        let directory = TempDir::new();
        let handle = open(config(directory.path(), Level::Info)).unwrap();
        handle
            .event(Level::Info, Component::Driver, Event::RoleChange)
            .message("last line")
            .emit();

        // No explicit flush or shutdown: the guard's Drop must drain and join.
        drop(Guard::new(handle));

        let text = fs::read_to_string(directory.path().join("driver-node-1.json")).unwrap();
        assert!(text.contains("\"msg\":\"last line\""));
    }
}
