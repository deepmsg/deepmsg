//! The driver's and the cluster's distinct error logs, as JSON events.
//!
//! An independent process beside the driver, like `deepmsg-counters-exporter`
//! and for the same reason: the error a dying driver recorded has to survive
//! the driver. Every source is read-only — this binary opens the CnC file with
//! [`deepmsg_cnc::CncFile::try_open`], which maps it read-only, and reads the
//! rescued files with [`std::fs::read`]. Nothing here writes beside an input.
//!
//! Three sources (`analysis/observability/deepmsg-l1b-plan-errorlog-exporter.md`):
//!
//! - the CnC file's distinct error log, scanned once per interval;
//! - `<aeron_dir>-<datestamp>-error.log` beside the directory, which the next
//!   driver writes when it reclaims a dead one's directory;
//! - the cluster mark's `save_existing_errors` file, in the cluster directory.
//!
//! The CnC region stores the description and the observation count but **no
//! error code** — the code lives in the writing process's de-duplication key
//! (`crates/cnc/src/error_log.rs`'s module documentation) — so an event names
//! its `source` and carries the text, and never invents a code. Entries are
//! identified by `(text, first_observation_timestamp_ms)` and reported by the
//! growth of `observation_count`; each rescued file is consumed once, by path.
//!
//! The CnC source is here; the two rescued-file sources arrive in the next
//! commits.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use deepmsg_cnc::CncFile;
use deepmsg_cnc::error_log::ErrorLogEntry;
use deepmsg_log::{
    Component, Config, DEFAULT_QUEUE_CAPACITY, Event, Guard, Handle, Level, Roll, open,
};

const USAGE: &str = "\
Usage: deepmsg-errorlog-exporter [options]

  --dir <path>          the aeron directory (default: DEEPMSG_AERON_DIR)
  --log-dir <path>      where the JSON files go (default: DEEPMSG_LOG_DIR)
  --node-id <name>      node_id written into every line (default: DEEPMSG_NODE_ID)
  --interval <n>[ms|s]  how often to look (default: 1s)
  --cluster-dir <path>  the cluster directory to watch (default: DEEPMSG_CLUSTER_DIR);
                        absent leaves the cluster source off
  -h, --help            print this message
";

const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);

/// What the environment said before the command line had its say.
#[derive(Debug, Default)]
struct Environment {
    dir: Option<String>,
    log_dir: Option<String>,
    node_id: Option<String>,
    cluster_dir: Option<String>,
}

impl Environment {
    fn read() -> Self {
        Self {
            dir: env_value("DEEPMSG_AERON_DIR"),
            log_dir: env_value("DEEPMSG_LOG_DIR"),
            node_id: env_value("DEEPMSG_NODE_ID"),
            cluster_dir: env_value("DEEPMSG_CLUSTER_DIR"),
        }
    }
}

/// An empty value is the same as an absent one: `DEEPMSG_LOG_DIR=""` is not a
/// directory, and `Config::from_env` reads it the same way.
fn env_value(name: &'static str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

#[derive(Debug)]
struct Options {
    dir: PathBuf,
    log_dir: PathBuf,
    node_id: String,
    interval: Duration,
    cluster_dir: Option<PathBuf>,
}

impl Options {
    fn from_args(args: impl Iterator<Item = String>) -> Result<Self, String> {
        Self::from_values(args, Environment::read())
    }

    /// The pure half of [`Options::from_args`], so tests never touch the
    /// process environment.
    fn from_values(
        args: impl Iterator<Item = String>,
        environment: Environment,
    ) -> Result<Self, String> {
        let mut dir = environment.dir;
        let mut log_dir = environment.log_dir;
        let mut node_id = environment.node_id;
        let mut cluster_dir = environment.cluster_dir;
        let mut interval = DEFAULT_INTERVAL;
        let mut args = args;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => {
                    print!("{USAGE}");
                    std::process::exit(0);
                }
                "--dir" => dir = Some(args.next().ok_or("--dir needs a path")?),
                "--log-dir" => log_dir = Some(args.next().ok_or("--log-dir needs a path")?),
                "--node-id" => node_id = Some(args.next().ok_or("--node-id needs a name")?),
                "--interval" => {
                    let value = args.next().ok_or("--interval needs a duration")?;
                    interval = parse_interval(&value)?;
                }
                "--cluster-dir" => {
                    cluster_dir = Some(args.next().ok_or("--cluster-dir needs a path")?);
                }
                other => return Err(format!("unknown option '{other}'\n\n{USAGE}")),
            }
        }

        let dir = dir.ok_or("no aeron directory: pass --dir or set DEEPMSG_AERON_DIR")?;
        let log_dir = log_dir.ok_or("no log directory: pass --log-dir or set DEEPMSG_LOG_DIR")?;
        let node_id = node_id.ok_or("no node id: pass --node-id or set DEEPMSG_NODE_ID")?;

        let options = Self {
            dir: PathBuf::from(dir),
            log_dir: PathBuf::from(log_dir),
            node_id,
            interval,
            cluster_dir: cluster_dir.map(PathBuf::from),
        };
        options.check_log_directory()?;
        Ok(options)
    }

    /// Refuse a log directory that a reclaimed aeron or cluster directory would
    /// take with it: the driver deletes its directory on the next start.
    fn check_log_directory(&self) -> Result<(), String> {
        let log_dir = resolve(&self.log_dir);
        let aeron = Some(&self.dir);
        let cluster = self.cluster_dir.as_ref();

        for (name, directory) in [("aeron", aeron), ("cluster", cluster)] {
            let Some(directory) = directory else {
                continue;
            };
            let resolved = resolve(directory);
            if log_dir.starts_with(&resolved) {
                return Err(format!(
                    "the log directory {} is inside the {name} directory {}; \
                     a reclaimed directory would delete the logs",
                    self.log_dir.display(),
                    directory.display(),
                ));
            }
        }
        Ok(())
    }

    fn startup_line(&self) -> String {
        format!(
            "deepmsg-errorlog-exporter: watching {} (every {:?}), writing into {}",
            self.dir.display(),
            self.interval,
            self.log_dir.display(),
        )
    }
}

/// `1s`, `500ms`, or a bare number of seconds.
fn parse_interval(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number.trim(), 1_u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number.trim(), 1_000_u64)
    } else {
        (value, 1_000_u64)
    };

    let number: u64 = number
        .parse()
        .map_err(|_| format!("--interval is not a duration: {value}"))?;
    let millis = number
        .checked_mul(multiplier)
        .ok_or("--interval is too large")?;
    if millis == 0 {
        return Err("--interval must be at least 1ms".to_owned());
    }
    Ok(Duration::from_millis(millis))
}

/// The path as this process can rely on comparing it: symlinks resolved when
/// both ends exist, made absolute when they do not.
fn resolve(path: &Path) -> PathBuf {
    std::fs::canonicalize(path)
        .or_else(|_| std::path::absolute(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

fn open_log(options: &Options) -> Result<Handle, deepmsg_log::InitError> {
    open(Config {
        dir: options.log_dir.clone(),
        node_id: options.node_id.clone(),
        component: Component::Tool,
        level: Level::Info,
        component_levels: Vec::new(),
        roll: Roll::default(),
        queue_capacity: DEFAULT_QUEUE_CAPACITY,
        cpu: None,
        pinner: None,
        clock: deepmsg_core::clock::epoch_nano_time,
    })
}

/// How many identities the count table remembers before it forgets the oldest.
///
/// One entry is one description; a busy driver does not produce tens of
/// thousands of distinct ones, so reaching this bound means something is
/// generating unbounded text and the oldest are the safest to forget.
const MAX_IDENTITIES: usize = 65_536;

/// What the exporter has seen of each source, so a status line is printed when
/// it changes and not once per interval.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum SourceState {
    #[default]
    Unseen,
    Readable,
    Unreadable,
}

#[derive(Debug, Default)]
struct State {
    cnc: SourceState,
}

impl State {
    /// Record what this sweep saw of the CnC, answering the line to print when
    /// the state changed.
    fn cnc(&mut self, now: SourceState) -> Option<&'static str> {
        if self.cnc == now || now == SourceState::Unseen {
            self.cnc = now;
            return None;
        }
        self.cnc = now;
        Some(match now {
            SourceState::Readable => "driver error log: readable",
            SourceState::Unreadable => "driver error log: not readable yet; still watching",
            SourceState::Unseen => unreachable!("handled above"),
        })
    }
}

/// The sweep's memory: what every error log said last time, and what it has
/// seen of each source.
#[derive(Debug, Default)]
struct Sweep {
    /// Identity -> the largest `observation_count` already reported.
    ///
    /// The identity is `(text, first_observation_timestamp_ms)` rather than the
    /// text alone: the region stores no error code, so the same text can be two
    /// entries written for two codes, and the first-seen time is the only field
    /// that tells them apart. Two such entries written in the same millisecond
    /// are indistinguishable from the reader and are reported once; the lost
    /// sighting arrives on the next one.
    emitted: HashMap<(String, i64), u64>,
    state: State,
}

impl Sweep {
    /// One sweep over every source.
    fn poll(&mut self, options: &Options, logger: &Handle) {
        self.poll_cnc(options, logger);
    }

    fn poll_cnc(&mut self, options: &Options, logger: &Handle) {
        let cnc = match CncFile::try_open(&options.dir) {
            Ok(cnc) => cnc,
            Err(_) => {
                self.note(SourceState::Unreadable);
                return;
            }
        };
        self.note(SourceState::Readable);

        let Some(reader) = cnc.error_log() else {
            return;
        };
        let mut entries = Vec::new();
        let scan = reader.read(i64::MIN, &mut entries);

        // A region the reader could not walk is a fact about the source, not
        // "no errors": say it, or the exporter looks quiet while it is blind.
        if scan.malformed != 0 || scan.truncated {
            eprintln!(
                "deepmsg-errorlog-exporter: the driver's error log is {}; {} entries read",
                if scan.truncated {
                    "truncated"
                } else {
                    "malformed"
                },
                scan.entries,
            );
        }

        for entry in &entries {
            self.report(
                logger,
                Component::Driver,
                Event::DriverError,
                "cnc",
                entry,
                None,
            );
        }
    }

    fn note(&mut self, now: SourceState) {
        if let Some(line) = self.state.cnc(now) {
            eprintln!("deepmsg-errorlog-exporter: {line}");
        }
    }

    /// Report one entry when its count grew, which is the only signal the
    /// region gives that it was seen again.
    fn report(
        &mut self,
        logger: &Handle,
        component: Component,
        event: Event,
        source: &'static str,
        entry: &ErrorLogEntry,
        rescued_from: Option<&Path>,
    ) {
        // A count of zero is an entry published but not yet counted; it is
        // reported on the sweep after the writer's increment.
        let count = u64::try_from(entry.observation_count).unwrap_or(0);
        let identity = (entry.text.clone(), entry.first_observation_timestamp_ms);
        let seen = self.emitted.get(&identity).copied().unwrap_or(0);
        if count <= seen {
            return;
        }

        let mut log_event = logger
            .event(Level::Error, component, event)
            .message(&entry.text)
            .field_str("source", source)
            .field_u64("observations", count - seen)
            .field_u64("observation_count", count)
            .field_i64("first_observation_ms", entry.first_observation_timestamp_ms)
            .field_i64("last_observation_ms", entry.last_observation_timestamp_ms);
        if let Some(path) = rescued_from {
            log_event = log_event.field_str("rescue_path", &path.to_string_lossy());
        }
        log_event.emit();

        self.remember(identity, count);
    }

    fn remember(&mut self, identity: (String, i64), count: u64) {
        if self.emitted.len() >= MAX_IDENTITIES && !self.emitted.contains_key(&identity) {
            if let Some(oldest) = self.emitted.keys().min_by_key(|(_, first)| *first).cloned() {
                self.emitted.remove(&oldest);
            }
        }
        self.emitted.insert(identity, count);
    }
}

fn main() -> ExitCode {
    let options = match Options::from_args(std::env::args().skip(1)) {
        Ok(options) => options,
        Err(message) => {
            eprintln!("deepmsg-errorlog-exporter: {message}");
            return ExitCode::from(2);
        }
    };

    let handle = match open_log(&options) {
        Ok(handle) => handle,
        Err(error) => {
            eprintln!("deepmsg-errorlog-exporter: cannot open the log: {error}");
            return ExitCode::FAILURE;
        }
    };
    let guard = Guard::new(handle);
    let logger = guard.handle().expect("a guard wraps a handle");

    eprintln!("{}", options.startup_line());
    let mut sweep = Sweep::default();
    if let Err(error) = run(&options, logger, &mut sweep) {
        eprintln!("deepmsg-errorlog-exporter: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run(options: &Options, logger: &Handle, sweep: &mut Sweep) -> io::Result<()> {
    loop {
        sweep.poll(options, logger);
        std::thread::sleep(options.interval);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use deepmsg_cnc::create::{COUNTERS_VALUES_BUFFER_LENGTH_MIN, CncIdentity, CncLayout};
    use deepmsg_cnc::error_log::DistinctErrorLog;

    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            let suffix = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "deepmsg-errorlog-exporter-{}-{suffix}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("temp directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn environment(dir: &Path, log_dir: &Path) -> Environment {
        Environment {
            dir: Some(dir.display().to_string()),
            log_dir: Some(log_dir.display().to_string()),
            node_id: Some("node-1".to_owned()),
            cluster_dir: None,
        }
    }

    fn options(args: &[&str], environment: Environment) -> Result<Options, String> {
        Options::from_values(args.iter().map(|arg| (*arg).to_owned()), environment)
    }

    #[test]
    fn a_duration_is_seconds_unless_it_names_milliseconds() {
        assert_eq!(Duration::from_secs(1), parse_interval("1").unwrap());
        assert_eq!(Duration::from_secs(1), parse_interval("1s").unwrap());
        assert_eq!(Duration::from_millis(500), parse_interval("500ms").unwrap());
        assert_eq!(Duration::from_millis(2), parse_interval(" 2 ms ").unwrap());
        assert_eq!(Duration::from_secs(90), parse_interval("90").unwrap());
    }

    #[test]
    fn a_zero_or_unreadable_interval_is_refused() {
        for value in ["0", "0s", "0ms", "", "abc", "1m", "-1s", "1.5s"] {
            assert!(parse_interval(value).is_err(), "{value} parsed");
        }
    }

    #[test]
    fn the_environment_is_the_fallback_and_the_flags_win() {
        let root = TempDir::new();
        let aeron = root.path().join("aeron");
        let logs = root.path().join("logs");
        let other = root.path().join("other");
        let other_logs = root.path().join("other-logs");

        let from_environment = options(&[], environment(&aeron, &logs)).expect("valid");
        assert_eq!(aeron, from_environment.dir);
        assert_eq!(logs, from_environment.log_dir);
        assert_eq!("node-1", from_environment.node_id);
        assert_eq!(DEFAULT_INTERVAL, from_environment.interval);
        assert_eq!(None, from_environment.cluster_dir);

        let overridden = options(
            &[
                "--dir",
                other.to_str().unwrap(),
                "--log-dir",
                other_logs.to_str().unwrap(),
                "--node-id",
                "node-2",
                "--interval",
                "250ms",
                "--cluster-dir",
                other.to_str().unwrap(),
            ],
            environment(&aeron, &logs),
        )
        .unwrap();
        assert_eq!(other, overridden.dir);
        assert_eq!("node-2", overridden.node_id);
        assert_eq!(Duration::from_millis(250), overridden.interval);
        assert_eq!(Some(other.clone()), overridden.cluster_dir);
    }

    #[test]
    fn a_missing_directory_or_node_id_is_refused() {
        let root = TempDir::new();
        let aeron = root.path().join("aeron");
        let logs = root.path().join("logs");
        let mut env = environment(&aeron, &logs);
        env.dir = None;
        assert!(options(&[], env).is_err());

        let mut env = environment(&aeron, &logs);
        env.log_dir = None;
        assert!(options(&[], env).is_err());

        let mut env = environment(&aeron, &logs);
        env.node_id = None;
        assert!(options(&[], env).is_err());
    }

    #[test]
    fn an_unknown_option_is_refused() {
        let root = TempDir::new();
        let environment = environment(&root.path().join("aeron"), &root.path().join("logs"));
        assert!(options(&["--nope"], environment).is_err());
    }

    #[test]
    fn a_log_directory_inside_the_aeron_directory_is_refused() {
        let root = TempDir::new();
        let aeron = root.path().join("aeron");
        std::fs::create_dir_all(aeron.join("logs")).unwrap();

        let error = options(&[], environment(&aeron, &aeron.join("logs"))).unwrap_err();
        assert!(error.contains("inside the aeron directory"), "{error}");
    }

    #[test]
    fn a_log_directory_inside_the_cluster_directory_is_refused() {
        let root = TempDir::new();
        let aeron = root.path().join("aeron");
        let cluster = root.path().join("cluster");
        std::fs::create_dir_all(cluster.join("logs")).unwrap();

        let mut environment = environment(&aeron, &cluster.join("logs"));
        environment.cluster_dir = Some(cluster.display().to_string());
        let error = options(&[], environment).unwrap_err();
        assert!(error.contains("inside the cluster directory"), "{error}");
    }

    #[test]
    fn a_separate_log_directory_is_accepted() {
        let root = TempDir::new();
        let aeron = root.path().join("aeron");
        let logs = root.path().join("logs");
        std::fs::create_dir_all(&aeron).unwrap();
        std::fs::create_dir_all(&logs).unwrap();

        options(&[], environment(&aeron, &logs)).expect("separate directories");
    }

    #[test]
    fn the_startup_line_names_the_directory_and_the_interval() {
        let root = TempDir::new();
        let aeron = root.path().join("aeron");
        let logs = root.path().join("logs");
        let options = options(&[], environment(&aeron, &logs)).unwrap();

        let line = options.startup_line();
        assert!(line.contains(&aeron.display().to_string()), "{line}");
        assert!(line.contains(&logs.display().to_string()), "{line}");
        assert!(line.contains("1s"), "{line}");
    }

    #[test]
    fn a_source_line_is_printed_when_the_state_changes_and_not_before() {
        let mut state = State::default();
        assert_eq!(
            Some("driver error log: not readable yet; still watching"),
            state.cnc(SourceState::Unreadable)
        );
        assert_eq!(None, state.cnc(SourceState::Unreadable));
        assert_eq!(
            Some("driver error log: readable"),
            state.cnc(SourceState::Readable)
        );
        assert_eq!(None, state.cnc(SourceState::Readable));
        assert_eq!(
            Some("driver error log: not readable yet; still watching"),
            state.cnc(SourceState::Unreadable)
        );
    }

    const NOW_MS: i64 = 1_700_000_000_000;

    /// A published CnC with a writable error log, the exporter reading it, and
    /// the first-seen state the exporter has.
    struct Fixture {
        options: Options,
        logger: Handle,
        cnc: CncFile,
        log: DistinctErrorLog,
        sweep: Sweep,
        // Dropped last: the directory outlives the writer and the mapping.
        _root: TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            let root = TempDir::new();
            let options = Options {
                dir: root.path().join("aeron"),
                log_dir: root.path().join("logs"),
                node_id: "node-1".to_owned(),
                interval: DEFAULT_INTERVAL,
                cluster_dir: None,
            };
            std::fs::create_dir_all(&options.dir).expect("aeron directory");

            let layout = CncLayout {
                counters_values_length: COUNTERS_VALUES_BUFFER_LENGTH_MIN,
                ..CncLayout::default()
            };
            let mut cnc = CncFile::create(
                &options.dir,
                &layout,
                &CncIdentity {
                    liveness_timeout_ns: 10_000_000_000,
                    start_timestamp_ms: NOW_MS,
                    pid: 4242,
                },
            )
            .expect("create the CnC file");
            cnc.write_consumer_heartbeat(NOW_MS).expect("heartbeat");
            cnc.publish().expect("publish");

            let logger = open_log(&options).expect("open the log");
            Self {
                options,
                logger,
                cnc,
                log: DistinctErrorLog::new(),
                sweep: Sweep::default(),
                _root: root,
            }
        }

        fn record(&mut self, code: i32, text: &str) {
            self.record_at(code, text, NOW_MS);
        }

        fn record_at(&mut self, code: i32, text: &str, now_ms: i64) {
            let region = self.cnc.error_log_writable().expect("writable error log");
            self.log
                .record(&region, now_ms, code, text)
                .expect("record");
        }

        fn poll(&mut self) {
            self.sweep.poll(&self.options, &self.logger);
            self.logger.flush();
        }

        fn lines(&self) -> Vec<String> {
            std::fs::read_to_string(self.options.log_dir.join("tool-node-1.json"))
                .unwrap_or_default()
                .lines()
                .map(str::to_owned)
                .collect()
        }
    }

    #[test]
    fn the_first_sweep_reports_every_distinct_error_once() {
        let mut fixture = Fixture::new();
        fixture.record(11, "first failure");
        fixture.record(11, "second failure");
        fixture.poll();

        let lines = fixture.lines();
        assert_eq!(2, lines.len(), "{lines:?}");
        for line in &lines {
            assert!(line.contains("\"level\":\"error\""), "{line}");
            assert!(line.contains("\"component\":\"driver\""), "{line}");
            assert!(line.contains("\"event\":\"driver.error\""), "{line}");
            assert!(line.contains("\"source\":\"cnc\""), "{line}");
            assert!(line.contains("\"observations\":1"), "{line}");
            assert!(line.contains("\"observation_count\":1"), "{line}");
            assert!(!line.contains("error_code"), "{line}");
        }
        assert!(lines.iter().any(|line| line.contains("first failure")));
        assert!(lines.iter().any(|line| line.contains("second failure")));
    }

    #[test]
    fn a_sweep_with_no_growth_says_nothing() {
        let mut fixture = Fixture::new();
        fixture.record(11, "a failure");
        fixture.poll();
        assert_eq!(1, fixture.lines().len());

        fixture.poll();
        fixture.poll();
        assert_eq!(1, fixture.lines().len(), "a quiet sweep emitted again");
    }

    #[test]
    fn a_repeated_sighting_reports_only_the_growth() {
        let mut fixture = Fixture::new();
        fixture.record(11, "a failure");
        fixture.poll();
        assert_eq!(1, fixture.lines().len());

        fixture.record(11, "a failure");
        fixture.record(11, "a failure");
        fixture.poll();

        let lines = fixture.lines();
        assert_eq!(2, lines.len(), "{lines:?}");
        let growth = &lines[1];
        assert!(growth.contains("\"observations\":2"), "{growth}");
        assert!(growth.contains("\"observation_count\":3"), "{growth}");
    }

    #[test]
    fn a_new_description_is_reported_after_the_first_sweep() {
        let mut fixture = Fixture::new();
        fixture.record(11, "a failure");
        fixture.poll();

        fixture.record(11, "another failure");
        fixture.poll();

        let lines = fixture.lines();
        assert_eq!(2, lines.len(), "{lines:?}");
        assert!(lines[1].contains("another failure"), "{}", lines[1]);
        assert!(lines[1].contains("\"observation_count\":1"), "{}", lines[1]);
    }

    #[test]
    fn the_same_text_under_two_codes_is_two_entries() {
        let mut fixture = Fixture::new();
        fixture.record_at(11, "same problem", NOW_MS);
        fixture.record_at(3, "same problem", NOW_MS + 1);
        fixture.poll();

        assert_eq!(2, fixture.lines().len());
    }

    #[test]
    fn a_sweep_changes_no_input_byte_and_no_directory_entry() {
        let mut fixture = Fixture::new();
        fixture.record(11, "a failure");

        let before = directory_digest(&fixture.options.dir);
        fixture.poll();
        assert_eq!(1, fixture.lines().len(), "the sweep was alive");
        let after = directory_digest(&fixture.options.dir);
        assert_eq!(before, after);
    }

    #[test]
    fn the_count_table_forgets_the_oldest_identity_at_its_bound() {
        let mut sweep = Sweep::default();
        for index in 0..=MAX_IDENTITIES {
            let first = i64::try_from(index).expect("an index that fits");
            sweep.remember((format!("error {index}"), first), 1);
        }

        assert_eq!(MAX_IDENTITIES, sweep.emitted.len());
        assert!(
            !sweep.emitted.contains_key(&("error 0".to_owned(), 0)),
            "the oldest identity survived"
        );
        assert!(
            sweep
                .emitted
                .contains_key(&(format!("error {MAX_IDENTITIES}"), MAX_IDENTITIES as i64))
        );
    }

    fn directory_digest(dir: &Path) -> Vec<(String, Vec<u8>)> {
        let mut entries: Vec<(String, Vec<u8>)> = std::fs::read_dir(dir)
            .expect("read the directory")
            .map(|entry| {
                let entry = entry.expect("a directory entry");
                (
                    entry.file_name().to_string_lossy().into_owned(),
                    std::fs::read(entry.path()).expect("read the entry"),
                )
            })
            .collect();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries
    }
}
