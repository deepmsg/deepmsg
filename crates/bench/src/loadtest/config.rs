//! The reference's knobs, and the rules it applies to them.
//!
//! Mirrors `benchmarks-api/src/main/java/io/aeron/benchmarks/Configuration.java`.
//! The property names, the defaults, the accepted ranges and the text of every
//! complaint are the reference's, because a run is described by the same
//! settings on both sides and a difference here would silently make two
//! configurations look like one.
//!
//! Where the reference names a *class* — for the transceiver and the idle
//! strategy — this names a variant, because Rust has no reflection to load one
//! by name from a string. Nothing feeds a Java-written file to this reader, so
//! the values never have to agree (the decision is recorded in
//! `analysis/bench/deepmsg-rust-loadtestrig-plan.md` §7 Q6).

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

/// Which properties name the transceiver, the idle strategy, and so on.
///
/// Spelled out rather than inlined so that a reader can compare them with
/// `Configuration.java:111-208` at a glance.
pub mod property {
    /// `Configuration.ITERATIONS_PROP_NAME`.
    pub const ITERATIONS: &str = "io.aeron.benchmarks.iterations";
    /// `Configuration.WARMUP_ITERATIONS_PROP_NAME`.
    pub const WARMUP_ITERATIONS: &str = "io.aeron.benchmarks.warmup.iterations";
    /// `Configuration.MESSAGE_RATE_PROP_NAME`.
    pub const MESSAGE_RATE: &str = "io.aeron.benchmarks.message.rate";
    /// `Configuration.WARMUP_MESSAGE_RATE_PROP_NAME`.
    pub const WARMUP_MESSAGE_RATE: &str = "io.aeron.benchmarks.warmup.message.rate";
    /// `Configuration.BATCH_SIZE_PROP_NAME`.
    pub const BATCH_SIZE: &str = "io.aeron.benchmarks.batch.size";
    /// `Configuration.MESSAGE_LENGTH_PROP_NAME`.
    pub const MESSAGE_LENGTH: &str = "io.aeron.benchmarks.message.length";
    /// `Configuration.MESSAGE_TRANSCEIVER_PROP_NAME`.
    pub const MESSAGE_TRANSCEIVER: &str = "io.aeron.benchmarks.message.transceiver";
    /// `Configuration.IDLE_STRATEGY_PROP_NAME`.
    pub const IDLE_STRATEGY: &str = "io.aeron.benchmarks.idle.strategy";
    /// `Configuration.OUTPUT_DIRECTORY_PROP_NAME`.
    pub const OUTPUT_DIRECTORY: &str = "io.aeron.benchmarks.output.directory";
    /// `Configuration.OUTPUT_FILE_NAME_PROP_NAME`.
    pub const OUTPUT_FILE: &str = "io.aeron.benchmarks.output.file";
    /// `Configuration.TRACK_HISTORY_PROP_NAME`.
    pub const TRACK_HISTORY: &str = "io.aeron.benchmarks.track.history";
    /// `Configuration.REPORT_PROGRESS_PROP_NAME`.
    pub const REPORT_PROGRESS: &str = "io.aeron.benchmarks.report.progress";
    /// `Configuration.OUTPUT_TIME_UNIT_PROPERTY_NAME`.
    pub const OUTPUT_TIME_UNIT: &str = "io.aeron.benchmarks.output.time.unit";
    /// `Configuration.RECEIVE_DEADLINE_SECONDS_PROP_NAME`.
    pub const RECEIVE_DEADLINE_SECONDS: &str = "io.aeron.benchmarks.receive.deadline.seconds";
    /// `Configuration.SEND_GRACE_MILLIS_PROP_NAME`.
    pub const SEND_GRACE_MILLIS: &str = "io.aeron.benchmarks.send.grace.millis";
}

/// `Configuration.DEFAULT_WARMUP_ITERATIONS`.
pub const DEFAULT_WARMUP_ITERATIONS: u32 = 10;
/// `Configuration.DEFAULT_ITERATIONS`.
pub const DEFAULT_ITERATIONS: u32 = 10;
/// `Configuration.DEFAULT_WARMUP_MESSAGE_RATE`.
pub const DEFAULT_WARMUP_MESSAGE_RATE: u32 = 10_000;
/// `Configuration.DEFAULT_BATCH_SIZE`.
pub const DEFAULT_BATCH_SIZE: u32 = 1;
/// `Configuration.DEFAULT_TRACK_HISTORY`.
pub const DEFAULT_TRACK_HISTORY: bool = false;
/// `Configuration.DEFAULT_REPORT_PROGRESS`.
pub const DEFAULT_REPORT_PROGRESS: bool = false;
/// `Configuration.DEFAULT_RECEIVE_DEADLINE_SECONDS`.
pub const DEFAULT_RECEIVE_DEADLINE_SECONDS: u32 = 3;
/// `Configuration.DEFAULT_SEND_GRACE_MILLIS`.
pub const DEFAULT_SEND_GRACE_MILLIS: u32 = 100;
/// `Configuration.MIN_MESSAGE_LENGTH` — a timestamp and a checksum, nothing more.
pub const MIN_MESSAGE_LENGTH: u32 = 16;
/// `Configuration.MAX_MESSAGE_RATE`, which is also the largest `int` the rate
/// parser will produce.
pub const MAX_MESSAGE_RATE: u32 = 1_000_000_000;

/// The largest `K` prefix that does not overflow an `int`
/// (`Configuration.MAX_K_VALUE`).
const MAX_K_VALUE: i32 = i32::MAX / 1000;
/// `Configuration.MAX_M_VALUE`.
const MAX_M_VALUE: i32 = i32::MAX / 1_000_000;

/// The subdirectory of the output directory the transceiver's own diagnostics
/// go in (`Configuration.LOGS_DIR`).
const LOGS_DIR: &str = "logs";

/// Why a configuration could not be built.
///
/// The variants carry values rather than a rendered message so a caller can act
/// on them, and [`fmt::Display`] renders the reference's own wording — the
/// tests that compare against `ConfigurationTest` compare text, and a rig that
/// refuses a run with a different complaint than the reference's would send a
/// reader looking in the wrong place.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// `"property '<name>' is required!"`.
    Required {
        /// The property that had no value.
        property: &'static str,
    },
    /// `"non-integer value for property '<name>', cause: error parsing int: <value>"`.
    NonInteger {
        /// The property being read.
        property: &'static str,
        /// What it held.
        value: String,
    },
    /// `"invalid rate specified in '<name>'"`, with the cause underneath.
    InvalidRate {
        /// The property being read.
        property: &'static str,
        /// Why the value was not a rate; the reference's `NumberFormatException`
        /// message.
        cause: String,
    },
    /// `"'<name>' cannot be less than <minimum>, got: <value>"`.
    BelowMinimum {
        /// The property being checked.
        property: &'static str,
        /// Its lower bound.
        minimum: i32,
        /// What it held.
        value: i32,
    },
    /// `"'<name>' cannot be greater than <maximum>, got: <value>"`.
    AboveMaximum {
        /// The property being checked.
        property: &'static str,
        /// Its upper bound.
        maximum: i32,
        /// What it held.
        value: i32,
    },
    /// `"Output file name prefix cannot be empty!"`.
    EmptyOutputFileNamePrefix,
    /// No idle strategy by that name.
    UnknownIdleStrategy {
        /// What the property held.
        value: String,
    },
    /// No transceiver by that name.
    UnknownTransceiver {
        /// What the property held.
        value: String,
    },
    /// No time unit by that name.
    UnknownTimeUnit {
        /// What the property held.
        value: String,
    },
    /// `"failed to create <name> directory: <path>"`.
    DirectoryNotCreated {
        /// What the directory is for — `output` or `log`.
        purpose: &'static str,
        /// Where it was meant to be.
        path: PathBuf,
    },
    /// `"<name> directory is not writeable: <path>"`.
    DirectoryNotWritable {
        /// What the directory is for.
        purpose: &'static str,
        /// Where it is.
        path: PathBuf,
    },
    /// The working directory could not be read, so a relative output path could
    /// not be made absolute.
    WorkingDirectory {
        /// What the operating system said.
        message: String,
    },
    /// A properties file could not be read.
    ///
    /// The reference swallows this — `PropertiesUtil.loadPropertiesFile` wraps
    /// each of its three attempts in `catch (final Exception ignore)` — and
    /// then complains about whichever required property the missing file was
    /// going to supply. Refusing by name is the same outcome with a better
    /// message, and the outcome is the same because a file that cannot be read
    /// supplies nothing either way.
    PropertiesFile {
        /// Which file.
        path: PathBuf,
        /// What the operating system said.
        message: String,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Required { property } => write!(f, "property '{property}' is required!"),
            Self::NonInteger { property, value } => write!(
                f,
                "non-integer value for property '{property}', cause: error parsing int: {value}"
            ),
            Self::InvalidRate { property, cause } => {
                write!(f, "invalid rate specified in '{property}': {cause}")
            }
            Self::BelowMinimum {
                property,
                minimum,
                value,
            } => write!(
                f,
                "'{property}' cannot be less than {minimum}, got: {value}"
            ),
            Self::AboveMaximum {
                property,
                maximum,
                value,
            } => write!(
                f,
                "'{property}' cannot be greater than {maximum}, got: {value}"
            ),
            Self::EmptyOutputFileNamePrefix => {
                write!(f, "Output file name prefix cannot be empty!")
            }
            Self::UnknownIdleStrategy { value } => write!(
                f,
                "no idle strategy named '{value}'; known: {}",
                IdleStrategy::NAMES.join(", ")
            ),
            Self::UnknownTransceiver { value } => write!(
                f,
                "no message transceiver named '{value}'; known: {}",
                Transceiver::NAMES.join(", ")
            ),
            Self::UnknownTimeUnit { value } => write!(
                f,
                "no time unit named '{value}'; known: {}",
                TimeUnit::NAMES.join(", ")
            ),
            Self::DirectoryNotCreated { purpose, path } => {
                write!(
                    f,
                    "failed to create {purpose} directory: {}",
                    path.display()
                )
            }
            Self::DirectoryNotWritable { purpose, path } => {
                write!(
                    f,
                    "{purpose} directory is not writeable: {}",
                    path.display()
                )
            }
            Self::WorkingDirectory { message } => {
                write!(f, "could not read the working directory: {message}")
            }
            Self::PropertiesFile { path, message } => {
                write!(
                    f,
                    "could not read properties file {}: {message}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// The unit a reported latency is in.
///
/// `java.util.concurrent.TimeUnit`, restricted to the seven values the
/// reference's `output.time.unit` accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeUnit {
    /// Nanoseconds.
    Nanoseconds,
    /// Microseconds — the reference's default.
    Microseconds,
    /// Milliseconds.
    Milliseconds,
    /// Seconds.
    Seconds,
    /// Minutes.
    Minutes,
    /// Hours.
    Hours,
    /// Days.
    Days,
}

impl TimeUnit {
    /// The names this accepts, for a caller that has to list them.
    pub const NAMES: &'static [&'static str] = &[
        "NANOSECONDS",
        "MICROSECONDS",
        "MILLISECONDS",
        "SECONDS",
        "MINUTES",
        "HOURS",
        "DAYS",
    ];

    /// The reference's name for this unit, which is what it prints.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Nanoseconds => "NANOSECONDS",
            Self::Microseconds => "MICROSECONDS",
            Self::Milliseconds => "MILLISECONDS",
            Self::Seconds => "SECONDS",
            Self::Minutes => "MINUTES",
            Self::Hours => "HOURS",
            Self::Days => "DAYS",
        }
    }

    /// Case-insensitively, as the reference does with `valueOf(toUpperCase())`.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let upper = text.to_ascii_uppercase();

        Self::NAMES
            .iter()
            .copied()
            .find(|name| *name == upper)
            .map(|name| match name {
                "NANOSECONDS" => Self::Nanoseconds,
                "MICROSECONDS" => Self::Microseconds,
                "MILLISECONDS" => Self::Milliseconds,
                "SECONDS" => Self::Seconds,
                "MINUTES" => Self::Minutes,
                "HOURS" => Self::Hours,
                _ => Self::Days,
            })
            .ok_or_else(|| ConfigError::UnknownTimeUnit {
                value: text.to_owned(),
            })
    }

    /// What to divide recorded nanoseconds by to print in this unit
    /// (`Configuration.outputScaleRatio`, `:885-896`).
    #[must_use]
    pub fn scale_ratio(self) -> f64 {
        match self {
            Self::Nanoseconds => 1.0,
            Self::Microseconds => 1000.0,
            Self::Milliseconds => 1_000_000.0,
            Self::Seconds => 1_000_000_000.0,
            Self::Minutes => 60.0 * 1_000_000_000.0,
            Self::Hours => 60.0 * 60.0 * 1_000_000_000.0,
            Self::Days => 24.0 * 60.0 * 60.0 * 1_000_000_000.0,
        }
    }
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// What the rig does while it waits.
///
/// The reference's `io.aeron.benchmarks.idle.strategy` holds a class name from
/// `org.agrona.concurrent`; there is no reflection here, so the property holds
/// one of these names instead. What each one *does* is in
/// [`super::transceiver`], next to the `idle` and `reset` the rig calls.
///
/// A configuration always holds a fresh one: the backoff's count is the
/// strategy's own state and not a setting, so a rig copies the value out of the
/// configuration and mutates the copy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IdleStrategy {
    /// The reference's default: `Thread.onSpinWait` in a loop.
    #[default]
    BusySpin,
    /// Do nothing at all.
    NoOp,
    /// `Thread.yield` in a loop.
    Yielding,
    /// Yield, then sleep, as the pauses grow.
    Backoff {
        /// How many times `idle` has been called since the last `reset`.
        idle_count: u32,
    },
    /// Sleep a fixed short interval.
    Sleeping,
}

impl IdleStrategy {
    /// The names this accepts, in the order the error message lists them.
    pub const NAMES: &'static [&'static str] =
        &["busy-spin", "no-op", "yielding", "backoff", "sleeping"];

    /// The name a configuration prints and a properties file holds.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::BusySpin => "busy-spin",
            Self::NoOp => "no-op",
            Self::Yielding => "yielding",
            Self::Backoff { .. } => "backoff",
            Self::Sleeping => "sleeping",
        }
    }

    /// The name only; the property's default is the reference's, not "the
    /// closest one".
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        match text {
            "busy-spin" => Ok(Self::BusySpin),
            "no-op" => Ok(Self::NoOp),
            "yielding" => Ok(Self::Yielding),
            "backoff" => Ok(Self::Backoff { idle_count: 0 }),
            "sleeping" => Ok(Self::Sleeping),
            other => Err(ConfigError::UnknownIdleStrategy {
                value: other.to_owned(),
            }),
        }
    }
}

impl fmt::Display for IdleStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Which system is under test.
///
/// The reference loads one of these by class name. Only the in-memory one
/// exists so far; the echo transceivers arrive with the client-side work, and
/// until they do a name that resolves to nothing is refused rather than
/// silently running something else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transceiver {
    /// The rig's own ring, which measures the rig and nothing else
    /// (`InMemoryMessageTransceiver`).
    InMemory,
}

impl Transceiver {
    /// The names this accepts.
    pub const NAMES: &'static [&'static str] = &["in-memory"];

    /// The name a properties file holds.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::InMemory => "in-memory",
        }
    }

    /// The name only.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        match text {
            "in-memory" => Ok(Self::InMemory),
            other => Err(ConfigError::UnknownTransceiver {
                value: other.to_owned(),
            }),
        }
    }
}

impl fmt::Display for Transceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A bag of settings, as a properties file or a `-D` argument leaves it.
///
/// `java.util.Properties`, in the two ways the reference actually uses it:
/// loading files in order, and merging a lower-priority set into it. The file
/// grammar is not the whole of `java.util.Properties` — see
/// [`Properties::load_file`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Properties {
    values: BTreeMap<String, String>,
}

impl Properties {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The value for `name`, if it has a non-empty one.
    ///
    /// Empty and absent are the same thing throughout the reference: its
    /// `isPropertyProvided` asks `!isEmpty(getProperty(name))`, so a property
    /// set to the empty string is a property that was not set.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.values
            .get(name)
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    }

    /// Set a value, replacing whatever was there.
    pub fn set(&mut self, name: &str, value: &str) {
        self.values.insert(name.to_owned(), value.to_owned());
    }

    /// Read a properties file into this set, later keys winning.
    ///
    /// The grammar is the subset the reference's own files are written in:
    /// `key=value` or `key: value` on one line, `#` or `!` to start a comment,
    /// blank lines ignored, surrounding whitespace trimmed, and a trailing
    /// `\` continuing onto the next line. `java.util.Properties` also decodes
    /// `\uXXXX`, `\t` and friends and treats a bare key as an empty value;
    /// none of that appears in the files this reads, and a half-implemented
    /// escape decoder would be worse than not having one.
    pub fn load_file(&mut self, path: &Path) -> Result<(), ConfigError> {
        let text = fs::read_to_string(path).map_err(|error| ConfigError::PropertiesFile {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;

        let mut logical = String::new();
        for line in text.lines() {
            let continued = line.ends_with('\\');
            // A continuation line's own leading whitespace is not part of the
            // value, which is what `java.util.Properties` does and what the
            // alignment of a wrapped key in a real file assumes.
            if logical.is_empty() {
                logical.push_str(line.trim_end_matches('\\'));
            } else {
                logical.push_str(line.trim_start().trim_end_matches('\\'));
            }

            if continued {
                continue;
            }

            self.take_logical_line(&logical);
            logical.clear();
        }
        self.take_logical_line(&logical);

        Ok(())
    }

    /// One line, as a key and a value.
    fn take_logical_line(&mut self, line: &str) {
        let line = line.trim();

        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            return;
        }

        let split = line.find(['=', ':']);

        match split {
            Some(at) => self.set(line[..at].trim(), line[at + 1..].trim()),
            // `java.util.Properties` reads a lone key as an empty value, which
            // `get` then treats as unset — the same as not reading it at all.
            None => self.set(line, ""),
        }
    }

    /// Merge a lower-priority set in: what is here wins
    /// (`PropertiesUtil.mergeWithProperties` with `PRESERVE`).
    pub fn merge_keeping_existing(&mut self, other: &Self) {
        for (name, value) in &other.values {
            if !self.values.contains_key(name) {
                self.values.insert(name.clone(), value.clone());
            }
        }
    }

    /// The value for a property that has to be there
    /// (`Configuration.getPropertyValue`, `:1017-1026`).
    fn required(&self, name: &'static str) -> Result<&str, ConfigError> {
        self.get(name)
            .ok_or(ConfigError::Required { property: name })
    }

    /// An `int`, refusing anything else with the reference's complaint
    /// (`Configuration.intProperty`, `:1003-1015`).
    fn integer(&self, name: &'static str) -> Result<i32, ConfigError> {
        let value = self.required(name)?;

        value.parse::<i32>().map_err(|_| ConfigError::NonInteger {
            property: name,
            value: value.to_owned(),
        })
    }

    /// A rate: a plain number, or one ending in `K` or `M`
    /// (`Configuration.rateProperty`, `:961-1001`).
    ///
    /// The suffix arithmetic is the reference's, including that it multiplies
    /// an `int` and refuses a prefix that would overflow one.
    fn rate(&self, name: &'static str) -> Result<i32, ConfigError> {
        let value = self.required(name)?;

        let parse = || -> Result<i32, String> {
            let last = value
                .chars()
                .last()
                .ok_or_else(|| format!("{name}: {value} should end with: K or M."))?;

            if last.is_ascii_digit() {
                return value
                    .parse::<i32>()
                    .map_err(|_| format!("error parsing int: {value}"));
            }

            let prefix = &value[..value.len() - last.len_utf8()];
            let prefix = prefix
                .parse::<i32>()
                .map_err(|_| format!("error parsing int: {value}"))?;

            match last {
                'K' => {
                    if prefix > MAX_K_VALUE {
                        return Err(format!("{name} would overflow an int: {value}"));
                    }
                    Ok(prefix * 1000)
                }
                'M' => {
                    if prefix > MAX_M_VALUE {
                        return Err(format!("{name} would overflow an int: {value}"));
                    }
                    Ok(prefix * 1_000_000)
                }
                _ => Err(format!("{name}: {value} should end with: K or M.")),
            }
        };

        parse().map_err(|cause| ConfigError::InvalidRate {
            property: name,
            cause,
        })
    }

    /// A rate when the property is there, and the fallback otherwise. The range
    /// is [`Builder::build`]'s business, as it is the reference's.
    fn rate_or_default(&self, name: &'static str, default: u32) -> Result<i32, ConfigError> {
        if self.get(name).is_none() {
            return Ok(i32::try_from(default).unwrap_or(i32::MAX));
        }

        self.rate(name)
    }

    /// `Boolean.parseBoolean`: only `true`, in any case, is true.
    fn boolean_or_default(&self, name: &str, default: bool) -> bool {
        match self.get(name) {
            Some(value) => value.eq_ignore_ascii_case("true"),
            None => default,
        }
    }
}

/// `Configuration.checkValueRange` (`:899-912`), whose two complaints are
/// compared against the reference's tests word for word.
fn check_range(
    value: i32,
    minimum: i32,
    maximum: i32,
    property: &'static str,
) -> Result<u32, ConfigError> {
    if value < minimum {
        return Err(ConfigError::BelowMinimum {
            property,
            minimum,
            value,
        });
    }

    if value > maximum {
        return Err(ConfigError::AboveMaximum {
            property,
            maximum,
            value,
        });
    }

    // Every minimum in this file is zero or more, so the result cannot be
    // negative.
    Ok(value.unsigned_abs())
}

/// Create a directory and insist that it is a writable one
/// (`Configuration.validateDirectory`, `:1061-1082`).
fn validate_directory(directory: &Path, purpose: &'static str) -> Result<PathBuf, ConfigError> {
    let absolute = absolute_path(directory)?;

    // `create_dir_all` succeeds when the directory is already there, which is
    // the reference's `mkdirs()` returning false and falling through.
    if fs::create_dir_all(&absolute).is_err() || !absolute.is_dir() {
        return Err(ConfigError::DirectoryNotCreated {
            purpose,
            path: absolute,
        });
    }

    match fs::metadata(&absolute) {
        Ok(metadata) if metadata.permissions().readonly() => {
            Err(ConfigError::DirectoryNotWritable {
                purpose,
                path: absolute,
            })
        }
        Ok(_) => Ok(absolute),
        Err(_) => Err(ConfigError::DirectoryNotCreated {
            purpose,
            path: absolute,
        }),
    }
}

/// The path made absolute against the working directory, as Java's
/// `toAbsolutePath` does — joined, not canonicalised, so a path with `..` in it
/// stays as it was written.
fn absolute_path(path: &Path) -> Result<PathBuf, ConfigError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    std::env::current_dir()
        .map(|directory| directory.join(path))
        .map_err(|error| ConfigError::WorkingDirectory {
            message: error.to_string(),
        })
}

/// One run's settings.
///
/// Built through [`Builder`], or from a set of [`Properties`]. Every field is
/// validated when the configuration is built, not when it is used, which is
/// what the reference does and what makes a bad properties file fail before a
/// driver is started.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Configuration {
    warmup_iterations: u32,
    iterations: u32,
    warmup_message_rate: u32,
    message_rate: u32,
    batch_size: u32,
    message_length: u32,
    transceiver: Transceiver,
    idle_strategy: IdleStrategy,
    output_directory: PathBuf,
    logs_directory: PathBuf,
    output_file_name_prefix: String,
    track_history: bool,
    report_progress: bool,
    output_time_unit: TimeUnit,
    receive_deadline_seconds: u32,
    send_grace_millis: u32,
    rate: String,
}

impl Configuration {
    /// The settings a set of properties describes
    /// (`Configuration.fromSystemProperties`, `:715-786`).
    pub fn from_properties(properties: &Properties) -> Result<Self, ConfigError> {
        let mut builder = Builder::new();

        if properties.get(property::WARMUP_ITERATIONS).is_some() {
            builder.warmup_iterations = properties.integer(property::WARMUP_ITERATIONS)?;
        }
        if properties.get(property::ITERATIONS).is_some() {
            builder.iterations = properties.integer(property::ITERATIONS)?;
        }
        if properties.get(property::BATCH_SIZE).is_some() {
            builder.batch_size = properties.integer(property::BATCH_SIZE)?;
        }
        if properties.get(property::MESSAGE_LENGTH).is_some() {
            builder.message_length = properties.integer(property::MESSAGE_LENGTH)?;
        }
        if let Some(text) = properties.get(property::IDLE_STRATEGY) {
            builder.idle_strategy = IdleStrategy::parse(text)?;
        }
        if let Some(text) = properties.get(property::OUTPUT_DIRECTORY) {
            builder.output_directory = PathBuf::from(text);
        }
        if properties.get(property::TRACK_HISTORY).is_some() {
            builder.track_history =
                properties.boolean_or_default(property::TRACK_HISTORY, DEFAULT_TRACK_HISTORY);
        }
        if properties.get(property::REPORT_PROGRESS).is_some() {
            builder.report_progress =
                properties.boolean_or_default(property::REPORT_PROGRESS, DEFAULT_REPORT_PROGRESS);
        }
        if let Some(text) = properties.get(property::OUTPUT_TIME_UNIT) {
            builder.output_time_unit = TimeUnit::parse(text)?;
        }
        if properties.get(property::RECEIVE_DEADLINE_SECONDS).is_some() {
            builder.receive_deadline_seconds =
                properties.integer(property::RECEIVE_DEADLINE_SECONDS)?;
        }
        if properties.get(property::SEND_GRACE_MILLIS).is_some() {
            builder.send_grace_millis = properties.integer(property::SEND_GRACE_MILLIS)?;
        }

        builder.warmup_message_rate = properties
            .rate_or_default(property::WARMUP_MESSAGE_RATE, DEFAULT_WARMUP_MESSAGE_RATE)?;
        builder.message_rate = properties.rate(property::MESSAGE_RATE)?;
        builder.transceiver =
            Transceiver::parse(properties.required(property::MESSAGE_TRANSCEIVER)?)?;
        builder.output_file_name_prefix =
            Some(properties.required(property::OUTPUT_FILE)?.to_owned());

        builder.build()
    }

    /// How many one-second iterations of warmup to run first, whose results are
    /// thrown away.
    #[must_use]
    pub fn warmup_iterations(&self) -> u32 {
        self.warmup_iterations
    }

    /// How many one-second iterations the measurement runs for.
    #[must_use]
    pub fn iterations(&self) -> u32 {
        self.iterations
    }

    /// The target message rate during warmup.
    #[must_use]
    pub fn warmup_message_rate(&self) -> u32 {
        self.warmup_message_rate
    }

    /// The target message rate during the measurement.
    #[must_use]
    pub fn message_rate(&self) -> u32 {
        self.message_rate
    }

    /// How many messages go out at one timestamp.
    #[must_use]
    pub fn batch_size(&self) -> u32 {
        self.batch_size
    }

    /// The payload length in bytes, which does not count any header the
    /// transceiver adds.
    #[must_use]
    pub fn message_length(&self) -> u32 {
        self.message_length
    }

    /// Which system is under test.
    #[must_use]
    pub fn transceiver(&self) -> Transceiver {
        self.transceiver
    }

    /// What to do while waiting.
    #[must_use]
    pub fn idle_strategy(&self) -> IdleStrategy {
        self.idle_strategy
    }

    /// Where results and diagnostics are written.
    #[must_use]
    pub fn output_directory(&self) -> &Path {
        &self.output_directory
    }

    /// The `logs` subdirectory of [`Configuration::output_directory`], where a
    /// transceiver puts its own diagnostics.
    #[must_use]
    pub fn logs_directory(&self) -> &Path {
        &self.logs_directory
    }

    /// The name results are written under, with the rate, batch size and length
    /// already folded in (`Configuration.computeFileNamePrefix`, `:482-495`).
    #[must_use]
    pub fn output_file_name_prefix(&self) -> &str {
        &self.output_file_name_prefix
    }

    /// Whether to keep a per-second history as well as the final histogram.
    #[must_use]
    pub fn track_history(&self) -> bool {
        self.track_history
    }

    /// Whether to print the send rate as the run goes.
    #[must_use]
    pub fn report_progress(&self) -> bool {
        self.report_progress
    }

    /// The unit latencies are reported in.
    #[must_use]
    pub fn output_time_unit(&self) -> TimeUnit {
        self.output_time_unit
    }

    /// How long to keep draining replies after the last message is sent.
    #[must_use]
    pub fn receive_deadline_seconds(&self) -> u32 {
        self.receive_deadline_seconds
    }

    /// How far past the nominal end the send loop may run to flush what is
    /// outstanding.
    #[must_use]
    pub fn send_grace_millis(&self) -> u32 {
        self.send_grace_millis
    }

    /// The rate as the file name spells it: `1M`, `500K`, or the number
    /// (`Configuration.rateAsString`, `:469-481`).
    #[must_use]
    pub fn rate(&self) -> &str {
        &self.rate
    }
}

impl fmt::Display for Configuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Configuration{{\n    warmUpIterations={}\n    warmupMessageRate={}\n    iterations={}\n    \
             messageRate={}\n    batchSize={}\n    messageLength={}\n    messageTransceiver={}\n    \
             idleStrategy={}\n    trackHistory={}\n    reportProgress={}\n    outputTimeUnit={}\n    \
             receiveDeadlineSeconds={}\n    sendGraceMillis={}\n    outputDirectory={}\n    \
             outputFileNamePrefix={}\n}}",
            self.warmup_iterations,
            self.warmup_message_rate,
            self.iterations,
            self.rate,
            self.batch_size,
            self.message_length,
            self.transceiver,
            self.idle_strategy,
            self.track_history,
            self.report_progress,
            self.output_time_unit,
            self.receive_deadline_seconds,
            self.send_grace_millis,
            self.output_directory.display(),
            self.output_file_name_prefix,
        )
    }
}

/// Builds a [`Configuration`], filling in the reference's defaults.
///
/// The fields are public within the crate so that the property reader can set
/// them directly; everything outside goes through the methods, which return
/// `self` so a chain reads like the reference's builder.
#[derive(Clone, Debug)]
pub struct Builder {
    pub(crate) warmup_iterations: i32,
    pub(crate) iterations: i32,
    pub(crate) warmup_message_rate: i32,
    pub(crate) message_rate: i32,
    pub(crate) batch_size: i32,
    pub(crate) message_length: i32,
    pub(crate) transceiver: Transceiver,
    pub(crate) idle_strategy: IdleStrategy,
    pub(crate) output_directory: PathBuf,
    pub(crate) output_file_name_prefix: Option<String>,
    pub(crate) track_history: bool,
    pub(crate) report_progress: bool,
    pub(crate) output_time_unit: TimeUnit,
    pub(crate) receive_deadline_seconds: i32,
    pub(crate) send_grace_millis: i32,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    /// A builder holding the reference's defaults.
    #[must_use]
    pub fn new() -> Self {
        Self {
            warmup_iterations: i32::try_from(DEFAULT_WARMUP_ITERATIONS).unwrap_or(0),
            iterations: i32::try_from(DEFAULT_ITERATIONS).unwrap_or(0),
            warmup_message_rate: i32::try_from(DEFAULT_WARMUP_MESSAGE_RATE).unwrap_or(10_000),
            message_rate: 0,
            batch_size: i32::try_from(DEFAULT_BATCH_SIZE).unwrap_or(1),
            message_length: i32::try_from(MIN_MESSAGE_LENGTH).unwrap_or(16),
            transceiver: Transceiver::InMemory,
            idle_strategy: IdleStrategy::BusySpin,
            output_directory: PathBuf::from("results"),
            output_file_name_prefix: None,
            track_history: DEFAULT_TRACK_HISTORY,
            report_progress: DEFAULT_REPORT_PROGRESS,
            output_time_unit: TimeUnit::Microseconds,
            receive_deadline_seconds: i32::try_from(DEFAULT_RECEIVE_DEADLINE_SECONDS).unwrap_or(3),
            send_grace_millis: i32::try_from(DEFAULT_SEND_GRACE_MILLIS).unwrap_or(100),
        }
    }

    /// See [`Configuration::message_rate`].
    #[must_use]
    pub fn message_rate(mut self, rate: i32) -> Self {
        self.message_rate = rate;
        self
    }

    /// See [`Configuration::warmup_iterations`].
    #[must_use]
    pub fn warmup_iterations(mut self, iterations: i32) -> Self {
        self.warmup_iterations = iterations;
        self
    }

    /// See [`Configuration::iterations`].
    #[must_use]
    pub fn iterations(mut self, iterations: i32) -> Self {
        self.iterations = iterations;
        self
    }

    /// See [`Configuration::batch_size`].
    #[must_use]
    pub fn batch_size(mut self, size: i32) -> Self {
        self.batch_size = size;
        self
    }

    /// See [`Configuration::message_length`].
    #[must_use]
    pub fn message_length(mut self, length: i32) -> Self {
        self.message_length = length;
        self
    }

    /// See [`Configuration::output_directory`].
    #[must_use]
    pub fn output_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.output_directory = directory.into();
        self
    }

    /// See [`Configuration::output_file_name_prefix`]. The rate, batch size and
    /// length are appended to whatever is given here.
    #[must_use]
    pub fn output_file_name_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.output_file_name_prefix = Some(prefix.into());
        self
    }

    /// See [`Configuration::send_grace_millis`].
    #[must_use]
    pub fn send_grace_millis(mut self, millis: i32) -> Self {
        self.send_grace_millis = millis;
        self
    }

    /// See [`Configuration::receive_deadline_seconds`].
    #[must_use]
    pub fn receive_deadline_seconds(mut self, seconds: i32) -> Self {
        self.receive_deadline_seconds = seconds;
        self
    }

    /// See [`Configuration::output_time_unit`].
    #[must_use]
    pub fn output_time_unit(mut self, unit: TimeUnit) -> Self {
        self.output_time_unit = unit;
        self
    }

    /// See [`Configuration::track_history`].
    #[must_use]
    pub fn track_history(mut self, track: bool) -> Self {
        self.track_history = track;
        self
    }

    /// See [`Configuration::idle_strategy`].
    #[must_use]
    pub fn idle_strategy(mut self, strategy: IdleStrategy) -> Self {
        self.idle_strategy = strategy;
        self
    }

    /// Check everything, and produce the configuration.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for the first setting that breaks a rule, in the order
    /// `Configuration.java:248-271` checks them.
    pub fn build(self) -> Result<Configuration, ConfigError> {
        let warmup_iterations = check_range(
            self.warmup_iterations,
            0,
            i32::MAX,
            property::WARMUP_ITERATIONS,
        )?;
        let iterations = check_range(self.iterations, 1, i32::MAX, property::ITERATIONS)?;
        let warmup_message_rate = check_range(
            self.warmup_message_rate,
            0,
            i32::try_from(MAX_MESSAGE_RATE).unwrap_or(i32::MAX),
            property::WARMUP_MESSAGE_RATE,
        )?;
        let message_rate = check_range(
            self.message_rate,
            1,
            i32::try_from(MAX_MESSAGE_RATE).unwrap_or(i32::MAX),
            property::MESSAGE_RATE,
        )?;
        let batch_size = check_range(self.batch_size, 1, i32::MAX, property::BATCH_SIZE)?;
        let message_length = check_range(
            self.message_length,
            i32::try_from(MIN_MESSAGE_LENGTH).unwrap_or(16),
            i32::MAX,
            property::MESSAGE_LENGTH,
        )?;
        // The reference makes the output directory before it checks the last
        // two numbers (`Configuration.java:260-267`), so a run with a bad
        // directory and a bad deadline complains about the directory.
        let output_directory = validate_directory(&self.output_directory, "output")?;
        let logs_directory = validate_directory(&output_directory.join(LOGS_DIR), "log")?;

        let receive_deadline_seconds = check_range(
            self.receive_deadline_seconds,
            0,
            i32::MAX,
            property::RECEIVE_DEADLINE_SECONDS,
        )?;
        let send_grace_millis = check_range(
            self.send_grace_millis,
            0,
            i32::MAX,
            property::SEND_GRACE_MILLIS,
        )?;

        let prefix = self.output_file_name_prefix.unwrap_or_default();
        let prefix = prefix.trim();
        if prefix.is_empty() {
            return Err(ConfigError::EmptyOutputFileNamePrefix);
        }

        let rate = rate_as_string(message_rate);

        Ok(Configuration {
            warmup_iterations,
            iterations,
            warmup_message_rate,
            message_rate,
            batch_size,
            message_length,
            transceiver: self.transceiver,
            idle_strategy: self.idle_strategy,
            output_directory,
            logs_directory,
            output_file_name_prefix: format!(
                "{prefix}_rate={rate}_batch={batch_size}_length={message_length}"
            ),
            track_history: self.track_history,
            report_progress: self.report_progress,
            output_time_unit: self.output_time_unit,
            receive_deadline_seconds,
            send_grace_millis,
            rate,
        })
    }
}

/// `Configuration.rateAsString` (`:469-481`).
///
/// Whole millions first, then whole thousands, then the number — so 7_890_000
/// is `7890K` and not `7.89M`, because the reference only divides when the
/// division is exact.
fn rate_as_string(message_rate: u32) -> String {
    if message_rate % 1_000_000 == 0 {
        format!("{}M", message_rate / 1_000_000)
    } else if message_rate % 1000 == 0 {
        format!("{}K", message_rate / 1000)
    } else {
        message_rate.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A directory that goes away when the test does.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "deepmsg-bench-config-{}-{name}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// A builder that is valid except for what a test is about.
    fn builder(directory: &Path) -> Builder {
        Builder::new()
            .message_rate(123)
            .output_directory(directory)
            .output_file_name_prefix("test")
    }

    #[test]
    fn the_defaults_are_the_reference_s() {
        let scratch = Scratch::new("defaults");
        let configuration = builder(scratch.path()).build().expect("valid");

        assert_eq!(configuration.message_rate(), 123);
        assert_eq!(configuration.warmup_iterations(), DEFAULT_WARMUP_ITERATIONS);
        assert_eq!(
            configuration.warmup_message_rate(),
            DEFAULT_WARMUP_MESSAGE_RATE
        );
        assert_eq!(configuration.iterations(), DEFAULT_ITERATIONS);
        assert_eq!(configuration.batch_size(), DEFAULT_BATCH_SIZE);
        assert_eq!(configuration.message_length(), MIN_MESSAGE_LENGTH);
        assert_eq!(
            configuration.receive_deadline_seconds(),
            DEFAULT_RECEIVE_DEADLINE_SECONDS
        );
        assert_eq!(configuration.send_grace_millis(), DEFAULT_SEND_GRACE_MILLIS);
        assert_eq!(configuration.idle_strategy(), IdleStrategy::BusySpin);
        assert_eq!(configuration.output_time_unit(), TimeUnit::Microseconds);
        assert_eq!(configuration.transceiver(), Transceiver::InMemory);
        assert_eq!(
            configuration.output_directory(),
            absolute_path(scratch.path()).expect("readable").as_path()
        );
        assert_eq!(
            configuration.output_file_name_prefix(),
            "test_rate=123_batch=1_length=16"
        );
    }

    #[test]
    fn the_output_directory_gains_a_logs_subdirectory() {
        let scratch = Scratch::new("logs");
        let configuration = builder(scratch.path()).build().expect("valid");

        assert_eq!(configuration.logs_directory(), scratch.path().join("logs"));
        assert!(configuration.logs_directory().is_dir());
    }

    #[test]
    fn a_warmup_of_zero_iterations_is_allowed() {
        let scratch = Scratch::new("warmup-zero");
        let configuration = builder(scratch.path())
            .warmup_iterations(0)
            .build()
            .expect("valid");

        assert_eq!(configuration.warmup_iterations(), 0);
    }

    #[test]
    fn a_negative_warmup_is_refused() {
        let scratch = Scratch::new("warmup-negative");
        let error = builder(scratch.path())
            .warmup_iterations(-1)
            .build()
            .expect_err("refused");

        assert_eq!(
            error.to_string(),
            "'io.aeron.benchmarks.warmup.iterations' cannot be less than 0, got: -1"
        );
    }

    #[test]
    fn a_rate_below_one_is_refused() {
        let scratch = Scratch::new("rate-low");
        let error = builder(scratch.path())
            .message_rate(0)
            .build()
            .expect_err("refused");

        assert_eq!(
            error.to_string(),
            "'io.aeron.benchmarks.message.rate' cannot be less than 1, got: 0"
        );
    }

    #[test]
    fn a_rate_above_the_maximum_is_refused() {
        let scratch = Scratch::new("rate-high");
        let error = builder(scratch.path())
            .message_rate(1_000_000_001)
            .build()
            .expect_err("refused");

        assert_eq!(
            error.to_string(),
            "'io.aeron.benchmarks.message.rate' cannot be greater than 1000000000, got: 1000000001"
        );
    }

    #[test]
    fn a_zero_batch_size_is_refused() {
        let scratch = Scratch::new("batch-zero");
        let error = builder(scratch.path())
            .batch_size(0)
            .build()
            .expect_err("refused");

        assert_eq!(
            error.to_string(),
            "'io.aeron.benchmarks.batch.size' cannot be less than 1, got: 0"
        );
    }

    #[test]
    fn a_message_shorter_than_a_timestamp_and_a_checksum_is_refused() {
        let scratch = Scratch::new("length-short");
        let error = builder(scratch.path())
            .message_length(15)
            .build()
            .expect_err("refused");

        assert_eq!(
            error.to_string(),
            "'io.aeron.benchmarks.message.length' cannot be less than 16, got: 15"
        );
    }

    #[test]
    fn a_negative_receive_deadline_is_refused() {
        let scratch = Scratch::new("deadline-negative");
        let error = builder(scratch.path())
            .receive_deadline_seconds(-1)
            .build()
            .expect_err("refused");

        assert_eq!(
            error.to_string(),
            "'io.aeron.benchmarks.receive.deadline.seconds' cannot be less than 0, got: -1"
        );
    }

    #[test]
    fn an_empty_output_file_name_prefix_is_refused() {
        let scratch = Scratch::new("prefix-empty");
        let error = builder(scratch.path())
            .output_file_name_prefix("   ")
            .build()
            .expect_err("refused");

        assert_eq!(
            error.to_string(),
            "Output file name prefix cannot be empty!"
        );
    }

    #[test]
    fn a_missing_prefix_is_refused_too() {
        let scratch = Scratch::new("prefix-missing");
        let error = Builder::new()
            .message_rate(123)
            .output_directory(scratch.path())
            .build()
            .expect_err("refused");

        assert_eq!(
            error.to_string(),
            "Output file name prefix cannot be empty!"
        );
    }

    /// The rows of `ConfigurationTest.outputFileNamePrefixUsesHumanReadableRateValues`.
    #[test]
    fn the_rate_in_the_file_name_is_human_readable() {
        let cases = [
            (1_000_000_000, "1000M"),
            (5000, "5K"),
            (12345, "12345"),
            (7_890_000, "7890K"),
            (400_000, "400K"),
            (5430, "5430"),
            (100, "100"),
            (7600, "7600"),
            (10, "10"),
        ];

        for (rate, expected) in cases {
            let scratch = Scratch::new("rate-name");
            let configuration = builder(scratch.path())
                .message_rate(rate)
                .build()
                .expect("valid");

            assert_eq!(
                configuration.output_file_name_prefix(),
                format!("test_rate={expected}_batch=1_length=16"),
                "for a rate of {rate}"
            );
        }
    }

    #[test]
    fn the_prefix_and_the_rate_carry_the_batch_size_and_length() {
        let scratch = Scratch::new("prefix-parts");
        let configuration = builder(scratch.path())
            .message_rate(12)
            .batch_size(3)
            .message_length(75)
            .build()
            .expect("valid");

        assert_eq!(
            configuration.output_file_name_prefix(),
            "test_rate=12_batch=3_length=75"
        );
    }

    #[test]
    fn a_rate_with_a_k_suffix_is_thousands() {
        let mut properties = Properties::new();
        properties.set(property::MESSAGE_RATE, "42K");
        properties.set(property::MESSAGE_TRANSCEIVER, "in-memory");
        properties.set(property::OUTPUT_FILE, "test");

        let configuration = Configuration::from_properties(&properties).expect("valid");

        assert_eq!(configuration.message_rate(), 42_000);
    }

    #[test]
    fn a_rate_with_an_m_suffix_is_millions() {
        let mut properties = Properties::new();
        properties.set(property::MESSAGE_RATE, "20M");
        properties.set(property::MESSAGE_TRANSCEIVER, "in-memory");
        properties.set(property::OUTPUT_FILE, "test");

        let configuration = Configuration::from_properties(&properties).expect("valid");

        assert_eq!(configuration.message_rate(), 20_000_000);
    }

    #[test]
    fn a_rate_with_an_unknown_suffix_names_the_suffixes_it_wants() {
        let mut properties = Properties::new();
        properties.set(property::MESSAGE_RATE, "25i");
        properties.set(property::MESSAGE_TRANSCEIVER, "in-memory");
        properties.set(property::OUTPUT_FILE, "test");

        let error = Configuration::from_properties(&properties).expect_err("refused");

        assert_eq!(
            error.to_string(),
            "invalid rate specified in 'io.aeron.benchmarks.message.rate': \
             io.aeron.benchmarks.message.rate: 25i should end with: K or M."
        );
    }

    /// The rows of `ConfigurationTest.fromSystemPropertiesThrowsIllegalArgumentExceptionIfRateOverflows`.
    #[test]
    fn a_rate_that_would_overflow_is_refused() {
        for (name, value) in [
            (property::MESSAGE_RATE, "3000M"),
            (property::WARMUP_MESSAGE_RATE, "2456789K"),
        ] {
            let mut properties = Properties::new();
            properties.set(property::MESSAGE_RATE, "1");
            properties.set(property::MESSAGE_TRANSCEIVER, "in-memory");
            properties.set(property::OUTPUT_FILE, "test");
            properties.set(name, value);

            let error = Configuration::from_properties(&properties).expect_err("refused");

            assert_eq!(
                error.to_string(),
                format!(
                    "invalid rate specified in '{name}': {name} would overflow an int: {value}"
                )
            );
        }
    }

    #[test]
    fn a_non_integer_deadline_says_so_the_way_the_reference_does() {
        let mut properties = Properties::new();
        properties.set(property::MESSAGE_RATE, "1");
        properties.set(property::MESSAGE_TRANSCEIVER, "in-memory");
        properties.set(property::OUTPUT_FILE, "test");
        properties.set(property::RECEIVE_DEADLINE_SECONDS, "20x");

        let error = Configuration::from_properties(&properties).expect_err("refused");

        assert_eq!(
            error.to_string(),
            "non-integer value for property 'io.aeron.benchmarks.receive.deadline.seconds', \
             cause: error parsing int: 20x"
        );
    }

    #[test]
    fn a_missing_transceiver_is_required_by_name() {
        let mut properties = Properties::new();
        properties.set(property::MESSAGE_RATE, "100");

        let error = Configuration::from_properties(&properties).expect_err("refused");

        assert_eq!(
            error.to_string(),
            "property 'io.aeron.benchmarks.message.transceiver' is required!"
        );
    }

    #[test]
    fn an_unknown_transceiver_lists_the_ones_there_are() {
        let mut properties = Properties::new();
        properties.set(property::MESSAGE_RATE, "100");
        properties.set(property::MESSAGE_TRANSCEIVER, "echo-udp");
        properties.set(property::OUTPUT_FILE, "test");

        let error = Configuration::from_properties(&properties).expect_err("refused");

        assert_eq!(
            error.to_string(),
            "no message transceiver named 'echo-udp'; known: in-memory"
        );
    }

    #[test]
    fn a_missing_output_file_is_required_by_name() {
        let mut properties = Properties::new();
        properties.set(property::MESSAGE_RATE, "100");
        properties.set(property::MESSAGE_TRANSCEIVER, "in-memory");

        let error = Configuration::from_properties(&properties).expect_err("refused");

        assert_eq!(
            error.to_string(),
            "property 'io.aeron.benchmarks.output.file' is required!"
        );
    }

    #[test]
    fn the_time_unit_is_read_in_any_case() {
        let mut properties = Properties::new();
        properties.set(property::MESSAGE_RATE, "42K");
        properties.set(property::MESSAGE_TRANSCEIVER, "in-memory");
        properties.set(property::OUTPUT_FILE, "test");
        properties.set(property::OUTPUT_TIME_UNIT, "days");

        let configuration = Configuration::from_properties(&properties).expect("valid");

        assert_eq!(configuration.output_time_unit(), TimeUnit::Days);
        assert_eq!(
            configuration.output_time_unit().scale_ratio(),
            86_400_000_000_000.0
        );
    }

    #[test]
    fn a_property_set_to_nothing_is_a_property_that_was_not_set() {
        let mut properties = Properties::new();
        properties.set(property::MESSAGE_RATE, "100");
        properties.set(property::MESSAGE_TRANSCEIVER, "in-memory");
        properties.set(property::OUTPUT_FILE, "test");
        properties.set(property::WARMUP_ITERATIONS, "");

        let configuration = Configuration::from_properties(&properties).expect("valid");

        assert_eq!(configuration.warmup_iterations(), DEFAULT_WARMUP_ITERATIONS);
    }

    #[test]
    fn a_properties_file_is_read_with_comments_and_continuations() {
        let scratch = Scratch::new("props-file");
        fs::create_dir_all(scratch.path()).expect("scratch");
        let file = scratch.path().join("benchmark.properties");
        fs::write(
            &file,
            "# a comment\n! another\n\nio.aeron.benchmarks.message.rate=1M\nio.aeron.benchmarks.\\\n  \
             message.transceiver=in-memory\nio.aeron.benchmarks.output.file: test\n",
        )
        .expect("written");

        let mut properties = Properties::new();
        properties.load_file(&file).expect("readable");

        assert_eq!(properties.get(property::MESSAGE_RATE), Some("1M"));
        assert_eq!(
            properties.get(property::MESSAGE_TRANSCEIVER),
            Some("in-memory")
        );
        assert_eq!(properties.get(property::OUTPUT_FILE), Some("test"));

        let configuration = Configuration::from_properties(&properties).expect("valid");
        assert_eq!(configuration.message_rate(), 1_000_000);
    }

    /// What is already there wins, which is how a `-D` argument beats the
    /// properties file it was passed with (`PropertiesUtil.mergeWithProperties`
    /// with `PRESERVE`).
    #[test]
    fn merging_keeps_what_is_already_set() {
        let mut from_the_command_line = Properties::new();
        from_the_command_line.set(property::MESSAGE_RATE, "1M");

        let mut from_the_file = Properties::new();
        from_the_file.set(property::MESSAGE_RATE, "10");
        from_the_file.set(property::OUTPUT_FILE, "from-the-file");

        from_the_command_line.merge_keeping_existing(&from_the_file);

        assert_eq!(
            from_the_command_line.get(property::MESSAGE_RATE),
            Some("1M")
        );
        assert_eq!(
            from_the_command_line.get(property::OUTPUT_FILE),
            Some("from-the-file")
        );
    }

    #[test]
    fn the_display_names_every_setting() {
        let scratch = Scratch::new("display");
        let configuration = builder(scratch.path()).build().expect("valid");
        let text = configuration.to_string();

        for expected in [
            "warmUpIterations=10",
            "warmupMessageRate=10000",
            "iterations=10",
            "messageRate=123",
            "batchSize=1",
            "messageLength=16",
            "messageTransceiver=in-memory",
            "idleStrategy=busy-spin",
            "trackHistory=false",
            "reportProgress=false",
            "outputTimeUnit=MICROSECONDS",
            "receiveDeadlineSeconds=3",
            "sendGraceMillis=100",
            "outputFileNamePrefix=test_rate=123_batch=1_length=16",
        ] {
            assert!(
                text.contains(expected),
                "{expected} is missing from:\n{text}"
            );
        }
    }
}
