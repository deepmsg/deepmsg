//! Configuration, read once at startup.
//!
//! The application-side contract has five names, and only five:
//!
//! | name | default | meaning |
//! |---|---|---|
//! | `DEEPMSG_LOG_DIR` | unset = **off** | where the JSON files go |
//! | `DEEPMSG_LOG_LEVEL` | `info` | a level, or `component=level` overrides |
//! | `DEEPMSG_LOG_ROLL` | `100MB:5` | file size limit, and how many files |
//! | `DEEPMSG_NODE_ID` | required when enabled | written into every line |
//! | `DEEPMSG_LOG_CPU` | unset = unpinned | CPU for the writer thread |
//!
//! "Unset directory = off" is the important one. Tests, benchmarks and one-off
//! runs must not create a thread or a directory as a side effect, and the
//! driver's stderr contract means a failed startup must not print anything
//! either. Production turns logging on explicitly in its supervisor unit.
//!
//! Parsing is kept pure (`from_values`) so tests never mutate process-global
//! environment variables; `from_env` is the thin reader over it.

use std::fmt;
use std::path::PathBuf;

use crate::event::{Component, Name};
use crate::level::Level;

/// The level used when `DEEPMSG_LOG_LEVEL` is unset.
pub const DEFAULT_LEVEL: &str = "info";
/// The roll policy used when `DEEPMSG_LOG_ROLL` is unset.
pub const DEFAULT_ROLL: &str = "100MB:5";
/// Records the queue holds before producers start dropping (and counting).
///
/// 8192 records of at most 512 bytes is about 4 MiB per process. It is a
/// memory bound, not a throughput target; a process that cannot drain 8192
/// records is already in trouble and should report the drop rather than grow.
pub const DEFAULT_QUEUE_CAPACITY: usize = 8192;

/// How large one file may get, and how many files exist including the current.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Roll {
    /// Rotate once the current file reaches this many bytes.
    pub bytes: u64,
    /// Total files kept, current file included. Never zero.
    pub files: usize,
}

impl Default for Roll {
    fn default() -> Self {
        Roll {
            bytes: 100 * 1024 * 1024,
            files: 5,
        }
    }
}

impl Roll {
    /// Parse `<size>:<count>`, e.g. `100MB:5`.
    ///
    /// Sizes use binary multiples (`K`/`KB` = 1024, and so on) because the
    /// limit is a disk bound, not a marketing number. Spaces around either half
    /// are ignored; a missing suffix means bytes.
    pub fn parse(value: &str) -> Result<Self, InitError> {
        let (size, count) = value
            .split_once(':')
            .ok_or_else(|| InitError::InvalidRoll {
                value: value.to_owned(),
                reason: "expected <size>:<count>, e.g. 100MB:5",
            })?;

        let bytes = parse_size(size).map_err(|reason| InitError::InvalidRoll {
            value: value.to_owned(),
            reason,
        })?;
        let files = count
            .trim()
            .parse::<usize>()
            .map_err(|_| InitError::InvalidRoll {
                value: value.to_owned(),
                reason: "the file count is not a positive integer",
            })?;

        if files == 0 {
            return Err(InitError::InvalidRoll {
                value: value.to_owned(),
                reason: "the file count must be at least 1",
            });
        }
        if bytes == 0 {
            return Err(InitError::InvalidRoll {
                value: value.to_owned(),
                reason: "the size must be at least 1 byte",
            });
        }

        Ok(Roll { bytes, files })
    }
}

fn parse_size(value: &str) -> Result<u64, &'static str> {
    let value = value.trim();
    let digits_end = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    if digits_end == 0 {
        return Err("the size is empty or does not start with a number");
    }

    let number = value[..digits_end]
        .parse::<u64>()
        .map_err(|_| "the size is not a number that fits in 64 bits")?;
    let suffix = value[digits_end..].trim();
    let multiplier = if suffix.is_empty() {
        1
    } else if suffix.eq_ignore_ascii_case("k") || suffix.eq_ignore_ascii_case("kb") {
        1024
    } else if suffix.eq_ignore_ascii_case("m") || suffix.eq_ignore_ascii_case("mb") {
        1024 * 1024
    } else if suffix.eq_ignore_ascii_case("g") || suffix.eq_ignore_ascii_case("gb") {
        1024 * 1024 * 1024
    } else {
        return Err("the suffix is not one of K, KB, M, MB, G, GB");
    };

    number
        .checked_mul(multiplier)
        .ok_or("the size does not fit in 64 bits")
}

/// How a binary pins the writer thread to a CPU.
///
/// The crate has no `unsafe`, so it cannot call `sched_setaffinity` itself; the
/// binary that owns a platform seam supplies the function. This is what keeps
/// `deepmsg-log` out of ADR-0002's unsafe zones.
pub type Pinner = fn(i32) -> std::io::Result<()>;

/// The configuration a process logs with.
///
/// Built once, before any thread starts. `component` is the process's default
/// and the filename's component; an event may name a different one (a cluster
/// node hosts clients too).
pub struct Config {
    /// Directory for the JSON files.
    pub dir: PathBuf,
    /// Written into every line and used as the `node_id` stream field.
    pub node_id: String,
    /// This process's component name. It is the filename's prefix and the
    /// default `component` field; an event may name a different one.
    pub component: &'static str,
    /// The global level.
    pub level: Level,
    /// Per-component overrides; later duplicates win.
    pub component_levels: Vec<(&'static str, Level)>,
    /// Size and count of files.
    pub roll: Roll,
    /// Records the queue holds before dropping.
    pub queue_capacity: usize,
    /// CPU to pin the writer thread to, if the binary supplies a pinner.
    pub cpu: Option<i32>,
    /// How to pin the writer thread. The crate has no `unsafe`, so the binary
    /// that owns a platform seam passes its own function here.
    pub pinner: Option<Pinner>,
    /// The clock used for `ts`; a function pointer so tests can freeze it.
    pub clock: fn() -> i64,
}

impl Config {
    /// Read the four names from the environment.
    ///
    /// `Ok(None)` is a complete answer: logging is off. A non-UTF-8 value is an
    /// error rather than a silently disabled logger, because the operator
    /// asked for something this process cannot honour.
    pub fn from_env(component: Component) -> Result<Option<Self>, InitError> {
        Self::from_env_with(component, &Component::ALL_NAMES)
    }

    /// Read the environment with an application-supplied vocabulary.
    ///
    /// `component` is this process's default name; `known_components` is the
    /// list a `DEEPMSG_LOG_LEVEL=name=level` token may use. Pass
    /// `&Component::ALL_NAMES` to accept only the built-in names; an
    /// application adds its own names (and their case-insensitive spellings
    /// are matched against this list, never guessed).
    pub fn from_env_with(
        component: impl Name,
        known_components: &'static [&'static str],
    ) -> Result<Option<Self>, InitError> {
        let dir = env_value("DEEPMSG_LOG_DIR")?;
        // Off is off: an unrelated variable that happens to be malformed must
        // not turn a deliberately silent process into a failed one.
        let Some(dir) = dir.filter(|dir| !dir.trim().is_empty()) else {
            return Ok(None);
        };

        let node_id = env_value("DEEPMSG_NODE_ID")?;
        let level = env_value("DEEPMSG_LOG_LEVEL")?;
        let roll = env_value("DEEPMSG_LOG_ROLL")?;
        let cpu = env_value("DEEPMSG_LOG_CPU")?;

        let Some(mut config) = Self::from_values(
            component.as_str(),
            known_components,
            Some(&dir),
            node_id.as_deref(),
            level.as_deref(),
            roll.as_deref(),
        )?
        else {
            return Ok(None);
        };
        config.cpu = parse_cpu(cpu.as_deref())?;
        Ok(Some(config))
    }

    /// The pure half of [`Config::from_env`].
    pub(crate) fn from_values(
        component: &'static str,
        known_components: &'static [&'static str],
        dir: Option<&str>,
        node_id: Option<&str>,
        level: Option<&str>,
        roll: Option<&str>,
    ) -> Result<Option<Self>, InitError> {
        let Some(dir) = dir.filter(|dir| !dir.trim().is_empty()) else {
            return Ok(None);
        };

        let node_id = node_id.ok_or(InitError::MissingNodeId)?;
        if node_id.trim().is_empty() {
            return Err(InitError::EmptyNodeId);
        }

        let (level, component_levels) =
            parse_level_directives(level.unwrap_or(DEFAULT_LEVEL), known_components)?;
        let roll = Roll::parse(roll.unwrap_or(DEFAULT_ROLL))?;

        Ok(Some(Self {
            dir: PathBuf::from(dir),
            node_id: node_id.to_owned(),
            component,
            level,
            component_levels,
            roll,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            cpu: None,
            pinner: None,
            clock: deepmsg_core::clock::epoch_nano_time,
        }))
    }
}

fn env_value(name: &'static str) -> Result<Option<String>, InitError> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(InitError::NotUnicode { name }),
    }
}

/// `DEEPMSG_LOG_CPU`: a non-negative CPU id, or absent for no pinning.
pub(crate) fn parse_cpu(value: Option<&str>) -> Result<Option<i32>, InitError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    let cpu = value.parse::<i32>().map_err(|_| InitError::InvalidCpu {
        value: value.to_owned(),
    })?;
    if cpu < 0 {
        return Err(InitError::InvalidCpu {
            value: value.to_owned(),
        });
    }
    Ok(Some(cpu))
}

/// A parsed `DEEPMSG_LOG_LEVEL`: the global level and the per-component
/// overrides, in the order they were first named.
pub(crate) type LevelDirectives = (Level, Vec<(&'static str, Level)>);

/// Parse `info`, or `driver=debug,cluster=info`, into a global level and overrides.
///
/// Grammar: comma-separated tokens; a token without `=` sets the global level,
/// a token with `=` sets one component's level. Later tokens win. An empty
/// token between commas is ignored; every other token must parse, so a typo is
/// an error rather than an override nobody notices is missing. A component
/// name must be in `known_components`; the built-ins pass
/// `Component::ALL_NAMES`, and an application passes its own list.
pub(crate) fn parse_level_directives(
    value: &str,
    known_components: &'static [&'static str],
) -> Result<LevelDirectives, InitError> {
    let mut global = Level::default();
    let mut overrides: Vec<(&'static str, Level)> = Vec::new();

    for token in value.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }

        if let Some((component, level)) = token.split_once('=') {
            let component_name = component.trim();
            let component = known_components
                .iter()
                .copied()
                .find(|known| known.eq_ignore_ascii_case(component_name))
                .ok_or_else(|| InitError::UnknownComponent {
                    value: component_name.to_owned(),
                })?;
            let level_name = level.trim();
            let level = Level::parse(level_name).ok_or_else(|| InitError::InvalidLevel {
                value: level_name.to_owned(),
            })?;

            if let Some(existing) = overrides
                .iter_mut()
                .find(|(candidate, _)| *candidate == component)
            {
                existing.1 = level;
            } else {
                overrides.push((component, level));
            }
        } else {
            global = Level::parse(token).ok_or_else(|| InitError::InvalidLevel {
                value: token.to_owned(),
            })?;
        }
    }

    Ok((global, overrides))
}

/// Why logging could not be configured.
///
/// Every variant is a startup error: the process has been asked to log in a way
/// it cannot. The binary decides how to report it; this crate never writes to
/// stdout or stderr.
#[derive(Debug, PartialEq, Eq)]
pub enum InitError {
    /// `DEEPMSG_LOG_DIR` is set but `DEEPMSG_NODE_ID` is not.
    MissingNodeId,
    /// `DEEPMSG_NODE_ID` is present but empty.
    EmptyNodeId,
    /// An environment variable is not valid UTF-8.
    NotUnicode {
        /// Which variable.
        name: &'static str,
    },
    /// A level name this build does not know.
    InvalidLevel {
        /// The offending text.
        value: String,
    },
    /// A component name this build does not know.
    UnknownComponent {
        /// The offending text.
        value: String,
    },
    /// `DEEPMSG_LOG_CPU` is not a CPU id.
    InvalidCpu {
        /// The offending text.
        value: String,
    },
    /// A roll policy that does not parse.
    InvalidRoll {
        /// The offending text.
        value: String,
        /// What was wrong with it.
        reason: &'static str,
    },
    /// `init_from_env` was called twice in one process.
    AlreadyInstalled,
    /// A CPU was configured without a function that can pin to it.
    MissingPinner,
    /// Creating the directory, opening the file, or starting the writer failed.
    Io {
        /// What was being attempted.
        operation: &'static str,
        /// The path involved.
        path: PathBuf,
        /// The platform's message.
        message: String,
    },
}

impl fmt::Display for InitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InitError::MissingNodeId => {
                formatter.write_str("DEEPMSG_LOG_DIR is set but DEEPMSG_NODE_ID is not")
            }
            InitError::EmptyNodeId => formatter.write_str("DEEPMSG_NODE_ID is empty"),
            InitError::NotUnicode { name } => {
                write!(formatter, "environment variable {name} is not valid UTF-8")
            }
            InitError::InvalidLevel { value } => {
                write!(formatter, "unknown log level: {value}")
            }
            InitError::UnknownComponent { value } => {
                write!(formatter, "unknown log component: {value}")
            }
            InitError::InvalidCpu { value } => {
                write!(formatter, "DEEPMSG_LOG_CPU is not a CPU id: {value}")
            }
            InitError::InvalidRoll { value, reason } => {
                write!(
                    formatter,
                    "invalid DEEPMSG_LOG_ROLL value {value}: {reason}"
                )
            }
            InitError::AlreadyInstalled => {
                formatter.write_str("a logger is already installed in this process")
            }
            InitError::MissingPinner => formatter
                .write_str("a writer CPU was configured but no pinner was supplied to set it"),
            InitError::Io {
                operation,
                path,
                message,
            } => write!(
                formatter,
                "could not {operation} at {}: {message}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for InitError {}

#[cfg(test)]
mod tests {
    use super::{
        Config, DEFAULT_QUEUE_CAPACITY, InitError, Roll, parse_cpu, parse_level_directives,
    };
    use crate::event::{Component, Name};
    use crate::level::Level;

    /// The plain form every existing test and caller uses: the built-in names.
    fn levels(value: &str) -> Result<super::LevelDirectives, InitError> {
        parse_level_directives(value, &Component::ALL_NAMES)
    }

    #[test]
    fn the_default_roll_is_100_mib_over_5_files() {
        assert_eq!(
            Roll {
                bytes: 100 * 1024 * 1024,
                files: 5
            },
            Roll::default()
        );
    }

    #[test]
    fn a_roll_parses_sizes_and_counts() {
        assert_eq!(
            Roll {
                bytes: 100 * 1024 * 1024,
                files: 5
            },
            Roll::parse("100MB:5").unwrap()
        );
        assert_eq!(
            Roll {
                bytes: 2048,
                files: 1
            },
            Roll::parse(" 2KB : 1 ").unwrap()
        );
        assert_eq!(Roll { bytes: 7, files: 2 }, Roll::parse("7:2").unwrap());
        assert_eq!(
            Roll {
                bytes: 3 * 1024 * 1024 * 1024,
                files: 4
            },
            Roll::parse("3g:4").unwrap()
        );
    }

    #[test]
    fn a_bad_roll_is_an_error_not_a_default() {
        for value in [
            "", "100MB", "100MB:", ":5", "MB:5", "100XB:5", "100MB:0", "0:5",
        ] {
            assert!(
                matches!(Roll::parse(value), Err(InitError::InvalidRoll { .. })),
                "{value} was accepted"
            );
        }
        assert!(matches!(
            Roll::parse("99999999999999999999:2"),
            Err(InitError::InvalidRoll { .. })
        ));
    }

    #[test]
    fn a_bare_level_sets_the_global_level() {
        let (global, overrides) = levels("debug").unwrap();
        assert_eq!(Level::Debug, global);
        assert!(overrides.is_empty());
    }

    #[test]
    fn component_directives_override_without_moving_the_global() {
        let (global, overrides) = levels("info,driver=debug,cluster=error").unwrap();
        assert_eq!(Level::Info, global);
        assert_eq!(
            vec![("driver", Level::Debug), ("cluster", Level::Error)],
            overrides
        );
    }

    #[test]
    fn later_directives_win_and_empty_tokens_are_ignored() {
        let (_, overrides) = levels("driver=debug,,driver=error,").unwrap();
        assert_eq!(vec![("driver", Level::Error)], overrides);
        let (global, _) = levels("warn,,info").unwrap();
        assert_eq!(Level::Info, global);
    }

    #[test]
    fn a_bad_directive_names_what_it_did_not_understand() {
        assert_eq!(
            Err(InitError::UnknownComponent {
                value: "mediadriver".to_owned()
            }),
            levels("mediadriver=debug")
        );
        assert_eq!(
            Err(InitError::InvalidLevel {
                value: "verbose".to_owned()
            }),
            levels("driver=verbose")
        );
        assert_eq!(
            Err(InitError::InvalidLevel {
                value: "verbose".to_owned()
            }),
            levels("verbose")
        );
    }

    #[test]
    fn an_enabled_configuration_needs_a_node_id() {
        assert_eq!(
            Err(InitError::MissingNodeId),
            Config::from_values(
                Component::Driver.as_str(),
                &Component::ALL_NAMES,
                Some("/tmp/logs"),
                None,
                None,
                None,
            )
            .map(|config| config.map(|_| ()))
        );
        assert_eq!(
            Err(InitError::EmptyNodeId),
            Config::from_values(
                Component::Driver.as_str(),
                &Component::ALL_NAMES,
                Some("/tmp/logs"),
                Some("  "),
                None,
                None,
            )
            .map(|config| config.map(|_| ()))
        );
    }

    #[test]
    fn an_enabled_configuration_carries_the_defaults() {
        let config = Config::from_values(
            Component::Cluster.as_str(),
            &Component::ALL_NAMES,
            Some("/tmp/logs"),
            Some("node-7"),
            Some("driver=debug"),
            Some("10MB:2"),
        )
        .unwrap()
        .unwrap();

        assert_eq!("cluster", config.component);
        assert_eq!(Level::Info, config.level);
        assert_eq!(vec![("driver", Level::Debug)], config.component_levels);
        assert_eq!(
            Roll {
                bytes: 10 * 1024 * 1024,
                files: 2
            },
            config.roll
        );
        assert_eq!(DEFAULT_QUEUE_CAPACITY, config.queue_capacity);
        assert_eq!(None, config.cpu);
        assert!(config.pinner.is_none());
        assert_eq!("node-7", config.node_id);
    }

    #[test]
    fn an_application_vocabulary_is_the_list_it_passes() {
        struct Engine;

        impl Name for Engine {
            fn as_str(&self) -> &'static str {
                "engine"
            }
        }

        static ENGINE_NAMES: [&str; 2] = ["engine", "gateway"];

        let config = Config::from_values(
            Engine.as_str(),
            &ENGINE_NAMES,
            Some("/tmp/logs"),
            Some("node-7"),
            Some("engine=debug"),
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!("engine", config.component);
        assert_eq!(vec![("engine", Level::Debug)], config.component_levels);

        // The list the application passed decides, not a guess: a name outside
        // it is still an error, so a typo in the unit is not silently ignored.
        assert_eq!(
            Err(InitError::UnknownComponent {
                value: "marketdata".to_owned()
            }),
            Config::from_values(
                Engine.as_str(),
                &ENGINE_NAMES,
                Some("/tmp/logs"),
                Some("node-7"),
                Some("marketdata=debug"),
                None,
            )
            .map(|config| config.map(|_| ()))
        );
    }

    #[test]
    fn a_cpu_is_optional_and_must_be_a_non_negative_id() {
        assert_eq!(Ok(None), parse_cpu(None));
        assert_eq!(Ok(Some(7)), parse_cpu(Some("7")));
        assert_eq!(Ok(Some(0)), parse_cpu(Some(" 0 ")));

        for value in ["-1", "cpu0", "", "9999999999999"] {
            assert!(
                matches!(parse_cpu(Some(value)), Err(InitError::InvalidCpu { .. })),
                "{value} was accepted"
            );
        }
    }
}
