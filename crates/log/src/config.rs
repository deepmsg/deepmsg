//! Configuration, read once at startup.
//!
//! The application-side contract has four names, and only four:
//!
//! | name | default | meaning |
//! |---|---|---|
//! | `DEEPMSG_LOG_DIR` | unset = **off** | where the JSON files go |
//! | `DEEPMSG_LOG_LEVEL` | `info` | a level, or `component=level` overrides |
//! | `DEEPMSG_LOG_ROLL` | `100MB:5` | file size limit, and how many files |
//! | `DEEPMSG_NODE_ID` | required when enabled | written into every line |
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

use crate::event::Component;
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
    /// This process's component.
    pub component: Component,
    /// The global level.
    pub level: Level,
    /// Per-component overrides; later duplicates win.
    pub component_levels: Vec<(Component, Level)>,
    /// Size and count of files.
    pub roll: Roll,
    /// Records the queue holds before dropping.
    pub queue_capacity: usize,
    /// CPU to pin the writer thread to, if the binary supplies a pinner.
    pub cpu: Option<i32>,
    /// How to pin the writer thread. The crate has no `unsafe`, so the binary
    /// that owns a platform seam passes its own function here.
    pub pinner: Option<fn(i32) -> std::io::Result<()>>,
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
        let dir = env_value("DEEPMSG_LOG_DIR")?;
        // Off is off: an unrelated variable that happens to be malformed must
        // not turn a deliberately silent process into a failed one.
        let Some(dir) = dir.filter(|dir| !dir.trim().is_empty()) else {
            return Ok(None);
        };

        let node_id = env_value("DEEPMSG_NODE_ID")?;
        let level = env_value("DEEPMSG_LOG_LEVEL")?;
        let roll = env_value("DEEPMSG_LOG_ROLL")?;

        Self::from_values(
            component,
            Some(&dir),
            node_id.as_deref(),
            level.as_deref(),
            roll.as_deref(),
        )
    }

    /// The pure half of [`Config::from_env`].
    pub(crate) fn from_values(
        component: Component,
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

        let (level, component_levels) = parse_level_directives(level.unwrap_or(DEFAULT_LEVEL))?;
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

/// Parse `info`, or `driver=debug,cluster=info`, into a global level and overrides.
///
/// Grammar: comma-separated tokens; a token without `=` sets the global level,
/// a token with `=` sets one component's level. Later tokens win. An empty
/// token between commas is ignored; every other token must parse, so a typo is
/// an error rather than an override nobody notices is missing.
pub(crate) fn parse_level_directives(
    value: &str,
) -> Result<(Level, Vec<(Component, Level)>), InitError> {
    let mut global = Level::default();
    let mut overrides: Vec<(Component, Level)> = Vec::new();

    for token in value.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }

        if let Some((component, level)) = token.split_once('=') {
            let component_name = component.trim();
            let component =
                Component::parse(component_name).ok_or_else(|| InitError::UnknownComponent {
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
    /// A roll policy that does not parse.
    InvalidRoll {
        /// The offending text.
        value: String,
        /// What was wrong with it.
        reason: &'static str,
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
            InitError::InvalidRoll { value, reason } => {
                write!(
                    formatter,
                    "invalid DEEPMSG_LOG_ROLL value {value}: {reason}"
                )
            }
        }
    }
}

impl std::error::Error for InitError {}

#[cfg(test)]
mod tests {
    use super::{Config, DEFAULT_QUEUE_CAPACITY, InitError, Roll, parse_level_directives};
    use crate::event::Component;
    use crate::level::Level;

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
        let (global, overrides) = parse_level_directives("debug").unwrap();
        assert_eq!(Level::Debug, global);
        assert!(overrides.is_empty());
    }

    #[test]
    fn component_directives_override_without_moving_the_global() {
        let (global, overrides) =
            parse_level_directives("info,driver=debug,cluster=error").unwrap();
        assert_eq!(Level::Info, global);
        assert_eq!(
            vec![
                (Component::Driver, Level::Debug),
                (Component::Cluster, Level::Error)
            ],
            overrides
        );
    }

    #[test]
    fn later_directives_win_and_empty_tokens_are_ignored() {
        let (_, overrides) = parse_level_directives("driver=debug,,driver=error,").unwrap();
        assert_eq!(vec![(Component::Driver, Level::Error)], overrides);
        let (global, _) = parse_level_directives("warn,,info").unwrap();
        assert_eq!(Level::Info, global);
    }

    #[test]
    fn a_bad_directive_names_what_it_did_not_understand() {
        assert_eq!(
            Err(InitError::UnknownComponent {
                value: "mediadriver".to_owned()
            }),
            parse_level_directives("mediadriver=debug")
        );
        assert_eq!(
            Err(InitError::InvalidLevel {
                value: "verbose".to_owned()
            }),
            parse_level_directives("driver=verbose")
        );
        assert_eq!(
            Err(InitError::InvalidLevel {
                value: "verbose".to_owned()
            }),
            parse_level_directives("verbose")
        );
    }

    #[test]
    fn an_enabled_configuration_needs_a_node_id() {
        assert_eq!(
            Err(InitError::MissingNodeId),
            Config::from_values(Component::Driver, Some("/tmp/logs"), None, None, None)
                .map(|config| config.map(|_| ()))
        );
        assert_eq!(
            Err(InitError::EmptyNodeId),
            Config::from_values(Component::Driver, Some("/tmp/logs"), Some("  "), None, None)
                .map(|config| config.map(|_| ()))
        );
    }

    #[test]
    fn an_enabled_configuration_carries_the_defaults() {
        let config = Config::from_values(
            Component::Cluster,
            Some("/tmp/logs"),
            Some("node-7"),
            Some("driver=debug"),
            Some("10MB:2"),
        )
        .unwrap()
        .unwrap();

        assert_eq!(Component::Cluster, config.component);
        assert_eq!(Level::Info, config.level);
        assert_eq!(
            vec![(Component::Driver, Level::Debug)],
            config.component_levels
        );
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
}
