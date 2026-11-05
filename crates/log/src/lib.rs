//! deepmsg's application-side log facade.
//!
//! The application writes **local files**. It never talks to a collector, never
//! writes to stdout or stderr, and never learns whether anything is reading the
//! files. That contract is what lets a deployment turn collection off without
//! touching the data plane, and it is also a hard requirement of the driver:
//! the reference's own system tests assert that a driver's stderr is **zero
//! bytes** on the happy path
//! (`deepmsg-driver`'s module documentation, `aeron-test-support/src/main/java/io/aeron/test/MediaDriverTestUtil.java:128-132`).
//!
//! The crate is deliberately small and dependency-free. Its design is
//! `analysis/observability/deepmsg-l0a-plan-log-crate.md`; the pipeline it feeds
//! is `analysis/observability/plan-2026-11-05-observability-pipeline.md`.
//!
//! Three decisions shape the API:
//!
//! - **Off by default.** `Config::from_env` answers `None` when
//!   `DEEPMSG_LOG_DIR` is unset. A test, a benchmark, or a one-off run must not
//!   create files or a thread as a side effect.
//! - **A controlled vocabulary.** [`Event`] and [`Component`] are enums, not
//!   free strings. A new event is a code change with a test, so queries and
//!   alerts do not drift with a version.
//! - **No allocation on the calling thread.** Levels, components and the
//!   environment are parsed once at startup; an event is encoded into a
//!   fixed-size record (ADR-0003: "hot paths stay allocation-free").
//!
//! This first slice contains the vocabulary, the levels and the configuration
//! parsing. The record encoder, the queue, the writer thread and rolling arrive
//! in the following slices (§6 of the design); nothing here writes a byte yet.

#![forbid(unsafe_code)]

mod config;
mod event;
mod level;
pub mod record;
mod time;

pub use config::{Config, DEFAULT_LEVEL, DEFAULT_QUEUE_CAPACITY, DEFAULT_ROLL, InitError, Roll};
pub use event::{Component, Event};
pub use level::{COMPILED_MAX, Level};

#[cfg(test)]
mod tests {
    use super::{COMPILED_MAX, Component, Config, Event, Level, Roll};

    #[test]
    fn the_controlled_vocabulary_has_no_duplicates() {
        let mut names: Vec<&str> = Event::ALL.iter().map(|event| event.as_str()).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "an event name is used twice");

        let mut components: Vec<&str> = Component::ALL
            .iter()
            .map(|component| component.as_str())
            .collect();
        components.sort_unstable();
        let before = components.len();
        components.dedup();
        assert_eq!(before, components.len(), "a component name is used twice");
    }

    #[test]
    fn levels_are_ordered_by_severity() {
        assert!(Level::Off < Level::Error);
        assert!(Level::Error < Level::Warn);
        assert!(Level::Warn < Level::Info);
        assert!(Level::Info < Level::Debug);
        assert!(Level::Debug < Level::Trace);
    }

    #[test]
    fn off_is_never_enabled_and_the_compiled_max_is_a_ceiling() {
        assert!(!Level::Off.is_enabled(Level::Trace));
        assert!(Level::Error.is_enabled(Level::Info));
        assert!(!Level::Debug.is_enabled(Level::Info));
        assert!(Level::Trace.is_enabled(Level::Trace));

        // In a test build the compiled ceiling is `Trace`; a release build
        // lowers it to `Info` unless `debug-in-release` is on.
        assert_eq!(
            COMPILED_MAX,
            if cfg!(debug_assertions) {
                Level::Trace
            } else {
                Level::Info
            }
        );
    }

    #[test]
    fn a_disabled_configuration_does_not_ask_for_a_node_id() {
        let config = Config::from_values(Component::Driver, None, None, None, None)
            .expect("a missing directory is not an error");
        assert!(config.is_none());
    }

    #[test]
    fn an_empty_directory_is_off_too() {
        let config = Config::from_values(Component::Driver, Some("  "), None, None, None)
            .expect("an empty directory is off");
        assert!(config.is_none());
    }

    #[test]
    fn an_enabled_configuration_defaults_to_info_and_100mb_by_5() {
        let config = Config::from_values(
            Component::Driver,
            Some("/var/log/deepmsg"),
            Some("node-1"),
            None,
            None,
        )
        .expect("valid configuration")
        .expect("enabled");

        assert_eq!(config.level, Level::Info);
        assert!(config.component_levels.is_empty());
        assert_eq!(Roll::default(), config.roll);
        assert_eq!("/var/log/deepmsg", config.dir.to_str().unwrap());
        assert_eq!("node-1", config.node_id);
    }
}
