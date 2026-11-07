//! Levels, and the two ceilings an event has to pass.
//!
//! The order is severity: `Off < Error < Warn < Info < Debug < Trace`. `Off` is
//! a level rather than "no filter" because `DEEPMSG_LOG_LEVEL=off` is how an
//! operator silences one process without unsetting its directory.
//!
//! Two ceilings apply, and both are checked before an event is built:
//!
//! - the **configured** level (global, overridden per [`Component`]), read once
//!   at startup;
//! - [`COMPILED_MAX`], which a release build lowers to `Info` so `Debug` and
//!   `Trace` disappear at compile time. This is the same trade tracing makes
//!   with `release_max_level_info`, and it is the reason a release build cannot
//!   be made to emit debug lines by changing an environment variable.
//!
//! [`Component`]: crate::Component

/// How loud an event is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    /// Emit nothing.
    Off,
    /// A failed invariant or a lost resource.
    Error,
    /// Something the operator will have to look at.
    Warn,
    /// A lifecycle event on the happy path.
    #[default]
    Info,
    /// Diagnostic detail.
    Debug,
    /// Per-cycle detail; nearly always off.
    Trace,
}

impl Level {
    /// The lowercase name written to the log and accepted from the environment.
    pub const fn as_str(self) -> &'static str {
        match self {
            Level::Off => "off",
            Level::Error => "error",
            Level::Warn => "warn",
            Level::Info => "info",
            Level::Debug => "debug",
            Level::Trace => "trace",
        }
    }

    /// Parse a level name, case-insensitively.
    ///
    /// Returns `None` for anything else; the caller turns that into an
    /// `InitError` so a typo is visible at startup rather than silently
    /// choosing a default.
    pub fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        if value.eq_ignore_ascii_case("off") {
            Some(Level::Off)
        } else if value.eq_ignore_ascii_case("error") {
            Some(Level::Error)
        } else if value.eq_ignore_ascii_case("warn") {
            Some(Level::Warn)
        } else if value.eq_ignore_ascii_case("info") {
            Some(Level::Info)
        } else if value.eq_ignore_ascii_case("debug") {
            Some(Level::Debug)
        } else if value.eq_ignore_ascii_case("trace") {
            Some(Level::Trace)
        } else {
            None
        }
    }

    /// Whether an event at this level survives a ceiling.
    ///
    /// `Off` never does: it is a value the environment can name, not an event
    /// anyone emits.
    pub fn is_enabled(self, ceiling: Level) -> bool {
        !matches!(self, Level::Off) && self <= ceiling
    }
}

/// The highest level this build can emit.
///
/// A release build cuts `Debug` and `Trace` at compile time. The
/// `debug-in-release` feature puts them back for a diagnostic build; it is
/// deliberately a build-time decision, because an operator changing an
/// environment variable should not be able to turn a busy-spin driver into a
/// debug-logging one.
#[cfg(all(not(debug_assertions), not(feature = "debug-in-release")))]
pub const COMPILED_MAX: Level = Level::Info;

/// See [`COMPILED_MAX`]: debug and test builds keep everything.
#[cfg(any(debug_assertions, feature = "debug-in-release"))]
pub const COMPILED_MAX: Level = Level::Trace;

#[cfg(test)]
mod tests {
    use super::Level;

    #[test]
    fn every_name_round_trips() {
        for level in [
            Level::Off,
            Level::Error,
            Level::Warn,
            Level::Info,
            Level::Debug,
            Level::Trace,
        ] {
            assert_eq!(Some(level), Level::parse(level.as_str()));
        }
    }

    #[test]
    fn parsing_is_case_insensitive_and_trims() {
        assert_eq!(Some(Level::Warn), Level::parse("  WARN "));
        assert_eq!(Some(Level::Debug), Level::parse("Debug"));
    }

    #[test]
    fn an_unknown_name_is_not_a_default() {
        assert_eq!(None, Level::parse("verbose"));
        assert_eq!(None, Level::parse(""));
    }

    #[test]
    fn a_ceiling_keeps_the_levels_at_or_below_it() {
        assert!(Level::Error.is_enabled(Level::Info));
        assert!(Level::Info.is_enabled(Level::Info));
        assert!(!Level::Debug.is_enabled(Level::Info));
        assert!(Level::Trace.is_enabled(Level::Trace));
        assert!(!Level::Off.is_enabled(Level::Trace));
    }
}
