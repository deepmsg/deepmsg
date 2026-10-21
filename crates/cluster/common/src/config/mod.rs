//! `aeron.cluster.*` properties: how they are read, and how their values are
//! parsed.
//!
//! Two things live here:
//!
//! - [`duration_ns`] and [`size`], which follow Agrona 2.6.1's parsers bit for
//!   bit. The accepted suffixes were read off the jar, not off its bytecode —
//!   see `tests/fixtures/agrona-parse.tsv` and `tests/agrona_parse.rs`. Two
//!   traps that fixture pins down:
//!   * **`m` is mebibytes, never minutes.** `3m` is `3145728` for a size and a
//!     *parse error* for a duration. Units for duration are `ns`, `us`, `ms`,
//!     `s` only — `h` and `d` do not exist.
//!   * **A size is an `i32`.** `3g` and `2147483648` are errors for a size
//!     while a duration of the same text is fine; the reference returns `int`.
//! - [`Properties`], the merged view of `-Dkey=value` arguments and properties
//!   files that the three `main` classes take (`SystemUtil.loadPropertiesFiles`).
//!
//! Everything else about the property layer is [`names`]: the disposition table
//! that keeps the 80 names from being silently ignored (LC-6).

pub mod names;

pub use names::{Disposition, NAMES};

use std::fmt;
use std::path::Path;

/// Why a value could not be read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// The text is not a duration Agrona would accept.
    Duration(String),
    /// The text is not a size Agrona would accept, or does not fit in an `i32`.
    Size(String),
    /// A properties file could not be read.
    File(String),
    /// A `-D` argument was not of the form `-Dname=value`.
    Argument(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duration(text) => write!(f, "invalid duration: {text:?}"),
            Self::Size(text) => write!(f, "invalid size: {text:?}"),
            Self::File(text) => write!(f, "{text}"),
            Self::Argument(text) => write!(f, "invalid -D argument: {text:?}"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// `SystemUtil.getDurationInNanos(..)`: parse a `ns`/`us`/`ms`/`s` value into
/// nanoseconds. No suffix means nanoseconds.
///
/// The unit is matched **case-insensitively** (`10MS` is ten milliseconds), and
/// the text is **not** trimmed — `"1us "` is an error, exactly as in the
/// reference.
pub fn duration_ns(value: &str) -> Result<i64, ConfigError> {
    if value.is_empty() {
        return Err(ConfigError::Duration(value.to_owned()));
    }
    let lower = value.to_ascii_lowercase();
    // Two-letter units first: "10ms" also ends with "s".
    for (suffix, scale) in [("ns", 1i64), ("us", 1_000i64), ("ms", 1_000_000i64)] {
        if let Some(digits) = lower.strip_suffix(suffix) {
            let n = parse_digits(value, digits)?;
            return Ok(n * scale);
        }
    }
    if let Some(digits) = lower.strip_suffix('s') {
        let n = parse_digits(value, digits)?;
        return Ok(n * 1_000_000_000);
    }
    parse_digits(value, value)
}

/// `SystemUtil.getSizeAsInt(..)`: parse a `k`/`m`/`g` value into bytes.
///
/// Case-insensitive, no decimals, no negatives, and the result must fit in an
/// `i32` — that last part is the reference's, not ours: `getSizeAsInt` returns
/// `int`, so `3g` is an error there and would silently truncate if we widened
/// it here.
pub fn size(value: &str) -> Result<i32, ConfigError> {
    let lower = value.to_ascii_lowercase();
    let (digits, scale) = if let Some(digits) = lower.strip_suffix('k') {
        (digits, 1024i64)
    } else if let Some(digits) = lower.strip_suffix('m') {
        (digits, 1024 * 1024)
    } else if let Some(digits) = lower.strip_suffix('g') {
        (digits, 1024 * 1024 * 1024)
    } else {
        (lower.as_str(), 1)
    };
    let n = parse_digits(value, digits)?;
    let bytes = n
        .checked_mul(scale)
        .ok_or_else(|| ConfigError::Size(value.to_owned()))?;
    i32::try_from(bytes).map_err(|_| ConfigError::Size(value.to_owned()))
}

/// Digits only: no sign, no whitespace, no decimal point. The original text is
/// passed back in the error so the message names what the caller wrote.
fn parse_digits(original: &str, digits: &str) -> Result<i64, ConfigError> {
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ConfigError::Duration(original.to_owned()));
    }
    digits
        .parse::<i64>()
        .map_err(|_| ConfigError::Duration(original.to_owned()))
}

/// The merged property view the cluster's `main` classes read.
///
/// Precedence is `-D` over file, and later over earlier, matching
/// `SystemUtil.loadPropertiesFiles` plus `System.getProperty`.
///
/// There is no `get_or` that silently invents a value: a name that is neither
/// set nor given a default is `None`, and the caller decides. That is the same
/// shape as [`names::Disposition`] — nothing about the property layer is
/// allowed to be implicitly ignored (LC-6).
#[derive(Clone, Debug, Default)]
pub struct Properties {
    values: std::collections::BTreeMap<String, String>,
}

impl Properties {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Read `-Dname=value` arguments. Anything else is ignored, because the
    /// three `main` classes are handed the whole argv and only these are theirs.
    pub fn apply_args<I, S>(&mut self, args: I) -> Result<(), ConfigError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for arg in args {
            let arg = arg.as_ref();
            let Some(rest) = arg.strip_prefix("-D") else {
                continue;
            };
            let Some((name, value)) = rest.split_once('=') else {
                return Err(ConfigError::Argument(arg.to_owned()));
            };
            self.values.insert(name.to_owned(), value.to_owned());
        }
        Ok(())
    }

    /// Load a Java-format properties file: `name=value` per line, `#` or `!`
    /// comments, blank lines ignored. Values keep their surrounding whitespace
    /// the way `java.util.Properties` keeps it after the separator.
    pub fn load_file(&mut self, path: &Path) -> Result<(), ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| ConfigError::File(format!("{}: {error}", path.display())))?;
        for line in text.lines() {
            let line = line.trim_end();
            if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
                continue;
            }
            let Some((name, value)) = line.split_once('=') else {
                continue;
            };
            self.values.insert(name.trim().to_owned(), value.to_owned());
        }
        Ok(())
    }

    /// Every name and value, in name order.
    ///
    /// A binary that settles several configurations from one property list
    /// needs this: `deepmsg-cluster-backup` reads its driver's and its
    /// archive's names through APIs that take `-D` arguments, and re-reading
    /// the files there would let the two halves see different lists.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.values
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// The raw value, if set.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    /// `getDurationInNanos(name, default_ns)`: the parsed value, or the default
    /// when the name is not set.
    pub fn duration_ns(&self, name: &str, default_ns: i64) -> Result<i64, ConfigError> {
        match self.get(name) {
            Some(value) => duration_ns(value),
            None => Ok(default_ns),
        }
    }

    /// `getSizeAsInt(name, default)`: parsed, or the default when unset.
    pub fn size(&self, name: &str, default: i32) -> Result<i32, ConfigError> {
        match self.get(name) {
            Some(value) => size(value),
            None => Ok(default),
        }
    }

    /// How many names are set — used by tests and by the `describe` output.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether nothing is set.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn args_are_merged_with_last_wins() {
        let mut p = Properties::new();
        p.apply_args(["-Da=1", "ignored", "-Db=2", "-Da=3"])
            .unwrap();
        assert_eq!(p.get("a"), Some("3"));
        assert_eq!(p.get("b"), Some("2"));
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn a_bare_d_argument_is_an_error_not_a_silent_skip() {
        let mut p = Properties::new();
        assert_eq!(
            p.apply_args(["-Dnope"]),
            Err(ConfigError::Argument("-Dnope".to_owned()))
        );
    }

    #[test]
    fn defaults_apply_only_when_unset() {
        let mut p = Properties::new();
        assert_eq!(p.duration_ns("x", 7).unwrap(), 7);
        p.apply_args(["-Dx=1s"]).unwrap();
        assert_eq!(p.duration_ns("x", 7).unwrap(), 1_000_000_000);
    }
}
