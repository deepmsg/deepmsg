//! Channel URIs, as the driver reads the ones clients send it.
//!
//! Mirrors `aeron-client/src/main/c/uri/aeron_uri.c`: the scheme and transport
//! test at `:253-330` and the parameter scanner at `:34-110`. (M16) The value
//! readers reproduce `aeron_uri_get_int32`/`_get_bool`/`_get_timeout`
//! (`aeron-client/src/main/c/uri/aeron_uri.c:355-430`) and
//! `aeron_parse_size64`/`aeron_parse_duration_ns`
//! (`aeron-client/src/main/c/util/aeron_parse_util.c:42-105`, `:170-268`).
//!
//! The driver parses every URI itself rather than trusting what the client says
//! about its own channel — the same `aeron:ipc?session-id=7` arrives as text
//! and is what the driver names counters and log files after — so this lives
//! here and not in the client crate.
//!
//! # The grammar
//!
//! `aeron:` then either `udp`, which **must** be followed by `?`, or `ipc`,
//! which may be followed by `?` and parameters or end there. Parameters are
//! `key=value` pairs separated by **`|`** — not `&`, not `,` — and a second
//! `=` is part of the value, because the scanner splits on the first one
//! (`:57-62`).
//!
//! ```text
//! aeron:ipc
//! aeron:ipc?session-id=1001|term-length=64k
//! aeron:udp?endpoint=localhost:40123|mtu=1408
//! ```
//!
//! # Faithful where it matters, strict where it does not
//!
//! Three of these readers are reproduced *including* the reference's
//! looseness, because a URI this driver refuses is a channel the reference
//! would have served, and the log buffer that follows would differ:
//!
//! * `aeron_parse_size64` stops looking after the `k`/`m`/`g` suffix, so
//!   `term-length=1kb` is 1024 bytes there (`aeron_parse_util.c:60-70`).
//! * `aeron_parse_duration_ns` accepts `5m` (and `5mx`) as five milliseconds
//!   (`:180-200`).
//! * `aeron_uri_get_bool` compares only the first four characters
//!   (`aeron-client/src/main/c/uri/aeron_uri.c:411-414`), so `sparse=truex` is
//!   true.
//!
//! Where the reference is *destructive* rather than permissive this refuses,
//! and each refusal is recorded in the module that acts on it:
//!
//! * A trailing `?key` with no `=` is silently dropped upstream (the scanner's
//!   final flush only runs in its value state, `:96-102`); here it is an error,
//!   because a channel whose parameter went missing is a channel that would
//!   silently not have it.
//! * `key=` at the end of the URI reaches the reference's callback with a
//!   **null** value, which its readers treat as the parameter not being there
//!   at all (`aeron_uri.c:357-361`): the channel is served, without the
//!   parameter. Here it is an error, because a parameter whose value went
//!   missing is not the same channel as one that never had the parameter.
//! * A URI that is not valid UTF-8 is refused: the reference compares bytes,
//!   and every parameter this driver acts on is an ASCII number, a boolean or
//!   a comma-separated tag.

use std::fmt;

/// What a URI must start with (`AERON_URI_SCHEME`, `aeron_uri.c:23`).
pub const SCHEME: &str = "aeron:";

/// The longest URI accepted, including nothing for a terminator
/// (`AERON_URI_MAX_LENGTH`, `aeron-client/src/main/c/uri/aeron_uri.h:90`).
pub const MAX_LENGTH: usize = 4096;

const UDP_TRANSPORT: &str = "udp";
const IPC_TRANSPORT: &str = "ipc";

/// Which transport a URI names.
///
/// The driver's two data planes, and the whole of what the scheme check
/// decides: a URI is one of these or it is refused before anything else
/// happens to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    /// In-process: publication and subscription share a log buffer's pages.
    Ipc,
    /// Over a network: `udp`, which P1-2 does not serve yet.
    Udp,
}

/// One `key=value` pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Param<'a> {
    /// The name, as written.
    pub key: &'a str,
    /// The value: everything after the first `=`.
    pub value: &'a str,
}

/// A parsed channel URI: which transport, and the parameters it carried.
///
/// Borrows the URI it was parsed from, which is the command's own bytes on the
/// ring — nothing is copied and the parameters are only valid while the
/// command payload is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelUri<'a> {
    transport: Transport,
    params: Vec<Param<'a>>,
}

/// Why a URI could not be read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UriError {
    /// Not `aeron:ipc...` or `aeron:udp?...`.
    InvalidScheme,
    /// Longer than [`MAX_LENGTH`] — the reference refuses these rather than
    /// truncating, because the tail is where the parameters are.
    TooLong {
        /// How long it was.
        length: usize,
    },
    /// Not UTF-8. See the module note: the reference compares bytes.
    NotUtf8,
    /// A parameter with no `=`, or an empty name.
    MissingKey {
        /// The text that had no key.
        text: String,
    },
    /// A parameter whose value is empty.
    MissingValue {
        /// The name that had no value.
        key: String,
    },
    /// A parameter that is not a number.
    NotANumber {
        /// The parameter's name.
        key: String,
        /// What it said.
        value: String,
    },
    /// A parameter that is a number but not one of the type asked for.
    OutOfRange {
        /// The parameter's name.
        key: String,
        /// What it said.
        value: String,
    },
    /// A parameter that is not `true` or `false`.
    NotABoolean {
        /// The parameter's name.
        key: String,
        /// What it said.
        value: String,
    },
}

impl fmt::Display for UriError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidScheme => f.write_str("invalid URI scheme or transport"),
            Self::TooLong { length } => write!(
                f,
                "URI length ({length}) exceeds the maximum supported length ({MAX_LENGTH})"
            ),
            Self::NotUtf8 => f.write_str("the URI is not valid UTF-8"),
            Self::MissingKey { text } => write!(f, "parameter `{text}` has no value"),
            Self::MissingValue { key } => write!(f, "parameter `{key}` has an empty value"),
            Self::NotANumber { key, value } => {
                write!(f, "could not parse {key}={value} in URI as a number")
            }
            Self::OutOfRange { key, value } => {
                write!(f, "could not parse {key}={value} in URI: out of range")
            }
            Self::NotABoolean { key, value } => {
                write!(f, "could not parse {key}={value} in URI as a boolean")
            }
        }
    }
}

impl std::error::Error for UriError {}

impl<'a> ChannelUri<'a> {
    /// Read a URI, exactly as far as the reference's scanner does: the scheme
    /// and the transport, then the parameters.
    ///
    /// # Errors
    ///
    /// [`UriError`] for anything the grammar above does not accept.
    pub fn parse(uri: &'a [u8]) -> Result<Self, UriError> {
        if uri.len() >= MAX_LENGTH {
            return Err(UriError::TooLong { length: uri.len() });
        }

        let text = std::str::from_utf8(uri).map_err(|_| UriError::NotUtf8)?;
        let rest = text.strip_prefix(SCHEME).ok_or(UriError::InvalidScheme)?;

        let (transport, rest) = if let Some(rest) = rest.strip_prefix(IPC_TRANSPORT) {
            (Transport::Ipc, rest)
        } else if let Some(rest) = rest.strip_prefix(UDP_TRANSPORT) {
            (Transport::Udp, rest)
        } else {
            return Err(UriError::InvalidScheme);
        };

        let params = match rest {
            // `aeron:ipc` with nothing after it is a channel with no
            // parameters, and is how most clients name one.
            "" if transport == Transport::Ipc => Vec::new(),
            // `aeron:udp` without a `?` is not a channel: the reference reads
            // the next byte as the separator and finds the terminator
            // (`aeron_uri.c:281-290`).
            "" => return Err(UriError::InvalidScheme),
            rest => {
                let rest = rest.strip_prefix('?').ok_or(UriError::InvalidScheme)?;
                scan_params(rest)?
            }
        };

        Ok(Self { transport, params })
    }

    /// Which transport this names.
    pub const fn transport(&self) -> Transport {
        self.transport
    }

    /// The parameters, in the order they were written.
    pub fn params(&self) -> &[Param<'a>] {
        &self.params
    }

    /// The value of `key`, or `None` if it is not there.
    ///
    /// The **first** match wins and a repeat is not an error — the same rule as
    /// `aeron_uri_find_param_value` (`aeron-client/src/main/c/uri/aeron_uri.c:337-352`),
    /// which scans forward and returns on the first hit.
    pub fn value(&self, key: &str) -> Option<&'a str> {
        self.params
            .iter()
            .find(|param| param.key == key)
            .map(|param| param.value)
    }

    /// An `int32` parameter (`aeron_uri_get_int32`,
    /// `aeron-client/src/main/c/uri/aeron_uri.c:355-385`).
    ///
    /// # Errors
    ///
    /// [`UriError::NotANumber`] if the value is not a number in the base the
    /// reference reads (`strtol`'s **zero**: decimal, `0x` hex, leading-`0`
    /// octal), and [`UriError::OutOfRange`] if it does not fit.
    pub fn i32(&self, key: &str) -> Result<Option<i32>, UriError> {
        let Some(value) = self.value(key) else {
            return Ok(None);
        };

        let Some(number) = parse_base_zero(value) else {
            return Err(UriError::NotANumber {
                key: key.to_owned(),
                value: value.to_owned(),
            });
        };

        i32::try_from(number)
            .map(Some)
            .map_err(|_| UriError::OutOfRange {
                key: key.to_owned(),
                value: value.to_owned(),
            })
    }

    /// A `bool` parameter (`aeron_uri_get_bool`,
    /// `aeron-client/src/main/c/uri/aeron_uri.c:409-430`).
    ///
    /// # Errors
    ///
    /// [`UriError::NotABoolean`] for anything that does not start with `true`
    /// or `false` — the reference compares the first four and five characters
    /// and not the whole value, so `truey` is true there and is true here.
    pub fn bool(&self, key: &str) -> Result<Option<bool>, UriError> {
        let Some(value) = self.value(key) else {
            return Ok(None);
        };

        if value.starts_with("true") {
            Ok(Some(true))
        } else if value.starts_with("false") {
            Ok(Some(false))
        } else {
            Err(UriError::NotABoolean {
                key: key.to_owned(),
                value: value.to_owned(),
            })
        }
    }

    /// A size parameter (`aeron_parse_size64`,
    /// `aeron-client/src/main/c/util/aeron_parse_util.c:42-105`): decimal
    /// digits and an optional `k`/`K`, `m`/`M` or `g`/`G`.
    ///
    /// Whatever follows a recognised suffix is **not looked at** — the
    /// reference matches the one character and then multiplies — so `1kb` and
    /// `1kzz` are both 1024. That is reproduced deliberately: a client that
    /// sends one of those gets a 1024-byte parameter from a reference driver,
    /// and the log buffer it names has to be the same one here. A character
    /// that is *not* a unit is an error in both implementations (`1408b` is
    /// refused), which is the other half of the same comparison.
    ///
    /// # Errors
    ///
    /// [`UriError::NotANumber`] for no digits or an unknown suffix, and
    /// [`UriError::OutOfRange`] for a negative value or one that overflows.
    pub fn size(&self, key: &str) -> Result<Option<u64>, UriError> {
        let Some(value) = self.value(key) else {
            return Ok(None);
        };

        parse_size(key, value).map(Some)
    }

    /// A duration parameter (`aeron_parse_duration_ns`,
    /// `aeron-client/src/main/c/util/aeron_parse_util.c:170-268`), as
    /// nanoseconds.
    ///
    /// The units are `ns`, `us`, `ms` and `s`; a bare number is nanoseconds.
    /// The suffix letter alone means the unit **and so does the letter followed
    /// by exactly one character** — the reference's check for `m`, `u` and `n`
    /// is `next != 's' && *(end + 2) != '\0'` — which is why `5m`, `5ms` and
    /// `5mx` are all five milliseconds there and here.
    ///
    /// A value too large for its unit saturates at
    /// [`i64::MAX`](i64::MAX) rather than failing, which is also the
    /// reference's answer (`*result = LLONG_MAX`, `:190`).
    ///
    /// # Errors
    ///
    /// [`UriError::NotANumber`] for no digits or an unknown unit.
    pub fn duration_ns(&self, key: &str) -> Result<Option<i64>, UriError> {
        let Some(value) = self.value(key) else {
            return Ok(None);
        };

        parse_duration(key, value).map(Some)
    }
}

/// Split `aeron:ipc`'s tail into parameters.
///
/// The scanner's grammar, with its two destructive cases turned into errors —
/// see the module note.
fn scan_params<'a>(text: &'a str) -> Result<Vec<Param<'a>>, UriError> {
    let mut params = Vec::new();

    // `aeron:ipc?` is a channel with no parameters rather than one empty one,
    // which is the reference's answer too: its loop never runs and the final
    // flush is inside the value branch it never reached (`aeron_uri.c:96-102`).
    if text.is_empty() {
        return Ok(params);
    }

    for pair in text.split('|') {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(UriError::MissingKey {
                text: pair.to_owned(),
            });
        };

        if key.is_empty() {
            return Err(UriError::MissingKey {
                text: pair.to_owned(),
            });
        }

        if value.is_empty() {
            return Err(UriError::MissingValue {
                key: key.to_owned(),
            });
        }

        params.push(Param { key, value });
    }

    Ok(params)
}

/// `strtol(value, &end, 0)`: an optional sign, then `0x` hexadecimal, a
/// leading-`0` octal or decimal — and every character consumed.
///
/// `None` for anything else, including a value that is empty or has trailing
/// characters. The reference's reader checks the same way round: it requires
/// `*end_ptr == '\0'` after the call
/// (`aeron-client/src/main/c/uri/aeron_uri.c:365-370`).
fn parse_base_zero(value: &str) -> Option<i128> {
    let (negative, magnitude) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };

    let digits = if let Some(hex) = magnitude
        .strip_prefix("0x")
        .or_else(|| magnitude.strip_prefix("0X"))
    {
        i128::from_str_radix(hex, 16).ok()?
    } else if magnitude.len() > 1 && magnitude.starts_with('0') {
        i128::from_str_radix(&magnitude[1..], 8).ok()?
    } else {
        magnitude.parse::<i128>().ok()?
    };

    Some(if negative { -digits } else { digits })
}

/// `aeron_parse_size64`, including its habit of ignoring what follows the
/// suffix.
fn parse_size(key: &str, value: &str) -> Result<u64, UriError> {
    let not_a_number = || UriError::NotANumber {
        key: key.to_owned(),
        value: value.to_owned(),
    };
    let out_of_range = || UriError::OutOfRange {
        key: key.to_owned(),
        value: value.to_owned(),
    };

    // `strtoll(value, &end, 10)` skips leading whitespace and accepts a sign.
    let trimmed = value.trim_start();
    let (negative, digits) = match trimmed.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };

    let end = digits
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(digits.len());
    if end == 0 {
        // `end == str`: not one digit was read.
        return Err(not_a_number());
    }

    let magnitude: i64 = digits[..end].parse().map_err(|_| out_of_range())?;
    if negative || magnitude < 0 {
        return Err(out_of_range());
    }

    #[allow(clippy::cast_sign_loss)] // non-negative above
    let magnitude = magnitude as u64;
    let multiplier = match digits[end..].chars().next() {
        None => 1,
        Some('k' | 'K') => 1024,
        Some('m' | 'M') => 1024 * 1024,
        Some('g' | 'G') => 1024 * 1024 * 1024,
        // Any other character is not a unit, and the reference reads the
        // suffix as the last thing it looks at.
        Some(_) => return Err(not_a_number()),
    };

    magnitude.checked_mul(multiplier).ok_or_else(out_of_range)
}

/// `aeron_parse_duration_ns`, including the one-character tail it tolerates.
fn parse_duration(key: &str, value: &str) -> Result<i64, UriError> {
    let not_a_number = || UriError::NotANumber {
        key: key.to_owned(),
        value: value.to_owned(),
    };

    let trimmed = value.trim_start();
    let (negative, digits) = match trimmed.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };

    let end = digits
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(digits.len());
    if end == 0 || negative {
        return Err(not_a_number());
    }

    let magnitude: i64 = digits[..end].parse().map_err(|_| not_a_number())?;
    let tail = &digits[end..];

    // A bare number is nanoseconds.
    let Some(unit) = tail.chars().next() else {
        return Ok(magnitude);
    };

    let after = &tail[unit.len_utf8()..];
    let multiplier = match unit {
        // Seconds take nothing after them: `5sx` is rejected upstream.
        's' | 'S' if after.is_empty() => 1_000_000_000,
        'm' | 'M' | 'u' | 'U' | 'n' | 'N' if after.len() <= 1 || after.starts_with(['s', 'S']) => {
            match unit {
                'm' | 'M' => 1_000_000,
                'u' | 'U' => 1_000,
                _ => 1,
            }
        }
        _ => return Err(not_a_number()),
    };

    // `value * multiplier` saturating, which is the reference's answer for a
    // value it would otherwise overflow: it stores `LLONG_MAX`.
    Ok(magnitude.saturating_mul(multiplier))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(uri: &str) -> ChannelUri<'_> {
        ChannelUri::parse(uri.as_bytes()).unwrap_or_else(|error| panic!("{uri}: {error}"))
    }

    #[test]
    fn the_two_transports_are_told_apart() {
        assert_eq!(Transport::Ipc, parse("aeron:ipc").transport());
        assert_eq!(Transport::Ipc, parse("aeron:ipc?").transport());
        assert_eq!(
            Transport::Udp,
            parse("aeron:udp?endpoint=localhost:40123").transport()
        );

        // A channel with no parameters is the common case and has to be free
        // of them, not one empty one.
        assert!(parse("aeron:ipc").params().is_empty());
        assert!(parse("aeron:ipc?").params().is_empty());

        for bad in [
            "aeron:ipcfoo",
            "aeron:udp",
            "ipc",
            "aeron:",
            "aeron:tcp?endpoint=localhost:1",
            "",
        ] {
            assert!(
                ChannelUri::parse(bad.as_bytes()).is_err(),
                "{bad} is not a channel"
            );
        }
    }

    #[test]
    fn parameters_are_separated_by_pipes_and_split_at_the_first_equals() {
        let uri = parse("aeron:ipc?session-id=1001|term-length=64k|tags=a,b");

        assert_eq!(Some("1001"), uri.value("session-id"));
        assert_eq!(Some("64k"), uri.value("term-length"));
        assert_eq!(
            Some("a,b"),
            uri.value("tags"),
            "the comma is not a separator"
        );
        assert_eq!(None, uri.value("mtu"));

        // A value with an `=` in it keeps everything after the first one, and
        // a repeated key answers with the first.
        assert_eq!(
            Some("a=b=c"),
            parse("aeron:ipc?alias=a=b=c").value("alias"),
            "everything after the first `=` is the value"
        );
        assert_eq!(
            Some("1"),
            parse("aeron:ipc?session-id=1|session-id=2").value("session-id")
        );
    }

    #[test]
    fn the_destructive_shapes_are_refused_here() {
        // Upstream these are a silently dropped parameter and a null pointer
        // reaching a number reader. See the module note.
        for bad in [
            "aeron:ipc?session-id",
            "aeron:ipc?=5",
            "aeron:ipc?session-id=",
            "aeron:ipc?session-id=1|",
        ] {
            assert!(
                ChannelUri::parse(bad.as_bytes()).is_err(),
                "{bad} should not parse"
            );
        }
    }

    #[test]
    fn an_i32_is_read_in_base_zero_and_must_fit() {
        let uri = parse("aeron:ipc?session-id=0x10|term-id=010|init-term-id=-5|big=3000000000");

        assert_eq!(Ok(Some(16)), uri.i32("session-id"), "hex");
        assert_eq!(Ok(Some(8)), uri.i32("term-id"), "octal, as strtol reads it");
        assert_eq!(Ok(Some(-5)), uri.i32("init-term-id"));
        assert_eq!(Ok(None), uri.i32("term-length"), "absent is not zero");
        assert_eq!(
            Err(UriError::OutOfRange {
                key: "big".to_owned(),
                value: "3000000000".to_owned(),
            }),
            uri.i32("big")
        );
        assert_eq!(
            Err(UriError::NotANumber {
                key: "session-id".to_owned(),
                value: "12x".to_owned(),
            }),
            parse("aeron:ipc?session-id=12x").i32("session-id")
        );
    }

    #[test]
    fn a_boolean_is_a_prefix_and_nothing_else() {
        // The reference compares four characters, so the tail is not read.
        assert_eq!(
            Ok(Some(true)),
            parse("aeron:ipc?sparse=true").bool("sparse")
        );
        assert_eq!(
            Ok(Some(true)),
            parse("aeron:ipc?sparse=truex").bool("sparse")
        );
        assert_eq!(Ok(Some(false)), parse("aeron:ipc?eos=false").bool("eos"));
        assert_eq!(Ok(None), parse("aeron:ipc").bool("sparse"));
        assert_eq!(
            Err(UriError::NotABoolean {
                key: "sparse".to_owned(),
                value: "1".to_owned(),
            }),
            parse("aeron:ipc?sparse=1").bool("sparse")
        );
    }

    #[test]
    fn a_size_takes_a_binary_suffix_and_ignores_what_follows_it() {
        let uri = parse("aeron:ipc?term-length=64k|mtu=1408|pub-wnd=1m");

        assert_eq!(Ok(Some(64 * 1024)), uri.size("term-length"));
        assert_eq!(Ok(Some(1408)), uri.size("mtu"));
        assert_eq!(Ok(Some(1024 * 1024)), uri.size("pub-wnd"));
        assert_eq!(Ok(None), uri.size("absent"));

        // The tail after the suffix is not read, which is what makes
        // `mtu=1408b` a 1408-byte MTU upstream.
        assert_eq!(Ok(Some(1024)), parse("aeron:ipc?mtu=1kb").size("mtu"));
        assert_eq!(Ok(Some(1024)), parse("aeron:ipc?mtu=1kzz").size("mtu"));
        assert_eq!(
            Err(UriError::NotANumber {
                key: "mtu".to_owned(),
                value: "1408b".to_owned(),
            }),
            parse("aeron:ipc?mtu=1408b").size("mtu"),
            "a character that is not a unit is not a suffix"
        );

        assert_eq!(
            Err(UriError::NotANumber {
                key: "mtu".to_owned(),
                value: "1x".to_owned(),
            }),
            parse("aeron:ipc?mtu=1x").size("mtu")
        );
        assert_eq!(
            Err(UriError::OutOfRange {
                key: "mtu".to_owned(),
                value: "-1".to_owned(),
            }),
            parse("aeron:ipc?mtu=-1").size("mtu")
        );
    }

    #[test]
    fn a_duration_reads_the_letter_and_tolerates_one_more_character() {
        let uri = parse(
            "aeron:ipc?linger=5s|untethered-window-limit-timeout=100ms|untethered-resting-timeout=10s|bare=250",
        );

        assert_eq!(Ok(Some(5_000_000_000)), uri.duration_ns("linger"));
        assert_eq!(
            Ok(Some(100_000_000)),
            uri.duration_ns("untethered-window-limit-timeout")
        );
        assert_eq!(
            Ok(Some(10_000_000_000)),
            uri.duration_ns("untethered-resting-timeout")
        );
        assert_eq!(Ok(Some(250)), uri.duration_ns("bare"), "nanoseconds");
        assert_eq!(Ok(None), uri.duration_ns("untethered-linger-timeout"));

        // `5m`, `5ms` and `5mx` are the same five milliseconds upstream.
        for written in ["5m", "5ms", "5mx"] {
            assert_eq!(
                Ok(Some(5_000_000)),
                parse(&format!("aeron:ipc?linger={written}")).duration_ns("linger"),
                "{written}"
            );
        }

        // A second is the one unit that takes nothing after it, and a unit-less
        // tail of two characters is not a unit at all.
        assert_eq!(
            Err(UriError::NotANumber {
                key: "linger".to_owned(),
                value: "5sx".to_owned(),
            }),
            parse("aeron:ipc?linger=5sx").duration_ns("linger")
        );
        assert_eq!(
            Err(UriError::NotANumber {
                key: "linger".to_owned(),
                value: "5mxy".to_owned(),
            }),
            parse("aeron:ipc?linger=5mxy").duration_ns("linger")
        );

        // And a value too large for its unit saturates rather than failing.
        assert_eq!(
            Ok(Some(i64::MAX)),
            parse("aeron:ipc?linger=99999999999999999s").duration_ns("linger")
        );
    }

    #[test]
    fn a_uri_longer_than_the_reference_allows_is_refused() {
        let uri = format!("aeron:ipc?alias={}", "x".repeat(MAX_LENGTH));
        assert_eq!(
            Err(UriError::TooLong { length: uri.len() }),
            ChannelUri::parse(uri.as_bytes())
        );
    }

    #[test]
    fn bytes_that_are_not_text_are_refused() {
        let mut uri = b"aeron:ipc?alias=".to_vec();
        uri.push(0xff);

        assert_eq!(Err(UriError::NotUtf8), ChannelUri::parse(&uri));
    }
}
