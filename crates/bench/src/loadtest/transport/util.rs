//! The message layout, the channel defaults, and the two decisions the
//! reference's `AeronUtil` holds.
//!
//! Mirrors `benchmarks-aeron/src/main/java/io/aeron/benchmarks/aeron/AeronUtil.java`
//! — the module of the reference's whose settings are read from wherever they
//! are needed rather than gathered into its `Configuration`.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use deepmsg_core::logbuffer::append::Appended;

use crate::loadtest::config::{ConfigError, Properties};
use crate::loadtest::transceiver::{Clock, Idle};

/// `AeronUtil.TIMESTAMP_OFFSET`: the message's timestamp is at its front.
pub const TIMESTAMP_OFFSET: usize = 0;

/// `AeronUtil.RECEIVER_INDEX_OFFSET`: after the timestamp, an `int32` naming
/// which receiver a message is for.
pub const RECEIVER_INDEX_OFFSET: usize = TIMESTAMP_OFFSET + 8;

/// `AeronUtil.MIN_MESSAGE_LENGTH`: a timestamp, a receiver index and a checksum,
/// and nothing else.
///
/// Longer than the rig's own minimum of sixteen, which is the two `int64`s
/// alone — this one is the Aeron transceivers' floor, and the reference refuses
/// a shorter message where a configuration would have accepted it.
pub const MIN_MESSAGE_LENGTH: usize = RECEIVER_INDEX_OFFSET + 8 + 8;

/// `AeronUtil.SEND_ATTEMPTS`: how many times a send that is back-pressured
/// tries again before it gives up and reports how far it got.
///
/// The count is not a timeout: an administrative action does not spend one,
/// which is why a publication that keeps answering one loops.
pub const SEND_ATTEMPTS: usize = 3;

/// `AeronUtil.FRAGMENT_LIMIT`'s default: how many fragments one poll takes.
pub const DEFAULT_FRAGMENT_LIMIT: usize = 10;

/// The channel a client publishes on, and the one it reads replies from.
pub const DEFAULT_DESTINATION_CHANNEL: &str = "aeron:udp?endpoint=localhost:13333|mtu=1408";
/// The stream a client publishes on.
pub const DEFAULT_DESTINATION_STREAM: i32 = 77777;
/// The channel a client reads replies from.
pub const DEFAULT_SOURCE_CHANNEL: &str = "aeron:udp?endpoint=localhost:13334|mtu=1408";
/// The stream a client reads replies from.
pub const DEFAULT_SOURCE_STREAM: i32 = 55555;

/// How long to wait for a publication and a subscription to find each other.
pub const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);

/// The settings `AeronUtil` reads, spelled out as `Configuration`'s are.
pub mod property {
    /// `AeronUtil.DESTINATION_CHANNEL_PROP_NAME`.
    pub const DESTINATION_CHANNEL: &str = "io.aeron.benchmarks.aeron.destination.channel";
    /// `AeronUtil.DESTINATION_STREAM_PROP_NAME`.
    pub const DESTINATION_STREAM: &str = "io.aeron.benchmarks.aeron.destination.stream";
    /// `AeronUtil.SOURCE_CHANNEL_PROP_NAME`.
    pub const SOURCE_CHANNEL: &str = "io.aeron.benchmarks.aeron.source.channel";
    /// `AeronUtil.SOURCE_STREAM_PROP_NAME`.
    pub const SOURCE_STREAM: &str = "io.aeron.benchmarks.aeron.source.stream";
    /// `AeronUtil.NUMBER_OF_RECEIVERS_PROP_NAME`.
    pub const RECEIVER_COUNT: &str = "io.aeron.benchmarks.aeron.receiver.count";
    /// `AeronUtil.RECEIVER_INDEX_PROP_NAME`.
    pub const RECEIVER_INDEX: &str = "io.aeron.benchmarks.aeron.receiver.index";
    /// `AeronUtil.USE_TRY_CLAIM_PROP_NAME`.
    pub const USE_TRY_CLAIM: &str = "io.aeron.benchmarks.aeron.use.try.claim";
    /// `AeronUtil.FRAGMENT_LIMIT_PROP_NAME`.
    pub const FRAGMENT_LIMIT: &str = "io.aeron.benchmarks.aeron.fragment.limit";
    /// `AeronUtil.CONNECTION_TIMEOUT_PROP_NAME`.
    pub const CONNECTION_TIMEOUT: &str = "io.aeron.benchmarks.aeron.connection.timeout";
    /// `AeronUtil.IDLE_STRATEGY_PROP_NAME` — the *node's* idle strategy, which
    /// the reference keeps apart from the client's
    /// `io.aeron.benchmarks.idle.strategy`.
    pub const IDLE_STRATEGY: &str = "io.aeron.benchmarks.aeron.idle.strategy";
    /// The directory the driver is in, which the client is told rather than
    /// left to guess — the same name the driver reads
    /// (`CommonContext.AERON_DIR_PROP_NAME`, `aeron.dir`).
    pub const DIRECTORY: &str = "aeron.dir";
}

/// The channel and connection settings of one run.
///
/// The reference reads these through `System.getProperty` wherever it needs
/// them, which is why they are not part of its `Configuration` either. Gathering
/// them once is the same set of values with somewhere to look them up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelSettings {
    /// Where the driver is. There is no default: the driver insists on being
    /// told, and a client that guessed a different directory would connect to
    /// nothing and say so only as a timeout.
    pub directory: PathBuf,
    /// Where a client publishes.
    pub destination_channel: String,
    /// The stream it publishes on.
    pub destination_stream: i32,
    /// Where a client reads replies.
    pub source_channel: String,
    /// The stream it reads replies on.
    pub source_stream: i32,
    /// How many receivers the far end has, which is how many replies one message
    /// earns.
    pub receiver_count: i32,
    /// Which receiver this process is, for a node that has to know.
    pub receiver_index: i32,
    /// Whether a sender claims into the log or offers into it.
    pub use_try_claim: bool,
    /// How many fragments one poll takes.
    pub fragment_limit: usize,
    /// How long to wait for a publication and a subscription to connect.
    pub connection_timeout: Duration,
}

impl ChannelSettings {
    /// The settings a run's properties describe, with the reference's defaults.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for an integer that is not one, or a duration that is not
    /// written the way Agrona writes one.
    pub fn from_properties(properties: &Properties) -> Result<Self, ConfigError> {
        Ok(Self {
            directory: PathBuf::from(properties.get(property::DIRECTORY).ok_or(
                ConfigError::Required {
                    property: property::DIRECTORY,
                },
            )?),
            destination_channel: text(properties, property::DESTINATION_CHANNEL)
                .unwrap_or_else(|| DEFAULT_DESTINATION_CHANNEL.to_owned()),
            destination_stream: properties
                .integer_or(property::DESTINATION_STREAM, DEFAULT_DESTINATION_STREAM)?,
            source_channel: text(properties, property::SOURCE_CHANNEL)
                .unwrap_or_else(|| DEFAULT_SOURCE_CHANNEL.to_owned()),
            source_stream: properties.integer_or(property::SOURCE_STREAM, DEFAULT_SOURCE_STREAM)?,
            receiver_count: properties.integer_or(property::RECEIVER_COUNT, 1)?,
            receiver_index: properties.integer_or(property::RECEIVER_INDEX, 0)?,
            // `Boolean.parseBoolean(System.getProperty(name, "true"))`: only a
            // literal `true` in any case is one, and the default is yes.
            use_try_claim: properties
                .get(property::USE_TRY_CLAIM)
                .is_none_or(|value| value.eq_ignore_ascii_case("true")),
            fragment_limit: usize::try_from(properties.integer_or(
                property::FRAGMENT_LIMIT,
                i32::try_from(DEFAULT_FRAGMENT_LIMIT).unwrap_or(10),
            )?)
            .unwrap_or(DEFAULT_FRAGMENT_LIMIT),
            connection_timeout: match text(properties, property::CONNECTION_TIMEOUT) {
                Some(value) => parse_duration(&value)?,
                None => DEFAULT_CONNECTION_TIMEOUT,
            },
        })
    }
}

/// A property's value, if it has a non-empty one.
fn text(properties: &Properties, name: &str) -> Option<String> {
    properties.get(name).map(str::to_owned)
}

/// Agrona's `parseDuration`, for the shapes the reference's own files write.
///
/// A number and a unit, with no space between them. Agrona also accepts the
/// nanosecond unit written as bare digits, and nothing here needs that: the one
/// place a duration is configured, the reference's scripts write seconds.
pub fn parse_duration(value: &str) -> Result<Duration, ConfigError> {
    let digits = value.trim_end_matches(|character: char| character.is_ascii_alphabetic());
    let unit = &value[digits.len()..];

    let amount: u64 = digits.parse().map_err(|_| ConfigError::Duration {
        value: value.to_owned(),
    })?;

    let unit = match unit {
        "ns" => Duration::from_nanos(amount),
        "us" => Duration::from_micros(amount),
        "ms" => Duration::from_millis(amount),
        "s" => Duration::from_secs(amount),
        "m" => Duration::from_secs(amount * 60),
        "h" => Duration::from_secs(amount * 60 * 60),
        "d" => Duration::from_secs(amount * 60 * 60 * 24),
        _ => {
            return Err(ConfigError::Duration {
                value: value.to_owned(),
            });
        }
    };

    Ok(unit)
}

/// What a send that the publication refused means.
#[derive(Debug)]
pub enum PublicationError {
    /// The publication said something the sender has no answer to, with the
    /// reference's own description of it.
    Rejected {
        /// What it said.
        appended: Appended,
        /// How the reference names it — `Publication.errorString`.
        description: &'static str,
    },
}

impl fmt::Display for PublicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { description, .. } => write!(f, "Publication error: {description}"),
        }
    }
}

impl std::error::Error for PublicationError {}

/// What `Publication.errorString` calls an append outcome.
///
/// The reference's client has a name for each of these and prints it in the
/// exception a run dies of; the names below are the ones a reader would see from
/// Java, so that a failure is recognisable on either side.
#[must_use]
pub fn error_string(appended: Appended) -> &'static str {
    match appended {
        Appended::Ok { .. } => "OK",
        Appended::BackPressured => "Back pressured",
        Appended::NotConnected => "Not connected",
        Appended::MaxPositionExceeded => "Max position exceeded",
        Appended::MessageTooLarge => "Max message length exceeded",
        // `ADMIN_ACTION` is not an error to the reference — it is "try again" —
        // so this is only reached if one is asked about outside
        // [`check_publication_result`], where it is handled before it gets here.
        Appended::MidRotation | Appended::EndOfLog => "Administrative action",
        Appended::Malformed => "Malformed log",
    }
}

/// What a sender does about an append that did not happen
/// (`AeronUtil.checkPublicationResult`).
///
/// `Ok(true)` means try again without counting it — an administrative action,
/// which is the log rotating under the producer and nothing to do with whether
/// the far end is keeping up. `Ok(false)` means try again with the attempt
/// counted, which is back pressure, and the caller gives up after
/// [`SEND_ATTEMPTS`] of those. `Err` is everything else, which the reference
/// throws on and which ends the run here too: a publication that is not
/// connected, or is closed, or has run out of position is not slow, it is broken.
pub fn check_publication_result(
    appended: Appended,
    idle: &mut impl Idle,
) -> Result<bool, PublicationError> {
    match appended {
        // `Publication.BACK_PRESSURED`: wait a moment and let the caller count it.
        Appended::BackPressured => {
            idle.idle();
            Ok(false)
        }
        // `Publication.ADMIN_ACTION`: retry, and do not count it — the log
        // rotated, and a producer that counted that would give up on a busy
        // publication for a reason that has nothing to do with the far end.
        Appended::MidRotation | Appended::EndOfLog => Ok(true),
        other => Err(PublicationError::Rejected {
            appended: other,
            description: error_string(other),
        }),
    }
}

/// Wait until something is true, or give up.
///
/// `AeronUtil.awaitConnected`: an idle yield in a loop against a deadline, so
/// that a publication and a subscription that never find each other fail the run
/// with a time rather than hanging it.
///
/// # Errors
///
/// A description of how long it waited, as the reference's message says.
pub fn await_connected(
    mut connected: impl FnMut() -> bool,
    timeout: Duration,
    clock: &impl Clock,
) -> Result<(), PublicationError> {
    let timeout_ns = i64::try_from(timeout.as_nanos()).unwrap_or(i64::MAX);
    let deadline_ns = clock.nano_time().saturating_add(timeout_ns);

    while !connected() {
        if clock.nano_time() < deadline_ns {
            std::thread::yield_now();
        } else {
            return Err(PublicationError::Rejected {
                appended: Appended::NotConnected,
                description: "Failed to connect within timeout",
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::*;
    use crate::loadtest::transceiver::SystemClock;

    /// The settings every test here starts from: the directory the driver is in
    /// is the one thing there is no default for.
    fn properties() -> Properties {
        let mut properties = Properties::new();
        properties.set(property::DIRECTORY, "/dev/shm/deepmsg-bench-test");

        properties
    }

    #[test]
    fn the_directory_is_required() {
        let error = ChannelSettings::from_properties(&Properties::new()).expect_err("refused");

        assert!(error.to_string().contains("aeron.dir"), "{error}");
    }

    #[test]
    fn the_defaults_are_the_reference_s() {
        let settings = ChannelSettings::from_properties(&properties()).expect("valid");

        assert_eq!(settings.destination_channel, DEFAULT_DESTINATION_CHANNEL);
        assert_eq!(settings.destination_stream, 77777);
        assert_eq!(settings.source_channel, DEFAULT_SOURCE_CHANNEL);
        assert_eq!(settings.source_stream, 55555);
        assert_eq!(settings.receiver_count, 1);
        assert_eq!(settings.receiver_index, 0);
        assert!(
            settings.use_try_claim,
            "the reference's default is to claim"
        );
        assert_eq!(settings.fragment_limit, 10);
        assert_eq!(settings.connection_timeout, Duration::from_secs(60));
    }

    #[test]
    fn the_settings_are_read_from_the_run_s_properties() {
        let mut properties = properties();
        properties.set(property::DESTINATION_CHANNEL, "aeron:ipc");
        properties.set(property::DESTINATION_STREAM, "1001");
        properties.set(property::SOURCE_STREAM, "1002");
        properties.set(property::RECEIVER_COUNT, "2");
        properties.set(property::RECEIVER_INDEX, "1");
        properties.set(property::USE_TRY_CLAIM, "false");
        properties.set(property::FRAGMENT_LIMIT, "16");
        properties.set(property::CONNECTION_TIMEOUT, "10s");

        let settings = ChannelSettings::from_properties(&properties).expect("valid");

        assert_eq!(settings.destination_channel, "aeron:ipc");
        assert_eq!(settings.destination_stream, 1001);
        assert_eq!(settings.source_stream, 1002);
        assert_eq!(settings.receiver_count, 2);
        assert_eq!(settings.receiver_index, 1);
        assert!(!settings.use_try_claim);
        assert_eq!(settings.fragment_limit, 16);
        assert_eq!(settings.connection_timeout, Duration::from_secs(10));
    }

    /// `Boolean.parseBoolean`: `True` is true, and so is `TRUE`; anything else
    /// is not.
    #[test]
    fn only_a_literal_true_is_true() {
        for (written, expected) in [
            ("true", true),
            ("TRUE", true),
            ("True", true),
            ("1", false),
            ("yes", false),
        ] {
            let mut properties = properties();
            properties.set(property::USE_TRY_CLAIM, written);

            let settings = ChannelSettings::from_properties(&properties).expect("valid");

            assert_eq!(settings.use_try_claim, expected, "for {written}");
        }
    }

    #[test]
    fn a_duration_is_a_number_and_a_unit() {
        assert_eq!(
            parse_duration("1ns").expect("valid"),
            Duration::from_nanos(1)
        );
        assert_eq!(
            parse_duration("2us").expect("valid"),
            Duration::from_micros(2)
        );
        assert_eq!(
            parse_duration("3ms").expect("valid"),
            Duration::from_millis(3)
        );
        assert_eq!(parse_duration("4s").expect("valid"), Duration::from_secs(4));
        assert_eq!(
            parse_duration("5m").expect("valid"),
            Duration::from_secs(300)
        );
        assert_eq!(
            parse_duration("6h").expect("valid"),
            Duration::from_secs(21_600)
        );
        assert_eq!(
            parse_duration("1d").expect("valid"),
            Duration::from_secs(86_400)
        );

        assert!(parse_duration("10").is_err(), "a bare number has no unit");
        assert!(parse_duration("ten seconds").is_err());
    }

    #[test]
    fn back_pressure_is_counted_and_an_administrative_action_is_not() {
        let idles = Rc::new(Cell::new(0));
        let mut idle = CountingIdle(Rc::clone(&idles));

        assert!(matches!(
            check_publication_result(Appended::BackPressured, &mut idle),
            Ok(false)
        ));
        assert_eq!(
            idles.get(),
            1,
            "back pressure waits a moment before the caller counts it"
        );

        assert!(matches!(
            check_publication_result(Appended::MidRotation, &mut idle),
            Ok(true)
        ));
        assert!(matches!(
            check_publication_result(Appended::EndOfLog, &mut idle),
            Ok(true)
        ));
        assert_eq!(idles.get(), 1, "a rotation does not wait");
    }

    #[test]
    fn anything_else_ends_the_run_with_the_reference_s_words() {
        for (appended, description) in [
            (Appended::NotConnected, "Not connected"),
            (Appended::MaxPositionExceeded, "Max position exceeded"),
            (Appended::MessageTooLarge, "Max message length exceeded"),
            (Appended::Malformed, "Malformed log"),
        ] {
            let idles = Rc::new(Cell::new(0));
            let error = check_publication_result(appended, &mut CountingIdle(Rc::clone(&idles)))
                .expect_err("refused");

            assert_eq!(
                error.to_string(),
                format!("Publication error: {description}")
            );
            assert_eq!(idles.get(), 0);
        }
    }

    #[derive(Clone)]
    struct CountingIdle(Rc<Cell<usize>>);

    impl Idle for CountingIdle {
        fn idle(&mut self) {
            self.0.set(self.0.get() + 1);
        }

        fn reset(&mut self) {}
    }

    #[test]
    fn waiting_for_a_connection_gives_up_instead_of_hanging() {
        let error =
            await_connected(|| false, Duration::from_millis(1), &SystemClock).expect_err("refused");

        assert!(error.to_string().contains("Failed to connect"), "{error}");
    }

    #[test]
    fn a_connection_that_is_already_there_is_not_waited_for() {
        assert!(await_connected(|| true, Duration::from_secs(60), &SystemClock).is_ok());
    }
}
