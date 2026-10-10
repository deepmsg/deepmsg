//! The archive client's context: where it connects, and what it will accept
//! (`aeron_archive_context.c`, 780 lines).
//!
//! # The dialect is the environment
//!
//! A C archive client is configured by **environment variables** and nothing
//! else, and the names are the reference's
//! (`aeron_archive_context.h:23-41`): `AERON_ARCHIVE_CONTROL_CHANNEL` for the
//! archive to talk to, `AERON_ARCHIVE_CONTROL_RESPONSE_CHANNEL` for the channel
//! it wants to be answered on, and ten more with defaults that matter — 10 and
//! 20 for the two stream ids, 30 for the events stream, 10 s for a request's
//! deadline, 3 for its retries, 64 KiB / 1408 / sparse for the control channel's
//! term and MTU.
//!
//! # Reading them is a function of a list, not of the process
//!
//! [`ArchiveContext::resolve`] takes the list, and [`ArchiveContext::from_env`]
//! is the three lines that build one. The split is [`crate::server::config`]'s
//! — a test that sets a process's environment cannot be run beside another that
//! sets it, and one of these tests is about a hundred-and-three-character
//! client name and another is about `9223372036s` meaning what it says.
//!
//! # Nothing here refuses a bad value, and that is the reference's choice
//!
//! `aeron_config_parse_*` **warns and keeps the default** when a value will not
//! parse (`aeron_parse_util.c:650-745`), and clamps a value that parses but is
//! out of range. So a typo is a client that runs with a default, not a client
//! that will not start — and the warning is the only thing that says so, which
//! is why [`ArchiveContext::warnings`] is part of the surface.
//!
//! [`ArchiveContext::conclude`] is where the refusals live, and there are four
//! of them. Three are about what a session cannot be opened without; the fourth
//! is the client name, and it is a **recorded deviation** — see its arm.

use core::fmt;

/// The aeron directory to look for, when nothing says otherwise
/// (`AERON_DIR_ENV_VAR`, `aeronc.h:...`). It is the *driver's* variable, not
/// the archive's, and it is here because a client that owns its own Aeron client
/// is a client that has to find one.
pub const AERON_DIR_ENV: &str = "AERON_DIR";

/// The archive to talk to. **No default**: a client without one has nowhere to
/// send anything (`aeron_archive_context.c:113-120`).
pub const CONTROL_CHANNEL_ENV: &str = "AERON_ARCHIVE_CONTROL_CHANNEL";

/// The stream the requests go out on (`:122-127`), 10 by default — and 10 is
/// the reference *server's* local control stream id too, which is not a
/// coincidence: the number is the protocol's.
pub const CONTROL_STREAM_ID_ENV: &str = "AERON_ARCHIVE_CONTROL_STREAM_ID";

/// The channel the answers come back on. **No default** (`:129-136`).
pub const CONTROL_RESPONSE_CHANNEL_ENV: &str = "AERON_ARCHIVE_CONTROL_RESPONSE_CHANNEL";

/// The stream the answers come back on (`:138-143`), 20 by default.
pub const CONTROL_RESPONSE_STREAM_ID_ENV: &str = "AERON_ARCHIVE_CONTROL_RESPONSE_STREAM_ID";

/// The stream recording lifecycle events are published on (`:145-152`), 30 by
/// default. No default *channel*: an archive that publishes events publishes
/// them where it was told to.
pub const RECORDING_EVENTS_CHANNEL_ENV: &str = "AERON_ARCHIVE_RECORDING_EVENTS_CHANNEL";

/// See [`RECORDING_EVENTS_CHANNEL_ENV`] (`:154-159`).
pub const RECORDING_EVENTS_STREAM_ID_ENV: &str = "AERON_ARCHIVE_RECORDING_EVENTS_STREAM_ID";

/// How long a request may take, as a duration (`:161-166`): `10s`, `500ms`,
/// `1001ns`. Ten seconds, and the same ten the reference *server* gives a
/// connect (`aeron.archive.connect.timeout`) — the two sides time out together.
pub const MESSAGE_TIMEOUT_ENV: &str = "AERON_ARCHIVE_MESSAGE_TIMEOUT";

/// How many times an offer is retried (`:168-173`). Three.
pub const MESSAGE_RETRY_ATTEMPTS_ENV: &str = "AERON_ARCHIVE_MESSAGE_RETRY_ATTEMPTS";

/// The control channel's term length (`:175-180`), a size: `64k`.
pub const CONTROL_TERM_BUFFER_LENGTH_ENV: &str = "AERON_ARCHIVE_CONTROL_TERM_BUFFER_LENGTH";

/// The client's name, which is a *counter's* name where it lands
/// (`:108-111`). Empty by default.
pub const CLIENT_NAME_ENV: &str = "AERON_ARCHIVE_CLIENT_NAME";

/// Whether the control channel's term buffer is sparse (`:189-190`). **True**
/// by default, unlike a data channel's.
pub const CONTROL_TERM_BUFFER_SPARSE_ENV: &str = "AERON_ARCHIVE_CONTROL_TERM_BUFFER_SPARSE";

/// The control channel's MTU (`:182-187`), a size. It defaults to
/// [`crate::client::context::MTU_LENGTH_DEFAULT`] — and *that* default is
/// seeded from `AERON_MTU_LENGTH`, the driver-wide setting, before this one
/// overrides it (`aeron_archive_context.c:92-97`).
pub const CONTROL_MTU_LENGTH_ENV: &str = "AERON_ARCHIVE_CONTROL_MTU_LENGTH";

/// The driver's own MTU, whose value seeds the control MTU's
/// (`aeronc.h`'s `AERON_MTU_LENGTH_ENV_VAR`).
pub const MTU_LENGTH_ENV: &str = "AERON_MTU_LENGTH";

/// The name the **driver** knows this process by — the driver's variable, not
/// the archive's, and the one whose width a counter's field bounds
/// (`aeronc.h:885`). Read by `aeron_client_init` when a client owns its own
/// Aeron client; it defaults to the process's name.
pub const AERON_CLIENT_NAME_ENV: &str = "AERON_CLIENT_NAME";

/// `AERON_COUNTER_MAX_CLIENT_NAME_LENGTH` (`aeron-client/src/main/c/aeronc.h:885`):
/// the width of a counter's client-name field, and the reason a longer name is
/// refused rather than truncated.
pub const MAX_CLIENT_NAME_LENGTH: usize = 100;

/// `AERON_DATA_HEADER_LENGTH (`aeron_udp_protocol.h:187`): the floor a control
/// MTU is clamped to, because a frame has to fit in it.
const DATA_HEADER_LENGTH: i32 = 32;

/// `AERON_MAX_UDP_PAYLOAD_LENGTH` (`aeron_udp_protocol.h:...`): the ceiling.
const MAX_UDP_PAYLOAD_LENGTH: i32 = 65504;

/// The control channel's term length bounds (`aeron_logbuffer_descriptor.h:28-29`),
/// which this workspace already names for the driver's own buffers.
const TERM_MIN_LENGTH: i32 = deepmsg_core::logbuffer::descriptor::TERM_MIN_LENGTH;
/// See [`TERM_MIN_LENGTH`].
const TERM_MAX_LENGTH: i32 = deepmsg_core::logbuffer::descriptor::TERM_MAX_LENGTH;

/// 10 s: `AERON_ARCHIVE_MESSAGE_TIMEOUT_NS_DEFAULT`.
const MESSAGE_TIMEOUT_NS_DEFAULT: i64 = 10 * 1_000_000_000;

/// A control MTU's default when neither variable says otherwise
/// (`AERON_ARCHIVE_CONTROL_MTU_LENGTH_DEFAULT`, `aeron_archive_context.h:92-97`).
const MTU_LENGTH_DEFAULT: i32 = 1408;

/// What a client is configured with.
///
/// The fields are the reference's
/// (`aeron_archive_context_stct`, `aeron_archive_context.h:43-83`), minus the
/// ones that do not exist until the layers above land: the credentials supplier,
/// the recording-signal handler, the error handler, the idle strategy and the
/// Aeron client this context may own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveContext {
    /// The driver to talk to, and the one thing here with a default that is
    /// computed rather than a constant — see [`default_aeron_dir`].
    pub aeron_dir: String,
    /// The name this client tells an *archive* it is, which is part of the
    /// `clientInfo` string the proxy sends with a connect
    /// (`AERON_ARCHIVE_CLIENT_NAME`).
    pub client_name: String,
    /// The name this client tells the *driver* it is (`AERON_CLIENT_NAME`).
    /// Two names and two variables, because they go to two different programs —
    /// and this is the one a counter's field has to hold, so this is the one
    /// [`ArchiveContext::conclude`] refuses when it is too wide.
    pub aeron_client_name: String,
    /// The archive to talk to. `None` until something sets it, and
    /// [`ArchiveContext::conclude`] refuses that.
    pub control_request_channel: Option<String>,
    /// See [`CONTROL_STREAM_ID_ENV`].
    pub control_request_stream_id: i32,
    /// The channel the answers come back on. `None` until something sets it.
    pub control_response_channel: Option<String>,
    /// See [`CONTROL_RESPONSE_STREAM_ID_ENV`].
    pub control_response_stream_id: i32,
    /// Where recording events are published, if anywhere.
    pub recording_events_channel: Option<String>,
    /// See [`RECORDING_EVENTS_STREAM_ID_ENV`].
    pub recording_events_stream_id: i32,
    /// How long a request may take.
    pub message_timeout_ns: i64,
    /// How many times an offer is retried.
    pub message_retry_attempts: u32,
    /// The control channel's term length.
    pub control_term_buffer_length: i32,
    /// The control channel's MTU.
    pub control_mtu_length: i32,
    /// Whether the control channel's term buffer is sparse.
    pub control_term_buffer_sparse: bool,
    /// Every value that would not parse, as `name=value`.
    ///
    /// The reference prints these to stderr as it reads them
    /// (`aeron_config_prop_warning`); here they are kept, because a library that
    /// writes to a process's stderr on construction is a library that has to be
    /// silenced, and because a test that reads them is a test that can say what
    /// the warning is for.
    pub warnings: Vec<String>,
}

impl Default for ArchiveContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ArchiveContext {
    /// A context with every default and nothing read yet.
    ///
    /// `aeron_archive_context_init`: what a client has before it has been told
    /// anything (`aeron_archive_context.c:33-198`).
    #[must_use]
    pub fn new() -> Self {
        Self {
            aeron_dir: default_aeron_dir(),
            client_name: String::new(),
            aeron_client_name: program_name(),
            control_request_channel: None,
            control_request_stream_id: 10,
            control_response_channel: None,
            control_response_stream_id: 20,
            recording_events_channel: None,
            recording_events_stream_id: 30,
            message_timeout_ns: MESSAGE_TIMEOUT_NS_DEFAULT,
            message_retry_attempts: 3,
            control_term_buffer_length: 64 * 1024,
            control_mtu_length: MTU_LENGTH_DEFAULT,
            control_term_buffer_sparse: true,
            warnings: Vec::new(),
        }
    }

    /// What a client is configured with, given a list of `(name, value)` pairs.
    ///
    /// The pairs are the environment's, and the order is the reference's: the
    /// control MTU's *default* is read out of `AERON_MTU_LENGTH` first and the
    /// archive's own variable overrides it after (`:92-97`), so a pair for the
    /// second has to be applied to a context that already read the first.
    #[must_use]
    pub fn resolve(environment: &[(String, String)]) -> Self {
        let get = |name: &str| -> Option<&str> {
            environment
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.as_str())
        };

        let mut context = Self::new();
        let mut warnings = Vec::new();

        if let Some(value) = get(AERON_DIR_ENV).filter(|value| !value.is_empty()) {
            context.aeron_dir = value.to_owned();
        }

        if let Some(value) = get(CLIENT_NAME_ENV) {
            context.client_name = value.to_owned();
        }

        if let Some(value) = get(AERON_CLIENT_NAME_ENV).filter(|value| !value.is_empty()) {
            context.aeron_client_name = value.to_owned();
        }

        context.control_request_channel = channel(get(CONTROL_CHANNEL_ENV));
        context.control_response_channel = channel(get(CONTROL_RESPONSE_CHANNEL_ENV));
        context.recording_events_channel = channel(get(RECORDING_EVENTS_CHANNEL_ENV));

        context.control_request_stream_id = int32(
            get(CONTROL_STREAM_ID_ENV),
            CONTROL_STREAM_ID_ENV,
            context.control_request_stream_id,
            i32::MIN,
            i32::MAX,
            &mut warnings,
        );
        context.control_response_stream_id = int32(
            get(CONTROL_RESPONSE_STREAM_ID_ENV),
            CONTROL_RESPONSE_STREAM_ID_ENV,
            context.control_response_stream_id,
            i32::MIN,
            i32::MAX,
            &mut warnings,
        );
        context.recording_events_stream_id = int32(
            get(RECORDING_EVENTS_STREAM_ID_ENV),
            RECORDING_EVENTS_STREAM_ID_ENV,
            context.recording_events_stream_id,
            i32::MIN,
            i32::MAX,
            &mut warnings,
        );

        // The MTU's default is the driver's own setting where there is one, and
        // the archive's variable is read against *that* default rather than
        // against the constant — which is what makes `AERON_MTU_LENGTH=8192`
        // alone change the control channel too.
        let mtu_seed = size64(
            get(MTU_LENGTH_ENV),
            MTU_LENGTH_ENV,
            context.control_mtu_length,
            DATA_HEADER_LENGTH,
            MAX_UDP_PAYLOAD_LENGTH,
            &mut warnings,
        );

        context.control_mtu_length = size64(
            get(CONTROL_MTU_LENGTH_ENV),
            CONTROL_MTU_LENGTH_ENV,
            mtu_seed,
            DATA_HEADER_LENGTH,
            MAX_UDP_PAYLOAD_LENGTH,
            &mut warnings,
        );

        context.control_term_buffer_length = size64(
            get(CONTROL_TERM_BUFFER_LENGTH_ENV),
            CONTROL_TERM_BUFFER_LENGTH_ENV,
            context.control_term_buffer_length,
            TERM_MIN_LENGTH,
            TERM_MAX_LENGTH,
            &mut warnings,
        );

        context.message_timeout_ns = duration_ns(
            get(MESSAGE_TIMEOUT_ENV),
            MESSAGE_TIMEOUT_ENV,
            context.message_timeout_ns,
            1_000,
            i64::MAX,
            &mut warnings,
        );

        // Not a mistake: the reference reads the retry count with the *duration*
        // reader (`:168-173`), so `3`, `3s` and `3000ms` all mean three. The two
        // it lands between are the ones the field can hold.
        context.message_retry_attempts = u32::try_from(duration_ns(
            get(MESSAGE_RETRY_ATTEMPTS_ENV),
            MESSAGE_RETRY_ATTEMPTS_ENV,
            i64::from(context.message_retry_attempts),
            0,
            i64::from(i32::MAX),
            &mut warnings,
        ))
        .unwrap_or(0);

        context.control_term_buffer_sparse = boolean(
            get(CONTROL_TERM_BUFFER_SPARSE_ENV),
            context.control_term_buffer_sparse,
        );

        context.warnings = warnings;
        context
    }

    /// The same, from this process's environment.
    ///
    /// The three lines that turn a process into a list, and the only place here
    /// that reads one.
    #[must_use]
    pub fn from_env() -> Self {
        let names = [
            AERON_DIR_ENV,
            CLIENT_NAME_ENV,
            AERON_CLIENT_NAME_ENV,
            CONTROL_CHANNEL_ENV,
            CONTROL_STREAM_ID_ENV,
            CONTROL_RESPONSE_CHANNEL_ENV,
            CONTROL_RESPONSE_STREAM_ID_ENV,
            RECORDING_EVENTS_CHANNEL_ENV,
            RECORDING_EVENTS_STREAM_ID_ENV,
            MTU_LENGTH_ENV,
            CONTROL_MTU_LENGTH_ENV,
            CONTROL_TERM_BUFFER_LENGTH_ENV,
            MESSAGE_TIMEOUT_ENV,
            MESSAGE_RETRY_ATTEMPTS_ENV,
            CONTROL_TERM_BUFFER_SPARSE_ENV,
        ];

        let environment: Vec<(String, String)> = names
            .iter()
            .filter_map(|name| {
                std::env::var(name)
                    .ok()
                    .map(|value| ((*name).to_owned(), value))
            })
            .collect();

        Self::resolve(&environment)
    }

    /// Whether this context is one a session can be opened with.
    ///
    /// `aeron_archive_context_conclude` (`:312-452`), which is the first half of
    /// it: the four refusals. What it does *after* the refusals — create the
    /// Aeron client, write the channel defaults and mint the session id — is
    /// P2-C1's next commit, and it needs a driver.
    ///
    /// # Errors
    ///
    /// [`ClientError`], with the reference's own reason text in every arm.
    pub fn conclude(&self) -> Result<(), ClientError> {
        if self.control_request_channel.is_none() {
            return Err(ClientError::ControlRequestChannelRequired);
        }

        if self.control_response_channel.is_none() {
            return Err(ClientError::ControlResponseChannelRequired);
        }

        if 0 == self.message_retry_attempts {
            return Err(ClientError::RetryAttemptsMustBePositive);
        }

        // **A recorded deviation.** The reference does not check this here: it
        // hands the name to `aeron_async_add_*`, and the *driver* refuses a name
        // wider than a counter's field, with this text and
        // `AERON_ERROR_CODE_INVALID_ARGUMENT` (`aeron_counter.c`'s
        // `aeron_counter_validate_client_name`). A C client therefore learns
        // about it by starting a driver round trip. This build's client carries
        // no client name to the driver yet, so the same rule is applied where
        // the same caller meets it — at `conclude`, before anything is created —
        // and the thing it should have refused is refused.
        if self.aeron_client_name.len() > MAX_CLIENT_NAME_LENGTH {
            return Err(ClientError::ClientNameTooLong {
                length: self.aeron_client_name.len(),
            });
        }

        Ok(())
    }
}

/// Why a context is not one a session can be opened with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// No `AERON_ARCHIVE_CONTROL_CHANNEL`, so there is nowhere to send.
    ControlRequestChannelRequired,
    /// No `AERON_ARCHIVE_CONTROL_RESPONSE_CHANNEL`, so nowhere to be answered.
    ControlResponseChannelRequired,
    /// `AERON_ARCHIVE_MESSAGE_RETRY_ATTEMPTS=0`, which is an offer that is never
    /// sent.
    RetryAttemptsMustBePositive,
    /// A client name wider than a counter's field.
    ClientNameTooLong {
        /// How wide the name is.
        length: usize,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ControlRequestChannelRequired => {
                write!(f, "control request channel is required")
            }
            Self::ControlResponseChannelRequired => {
                write!(f, "control response channel is required")
            }
            Self::RetryAttemptsMustBePositive => {
                write!(f, "message_retry_attempts must be > 0")
            }
            Self::ClientNameTooLong { length } => write!(
                f,
                "client_name length must <= {MAX_CLIENT_NAME_LENGTH}, was {length}"
            ),
        }
    }
}

impl std::error::Error for ClientError {}

/// Where a client looks for a driver when nothing says.
///
/// `aeron_default_path` (`aeron_utility.c`): `/dev/shm/aeron-<user>` on Linux,
/// the user's home under a temporary directory elsewhere. The same default
/// `crates/tools/src/bin/cnc-dump.rs:102-114` computes, and it is here rather
/// than shared with it because that one is a binary that reads one argument and
/// this one is a library that must not print.
#[must_use]
pub fn default_aeron_dir() -> String {
    let user = std::env::var("USER").unwrap_or_else(|_| "default".to_owned());

    if cfg!(target_os = "linux") && std::path::Path::new("/dev/shm").is_dir() {
        return format!("/dev/shm/aeron-{user}");
    }

    std::env::temp_dir()
        .join(format!("aeron-{user}"))
        .to_string_lossy()
        .into_owned()
}

/// What the process is called, which is what the driver calls a client that was
/// not named (`aeron_client_init`'s default).
fn program_name() -> String {
    std::env::args()
        .next()
        .and_then(|path| {
            std::path::Path::new(&path)
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_default()
}

/// A channel, which is `None` when it was never set and *also* when it was set
/// to nothing — a client that exports an empty variable means the same as one
/// that does not export it at all.
fn channel(value: Option<&str>) -> Option<String> {
    value.filter(|value| !value.is_empty()).map(str::to_owned)
}

/// `aeron_config_parse_int32` (`aeron_parse_util.c:650-670`).
///
/// Base 0, so `0x10` is sixteen (the reference's `strtoll(.., 0)`); a value that
/// does not parse at all, or does not fit an `i32`, is the default *and* a
/// warning; and the result is clamped either way — the default included.
fn int32(
    value: Option<&str>,
    name: &str,
    default: i32,
    min: i32,
    max: i32,
    warnings: &mut Vec<String>,
) -> i32 {
    let Some(value) = value else {
        return default;
    };

    // A Rust `i64::from_str_radix` with radix 0 does not exist, and the two
    // spellings the reference accepts by hand are decimal and `0x`.
    let parsed = if let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        i64::from_str_radix(hex, 16)
    } else {
        value.parse::<i64>()
    };

    let Ok(parsed) = parsed else {
        warnings.push(format!("{name}={value}"));
        return default.clamp(min, max);
    };

    if parsed < i64::from(i32::MIN) || parsed > i64::from(i32::MAX) {
        warnings.push(format!("{name}={value}"));
        return default.clamp(min, max);
    }

    i32::try_from(parsed).unwrap_or(default).clamp(min, max)
}

/// `aeron_config_parse_size64` (`aeron_parse_util.c:701-745`), clamped to an
/// `i32` because every size this context holds is one.
fn size64(
    value: Option<&str>,
    name: &str,
    default: i32,
    min: i32,
    max: i32,
    warnings: &mut Vec<String>,
) -> i32 {
    let Some(value) = value else {
        return default;
    };

    let Some(parsed) = deepmsg_core::uri::parse_size(value) else {
        warnings.push(format!("{name}={value}"));
        return default.clamp(min, max);
    };

    parsed.clamp(min, max)
}

/// `aeron_config_parse_duration_ns` (`aeron_parse_util.c:724-745`): a duration
/// with an agrona suffix — `10s`, `500ms`, `1001ns` — or bare nanoseconds.
///
/// The bounds are the caller's and they are *clamps*, not refusals: the
/// reference's own default for a request's deadline is 10 s and its floor is
/// 1000 ns, so asking for `1ns` gets a microsecond and asking for nothing gets
/// ten seconds.
fn duration_ns(
    value: Option<&str>,
    name: &str,
    default: i64,
    min: i64,
    max: i64,
    warnings: &mut Vec<String>,
) -> i64 {
    let Some(value) = value else {
        return default;
    };

    let parsed = duration_ns_of(value).or_else(|| {
        warnings.push(format!("{name}={value}"));
        None
    });

    let Some(parsed) = parsed else {
        return default.clamp(min, max);
    };

    parsed.clamp(min, max)
}

/// The grammar itself: digits, then one of `ns` / `us` / `ms` / `s`.
///
/// The suffixes are read longest-first, and that is not a detail: `100ms` ends
/// in `s` as well, so a reader that tried `s` first would answer ten microseconds
/// for a hundred milliseconds.
fn duration_ns_of(value: &str) -> Option<i64> {
    let (digits, scale) = if let Some(digits) = value.strip_suffix("ns") {
        (digits, 1_i64)
    } else if let Some(digits) = value.strip_suffix("us") {
        (digits, 1_000)
    } else if let Some(digits) = value.strip_suffix("ms") {
        (digits, 1_000_000)
    } else if let Some(digits) = value.strip_suffix('s') {
        (digits, 1_000_000_000)
    } else {
        (value, 1)
    };

    // Negative never parses: the reference's reader takes an `int64`, finds it
    // below zero and gives up (`aeron_parse_util.c:186-189`).
    let number: u64 = digits.parse().ok()?;
    let scaled = number.checked_mul(u64::try_from(scale).ok()?)?;

    i64::try_from(scaled).ok()
}

/// `aeron_parse_bool` (`aeron_parse_util.c:332-346`): `1`/`on`/`true`, or
/// `0`/`off`/`false`, by **prefix** — so `trueish` is true — and anything else is
/// the default.
///
/// A bad value is not a warning here: the reference does not warn for this one
/// (`:346` returns the default silently), and a `sparse=tru` that silently means
/// "default" is one of the few places the two disagree.
fn boolean(value: Option<&str>, default: bool) -> bool {
    let Some(value) = value else {
        return default;
    };

    if value.starts_with('1') || value.starts_with("on") || value.starts_with("true") {
        return true;
    }

    if value.starts_with('0') || value.starts_with("off") || value.starts_with("false") {
        return false;
    }

    default
}
