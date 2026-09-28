//! Driver configuration: the settings the driver reads before it does
//! anything else, and the names it answers to.
//!
//! # Names
//!
//! Each setting has one *property* name and one *environment* name. The
//! property name is the reference's minus its `aeron.` prefix — `aeron.dir`
//! becomes `deepmsg.dir` — and both dialects are accepted, so:
//!
//! ```text
//! -Ddeepmsg.dir=/tmp/aeron      # deepmsg's own name
//! -Daeron.dir=/tmp/aeron        # the reference's, for a driver dropped
//! DEEPMSG_DIR=/tmp/aeron        # into an existing deployment
//! AERON_DIR=/tmp/aeron
//! ```
//!
//! Within one dialect a `-D` argument beats the environment, and across the two
//! the **whole deepmsg chain beats the whole reference chain** — so
//! `DEEPMSG_DIR` beats `-Daeron.dir`. That is the precedence the lookup table
//! implements and the one a one-off override wants: the deployment sets the
//! reference's names, and a deepmsg name is how somebody overrides them. Unknown properties are ignored rather than
//! rejected, because a reference deployment's configuration file will carry
//! settings this driver has no use for yet, and refusing to start over one of
//! them would make the alias worse than useless.
//!
//! # Where the values are parsed
//!
//! The reference parses every value by hand
//! (`aeron-client/src/main/c/util/aeron_parse_util.c`), including size
//! suffixes (`:42-105`: `k`, `m`, `g`, binary multiples) and duration suffixes
//! (`:170-268`: `s`, `ms`, `us`, `ns`). Those are reproduced, because a
//! deployment that writes `-Daeron.to.conductor.buffer.length=4m` means 4 MiB
//! and would get a driver that refused to start if this parsed plain integers.
//!
//! **One deliberate divergence.** The reference *warns and uses the default*
//! for a value it cannot parse, and *clamps* one that is out of range
//! (`aeron_parse_util.c:701-722`). This refuses instead. A driver that starts
//! with a buffer length nobody asked for is a driver whose CnC file does not
//! match what the deployment believes it configured, and the failure would
//! surface much later as a capacity that is subtly wrong. A bad value here is
//! a start-up error, and the error names the setting.

use std::path::PathBuf;

use deepmsg_cnc::{CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT, CncCreateError, CncLayout};

use crate::publication_params;
use crate::sys::{self, SocketBufferLengths};

/// Timer interval default: one second (`aeron-driver/src/main/c/aeron_driver_context.h:194`,
/// assigned from `AERON_TIMER_INTERVAL_NS_DEFAULT` at `aeron_driver_context.c:470`).
pub const TIMER_INTERVAL_NS_DEFAULT: i64 = 1_000_000_000;

/// How stale another driver's heartbeat may be before this one treats the
/// directory as abandoned: `AERON_DRIVER_TIMEOUT_MS_DEFAULT (10 * 1000)`
/// (`aeron-driver/src/main/c/aeron_driver_context.c:217`).
pub const DRIVER_TIMEOUT_MS_DEFAULT: i64 = 10 * 1000;

/// The largest tier period this driver accepts: one hour.
///
/// Not the reference's limit — it has none — but ours, and for a reason the
/// reference gets away with because it clamps everywhere and this does not: a
/// deadline is `now + period`, and a period near `i64::MAX` makes that sum
/// wrap. One hour is a cadence no deployment asks for deliberately.
pub const MAX_TIMER_INTERVAL_NS: i64 = 60 * 60 * 1_000_000_000;

/// How long a reclaimed counter stays out of reuse: one second, the reference's
/// `AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_NS_DEFAULT`
/// (`aeron-driver/src/main/c/aeron_driver_context.c:211`).
///
/// It is a *floor* on how soon an id can come back, not a delay before a
/// reclaimed slot is readable again: the slot is unusable the moment its state
/// turns `RECLAIMED`, and this only says how long it must stay that way.
pub const COUNTER_FREE_TO_REUSE_NS_DEFAULT: i64 = 1_000_000_000;

/// The length of one term of an IPC publication's log buffer: 64 MiB
/// (`aeron.ipc.term.buffer.length`, `aeron-driver/src/main/c/aeron_driver_context.c:180`).
///
/// Sixty-four *mega*, not kilo: a log buffer is three terms and a metadata
/// page, so one IPC publication of the default size is a 192 MiB file. It is
/// the term length a URI's `term-length` replaces, and the one
/// `AERON_IPC_TERM_BUFFER_LENGTH` overrides for a deployment that wants
/// smaller — which is what a test that creates publications wants.
pub const IPC_TERM_BUFFER_LENGTH_DEFAULT: i32 = 64 * 1024 * 1024;

/// `AERON_TERM_BUFFER_LENGTH_DEFAULT`
/// (`aeron-driver/src/main/c/aeron_driver_context.c:179`).
pub const TERM_BUFFER_LENGTH_DEFAULT: i32 = 16 * 1024 * 1024;

/// `AERON_MTU_LENGTH_DEFAULT` (`aeron_driver_context.c:186`).
pub const MTU_LENGTH_DEFAULT: i32 = 1408;

/// `aeron.publication.term.window.length`'s default: zero, which means half a
/// term (`aeron_driver_context.c:189`).
pub const PUBLICATION_WINDOW_LENGTH_DEFAULT: i32 = 0;

/// `AERON_SOCKET_SO_RCVBUF_DEFAULT` (`aeron_driver_context.c:191`).
pub const SOCKET_SO_RCVBUF_DEFAULT: i32 = 128 * 1024;

/// `AERON_SOCKET_SO_SNDBUF_DEFAULT` (`aeron_driver_context.c:192`): zero, and
/// for a *send* buffer that is not a mistake — the kernel's default is what
/// the socket gets unless a channel asks for more.
pub const SOCKET_SO_SNDBUF_DEFAULT: i32 = 0;

/// `AERON_RCV_INITIAL_WINDOW_LENGTH_DEFAULT` (`aeron_driver_context.c:205`).
pub const RCV_INITIAL_WINDOW_LENGTH_DEFAULT: i32 = 128 * 1024;

/// `AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_DEFAULT`
/// (`aeron_driver_context.c:242`), which is clamped to
/// [`crate::media::udp_transport`]'s sixteen by the context setter
/// (`:3378-3384`).
pub const NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_DEFAULT: usize = 4;

/// `AERON_SEND_TO_STATUS_POLL_RATIO_DEFAULT` (`aeron_driver_context.c:199`).
pub const SEND_TO_STATUS_POLL_RATIO_DEFAULT: u8 = 6;

/// `AERON_PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT`
/// (`aeron_driver_context.c:208`).
pub const PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT: i64 = 5_000_000_000;

/// `AERON_RETRANSMIT_UNICAST_DELAY_NS_DEFAULT` (`aeron_driver_context.c:218`).
pub const RETRANSMIT_UNICAST_DELAY_NS_DEFAULT: i64 = 0;

/// `AERON_RETRANSMIT_UNICAST_LINGER_NS_DEFAULT`
/// (`aeron_driver_context.c:219`).
pub const RETRANSMIT_UNICAST_LINGER_NS_DEFAULT: i64 = 10_000_000;

/// `AERON_RETRANSMIT_HANDLER_MAX_RESEND` (`aeron_driver_context.c:499`).
pub const MAX_RESEND_DEFAULT: i32 = 16;

/// `AERON_DRIVER_STREAM_SESSION_LIMIT_DEFAULT`
/// (`aeron_driver_context.c:249`): no limit.
#[allow(clippy::cast_sign_loss)] // the reference's own INT32_MAX default
pub const STREAM_SESSION_LIMIT_DEFAULT: usize = i32::MAX as usize;

/// The largest frame an IPC publication writes: 1408 bytes
/// (`aeron.ipc.mtu.length`, `aeron_driver_context.c:187`).
///
/// Thirty-two bytes larger than the UDP default's payload because the IPC
/// fixed header is what it is, and it is written into the log buffer's
/// metadata where a subscriber reads its own MTU from.
pub const IPC_MTU_LENGTH_DEFAULT: i32 = 1408;

/// How far ahead of its slowest reader an IPC producer may run: zero, which
/// means half a term (`aeron.ipc.publication.term.window.length`,
/// `aeron_driver_context.h:216`).
pub const IPC_PUBLICATION_WINDOW_LENGTH_DEFAULT: i32 = 0;

/// How long a drained publication lingers before it is closed: five seconds
/// (`aeron.publication.linger.timeout`, `aeron_driver_context.h:189`).
pub const PUBLICATION_LINGER_TIMEOUT_NS_DEFAULT: i64 = 5_000_000_000;

/// How long a subscription may fail to keep up before the publication stops
/// counting it towards the limit: five seconds
/// (`aeron.untethered.window.limit.timeout`, `aeron_driver_context.h:196`).
pub const UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_DEFAULT: i64 = 5_000_000_000;

/// The same for the lingering half of the tether cycle: unset, which means
/// "the window limit timeout" (`aeron.untethered.linger.timeout`,
/// `aeron_driver_context.h:197`).
///
/// `-1` is the reference's `AERON_NULL_VALUE`, and it is a value rather than an
/// `Option` because it is a byte in every log buffer's metadata: the fallback
/// happens while the publication is created, not here.
pub const UNTETHERED_LINGER_TIMEOUT_NS_DEFAULT: i64 = -1;

/// And the resting half: ten seconds (`aeron.untethered.resting.timeout`,
/// `aeron_driver_context.h:198`).
pub const UNTETHERED_RESTING_TIMEOUT_NS_DEFAULT: i64 = 10_000_000_000;

/// The bottom of the session id range the driver keeps for itself: `-1`
/// (`aeron.publication.reserved.session.id.low`, `aeron_driver_context.c:229`).
pub const PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT: i32 = -1;

/// And the top: `1000` (`:230`).
///
/// The range exists so that a session id a *client* invents cannot collide
/// with one the driver hands out, and so that `session-id=-1` — which a client
/// that does not care about sessions sends — is never a real session.
pub const PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT: i32 = 1000;

/// Whether a log buffer is left sparse: **true**
/// (`aeron.term.buffer.sparse.file`, `aeron_driver_context.c:181`).
///
/// True means the file system fills the file with zeros as it is read, and the
/// driver does not touch the pages it allocated. It is a byte in the log
/// buffer's metadata as well as a behaviour, so a driver that wrote `false`
/// would differ from the reference in the file *and* in what a reader of the
/// file is told about it.
pub const TERM_BUFFER_SPARSE_FILE_DEFAULT: bool = true;

/// Whether creating a log buffer is preceded by a space check: **true**
/// (`perform.storage.checks`, `aeron_driver_context.c:182`, set at `:457`).
pub const PERFORM_STORAGE_CHECKS_DEFAULT: bool = true;

/// The free space below which the reference records a low-space warning:
/// ten term buffer lengths, at the reference's default term length of
/// 16 MiB (`low.file.store.warning.threshold`,
/// `aeron_driver_context.c:179` and `:183`).
pub const LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT: u64 = 10 * 16 * 1024 * 1024;

/// What the driver does with a `TERMINATE_DRIVER` command.
///
/// The reference has no compiled-in answer: it loads one of two functions by
/// name (`aeron-driver/src/main/c/aeron_termination_validator.c:28-40`),
/// defaulting to `deny` (`aeron_driver_context.c:1247-1251`). Denying by
/// default is the safety property — a stray client must not be able to stop a
/// production driver — and it is what `aeronmd` ships with, so a driver that
/// accepted termination out of the box would be the surprising one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminationPolicy {
    /// Run the termination hook.
    Allow,
    /// Refuse every request, whatever token it carries.
    Deny,
}

impl TerminationPolicy {
    /// Parse the validator name, exactly as the reference's symbol table does:
    /// two names, anything else is a start-up failure
    /// (`aeron_termination_validator.c:55-62` returns null, and
    /// `aeron_driver_context.c:1247-1251` fails initialisation on null).
    fn parse(name: &str) -> Result<Self, ConfigError> {
        match name {
            "allow" => Ok(Self::Allow),
            "deny" => Ok(Self::Deny),
            other => Err(ConfigError::UnknownValidator {
                value: other.to_owned(),
            }),
        }
    }

    /// The name this policy is configured by.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
        }
    }
}

/// Everything the driver needs to know before it touches the file system.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverConfig {
    /// Where the CnC file, the term buffers and the subdirectories live.
    /// Mandatory: the reference has no default for it either.
    pub aeron_dir: PathBuf,
    /// Delete the directory before creating it, rather than checking it.
    pub dirs_delete_on_start: bool,
    /// Delete the directory on a clean shutdown.
    pub dirs_delete_on_shutdown: bool,
    /// Print a warning when the directory already exists.
    pub warn_if_dirs_exist: bool,
    /// What to do with a termination request.
    pub termination: TerminationPolicy,
    /// The CnC file's region lengths.
    pub layout: CncLayout,
    /// How long a client may go unheard before it is reaped; written into the
    /// CnC metadata for clients to read.
    pub client_liveness_timeout_ns: i64,
    /// How often the conductor's timeout tier runs.
    pub timer_interval_ns: i64,
    /// How old a heartbeat may be before another driver may take the
    /// directory over. The liveness window this driver checks *other* drivers
    /// against; it is not what it asks of its clients.
    pub driver_timeout_ms: i64,
    /// How long a reclaimed counter stays out of reuse, so a client holding a
    /// stale id cannot read a fresh counter as if it were the old one
    /// (`aeron.counters.free.to.reuse.timeout`, one second by default).
    pub counter_free_to_reuse_ns: i64,
    /// The length of one term of an IPC publication's log buffer
    /// (`aeron.ipc.term.buffer.length`, see
    /// [`IPC_TERM_BUFFER_LENGTH_DEFAULT`]).
    pub ipc_term_buffer_length: i32,
    /// The largest frame an IPC publication writes
    /// (`aeron.ipc.mtu.length`, see [`IPC_MTU_LENGTH_DEFAULT`]).
    pub ipc_mtu_length: i32,
    /// How far ahead of its slowest reader an IPC producer may run
    /// (`aeron.ipc.publication.term.window.length`; zero means half a term,
    /// see [`IPC_PUBLICATION_WINDOW_LENGTH_DEFAULT`]).
    pub ipc_publication_window_length: i32,
    /// How long a drained publication lingers
    /// (`aeron.publication.linger.timeout`).
    pub publication_linger_timeout_ns: i64,
    /// How long a subscription may fail to keep up before the publication
    /// stops counting it (`aeron.untethered.window.limit.timeout`).
    pub untethered_window_limit_timeout_ns: i64,
    /// The same for the lingering half of the tether cycle; `-1` means "the
    /// window limit timeout" (`aeron.untethered.linger.timeout`).
    pub untethered_linger_timeout_ns: i64,
    /// And for the resting half (`aeron.untethered.resting.timeout`).
    pub untethered_resting_timeout_ns: i64,
    /// Whether a log buffer is left sparse
    /// (`aeron.term.buffer.sparse.file`, true by default).
    pub term_buffer_sparse_file: bool,
    /// Whether a log buffer is refused before it is created when the
    /// filesystem it will land on cannot hold it
    /// (`perform.storage.checks`, true by default).
    pub perform_storage_checks: bool,
    /// The usable space below which a low-space warning would be recorded
    /// (`low.file.store.warning.threshold`, ten default term lengths).
    ///
    /// The warning itself waits for the driver's error log, so until that
    /// arrives this names a level nothing yet acts on — the refusals the
    /// check above makes do not need it.
    pub low_file_store_warning_threshold: u64,
    /// The session ids the driver keeps for itself, low end
    /// (`aeron.publication.reserved.session.id.low`).
    pub publication_reserved_session_id_low: i32,
    /// ...and the high end (`aeron.publication.reserved.session.id.high`).
    pub publication_reserved_session_id_high: i32,
    /// What a fresh socket reports as its receive and send buffer sizes.
    ///
    /// These are four of the fields every log buffer's metadata carries
    /// (`aeron-driver/src/main/c/aeron_ipc_publication.c:117-124`), so they are
    /// **not** zeroes and they are not this driver's to choose: the reference
    /// probes the kernel at start-up
    /// (`aeron-client/src/main/c/util/aeron_netutil.c:883-919`) and writes
    /// what it is told. A probe that fails leaves zeroes, which is what a
    /// process that cannot make a socket would have written anyway.
    pub socket_buffers: SocketBufferLengths,
    /// The length of one term of a *network* publication's log buffer
    /// (`aeron.term.buffer.length`, [`TERM_BUFFER_LENGTH_DEFAULT`]).
    ///
    /// Sixteen megabytes, a quarter of the IPC default: a network term is
    /// bounded by what a receiver can hold and by how long a term takes to
    /// fill at the link's rate, neither of which shared memory is.
    pub term_buffer_length: i32,
    /// The largest frame a network publication writes (`aeron.mtu.length`,
    /// [`MTU_LENGTH_DEFAULT`]). It is the datagram size, so it is also what a
    /// batch of frames is cut into.
    pub mtu_length: i32,
    /// How far ahead of its slowest reader a network producer may run
    /// (`aeron.publication.term.window.length`; zero means half a term).
    pub publication_window_length: i32,
    /// `SO_RCVBUF` for a channel that named none (`aeron.socket.so.rcvbuf`,
    /// [`SOCKET_SO_RCVBUF_DEFAULT`]).
    pub socket_so_rcvbuf: i32,
    /// `SO_SNDBUF`, likewise (`aeron.socket.so.sndbuf`; zero leaves the
    /// kernel's default, which is a socket *sending* into a local buffer).
    pub socket_so_sndbuf: i32,
    /// The window a subscription offers a publication when the channel named
    /// none (`aeron.receiver.window.length`,
    /// [`RCV_INITIAL_WINDOW_LENGTH_DEFAULT`]).
    pub receiver_window_length: i32,
    /// How many datagrams one send batch may carry
    /// (`aeron.network.publication.max.messages.per.send`, default four, one
    /// to sixteen).
    pub network_publication_max_messages_per_send: usize,
    /// How many sender passes go by between two polls of the control sockets
    /// (`aeron.send.to.sm.poll.ratio`, [`SEND_TO_STATUS_POLL_RATIO_DEFAULT`]).
    pub send_to_sm_poll_ratio: u8,
    /// How long a publication waits for a status message before it decides its
    /// receivers are gone (`aeron.publication.connection.timeout`, five
    /// seconds).
    pub publication_connection_timeout_ns: i64,
    /// How long a NAK's answer waits before it is sent
    /// (`aeron.retransmit.unicast.delay`; zero answers at once, which is the
    /// default).
    pub retransmit_unicast_delay_ns: i64,
    /// How long a served NAK lingers before its slot may be reused
    /// (`aeron.retransmit.unicast.linger`, ten milliseconds).
    pub retransmit_unicast_linger_ns: i64,
    /// How many times a term may be retransmitted before the publication gives
    /// up on it (`aeron.max.resend`, sixteen).
    pub max_resend: i32,
    /// How many sessions one stream may have before the driver stops making
    /// images for it (`aeron.stream.session.limit`; the reference's default is
    /// no limit at all, `INT32_MAX`,
    /// `aeron-driver/src/main/c/aeron_driver_context.c:249`).
    pub stream_session_limit: usize,
}

impl Default for DriverConfig {
    /// The reference's defaults, with the one setting it has no default for
    /// left empty: [`DriverConfig::aeron_dir`].
    fn default() -> Self {
        Self {
            aeron_dir: PathBuf::new(),
            dirs_delete_on_start: false,
            dirs_delete_on_shutdown: false,
            warn_if_dirs_exist: false,
            termination: TerminationPolicy::Deny,
            layout: CncLayout::default(),
            client_liveness_timeout_ns: CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT,
            timer_interval_ns: TIMER_INTERVAL_NS_DEFAULT,
            driver_timeout_ms: DRIVER_TIMEOUT_MS_DEFAULT,
            counter_free_to_reuse_ns: COUNTER_FREE_TO_REUSE_NS_DEFAULT,
            ipc_term_buffer_length: IPC_TERM_BUFFER_LENGTH_DEFAULT,
            ipc_mtu_length: IPC_MTU_LENGTH_DEFAULT,
            ipc_publication_window_length: IPC_PUBLICATION_WINDOW_LENGTH_DEFAULT,
            publication_linger_timeout_ns: PUBLICATION_LINGER_TIMEOUT_NS_DEFAULT,
            untethered_window_limit_timeout_ns: UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_DEFAULT,
            untethered_linger_timeout_ns: UNTETHERED_LINGER_TIMEOUT_NS_DEFAULT,
            untethered_resting_timeout_ns: UNTETHERED_RESTING_TIMEOUT_NS_DEFAULT,
            term_buffer_sparse_file: TERM_BUFFER_SPARSE_FILE_DEFAULT,
            perform_storage_checks: PERFORM_STORAGE_CHECKS_DEFAULT,
            low_file_store_warning_threshold: LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT,
            publication_reserved_session_id_low: PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT,
            publication_reserved_session_id_high: PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT,
            socket_buffers: SocketBufferLengths {
                rcvbuf: 0,
                sndbuf: 0,
            },
            term_buffer_length: TERM_BUFFER_LENGTH_DEFAULT,
            mtu_length: MTU_LENGTH_DEFAULT,
            publication_window_length: PUBLICATION_WINDOW_LENGTH_DEFAULT,
            socket_so_rcvbuf: SOCKET_SO_RCVBUF_DEFAULT,
            socket_so_sndbuf: SOCKET_SO_SNDBUF_DEFAULT,
            receiver_window_length: RCV_INITIAL_WINDOW_LENGTH_DEFAULT,
            network_publication_max_messages_per_send:
                NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_DEFAULT,
            send_to_sm_poll_ratio: SEND_TO_STATUS_POLL_RATIO_DEFAULT,
            publication_connection_timeout_ns: PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT,
            retransmit_unicast_delay_ns: RETRANSMIT_UNICAST_DELAY_NS_DEFAULT,
            retransmit_unicast_linger_ns: RETRANSMIT_UNICAST_LINGER_NS_DEFAULT,
            max_resend: MAX_RESEND_DEFAULT,
            stream_session_limit: STREAM_SESSION_LIMIT_DEFAULT,
        }
    }
}

impl DriverConfig {
    /// Read the configuration from `args` (which may include `-D` properties)
    /// and the process environment.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for a malformed argument, an unparsable value, a value
    /// the reference would reject, or a missing aeron directory.
    pub fn from_args<I>(args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = String>,
    {
        let properties = parse_properties(args)?;
        Self::resolve(&properties, &|name| std::env::var(name).ok())
    }

    /// The testable core of [`DriverConfig::from_args`]: properties and an
    /// environment lookup, with no globals involved.
    ///
    /// # Errors
    ///
    /// As [`DriverConfig::from_args`].
    pub fn resolve(
        properties: &[(String, String)],
        env: &impl Fn(&str) -> Option<String>,
    ) -> Result<Self, ConfigError> {
        let get = |setting: &Setting| lookup(properties, env, setting);

        let aeron_dir = get(&Setting::DIR).ok_or(ConfigError::MissingAeronDir {
            property: Setting::DIR.property,
            env: Setting::DIR.env,
        })?;

        let mut config = Self {
            aeron_dir: PathBuf::from(aeron_dir),
            ..Self::default()
        };

        if let Some(value) = get(&Setting::DIR_DELETE_ON_START) {
            config.dirs_delete_on_start = parse_bool(&Setting::DIR_DELETE_ON_START, &value)?;
        }
        if let Some(value) = get(&Setting::DIR_DELETE_ON_SHUTDOWN) {
            config.dirs_delete_on_shutdown = parse_bool(&Setting::DIR_DELETE_ON_SHUTDOWN, &value)?;
        }
        if let Some(value) = get(&Setting::DIR_WARN_IF_EXISTS) {
            config.warn_if_dirs_exist = parse_bool(&Setting::DIR_WARN_IF_EXISTS, &value)?;
        }
        if let Some(value) = get(&Setting::TERMINATION_VALIDATOR) {
            config.termination = TerminationPolicy::parse(&value)?;
        }

        if let Some(value) = get(&Setting::TO_CONDUCTOR_BUFFER_LENGTH) {
            config.layout.to_driver_length =
                parse_size64(&Setting::TO_CONDUCTOR_BUFFER_LENGTH, &value)?;
        }
        if let Some(value) = get(&Setting::TO_CLIENTS_BUFFER_LENGTH) {
            config.layout.to_clients_length =
                parse_size64(&Setting::TO_CLIENTS_BUFFER_LENGTH, &value)?;
        }
        if let Some(value) = get(&Setting::COUNTERS_VALUES_BUFFER_LENGTH) {
            config.layout.counters_values_length =
                parse_size64(&Setting::COUNTERS_VALUES_BUFFER_LENGTH, &value)?;
        }
        if let Some(value) = get(&Setting::ERROR_BUFFER_LENGTH) {
            config.layout.error_log_length = parse_size64(&Setting::ERROR_BUFFER_LENGTH, &value)?;
        }
        if let Some(value) = get(&Setting::FILE_PAGE_SIZE) {
            config.layout.page_size = parse_size64(&Setting::FILE_PAGE_SIZE, &value)?;
        }
        if let Some(value) = get(&Setting::CLIENT_LIVENESS_TIMEOUT) {
            config.client_liveness_timeout_ns =
                parse_duration_ns(&Setting::CLIENT_LIVENESS_TIMEOUT, &value)?;
        }
        if let Some(value) = get(&Setting::TIMER_INTERVAL) {
            config.timer_interval_ns = parse_duration_ns(&Setting::TIMER_INTERVAL, &value)?;
        }
        if let Some(value) = get(&Setting::DRIVER_TIMEOUT) {
            config.driver_timeout_ms = parse_count(&Setting::DRIVER_TIMEOUT, &value)?;
        }
        if let Some(value) = get(&Setting::COUNTER_FREE_TO_REUSE_TIMEOUT) {
            config.counter_free_to_reuse_ns =
                parse_duration_ns(&Setting::COUNTER_FREE_TO_REUSE_TIMEOUT, &value)?;
        }

        // The publication settings: what a channel URI's parameters override,
        // and therefore the values a log buffer's metadata is written from
        // when a URI says nothing.
        if let Some(value) = get(&Setting::IPC_TERM_BUFFER_LENGTH) {
            let length = parse_size64(&Setting::IPC_TERM_BUFFER_LENGTH, &value)?;
            config.ipc_term_buffer_length =
                publication_params::check_term_length(u64::try_from(length).unwrap_or(u64::MAX))
                    .map_err(|_| ConfigError::OutOfRange {
                        name: Setting::IPC_TERM_BUFFER_LENGTH.property,
                        value,
                    })?;
        }
        if let Some(value) = get(&Setting::IPC_MTU_LENGTH) {
            let mtu = parse_size64(&Setting::IPC_MTU_LENGTH, &value)?;
            config.ipc_mtu_length = publication_params::check_mtu(
                u64::try_from(mtu).unwrap_or(u64::MAX),
            )
            .map_err(|_| ConfigError::OutOfRange {
                name: Setting::IPC_MTU_LENGTH.property,
                value,
            })?;
        }
        if let Some(value) = get(&Setting::IPC_PUBLICATION_WINDOW_LENGTH) {
            let window = parse_size64(&Setting::IPC_PUBLICATION_WINDOW_LENGTH, &value)?;
            config.ipc_publication_window_length =
                i32::try_from(window).map_err(|_| ConfigError::OutOfRange {
                    name: Setting::IPC_PUBLICATION_WINDOW_LENGTH.property,
                    value,
                })?;
        }
        if let Some(value) = get(&Setting::PUBLICATION_LINGER_TIMEOUT) {
            config.publication_linger_timeout_ns =
                parse_duration_ns(&Setting::PUBLICATION_LINGER_TIMEOUT, &value)?;
        }
        if let Some(value) = get(&Setting::UNTETHERED_WINDOW_LIMIT_TIMEOUT) {
            config.untethered_window_limit_timeout_ns =
                parse_duration_ns(&Setting::UNTETHERED_WINDOW_LIMIT_TIMEOUT, &value)?;
        }
        if let Some(value) = get(&Setting::UNTETHERED_LINGER_TIMEOUT) {
            config.untethered_linger_timeout_ns =
                parse_duration_ns(&Setting::UNTETHERED_LINGER_TIMEOUT, &value)?;
        }
        if let Some(value) = get(&Setting::UNTETHERED_RESTING_TIMEOUT) {
            config.untethered_resting_timeout_ns =
                parse_duration_ns(&Setting::UNTETHERED_RESTING_TIMEOUT, &value)?;
        }
        if let Some(value) = get(&Setting::TERM_BUFFER_SPARSE_FILE) {
            config.term_buffer_sparse_file = parse_bool(&Setting::TERM_BUFFER_SPARSE_FILE, &value)?;
        }
        if let Some(value) = get(&Setting::PERFORM_STORAGE_CHECKS) {
            config.perform_storage_checks = parse_bool(&Setting::PERFORM_STORAGE_CHECKS, &value)?;
        }
        if let Some(value) = get(&Setting::LOW_FILE_STORE_WARNING_THRESHOLD) {
            config.low_file_store_warning_threshold = u64::try_from(parse_size64(
                &Setting::LOW_FILE_STORE_WARNING_THRESHOLD,
                &value,
            )?)
            .map_err(|_| ConfigError::OutOfRange {
                name: Setting::LOW_FILE_STORE_WARNING_THRESHOLD.property,
                value,
            })?;
        }
        if let Some(value) = get(&Setting::PUBLICATION_RESERVED_SESSION_ID_LOW) {
            let id = parse_count(&Setting::PUBLICATION_RESERVED_SESSION_ID_LOW, &value)?;
            config.publication_reserved_session_id_low =
                i32::try_from(id).map_err(|_| ConfigError::OutOfRange {
                    name: Setting::PUBLICATION_RESERVED_SESSION_ID_LOW.property,
                    value,
                })?;
        }
        if let Some(value) = get(&Setting::PUBLICATION_RESERVED_SESSION_ID_HIGH) {
            let id = parse_count(&Setting::PUBLICATION_RESERVED_SESSION_ID_HIGH, &value)?;
            config.publication_reserved_session_id_high =
                i32::try_from(id).map_err(|_| ConfigError::OutOfRange {
                    name: Setting::PUBLICATION_RESERVED_SESSION_ID_HIGH.property,
                    value,
                })?;
        }

        // The one setting that is not read from anywhere: the kernel is asked.
        // It is here rather than in the conductor because it belongs with the
        // values a log buffer is written from, and it is asked once.
        config.socket_buffers = sys::default_socket_buffers().unwrap_or(SocketBufferLengths {
            rcvbuf: 0,
            sndbuf: 0,
        });

        // The lengths come from six independent settings, so the range checks
        // have to run after all of them are in place. This is the same
        // validation `CncFile::create` performs; running it here as well means
        // a bad value is reported before the directory is touched.
        config.layout.validate().map_err(ConfigError::Layout)?;

        // A tier period is a cadence, and everything above an hour is a typo
        // rather than a configuration. It also keeps every deadline this
        // driver computes provably inside an `i64`: the reference parses its
        // own interval up to `INT64_MAX` and then adds it to a nanosecond
        // clock, which is where a value like 8e18 turns into a wrapped
        // deadline and a driver that busy-spins a core while looking healthy.
        if config.timer_interval_ns <= 0 || config.timer_interval_ns > MAX_TIMER_INTERVAL_NS {
            return Err(ConfigError::OutOfRange {
                name: Setting::TIMER_INTERVAL.property,
                value: config.timer_interval_ns.to_string(),
            });
        }

        if config.driver_timeout_ms <= 0 {
            return Err(ConfigError::OutOfRange {
                name: Setting::DRIVER_TIMEOUT.property,
                value: config.driver_timeout_ms.to_string(),
            });
        }

        // The liveness window is written into the CnC metadata and every
        // compatible client derives its keepalive contract from it, so a value
        // that is zero, negative, or no longer than the tier that checks it is
        // a promise the driver cannot keep. The reference refuses both
        // (`aeron_driver_context.c:1526-1533`).
        if config.client_liveness_timeout_ns <= 0
            || config.client_liveness_timeout_ns <= config.timer_interval_ns
        {
            return Err(ConfigError::OutOfRange {
                name: Setting::CLIENT_LIVENESS_TIMEOUT.property,
                value: config.client_liveness_timeout_ns.to_string(),
            });
        }

        Ok(config)
    }
}

/// One setting, under both of the names it answers to.
///
/// The environment names are the reference's own strings
/// (`aeron-driver/src/main/c/aeronmd.h`), and three of them are *not* the
/// property name in capitals: `aeron.to.conductor.buffer.length` is
/// `AERON_CONDUCTOR_BUFFER_LENGTH` (`:97`), not `AERON_TO_CONDUCTOR_...`. That
/// is why this is a table rather than a derivation.
struct Setting {
    /// `deepmsg.` plus this, or `aeron.` plus this.
    property: &'static str,
    /// The reference's environment variable.
    env: &'static str,
}

impl Setting {
    /// `aeron.dir` (`aeronmd.h:38`).
    const DIR: Self = Self {
        property: "dir",
        env: "AERON_DIR",
    };
    /// `aeron.dir.warn.if.exists` (`:46`).
    const DIR_WARN_IF_EXISTS: Self = Self {
        property: "dir.warn.if.exists",
        env: "AERON_DIR_WARN_IF_EXISTS",
    };
    /// `aeron.dir.delete.on.start` (`:81`).
    const DIR_DELETE_ON_START: Self = Self {
        property: "dir.delete.on.start",
        env: "AERON_DIR_DELETE_ON_START",
    };
    /// `aeron.dir.delete.on.shutdown` (`:89`).
    const DIR_DELETE_ON_SHUTDOWN: Self = Self {
        property: "dir.delete.on.shutdown",
        env: "AERON_DIR_DELETE_ON_SHUTDOWN",
    };
    /// `aeron.to.conductor.buffer.length` (`:97`).
    const TO_CONDUCTOR_BUFFER_LENGTH: Self = Self {
        property: "to.conductor.buffer.length",
        env: "AERON_CONDUCTOR_BUFFER_LENGTH",
    };
    /// `aeron.to.clients.buffer.length` (`:105`).
    const TO_CLIENTS_BUFFER_LENGTH: Self = Self {
        property: "to.clients.buffer.length",
        env: "AERON_CLIENTS_BUFFER_LENGTH",
    };
    /// `aeron.counters.values.buffer.length` (`:113`).
    const COUNTERS_VALUES_BUFFER_LENGTH: Self = Self {
        property: "counters.values.buffer.length",
        env: "AERON_COUNTERS_BUFFER_LENGTH",
    };
    /// `aeron.error.buffer.length` (`:121`).
    const ERROR_BUFFER_LENGTH: Self = Self {
        property: "error.buffer.length",
        env: "AERON_ERROR_BUFFER_LENGTH",
    };
    /// `aeron.ipc.term.buffer.length` (`aeronmd.h:145`).
    const IPC_TERM_BUFFER_LENGTH: Self = Self {
        property: "ipc.term.buffer.length",
        env: "AERON_IPC_TERM_BUFFER_LENGTH",
    };
    /// `aeron.ipc.mtu.length` (`aeronmd.h:201`).
    const IPC_MTU_LENGTH: Self = Self {
        property: "ipc.mtu.length",
        env: "AERON_IPC_MTU_LENGTH",
    };
    /// `aeron.ipc.publication.term.window.length` (`aeronmd.h:209`).
    const IPC_PUBLICATION_WINDOW_LENGTH: Self = Self {
        property: "ipc.publication.term.window.length",
        env: "AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH",
    };
    /// `aeron.publication.reserved.session.id.low` (`aeronmd.h:760`).
    const PUBLICATION_RESERVED_SESSION_ID_LOW: Self = Self {
        property: "publication.reserved.session.id.low",
        env: "AERON_PUBLICATION_RESERVED_SESSION_ID_LOW",
    };
    /// `aeron.publication.reserved.session.id.high` (`aeronmd.h:765`).
    const PUBLICATION_RESERVED_SESSION_ID_HIGH: Self = Self {
        property: "publication.reserved.session.id.high",
        env: "AERON_PUBLICATION_RESERVED_SESSION_ID_HIGH",
    };
    /// `aeron.publication.linger.timeout` (`aeronmd.h:225`).
    const PUBLICATION_LINGER_TIMEOUT: Self = Self {
        property: "publication.linger.timeout",
        env: "AERON_PUBLICATION_LINGER_TIMEOUT",
    };
    /// `aeron.term.buffer.sparse.file` (`aeronmd.h:153`).
    const TERM_BUFFER_SPARSE_FILE: Self = Self {
        property: "term.buffer.sparse.file",
        env: "AERON_TERM_BUFFER_SPARSE_FILE",
    };
    /// `aeron.perform.storage.checks` (`aeronmd.h:163-164`).
    const PERFORM_STORAGE_CHECKS: Self = Self {
        property: "perform.storage.checks",
        env: "AERON_PERFORM_STORAGE_CHECKS",
    };
    /// `aeron.low.file.store.warning.threshold` (`aeronmd.h:171-172`).
    const LOW_FILE_STORE_WARNING_THRESHOLD: Self = Self {
        property: "low.file.store.warning.threshold",
        env: "AERON_LOW_FILE_STORE_WARNING_THRESHOLD",
    };
    /// `aeron.untethered.window.limit.timeout` (`aeronmd.h:611`).
    const UNTETHERED_WINDOW_LIMIT_TIMEOUT: Self = Self {
        property: "untethered.window.limit.timeout",
        env: "AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT",
    };
    /// `aeron.untethered.linger.timeout` (`aeronmd.h:620`).
    const UNTETHERED_LINGER_TIMEOUT: Self = Self {
        property: "untethered.linger.timeout",
        env: "AERON_UNTETHERED_LINGER_TIMEOUT",
    };
    /// `aeron.untethered.resting.timeout` (`aeronmd.h:629`).
    const UNTETHERED_RESTING_TIMEOUT: Self = Self {
        property: "untethered.resting.timeout",
        env: "AERON_UNTETHERED_RESTING_TIMEOUT",
    };
    const CLIENT_LIVENESS_TIMEOUT: Self = Self {
        property: "client.liveness.timeout",
        env: "AERON_CLIENT_LIVENESS_TIMEOUT",
    };
    /// `aeron.file.page.size` (`:185`).
    const FILE_PAGE_SIZE: Self = Self {
        property: "file.page.size",
        env: "AERON_FILE_PAGE_SIZE",
    };
    /// `aeron.timer.interval` (`:409`).
    const TIMER_INTERVAL: Self = Self {
        property: "timer.interval",
        env: "AERON_TIMER_INTERVAL",
    };
    /// `aeron.driver.termination.validator` (`:567`).
    const TERMINATION_VALIDATOR: Self = Self {
        property: "driver.termination.validator",
        env: "AERON_DRIVER_TERMINATION_VALIDATOR",
    };
    /// `aeron.driver.timeout` (`:637`).
    const DRIVER_TIMEOUT: Self = Self {
        property: "driver.timeout",
        env: "AERON_DRIVER_TIMEOUT",
    };
    /// `aeron.counters.free.to.reuse.timeout` (`:525`). Zero is legal and means
    /// a reclaimed counter is immediately reusable.
    const COUNTER_FREE_TO_REUSE_TIMEOUT: Self = Self {
        property: "counters.free.to.reuse.timeout",
        env: "AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT",
    };
}

/// Why a configuration could not be resolved.
#[derive(Debug)]
pub enum ConfigError {
    /// An argument that is not `-Dname=value`.
    MalformedArgument {
        /// The argument as it arrived.
        argument: String,
    },
    /// No aeron directory was configured.
    MissingAeronDir {
        /// The property that would have set it.
        property: &'static str,
        /// The environment variable that would have set it.
        env: &'static str,
    },
    /// A boolean that is not one of the six the reference parses
    /// (`aeron-client/src/main/c/util/aeron_parse_util.c:332-350`).
    NotABoolean {
        /// The setting, by property name.
        name: &'static str,
        /// What it was set to.
        value: String,
    },
    /// A value the reference's parsers would reject
    /// (`aeron_parse_util.c:42-105`, `:170-268`).
    NotANumber {
        /// The setting, by property name.
        name: &'static str,
        /// What it was set to.
        value: String,
    },
    /// A size or duration that does not fit in the type the driver holds it
    /// in.
    OutOfRange {
        /// The setting, by property name.
        name: &'static str,
        /// What it was set to.
        value: String,
    },
    /// A validator name the reference's symbol table does not know.
    UnknownValidator {
        /// What it was set to.
        value: String,
    },
    /// The lengths do not describe a CnC file the reference would accept.
    Layout(CncCreateError),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedArgument { argument } => {
                write!(f, "expected -Dname=value, got {argument}")
            }
            Self::MissingAeronDir { property, env } => write!(
                f,
                "no aeron directory: set -Ddeepmsg.{property}, -Daeron.{property}, {env} or DEEPMSG_{}",
                env.strip_prefix("AERON_").unwrap_or(env)
            ),
            Self::NotABoolean { name, value } => {
                write!(f, "{name} is {value}, expected 1/on/true or 0/off/false")
            }
            Self::NotANumber { name, value } => {
                write!(f, "{name} is {value}, which is not a number")
            }
            Self::OutOfRange { name, value } => {
                write!(f, "{name} is {value}, outside the range the driver holds")
            }
            Self::UnknownValidator { value } => {
                write!(
                    f,
                    "termination validator is {value}, expected allow or deny"
                )
            }
            Self::Layout(error) => write!(f, "the CnC layout is not usable: {error}"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Layout(error) => Some(error),
            Self::MalformedArgument { .. }
            | Self::MissingAeronDir { .. }
            | Self::NotABoolean { .. }
            | Self::NotANumber { .. }
            | Self::OutOfRange { .. }
            | Self::UnknownValidator { .. } => None,
        }
    }
}

impl From<CncCreateError> for ConfigError {
    fn from(error: CncCreateError) -> Self {
        Self::Layout(error)
    }
}

/// Split `-Dname=value` arguments out of an argument list.
///
/// Anything else is an error rather than a positional argument, because there
/// are none: the reference's driver takes properties and nothing else.
fn parse_properties<I>(args: I) -> Result<Vec<(String, String)>, ConfigError>
where
    I: IntoIterator<Item = String>,
{
    let mut properties = Vec::new();

    for argument in args {
        let Some(body) = argument.strip_prefix("-D") else {
            return Err(ConfigError::MalformedArgument { argument });
        };

        let Some((name, value)) = body.split_once('=') else {
            return Err(ConfigError::MalformedArgument { argument });
        };

        properties.push((name.to_owned(), value.to_owned()));
    }

    Ok(properties)
}

/// The value of a setting, from the first place that has one.
///
/// `-Ddeepmsg.<property>`, then `DEEPMSG_<PROPERTY>`, then
/// `-Daeron.<property>`, then the reference's environment variable.
fn lookup(
    properties: &[(String, String)],
    env: &impl Fn(&str) -> Option<String>,
    setting: &Setting,
) -> Option<String> {
    let ours = format!("deepmsg.{}", setting.property);
    let theirs = format!("aeron.{}", setting.property);
    let our_env = format!(
        "DEEPMSG_{}",
        setting.property.replace('.', "_").to_uppercase()
    );

    property(properties, &ours)
        .or_else(|| env(&our_env))
        .or_else(|| property(properties, &theirs))
        .or_else(|| env(setting.env))
        // An empty value is an unset value, which is how the reference reads
        // its own properties (`aeron_properties_util.c:151-179`). Without
        // this, `-Ddeepmsg.dir=` would pass the mandatory-directory check as
        // the empty path: the driver would skip the directory discipline
        // entirely and write `cnc.dat`, `publications/` and `images/` into
        // whatever directory it was started from.
        .filter(|value| !value.trim().is_empty())
}

fn property(properties: &[(String, String)], name: &str) -> Option<String> {
    properties
        .iter()
        .rev()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.clone())
}

/// Parse a boolean the way the reference does
/// (`aeron-client/src/main/c/util/aeron_parse_util.c:332-350`): six accepted
/// spellings, compared from the start of the string.
///
/// The reference compares a prefix, so `turnip` is `true` and `offside` is
/// `false`; this accepts the six whole words instead. The divergence can only
/// turn a typo into an error, never a value into a different value.
fn parse_bool(setting: &Setting, value: &str) -> Result<bool, ConfigError> {
    match value {
        "1" | "on" | "true" => Ok(true),
        "0" | "off" | "false" => Ok(false),
        other => Err(ConfigError::NotABoolean {
            name: setting.property,
            value: other.to_owned(),
        }),
    }
}

/// Parse a byte count, with the reference's suffixes
/// (`aeron-client/src/main/c/util/aeron_parse_util.c:42-105`): `k`, `m`, `g`,
/// either case, as binary multiples.
fn parse_size64(setting: &Setting, value: &str) -> Result<usize, ConfigError> {
    let (digits, multiplier) = if let Some(head) = value.strip_suffix(['k', 'K']) {
        (head, 1024usize)
    } else if let Some(head) = value.strip_suffix(['m', 'M']) {
        (head, 1024 * 1024)
    } else if let Some(head) = value.strip_suffix(['g', 'G']) {
        (head, 1024 * 1024 * 1024)
    } else {
        (value, 1)
    };

    let count: usize = digits.parse().map_err(|_| ConfigError::NotANumber {
        name: setting.property,
        value: value.to_owned(),
    })?;

    count
        .checked_mul(multiplier)
        .ok_or_else(|| ConfigError::OutOfRange {
            name: setting.property,
            value: value.to_owned(),
        })
}

/// Parse a plain count, the way `aeron_config_parse_uint64` does: digits, and
/// nothing else.
fn parse_count(setting: &Setting, value: &str) -> Result<i64, ConfigError> {
    value.parse().map_err(|_| ConfigError::NotANumber {
        name: setting.property,
        value: value.to_owned(),
    })
}

/// Parse a duration in nanoseconds, with the reference's suffixes
/// (`aeron-client/src/main/c/util/aeron_parse_util.c:170-268`): `s`, `ms`,
/// `us`, `ns`, either case.
fn parse_duration_ns(setting: &Setting, value: &str) -> Result<i64, ConfigError> {
    let lower = value.to_ascii_lowercase();
    let (digits, multiplier) = if let Some(head) = lower.strip_suffix("ns") {
        (head, 1i64)
    } else if let Some(head) = lower.strip_suffix("us") {
        (head, 1_000)
    } else if let Some(head) = lower.strip_suffix("ms") {
        (head, 1_000_000)
    } else if let Some(head) = lower.strip_suffix('s') {
        (head, 1_000_000_000)
    } else {
        (lower.as_str(), 1)
    };

    let count: i64 = digits.parse().map_err(|_| ConfigError::NotANumber {
        name: setting.property,
        value: value.to_owned(),
    })?;

    count
        .checked_mul(multiplier)
        .ok_or_else(|| ConfigError::OutOfRange {
            name: setting.property,
            value: value.to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Resolve against an empty environment.
    fn resolve(properties: &[(&str, &str)]) -> Result<DriverConfig, ConfigError> {
        let owned: Vec<(String, String)> = properties
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        DriverConfig::resolve(&owned, &|_| None)
    }

    /// Resolve against a fixed environment.
    fn resolve_with_env(
        properties: &[(&str, &str)],
        env: &[(&str, &str)],
    ) -> Result<DriverConfig, ConfigError> {
        let owned: Vec<(String, String)> = properties
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        DriverConfig::resolve(&owned, &|name| {
            env.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        })
    }

    #[test]
    fn the_defaults_are_the_reference_defaults() {
        let config = DriverConfig::default();

        assert_eq!(CncLayout::default(), config.layout);
        assert_eq!(10_000_000_000, config.client_liveness_timeout_ns);
        assert_eq!(1_000_000_000, config.timer_interval_ns);
        assert_eq!(1_000_000_000, config.counter_free_to_reuse_ns);
        assert_eq!(TerminationPolicy::Deny, config.termination);
        assert!(!config.dirs_delete_on_start);
        assert!(!config.dirs_delete_on_shutdown);
        assert!(!config.warn_if_dirs_exist);
        assert!(config.aeron_dir.as_os_str().is_empty());
    }

    #[test]
    fn an_empty_value_is_an_unset_value() {
        // The reference reads an empty `-D` value as "not set"
        // (`aeron_properties_util.c:151-179`). Here that has to mean the
        // mandatory-directory check fails, because the empty path would skip
        // the directory discipline and write the CnC file into the working
        // directory instead.
        for empty in ["", "   ", "\t"] {
            assert!(
                matches!(
                    resolve(&[("deepmsg.dir", empty)]),
                    Err(ConfigError::MissingAeronDir { .. })
                ),
                "{empty:?} must not be a directory"
            );
        }

        assert!(
            matches!(
                resolve_with_env(&[("aeron.dir", "/tmp/aeron")], &[("DEEPMSG_DIR", "")]),
                Err(ConfigError::MissingAeronDir { .. })
            ),
            "our empty environment value must not shadow the reference's flag"
        );

        // And an empty value for any other setting is "unset", not a parse
        // error: a deployment's configuration file may carry a blank line for
        // something it does not configure.
        let config = resolve(&[
            ("deepmsg.dir", "/tmp/deepmsg"),
            ("deepmsg.timer.interval", ""),
        ])
        .expect("resolve");
        assert_eq!(1_000_000_000, config.timer_interval_ns, "the default");
    }

    #[test]
    fn the_liveness_window_must_outlast_the_tier_that_checks_it() {
        // Zero or negative first: the metadata field is a contract with every
        // client, and zero is a promise of immediate reaping.
        for bad in ["0", "-1ns"] {
            assert!(
                matches!(
                    resolve(&[
                        ("deepmsg.dir", "/tmp/deepmsg"),
                        ("deepmsg.client.liveness.timeout", bad),
                    ]),
                    Err(ConfigError::OutOfRange { .. })
                ),
                "{bad} must be refused"
            );
        }

        // And a window the timeout tier cannot honour: the reference refuses
        // `client_liveness_timeout_ns <= timer_interval_ns`
        // (`aeron_driver_context.c:1526-1533`).
        assert!(matches!(
            resolve(&[
                ("deepmsg.dir", "/tmp/deepmsg"),
                ("deepmsg.timer.interval", "1s"),
                ("deepmsg.client.liveness.timeout", "1s"),
            ]),
            Err(ConfigError::OutOfRange { .. })
        ));
        assert!(
            resolve(&[
                ("deepmsg.dir", "/tmp/deepmsg"),
                ("deepmsg.timer.interval", "1s"),
                ("deepmsg.client.liveness.timeout", "1001ms"),
            ])
            .is_ok(),
            "one millisecond more is enough"
        );
    }

    #[test]
    fn the_aeron_directory_is_mandatory() {
        let error = resolve(&[]).expect_err("the reference has no default either");

        assert!(matches!(error, ConfigError::MissingAeronDir { .. }));
        assert!(error.to_string().contains("-Ddeepmsg.dir"));
    }

    #[test]
    fn the_storage_check_answers_to_the_references_names() {
        // On by default, and the warning level ten of the reference's default
        // term lengths (`aeron_driver_context.c:179-183`, `:457`, `:492`).
        let config = DriverConfig::default();
        assert!(config.perform_storage_checks);
        assert_eq!(160 * 1024 * 1024, config.low_file_store_warning_threshold);

        let off = resolve(&[
            ("deepmsg.dir", "/tmp/deepmsg"),
            ("aeron.perform.storage.checks", "false"),
        ])
        .expect("resolve");
        assert!(!off.perform_storage_checks);

        let level = resolve(&[
            ("deepmsg.dir", "/tmp/deepmsg"),
            ("aeron.low.file.store.warning.threshold", "1g"),
        ])
        .expect("resolve");
        assert_eq!(1024 * 1024 * 1024, level.low_file_store_warning_threshold);
    }

    #[test]
    fn the_counter_reuse_timeout_answers_to_the_references_names() {
        // The reference's environment name is not the property name in
        // capitals, which is why the table carries both (`aeronmd.h:525`).
        let by_property = resolve(&[
            ("deepmsg.dir", "/tmp/deepmsg"),
            ("aeron.counters.free.to.reuse.timeout", "5s"),
        ])
        .expect("resolve");
        assert_eq!(5_000_000_000, by_property.counter_free_to_reuse_ns);

        let by_env = resolve_with_env(
            &[("deepmsg.dir", "/tmp/deepmsg")],
            &[("AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT", "250ms")],
        )
        .expect("resolve");
        assert_eq!(250_000_000, by_env.counter_free_to_reuse_ns);

        // Zero is legal and means "reusable at once"; the reference accepts it
        // (`aeron_driver_context.c:883-890` parses from a floor of zero).
        let zero = resolve(&[
            ("deepmsg.dir", "/tmp/deepmsg"),
            ("deepmsg.counters.free.to.reuse.timeout", "0ns"),
        ])
        .expect("resolve");
        assert_eq!(0, zero.counter_free_to_reuse_ns);
    }

    #[test]
    fn a_property_sets_a_value_and_the_alias_does_too() {
        let ours = resolve(&[("deepmsg.dir", "/tmp/deepmsg")]).expect("resolve");
        assert_eq!(PathBuf::from("/tmp/deepmsg"), ours.aeron_dir);

        let theirs = resolve(&[("aeron.dir", "/tmp/aeron")]).expect("resolve");
        assert_eq!(PathBuf::from("/tmp/aeron"), theirs.aeron_dir);
    }

    #[test]
    fn a_flag_beats_the_environment_and_ours_beats_theirs() {
        let config = resolve_with_env(
            &[("deepmsg.dir", "/from/flag"), ("aeron.dir", "/from/alias")],
            &[
                ("DEEPMSG_DIR", "/from/our/env"),
                ("AERON_DIR", "/from/their/env"),
            ],
        )
        .expect("resolve");
        assert_eq!(PathBuf::from("/from/flag"), config.aeron_dir);

        // Drop our flag: our environment still wins over their flag.
        let config = resolve_with_env(
            &[("aeron.dir", "/from/alias")],
            &[("DEEPMSG_DIR", "/from/our/env")],
        )
        .expect("resolve");
        assert_eq!(PathBuf::from("/from/our/env"), config.aeron_dir);

        // Drop both of ours: the reference's environment variable is still
        // read, which is the whole point of the alias.
        let config = resolve_with_env(&[], &[("AERON_DIR", "/from/their/env")]).expect("resolve");
        assert_eq!(PathBuf::from("/from/their/env"), config.aeron_dir);
    }

    #[test]
    fn the_reference_environment_names_are_the_ones_it_defines() {
        // Three of these are not the property name in capitals, which is why
        // the table exists rather than a name transformation.
        // The two ring lengths are the reason this is a table and not a name
        // transformation: `AERON_CONDUCTOR_BUFFER_LENGTH` is what
        // `aeron.to.conductor.buffer.length` is called in the environment, and
        // the value is a *region* length — two mebibytes of capacity plus the
        // trailer, which no size suffix can spell.
        let config = resolve_with_env(
            &[],
            &[
                ("AERON_DIR", "/tmp/aeron"),
                ("AERON_CONDUCTOR_BUFFER_LENGTH", "2097920"),
                ("AERON_CLIENTS_BUFFER_LENGTH", "2097280"),
                ("AERON_COUNTERS_BUFFER_LENGTH", "8m"),
                ("AERON_ERROR_BUFFER_LENGTH", "4m"),
                ("AERON_FILE_PAGE_SIZE", "4096"),
                ("AERON_CLIENT_LIVENESS_TIMEOUT", "5s"),
                ("AERON_TIMER_INTERVAL", "250ms"),
                ("AERON_DRIVER_TIMEOUT", "30000"),
                ("AERON_DRIVER_TERMINATION_VALIDATOR", "allow"),
                ("AERON_DIR_DELETE_ON_START", "true"),
            ],
        )
        .expect("resolve");

        assert_eq!(2 * 1024 * 1024 + 768, config.layout.to_driver_length);
        assert_eq!(2 * 1024 * 1024 + 128, config.layout.to_clients_length);
        assert_eq!(5_000_000_000, config.client_liveness_timeout_ns);
        assert_eq!(250_000_000, config.timer_interval_ns);
        assert_eq!(30_000, config.driver_timeout_ms);
        assert_eq!(TerminationPolicy::Allow, config.termination);
        assert!(config.dirs_delete_on_start);
    }

    #[test]
    fn a_round_number_ring_length_fails_here_as_it_fails_there() {
        // `2m` is 768 bytes short of a legal capacity. The reference fails on
        // it too, in conductor init, with "Invalid capacity"
        // (`aeron-client/src/main/c/concurrent/aeron_mpsc_rb.c:37`); this
        // fails at start-up and names the setting.
        let error = resolve(&[
            ("deepmsg.dir", "/tmp/x"),
            ("deepmsg.to.conductor.buffer.length", "2m"),
        ])
        .expect_err("2 MiB of region is not a legal ring");

        assert!(matches!(error, ConfigError::Layout(_)));
        assert!(error.to_string().contains("to-driver"));
    }

    #[test]
    fn sizes_carry_the_reference_suffixes() {
        // The parser on its own: every setting has a floor the reference
        // enforces, so a bare `1k` cannot reach the layout through `resolve`.
        for (text, expected) in [
            ("1024", 1024usize),
            ("1k", 1024),
            ("1K", 1024),
            ("4m", 4 * 1024 * 1024),
            ("4M", 4 * 1024 * 1024),
            ("1g", 1024 * 1024 * 1024),
            ("1048576", 1024 * 1024),
            ("0", 0),
        ] {
            assert_eq!(
                expected,
                parse_size64(&Setting::ERROR_BUFFER_LENGTH, text).expect(text),
                "{text}"
            );
        }

        for bad in ["4x", "4mb", "m", "-1", "4.5m"] {
            assert!(
                parse_size64(&Setting::ERROR_BUFFER_LENGTH, bad).is_err(),
                "{bad} is not a size"
            );
        }
    }

    #[test]
    fn durations_carry_the_reference_suffixes() {
        for (text, expected) in [
            ("10", 10i64),
            ("10ns", 10),
            ("10us", 10_000),
            ("10ms", 10_000_000),
            ("10s", 10_000_000_000),
            ("1S", 1_000_000_000),
            ("1MS", 1_000_000),
            ("250ms", 250_000_000),
        ] {
            assert_eq!(
                expected,
                parse_duration_ns(&Setting::CLIENT_LIVENESS_TIMEOUT, text).expect(text),
                "{text}"
            );
        }

        for bad in ["10s ", "s", "10mss"] {
            assert!(
                parse_duration_ns(&Setting::CLIENT_LIVENESS_TIMEOUT, bad).is_err(),
                "{bad} is not a duration"
            );
        }
    }

    #[test]
    fn a_size_reaches_the_layout_through_the_property_name() {
        let config = resolve(&[
            ("deepmsg.dir", "/tmp/x"),
            ("deepmsg.error.buffer.length", "8m"),
        ])
        .expect("resolve");

        assert_eq!(8 * 1024 * 1024, config.layout.error_log_length);
    }

    #[test]
    fn booleans_take_the_six_spellings_the_reference_parses() {
        for (text, expected) in [
            ("1", true),
            ("on", true),
            ("true", true),
            ("0", false),
            ("off", false),
            ("false", false),
        ] {
            assert_eq!(
                expected,
                parse_bool(&Setting::DIR_DELETE_ON_SHUTDOWN, text).expect(text),
                "{text}"
            );
        }

        let error =
            parse_bool(&Setting::DIR_DELETE_ON_SHUTDOWN, "yes").expect_err("not one of the six");
        assert!(matches!(error, ConfigError::NotABoolean { .. }));
    }

    #[test]
    fn a_value_the_reference_would_clamp_is_an_error_here() {
        // The reference warns and clamps (`aeron_parse_util.c:701-722`); this
        // refuses, because a driver whose CnC file is a different size from
        // the one the deployment configured should say so at start-up.
        let error = resolve(&[
            ("deepmsg.dir", "/tmp/x"),
            ("deepmsg.counters.values.buffer.length", "512k"),
        ])
        .expect_err("below the reference's floor");

        assert!(matches!(error, ConfigError::Layout(_)));
        assert!(error.to_string().contains("counters_values_length"));
    }

    #[test]
    fn an_unparsable_value_names_the_setting() {
        let error = resolve(&[("deepmsg.dir", "/tmp/x"), ("deepmsg.file.page.size", "4x")])
            .expect_err("x is not a suffix");

        assert!(matches!(error, ConfigError::NotANumber { .. }));
        assert!(error.to_string().contains("file.page.size"));
    }

    #[test]
    fn an_unknown_validator_is_fatal_as_it_is_in_the_reference() {
        let error = resolve(&[
            ("deepmsg.dir", "/tmp/x"),
            ("deepmsg.driver.termination.validator", "maybe"),
        ])
        .expect_err("the symbol table has two entries");

        assert!(matches!(error, ConfigError::UnknownValidator { .. }));
    }

    #[test]
    fn arguments_that_are_not_properties_are_rejected() {
        for argument in ["dir=/tmp/x", "-Ddeepmsg.dir", "--dir=/tmp/x"] {
            let error = parse_properties([argument.to_owned()]).expect_err("not a property");
            assert!(matches!(error, ConfigError::MalformedArgument { .. }));
        }
    }

    #[test]
    fn unknown_properties_are_ignored_because_a_reference_config_has_them() {
        let config = resolve(&[
            ("deepmsg.dir", "/tmp/x"),
            ("aeron.threading.mode", "SHARED"),
            ("aeron.conductor.idle.strategy", "backoff"),
            ("aeron.term.buffer.length", "64m"),
        ])
        .expect("resolve");

        assert_eq!(PathBuf::from("/tmp/x"), config.aeron_dir);
    }
}
