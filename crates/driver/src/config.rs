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

use crate::flowcontrol::Supplier;
use crate::port_manager::{PortRange, PortRangeError};
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

/// `AERON_SOCKET_MULTICAST_TTL_DEFAULT` (`aeron_driver_context.c:193`): zero,
/// which leaves the hop limit to the kernel (one) unless a channel names one
/// with `ttl=`.
pub const SOCKET_MULTICAST_TTL_DEFAULT: u8 = 0;

/// `AERON_NAK_MULTICAST_GROUP_SIZE_DEFAULT`
/// (`aeron_driver_context.c:220`): how many receivers a group is assumed to
/// have, which is what a NAK's backoff is drawn against.
pub const NAK_MULTICAST_GROUP_SIZE_DEFAULT: usize = 10;

/// The least a multicast backoff may be set to
/// (`aeron_driver_context.c:945-950`): a microsecond.
pub const NAK_MULTICAST_MAX_BACKOFF_NS_MIN: i64 = 1_000;

/// `AERON_NAK_MULTICAST_MAX_BACKOFF_NS_DEFAULT`
/// (`aeron_driver_context.c:221`): ten milliseconds, and **not** the sixty the
/// dead `AERON_LOSS_DETECTOR_NAK_MULTICAST_MAX_BACKOFF_NS` macro says
/// (`aeron_loss_detector.h:76`). Ten is what the scale factor of this
/// distribution is, so it is also the mean backoff.
pub const NAK_MULTICAST_MAX_BACKOFF_NS_DEFAULT: i64 = 10 * 1000 * 1000;

/// `AERON_RECEIVER_GROUP_CONSIDERATION_DEFAULT` (`aeron_driver_context.c:227`).
pub const RECEIVER_GROUP_CONSIDERATION_DEFAULT: InferableBoolean = InferableBoolean::Infer;

/// What a channel that names no `gtag=` stamps into its status messages: none
/// (`AERON_RECEIVER_GROUP_TAG_IS_PRESENT_DEFAULT` and
/// `..._VALUE_DEFAULT`, `aeron_driver_context.c:194-195`).
///
/// The reference keeps the two halves apart — `is_present = false` *and*
/// `value = -1` — and the difference is real: an endpoint with no tag sends a
/// 36-byte status message, and one whose tag is `-1` sends 44
/// (`aeron_receive_channel_endpoint.c:309-312`). [`None`] is the absent half.
pub const RECEIVER_GROUP_TAG_DEFAULT: Option<i64> = None;

/// `AERON_FLOW_CONTROL_GROUP_TAG_DEFAULT` (`aeron_driver_context.c:196`): no
/// tag, which is a tag of `-1` and not an absent one — this is the `tagged`
/// strategy's own group tag, the one a status message has to carry, and the
/// endpoint's `gtag` is a different setting.
pub const FLOW_CONTROL_GROUP_TAG_DEFAULT: i64 = -1;

/// `AERON_FLOW_CONTROL_GROUP_MIN_SIZE_DEFAULT` (`:197`): a group of one, so
/// that naming `fc=min` alone means "wait for the slowest of whoever is there"
/// rather than "wait for a quorum nobody configured".
pub const FLOW_CONTROL_GROUP_MIN_SIZE_DEFAULT: i32 = 0;

/// `AERON_FLOW_CONTROL_RECEIVER_TIMEOUT_NS_DEFAULT` (`:198`): five seconds
/// before a receiver that has gone quiet is dropped.
pub const FLOW_CONTROL_RECEIVER_TIMEOUT_NS_DEFAULT: i64 = 5 * 1000 * 1000 * 1000;

/// `AERON_IMAGE_LIVENESS_TIMEOUT_NS_DEFAULT` (`aeron_driver_context.c:204`):
/// how long an image may go quiet before it starts draining.
///
/// It is also what an IPC publication measures a refusal against
/// (`aeron_ipc_publication.c:177`), which is why it is a driver setting rather
/// than an image's own.
pub const IMAGE_LIVENESS_TIMEOUT_NS_DEFAULT: i64 = 10 * 1000 * 1000 * 1000;

/// `AERON_NAME_RESOLVER_SUPPLIER_DEFAULT` (`aeronmd.h:809`): `default`, which
/// is the synchronous resolver this build has always had.
pub const NAME_RESOLVER_SUPPLIER_DEFAULT: crate::name_resolver::Supplier =
    crate::name_resolver::Supplier::Default;

/// `AERON_DRIVER_RESOLVER_NEIGHBOR_TIMEOUT_NS_DEFAULT`
/// (`aeron_driver_context.c:231`): how long a neighbor, and a cached name, are
/// believed after the moment they were last about.
pub const RESOLVER_NEIGHBOR_TIMEOUT_NS_DEFAULT: i64 = 10 * 1000 * 1000 * 1000;

/// `AERON_DRIVER_RESOLVER_SELF_RESOLUTION_INTERVAL_NS_DEFAULT` (`:232`).
pub const RESOLVER_SELF_RESOLUTION_INTERVAL_NS_DEFAULT: i64 = 1000 * 1000 * 1000;

/// `AERON_DRIVER_RESOLVER_NEIGHBOR_RESOLUTION_INTERVAL_NS_DEFAULT` (`:233`).
pub const RESOLVER_NEIGHBOR_RESOLUTION_INTERVAL_NS_DEFAULT: i64 = 2 * 1000 * 1000 * 1000;

/// `AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR_RESOLUTION_INTERVAL_NS_DEFAULT`
/// (`:234`).
pub const RESOLVER_BOOTSTRAP_NEIGHBOR_RESOLUTION_INTERVAL_NS_DEFAULT: i64 = 10 * 1000 * 1000 * 1000;

/// `AERON_DRIVER_RERESOLUTION_CHECK_INTERVAL_NS_DEFAULT`
/// (`aeron_driver_context.c:235`): how often a sender and a receiver look for
/// names that need resolving again.
///
/// One second, and a value of **zero turns the whole feature off** — the two
/// loops are `if interval_ns > 0 && deadline passed`
/// (`aeron_driver_sender.c:183`, `aeron_driver_receiver.c:254`), so a driver
/// configured with zero does no re-resolution at all.
pub const RERESOLUTION_CHECK_INTERVAL_NS_DEFAULT: i64 = 1000 * 1000 * 1000;

/// `AERON_DRIVER_NAME_RESOLVER_THRESHOLD_NS_DEFAULT` (`:239`): how long a
/// single resolution may take before system counter 33 counts it.
///
/// Five **seconds**, which reads like a typo for milliseconds and is not one:
/// the reference writes `5 * 1000 * 1000 * INT64_C(1000)`, and the Java
/// `Configuration` default is the same five seconds
/// (`aeron-driver/src/main/java/io/aeron/driver/Configuration.java:1118-1124`).
/// A threshold that only a name server on fire can cross is the point: ordinary
/// resolution is microseconds.
pub const NAME_RESOLVER_THRESHOLD_NS_DEFAULT: i64 = 5 * 1000 * 1000 * 1000;

/// The smallest one of the resolver's four **intervals** may be
/// (`aeron_config_parse_duration_ns(..., 1000 * 1000, INT64_MAX)`, which is how
/// each of them is read, `aeron_driver_context.c:609-636`): a millisecond, so
/// that a resolver cannot gossip in a busy loop.
///
/// The **threshold** beside them has no floor at all — it is read with `0` as
/// its minimum (`:1049-1055`), and the reference's own re-resolution test sets
/// it to a single nanosecond to make every resolution count
/// (`NameReResolutionTest.java`'s `nameResolverThresholdNs(1)`). A floor here
/// would not be strictness: it would be a driver that will not start for a
/// configuration the reference serves.
pub const RESOLVER_INTERVAL_NS_MIN: i64 = 1000 * 1000;

/// `AERON_MULTICAST_FLOWCONTROL_SUPPLIER_DEFAULT` (`aeron_driver_context.c:201`):
/// `max`.
pub const MULTICAST_FLOW_CONTROL_SUPPLIER_DEFAULT: Supplier = Supplier::Max;

/// `AERON_UNICAST_FLOWCONTROL_SUPPLIER_DEFAULT` (`:202`): `max` as well, under
/// the name of the unicast supplier (`aeron_flow_control.c:326-365`).
pub const UNICAST_FLOW_CONTROL_SUPPLIER_DEFAULT: Supplier = Supplier::Max;

/// A boolean that has a third answer: **work it out**
/// (`aeron_inferable_boolean_t`, `aeronmd.h:703-709`).
///
/// One parameter needs it — a subscription's `group=` — because "this channel
/// is a group" is a question the channel itself usually answers, and a client
/// sometimes wants to overrule it in either direction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum InferableBoolean {
    /// The channel decided: it is a group if it is a multicast one, or if the
    /// `SETUP` that opened it said so.
    #[default]
    Infer,
    /// It is one whatever the channel says.
    ForceTrue,
    /// It is not, whatever the channel says.
    ForceFalse,
}

impl InferableBoolean {
    /// `aeron_config_parse_inferable_boolean`
    /// (`aeron_driver_context.c:99-119`).
    ///
    /// Both comparisons are **exact**, though they do not look it: the
    /// reference's `strncmp(text, "true", sizeof("true"))` compares five
    /// bytes, the fifth being the literal's own terminator — so `truex` and
    /// `inferno` are `ForceFalse` rather than a prefix match.
    pub fn parse(text: Option<&str>, default: Self) -> Self {
        match text {
            None => default,
            Some("true") => Self::ForceTrue,
            Some("infer") => Self::Infer,
            Some(_) => Self::ForceFalse,
        }
    }

    /// The answer, given what the channel itself says
    /// (`aeron_driver_conductor_treat_image_as_multicast`,
    /// `aeron_driver_conductor.c:674-680`).
    pub const fn resolve(self, channel_says_so: bool) -> bool {
        match self {
            Self::Infer => channel_says_so,
            Self::ForceTrue => true,
            Self::ForceFalse => false,
        }
    }
}

/// `AERON_RCV_INITIAL_WINDOW_LENGTH_DEFAULT` (`aeron_driver_context.c:205`).
pub const RCV_INITIAL_WINDOW_LENGTH_DEFAULT: i32 = 128 * 1024;

/// `AERON_RCV_STATUS_MESSAGE_TIMEOUT_NS_DEFAULT` (`aeron_driver_context.c:200`):
/// 200 milliseconds, and the window after which a receiver decides the sender
/// has gone.
pub const RCV_STATUS_MESSAGE_TIMEOUT_NS_DEFAULT: i64 = 200 * 1000 * 1000;

/// `AERON_SPIES_SIMULATE_CONNECTION_DEFAULT` (`aeron_driver_context.c:210`):
/// false, so a stream with only spies looks unconnected until someone asks.
pub const SPIES_SIMULATE_CONNECTION_DEFAULT: bool = false;

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
    /// Whether a publication counts its spies as receivers, so a stream with
    /// only spies looks connected (`aeron.spies.simulate.connection`,
    /// `aeronmd.h:178`, false by default).
    ///
    /// The setting is read and reaches a publication's parameters; what acts
    /// on it is the *spy* machinery (`aeron_network_publication.c:618`, `:761`),
    /// which this build does not have yet — see `docs/compat.md`.
    pub spies_simulate_connection: bool,
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
    /// The multicast hop limit a channel that named none gets
    /// (`aeron.socket.multicast.ttl = 0`, `AERON_SOCKET_MULTICAST_TTL`).
    ///
    /// Zero is the reference's default and is also what `ttl=` with no value
    /// leaves behind, so the two are one case and the socket keeps the
    /// kernel's own limit (`aeron_send_channel_endpoint.c:129`).
    pub socket_multicast_ttl: u8,
    /// Which strategy a multicast or multi-destination channel gets when it
    /// names no `fc=`
    /// (`aeron.multicast.flowcontrol.supplier`,
    /// `AERON_MULTICAST_FLOWCONTROL_SUPPLIER`).
    pub multicast_flow_control_supplier: Supplier,
    /// Which strategy a **unicast** channel gets, which never reads `fc=`
    /// (`aeron.unicast.flowcontrol.supplier`,
    /// `AERON_UNICAST_FLOWCONTROL_SUPPLIER`).
    pub unicast_flow_control_supplier: Supplier,
    /// How long an image may go without a packet before it drains, and how
    /// long an IPC publication's refusal lasts
    /// (`aeron.image.liveness.timeout`, `AERON_IMAGE_LIVENESS_TIMEOUT`).
    pub image_liveness_timeout_ns: i64,
    /// Which supplier builds an image's congestion-control strategy
    /// (`aeron.congestioncontrol.supplier`,
    /// `AERON_CONGESTIONCONTROL_SUPPLIER`) — `default`, `static` or `cubic`.
    ///
    /// The default is the **chooser**: with it, the channel's `cc=` decides,
    /// image by image. Naming one of the others makes every image that one.
    pub congestion_control_supplier: crate::congestion_control::Supplier,
    /// The raw `aeron.cubiccongestioncontrol.initialrtt`
    /// (`AERON_CUBICCONGESTIONCONTROL_INITIALRTT`, `aeronmd.h:351`).
    ///
    /// **A string, not a duration, and the shape is the reference's.** The
    /// three `aeron.cubiccongestioncontrol.*` settings are read by `getenv`
    /// **inside CUBIC's supplier**, not into the driver context
    /// (`aeron_congestion_control.c:392-402`) — so a value that is not a
    /// duration is not a driver that will not start: it is every CUBIC image
    /// failing to be created, which is what [`Self::cubic_initial_rtt_ns`]
    /// keeps. Every other timeout in this file is a context value in the
    /// reference and fails the start-up instead.
    pub cubic_initial_rtt: Option<String>,
    /// Whether CUBIC measures round trips at all
    /// (`aeron.cubiccongestioncontrol.measurertt`, `aeronmd.h:346`).
    ///
    /// Off by default, and that is the reference's default — an image that does
    /// not measure sends no RTTM and keeps the initial RTT for ever.
    pub cubic_measure_rtt: bool,
    /// Whether CUBIC takes the TCP-friendly window after a loss
    /// (`aeron.cubiccongestioncontrol.tcpmode`, `aeronmd.h:359`).
    pub cubic_tcp_mode: bool,
    /// The tag a `fc=tagged` channel that names no `g:` matches against
    /// (`aeron.flow.control.gtag`, `AERON_FLOW_CONTROL_GROUP_TAG`).
    pub flow_control_group_tag: i64,
    /// How many receivers a group needs before the sender limit moves
    /// (`aeron.flow.control.group.min.size`,
    /// `AERON_FLOW_CONTROL_GROUP_MIN_SIZE`).
    pub flow_control_group_min_size: i32,
    /// How long a receiver may go quiet before a group strategy drops it
    /// (`aeron.flow.control.receiver.timeout`,
    /// `AERON_FLOW_CONTROL_RECEIVER_TIMEOUT`).
    pub flow_control_receiver_timeout_ns: i64,
    /// The group tag a channel that names no `gtag=` gets
    /// (`aeron.receiver.group.tag`, `AERON_RECEIVER_GROUP_TAG`).
    ///
    /// It is the driver's half of the tag an endpoint stamps into its status
    /// messages; the channel's own `gtag=` wins where it names one
    /// (`aeron_receive_channel_endpoint_set_group_tag`, `:39-46`).
    pub receiver_group_tag: Option<i64>,
    /// What a subscription's `group=` does when it names nothing
    /// (`aeron.receiver.group.consideration`, `AERON_RECEIVER_GROUP_CONSIDERATION`;
    /// the default is `infer`, `aeron_driver_context.c:227`).
    ///
    /// It is the *default for the parameter*, not a driver-wide switch: a
    /// subscription that names `group=true` or `group=infer` overrules it.
    pub receiver_group_consideration: InferableBoolean,
    /// How many receivers a group is assumed to have
    /// (`aeron.nak.multicast.group.size = 10`, `AERON_NAK_MULTICAST_GROUP_SIZE`).
    ///
    /// It is the `group_size` of the log-normal a multicast image's NAK delays
    /// are drawn from: more receivers means more of them are likely to ask for
    /// the same gap, so each waits longer to let the others be the one that
    /// asks.
    pub nak_multicast_group_size: usize,
    /// The longest a multicast image waits before asking again
    /// (`aeron.nak.multicast.max.backoff = 10ms`,
    /// `AERON_NAK_MULTICAST_MAX_BACKOFF`). It is the *scale* of the same
    /// distribution, and so also its mean.
    pub nak_multicast_max_backoff_ns: i64,
    /// The window a receiver offers a publication when the channel named none
    /// (`aeron.rcv.initial.window.length`, which `aeronmd` turns into
    /// `AERON_RCV_INITIAL_WINDOW_LENGTH`,
    /// `aeron-driver/src/main/c/aeronmd.h:331`, read at
    /// `aeron_driver_context.c:834-840`; see
    /// [`RCV_INITIAL_WINDOW_LENGTH_DEFAULT`]).
    ///
    /// It is the window an *image* advertises in its status messages, and so
    /// the one the untethered state machine measures a reader's lag against
    /// (`aeron_publication_image.c:1180-1181`, three quarters of it).
    pub receiver_window_length: i32,
    /// How many datagrams one send batch may carry
    /// (`aeron.network.publication.max.messages.per.send`, default four, one
    /// to sixteen).
    pub network_publication_max_messages_per_send: usize,
    /// How many sender passes go by between two polls of the control sockets
    /// (`aeron.send.to.sm.poll.ratio`, [`SEND_TO_STATUS_POLL_RATIO_DEFAULT`]).
    pub send_to_sm_poll_ratio: u8,
    /// How long a *receiver* may hear nothing before it declares the sender
    /// gone (`aeron.rcv.status.message.timeout`, 200 milliseconds).
    pub status_message_timeout_ns: i64,
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
    /// Withhold one data frame in every this many from every send endpoint, so
    /// that a test can exercise loss recovery on a wire that does not lose
    /// anything (`debug.send.data.loss.drop.every`).
    ///
    /// **This has no counterpart in the reference.** Its debug loss surface is
    /// eight `AERON_DEBUG_{SEND,RECEIVE}_{DATA,CONTROL}_LOSS_{RATE,SEED}`
    /// variables read by an installer that only a C++ test calls
    /// (`media/aeron_debug_channel_endpoint_configuration.h:22-29`,
    /// `.c:126-172`) — a driver started as a process, which every interop test
    /// here is, has no way in. This build's own spelling is therefore a property of its own,
    /// and its value is a count rather than a rate: `None` injects nothing.
    /// `docs/compat.md` records the difference.
    pub data_loss_drop_every: Option<u64>,
    /// Hold every resolution this many milliseconds before it is answered
    /// (`debug.resolver.delay.millis`), so that a test can put a name that
    /// stalls under the driver and watch the conductor's own clock while it
    /// does.
    ///
    /// **This has no counterpart in the reference**, and it exists because of
    /// what it reproduces: a nameserver that does not answer. No setting on a
    /// driver can make one of those, and the property it is here to pin — that
    /// the conductor keeps publishing its heartbeat while a channel's names
    /// are being resolved — is otherwise only observable on a host whose
    /// resolver happens to be slow, which is a test that passes on the days it
    /// is not. Zero, the default, delays nothing. `docs/compat.md` records the
    /// difference, as it does for `data_loss_drop_every`.
    pub debug_resolver_delay_ms: u64,
    /// Which resolver this driver builds and keeps
    /// (`aeron.name.resolver.supplier`, `AERON_NAME_RESOLVER_SUPPLIER`,
    /// `aeronmd.h:808-809`, whose default is `default`) — `default`,
    /// `csv_table` or `driver`.
    ///
    /// A name that is not one of the three fails the driver's start-up, which
    /// is what the reference's `aeron_name_resolver_supplier_load` returning
    /// `NULL` does to its context init
    /// (`aeron_driver_context.c:588-594`).
    pub name_resolver_supplier: crate::name_resolver::Supplier,
    /// The raw `AERON_NAME_RESOLVER_INIT_ARGS` (`aeronmd.h:818`), which is the
    /// CSV table's configuration and nothing else's — read into the context by
    /// the reference (`aeron_driver_context.c:599`) and handed to whichever
    /// supplier was named.
    ///
    /// A string here rather than a parsed table, because who parses it depends
    /// on the supplier: the CSV table's own `init` does
    /// (`aeron_csv_table_name_resolver.c:110-160`), which is also why a table
    /// that will not parse fails the *resolver* and not the driver.
    pub name_resolver_init_args: Option<String>,
    /// What this driver answers to (`aeron.driver.resolver.name`,
    /// `AERON_DRIVER_RESOLVER_NAME`, `aeronmd.h:780`).
    ///
    /// Required as soon as an interface is named
    /// (`aeron_driver_context.c:601-608`), because a resolver that gossips
    /// without a name has nothing to announce.
    pub resolver_name: Option<String>,
    /// What the driver's resolver binds and announces
    /// (`aeron.driver.resolver.interface`, `AERON_DRIVER_RESOLVER_INTERFACE`,
    /// `aeronmd.h:790`).
    pub resolver_interface: Option<String>,
    /// Who to start gossiping with, comma-separated
    /// (`aeron.driver.resolver.bootstrap.neighbor`,
    /// `AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR`, `aeronmd.h:800`).
    pub resolver_bootstrap_neighbor: Option<String>,
    /// `aeron.driver.resolver.neighbor.timeout` (`AERON_DRIVER_RESOLVER_NEIGHBOR_TIMEOUT`),
    /// which is both how long a neighbor may go unheard-from and how long a
    /// cached name lives (`aeron_driver_name_resolver.c:463`).
    pub resolver_neighbor_timeout_ns: i64,
    /// `aeron.driver.resolver.self.resolution.interval`
    /// (`AERON_DRIVER_RESOLVER_SELF_RESOLUTION_INTERVAL`).
    pub resolver_self_resolution_interval_ns: i64,
    /// `aeron.driver.resolver.neighbor.resolution.interval`
    /// (`AERON_DRIVER_RESOLVER_NEIGHBOR_RESOLUTION_INTERVAL`).
    pub resolver_neighbor_resolution_interval_ns: i64,
    /// `aeron.driver.resolver.bootstrap.neighbor.resolution.interval`
    /// (`AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR_RESOLUTION_INTERVAL`).
    pub resolver_bootstrap_neighbor_resolution_interval_ns: i64,
    /// `aeron.name.resolver.threshold` (`AERON_DRIVER_NAME_RESOLVER_THRESHOLD`,
    /// `aeronmd.h:932`): how long one resolution may take before system counter
    /// 33 counts it (`aeron_driver_native_resource_agent.c:39-60`).
    pub name_resolver_threshold_ns: i64,
    /// `aeron.driver.reresolution.check.interval`
    /// (`AERON_DRIVER_RERESOLUTION_CHECK_INTERVAL`, `aeronmd.h:857`): how often
    /// the sender and the receiver look for names that need resolving again,
    /// and **zero to turn it off** (`aeron_driver_sender.c:183`).
    pub re_resolution_check_interval_ns: i64,
    /// `aeron.sender.wildcard.port.range`
    /// (`AERON_SENDER_WILDCARD_PORT_RANGE`, `aeronmd.h:878`): the ports a
    /// publication whose channel named port zero is given, and `0 0` to let the
    /// kernel choose (`aeron_driver_context.c:1058-1069`).
    pub sender_wildcard_port_range: PortRange,
    /// `aeron.receiver.wildcard.port.range`
    /// (`AERON_RECEIVER_WILDCARD_PORT_RANGE`, `aeronmd.h:888`), likewise, for
    /// the destinations a subscription listens on (`:1071-1081`).
    pub receiver_wildcard_port_range: PortRange,
    /// How the driver's work is spread over threads (`aeron.threading.mode`).
    pub threading_mode: ThreadingMode,
    /// Which set of names those threads are given (`aeron.thread.naming`).
    pub thread_naming: ThreadNaming,
    /// The conductor slot's idle strategy, and the five others beside it.
    /// Each is read when its runner is built, and a name that is not one of the
    /// six is refused there — the reference refuses at the same point
    /// (`aeron_driver_context.c:1152-1158`).
    pub conductor_idle: IdleStrategySetting,
    pub sender_idle: IdleStrategySetting,
    pub receiver_idle: IdleStrategySetting,
    pub shared_idle: IdleStrategySetting,
    pub shared_network_idle: IdleStrategySetting,
    pub native_resource_agent_idle: IdleStrategySetting,
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
            spies_simulate_connection: SPIES_SIMULATE_CONNECTION_DEFAULT,
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
            socket_multicast_ttl: SOCKET_MULTICAST_TTL_DEFAULT,
            receiver_group_consideration: RECEIVER_GROUP_CONSIDERATION_DEFAULT,
            receiver_group_tag: RECEIVER_GROUP_TAG_DEFAULT,
            image_liveness_timeout_ns: IMAGE_LIVENESS_TIMEOUT_NS_DEFAULT,
            multicast_flow_control_supplier: MULTICAST_FLOW_CONTROL_SUPPLIER_DEFAULT,
            unicast_flow_control_supplier: UNICAST_FLOW_CONTROL_SUPPLIER_DEFAULT,
            congestion_control_supplier: crate::congestion_control::Supplier::default(),
            cubic_initial_rtt: None,
            cubic_measure_rtt: false,
            cubic_tcp_mode: false,
            flow_control_group_tag: FLOW_CONTROL_GROUP_TAG_DEFAULT,
            flow_control_group_min_size: FLOW_CONTROL_GROUP_MIN_SIZE_DEFAULT,
            flow_control_receiver_timeout_ns: FLOW_CONTROL_RECEIVER_TIMEOUT_NS_DEFAULT,
            nak_multicast_group_size: NAK_MULTICAST_GROUP_SIZE_DEFAULT,
            nak_multicast_max_backoff_ns: NAK_MULTICAST_MAX_BACKOFF_NS_DEFAULT,
            receiver_window_length: RCV_INITIAL_WINDOW_LENGTH_DEFAULT,
            network_publication_max_messages_per_send:
                NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_DEFAULT,
            send_to_sm_poll_ratio: SEND_TO_STATUS_POLL_RATIO_DEFAULT,
            status_message_timeout_ns: RCV_STATUS_MESSAGE_TIMEOUT_NS_DEFAULT,
            publication_connection_timeout_ns: PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT,
            retransmit_unicast_delay_ns: RETRANSMIT_UNICAST_DELAY_NS_DEFAULT,
            retransmit_unicast_linger_ns: RETRANSMIT_UNICAST_LINGER_NS_DEFAULT,
            max_resend: MAX_RESEND_DEFAULT,
            data_loss_drop_every: None,
            debug_resolver_delay_ms: 0,
            stream_session_limit: STREAM_SESSION_LIMIT_DEFAULT,
            name_resolver_supplier: NAME_RESOLVER_SUPPLIER_DEFAULT,
            name_resolver_init_args: None,
            resolver_name: None,
            resolver_interface: None,
            resolver_bootstrap_neighbor: None,
            resolver_neighbor_timeout_ns: RESOLVER_NEIGHBOR_TIMEOUT_NS_DEFAULT,
            resolver_self_resolution_interval_ns: RESOLVER_SELF_RESOLUTION_INTERVAL_NS_DEFAULT,
            resolver_neighbor_resolution_interval_ns:
                RESOLVER_NEIGHBOR_RESOLUTION_INTERVAL_NS_DEFAULT,
            resolver_bootstrap_neighbor_resolution_interval_ns:
                RESOLVER_BOOTSTRAP_NEIGHBOR_RESOLUTION_INTERVAL_NS_DEFAULT,
            name_resolver_threshold_ns: NAME_RESOLVER_THRESHOLD_NS_DEFAULT,
            re_resolution_check_interval_ns: RERESOLUTION_CHECK_INTERVAL_NS_DEFAULT,
            // Both default to the kernel's wildcard, which is the state
            // `aeron_wildcard_port_manager_init` leaves them in
            // (`aeron_port_manager.c:49-52`): a driver that named no range is
            // not a driver that named `0 0`, but it behaves as one.
            sender_wildcard_port_range: PortRange::OS_WILDCARD,
            receiver_wildcard_port_range: PortRange::OS_WILDCARD,
            threading_mode: ThreadingMode::Dedicated,
            thread_naming: ThreadNaming::Classic,
            conductor_idle: IdleStrategySetting::default(),
            sender_idle: IdleStrategySetting::default(),
            receiver_idle: IdleStrategySetting::default(),
            shared_idle: IdleStrategySetting::default(),
            shared_network_idle: IdleStrategySetting::default(),
            native_resource_agent_idle: IdleStrategySetting::sleeping_default(),
        }
    }
}

/// How the driver's work is spread over threads
/// (`aeron_config_parse_threading_mode`, `aeron_driver_context.c:45-71`).
///
/// The four values are the reference's, and each names a set of runners
/// (`aeron_driver.c:1003-1122`): four threads, three, one, or none at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadingMode {
    /// One thread each, which is the reference's default.
    Dedicated,
    /// The sender and the receiver share one thread, which is what the
    /// reference's own system tests ask for.
    SharedNetwork,
    /// One thread for everything the driver does.
    Shared,
    /// No threads of the driver's own: the caller drives them, and the medium
    /// driver process **refuses to start with it**
    /// (`aeron_driver_start`, `aeron_driver.c:1225-1230`).
    Invoker,
}

impl ThreadingMode {
    pub const DEFAULT: Self = Self::Dedicated;

    /// The mode `value` names, or `None` for one the reference does not know —
    /// which it treats as a typo worth a warning and its default, and this
    /// build refuses, as it does every unparsable value.
    ///
    /// The match is the reference's own: exact and case-sensitive, because its
    /// `strncmp` compares the terminating byte too
    /// (`aeron_driver_context.c:49-68`).
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "DEDICATED" => Some(Self::Dedicated),
            "SHARED_NETWORK" => Some(Self::SharedNetwork),
            "SHARED" => Some(Self::Shared),
            "INVOKER" => Some(Self::Invoker),
            _ => None,
        }
    }

    /// Whether this mode leaves the driver's work to the process's own threads.
    pub const fn is_invoker(self) -> bool {
        matches!(self, Self::Invoker)
    }

    /// The name the reference prints for it (`aeron_driver_threading_mode_to_string`,
    /// `aeron_driver.c:487-503`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dedicated => "DEDICATED",
            Self::SharedNetwork => "SHARED_NETWORK",
            Self::Shared => "SHARED",
            Self::Invoker => "INVOKER",
        }
    }
}

/// The two spellings of the driver's thread names
/// (`aeron_driver_context.h:37-48`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadNaming {
    /// `conductor`, `receiver`, `sender`, `aeron-md-nra`, and the two bracketed
    /// names a shared runner uses — the reference's default.
    Classic,
    /// The same roles under the short `aeron-md-*` names.
    New,
}

impl ThreadNaming {
    pub const DEFAULT: Self = Self::Classic;

    /// The naming `value` names, or `None` for one the reference does not know
    /// (`aeron_config_parse_thread_naming`, `aeron_driver_context.c:76-99`).
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "classic" => Some(Self::Classic),
            "new" => Some(Self::New),
            _ => None,
        }
    }
}

/// One slot's idle strategy: the name it answers to and the init args beside
/// it, which the reference reads as two separate settings
/// (`aeron_driver_context.c:1150-1200`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdleStrategySetting {
    /// The name, one of the six `aeron_idle_strategy_load` knows
    /// (`aeron_agent.c:286-294`).
    pub name: String,
    /// What that strategy's initialiser is given, if anything.
    pub init_args: Option<String>,
}

impl IdleStrategySetting {
    /// What five of the driver's six slots default to
    /// (`aeron_driver_context.c:1143-1147`).
    pub const BACKOFF_DEFAULT: &'static str = "backoff";
    /// What the native resource agent defaults to (`:1148`) — it maps files and
    /// resolves names on its own thread, and spinning there is a core nobody
    /// can use.
    pub const NATIVE_RESOURCE_AGENT_DEFAULT_NAME: &'static str = "sleep-ns";

    /// The strategy this setting names, or `None` when the name is not one of
    /// the six — which the reference refuses to start over
    /// (`aeron_driver_context.c:1152-1158`).
    pub fn strategy(&self) -> Option<crate::idle::Strategy> {
        crate::idle::Strategy::by_name(&self.name, self.init_args.as_deref())
    }
}

/// The native resource agent's default, which is the one slot that is not
/// backoff (`aeron_driver_context.c:1148`).
impl IdleStrategySetting {
    pub fn sleeping_default() -> Self {
        Self {
            name: Self::NATIVE_RESOURCE_AGENT_DEFAULT_NAME.to_owned(),
            init_args: None,
        }
    }
}

impl Default for IdleStrategySetting {
    fn default() -> Self {
        Self {
            name: Self::BACKOFF_DEFAULT.to_owned(),
            init_args: None,
        }
    }
}

impl DriverConfig {
    /// CUBIC's initial RTT as a duration — the property if it named one, the
    /// reference's 100 µs otherwise (`aeron_congestion_control.c:38`,
    /// `:396-402`).
    ///
    /// # Returns
    ///
    /// `None` when the property named something that is not a duration, which
    /// is the supplier's failure and not the driver's.
    pub fn cubic_initial_rtt_ns(&self) -> Option<i64> {
        match self.cubic_initial_rtt.as_deref() {
            None => Some(crate::congestion_control::INITIAL_RTT_NS_DEFAULT),
            Some(value) => cubic_duration(value),
        }
    }

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

        // The network side's settings, each bounded the way the reference
        // bounds it in `aeron_driver_context.c`. The bounds are the reference's
        // `min`/`max` arguments rather than this build's own invention: a
        // deployment that names a value the reference would have refused gets
        // the same refusal here, and one it would have accepted gets in.
        if let Some(value) = get(&Setting::TERM_BUFFER_LENGTH) {
            config.term_buffer_length = parse_bounded_size32(
                &Setting::TERM_BUFFER_LENGTH,
                &value,
                1024,
                u64::try_from(i32::MAX).unwrap_or(u64::MAX),
            )?;
        }
        if let Some(value) = get(&Setting::MTU_LENGTH) {
            config.mtu_length = parse_bounded_size32(
                &Setting::MTU_LENGTH,
                &value,
                u64::try_from(crate::protocol::DataFrame::LENGTH).unwrap_or(u64::MAX),
                crate::publication_params::MAX_UDP_PAYLOAD_LENGTH,
            )?;
        }
        if let Some(value) = get(&Setting::PUBLICATION_WINDOW_LENGTH) {
            config.publication_window_length = parse_bounded_size32(
                &Setting::PUBLICATION_WINDOW_LENGTH,
                &value,
                0,
                u64::try_from(deepmsg_core::logbuffer::descriptor::TERM_MAX_LENGTH)
                    .unwrap_or(u64::MAX),
            )?;
        }
        if let Some(value) = get(&Setting::SOCKET_SO_RCVBUF) {
            config.socket_so_rcvbuf = parse_bounded_size32(
                &Setting::SOCKET_SO_RCVBUF,
                &value,
                0,
                u64::try_from(i32::MAX).unwrap_or(u64::MAX),
            )?;
        }
        if let Some(value) = get(&Setting::SOCKET_SO_SNDBUF) {
            config.socket_so_sndbuf = parse_bounded_size32(
                &Setting::SOCKET_SO_SNDBUF,
                &value,
                0,
                u64::try_from(i32::MAX).unwrap_or(u64::MAX),
            )?;
        }
        if let Some(value) = get(&Setting::SOCKET_MULTICAST_TTL) {
            // The reference's bounds are a `uint8_t`'s: `0..255`, refused
            // rather than clamped (`aeron_driver_context.c:768-772`).
            config.socket_multicast_ttl = u8::try_from(parse_bounded_size32(
                &Setting::SOCKET_MULTICAST_TTL,
                &value,
                0,
                255,
            )?)
            .unwrap_or(SOCKET_MULTICAST_TTL_DEFAULT);
        }
        if let Some(value) = get(&Setting::NAK_MULTICAST_GROUP_SIZE) {
            // Bounded where the reference bounds it (`:938-943`, one to
            // `INT32_MAX`): a group of nobody is a backoff with no scale.
            config.nak_multicast_group_size = usize::try_from(parse_bounded_size32(
                &Setting::NAK_MULTICAST_GROUP_SIZE,
                &value,
                1,
                u64::try_from(i32::MAX).unwrap_or(u64::MAX),
            )?)
            .unwrap_or(NAK_MULTICAST_GROUP_SIZE_DEFAULT);
        }
        if let Some(value) = get(&Setting::NAK_MULTICAST_MAX_BACKOFF) {
            // The reference's lower bound is a microsecond (`:945-950`);
            // above it there is none worth naming.
            let backoff = parse_duration_ns(&Setting::NAK_MULTICAST_MAX_BACKOFF, &value)?;
            if backoff < NAK_MULTICAST_MAX_BACKOFF_NS_MIN {
                return Err(ConfigError::OutOfRange {
                    name: Setting::NAK_MULTICAST_MAX_BACKOFF.property,
                    value,
                });
            }
            config.nak_multicast_max_backoff_ns = backoff;
        }
        if let Some(value) = get(&Setting::MULTICAST_FLOWCONTROL_SUPPLIER) {
            config.multicast_flow_control_supplier =
                parse_supplier(&Setting::MULTICAST_FLOWCONTROL_SUPPLIER, &value)?;
        }
        if let Some(value) = get(&Setting::UNICAST_FLOWCONTROL_SUPPLIER) {
            config.unicast_flow_control_supplier =
                parse_supplier(&Setting::UNICAST_FLOWCONTROL_SUPPLIER, &value)?;
        }
        // The three cubic settings, carried rather than parsed: see
        // `cubic_initial_rtt`. The two booleans are parsed here like every
        // other boolean property, which is the one place this build is
        // stricter than the reference's prefix comparison (see `parse_bool`).
        if let Some(value) = get(&Setting::CONGESTIONCONTROL_SUPPLIER) {
            // A name the reference's table does not carry is a **driver that
            // does not start** (`aeron_driver_context.c:579-585`'s `goto
            // error`), not an image that fails later.
            config.congestion_control_supplier =
                parse_congestion_control_supplier(&Setting::CONGESTIONCONTROL_SUPPLIER, &value)?;
        }
        if let Some(value) = get(&Setting::CUBIC_INITIAL_RTT) {
            config.cubic_initial_rtt = Some(value);
        }
        if let Some(value) = get(&Setting::CUBIC_MEASURE_RTT) {
            config.cubic_measure_rtt = parse_bool(&Setting::CUBIC_MEASURE_RTT, &value)?;
        }
        if let Some(value) = get(&Setting::CUBIC_TCP_MODE) {
            config.cubic_tcp_mode = parse_bool(&Setting::CUBIC_TCP_MODE, &value)?;
        }
        if let Some(value) = get(&Setting::IMAGE_LIVENESS_TIMEOUT) {
            config.image_liveness_timeout_ns =
                parse_duration_ns(&Setting::IMAGE_LIVENESS_TIMEOUT, &value)?;
        }
        if let Some(value) = get(&Setting::THREADING_MODE) {
            config.threading_mode =
                ThreadingMode::parse(&value).ok_or(ConfigError::UnknownThreadingMode {
                    value: value.clone(),
                })?;
        }
        if let Some(value) = get(&Setting::THREAD_NAMING) {
            config.thread_naming =
                ThreadNaming::parse(&value).ok_or(ConfigError::UnknownThreadNaming {
                    value: value.clone(),
                })?;
        }

        config.conductor_idle = idle_strategy(
            &get,
            IdleStrategySetting::BACKOFF_DEFAULT,
            &Setting::CONDUCTOR_IDLE_STRATEGY,
            &Setting::CONDUCTOR_IDLE_STRATEGY_INIT_ARGS,
        )?;
        config.sender_idle = idle_strategy(
            &get,
            IdleStrategySetting::BACKOFF_DEFAULT,
            &Setting::SENDER_IDLE_STRATEGY,
            &Setting::SENDER_IDLE_STRATEGY_INIT_ARGS,
        )?;
        config.receiver_idle = idle_strategy(
            &get,
            IdleStrategySetting::BACKOFF_DEFAULT,
            &Setting::RECEIVER_IDLE_STRATEGY,
            &Setting::RECEIVER_IDLE_STRATEGY_INIT_ARGS,
        )?;
        config.shared_idle = idle_strategy(
            &get,
            IdleStrategySetting::BACKOFF_DEFAULT,
            &Setting::SHARED_IDLE_STRATEGY,
            &Setting::SHARED_IDLE_STRATEGY_INIT_ARGS,
        )?;
        config.shared_network_idle = idle_strategy(
            &get,
            IdleStrategySetting::BACKOFF_DEFAULT,
            &Setting::SHARED_NETWORK_IDLE_STRATEGY,
            &Setting::SHARED_NETWORK_IDLE_STRATEGY_INIT_ARGS,
        )?;
        config.native_resource_agent_idle = idle_strategy(
            &get,
            IdleStrategySetting::NATIVE_RESOURCE_AGENT_DEFAULT_NAME,
            &Setting::NATIVE_RESOURCE_AGENT_IDLE_STRATEGY,
            &Setting::NATIVE_RESOURCE_AGENT_IDLE_STRATEGY_INIT_ARGS,
        )?;
        if let Some(value) = get(&Setting::FLOW_CONTROL_GROUP_TAG) {
            config.flow_control_group_tag = parse_count(&Setting::FLOW_CONTROL_GROUP_TAG, &value)?;
        }
        if let Some(value) = get(&Setting::FLOW_CONTROL_GROUP_MIN_SIZE) {
            // `aeron_config_parse_int32` refuses a value the type cannot hold
            // rather than wrapping it (`aeron_driver_context.c:249-290`).
            config.flow_control_group_min_size =
                i32::try_from(parse_count(&Setting::FLOW_CONTROL_GROUP_MIN_SIZE, &value)?)
                    .map_err(|_| ConfigError::OutOfRange {
                        name: Setting::FLOW_CONTROL_GROUP_MIN_SIZE.property,
                        value: value.clone(),
                    })?;
        }
        if let Some(value) = get(&Setting::FLOW_CONTROL_RECEIVER_TIMEOUT) {
            config.flow_control_receiver_timeout_ns =
                parse_duration_ns(&Setting::FLOW_CONTROL_RECEIVER_TIMEOUT, &value)?;
        }
        if let Some(value) = get(&Setting::RECEIVER_GROUP_TAG) {
            config.receiver_group_tag = Some(parse_count(&Setting::RECEIVER_GROUP_TAG, &value)?);
        }
        if let Some(value) = get(&Setting::RECEIVER_GROUP_CONSIDERATION) {
            config.receiver_group_consideration =
                InferableBoolean::parse(Some(&value), RECEIVER_GROUP_CONSIDERATION_DEFAULT);
        }
        if let Some(value) = get(&Setting::RCV_INITIAL_WINDOW_LENGTH) {
            config.receiver_window_length = parse_bounded_size32(
                &Setting::RCV_INITIAL_WINDOW_LENGTH,
                &value,
                256,
                u64::try_from(i32::MAX).unwrap_or(u64::MAX),
            )?;
        }
        if let Some(value) = get(&Setting::RCV_STATUS_MESSAGE_TIMEOUT) {
            let timeout = parse_duration_ns(&Setting::RCV_STATUS_MESSAGE_TIMEOUT, &value)?;
            if timeout < 1_000 {
                return Err(ConfigError::OutOfRange {
                    name: Setting::RCV_STATUS_MESSAGE_TIMEOUT.property,
                    value,
                });
            }
            config.status_message_timeout_ns = timeout;
        }
        if let Some(value) = get(&Setting::SEND_TO_STATUS_POLL_RATIO) {
            // The reference reads up to `INT32_MAX` and casts to `uint8_t`, so
            // `256` arrives as zero polls between passes. This refuses it
            // instead: a value that means a different value is worse than an
            // error (the same rule `parse_bool` follows).
            let ratio = parse_count(&Setting::SEND_TO_STATUS_POLL_RATIO, &value)?;
            if !(1..=i64::from(u8::MAX)).contains(&ratio) {
                return Err(ConfigError::OutOfRange {
                    name: Setting::SEND_TO_STATUS_POLL_RATIO.property,
                    value,
                });
            }
            config.send_to_sm_poll_ratio = u8::try_from(ratio).unwrap_or(1);
        }
        if let Some(value) = get(&Setting::SPIES_SIMULATE_CONNECTION) {
            config.spies_simulate_connection =
                parse_bool(&Setting::SPIES_SIMULATE_CONNECTION, &value)?;
        }
        if let Some(value) = get(&Setting::MAX_RESEND) {
            // `AERON_RETRANSMIT_HANDLER_MAX_RESEND_MAX` (`aeron_retransmit_handler.h:43`).
            let resend = parse_count(&Setting::MAX_RESEND, &value)?;
            if !(1..=256).contains(&resend) {
                return Err(ConfigError::OutOfRange {
                    name: Setting::MAX_RESEND.property,
                    value,
                });
            }
            #[allow(clippy::cast_possible_truncation)] // bounded by 256 above
            {
                config.max_resend = resend as i32;
            }
        }

        if let Some(value) = get(&Setting::DEBUG_RESOLVER_DELAY_MILLIS) {
            // Zero delays nothing, which is what leaving it unset means; the
            // count is milliseconds, and a negative one fails the conversion
            // rather than wrapping into a long delay.
            match u64::try_from(parse_count(&Setting::DEBUG_RESOLVER_DELAY_MILLIS, &value)?) {
                Ok(delay) => config.debug_resolver_delay_ms = delay,
                Err(_) => {
                    return Err(ConfigError::OutOfRange {
                        name: Setting::DEBUG_RESOLVER_DELAY_MILLIS.property,
                        value,
                    });
                }
            }
        }

        if let Some(value) = get(&Setting::DATA_LOSS_DROP_EVERY) {
            // Zero is "inject nothing", the same as leaving it unset; one
            // would withhold every frame, which no test means and a typo can
            // produce. A rate of one is refused here rather than obeyed, and
            // a negative count falls out of the conversion the same way.
            match u64::try_from(parse_count(&Setting::DATA_LOSS_DROP_EVERY, &value)?) {
                Ok(0) => {}
                Ok(1) | Err(_) => {
                    return Err(ConfigError::OutOfRange {
                        name: Setting::DATA_LOSS_DROP_EVERY.property,
                        value,
                    });
                }
                Ok(drop_every) => config.data_loss_drop_every = Some(drop_every),
            }
        }

        if let Some(value) = get(&Setting::NAME_RESOLVER_SUPPLIER) {
            config.name_resolver_supplier = crate::name_resolver::Supplier::from_name(&value)
                .ok_or_else(|| ConfigError::UnknownSupplier {
                    name: Setting::NAME_RESOLVER_SUPPLIER.property,
                    value: value.clone(),
                })?;
        }

        // Carried as it was written, not parsed: the CSV table's own `init`
        // reads it (`aeron_csv_table_name_resolver.c:110-160`), and a table
        // that will not parse is a resolver that will not build rather than a
        // driver that will not start (`aeron_driver_context.c:599` reads the
        // string and nothing else).
        config.name_resolver_init_args = get(&Setting::NAME_RESOLVER_INIT_ARGS);
        config.resolver_name = get(&Setting::DRIVER_RESOLVER_NAME);
        config.resolver_interface = get(&Setting::DRIVER_RESOLVER_INTERFACE);
        config.resolver_bootstrap_neighbor = get(&Setting::DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR);

        for (setting, field) in [
            (
                &Setting::DRIVER_RESOLVER_NEIGHBOR_TIMEOUT,
                &mut config.resolver_neighbor_timeout_ns,
            ),
            (
                &Setting::DRIVER_RESOLVER_SELF_RESOLUTION_INTERVAL,
                &mut config.resolver_self_resolution_interval_ns,
            ),
            (
                &Setting::DRIVER_RESOLVER_NEIGHBOR_RESOLUTION_INTERVAL,
                &mut config.resolver_neighbor_resolution_interval_ns,
            ),
            (
                &Setting::DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR_RESOLUTION_INTERVAL,
                &mut config.resolver_bootstrap_neighbor_resolution_interval_ns,
            ),
        ] {
            if let Some(value) = get(setting) {
                let parsed = parse_duration_ns(setting, &value)?;

                // The four intervals are read with a floor of a millisecond
                // (`aeron_config_parse_duration_ns(..., 1000 * 1000, INT64_MAX)`,
                // `aeron_driver_context.c:609-636`), which is what stops a
                // resolver that gossips in a busy loop.
                if parsed < RESOLVER_INTERVAL_NS_MIN {
                    return Err(ConfigError::OutOfRange {
                        name: setting.property,
                        value,
                    });
                }

                *field = parsed;
            }
        }

        // The threshold is read with **no** floor (`:1049-1055`), which is not
        // an oversight in the reference: the test that counts what a slow
        // resolution costs sets it to one nanosecond
        // (`NameReResolutionTest.java`, `nameResolverThresholdNs(1)`).
        if let Some(value) = get(&Setting::DRIVER_NAME_RESOLVER_THRESHOLD) {
            config.name_resolver_threshold_ns =
                parse_duration_ns(&Setting::DRIVER_NAME_RESOLVER_THRESHOLD, &value)?;
        }

        // And so is the re-resolution interval, whose minimum is zero for the
        // same reason (`:1024-1028`): zero is how a deployment turns the whole
        // feature off, and the reference's two loops test for it
        // (`aeron_driver_sender.c:183`).
        if let Some(value) = get(&Setting::DRIVER_RERESOLUTION_CHECK_INTERVAL) {
            config.re_resolution_check_interval_ns =
                parse_duration_ns(&Setting::DRIVER_RERESOLUTION_CHECK_INTERVAL, &value)?;
        }

        // The two wildcard ranges are read as text and parsed here, which is
        // where the reference reads them too: a range that will not parse
        // fails the driver's start-up rather than the channel that needed it
        // (`aeron_driver_context.c:1058-1082`, whose `goto error` is the whole
        // of what its `AERON_APPEND_ERR` is for).
        for (setting, field) in [
            (
                &Setting::SENDER_WILDCARD_PORT_RANGE,
                &mut config.sender_wildcard_port_range,
            ),
            (
                &Setting::RECEIVER_WILDCARD_PORT_RANGE,
                &mut config.receiver_wildcard_port_range,
            ),
        ] {
            if let Some(value) = get(setting) {
                *field = PortRange::parse(&value).map_err(|reason| ConfigError::PortRange {
                    name: setting.property,
                    value,
                    reason,
                })?;
            }
        }

        // A resolver that gossips needs a name: its own name is what it
        // announces, and the reference refuses the start-up rather than
        // running one that can only answer other people's questions
        // (`aeron_driver_context.c:601-608`, the only cross-field check in
        // this group).
        if config.resolver_interface.is_some()
            && config
                .resolver_name
                .as_ref()
                .is_none_or(|name| name.is_empty())
        {
            return Err(ConfigError::ResolverNameRequired);
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
    /// `aeron.term.buffer.length`: the length of one term of a *network*
    /// publication's log buffer (`aeronmd.h:137`, read at
    /// `aeron_driver_context.c:712-717`, bounds 1024 to `INT32_MAX`).
    const TERM_BUFFER_LENGTH: Self = Self {
        property: "term.buffer.length",
        env: "AERON_TERM_BUFFER_LENGTH",
    };
    /// `aeron.mtu.length`: the largest frame a network publication writes
    /// (`aeronmd.h:193`, read at `:726-731`, bounded by the data header and the
    /// largest UDP payload).
    const MTU_LENGTH: Self = Self {
        property: "mtu.length",
        env: "AERON_MTU_LENGTH",
    };
    /// `aeron.publication.term.window.length`: how far ahead of its slowest
    /// reader a network producer may run (`aeronmd.h:217`, read at `:747-752`,
    /// zero meaning half a term).
    const PUBLICATION_WINDOW_LENGTH: Self = Self {
        property: "publication.term.window.length",
        env: "AERON_PUBLICATION_TERM_WINDOW_LENGTH",
    };
    /// `aeron.socket.so.rcvbuf`: the socket buffer a channel that named none
    /// gets (`aeronmd.h:233`, read at `:754-759`).
    const SOCKET_SO_RCVBUF: Self = Self {
        property: "socket.so.rcvbuf",
        env: "AERON_SOCKET_SO_RCVBUF",
    };
    /// `aeron.socket.so.sndbuf` (`aeronmd.h:241`, read at `:761-766`).
    const SOCKET_SO_SNDBUF: Self = Self {
        property: "socket.so.sndbuf",
        env: "AERON_SOCKET_SO_SNDBUF",
    };
    /// `aeron.socket.multicast.ttl` (`aeronmd.h:249`, read at `:768-772`).
    const SOCKET_MULTICAST_TTL: Self = Self {
        property: "socket.multicast.ttl",
        env: "AERON_SOCKET_MULTICAST_TTL",
    };
    /// `aeron.nak.multicast.group.size` (`aeronmd.h:645`, read at `:938-943`).
    const NAK_MULTICAST_GROUP_SIZE: Self = Self {
        property: "nak.multicast.group.size",
        env: "AERON_NAK_MULTICAST_GROUP_SIZE",
    };
    /// `aeron.nak.multicast.max.backoff` (`aeronmd.h:653`, read at `:945-950`).
    const NAK_MULTICAST_MAX_BACKOFF: Self = Self {
        property: "nak.multicast.max.backoff",
        env: "AERON_NAK_MULTICAST_MAX_BACKOFF",
    };
    /// `aeron.receiver.group.consideration` (`aeronmd.h:701`, read at
    /// `:451-452` — environment only, with no property read in the reference;
    /// this build reads the property too, as it does for every other name).
    /// `aeron.multicast.flowcontrol.supplier`
    /// (`aeronmd.h:684-690`).
    const MULTICAST_FLOWCONTROL_SUPPLIER: Self = Self {
        property: "multicast.flowcontrol.supplier",
        env: "AERON_MULTICAST_FLOWCONTROL_SUPPLIER",
    };
    /// `aeron.unicast.flowcontrol.supplier` (`aeronmd.h:676-682`).
    const UNICAST_FLOWCONTROL_SUPPLIER: Self = Self {
        property: "unicast.flowcontrol.supplier",
        env: "AERON_UNICAST_FLOWCONTROL_SUPPLIER",
    };
    /// `aeron.congestioncontrol.supplier`
    /// (`AERON_CONGESTIONCONTROL_SUPPLIER`, `aeronmd.h:339`).
    const CONGESTIONCONTROL_SUPPLIER: Self = Self {
        property: "congestioncontrol.supplier",
        env: "AERON_CONGESTIONCONTROL_SUPPLIER",
    };
    /// `aeron.name.resolver.supplier` (`aeronmd.h:808`): `default`,
    /// `csv_table` or `driver`.
    const NAME_RESOLVER_SUPPLIER: Self = Self {
        property: "name.resolver.supplier",
        env: "AERON_NAME_RESOLVER_SUPPLIER",
    };
    /// `aeron.name.resolver.init.args` (`aeronmd.h:818`), the CSV table's
    /// configuration — **environment only**, and deliberately: the reference
    /// reads it into the context with `getenv` and never a property
    /// (`aeron_driver_context.c:599`), and this build's `Setting` table answers
    /// to both names for every other setting.
    const NAME_RESOLVER_INIT_ARGS: Self = Self {
        property: "name.resolver.init.args",
        env: "AERON_NAME_RESOLVER_INIT_ARGS",
    };
    /// `aeron.driver.resolver.name` (`aeronmd.h:780`).
    const DRIVER_RESOLVER_NAME: Self = Self {
        property: "driver.resolver.name",
        env: "AERON_DRIVER_RESOLVER_NAME",
    };
    /// `aeron.driver.resolver.interface` (`aeronmd.h:790`).
    const DRIVER_RESOLVER_INTERFACE: Self = Self {
        property: "driver.resolver.interface",
        env: "AERON_DRIVER_RESOLVER_INTERFACE",
    };
    /// `aeron.sender.wildcard.port.range`
    /// (`AERON_SENDER_WILDCARD_PORT_RANGE`, `aeronmd.h:878`): **two numbers
    /// separated by a space**, which is what the reference's two `strtoll`
    /// calls make of it (`aeron_port_manager.c:176-211`).
    const SENDER_WILDCARD_PORT_RANGE: Self = Self {
        property: "sender.wildcard.port.range",
        env: "AERON_SENDER_WILDCARD_PORT_RANGE",
    };
    /// `aeron.receiver.wildcard.port.range`
    /// (`AERON_RECEIVER_WILDCARD_PORT_RANGE`, `aeronmd.h:888`).
    const RECEIVER_WILDCARD_PORT_RANGE: Self = Self {
        property: "receiver.wildcard.port.range",
        env: "AERON_RECEIVER_WILDCARD_PORT_RANGE",
    };
    /// `aeron.driver.resolver.bootstrap.neighbor` (`aeronmd.h:800`).
    const DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR: Self = Self {
        property: "driver.resolver.bootstrap.neighbor",
        env: "AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR",
    };
    /// `aeron.driver.resolver.neighbor.timeout` (`aeronmd.h:827`).
    const DRIVER_RESOLVER_NEIGHBOR_TIMEOUT: Self = Self {
        property: "driver.resolver.neighbor.timeout",
        env: "AERON_DRIVER_RESOLVER_NEIGHBOR_TIMEOUT",
    };
    /// `aeron.driver.resolver.self.resolution.interval` (`aeronmd.h:834`).
    const DRIVER_RESOLVER_SELF_RESOLUTION_INTERVAL: Self = Self {
        property: "driver.resolver.self.resolution.interval",
        env: "AERON_DRIVER_RESOLVER_SELF_RESOLUTION_INTERVAL",
    };
    /// `aeron.driver.resolver.neighbor.resolution.interval` (`aeronmd.h:842`).
    const DRIVER_RESOLVER_NEIGHBOR_RESOLUTION_INTERVAL: Self = Self {
        property: "driver.resolver.neighbor.resolution.interval",
        env: "AERON_DRIVER_RESOLVER_NEIGHBOR_RESOLUTION_INTERVAL",
    };
    /// `aeron.driver.resolver.bootstrap.neighbor.resolution.interval`
    /// (`aeronmd.h:848`).
    const DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR_RESOLUTION_INTERVAL: Self = Self {
        property: "driver.resolver.bootstrap.neighbor.resolution.interval",
        env: "AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR_RESOLUTION_INTERVAL",
    };
    /// `aeron.driver.reresolution.check.interval` (`aeronmd.h:857`).
    const DRIVER_RERESOLUTION_CHECK_INTERVAL: Self = Self {
        property: "driver.reresolution.check.interval",
        env: "AERON_DRIVER_RERESOLUTION_CHECK_INTERVAL",
    };
    /// `aeron.name.resolver.threshold` (`aeronmd.h:932`).
    const DRIVER_NAME_RESOLVER_THRESHOLD: Self = Self {
        property: "name.resolver.threshold",
        env: "AERON_DRIVER_NAME_RESOLVER_THRESHOLD",
    };
    /// `aeron.cubiccongestioncontrol.initialrtt` (`aeronmd.h:351`).
    const CUBIC_INITIAL_RTT: Self = Self {
        property: "cubiccongestioncontrol.initialrtt",
        env: "AERON_CUBICCONGESTIONCONTROL_INITIALRTT",
    };
    /// `aeron.cubiccongestioncontrol.measurertt` (`aeronmd.h:346`).
    const CUBIC_MEASURE_RTT: Self = Self {
        property: "cubiccongestioncontrol.measurertt",
        env: "AERON_CUBICCONGESTIONCONTROL_MEASURERTT",
    };
    /// `aeron.cubiccongestioncontrol.tcpmode` (`aeronmd.h:359`).
    const CUBIC_TCP_MODE: Self = Self {
        property: "cubiccongestioncontrol.tcpmode",
        env: "AERON_CUBICCONGESTIONCONTROL_TCPMODE",
    };
    /// `aeron.image.liveness.timeout` (`aeronmd.h:323`).
    const IMAGE_LIVENESS_TIMEOUT: Self = Self {
        property: "image.liveness.timeout",
        env: "AERON_IMAGE_LIVENESS_TIMEOUT",
    };
    /// `aeron.threading.mode` (`aeronmd.h:54`).
    const THREADING_MODE: Self = Self {
        property: "threading.mode",
        env: "AERON_THREADING_MODE",
    };
    /// `aeron.thread.naming` (`aeronmd.h:68`).
    const THREAD_NAMING: Self = Self {
        property: "thread.naming",
        env: "AERON_THREAD_NAMING",
    };
    /// `aeron.conductor.idle.strategy` (`aeronmd.h:425`).
    const CONDUCTOR_IDLE_STRATEGY: Self = Self {
        property: "conductor.idle.strategy",
        env: "AERON_CONDUCTOR_IDLE_STRATEGY",
    };
    /// `aeron.conductor.idle.strategy.init.args` (`aeronmd.h:465`).
    const CONDUCTOR_IDLE_STRATEGY_INIT_ARGS: Self = Self {
        property: "conductor.idle.strategy.init.args",
        env: "AERON_CONDUCTOR_IDLE_STRATEGY_INIT_ARGS",
    };
    /// `aeron.sender.idle.strategy` (`aeronmd.h:417`).
    const SENDER_IDLE_STRATEGY: Self = Self {
        property: "sender.idle.strategy",
        env: "AERON_SENDER_IDLE_STRATEGY",
    };
    /// `aeron.sender.idle.strategy.init.args` (`aeronmd.h:457`).
    const SENDER_IDLE_STRATEGY_INIT_ARGS: Self = Self {
        property: "sender.idle.strategy.init.args",
        env: "AERON_SENDER_IDLE_STRATEGY_INIT_ARGS",
    };
    /// `aeron.receiver.idle.strategy` (`aeronmd.h:433`).
    const RECEIVER_IDLE_STRATEGY: Self = Self {
        property: "receiver.idle.strategy",
        env: "AERON_RECEIVER_IDLE_STRATEGY",
    };
    /// `aeron.receiver.idle.strategy.init.args` (`aeronmd.h:473`).
    const RECEIVER_IDLE_STRATEGY_INIT_ARGS: Self = Self {
        property: "receiver.idle.strategy.init.args",
        env: "AERON_RECEIVER_IDLE_STRATEGY_INIT_ARGS",
    };
    /// `aeron.shared.idle.strategy` (`aeronmd.h:449`).
    const SHARED_IDLE_STRATEGY: Self = Self {
        property: "shared.idle.strategy",
        env: "AERON_SHARED_IDLE_STRATEGY",
    };
    /// `aeron.shared.idle.strategy.init.args` (`aeronmd.h:489`).
    const SHARED_IDLE_STRATEGY_INIT_ARGS: Self = Self {
        property: "shared.idle.strategy.init.args",
        env: "AERON_SHARED_IDLE_STRATEGY_INIT_ARGS",
    };
    /// `aeron.sharednetwork.idle.strategy` (`aeronmd.h:441`) — the reference
    /// spells this one without the underscore between the two words, and its
    /// env var is what a deployment writes.
    const SHARED_NETWORK_IDLE_STRATEGY: Self = Self {
        property: "sharednetwork.idle.strategy",
        env: "AERON_SHAREDNETWORK_IDLE_STRATEGY",
    };
    /// `aeron.sharednetwork.idle.strategy.init.args` (`aeronmd.h:481`).
    const SHARED_NETWORK_IDLE_STRATEGY_INIT_ARGS: Self = Self {
        property: "sharednetwork.idle.strategy.init.args",
        env: "AERON_SHAREDNETWORK_IDLE_STRATEGY_INIT_ARGS",
    };
    /// `aeron.driver.native.resource.agent.idle.strategy` (`aeronmd.h:497`) —
    /// the one slot whose names carry the `driver.` infix, which is what its
    /// environment variable spells.
    const NATIVE_RESOURCE_AGENT_IDLE_STRATEGY: Self = Self {
        property: "driver.native.resource.agent.idle.strategy",
        env: "AERON_DRIVER_NATIVE_RESOURCE_AGENT_IDLE_STRATEGY",
    };
    /// `aeron.driver.native.resource.agent.idle.strategy.init.args`
    /// (`aeronmd.h:505`).
    const NATIVE_RESOURCE_AGENT_IDLE_STRATEGY_INIT_ARGS: Self = Self {
        property: "driver.native.resource.agent.idle.strategy.init.args",
        env: "AERON_DRIVER_NATIVE_RESOURCE_AGENT_IDLE_STRATEGY_INIT_ARGS",
    };
    /// `aeron.receiver.group.tag` (`aeronmd.h:558`).
    const RECEIVER_GROUP_TAG: Self = Self {
        property: "receiver.group.tag",
        env: "AERON_RECEIVER_GROUP_TAG",
    };
    /// `aeron.flow.control.gtag` (`aeronmd.h:542`).
    const FLOW_CONTROL_GROUP_TAG: Self = Self {
        property: "flow.control.gtag",
        env: "AERON_FLOW_CONTROL_GROUP_TAG",
    };
    /// `aeron.flow.control.group.min.size` (`:550`).
    const FLOW_CONTROL_GROUP_MIN_SIZE: Self = Self {
        property: "flow.control.group.min.size",
        env: "AERON_FLOW_CONTROL_GROUP_MIN_SIZE",
    };
    /// `aeron.flow.control.receiver.timeout` (`:533`).
    const FLOW_CONTROL_RECEIVER_TIMEOUT: Self = Self {
        property: "flow.control.receiver.timeout",
        env: "AERON_FLOW_CONTROL_RECEIVER_TIMEOUT",
    };
    const RECEIVER_GROUP_CONSIDERATION: Self = Self {
        property: "receiver.group.consideration",
        env: "AERON_RECEIVER_GROUP_CONSIDERATION",
    };
    /// `aeron.rcv.initial.window.length`: the window a receiver offers when the
    /// channel named none (`aeronmd.h:331`, read at `:834-839`).
    ///
    /// It is the window an image advertises, and so the one the untethered
    /// state machine measures a reader's lag against.
    const RCV_INITIAL_WINDOW_LENGTH: Self = Self {
        property: "rcv.initial.window.length",
        env: "AERON_RCV_INITIAL_WINDOW_LENGTH",
    };
    /// `aeron.rcv.status.message.timeout`: how long a receiver may hear nothing
    /// before it decides the sender is gone (`aeronmd.h:265`, read at
    /// `:820-825`).
    const RCV_STATUS_MESSAGE_TIMEOUT: Self = Self {
        property: "rcv.status.message.timeout",
        env: "AERON_RCV_STATUS_MESSAGE_TIMEOUT",
    };
    /// `aeron.send.to.status.poll.ratio`: how many sender passes go by between
    /// two polls of the control sockets (`aeronmd.h:257`, read at `:806-811`).
    const SEND_TO_STATUS_POLL_RATIO: Self = Self {
        property: "send.to.status.poll.ratio",
        env: "AERON_SEND_TO_STATUS_POLL_RATIO",
    };
    /// `aeron.spies.simulate.connection`: whether a publication counts its
    /// spies as receivers (`aeronmd.h:178`).
    const SPIES_SIMULATE_CONNECTION: Self = Self {
        property: "spies.simulate.connection",
        env: "AERON_SPIES_SIMULATE_CONNECTION",
    };
    /// `aeron.max.resend`: how many times a term may be retransmitted
    /// (`aeronmd.h:677`, read at `:916-921`, bounded by
    /// `AERON_RETRANSMIT_HANDLER_MAX_RESEND_MAX`).
    const MAX_RESEND: Self = Self {
        property: "max.resend",
        env: "AERON_MAX_RESEND",
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
    /// `deepmsg.debug.send.data.loss.drop.every`: withhold one data frame in
    /// every this many from each send endpoint.
    ///
    /// The one setting here with no reference variable to borrow, so it
    /// names its own — the same string the `DEEPMSG_` derivation produces,
    /// spelled out because the table is where a reader looks for a setting's
    /// names. See [`DriverConfig::data_loss_drop_every`] for why the
    /// reference's eight `AERON_DEBUG_*` variables are not what this reads.
    const DATA_LOSS_DROP_EVERY: Self = Self {
        property: "debug.send.data.loss.drop.every",
        env: "DEEPMSG_DEBUG_SEND_DATA_LOSS_DROP_EVERY",
    };
    /// `deepmsg.debug.resolver.delay.millis`: hold every resolution this long
    /// before answering it.
    ///
    /// The second setting here with no reference variable to borrow, for the
    /// reason [`DriverConfig::debug_resolver_delay_ms`] gives — what it
    /// reproduces is a nameserver that does not answer, which no driver
    /// setting can make happen.
    const DEBUG_RESOLVER_DELAY_MILLIS: Self = Self {
        property: "debug.resolver.delay.millis",
        env: "DEEPMSG_DEBUG_RESOLVER_DELAY_MILLIS",
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
    /// A supplier name the reference's symbol table does not know — flow
    /// control, congestion control or name resolution — which for the
    /// reference is a driver that does not start.
    UnknownSupplier {
        /// The setting, by property name.
        name: &'static str,
        /// What it was set to.
        value: String,
    },
    /// An interface was named for the driver's own resolver without a name for
    /// the driver to answer to, which the reference refuses
    /// (`aeron_driver_context.c:601-608`).
    ResolverNameRequired,
    /// An `aeron.threading.mode` the reference does not know
    /// (`aeron_config_parse_threading_mode`, `aeron_driver_context.c:45-71`).
    ///
    /// The reference warns and keeps its default; this refuses, which is the
    /// stance `docs/compat.md` records for every value it cannot parse.
    UnknownThreadingMode {
        /// What it was set to.
        value: String,
    },
    /// An `aeron.thread.naming` the reference does not know
    /// (`aeron_config_parse_thread_naming`, `:76-99`).
    UnknownThreadNaming {
        /// What it was set to.
        value: String,
    },
    /// An idle strategy name the reference's own table does not have, which
    /// for the reference is a driver that does not start
    /// (`aeron_driver_context.c:1152-1158`).
    UnknownIdleStrategy {
        /// The slot, by property name.
        name: &'static str,
        /// What it was set to.
        value: String,
    },
    /// A validator name the reference's symbol table does not know.
    UnknownValidator {
        /// What it was set to.
        value: String,
    },
    /// A wildcard port range the reference's parser would reject
    /// (`aeron_parse_port_range`, `aeron_port_manager.c:176-211`) — which for
    /// the reference is a driver that does not start
    /// (`aeron_driver_context.c:1062-1066`).
    PortRange {
        /// The setting, by property name.
        name: &'static str,
        /// What it was set to.
        value: String,
        /// Which of the three ways it was not a range.
        reason: PortRangeError,
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
            Self::UnknownSupplier { name, value } => {
                write!(f, "{name} is {value}, which names no supplier")
            }
            Self::UnknownThreadingMode { value } => write!(
                f,
                "{value} is not a threading mode: DEDICATED, SHARED_NETWORK, SHARED or INVOKER"
            ),
            Self::UnknownThreadNaming { value } => {
                write!(f, "{value} is not a thread naming: classic or new")
            }
            Self::UnknownIdleStrategy { name, value } => {
                write!(f, "{name} is {value}, which names no idle strategy")
            }
            Self::PortRange {
                name,
                value,
                reason,
            } => write!(f, "{name} is {value}, which is not a port range: {reason}"),
            Self::ResolverNameRequired => write!(
                f,
                "`resolverName` is required when `resolverInterface` is set"
            ),
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
            Self::PortRange { reason, .. } => Some(reason),
            Self::MalformedArgument { .. }
            | Self::MissingAeronDir { .. }
            | Self::NotABoolean { .. }
            | Self::NotANumber { .. }
            | Self::OutOfRange { .. }
            | Self::UnknownSupplier { .. }
            | Self::ResolverNameRequired
            | Self::UnknownValidator { .. }
            | Self::UnknownThreadingMode { .. }
            | Self::UnknownThreadNaming { .. }
            | Self::UnknownIdleStrategy { .. } => None,
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

/// A size setting with the reference's own bounds
/// (`aeron_config_parse_size64`'s `min` and `max` arguments,
/// `aeron-client/src/main/c/util/aeron_parse_util.c:170-268`), as the `i32` the
/// driver holds it in.
fn parse_bounded_size32(
    setting: &Setting,
    value: &str,
    min: u64,
    max: u64,
) -> Result<i32, ConfigError> {
    let parsed = u64::try_from(parse_size64(setting, value)?).unwrap_or(u64::MAX);

    if parsed < min || parsed > max {
        return Err(ConfigError::OutOfRange {
            name: setting.property,
            value: value.to_owned(),
        });
    }

    i32::try_from(parsed).map_err(|_| ConfigError::OutOfRange {
        name: setting.property,
        value: value.to_owned(),
    })
}

/// Parse a plain count, the way `aeron_config_parse_uint64` does: digits, and
/// nothing else.
/// A flow-control supplier, named the way the reference names it — its symbol,
/// or the short name beside it in the same table
/// (`aeron_flow_control_strategy_supplier_load`, `aeron_flow_control.c:70-79`).
///
/// # Errors
///
/// [`ConfigError::UnknownSupplier`] for a name the reference's own table does
/// not hold, which in the reference makes the context fail to initialise.
fn parse_supplier(setting: &Setting, value: &str) -> Result<Supplier, ConfigError> {
    Supplier::from_name(value).ok_or_else(|| ConfigError::UnknownSupplier {
        name: setting.property,
        value: value.to_owned(),
    })
}

/// The congestion-control supplier a property names
/// (`aeron_congestion_control_strategy_supplier_load`'s table,
/// `aeron_congestion_control.c:45-62`).
fn parse_congestion_control_supplier(
    setting: &Setting,
    value: &str,
) -> Result<crate::congestion_control::Supplier, ConfigError> {
    crate::congestion_control::Supplier::from_name(value).ok_or_else(|| {
        ConfigError::UnknownSupplier {
            name: setting.property,
            value: value.to_owned(),
        }
    })
}

fn parse_count(setting: &Setting, value: &str) -> Result<i64, ConfigError> {
    value.parse().map_err(|_| ConfigError::NotANumber {
        name: setting.property,
        value: value.to_owned(),
    })
}

/// Parse a duration in nanoseconds, with the reference's suffixes
/// (`aeron-client/src/main/c/util/aeron_parse_util.c:170-268`): `s`, `ms`,
/// `us`, `ns`, either case.
/// The duration a CUBIC property names, or `None` when it is not one.
///
/// Read lazily, at the point the reference reads it: CUBIC's supplier calls
/// `aeron_parse_duration_ns` on the `getenv` result and fails the image when it
/// will not parse (`aeron_congestion_control.c:394-402`). A driver whose start
/// depended on this value would be one the reference does not have.
pub(crate) fn cubic_duration(value: &str) -> Option<i64> {
    parse_duration_ns(&Setting::CUBIC_INITIAL_RTT, value).ok()
}

/// One slot's idle strategy from its two settings — the name and the init args
/// beside it, which the reference reads separately
/// (`aeron_driver_context.c:1150-1200`).
///
/// The name is checked **here**, at resolve time, because the reference checks
/// it at the same point and refuses to start over it: an unknown name makes its
/// loader return null and its context jump to the error arm (`:1152-1158`). A
/// typo in an idle strategy name is not a driver that idles differently.
fn idle_strategy(
    get: &impl Fn(&Setting) -> Option<String>,
    default_name: &str,
    name_setting: &Setting,
    args_setting: &Setting,
) -> Result<IdleStrategySetting, ConfigError> {
    let setting = IdleStrategySetting {
        name: get(name_setting).unwrap_or_else(|| default_name.to_owned()),
        init_args: get(args_setting),
    };

    match setting.strategy() {
        Some(_) => Ok(setting),
        None => Err(ConfigError::UnknownIdleStrategy {
            name: name_setting.property,
            value: setting.name,
        }),
    }
}

/// The same parse, without a setting to blame: `None` when the value is not a
/// duration. Used by the idle strategies' init args, which the reference reads
/// with this very function (`aeron_idle_strategy_sleeping_init_args`,
/// `aeron_agent.c:47-66`).
pub(crate) fn duration_ns(value: &str) -> Option<i64> {
    let lower = value.to_ascii_lowercase();
    let (digits, multiplier) = split_duration(&lower);

    digits.parse::<i64>().ok()?.checked_mul(multiplier)
}

/// A duration's digits and the multiplier its suffix asks for
/// (`aeron_parse_duration_ns`, `util/aeron_parse_util.c:193-266`).
///
/// No suffix at all is nanoseconds — and a suffix that is not one of these
/// leaves the letters in the digits, which is what makes the caller's parse
/// fail on `1m` or `1x` rather than read them as a number.
fn split_duration(value: &str) -> (&str, i64) {
    if let Some(head) = value.strip_suffix("ns") {
        (head, 1)
    } else if let Some(head) = value.strip_suffix("us") {
        (head, 1_000)
    } else if let Some(head) = value.strip_suffix("ms") {
        (head, 1_000_000)
    } else if let Some(head) = value.strip_suffix('s') {
        (head, 1_000_000_000)
    } else {
        (value, 1)
    }
}

fn parse_duration_ns(setting: &Setting, value: &str) -> Result<i64, ConfigError> {
    let lower = value.to_ascii_lowercase();
    let (digits, multiplier) = split_duration(&lower);

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

    /// The two wildcard port ranges, under both of the names every setting
    /// answers to — including the property the Java client writes, which is
    /// the one a system test sets (`WildcardPortManagerSystemTest.java:68-69`).
    #[test]
    fn the_wildcard_port_ranges_are_read_and_a_bad_one_stops_the_driver() {
        let config = resolve_with_env(
            &[
                ("deepmsg.dir", "/tmp/aeron"),
                ("aeron.sender.wildcard.port.range", "20702 20702"),
            ],
            &[("AERON_RECEIVER_WILDCARD_PORT_RANGE", "20700 20701")],
        )
        .expect("a config");

        assert_eq!(
            PortRange {
                low: 20702,
                high: 20702
            },
            config.sender_wildcard_port_range
        );
        assert_eq!(
            PortRange {
                low: 20700,
                high: 20701
            },
            config.receiver_wildcard_port_range
        );

        // A driver that named none is the kernel's wildcard, not an empty
        // range: the manager leaves the zero alone.
        assert_eq!(
            PortRange::OS_WILDCARD,
            resolve(&[("deepmsg.dir", "/tmp/aeron")])
                .expect("a config")
                .receiver_wildcard_port_range
        );

        // The three ways a range is not one, and the one that would look right
        // to a reader who never read the parser.
        for (value, reason) in [
            ("20700-20701", PortRangeError::SecondPart),
            ("20700", PortRangeError::SecondPart),
            ("20701 20700", PortRangeError::LowAboveHigh),
        ] {
            let error = resolve(&[
                ("deepmsg.dir", "/tmp/aeron"),
                ("aeron.sender.wildcard.port.range", value),
            ])
            .expect_err("a range that is not one");

            assert_eq!(
                format!(
                    "sender.wildcard.port.range is {value}, which is not a port range: {reason}"
                ),
                error.to_string()
            );
        }
    }

    /// The resolver settings, under both of the names every setting answers
    /// to — and the two cross-field rules the reference has for them.
    #[test]
    fn the_resolver_settings_are_read_and_the_pair_that_must_agree_is_checked() {
        let config = resolve_with_env(
            &[("deepmsg.dir", "/tmp/aeron")],
            &[
                ("AERON_NAME_RESOLVER_SUPPLIER", "driver"),
                ("AERON_DRIVER_RESOLVER_NAME", "A"),
                ("AERON_DRIVER_RESOLVER_INTERFACE", "0.0.0.0:8050"),
                ("AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR", "localhost:8051"),
                ("AERON_DRIVER_RESOLVER_NEIGHBOR_TIMEOUT", "3s"),
                ("AERON_DRIVER_RESOLVER_SELF_RESOLUTION_INTERVAL", "250ms"),
                (
                    "AERON_DRIVER_RESOLVER_NEIGHBOR_RESOLUTION_INTERVAL",
                    "500ms",
                ),
                (
                    "AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR_RESOLUTION_INTERVAL",
                    "4s",
                ),
                ("AERON_DRIVER_NAME_RESOLVER_THRESHOLD", "1s"),
                ("AERON_NAME_RESOLVER_INIT_ARGS", "a,b,c"),
            ],
        )
        .expect("a config");

        assert_eq!(
            crate::name_resolver::Supplier::Driver,
            config.name_resolver_supplier
        );
        assert_eq!(Some("A".to_owned()), config.resolver_name);
        assert_eq!(Some("0.0.0.0:8050".to_owned()), config.resolver_interface);
        assert_eq!(
            Some("localhost:8051".to_owned()),
            config.resolver_bootstrap_neighbor
        );
        assert_eq!(3_000_000_000, config.resolver_neighbor_timeout_ns);
        assert_eq!(250_000_000, config.resolver_self_resolution_interval_ns);
        assert_eq!(500_000_000, config.resolver_neighbor_resolution_interval_ns);
        assert_eq!(
            4_000_000_000,
            config.resolver_bootstrap_neighbor_resolution_interval_ns
        );
        assert_eq!(1_000_000_000, config.name_resolver_threshold_ns);

        // The threshold has **no** floor, unlike the four intervals beside it:
        // the reference's own re-resolution test runs one at a nanosecond.
        let threshold = resolve_with_env(
            &[("deepmsg.dir", "/tmp/aeron")],
            &[("AERON_DRIVER_NAME_RESOLVER_THRESHOLD", "1")],
        )
        .expect("a config");
        assert_eq!(1, threshold.name_resolver_threshold_ns);
        assert_eq!(Some("a,b,c".to_owned()), config.name_resolver_init_args);

        // A name no table has is a driver that does not start — for this
        // supplier as for the two flow-control ones, which is why they share
        // an error.
        let unknown = resolve(&[
            ("deepmsg.dir", "/tmp/aeron"),
            ("aeron.name.resolver.supplier", "dns"),
        ])
        .expect_err("no such supplier");
        assert!(matches!(unknown, ConfigError::UnknownSupplier { .. }));

        // An interface with no name is the reference's own refusal
        // (`aeron_driver_context.c:601-608`): a resolver that gossips has to
        // say what it is called.
        let nameless = resolve_with_env(
            &[("deepmsg.dir", "/tmp/aeron")],
            &[("AERON_DRIVER_RESOLVER_INTERFACE", "0.0.0.0:8050")],
        )
        .expect_err("a name is required");
        assert!(matches!(nameless, ConfigError::ResolverNameRequired));

        // And every interval has a floor of a millisecond, because a resolver
        // that gossips faster than that is a busy loop
        // (`aeron_config_parse_duration_ns(..., 1000 * 1000, INT64_MAX)`).
        let too_fast = resolve_with_env(
            &[("deepmsg.dir", "/tmp/aeron")],
            &[("AERON_DRIVER_RESOLVER_SELF_RESOLUTION_INTERVAL", "1us")],
        )
        .expect_err("below the floor");
        assert!(matches!(too_fast, ConfigError::OutOfRange { .. }));
    }

    /// The three cubic settings are the only ones this driver carries as
    /// **strings** and parses where the supplier is built
    /// (`aeron_congestion_control.c:392-402`), and the supplier's own name is
    /// the one cubic setting that fails the *driver*: the reference's load
    /// returns `NULL` and its context init goes to `error`
    /// (`aeron_driver_context.c:579-585`).
    #[test]
    fn the_cubic_settings_are_carried_and_the_supplier_name_is_checked() {
        let config = resolve(&[
            ("deepmsg.dir", "/tmp/aeron"),
            ("aeron.cubiccongestioncontrol.initialrtt", "1s"),
            ("aeron.cubiccongestioncontrol.measurertt", "true"),
            ("aeron.cubiccongestioncontrol.tcpmode", "on"),
            ("aeron.congestioncontrol.supplier", "cubic"),
        ])
        .expect("a config");

        assert_eq!(Some("1s".to_owned()), config.cubic_initial_rtt);
        assert_eq!(Some(1_000_000_000), config.cubic_initial_rtt_ns());
        assert!(config.cubic_measure_rtt);
        assert!(config.cubic_tcp_mode);
        assert_eq!(
            crate::congestion_control::Supplier::Cubic,
            config.congestion_control_supplier
        );

        // A duration that will not parse is **not** an error here: it is an
        // image that will not be built, which is where the reference fails it.
        let unparseable = resolve(&[
            ("deepmsg.dir", "/tmp/aeron"),
            ("aeron.cubiccongestioncontrol.initialrtt", "soon"),
        ])
        .expect("a config");

        assert_eq!(Some("soon".to_owned()), unparseable.cubic_initial_rtt);
        assert_eq!(None, unparseable.cubic_initial_rtt_ns());

        // And a supplier this build cannot load stops the driver, as the
        // reference's does.
        assert!(matches!(
            resolve(&[
                ("deepmsg.dir", "/tmp/aeron"),
                (
                    "aeron.congestioncontrol.supplier",
                    "aeron_cubic_congestion_control_strategy_supplier"
                ),
            ]),
            Err(ConfigError::UnknownSupplier { .. })
        ));
    }

    #[test]
    fn a_loss_generator_is_installed_only_when_a_frame_count_was_asked_for() {
        // The one setting with no reference variable behind it: our property,
        // our spelling, and a count rather than the reference's rate.
        assert_eq!(
            Some(8),
            resolve_with_env(
                &[("deepmsg.dir", "/tmp/aeron")],
                &[("DEEPMSG_DEBUG_SEND_DATA_LOSS_DROP_EVERY", "8")]
            )
            .expect("a config")
            .data_loss_drop_every
        );

        assert_eq!(
            Option::None,
            resolve(&[("deepmsg.dir", "/tmp/aeron")])
                .expect("a config")
                .data_loss_drop_every,
            "a driver nobody configured injects nothing"
        );

        assert_eq!(
            Option::None,
            resolve(&[
                ("deepmsg.dir", "/tmp/aeron"),
                ("deepmsg.debug.send.data.loss.drop.every", "0")
            ])
            .expect("a config")
            .data_loss_drop_every,
            "zero is the same as unset"
        );
    }

    #[test]
    fn a_loss_rate_of_one_is_refused_rather_than_emptying_the_wire() {
        // Every frame dropped is a driver that sends nothing at all, which is
        // a typo or a missing digit rather than a configuration: it is worth a
        // refusal, not a silent outage.
        for value in ["1", "-1", "many"] {
            assert!(
                matches!(
                    resolve(&[
                        ("deepmsg.dir", "/tmp/aeron"),
                        ("deepmsg.debug.send.data.loss.drop.every", value)
                    ]),
                    Err(ConfigError::OutOfRange { .. } | ConfigError::NotANumber { .. })
                ),
                "{value:?} must not install a generator"
            );
        }
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
                // The network side's, which are the ones a deployment sets to
                // size a UDP stream: each name is the reference's own
                // (`aeronmd.h:137`, `:193`, `:217`, `:233`, `:241`, `:331`,
                // `:265`, `:257`, `:677`, `:178`).
                ("AERON_TERM_BUFFER_LENGTH", "1m"),
                ("AERON_MTU_LENGTH", "2048"),
                ("AERON_PUBLICATION_TERM_WINDOW_LENGTH", "64k"),
                ("AERON_SOCKET_SO_RCVBUF", "1m"),
                ("AERON_SOCKET_SO_SNDBUF", "512k"),
                ("AERON_RCV_INITIAL_WINDOW_LENGTH", "64k"),
                ("AERON_RCV_STATUS_MESSAGE_TIMEOUT", "1s"),
                ("AERON_SEND_TO_STATUS_POLL_RATIO", "3"),
                ("AERON_MAX_RESEND", "4"),
                ("AERON_SPIES_SIMULATE_CONNECTION", "true"),
                ("AERON_SOCKET_MULTICAST_TTL", "12"),
                ("AERON_RECEIVER_GROUP_CONSIDERATION", "true"),
                ("AERON_NAK_MULTICAST_GROUP_SIZE", "4"),
                ("AERON_NAK_MULTICAST_MAX_BACKOFF", "25ms"),
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

        assert_eq!(1024 * 1024, config.term_buffer_length);
        assert_eq!(2048, config.mtu_length);
        assert_eq!(64 * 1024, config.publication_window_length);
        assert_eq!(1024 * 1024, config.socket_so_rcvbuf);
        assert_eq!(512 * 1024, config.socket_so_sndbuf);
        assert_eq!(64 * 1024, config.receiver_window_length);
        assert_eq!(1_000_000_000, config.status_message_timeout_ns);
        assert_eq!(3, config.send_to_sm_poll_ratio);
        assert_eq!(4, config.max_resend);
        assert!(config.spies_simulate_connection);
        assert_eq!(12, config.socket_multicast_ttl);
        assert_eq!(
            InferableBoolean::ForceTrue,
            config.receiver_group_consideration
        );
    }

    #[test]
    fn the_group_consideration_is_read_the_way_the_reference_reads_it() {
        // `aeron_config_parse_inferable_boolean` (`aeron_driver_context.c:99-119`)
        // compares against the literal **including its terminator**, so these
        // are exact matches and not prefixes — `truex` is neither `true` nor
        // an error, it is the third answer.
        for (text, expected) in [
            ("true", InferableBoolean::ForceTrue),
            ("infer", InferableBoolean::Infer),
            ("false", InferableBoolean::ForceFalse),
            ("truex", InferableBoolean::ForceFalse),
            ("inferno", InferableBoolean::ForceFalse),
            ("", InferableBoolean::ForceFalse),
        ] {
            assert_eq!(
                expected,
                InferableBoolean::parse(Some(text), InferableBoolean::Infer),
                "{text}"
            );
        }

        assert_eq!(
            InferableBoolean::ForceTrue,
            InferableBoolean::parse(None, InferableBoolean::ForceTrue),
            "naming nothing is the driver's own consideration, whatever it is"
        );
        assert_eq!(
            RECEIVER_GROUP_CONSIDERATION_DEFAULT,
            DriverConfig::default().receiver_group_consideration
        );
    }

    #[test]
    fn the_network_settings_refuse_what_the_reference_refuses() {
        // Each of these is bounded in `aeron_driver_context.c` where it is
        // read, and the bound is the reference's rather than this build's:
        // `min`/`max` arguments (`:712-767`, `:806-839`, `:916-921`).
        for (property, value) in [
            ("term.buffer.length", "512"),         // below 1024
            ("mtu.length", "16"),                  // below the data header
            ("mtu.length", "65505"),               // above the largest UDP payload
            ("rcv.initial.window.length", "255"),  // below 256
            ("max.resend", "0"),                   // below one
            ("max.resend", "257"),                 // above 256
            ("nak.multicast.max.backoff", "999"),  // below a microsecond
            ("nak.multicast.group.size", "0"),     // below one
            ("send.to.status.poll.ratio", "0"),    // below one
            ("send.to.status.poll.ratio", "256"),  // the reference truncates this to zero
            ("rcv.status.message.timeout", "999"), // below a microsecond
        ] {
            let name = format!("deepmsg.{property}");
            assert!(
                matches!(
                    resolve(&[("deepmsg.dir", "/tmp/x"), (name.as_str(), value)]),
                    Err(ConfigError::OutOfRange { .. })
                ),
                "{property}={value} must be refused"
            );
        }
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

    /// The four settings a group strategy reads, with the reference's own
    /// defaults: no tag and no minimum group, a five-second receiver timeout,
    /// and — a different setting — no endpoint tag at all.
    #[test]
    fn the_flow_control_settings_have_the_references_defaults() {
        let config = resolve(&[("aeron.dir", "/tmp/aeron-test")]).expect("a config");

        assert_eq!(-1, config.flow_control_group_tag);
        assert_eq!(0, config.flow_control_group_min_size);
        assert_eq!(5_000_000_000, config.flow_control_receiver_timeout_ns);
        assert_eq!(None, config.receiver_group_tag);
    }

    #[test]
    fn the_flow_control_settings_are_read_under_both_names() {
        let config = resolve(&[
            ("aeron.dir", "/tmp/aeron-test"),
            ("aeron.flow.control.gtag", "123"),
            ("aeron.flow.control.group.min.size", "3"),
            ("aeron.flow.control.receiver.timeout", "1s"),
            ("aeron.receiver.group.tag", "-1"),
        ])
        .expect("a config");

        assert_eq!(123, config.flow_control_group_tag);
        assert_eq!(3, config.flow_control_group_min_size);
        assert_eq!(1_000_000_000, config.flow_control_receiver_timeout_ns);
        assert_eq!(
            Some(-1),
            config.receiver_group_tag,
            "and `-1` is a tag, unlike naming nothing"
        );
    }

    /// The two supplier settings name the reference's **symbols**, and a name
    /// its table does not hold is a driver that does not start
    /// (`aeron_driver_context.c:563-580`).
    #[test]
    fn a_flow_control_supplier_is_named_by_its_the_references_symbol() {
        let config = resolve(&[
            ("aeron.dir", "/tmp/aeron-test"),
            (
                "aeron.multicast.flowcontrol.supplier",
                "aeron_min_flow_control_strategy_supplier",
            ),
            (
                "aeron.unicast.flowcontrol.supplier",
                "aeron_tagged_flow_control_strategy_supplier",
            ),
        ])
        .expect("a config");

        assert_eq!(Supplier::Min, config.multicast_flow_control_supplier);
        assert_eq!(Supplier::Tagged, config.unicast_flow_control_supplier);

        assert_eq!(
            Supplier::Max,
            resolve(&[("aeron.dir", "/tmp/aeron-test")])
                .expect("a config")
                .multicast_flow_control_supplier,
            "and `max` is what a driver that names none gets"
        );

        // The short name beside the symbol in the same table is the same
        // supplier, and the name `fc=` uses is not on that table at all
        // (`aeron_flow_control.c:43-64` against `aeronmd.h:286-289`).
        assert_eq!(
            Supplier::Tagged,
            resolve(&[
                ("aeron.dir", "/tmp/aeron-test"),
                ("aeron.multicast.flowcontrol.supplier", "multicast_tagged"),
            ])
            .expect("a config")
            .multicast_flow_control_supplier
        );

        for unknown in ["min", "aeron_cubic_supplier"] {
            assert!(
                matches!(
                    resolve(&[
                        ("aeron.dir", "/tmp/aeron-test"),
                        ("aeron.multicast.flowcontrol.supplier", unknown),
                    ]),
                    Err(ConfigError::UnknownSupplier { .. })
                ),
                "{unknown} names no supplier"
            );
        }
    }

    #[test]
    fn a_group_min_size_the_type_cannot_hold_is_refused() {
        assert!(
            resolve(&[
                ("aeron.dir", "/tmp/aeron-test"),
                ("aeron.flow.control.group.min.size", "2147483648"),
            ])
            .is_err()
        );
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
            ("aeron.threading.mode.typo", "SHARED"),
            ("aeron.term.buffer.length", "64m"),
        ])
        .expect("resolve");

        assert_eq!(PathBuf::from("/tmp/x"), config.aeron_dir);
    }

    /// The four modes, by the names the reference's parser compares
    /// case-sensitively and exactly (`aeron_driver_context.c:45-71`).
    #[test]
    fn the_threading_modes_are_the_references_four() {
        assert_eq!(
            ThreadingMode::Dedicated,
            DriverConfig::default().threading_mode
        );

        for (value, expected) in [
            ("DEDICATED", ThreadingMode::Dedicated),
            ("SHARED_NETWORK", ThreadingMode::SharedNetwork),
            ("SHARED", ThreadingMode::Shared),
            ("INVOKER", ThreadingMode::Invoker),
        ] {
            let config = resolve(&[("deepmsg.dir", "/tmp/x"), ("aeron.threading.mode", value)])
                .expect("resolve");

            assert_eq!(expected, config.threading_mode, "{value}");
            assert_eq!(value, expected.as_str(), "and it prints back as it came");
        }

        // `SHARED` is a prefix of `SHARED_NETWORK`, and the reference's
        // `strncmp` compares the terminating byte — so the two do not collide.
        // Anything else is a typo the reference warns about and this refuses.
        for value in ["shared", "SHARED_NETWORK_", "SHARE", "DEDICATED "] {
            let error = resolve(&[("deepmsg.dir", "/tmp/x"), ("aeron.threading.mode", value)])
                .expect_err("not a mode");

            assert!(
                matches!(error, ConfigError::UnknownThreadingMode { .. }),
                "{value}"
            );
        }
    }

    #[test]
    fn the_thread_naming_is_classic_or_new() {
        assert_eq!(ThreadNaming::Classic, DriverConfig::default().thread_naming);

        let new =
            resolve(&[("deepmsg.dir", "/tmp/x"), ("aeron.thread.naming", "new")]).expect("resolve");
        assert_eq!(ThreadNaming::New, new.thread_naming);

        let error = resolve(&[("deepmsg.dir", "/tmp/x"), ("aeron.thread.naming", "New")])
            .expect_err("case matters");
        assert!(matches!(error, ConfigError::UnknownThreadNaming { .. }));
    }

    /// Five slots default to `backoff` and the native resource agent to
    /// `sleep-ns` (`aeron_driver_context.c:1143-1148`), which is the one place
    /// the reference's defaults differ between slots.
    #[test]
    fn every_slot_has_its_own_idle_strategy_and_its_own_default() {
        let config = DriverConfig::default();

        for setting in [
            &config.conductor_idle,
            &config.sender_idle,
            &config.receiver_idle,
            &config.shared_idle,
            &config.shared_network_idle,
        ] {
            assert_eq!("backoff", setting.name);
            assert_eq!(None, setting.init_args);
        }

        assert_eq!("sleep-ns", config.native_resource_agent_idle.name);
        assert!(matches!(
            config.native_resource_agent_idle.strategy(),
            Some(crate::idle::Strategy::Sleeping(_))
        ));
    }

    /// The names are the reference's environment variables, two of which are
    /// not what the slot is called: the shared-network slot has no underscore
    /// between the words, and the native resource agent's carries a `DRIVER_`
    /// infix (`aeronmd.h:441`, `:497`).
    #[test]
    fn the_idle_strategy_names_are_the_references_even_where_they_are_odd() {
        let config = resolve_with_env(
            &[("deepmsg.dir", "/tmp/x")],
            &[
                ("AERON_SHAREDNETWORK_IDLE_STRATEGY", "spin"),
                ("AERON_DRIVER_NATIVE_RESOURCE_AGENT_IDLE_STRATEGY", "yield"),
                (
                    "AERON_DRIVER_NATIVE_RESOURCE_AGENT_IDLE_STRATEGY_INIT_ARGS",
                    "ignored",
                ),
                ("AERON_CONDUCTOR_IDLE_STRATEGY_INIT_ARGS", "2-3-10us-1ms"),
            ],
        )
        .expect("resolve");

        assert_eq!("spin", config.shared_network_idle.name);
        assert_eq!("yield", config.native_resource_agent_idle.name);
        assert_eq!(
            Some("ignored".to_owned()),
            config.native_resource_agent_idle.init_args,
            "the args are read even where the strategy ignores them, as the \
             reference reads both"
        );
        assert_eq!(
            Some("2-3-10us-1ms".to_owned()),
            config.conductor_idle.init_args
        );

        // And the property spelling, which is the environment variable's name
        // in lower case with its dots back.
        let by_property = resolve(&[
            ("deepmsg.dir", "/tmp/x"),
            ("aeron.sharednetwork.idle.strategy", "noop"),
            ("aeron.sender.idle.strategy", "sleep-ns"),
        ])
        .expect("resolve");

        assert_eq!("noop", by_property.shared_network_idle.name);
        assert_eq!("sleep-ns", by_property.sender_idle.name);
    }

    /// An idle strategy name the reference's table does not have is a driver
    /// that does not start, on both sides (`aeron_driver_context.c:1152-1158`),
    /// and malformed init args are the same kind of refusal
    /// (`aeron_agent.c:186-248`).
    #[test]
    fn an_idle_strategy_that_does_not_exist_or_will_not_parse_is_refused() {
        // An empty value is not one of these: this build reads it as unset, as
        // it does everywhere (`an_empty_value_is_an_unset_value`), so the slot
        // keeps its default rather than becoming a nameless strategy.
        for (property, value) in [
            ("aeron.conductor.idle.strategy", "busy-wait"),
            ("aeron.sender.idle.strategy", "Sleeping"),
            ("aeron.receiver.idle.strategy", "backoff "),
        ] {
            let error = resolve(&[("deepmsg.dir", "/tmp/x"), (property, value)])
                .expect_err("not a strategy");

            assert!(
                matches!(error, ConfigError::UnknownIdleStrategy { .. }),
                "{value}"
            );
        }

        let error = resolve(&[
            ("deepmsg.dir", "/tmp/x"),
            ("aeron.conductor.idle.strategy", "backoff"),
            ("aeron.conductor.idle.strategy.init.args", "3-4"),
        ])
        .expect_err("four values are required");

        assert!(matches!(error, ConfigError::UnknownIdleStrategy { .. }));
    }
}
