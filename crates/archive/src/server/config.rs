//! What an archive is told about itself.
//!
//! The reference's archive is configured the way every Java Aeron component is:
//! a pile of `System.getProperty` reads, one per name, with the default written
//! into the constant beside it (`Archive.java:295-663` for the server's own,
//! `client/AeronArchive.java:2674-2848` for the ones the archive shares with the
//! client it embeds). There is no environment layer to mirror — the C driver
//! reads `AERON_*` because `aeronmd` turns `-D` into the environment for it
//! (`aeron-driver/src/main/c/aeronmd.c:72-81`), while the Java archive reads
//! **system properties and nothing else**.
//!
//! How those properties reach us is the shim's doing and worth stating: it
//! hands a replacement binary the invocation's `-D` arguments as **argv**
//! (`crates/tools/src/bin/archive-shim.rs:443-448`), so what a Java archive
//! would have found in `System.getProperty` is here an argument. A *properties
//! file* — the other thing `loadPropertiesFiles(args)` accepts
//! (`ArchivingMediaDriver.java:49`) — is an argument that is not a `-D`, and is
//! read as one.
//!
//! # What this module does not do
//!
//! Only the names this slice uses are resolved. The reference has 45 of them;
//! the rest arrive with the sessions that read them, and the whole set is
//! P2-9c's, whose own acceptance is that an unknown name is accepted without
//! being silently *used* as something else.
//!
//! # Errors
//!
//! Three settings have no default and are refused when missing or wrong, with
//! the reference's own words: `aeron.dir` (the directory the archive is a
//! client of), and the two control channels' media types
//! (`Archive.java:1216-1233`).

use std::fmt;
use std::path::{Path, PathBuf};

use crate::server::idle::IdleStrategy;

/// How the archive's agents are run (`ArchiveThreadingMode.java:28-38`).
///
/// The default is `Dedicated` (`Archive.java:394-397`), and this slice
/// implements `Shared` only — `Archive` picks its conductor with
/// `DEDICATED == ctx.threadingMode() ? DedicatedModeArchiveConductor :
/// SharedModeArchiveConductor` (`Archive.java:146-147`), so `Invoker` is
/// `Shared` seen from the caller's side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadingMode {
    /// One thread per agent.
    Dedicated,
    /// The agents are driven in the caller's turn.
    Shared,
    /// `Shared`, with the caller owning the turn.
    Invoker,
}

impl ThreadingMode {
    /// The reference's spelling, which is also the value a property carries.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "DEDICATED" => Some(Self::Dedicated),
            "SHARED" => Some(Self::Shared),
            "INVOKER" => Some(Self::Invoker),
            _ => None,
        }
    }
}

impl fmt::Display for ThreadingMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Dedicated => "DEDICATED",
            Self::Shared => "SHARED",
            Self::Invoker => "INVOKER",
        })
    }
}

/// Which spelling of its name a thread the archive starts is given
/// (`CommonContext.java:534-566`, read through `CommonContext.threadName`).
///
/// **Not one of the archive's own 45 names.** `aeron.thread.naming` is a
/// `CommonContext` name — the client, the driver and the archive all resolve it
/// through the same static — which is why it is not in the ledger
/// `every_name_the_reference_reads_is_accounted_for` keeps.
///
/// The reference **throws** on a value that is neither spelling
/// (`:563-565`), so a typo is a refusal here rather than a silent fall back to
/// `classic`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadNaming {
    /// `classic`, the default (`:547`): `archive-conductor`,
    /// `archive-recorder`, `archive-replayer`.
    Classic,
    /// `new` (`:543`): the same three as `aeron-ar-cnd` / `aeron-ar-rec` /
    /// `aeron-ar-rep`.
    New,
}

impl ThreadNaming {
    /// `THREAD_NAMING_DEFAULT` (`CommonContext.java:547`), which is `classic`.
    pub const DEFAULT: Self = Self::Classic;

    /// The naming `value` names, or `None` for one the reference does not know
    /// (its `default ->` arm, `:564-565`).
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "classic" => Some(Self::Classic),
            "new" => Some(Self::New),
            _ => None,
        }
    }
}

/// A setting's value was there and could not be used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// The invocation carried something that is not a `-D` property and is not
    /// a properties file that can be read.
    UnreadablePropertiesFile { argument: String },
    /// A `-D` argument with no `=`.
    MalformedArgument { argument: String },
    /// `aeron.dir` was not given.
    ///
    /// The driver's own error, in its words: an archive is a *client* of a
    /// media driver, and the directory is how it finds one
    /// (`crates/driver/src/config.rs:1187-1191`).
    MissingAeronDir,
    /// `aeron.archive.control.channel` is required when the control channel is
    /// enabled (`Archive.java:1218-1221`).
    MissingControlChannel,
    /// ...and must be UDP (`Archive.java:1223-1227`).
    ControlChannelNotUdp { channel: String },
    /// ...while the local one must be IPC (`Archive.java:1230-1233`).
    LocalControlChannelNotIpc { channel: String },
    /// A value that was present and unparseable.
    InvalidValue { name: String, value: String },
    /// A threading mode that is not one of the three.
    UnknownThreadingMode { value: String },
    /// A thread naming mode that is neither `classic` nor `new`
    /// (`CommonContext.java:563-565`).
    UnknownThreadNaming { value: String },
    /// An idle strategy name this build has no strategy for
    /// (`io.aeron.driver.Configuration.agentIdleStrategy:2098-2143`). The
    /// reference refuses the same way for a name nothing answers to; see
    /// [`crate::server::idle`] for the one class of name it accepts and this
    /// build does not. Carries the property it came from: three names resolve
    /// through this table — the archive's own and each agent's.
    UnknownIdleStrategy { name: String, value: String },
    /// `aeron.archive.segment.file.length` is not a power of two
    /// (`Archive.java:1482-1485`).
    SegmentFileLengthNotPowerOfTwo { length: usize },
    /// ...or is outside the term buffer's own range (`:1486-1489`).
    SegmentFileLengthOutOfRange { length: usize },
    /// `aeron.archive.file.io.max.length` is below a term or not a power of
    /// two (`:1208-1211`). It is also a recording session's
    /// `blockLengthLimit`'s upper bound (`RecordingSession.java:81`).
    InvalidFileIoMaxLength { length: usize },
    /// The archive was told to publish recording events
    /// (`aeron.archive.recording.events.enabled`), which this build does not
    /// do yet — the P2-S2 plan's §4. Refused rather than accepted-and-ignored:
    /// a client waiting for events that will never come is the silent kind of
    /// wrong.
    RecordingEventsNotImplemented,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnreadablePropertiesFile { argument } => {
                write!(f, "properties file could not be read: {argument}")
            }
            Self::MalformedArgument { argument } => {
                write!(
                    f,
                    "malformed property argument (expected -Dname=value): {argument}"
                )
            }
            Self::MissingAeronDir => write!(f, "aeron.dir must be set"),
            Self::MissingControlChannel => {
                write!(f, "Archive.Context.controlChannel must be set")
            }
            Self::ControlChannelNotUdp { channel } => {
                write!(
                    f,
                    "Archive.Context.controlChannel must be UDP media: uri={channel}"
                )
            }
            Self::LocalControlChannelNotIpc { channel } => {
                write!(f, "local control channel must be IPC media: uri={channel}")
            }
            Self::InvalidValue { name, value } => write!(f, "{name}={value} is not a valid value"),
            Self::SegmentFileLengthNotPowerOfTwo { length } => {
                write!(f, "segment file length not a power of 2: {length}")
            }
            Self::SegmentFileLengthOutOfRange { length } => {
                write!(f, "segment file length not in valid range: {length}")
            }
            Self::InvalidFileIoMaxLength { length } => {
                write!(f, "invalid fileIoMaxLength={length}")
            }
            Self::RecordingEventsNotImplemented => write!(
                f,
                "aeron.archive.recording.events.enabled is not supported by this build \
                 (see the P2-S2 plan, which defers the events publication)"
            ),
            Self::UnknownThreadingMode { value } => {
                write!(
                    f,
                    "aeron.archive.threading.mode={value} is not one of DEDICATED, SHARED, INVOKER"
                )
            }
            Self::UnknownIdleStrategy { name, value } => write!(
                f,
                "{name}={value} is not an idle strategy this build can give an archive agent"
            ),
            Self::UnknownThreadNaming { value } => {
                write!(f, "aeron.thread.naming={value} is not one of classic, new")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// The properties this slice reads, each with the reference's name and default.
///
/// A table rather than scattered lookups, because the names are the contract:
/// the reference's deployment files spell these, and every one of them is
/// checked by a test below.
mod settings {
    /// The directory the archive finds its media driver in
    /// (`AeronArchive`... no: the driver's own name, read by the shim's
    /// replacement binary out of the same argument list — `TestArchive.h:104`).
    pub const AERON_DIR: &str = "aeron.dir";

    /// `Archive.java:307`, default `aeron-archive` (`:315`).
    pub const ARCHIVE_DIR: &str = "aeron.archive.dir";
    /// `Archive.java:321`, default `""` — empty means "beside the archive"
    /// (`:320`).
    pub const MARK_FILE_DIR: &str = "aeron.archive.mark.file.dir";
    /// `Archive.java:578`, default false (`:577`).
    pub const DIR_DELETE_ON_START: &str = "aeron.archive.dir.delete.on.start";
    /// `Archive.java:394`, default `DEDICATED` (`:393`).
    pub const THREADING_MODE: &str = "aeron.archive.threading.mode";
    /// `CommonContext.java:534`, default `classic` (`:547`) — **not one of the
    /// archive's 45**: it is a `CommonContext` name, resolved by the same static
    /// the client and the driver use, so it is deliberately outside the three
    /// lists `every_name_the_reference_reads_is_accounted_for` keeps.
    pub const THREAD_NAMING: &str = "aeron.thread.naming";
    /// `Archive.java:590`, default `DefaultAuthenticatorSupplier` (`:597`).
    pub const AUTHENTICATOR_SUPPLIER: &str = "aeron.archive.authenticator.supplier";
    /// `Archive.java:603`.
    pub const AUTHORISATION_SERVICE_SUPPLIER: &str = "aeron.archive.authorisation.service.supplier";
    /// `Archive.java:493`, default 5 s (`:502`).
    pub const CONNECT_TIMEOUT: &str = "aeron.archive.connect.timeout";
    /// `Archive.java:510-511`, default 1 s (`:520`).
    pub const SESSION_LIVENESS_CHECK_INTERVAL: &str =
        "aeron.archive.session.liveness.check.interval";
    /// `Archive.java:584`, default `""` (`:583`).
    pub const REPLICATION_CHANNEL: &str = "aeron.archive.replication.channel";
    /// `Archive.java:654`, default `-1` — "the Aeron client's own id"
    /// (`:650-655`).
    pub const ARCHIVE_ID: &str = "aeron.archive.id";
    /// `Archive.java:663`, default true (`:662`).
    pub const CONTROL_CHANNEL_ENABLED: &str = "aeron.archive.control.channel.enabled";

    /// `AeronArchive.java:2698`. No default: required when the control channel
    /// is enabled.
    pub const CONTROL_CHANNEL: &str = "aeron.archive.control.channel";
    /// `AeronArchive.java:2704`, default 10 (`:2710`).
    pub const CONTROL_STREAM_ID: &str = "aeron.archive.control.stream.id";
    /// `AeronArchive.java:2716`, default `aeron:ipc?term-length=64k` (`:2722`).
    pub const LOCAL_CONTROL_CHANNEL: &str = "aeron.archive.local.control.channel";
    /// `AeronArchive.java:2728`, default 10 (`:2734`).
    pub const LOCAL_CONTROL_STREAM_ID: &str = "aeron.archive.local.control.stream.id";
    /// `AeronArchive.java:2752`. No default.
    pub const CONTROL_RESPONSE_CHANNEL: &str = "aeron.archive.control.response.channel";
    /// `AeronArchive.java:2758`, default 20 (`:2764`).
    pub const CONTROL_RESPONSE_STREAM_ID: &str = "aeron.archive.control.response.stream.id";
    /// `AeronArchive.java:2809`, default true (`:2816`).
    pub const CONTROL_TERM_BUFFER_SPARSE: &str = "aeron.archive.control.term.buffer.sparse";
    /// `AeronArchive.java:2822`, default 64 K (`:2828`).
    pub const CONTROL_TERM_BUFFER_LENGTH: &str = "aeron.archive.control.term.buffer.length";
    /// `AeronArchive.java:2834`, default: the media driver's own MTU (`:2840`).
    pub const CONTROL_MTU_LENGTH: &str = "aeron.archive.control.mtu.length";

    /// `Archive.java:429`, default 20 (`:438`).
    pub const MAX_CONCURRENT_RECORDINGS: &str = "aeron.archive.max.concurrent.recordings";
    /// `Archive.java:445`, default 20 (`:452`). What bounds how many replays may
    /// be in flight at once (`ArchiveConductor.java:775-780`).
    pub const MAX_CONCURRENT_REPLAYS: &str = "aeron.archive.max.concurrent.replays";
    /// `Archive.java:328`, default 128 MiB (`:336`).
    pub const SEGMENT_FILE_LENGTH: &str = "aeron.archive.segment.file.length";
    /// `Archive.java:301`, default 1 MiB (`:295`).
    pub const FILE_IO_MAX_LENGTH: &str = "aeron.archive.file.io.max.length";
    /// `Archive.java:342`, default: the segment file length (`:350`).
    pub const LOW_STORAGE_SPACE_THRESHOLD: &str = "aeron.archive.low.storage.space.threshold";
    /// `Archive.java:361`, default 0 (`:369`).
    pub const FILE_SYNC_LEVEL: &str = "aeron.archive.file.sync.level";
    /// `AeronArchive.java:2797`, default false (`:2803`).
    pub const RECORDING_EVENTS_ENABLED: &str = "aeron.archive.recording.events.enabled";
    /// `AeronArchive.java:2770`, default
    /// `aeron:udp?control-mode=dynamic|control=localhost:8030` (`:2778-2779`).
    pub const RECORDING_EVENTS_CHANNEL: &str = "aeron.archive.recording.events.channel";
    /// `AeronArchive.java:2785`, default 30 (`:2791`).
    pub const RECORDING_EVENTS_STREAM_ID: &str = "aeron.archive.recording.events.stream.id";

    /// `Archive.java:479`, default `Catalog.DEFAULT_CAPACITY` (`:487`,
    /// `Catalog.java:105`), read as a size at `:861`.
    pub const CATALOG_CAPACITY: &str = "aeron.archive.catalog.capacity";
    /// `Archive.java:380`, default `FILE_SYNC_LEVEL_DEFAULT` (`:388`), read at
    /// `:756`.
    pub const CATALOG_FILE_SYNC_LEVEL: &str = "aeron.archive.catalog.file.sync.level";
    /// `Archive.java:627`, default 1 MiB (`:633`), read at `:946`.
    pub const ERROR_BUFFER_LENGTH: &str = "aeron.archive.error.buffer.length";

    /// `AeronArchive.java:2674`, default 10 s (`:2680`), read at `:2864`.
    ///
    /// What this build's control sessions already wait — as
    /// `deepmsg_client::client::DEFAULT_TIMEOUT` — and what the name makes
    /// configurable.
    pub const MESSAGE_TIMEOUT: &str = "aeron.archive.message.timeout";
    /// `AeronArchive.java:2686`, default 3 (`:2692`), read at `:2875`.
    ///
    /// What [`crate::server::response_proxy::SEND_ATTEMPTS`] already is.
    pub const MESSAGE_RETRY_ATTEMPTS: &str = "aeron.archive.message.retry.attempts";

    /// `Archive.java:526`, default `Context.replayLingerTimeoutNs()` (`:535`),
    /// read at `:884`.
    pub const REPLAY_LINGER_TIMEOUT: &str = "aeron.archive.replay.linger.timeout";
    /// `Archive.java:647`, default off, read at `:1027`.
    pub const REPLAY_CHECKSUM: &str = "aeron.archive.replay.checksum";
    /// `Archive.java:640`, default off, read at `:1017`. The recording side of
    /// the same choice.
    pub const RECORD_CHECKSUM: &str = "aeron.archive.record.checksum";
    /// `Archive.java:400`; the default is `DEFAULT_IDLE_STRATEGY` at `:420`,
    /// read by the conductor's supplier at `:776-784`.
    ///
    /// ★ **Honoured since P2-9a**, after being a name this build accepted and
    /// ignored — see [`crate::server::idle`].
    pub const IDLE_STRATEGY: &str = "aeron.archive.idle.strategy";

    /// ★ The five below belong to other slices, and this build **refuses them by
    /// name** rather than accepting them silently: the cycle thresholds are
    /// P2-9b's counters, and the two idle strategies are P2-9a's threading work.
    /// A name that is accepted and does nothing would make those slices look
    /// wired when they are not.
    ///
    /// `Archive.java:542`, default 100 ms (`:548`), read at `:894`.
    pub const CONDUCTOR_CYCLE_THRESHOLD: &str = "aeron.archive.conductor.cycle.threshold";
    /// `Archive.java:554`, default 100 ms (`:560`), read at `:904`.
    pub const RECORDER_CYCLE_THRESHOLD: &str = "aeron.archive.recorder.cycle.threshold";
    /// `Archive.java:566`, default 100 ms (`:572`), read at `:914`.
    pub const REPLAYER_CYCLE_THRESHOLD: &str = "aeron.archive.replayer.cycle.threshold";
    /// `Archive.java:406`, read by the recorder's idle strategy supplier
    /// (`:786-802`).
    pub const RECORDER_IDLE_STRATEGY: &str = "aeron.archive.recorder.idle.strategy";
    /// `Archive.java:412`, similarly (`:804-820`).
    pub const REPLAYER_IDLE_STRATEGY: &str = "aeron.archive.replayer.idle.strategy";

    /// `Archive.java:463`, default 8192 (`:472`), read at `:851` — and **read by
    /// nobody else**: the reference exposes it and constructs its `Catalog`
    /// without it (`Archive.java:1504-1511`, where the arguments are the sync
    /// level, the capacity, the clock and the checksum). Accepted here for the
    /// same reason it is inert there.
    pub const MAX_CATALOG_ENTRIES: &str = "aeron.archive.max.catalog.entries";
    /// `AeronArchive.java:2848`, default `""` (`:3025`) — a name the **client**
    /// reads (`ArchiveContext::client_name`), and one this build already carries
    /// as a recorded deviation on that side (`crate::client::context`). The
    /// archive server has no reader for it in the reference either.
    pub const CLIENT_NAME: &str = "aeron.archive.client.name";
}

/// ★ **The names the reference reads that this build accepts and does not act
/// on**, with the reason each one is on the list.
///
/// It is here, and not only in the test below, because it is the thing a reader
/// of this module needs: the reference's 45 names all change its behaviour, and
/// nine of them do not change this build's — these eight, which are accepted and
/// ignored, and `recording.events.enabled`, which is refused. Two of the eight
/// are the reference's own doing — `max.catalog.entries` has no reader there
/// either, and `client.name` is read by the *client* — two are read there only
/// where the events publication is enabled, and the rest are work this build has
/// not done.
///
/// Every name here is **accepted**, which is the reference's own behaviour for a
/// name it does not know (`System.getProperty` per name, no validation): a
/// deployment's property file starts this archive whether or not this build can
/// honour all of it. What the list buys is that the gap is written down and
/// pinned by `every_name_the_reference_reads_is_accounted_for` below, instead of
/// being discovered by the deployment.
pub const NOT_HONOURED: &[(&str, &str)] = &[
    (
        settings::RECORD_CHECKSUM,
        "recordings are written without a checksum; the reference's default is off too, \
         but a deployment that asks for `crc32` is not told it will not get it",
    ),
    (settings::REPLAY_CHECKSUM, "a replay is not checksummed"),
    (
        settings::REPLAY_LINGER_TIMEOUT,
        "the replay publication is not given a linger",
    ),
    (
        settings::CATALOG_FILE_SYNC_LEVEL,
        "the catalog is not forced to disk on a level",
    ),
    (
        settings::MAX_CATALOG_ENTRIES,
        "★ the reference has no reader for it either: `Archive.maxCatalogEntries()` is \
         defined and never called, and its `Catalog` is constructed without it \
         (`Archive.java:1504-1511`)",
    ),
    (
        settings::CLIENT_NAME,
        "★ it is read by the *client* (`AeronArchive.java:3025`); this build's client takes \
         it from `AERON_ARCHIVE_CLIENT_NAME` instead, which is a recorded deviation on \
         that side",
    ),
    (
        settings::RECORDING_EVENTS_CHANNEL,
        "only read when the events publication is enabled, and this build refuses that",
    ),
    (
        settings::RECORDING_EVENTS_STREAM_ID,
        "the same: it is the events publication's stream",
    ),
];

/// The reference's default archive directory (`Archive.java:315`).
const ARCHIVE_DIR_DEFAULT: &str = "aeron-archive";

/// `AeronArchive.Configuration.MESSAGE_TIMEOUT_DEFAULT_NS` (`:2680`).
const MESSAGE_TIMEOUT_DEFAULT_NS: i64 = 10_000_000_000;

/// `AeronArchive.Configuration.MESSAGE_RETRY_ATTEMPTS_DEFAULT` (`:2692`), which
/// is what [`crate::server::response_proxy::SEND_ATTEMPTS`] already is.
const MESSAGE_RETRY_ATTEMPTS_DEFAULT: usize = 3;

/// The reference's default for all three cycle thresholds
/// (`Archive.java:548`, `:560`, `:572`).
const CYCLE_THRESHOLD_DEFAULT_NS: i64 = 100_000_000;

/// `Archive.Configuration.ERROR_BUFFER_LENGTH_DEFAULT` (`:633`).
const ERROR_BUFFER_LENGTH_DEFAULT: usize = 1024 * 1024;

/// `Catalog.DEFAULT_CAPACITY` (`Catalog.java:105`), which is what
/// `Archive.Configuration.CATALOG_CAPACITY_DEFAULT` is (`:487`).
const CATALOG_CAPACITY_DEFAULT: usize = 1024 * 1024;
/// `AeronArchive.java:2722`.
const LOCAL_CONTROL_CHANNEL_DEFAULT: &str = "aeron:ipc?term-length=64k";
/// `AeronArchive.java:2710`.
const CONTROL_STREAM_ID_DEFAULT: i32 = 10;
/// `AeronArchive.java:2764`.
const CONTROL_RESPONSE_STREAM_ID_DEFAULT: i32 = 20;
/// `AeronArchive.java:2828`.
const CONTROL_TERM_BUFFER_LENGTH_DEFAULT: usize = 64 * 1024;
/// `Archive.java:502`.
const CONNECT_TIMEOUT_DEFAULT_NS: i64 = 5 * 1_000_000_000;
/// `Archive.java:520`.
const SESSION_LIVENESS_CHECK_INTERVAL_DEFAULT_NS: i64 = 1_000_000_000;
/// `Archive.java:438`.
const MAX_CONCURRENT_RECORDINGS_DEFAULT: i32 = 20;
/// `Archive.java:452`.
const MAX_CONCURRENT_REPLAYS_DEFAULT: i32 = 20;
/// `Archive.java:336`.
const SEGMENT_FILE_LENGTH_DEFAULT: usize = 128 * 1024 * 1024;
/// `Archive.java:295`.
const FILE_IO_MAX_LENGTH_DEFAULT: usize = 1024 * 1024;
/// `LogBufferDescriptor.java:58` and `:63`, which the segment file length is
/// bounded by (`Archive.java:1486-1489`).
const TERM_MIN_LENGTH: usize = 64 * 1024;
/// See [`TERM_MIN_LENGTH`].
const TERM_MAX_LENGTH: usize = 1024 * 1024 * 1024;
/// `Archive.java:597`.
const AUTHENTICATOR_SUPPLIER_DEFAULT: &str = "io.aeron.security.DefaultAuthenticatorSupplier";
/// `Archive.java:610-611` — and this one is **not a class name**.
///
/// The two suppliers default differently, and the difference is in the
/// reference rather than in this build. The authenticator's default is a class
/// name, resolved through `Class.forName` (`:956-964`). The authorisation
/// service's is a *supplier* — `DEFAULT_AUTHORISATION_SERVICE_SUPPLIER =
/// () -> AuthorisationService.ALLOW_ALL` — and the property is read with no
/// default at all, so an archive that does not set it gets the empty name and
/// the built-in allow-all service (`:982-1002`):
///
/// ```text
/// final String supplierClassName = System.getProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
/// if (Strings.isEmpty(supplierClassName)) { return DEFAULT_AUTHORISATION_SERVICE_SUPPLIER; }
/// ```
///
/// Writing the lambda's *behaviour* here as though it were a class name is the
/// mistake this constant used to make, and it is not a harmless one: nothing
/// can build that supplier, so an archive with a default configuration refused
/// to start.
const AUTHORISATION_SERVICE_SUPPLIER_DEFAULT: &str = "";

/// Everything an archive is told, resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveConfig {
    /// The media driver's directory. The archive is a client of it.
    pub aeron_dir: PathBuf,
    /// Where the catalog and segments live (`Archive.java:307`).
    pub archive_dir: PathBuf,
    /// Where `archive-mark.dat` lives; [`None`] means the archive directory
    /// (`Archive.java:321-325`).
    pub mark_file_dir: Option<PathBuf>,
    /// Whether the archive directory is deleted on start.
    pub delete_dir_on_start: bool,
    /// The archive's identity, or [`None`] for "the Aeron client's own id",
    /// which is the reference's `-1` (`Archive.java:650-655`).
    pub archive_id: Option<i64>,
    /// Whether the UDP control channel is served (`Archive.java:663`).
    pub control_channel_enabled: bool,
    /// The UDP control channel, required when that channel is enabled.
    pub control_channel: Option<String>,
    /// The stream the control channel carries (`AeronArchive.java:2710`).
    pub control_stream_id: i32,
    /// The local (IPC) control channel, which is served whether or not the UDP
    /// one is (`ArchiveConductor.java:239-240`).
    pub local_control_channel: String,
    /// ...and its stream, which is the same 10 by default
    /// (`AeronArchive.java:2734`).
    pub local_control_stream_id: i32,
    /// The channel a client is answered on, when it asked to be
    /// (`AeronArchive.java:2752`).
    pub control_response_channel: Option<String>,
    /// Its stream, 20 by default (`AeronArchive.java:2764`).
    pub control_response_stream_id: i32,
    /// Whether the control channel's term buffers are sparse
    /// (`AeronArchive.java:2816`), which this build forces onto the URI
    /// (`ArchiveConductor.java:230-234`).
    pub control_term_buffer_sparse: bool,
    /// The control channel's term length (`AeronArchive.java:2828`).
    pub control_term_buffer_length: usize,
    /// The control channel's MTU, or [`None`] for the driver's own
    /// (`AeronArchive.java:2840`).
    pub control_mtu_length: Option<usize>,
    /// How long a control session may take to get its first response out
    /// (`Archive.java:502`).
    pub connect_timeout_ns: i64,
    /// How often an active session is pinged (`Archive.java:520`).
    pub session_liveness_check_interval_ns: i64,
    /// `DEDICATED`, `SHARED` or `INVOKER` (`Archive.java:394-397`).
    pub threading_mode: ThreadingMode,
    /// Which spelling the threads the archive starts are named with
    /// (`CommonContext.java:557-566`). Read only when there are threads to name
    /// — see [`crate::server::threading`].
    pub thread_naming: ThreadNaming,
    /// How the conductor waits when it has nothing to do
    /// (`Archive.java:400`, default `BackoffIdleStrategy` at `:420`).
    ///
    /// ★ **This is the conductor's, and it used to be hardcoded.** The loop
    /// yielded (`launch::run`'s own `yield_now`), which is what the reference's
    /// own C suite asks for by name (`TestArchive.h:47`,
    /// `TestStandaloneArchive.h:122` both pass `aeron.archive.idle.strategy=yield`)
    /// — so the suite could not tell the difference and a deployment that asked
    /// for anything else was told nothing.
    ///
    /// The **recorder's and replayer's** names sit beside this one, and under
    /// `SHARED` none of the three is used: neither agent has a runner of its own
    /// — `SharedModeArchiveConductor.onStart` drives each through an
    /// `AgentInvoker` (`:40-44`), which has no idle strategy at all, and the
    /// conductor's `doWork` invokes both inside its own turn (`:57-64`).
    pub idle_strategy: IdleStrategy,
    /// The recorder's own (`Archive.java:406`), which is what its thread idles
    /// with under `DEDICATED`; the archive's when this names nothing
    /// (`:1423-1431`). ★ Honoured since P2-9a — the thread that idles arrived
    /// with the recorder's own thread.
    pub recorder_idle_strategy: IdleStrategy,
    /// The replayer's (`Archive.java:412`), the same rule (`:1433-1440`).
    pub replayer_idle_strategy: IdleStrategy,
    /// The authenticator to build (`Archive.java:590-597`).
    pub authenticator_supplier: String,
    /// The authorisation service to build (`Archive.java:603-610`).
    pub authorisation_service_supplier: String,
    /// The channel replication streams arrive on (`Archive.java:584`).
    pub replication_channel: Option<String>,

    /// How many recordings may be active at once (`Archive.java:429`, `:438`).
    pub max_concurrent_recordings: i32,
    /// How many replays may be in flight at once (`Archive.java:445`, `:452`).
    pub max_concurrent_replays: i32,
    /// How long one segment file of a recording is (`Archive.java:328`,
    /// `:336`).
    pub segment_file_length: usize,
    /// The largest block a recording session hands to its writer
    /// (`Archive.java:301`, `:295`). A session's limit is
    /// `min(image.termBufferLength(), this)` (`RecordingSession.java:81`).
    pub file_io_max_length: usize,
    /// Below this much usable space a recording is refused
    /// (`ArchiveConductor.java:2597-2617`), and it defaults to the segment
    /// file length (`Archive.java:350`).
    pub low_storage_space_threshold: u64,
    /// Above zero every recorded block is forced to disk, above one the
    /// metadata as well (`RecordingWriter.java:88-89`).
    pub file_sync_level: i32,
    /// Whether the archive publishes recording events
    /// (`AeronArchive.java:2797`). This build refuses it — see
    /// [`ConfigError::RecordingEventsNotImplemented`].
    pub recording_events_enabled: bool,
    /// How long a control session waits for the archive's answer
    /// (`AeronArchive.java:2680`, ten seconds) — what this build's sessions
    /// already wait, now a name a deployment can set.
    pub message_timeout_ns: i64,
    /// How many times one response is offered before the turn gives up
    /// (`AeronArchive.java:2692`, three) — what
    /// [`crate::server::response_proxy::SEND_ATTEMPTS`] is.
    pub message_retry_attempts: usize,
    /// The mark file's error buffer (`Archive.java:633`, 1 MiB).
    pub error_buffer_length: usize,
    /// How long one turn of the conductor's may take before it is counted as
    /// over the threshold (`Archive.java:548`, 100 ms). What this build's
    /// archive already is — and now a name a deployment can set.
    pub conductor_cycle_threshold_ns: i64,
    /// The recorder's (`Archive.java:560`, 100 ms). Allocated and labelled,
    /// never measured in this build: the reference measures it only from
    /// `DedicatedModeArchiveConductor` (`:191`) and this build runs `SHARED`.
    pub recorder_cycle_threshold_ns: i64,
    /// The replayer's (`Archive.java:572`, 100 ms), for the same reason.
    pub replayer_cycle_threshold_ns: i64,
    /// The size a catalog **starts** at (`Archive.java:487`,
    /// `Catalog.DEFAULT_CAPACITY`); it grows as records are added, so this is
    /// where the file begins rather than how large it may become.
    pub catalog_capacity: usize,
}

impl ArchiveConfig {
    /// Resolve the configuration from an invocation's arguments: `-D` pairs,
    /// and properties files.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for a `-D` with no `=`, a file that will not read, or
    /// any of the refusals [`ArchiveConfig::resolve`] makes.
    pub fn from_args<I, S>(args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut properties = Vec::new();

        for argument in args {
            let argument = argument.into();

            if let Some(body) = argument.strip_prefix("-D") {
                let Some((name, value)) = body.split_once('=') else {
                    return Err(ConfigError::MalformedArgument { argument });
                };

                properties.push((name.to_owned(), value.to_owned()));
            } else {
                properties.extend(read_properties_file(&argument)?);
            }
        }

        Self::resolve(&properties)
    }

    /// The testable core: a property list, and nothing else.
    ///
    /// # Errors
    ///
    /// As [`ConfigError`].
    pub fn resolve(properties: &[(String, String)]) -> Result<Self, ConfigError> {
        let get = |name: &str| -> Option<&str> {
            properties
                .iter()
                .rev()
                .find(|(property, _)| property == name)
                .map(|(_, value)| value.as_str())
        };

        let aeron_dir = get(settings::AERON_DIR).ok_or(ConfigError::MissingAeronDir)?;

        let control_channel_enabled =
            boolean(get(settings::CONTROL_CHANNEL_ENABLED)).unwrap_or(true);

        let control_channel = non_empty(get(settings::CONTROL_CHANNEL)).map(str::to_owned);
        if control_channel_enabled {
            let Some(channel) = control_channel.as_deref() else {
                return Err(ConfigError::MissingControlChannel);
            };

            if !channel.starts_with("aeron:udp") {
                return Err(ConfigError::ControlChannelNotUdp {
                    channel: channel.to_owned(),
                });
            }
        }

        let local_control_channel = non_empty(get(settings::LOCAL_CONTROL_CHANNEL))
            .unwrap_or(LOCAL_CONTROL_CHANNEL_DEFAULT)
            .to_owned();
        if !local_control_channel.starts_with("aeron:ipc") {
            return Err(ConfigError::LocalControlChannelNotIpc {
                channel: local_control_channel,
            });
        }

        let threading_mode = match get(settings::THREADING_MODE) {
            None => ThreadingMode::Dedicated,
            Some(value) => {
                ThreadingMode::parse(value).ok_or_else(|| ConfigError::UnknownThreadingMode {
                    value: value.to_owned(),
                })?
            }
        };

        // The reference's own refusal, and it is a throw rather than a fall
        // back: `CommonContext.threadName`'s `default ->` arm
        // (`CommonContext.java:564-565`).
        let thread_naming = match get(settings::THREAD_NAMING) {
            None => ThreadNaming::DEFAULT,
            Some(value) => {
                ThreadNaming::parse(value).ok_or_else(|| ConfigError::UnknownThreadNaming {
                    value: value.to_owned(),
                })?
            }
        };

        // The reference's default is written as the **class name**
        // (`Archive.java:420`), and a name it cannot resolve is a failure to
        // start rather than a fall back — the supplier throws out of `conclude`
        // (`:2134-2142`'s `default ->` arm rethrows the `ClassNotFoundException`).
        // See [`crate::server::idle`] for the two names the reference resolves
        // and this build refuses instead.
        let idle_strategy = match get(settings::IDLE_STRATEGY) {
            None => IdleStrategy::DEFAULT,
            Some(value) => {
                IdleStrategy::parse(value).ok_or_else(|| ConfigError::UnknownIdleStrategy {
                    name: settings::IDLE_STRATEGY.to_owned(),
                    value: value.to_owned(),
                })?
            }
        };

        // ★ P2-9a: the two per-agent names, and the reference's fallback — a
        // per-agent name that names nothing gets the **archive's** strategy, not
        // the default (`Archive.java:1423-1440`; the property is read by
        // `Configuration.recorderIdleStrategySupplier(null)` and only the
        // fallback is the archive's own supplier). Resolved here rather than
        // where each thread is built, so that a name this build has no strategy
        // for is refused at the same point and in the same words as the
        // archive's own — the reference reads these two inside its `DEDICATED`
        // arm, so under `SHARED` it never looks at them; this build resolves
        // every name it is given.
        let fall_back_to_the_archives = |name: &str| -> Result<IdleStrategy, ConfigError> {
            match get(name) {
                None => Ok(idle_strategy),
                Some(value) => {
                    IdleStrategy::parse(value).ok_or_else(|| ConfigError::UnknownIdleStrategy {
                        name: name.to_owned(),
                        value: value.to_owned(),
                    })
                }
            }
        };
        let recorder_idle_strategy = fall_back_to_the_archives(settings::RECORDER_IDLE_STRATEGY)?;
        let replayer_idle_strategy = fall_back_to_the_archives(settings::REPLAYER_IDLE_STRATEGY)?;

        let archive_id = match get(settings::ARCHIVE_ID) {
            // Absent is "not told", which the archive runs as
            // [`ARCHIVE_ID_DEFAULT`]. **A configured id is an id whatever its
            // sign**: the reference parses this property as a long and tests
            // nothing about it (`Archive.Configuration.archiveId`,
            // `Archive.java:1051-1058`), and its own suite runs a *destination*
            // archive as `-7777` (`aeron_archive_test.cpp:452`) and asserts the
            // id it reports back (`:2659`). Dropping negatives here made that
            // archive answer `-1` instead — a whole family of criteria red
            // before their first request, which is how it was found.
            None => None,
            Some(value) => match value.parse::<i64>() {
                Ok(id) => Some(id),
                Err(_) => {
                    return Err(ConfigError::InvalidValue {
                        name: settings::ARCHIVE_ID.to_owned(),
                        value: value.to_owned(),
                    });
                }
            },
        };

        // The segment file length is checked where the reference checks it —
        // in the archive's own constructor rather than in the context
        // (`Archive.java:1480-1489`) — and refused with its words.
        let segment_file_length = size(
            get(settings::SEGMENT_FILE_LENGTH),
            settings::SEGMENT_FILE_LENGTH,
        )?
        .unwrap_or(SEGMENT_FILE_LENGTH_DEFAULT);

        if !segment_file_length.is_power_of_two() {
            return Err(ConfigError::SegmentFileLengthNotPowerOfTwo {
                length: segment_file_length,
            });
        }

        if !(TERM_MIN_LENGTH..=TERM_MAX_LENGTH).contains(&segment_file_length) {
            return Err(ConfigError::SegmentFileLengthOutOfRange {
                length: segment_file_length,
            });
        }

        let file_io_max_length = size(
            get(settings::FILE_IO_MAX_LENGTH),
            settings::FILE_IO_MAX_LENGTH,
        )?
        .unwrap_or(FILE_IO_MAX_LENGTH_DEFAULT);

        if file_io_max_length < TERM_MIN_LENGTH || !file_io_max_length.is_power_of_two() {
            return Err(ConfigError::InvalidFileIoMaxLength {
                length: file_io_max_length,
            });
        }

        let recording_events_enabled =
            boolean(get(settings::RECORDING_EVENTS_ENABLED)).unwrap_or(false);

        if recording_events_enabled {
            return Err(ConfigError::RecordingEventsNotImplemented);
        }

        Ok(Self {
            aeron_dir: PathBuf::from(aeron_dir),
            archive_dir: PathBuf::from(
                non_empty(get(settings::ARCHIVE_DIR)).unwrap_or(ARCHIVE_DIR_DEFAULT),
            ),
            mark_file_dir: non_empty(get(settings::MARK_FILE_DIR)).map(PathBuf::from),
            delete_dir_on_start: boolean(get(settings::DIR_DELETE_ON_START)).unwrap_or(false),
            archive_id,
            control_channel_enabled,
            control_channel,
            control_stream_id: stream_id(
                get(settings::CONTROL_STREAM_ID),
                settings::CONTROL_STREAM_ID,
            )?
            .unwrap_or(CONTROL_STREAM_ID_DEFAULT),
            local_control_channel,
            local_control_stream_id: stream_id(
                get(settings::LOCAL_CONTROL_STREAM_ID),
                settings::LOCAL_CONTROL_STREAM_ID,
            )?
            .unwrap_or(CONTROL_STREAM_ID_DEFAULT),
            control_response_channel: non_empty(get(settings::CONTROL_RESPONSE_CHANNEL))
                .map(str::to_owned),
            control_response_stream_id: stream_id(
                get(settings::CONTROL_RESPONSE_STREAM_ID),
                settings::CONTROL_RESPONSE_STREAM_ID,
            )?
            .unwrap_or(CONTROL_RESPONSE_STREAM_ID_DEFAULT),
            control_term_buffer_sparse: boolean(get(settings::CONTROL_TERM_BUFFER_SPARSE))
                .unwrap_or(true),
            control_term_buffer_length: size(
                get(settings::CONTROL_TERM_BUFFER_LENGTH),
                settings::CONTROL_TERM_BUFFER_LENGTH,
            )?
            .unwrap_or(CONTROL_TERM_BUFFER_LENGTH_DEFAULT),
            control_mtu_length: size(
                get(settings::CONTROL_MTU_LENGTH),
                settings::CONTROL_MTU_LENGTH,
            )?,
            connect_timeout_ns: duration(
                get(settings::CONNECT_TIMEOUT),
                settings::CONNECT_TIMEOUT,
            )?
            .unwrap_or(CONNECT_TIMEOUT_DEFAULT_NS),
            session_liveness_check_interval_ns: duration(
                get(settings::SESSION_LIVENESS_CHECK_INTERVAL),
                settings::SESSION_LIVENESS_CHECK_INTERVAL,
            )?
            .unwrap_or(SESSION_LIVENESS_CHECK_INTERVAL_DEFAULT_NS),
            threading_mode,
            thread_naming,
            idle_strategy,
            recorder_idle_strategy,
            replayer_idle_strategy,
            authenticator_supplier: non_empty(get(settings::AUTHENTICATOR_SUPPLIER))
                .unwrap_or(AUTHENTICATOR_SUPPLIER_DEFAULT)
                .to_owned(),
            authorisation_service_supplier: non_empty(get(
                settings::AUTHORISATION_SERVICE_SUPPLIER,
            ))
            .unwrap_or(AUTHORISATION_SERVICE_SUPPLIER_DEFAULT)
            .to_owned(),
            replication_channel: non_empty(get(settings::REPLICATION_CHANNEL)).map(str::to_owned),
            max_concurrent_recordings: integer(
                get(settings::MAX_CONCURRENT_RECORDINGS),
                settings::MAX_CONCURRENT_RECORDINGS,
            )?
            .unwrap_or(MAX_CONCURRENT_RECORDINGS_DEFAULT),
            max_concurrent_replays: integer(
                get(settings::MAX_CONCURRENT_REPLAYS),
                settings::MAX_CONCURRENT_REPLAYS,
            )?
            .unwrap_or(MAX_CONCURRENT_REPLAYS_DEFAULT),
            segment_file_length,
            file_io_max_length,
            // The **constant**, not whatever the segment file length resolved
            // to: `LOW_STORAGE_SPACE_THRESHOLD_DEFAULT = SEGMENT_FILE_LENGTH_DEFAULT`
            // (`Archive.java:350`), so a build with a 1 MB segment length
            // still gets a 128 MB threshold.
            low_storage_space_threshold: size(
                get(settings::LOW_STORAGE_SPACE_THRESHOLD),
                settings::LOW_STORAGE_SPACE_THRESHOLD,
            )?
            .unwrap_or(SEGMENT_FILE_LENGTH_DEFAULT) as u64,
            file_sync_level: integer(get(settings::FILE_SYNC_LEVEL), settings::FILE_SYNC_LEVEL)?
                .unwrap_or(0),
            recording_events_enabled,
            // The three names this slice wires. Each is read the way its
            // neighbours are — through the same helpers — so that a value that
            // will not parse is the *same* refusal as everywhere else in this
            // table, not a new one.
            message_timeout_ns: duration(
                get(settings::MESSAGE_TIMEOUT),
                settings::MESSAGE_TIMEOUT,
            )?
            .unwrap_or(MESSAGE_TIMEOUT_DEFAULT_NS),
            message_retry_attempts: retry_attempts(
                get(settings::MESSAGE_RETRY_ATTEMPTS),
                settings::MESSAGE_RETRY_ATTEMPTS,
            )?
            .unwrap_or(MESSAGE_RETRY_ATTEMPTS_DEFAULT),
            error_buffer_length: size(
                get(settings::ERROR_BUFFER_LENGTH),
                settings::ERROR_BUFFER_LENGTH,
            )?
            .unwrap_or(ERROR_BUFFER_LENGTH_DEFAULT),
            catalog_capacity: size(get(settings::CATALOG_CAPACITY), settings::CATALOG_CAPACITY)?
                .unwrap_or(CATALOG_CAPACITY_DEFAULT),
            conductor_cycle_threshold_ns: duration(
                get(settings::CONDUCTOR_CYCLE_THRESHOLD),
                settings::CONDUCTOR_CYCLE_THRESHOLD,
            )?
            .unwrap_or(CYCLE_THRESHOLD_DEFAULT_NS),
            recorder_cycle_threshold_ns: duration(
                get(settings::RECORDER_CYCLE_THRESHOLD),
                settings::RECORDER_CYCLE_THRESHOLD,
            )?
            .unwrap_or(CYCLE_THRESHOLD_DEFAULT_NS),
            replayer_cycle_threshold_ns: duration(
                get(settings::REPLAYER_CYCLE_THRESHOLD),
                settings::REPLAYER_CYCLE_THRESHOLD,
            )?
            .unwrap_or(CYCLE_THRESHOLD_DEFAULT_NS),
        })
    }

    /// Where `archive-mark.dat` goes: the mark file directory when one was
    /// named, and the archive directory otherwise (`Archive.java:321-325`).
    pub fn mark_file_path(&self) -> PathBuf {
        self.mark_file_dir
            .as_deref()
            .unwrap_or(&self.archive_dir)
            .join("archive-mark.dat")
    }
}

/// A property that is present and empty is absent, which is what the
/// reference's `""` defaults mean (`Archive.java:320`, `:583`).
fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

/// `"true".equals(value)`, which is how the reference reads every one of its
/// booleans (`Archive.java:925`, `:1042`): anything that is not `true` is
/// false, and that is not an error.
fn boolean(value: Option<&str>) -> Option<bool> {
    value.map(|value| value.eq_ignore_ascii_case("true"))
}

/// An `i32` property, with the name only there to say which one was wrong.
/// A count of attempts, which is a `usize` here and an `int` there
/// (`AeronArchive.Configuration.MESSAGE_RETRY_ATTEMPTS_PROP_NAME`). Zero is
/// refused the way `integer` refuses what will not read: a response offered no
/// times is a setting that says nothing.
fn retry_attempts(value: Option<&str>, name: &str) -> Result<Option<usize>, ConfigError> {
    match value {
        None => Ok(None),
        Some(value) => match value.parse::<usize>() {
            Ok(attempts) if attempts > 0 => Ok(Some(attempts)),
            _ => Err(ConfigError::InvalidValue {
                name: name.to_owned(),
                value: value.to_owned(),
            }),
        },
    }
}

fn integer(value: Option<&str>, name: &str) -> Result<Option<i32>, ConfigError> {
    match value {
        None => Ok(None),
        Some(value) => value
            .parse::<i32>()
            .map(Some)
            .map_err(|_| ConfigError::InvalidValue {
                name: name.to_owned(),
                value: value.to_owned(),
            }),
    }
}

/// A stream id, which is an `i32` with one of the reference's defaults — named
/// apart from [`integer`] because a stream id is what most of them are, and a
/// reader looking for one should not have to notice that it is an `i32`.
fn stream_id(value: Option<&str>, name: &str) -> Result<Option<i32>, ConfigError> {
    integer(value, name)
}

/// A size with a suffix: `64k`, `1m`, `1408` (`SystemUtil.parseSize`, the
/// reference's own reader for the three control-channel sizes,
/// `ArchiveConductor.java:459-467`).
fn size(value: Option<&str>, name: &str) -> Result<Option<usize>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };

    let invalid = || ConfigError::InvalidValue {
        name: name.to_owned(),
        value: value.to_owned(),
    };

    let (digits, scale) = match value.chars().last() {
        Some('k') | Some('K') => (&value[..value.len() - 1], 1024usize),
        Some('m') | Some('M') => (&value[..value.len() - 1], 1024 * 1024),
        Some('g') | Some('G') => (&value[..value.len() - 1], 1024 * 1024 * 1024),
        _ => (value, 1),
    };

    let number: usize = digits.trim().parse().map_err(|_| invalid())?;
    number.checked_mul(scale).map(Some).ok_or_else(invalid)
}

/// A duration in nanoseconds with a suffix: `5s`, `100ms`, `1us`, or bare
/// nanoseconds (`aeron_format_duration_ns`'s inverse, and what the reference's
/// `Configuration` does with these three names).
fn duration(value: Option<&str>, name: &str) -> Result<Option<i64>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };

    let invalid = || ConfigError::InvalidValue {
        name: name.to_owned(),
        value: value.to_owned(),
    };

    let (digits, scale) = if let Some(rest) = value.strip_suffix("ns") {
        (rest, 1i64)
    } else if let Some(rest) = value.strip_suffix("us") {
        (rest, 1_000)
    } else if let Some(rest) = value.strip_suffix("ms") {
        (rest, 1_000_000)
    } else if let Some(rest) = value.strip_suffix('s') {
        (rest, 1_000_000_000)
    } else {
        (value, 1)
    };

    let number: i64 = digits.trim().parse().map_err(|_| invalid())?;
    number.checked_mul(scale).map(Some).ok_or_else(invalid)
}

/// A properties file, as Agrona reads one: `name=value` per line, `#` and `!`
/// comments, blank lines ignored.
fn read_properties_file(path: &str) -> Result<Vec<(String, String)>, ConfigError> {
    let unreadable = || ConfigError::UnreadablePropertiesFile {
        argument: path.to_owned(),
    };

    let text = std::fs::read_to_string(Path::new(path)).map_err(|_| unreadable())?;

    Ok(text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with('!'))
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.trim().to_owned(), value.trim().to_owned()))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    /// The reference's 19 defaults, as `TestArchive.h:32-55` spells them, with
    /// the one property that has no default supplied.
    fn archive_defaults() -> Vec<(String, String)> {
        props(&[
            (settings::AERON_DIR, "/dev/shm/aeron-test"),
            (settings::ARCHIVE_DIR, "/tmp/archive/source"),
            (settings::MARK_FILE_DIR, "/dev/shm/aeron-test"),
            (
                settings::CONTROL_CHANNEL,
                "aeron:udp?endpoint=localhost:8010",
            ),
            (
                settings::REPLICATION_CHANNEL,
                "aeron:udp?endpoint=localhost:0",
            ),
            (settings::ARCHIVE_ID, "42"),
            (settings::THREADING_MODE, "SHARED"),
            (settings::IDLE_STRATEGY, "yield"),
            (settings::MAX_CONCURRENT_REPLAYS, "100"),
            (settings::CONTROL_CHANNEL_ENABLED, "true"),
            (settings::SEGMENT_FILE_LENGTH, "131072"),
            (settings::RECORDING_EVENTS_ENABLED, "false"),
        ])
    }

    /// **A negative archive id is an id.**
    ///
    /// The reference parses `aeron.archive.id` as a long and asks nothing about
    /// its sign (`Archive.Configuration.archiveId`, `Archive.java:1051-1058`),
    /// and its own suite relies on that: the *destination* archive of the
    /// replication criteria is started as `-7777` (`aeron_archive_test.cpp:452`)
    /// and the criteria assert that the id comes back (`:2659`, `:2793`,
    /// `:2910`). Dropping negatives — which this resolver used to do, reading
    /// "`-1` is not told" as "no negative is told" — made that archive answer
    /// `-1`, and all four criteria failed in their setup before sending a single
    /// request.
    #[test]
    fn a_negative_archive_id_is_taken_as_given() {
        for id in ["-7777", "-1"] {
            let mut properties = archive_defaults();
            properties.retain(|(property, _)| property != settings::ARCHIVE_ID);
            properties.push((settings::ARCHIVE_ID.to_owned(), id.to_owned()));

            let config = ArchiveConfig::resolve(&properties).expect("resolves");

            assert_eq!(
                Some(id.parse::<i64>().expect("a number")),
                config.archive_id,
                "the id `{id}` is what the archive was told it is"
            );
        }
    }

    #[test]
    fn the_defaults_are_the_references() {
        let config = ArchiveConfig::resolve(&archive_defaults()).expect("resolves");

        assert_eq!(config.control_stream_id, 10);
        assert_eq!(config.local_control_channel, "aeron:ipc?term-length=64k");
        assert_eq!(config.local_control_stream_id, 10);
        assert_eq!(config.control_response_stream_id, 20);
        assert_eq!(config.control_response_channel, None);
        assert!(config.control_term_buffer_sparse);
        assert_eq!(config.control_term_buffer_length, 64 * 1024);
        assert_eq!(config.control_mtu_length, None);
        assert_eq!(config.connect_timeout_ns, 5_000_000_000);
        assert_eq!(config.session_liveness_check_interval_ns, 1_000_000_000);
        assert_eq!(config.threading_mode, ThreadingMode::Shared);
        assert_eq!(
            config.authenticator_supplier,
            "io.aeron.security.DefaultAuthenticatorSupplier"
        );
        assert_eq!(config.archive_id, Some(42));
        assert!(!config.delete_dir_on_start);
    }

    /// The recording settings, whose defaults the harness mostly does not
    /// override — so they are the reference's own, checked name by name.
    #[test]
    fn the_recording_defaults_are_the_references() {
        let config = ArchiveConfig::resolve(&archive_defaults()).expect("resolves");

        // `TestArchive.h:43` sets this one; the rest are defaults.
        assert_eq!(config.segment_file_length, 128 * 1024);
        assert_eq!(config.max_concurrent_recordings, 20);
        assert_eq!(config.file_io_max_length, 1024 * 1024);
        assert_eq!(config.file_sync_level, 0);
        assert!(!config.recording_events_enabled);
        assert_eq!(
            config.low_storage_space_threshold,
            128 * 1024 * 1024,
            "the **constant**, not the configured segment file length (Archive.java:350)"
        );
    }

    #[test]
    fn the_segment_file_length_is_refused_the_references_way() {
        let not_a_power_of_two = ArchiveConfig::resolve(&props(&[
            (settings::AERON_DIR, "/dev/shm/a"),
            (
                settings::CONTROL_CHANNEL,
                "aeron:udp?endpoint=localhost:8010",
            ),
            (settings::SEGMENT_FILE_LENGTH, "100000"),
        ]))
        .expect_err("100000 is not a power of two");
        assert_eq!(
            not_a_power_of_two.to_string(),
            "segment file length not a power of 2: 100000"
        );

        let too_small = ArchiveConfig::resolve(&props(&[
            (settings::AERON_DIR, "/dev/shm/a"),
            (
                settings::CONTROL_CHANNEL,
                "aeron:udp?endpoint=localhost:8010",
            ),
            (settings::SEGMENT_FILE_LENGTH, "32k"),
        ]))
        .expect_err("a term is at least 64k");
        assert_eq!(
            too_small.to_string(),
            "segment file length not in valid range: 32768"
        );

        let too_big = ArchiveConfig::resolve(&props(&[
            (settings::AERON_DIR, "/dev/shm/a"),
            (
                settings::CONTROL_CHANNEL,
                "aeron:udp?endpoint=localhost:8010",
            ),
            (settings::SEGMENT_FILE_LENGTH, "2g"),
        ]))
        .expect_err("a term is at most 1g");
        assert_eq!(
            too_big.to_string(),
            "segment file length not in valid range: 2147483648"
        );
    }

    #[test]
    fn the_file_io_max_length_is_refused_the_references_way() {
        let too_small = ArchiveConfig::resolve(&props(&[
            (settings::AERON_DIR, "/dev/shm/a"),
            (
                settings::CONTROL_CHANNEL,
                "aeron:udp?endpoint=localhost:8010",
            ),
            (settings::FILE_IO_MAX_LENGTH, "32k"),
        ]))
        .expect_err("a term is at least 64k");
        assert_eq!(too_small.to_string(), "invalid fileIoMaxLength=32768");

        let not_a_power_of_two = ArchiveConfig::resolve(&props(&[
            (settings::AERON_DIR, "/dev/shm/a"),
            (
                settings::CONTROL_CHANNEL,
                "aeron:udp?endpoint=localhost:8010",
            ),
            (settings::FILE_IO_MAX_LENGTH, "96k"),
        ]))
        .expect_err("96k is not a power of two");
        assert_eq!(
            not_a_power_of_two.to_string(),
            "invalid fileIoMaxLength=98304"
        );
    }

    /// The events publication is a later slice's, and a build that was told to
    /// publish events it will never publish is the silent kind of wrong — so
    /// the name is read and **refused**, not ignored. The harness sets it
    /// false (`TestArchive.h:47`), which is the branch this slice runs.
    #[test]
    fn recording_events_are_refused_rather_than_ignored() {
        let mut properties = archive_defaults();
        properties.push((
            settings::RECORDING_EVENTS_ENABLED.to_owned(),
            "true".to_owned(),
        ));

        let refused = ArchiveConfig::resolve(&properties).expect_err("not this build's");
        assert_eq!(refused, ConfigError::RecordingEventsNotImplemented);
    }

    /// A default-configured archive has to be able to *start*, so the two
    /// names it hands its security collaborators have to be names this build
    /// can build. Asserting the strings alone would not have caught the defect
    /// this test was written for: the authorisation default used to be a class
    /// name that nothing can instantiate, and the archive refused to start
    /// with it.
    #[test]
    fn an_unconfigured_archive_can_build_its_security_collaborators() {
        let config = ArchiveConfig::resolve(&archive_defaults()).expect("resolves");

        assert_eq!("", config.authorisation_service_supplier);
        assert!(
            crate::server::auth::authorisation_service(&config.authorisation_service_supplier)
                .is_ok(),
            "an archive that cannot build this one cannot start at all"
        );
        assert!(crate::server::auth::authenticator(&config.authenticator_supplier).is_ok());
    }

    #[test]
    fn the_defaults_of_names_the_harness_never_sets_are_the_references_too() {
        // Everything `TestArchive.h` does not pass has to come from the
        // reference's own defaults, which is what these names are checked
        // against — a typo here is a name no deployment sets and nobody misses
        // until a real one does.
        let config = ArchiveConfig::resolve(&props(&[
            (settings::AERON_DIR, "/dev/shm/aeron-test"),
            (
                settings::CONTROL_CHANNEL,
                "aeron:udp?endpoint=localhost:8010",
            ),
        ]))
        .expect("resolves");

        assert_eq!(config.archive_dir, PathBuf::from("aeron-archive"));
        assert_eq!(config.mark_file_dir, None);
        assert_eq!(config.threading_mode, ThreadingMode::Dedicated);
        assert_eq!(
            config.idle_strategy,
            IdleStrategy::Backoff,
            "`aeron.archive.idle.strategy` is not one `TestArchive.h` sets, so its default is \
             the one that shows (`Archive.java:420`)"
        );
        assert_eq!(
            config.archive_id, None,
            "-1, or nothing, is the client's own id"
        );
        assert_eq!(config.replication_channel, None);
        assert_eq!(
            config.mark_file_path(),
            PathBuf::from("aeron-archive/archive-mark.dat"),
            "an unnamed mark file directory is the archive directory"
        );
    }

    /// `aeron.thread.naming` is a `CommonContext` name rather than one of the
    /// archive's own, so it is not in the 45-name ledger above — which means
    /// the ledger does not pin it and this does.
    #[test]
    fn the_thread_naming_is_the_references_and_a_typo_is_refused() {
        let config = ArchiveConfig::resolve(&archive_defaults()).expect("the defaults resolve");
        assert_eq!(
            config.thread_naming,
            ThreadNaming::Classic,
            "`THREAD_NAMING_DEFAULT` is `classic` (`CommonContext.java:547`)"
        );

        let config = resolve_with(&archive_defaults(), settings::THREAD_NAMING, "new")
            .expect("`new` is a spelling the reference knows");
        assert_eq!(config.thread_naming, ThreadNaming::New);

        // The reference's `default ->` arm throws (`:564-565`), so this is a
        // refusal and not a fall back to `classic`.
        assert!(
            matches!(
                resolve_with(&archive_defaults(), settings::THREAD_NAMING, "Classic"),
                Err(ConfigError::UnknownThreadNaming { value }) if value == "Classic"
            ),
            "a value that is neither spelling is refused by name"
        );
    }

    /// `aeron.archive.idle.strategy` — the conductor's idle strategy, and the
    /// **first** of the three names P2-9a owns to be honoured: the loop used to
    /// hardcode the `yield` the reference's own suite asks for, so the name was
    /// accepted and ignored and nothing could tell.
    ///
    /// The default is the reference's, written there as a class name
    /// (`Archive.java:420`), and a name it cannot resolve is a refusal rather
    /// than a fall back to it.
    #[test]
    fn the_idle_strategy_is_the_references_and_a_name_it_has_none_for_is_refused() {
        // `archive_defaults` carries the suite's own `yield` (it mirrors
        // `TestArchive.h:47`), so the default is asked of a list without it.
        let mut unset = archive_defaults();
        unset.retain(|(property, _)| property != settings::IDLE_STRATEGY);

        let config = ArchiveConfig::resolve(&unset).expect("the defaults resolve");
        assert_eq!(
            config.idle_strategy,
            IdleStrategy::Backoff,
            "`DEFAULT_IDLE_STRATEGY` is `org.agrona.concurrent.BackoffIdleStrategy` (`:420`)"
        );

        // The suite's own setting, and the shape a deployment is most likely to
        // write.
        for name in ["yield", "org.agrona.concurrent.YieldingIdleStrategy"] {
            let config = resolve_with(&archive_defaults(), settings::IDLE_STRATEGY, name)
                .unwrap_or_else(|error| panic!("{name} is a name the reference resolves: {error}"));
            assert_eq!(config.idle_strategy, IdleStrategy::Yielding, "{name}");
        }

        // `controllable` wants a `StatusIndicator` the archive does not give it,
        // and the reference's own `default ->` arm loads a class by reflection —
        // neither is a strategy this build can hand an agent, so both are
        // refused by name rather than idled as something else.
        //
        // All three names, because one table answers for all three: a name it
        // has no strategy for is refused wherever it was written, and the
        // refusal says which property it came from.
        for property in [
            settings::IDLE_STRATEGY,
            settings::RECORDER_IDLE_STRATEGY,
            settings::REPLAYER_IDLE_STRATEGY,
        ] {
            for name in ["controllable", "com.example.MyIdleStrategy"] {
                assert!(
                    matches!(
                        resolve_with(&archive_defaults(), property, name),
                        Err(ConfigError::UnknownIdleStrategy { name: refused, value })
                            if refused == property && value == name
                    ),
                    "{property}={name} is refused by name"
                );
            }
        }
    }

    /// ★ The reference's fallback for the two per-agent names: a name that names
    /// nothing gets the **archive's** strategy rather than the default
    /// (`Archive.java:1423-1440` — the property is read first, and only an
    /// absent one falls through to the archive's own supplier).
    #[test]
    fn an_agents_idle_strategy_falls_back_to_the_archives_own() {
        // `archive_defaults` carries the suite's own `yield`, so the archive's
        // strategy is asked of a list that names it explicitly.
        let mut properties = archive_defaults();
        properties.retain(|(property, _)| property != settings::IDLE_STRATEGY);
        properties.push((settings::IDLE_STRATEGY.to_owned(), "spin".to_owned()));

        let config = ArchiveConfig::resolve(&properties).expect("the defaults resolve");
        assert_eq!(config.idle_strategy, IdleStrategy::BusySpinning);
        assert_eq!(
            config.recorder_idle_strategy,
            IdleStrategy::BusySpinning,
            "a recorder that names nothing idles the way the archive does"
        );
        assert_eq!(config.replayer_idle_strategy, IdleStrategy::BusySpinning);

        // A name of its own wins over the archive's, for that agent and no
        // other.
        properties.push((
            settings::RECORDER_IDLE_STRATEGY.to_owned(),
            "noop".to_owned(),
        ));

        let config = ArchiveConfig::resolve(&properties).expect("the defaults resolve");
        assert_eq!(config.idle_strategy, IdleStrategy::BusySpinning);
        assert_eq!(config.recorder_idle_strategy, IdleStrategy::Noop);
        assert_eq!(config.replayer_idle_strategy, IdleStrategy::BusySpinning);
    }

    #[test]
    fn a_mark_file_directory_moves_the_mark_file_and_nothing_else() {
        let mut properties = archive_defaults();
        properties.push((
            settings::MARK_FILE_DIR.to_owned(),
            "/dev/shm/aeron-test".to_owned(),
        ));

        let config = ArchiveConfig::resolve(&properties).expect("resolves");

        assert_eq!(
            config.mark_file_path(),
            PathBuf::from("/dev/shm/aeron-test/archive-mark.dat")
        );
        assert_eq!(config.archive_dir, PathBuf::from("/tmp/archive/source"));
    }

    #[test]
    fn the_two_control_channels_media_types_are_refused_the_references_way() {
        // These are the two refusals P2-1c met the hard way, and the words
        // matter: a client reads them.
        let no_channel = ArchiveConfig::resolve(&props(&[(settings::AERON_DIR, "/dev/shm/a")]))
            .expect_err("the control channel has no default");
        assert_eq!(no_channel, ConfigError::MissingControlChannel);
        assert_eq!(
            no_channel.to_string(),
            "Archive.Context.controlChannel must be set"
        );

        let ipc_control = ArchiveConfig::resolve(&props(&[
            (settings::AERON_DIR, "/dev/shm/a"),
            (settings::CONTROL_CHANNEL, "aeron:ipc?term-length=64k"),
        ]))
        .expect_err("a control channel must be UDP");
        assert_eq!(
            ipc_control.to_string(),
            "Archive.Context.controlChannel must be UDP media: \
             uri=aeron:ipc?term-length=64k"
        );

        let udp_local = ArchiveConfig::resolve(&props(&[
            (settings::AERON_DIR, "/dev/shm/a"),
            (
                settings::CONTROL_CHANNEL,
                "aeron:udp?endpoint=localhost:8010",
            ),
            (
                settings::LOCAL_CONTROL_CHANNEL,
                "aeron:udp?endpoint=localhost:0",
            ),
        ]))
        .expect_err("a local control channel must be IPC");
        assert_eq!(
            udp_local.to_string(),
            "local control channel must be IPC media: uri=aeron:udp?endpoint=localhost:0"
        );
    }

    #[test]
    fn a_disabled_control_channel_needs_no_channel_and_the_local_one_still_exists() {
        // `ArchiveConductor.java:239-240` adds the local subscription with no
        // condition on it, so IPC-only mode still has one channel to serve.
        let config = ArchiveConfig::resolve(&props(&[
            (settings::AERON_DIR, "/dev/shm/a"),
            (settings::CONTROL_CHANNEL_ENABLED, "false"),
        ]))
        .expect("resolves");

        assert!(!config.control_channel_enabled);
        assert_eq!(config.control_channel, None);
        assert_eq!(config.local_control_channel, "aeron:ipc?term-length=64k");
    }

    #[test]
    fn sizes_and_durations_are_read_the_references_way() {
        let config = ArchiveConfig::resolve(&props(&[
            (settings::AERON_DIR, "/dev/shm/a"),
            (
                settings::CONTROL_CHANNEL,
                "aeron:udp?endpoint=localhost:8010",
            ),
            (settings::CONTROL_TERM_BUFFER_LENGTH, "1m"),
            (settings::CONTROL_MTU_LENGTH, "1408"),
            (settings::CONNECT_TIMEOUT, "250ms"),
            ("aeron.archive.session.liveness.check.interval", "2s"),
        ]))
        .expect("resolves");

        assert_eq!(config.control_term_buffer_length, 1024 * 1024);
        assert_eq!(config.control_mtu_length, Some(1408));
        assert_eq!(config.connect_timeout_ns, 250_000_000);
        assert_eq!(config.session_liveness_check_interval_ns, 2_000_000_000);
    }

    #[test]
    fn a_value_that_is_there_and_wrong_is_an_error_rather_than_a_default() {
        // The G-series lesson this repository paid for once already: an unknown
        // *name* is ignored, but a name that is present with a value nobody can
        // use must not quietly become the default.
        for (name, value) in [
            (settings::CONTROL_STREAM_ID, "ten"),
            (settings::CONTROL_TERM_BUFFER_LENGTH, "64 kilo"),
            (settings::CONNECT_TIMEOUT, "soon"),
            (settings::ARCHIVE_ID, "the-answer"),
            (settings::THREADING_MODE, "PARALLEL"),
        ] {
            let mut properties = archive_defaults();
            properties.retain(|(property, _)| property != name);
            properties.push((name.to_owned(), value.to_owned()));

            assert!(
                ArchiveConfig::resolve(&properties).is_err(),
                "{name}={value} must be refused"
            );
        }
    }

    /// ★ **Every one of the reference's 45 names is accounted for, and the
    /// account is what this case keeps from rotting.**
    ///
    /// The reference reads 45 `aeron.archive.*` names and **honours every one of
    /// them**: a deployment's property file is its configuration, and nothing in
    /// it is dropped on the floor. This build reads the same names, and what it
    /// does with some of them is less — so each name is in exactly one of three
    /// lists, and a name that moves between them without this list moving is a
    /// failing case:
    ///
    /// * **honoured**: setting it changes the resolved configuration;
    /// * **accepted, not honoured**: setting it changes nothing, and the reason
    ///   beside it is a fact about this build or about the reference — not an
    ///   apology. Two of the eight are the reference's own doing:
    ///   `max.catalog.entries` has **no reader there either**
    ///   (`Archive.maxCatalogEntries()` is defined and never called), and
    ///   `client.name` is read by the *client*, whose handling of it here is a
    ///   recorded deviation;
    /// * **refused**: one name, the events publication, refused by name rather
    ///   than accepted with nothing behind it.
    ///
    /// What this replaces is silence: before this slice a deployment could set
    /// any of the fourteen names this list held then and be told nothing at all
    /// — three of them
    /// (`message.timeout`, `message.retry.attempts`, `error.buffer.length`) were
    /// hardcoded to the reference's own defaults, so only a deployment that
    /// *changed* one was affected, and it could not tell.
    #[test]
    fn every_name_the_reference_reads_is_accounted_for() {
        // (name, a value a deployment might set — never the default)
        const HONOURED: &[(&str, &str)] = &[
            (settings::ARCHIVE_DIR, "/tmp/archive/other"),
            (settings::MARK_FILE_DIR, "/dev/shm/aeron-other"),
            (settings::DIR_DELETE_ON_START, "true"),
            (settings::THREADING_MODE, "INVOKER"),
            (settings::AUTHENTICATOR_SUPPLIER, "SomeAuthenticator"),
            (settings::AUTHORISATION_SERVICE_SUPPLIER, "SomeService"),
            (settings::CONNECT_TIMEOUT, "7s"),
            ("aeron.archive.session.liveness.check.interval", "2s"),
            (
                settings::REPLICATION_CHANNEL,
                "aeron:udp?endpoint=localhost:9999",
            ),
            (settings::ARCHIVE_ID, "7"),
            (settings::CONTROL_CHANNEL_ENABLED, "false"),
            (
                settings::CONTROL_CHANNEL,
                "aeron:udp?endpoint=localhost:8123",
            ),
            (settings::CONTROL_STREAM_ID, "11"),
            (
                settings::LOCAL_CONTROL_CHANNEL,
                "aeron:ipc?term-length=128k",
            ),
            (settings::LOCAL_CONTROL_STREAM_ID, "12"),
            (
                settings::CONTROL_RESPONSE_CHANNEL,
                "aeron:ipc?term-length=64k",
            ),
            (settings::CONTROL_RESPONSE_STREAM_ID, "21"),
            (settings::CONTROL_TERM_BUFFER_SPARSE, "false"),
            (settings::CONTROL_TERM_BUFFER_LENGTH, "128k"),
            (settings::CONTROL_MTU_LENGTH, "1408"),
            (settings::MAX_CONCURRENT_RECORDINGS, "5"),
            (settings::MAX_CONCURRENT_REPLAYS, "7"),
            (settings::SEGMENT_FILE_LENGTH, "262144"),
            (settings::FILE_IO_MAX_LENGTH, "262144"),
            (settings::LOW_STORAGE_SPACE_THRESHOLD, "512k"),
            (settings::FILE_SYNC_LEVEL, "1"),
            // The three this slice wired, each of which was hardcoded to the
            // reference's default before it.
            (settings::MESSAGE_TIMEOUT, "5s"),
            (settings::MESSAGE_RETRY_ATTEMPTS, "5"),
            (settings::ERROR_BUFFER_LENGTH, "2m"),
            (settings::CATALOG_CAPACITY, "2m"),
            // P2-9b: the three thresholds used to be names this build accepted
            // and did nothing with; they now set what the 104 counters are
            // measured against.
            (settings::CONDUCTOR_CYCLE_THRESHOLD, "5ms"),
            (settings::RECORDER_CYCLE_THRESHOLD, "7ms"),
            (settings::REPLAYER_CYCLE_THRESHOLD, "9ms"),
            // P2-9a: the conductor's idle strategy, which used to be a name this
            // build accepted and did nothing with — the loop yielded whatever it
            // was told. Given as the **class name**, which is how the
            // reference's own default is written (`Archive.java:420`).
            (
                settings::IDLE_STRATEGY,
                "org.agrona.concurrent.SleepingIdleStrategy",
            ),
            // P2-9a: the two per-agent names, which the reference reads inside
            // its `DEDICATED` arm and this build resolves for every mode. The
            // values are neither the default nor the archive's own: a fallback
            // that is never overridden would let a name that does nothing pass
            // this list.
            (settings::RECORDER_IDLE_STRATEGY, "spin"),
            (settings::REPLAYER_IDLE_STRATEGY, "noop"),
        ];

        // (name, a value) — the one the reference has that this build refuses
        const REFUSED: &[(&str, &str)] = &[(settings::RECORDING_EVENTS_ENABLED, "true")];

        let baseline = ArchiveConfig::resolve(&archive_defaults()).expect("the defaults resolve");

        for (name, value) in HONOURED {
            let config = resolve_with(&archive_defaults(), name, value)
                .unwrap_or_else(|error| panic!("{name} must be accepted: {error}"));

            assert_ne!(
                baseline, config,
                "{name} is listed as honoured, but setting it to {value} changed nothing"
            );
        }

        for (name, why) in NOT_HONOURED {
            // A value a deployment might set: any value at all will do, because
            // what is asserted is that it changes nothing.
            let config = resolve_with(&archive_defaults(), name, "0001")
                .unwrap_or_else(|error| panic!("{name} must be accepted: {error}"));

            assert_eq!(
                baseline, config,
                "{name} is listed as not honoured ({why}), but setting it changed the configuration"
            );
        }

        for (name, value) in REFUSED {
            assert!(
                matches!(
                    resolve_with(&archive_defaults(), name, value),
                    Err(ConfigError::RecordingEventsNotImplemented)
                ),
                "{name}={value} must be refused by name"
            );
        }

        assert_eq!(
            45,
            HONOURED.len() + NOT_HONOURED.len() + REFUSED.len(),
            "the reference reads 45 names, and every one of them belongs in exactly one list"
        );
    }

    /// The defaults with one name set to `value`, spelled the way a deployment
    /// would.
    fn resolve_with(
        defaults: &[(String, String)],
        name: &str,
        value: &str,
    ) -> Result<ArchiveConfig, ConfigError> {
        let mut properties = defaults.to_vec();
        properties.push((name.to_owned(), value.to_owned()));

        ArchiveConfig::resolve(&properties)
    }

    #[test]
    fn an_unknown_name_is_ignored() {
        // 45 names exist and this slice reads twenty; the rest arrive with the
        // sessions that use them. A deployment that spells all 45 must start.
        let mut properties = archive_defaults();
        properties.push((settings::SEGMENT_FILE_LENGTH.to_owned(), "128m".to_owned()));
        properties.push((settings::RECORD_CHECKSUM.to_owned(), "crc32".to_owned()));

        assert!(ArchiveConfig::resolve(&properties).is_ok());
    }

    #[test]
    fn the_last_spelling_of_a_name_wins() {
        let mut properties = archive_defaults();
        properties.push((settings::ARCHIVE_ID.to_owned(), "7".to_owned()));

        assert_eq!(
            ArchiveConfig::resolve(&properties)
                .expect("resolves")
                .archive_id,
            Some(7)
        );
    }

    #[test]
    fn arguments_are_properties_and_files() {
        let dir =
            std::env::temp_dir().join(format!("deepmsg-archive-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a temp directory");
        let file = dir.join("archive.properties");
        std::fs::write(
            &file,
            "# a comment\n! another\naeron.archive.id=99\n\n aeron.archive.control.stream.id = 11 \n",
        )
        .expect("writes");

        let config = ArchiveConfig::from_args([
            format!("-D{}={}", settings::AERON_DIR, "/dev/shm/a"),
            format!(
                "-D{}={}",
                settings::CONTROL_CHANNEL,
                "aeron:udp?endpoint=localhost:8010"
            ),
            file.to_string_lossy().into_owned(),
        ])
        .expect("resolves");

        assert_eq!(config.archive_id, Some(99));
        assert_eq!(config.control_stream_id, 11);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_argument_that_is_neither_is_refused_by_name() {
        let error = ArchiveConfig::from_args(["-Daeron.dir".to_owned()]).expect_err("no = in it");
        assert_eq!(
            error.to_string(),
            "malformed property argument (expected -Dname=value): -Daeron.dir"
        );

        let error = ArchiveConfig::from_args(["/no/such/file.properties".to_owned()])
            .expect_err("nothing there");
        assert_eq!(
            error.to_string(),
            "properties file could not be read: /no/such/file.properties"
        );
    }
}
