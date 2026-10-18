//! The consensus module's configuration: the `aeron.cluster.*` names it reads,
//! with the reference's defaults.
//!
//! Counterpart of `ConsensusModule.Context` / `Configuration`
//! (`aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModule.java:363-1725`).
//! Only the names `P3-4a` uses are read here; the rest stay `Pending` in
//! [`deepmsg_cluster_common::config::names`] until the slice that honours them
//! arrives. A name this build reads is marked `Honoured` there in the same
//! commit, so the table keeps saying what the code does.
//!
//! # What `conclude` owns
//!
//! The reference's `Context.conclude()` (`:1725-2100`) does three jobs at once:
//! it validates and normalises the directories, it creates the mark file and the
//! two on-disk state files, and it allocates the counters. This split keeps the
//! first: [`ConsensusModuleConfig::conclude`] settles paths and validates, and
//! the agent's `launch` creates the files and counters — because those need the
//! client, which needs a concluded directory.

use std::fmt;
use std::path::{Path, PathBuf};

use deepmsg_archive::client::context::ArchiveContext;
use deepmsg_cluster_common::config::{ConfigError, Properties};
use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;

use crate::error::ClusterError;

/// `aeron.cluster.dir` (`ClusteredServiceContainer.java:343`).
pub const CLUSTER_DIR_PROP_NAME: &str = "aeron.cluster.dir";
/// `CLUSTER_DIR_DEFAULT` (`ClusteredServiceContainer.java:349`).
pub const CLUSTER_DIR_DEFAULT: &str = "aeron-cluster";
/// `aeron.cluster.mark.file.dir` (`ClusteredServiceContainer.java:362`).
pub const MARK_FILE_DIR_PROP_NAME: &str = "aeron.cluster.mark.file.dir";
/// `aeron.cluster.id` (`ClusteredServiceContainer.java:211`).
pub const CLUSTER_ID_PROP_NAME: &str = "aeron.cluster.id";
/// `CLUSTER_ID_DEFAULT` (`ClusteredServiceContainer.java:217`).
pub const CLUSTER_ID_DEFAULT: i32 = 0;
/// `aeron.cluster.member.id` (`ConsensusModule.java:437`).
pub const CLUSTER_MEMBER_ID_PROP_NAME: &str = "aeron.cluster.member.id";
/// `CLUSTER_MEMBER_ID_DEFAULT` (`ConsensusModule.java:443`).
pub const CLUSTER_MEMBER_ID_DEFAULT: i32 = 0;
/// `aeron.cluster.members` (`ConsensusModule.java:507`).
pub const CLUSTER_MEMBERS_PROP_NAME: &str = "aeron.cluster.members";
/// `aeron.cluster.ingress.channel` (`AeronCluster.java:1267`).
pub const INGRESS_CHANNEL_PROP_NAME: &str = "aeron.cluster.ingress.channel";
/// `aeron.cluster.consensus.channel` (`ConsensusModule.java:595`).
pub const CONSENSUS_CHANNEL_PROP_NAME: &str = "aeron.cluster.consensus.channel";
/// `CONSENSUS_CHANNEL_DEFAULT` (`ConsensusModule.java:602`).
pub const CONSENSUS_CHANNEL_DEFAULT: &str = "aeron:udp";
/// `aeron.cluster.log.channel` (`ConsensusModule.java:527`).
pub const LOG_CHANNEL_PROP_NAME: &str = "aeron.cluster.log.channel";
/// `LOG_CHANNEL_DEFAULT` (`ConsensusModule.java:533`).
pub const LOG_CHANNEL_DEFAULT: &str = "aeron:udp?term-length=64m";
/// `aeron.cluster.log.control.enable` (`ConsensusModule.java:1026-1027`).
pub const LOG_CONTROL_ENABLE_PROP_NAME: &str = "aeron.cluster.log.control.enable";
/// `CONSENSUS_MODULE_ENABLE_CONTROL_ON_LOG_CHANNEL_DEFAULT` (`ConsensusModule.java:1569`).
pub const LOG_CONTROL_ENABLE_DEFAULT: bool = true;
/// `aeron.cluster.egress.channel` (`AeronCluster.java:1302`).
pub const EGRESS_CHANNEL_PROP_NAME: &str = "aeron.cluster.egress.channel";
/// `aeron.cluster.snapshot.channel` (`ClusteredServiceContainer.java:319`).
pub const SNAPSHOT_CHANNEL_PROP_NAME: &str = "aeron.cluster.snapshot.channel";
/// `SNAPSHOT_CHANNEL_DEFAULT` (`ConsensusModule.java:568`).
pub const SNAPSHOT_CHANNEL_DEFAULT: &str = "aeron:ipc?alias=snapshot";
/// `aeron.cluster.snapshot.stream.id` (`ClusteredServiceContainer.java:331`).
pub const SNAPSHOT_STREAM_ID_PROP_NAME: &str = "aeron.cluster.snapshot.stream.id";
/// `SNAPSHOT_STREAM_ID_DEFAULT` (`ConsensusModule.java:573`).
pub const SNAPSHOT_STREAM_ID_DEFAULT: i32 = 107;
/// `aeron.cluster.replay.channel` (`ClusteredServiceContainer.java:259`).
pub const REPLAY_CHANNEL_PROP_NAME: &str = "aeron.cluster.replay.channel";
/// `REPLAY_CHANNEL_DEFAULT` (`ClusteredServiceContainer.java:265`): IPC.
pub const REPLAY_CHANNEL_DEFAULT: &str = "aeron:ipc";
/// `aeron.cluster.replay.stream.id` (`ClusteredServiceContainer.java:271`).
pub const REPLAY_STREAM_ID_PROP_NAME: &str = "aeron.cluster.replay.stream.id";
/// `REPLAY_STREAM_ID_DEFAULT` (`ClusteredServiceContainer.java:277`).
pub const REPLAY_STREAM_ID_DEFAULT: i32 = 103;
/// `aeron.cluster.log.fragment.limit` (`ConsensusModule.java:...`), default 10.
pub const LOG_FRAGMENT_LIMIT_PROP_NAME: &str = "aeron.cluster.log.fragment.limit";
/// `LOG_FRAGMENT_LIMIT_DEFAULT` (`ConsensusModule.java:...`).
pub const LOG_FRAGMENT_LIMIT_DEFAULT: i32 = 10;
/// `aeron.cluster.replication.channel` (`ConsensusModule.java:620`); **required**.
pub const REPLICATION_CHANNEL_PROP_NAME: &str = "aeron.cluster.replication.channel";
/// `aeron.cluster.control.channel` (`ClusteredServiceContainer.java:283`).
pub const CONTROL_CHANNEL_PROP_NAME: &str = "aeron.cluster.control.channel";
/// `CONTROL_CHANNEL_DEFAULT` (`ClusteredServiceContainer.java:289`).
pub const CONTROL_CHANNEL_DEFAULT: &str = "aeron:ipc?term-length=128k";
/// `aeron.cluster.consensus.stream.id` (`ConsensusModule.java:608`).
pub const CONSENSUS_STREAM_ID_PROP_NAME: &str = "aeron.cluster.consensus.stream.id";
/// `CONSENSUS_STREAM_ID_DEFAULT` (`ConsensusModule.java:614`).
pub const CONSENSUS_STREAM_ID_DEFAULT: i32 = 108;
/// `aeron.cluster.log.stream.id` (`ConsensusModule.java:557`).
pub const LOG_STREAM_ID_PROP_NAME: &str = "aeron.cluster.log.stream.id";
/// `LOG_STREAM_ID_DEFAULT` (`ConsensusModule.java:563`).
pub const LOG_STREAM_ID_DEFAULT: i32 = 100;
/// `aeron.cluster.ingress.stream.id` (`AeronCluster.java:1279`).
pub const INGRESS_STREAM_ID_PROP_NAME: &str = "aeron.cluster.ingress.stream.id";
/// `INGRESS_STREAM_ID_DEFAULT` (`AeronCluster.java:1285`).
pub const INGRESS_STREAM_ID_DEFAULT: i32 = 101;
/// `aeron.cluster.egress.stream.id` (`AeronCluster.java:1314`).
pub const EGRESS_STREAM_ID_PROP_NAME: &str = "aeron.cluster.egress.stream.id";
/// `EGRESS_STREAM_ID_DEFAULT` (`AeronCluster.java:1320`).
pub const EGRESS_STREAM_ID_DEFAULT: i32 = 102;
/// `aeron.cluster.service.stream.id` (`ClusteredServiceContainer.java:295`).
pub const SERVICE_STREAM_ID_PROP_NAME: &str = "aeron.cluster.service.stream.id";
/// `SERVICE_STREAM_ID_DEFAULT` (`ClusteredServiceContainer.java:301`).
pub const SERVICE_STREAM_ID_DEFAULT: i32 = 104;
/// `aeron.cluster.consensus.module.stream.id` (`ClusteredServiceContainer.java:307`).
pub const CONSENSUS_MODULE_STREAM_ID_PROP_NAME: &str = "aeron.cluster.consensus.module.stream.id";
/// `CONSENSUS_MODULE_STREAM_ID_DEFAULT` (`ClusteredServiceContainer.java:313`).
pub const CONSENSUS_MODULE_STREAM_ID_DEFAULT: i32 = 105;
/// `aeron.cluster.service.count` (`ConsensusModule.java:709`).
pub const SERVICE_COUNT_PROP_NAME: &str = "aeron.cluster.service.count";
/// `SERVICE_COUNT_DEFAULT` (`ConsensusModule.java:715`).
pub const SERVICE_COUNT_DEFAULT: i32 = 1;
/// `MAX_SERVICE_COUNT` (`ClusteredServiceContainer.java:234`).
pub const MAX_SERVICE_COUNT: i32 = 10;
/// `aeron.cluster.ingress.fragment.limit` (`ConsensusModule.java:427`).
pub const INGRESS_FRAGMENT_LIMIT_PROP_NAME: &str = "aeron.cluster.ingress.fragment.limit";
/// `CLUSTER_INGRESS_FRAGMENT_LIMIT_DEFAULT` (`ConsensusModule.java:433`).
pub const INGRESS_FRAGMENT_LIMIT_DEFAULT: i32 = 50;
/// `aeron.cluster.ingress.ipc.allowed` (`ConsensusModule.java:439`).
pub const INGRESS_IPC_ALLOWED_PROP_NAME: &str = "aeron.cluster.ingress.ipc.allowed";
/// `CLUSTER_INGRESS_IPC_ALLOWED_DEFAULT` (`ConsensusModule.java:445`).
pub const INGRESS_IPC_ALLOWED_DEFAULT: bool = false;
/// `aeron.cluster.max.sessions` (`ConsensusModule.java:721`).
pub const MAX_SESSIONS_PROP_NAME: &str = "aeron.cluster.max.sessions";
/// `MAX_CONCURRENT_SESSIONS_DEFAULT` (`ConsensusModule.java:727`).
pub const MAX_CONCURRENT_SESSIONS_DEFAULT: i32 = 10;
/// `aeron.cluster.authenticator.supplier` (`ConsensusModule.java:807`).
pub const AUTHENTICATOR_SUPPLIER_PROP_NAME: &str = "aeron.cluster.authenticator.supplier";
/// The class the reference names when the property is unset
/// (`ConsensusModule.java:804-807`); the mark file records it as well.
pub const DEFAULT_AUTHENTICATOR_SUPPLIER: &str = "io.aeron.security.DefaultAuthenticatorSupplier";
/// `aeron.cluster.authorisation.service.supplier` (`ConsensusModule.java:813`).
pub const AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME: &str =
    "aeron.cluster.authorisation.service.supplier";
/// `aeron.cluster.session.timeout` (`ConsensusModule.java:733`), 10 s.
pub const SESSION_TIMEOUT_PROP_NAME: &str = "aeron.cluster.session.timeout";
/// `SESSION_TIMEOUT_DEFAULT_NS` (`ConsensusModule.java:739`).
pub const SESSION_TIMEOUT_DEFAULT_NS: i64 = 10_000_000_000;
/// `aeron.cluster.leader.heartbeat.timeout` (`ConsensusModule.java:745`), 10 s.
pub const LEADER_HEARTBEAT_TIMEOUT_PROP_NAME: &str = "aeron.cluster.leader.heartbeat.timeout";
/// `LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS` (`ConsensusModule.java:751`).
pub const LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS: i64 = 10_000_000_000;
/// `aeron.cluster.leader.heartbeat.interval` (`ConsensusModule.java:757`), 200 ms.
pub const LEADER_HEARTBEAT_INTERVAL_PROP_NAME: &str = "aeron.cluster.leader.heartbeat.interval";
/// `LEADER_HEARTBEAT_INTERVAL_DEFAULT_NS` (`ConsensusModule.java:763`).
pub const LEADER_HEARTBEAT_INTERVAL_DEFAULT_NS: i64 = 200_000_000;
/// `aeron.cluster.startup.canvass.timeout` (`ConsensusModule.java:770`), 60 s.
pub const STARTUP_CANVASS_TIMEOUT_PROP_NAME: &str = "aeron.cluster.startup.canvass.timeout";
/// `STARTUP_CANVASS_TIMEOUT_DEFAULT_NS` (`ConsensusModule.java:777`).
pub const STARTUP_CANVASS_TIMEOUT_DEFAULT_NS: i64 = 60_000_000_000;
/// `aeron.cluster.termination.timeout` (`ConsensusModule.java:840`), 10 s: how
/// long a terminating leader waits for followers that never answer.
pub const TERMINATION_TIMEOUT_PROP_NAME: &str = "aeron.cluster.termination.timeout";
/// `TERMINATION_TIMEOUT_DEFAULT_NS` (`ConsensusModule.java:886`).
pub const TERMINATION_TIMEOUT_DEFAULT_NS: i64 = 10_000_000_000;
/// `aeron.cluster.election.timeout` (`ConsensusModule.java:783`), 1 s.
pub const ELECTION_TIMEOUT_PROP_NAME: &str = "aeron.cluster.election.timeout";
/// `ELECTION_TIMEOUT_DEFAULT_NS` (`ConsensusModule.java:789`).
pub const ELECTION_TIMEOUT_DEFAULT_NS: i64 = 1_000_000_000;
/// `aeron.cluster.election.status.interval` (`ConsensusModule.java:795`), 100 ms.
pub const ELECTION_STATUS_INTERVAL_PROP_NAME: &str = "aeron.cluster.election.status.interval";
/// `ELECTION_STATUS_INTERVAL_DEFAULT_NS` (`ConsensusModule.java:801`).
pub const ELECTION_STATUS_INTERVAL_DEFAULT_NS: i64 = 100_000_000;
/// `aeron.cluster.appointed.leader.id` (`ConsensusModule.java:451`).
pub const APPOINTED_LEADER_ID_PROP_NAME: &str = "aeron.cluster.appointed.leader.id";
/// `aeron.cluster.file.sync.level` (`ConsensusModule.java:923`).
pub const FILE_SYNC_LEVEL_PROP_NAME: &str = "aeron.cluster.file.sync.level";
/// `FILE_SYNC_LEVEL_DEFAULT` (`ConsensusModule.java:929`).
pub const FILE_SYNC_LEVEL_DEFAULT: i32 = 0;
/// `aeron.cluster.error.buffer.length` (`ConsensusModule.java:828`).
pub const ERROR_BUFFER_LENGTH_PROP_NAME: &str = "aeron.cluster.error.buffer.length";
/// `aeron.cluster.cycle.threshold` (`ConsensusModule.java:847`), 100 ms.
pub const CYCLE_THRESHOLD_PROP_NAME: &str = "aeron.cluster.cycle.threshold";
/// `CYCLE_THRESHOLD_DEFAULT_NS` (`ConsensusModule.java:853`).
pub const CYCLE_THRESHOLD_DEFAULT_NS: i64 = 100_000_000;
/// `aeron.cluster.total.snapshot.duration.threshold` (`ConsensusModule.java:859`), 10 s.
pub const TOTAL_SNAPSHOT_DURATION_THRESHOLD_PROP_NAME: &str =
    "aeron.cluster.total.snapshot.threshold";
/// `TOTAL_SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS` (`ConsensusModule.java:866`).
pub const TOTAL_SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS: i64 = 10_000_000_000;
/// `aeron.cluster.clock` (`ConsensusModule.java:906`); only milliseconds is implemented.
pub const CLUSTER_CLOCK_PROP_NAME: &str = "aeron.cluster.clock";
/// `aeron.dir`, the driver's directory (`CommonContext.java`).
pub const AERON_DIR_PROP_NAME: &str = "aeron.dir";
/// `aeron.cluster.idle.strategy` (`ClusteredServiceContainer.java:575`).
pub const CLUSTER_IDLE_STRATEGY_PROP_NAME: &str = "aeron.cluster.idle.strategy";
/// `DEFAULT_IDLE_STRATEGY` (`ClusteredServiceContainer.java:569`).
pub const CLUSTER_IDLE_STRATEGY_DEFAULT: &str = "org.agrona.concurrent.BackoffIdleStrategy";
/// `Context.appVersion`'s default (`ConsensusModule.java:1610`).
pub const APP_VERSION_DEFAULT: i32 = 1;

/// The only clock this build has: the millisecond one
/// (`MillisecondClusterClock`, `ClusterClock.timeUnit()` = milliseconds).
pub const CLUSTER_TIME_UNIT: ClusterTimeUnit = ClusterTimeUnit::MILLIS;

/// `ClusterSession.MAX_ENCODED_PRINCIPAL_LENGTH` (`ClusterSession.java:49`).
pub const MAX_ENCODED_PRINCIPAL_LENGTH: usize = 4 * 1024;

/// `ConsensusModule.Configuration.SESSION_LIMIT_MSG` (`ConsensusModule.java:578`).
pub const SESSION_LIMIT_MSG: &str = "concurrent session limit";

/// `ConsensusModule.Configuration.SESSION_INVALID_VERSION_MSG`
/// (`ConsensusModule.java:588`).
pub const SESSION_INVALID_VERSION_MSG: &str = "invalid client version";

/// `ConsensusModule.Configuration.SESSION_REJECTED_MSG` (`ConsensusModule.java:583`).
pub const SESSION_REJECTED_MSG: &str = "session failed authentication";

/// `AeronCluster.Configuration.PROTOCOL_MAJOR_VERSION` (`AeronCluster.java:1209`).
pub const PROTOCOL_MAJOR_VERSION: i32 = 0;
/// `PROTOCOL_MINOR_VERSION` (`AeronCluster.java:1219`).
pub const PROTOCOL_MINOR_VERSION: i32 = 3;
/// `PROTOCOL_PATCH_VERSION` (`AeronCluster.java:1228`).
pub const PROTOCOL_PATCH_VERSION: i32 = 0;
/// `PROTOCOL_SEMANTIC_VERSION` (`AeronCluster.java:1228-1229`).
pub const PROTOCOL_SEMANTIC_VERSION: i32 = compose_semantic_version(
    PROTOCOL_MAJOR_VERSION,
    PROTOCOL_MINOR_VERSION,
    PROTOCOL_PATCH_VERSION,
);

/// Agrona's `SemanticVersion.compose`: major in the high 16 bits, then minor,
/// then patch (`SemanticVersion.java`).
pub const fn compose_semantic_version(major: i32, minor: i32, patch: i32) -> i32 {
    (major << 16) | (minor << 8) | patch
}

/// Agrona's `SemanticVersion.major`.
pub const fn semantic_version_major(version: i32) -> i32 {
    (version >> 16) & 0xFFFF
}

/// Why a configuration could not be settled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigProblem {
    /// A property the reference requires is absent.
    Missing(&'static str),
    /// A property did not parse.
    Value {
        /// The property name.
        name: &'static str,
        /// Its value.
        value: String,
    },
    /// A value is syntactically fine and semantically wrong.
    Invalid(String),
    /// A directory could not be canonicalised or created.
    Directory(String),
}

impl fmt::Display for ConfigProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing(name) => write!(f, "{name} must be specified"),
            Self::Value { name, value } => write!(f, "{name}={value:?} is not valid"),
            Self::Invalid(reason) => write!(f, "{reason}"),
            Self::Directory(reason) => write!(f, "{reason}"),
        }
    }
}

impl std::error::Error for ConfigProblem {}

impl From<ConfigError> for ConfigProblem {
    fn from(error: ConfigError) -> Self {
        Self::Invalid(error.to_string())
    }
}

/// The consensus module's settled configuration.
#[derive(Clone, Debug)]
pub struct ConsensusModuleConfig {
    /// `<aeron.cluster.dir>`: `recording.log`, `node-state.dat` and the `.lnk`.
    pub cluster_dir: PathBuf,
    /// Where `cluster-mark.dat` is; defaults to `cluster_dir`.
    pub mark_file_dir: PathBuf,
    /// The driver's CnC directory.
    pub aeron_dir: PathBuf,
    /// This member's id (`aeron.cluster.member.id`).
    pub member_id: i32,
    /// The cluster's id (`aeron.cluster.id`); the counter key's first `i32`.
    pub cluster_id: i32,
    /// `aeron.cluster.members`, verbatim.
    pub cluster_members: String,
    /// The ingress channel (`aeron.cluster.ingress.channel`); required by the reference.
    pub ingress_channel: String,
    /// Consensus channel (`aeron.cluster.consensus.channel`).
    pub consensus_channel: String,
    /// Log channel (`aeron.cluster.log.channel`).
    pub log_channel: String,
    /// Whether a manual-MDC log publication also binds the control endpoint
    /// (`aeron.cluster.log.control.enable`, default true).
    pub log_control_enabled: bool,
    /// Egress channel template, or `None`.
    pub egress_channel: Option<String>,
    /// The IPC control channel both agents share (`aeron.cluster.control.channel`).
    pub control_channel: String,
    /// The replication channel (`aeron.cluster.replication.channel`); required.
    pub replication_channel: String,
    /// Consensus stream id.
    pub consensus_stream_id: i32,
    /// Log stream id.
    pub log_stream_id: i32,
    /// Ingress stream id.
    pub ingress_stream_id: i32,
    /// How many fragments one ingress poll reads (`aeron.cluster.ingress.fragment.limit`).
    pub ingress_fragment_limit: usize,
    /// Whether clients may connect over IPC (`aeron.cluster.ingress.ipc.allowed`).
    pub ingress_ipc_allowed: bool,
    /// How many sessions may be pending or open at once (`aeron.cluster.max.sessions`).
    pub max_concurrent_sessions: usize,
    /// `aeron.cluster.authenticator.supplier`, or the reference's default name.
    pub authenticator_supplier: String,
    /// `aeron.cluster.authorisation.service.supplier`, or `None` for the default.
    pub authorisation_service_supplier: Option<String>,
    /// Egress stream id.
    pub egress_stream_id: i32,
    /// `aeron.cluster.snapshot.channel`.
    pub snapshot_channel: String,
    /// `aeron.cluster.snapshot.stream.id`.
    pub snapshot_stream_id: i32,
    /// `aeron.cluster.replay.channel`.
    pub replay_channel: String,
    /// `aeron.cluster.replay.stream.id`.
    pub replay_stream_id: i32,
    /// `aeron.cluster.log.fragment.limit`.
    pub log_fragment_limit: usize,
    /// Service stream id (CM -> container).
    pub service_stream_id: i32,
    /// Consensus module stream id (container -> CM).
    pub consensus_module_stream_id: i32,
    /// How many services the cluster runs.
    pub service_count: i32,
    /// The archive to connect to.
    pub archive: ArchiveContext,
    /// `aeron.cluster.session.timeout`.
    pub session_timeout_ns: i64,
    /// `aeron.cluster.leader.heartbeat.timeout`.
    pub leader_heartbeat_timeout_ns: i64,
    /// `aeron.cluster.leader.heartbeat.interval`.
    pub leader_heartbeat_interval_ns: i64,
    /// `aeron.cluster.startup.canvass.timeout`.
    pub startup_canvass_timeout_ns: i64,
    /// `aeron.cluster.termination.timeout`: how long a terminating leader
    /// waits for followers to acknowledge (`ConsensusModule.java:1254`).
    pub termination_timeout_ns: i64,
    /// `aeron.cluster.election.timeout`.
    pub election_timeout_ns: i64,
    /// `aeron.cluster.election.status.interval`.
    pub election_status_interval_ns: i64,
    /// `aeron.cluster.appointed.leader.id`, or `-1`.
    pub appointed_leader_id: i32,
    /// `aeron.cluster.file.sync.level`.
    pub file_sync_level: i32,
    /// `aeron.cluster.error.buffer.length`.
    pub error_buffer_length: i32,
    /// `aeron.cluster.cycle.threshold`.
    pub cycle_threshold_ns: i64,
    /// `aeron.cluster.total.snapshot.duration.threshold`.
    pub snapshot_duration_threshold_ns: i64,
    /// `aeron.cluster.mark.file.dir`; empty when unset.
    pub mark_file_dir_property: Option<String>,
    /// Delete the cluster directory before starting (programmatic, not a property).
    pub delete_dir_on_start: bool,
    /// The application version stamped into `NewLeadershipTermEvent.appVersion`
    /// (`ConsensusModule.java:1610`, default `SemanticVersion.compose(0, 0, 1)`).
    pub app_version: i32,
    /// `aeron.cluster.idle.strategy` (`ClusteredServiceContainer.java:575`),
    /// default `org.agrona.concurrent.BackoffIdleStrategy` (`:569`).
    pub idle_strategy: String,
    /// `aeron.cluster.timer.service.supplier` (`ConsensusModule.java:935`).
    pub timer_service_supplier: String,
    /// `aeron.cluster.wheel.tick.resolution`, in nanoseconds
    /// (`ConsensusModule.java:892-898`, default 8 ms).
    pub wheel_tick_resolution_ns: i64,
    /// `aeron.cluster.ticks.per.wheel` (`ConsensusModule.java:905-912`).
    pub ticks_per_wheel: i32,
}

impl ConsensusModuleConfig {
    /// Read the names `P3-4a` honours, with the reference's defaults.
    ///
    /// # Errors
    ///
    /// [`ConfigProblem`] for an unparsable value or a required name that is
    /// missing. Missing names the reference also requires are refused here; the
    /// reference refuses them in `conclude` with the same words.
    pub fn from_properties(properties: &Properties) -> Result<Self, ConfigProblem> {
        let required = |name: &'static str| -> Result<String, ConfigProblem> {
            properties
                .get(name)
                .map(str::to_owned)
                .ok_or(ConfigProblem::Missing(name))
        };

        let int = |name: &'static str, default: i32| -> Result<i32, ConfigProblem> {
            match properties.get(name) {
                None => Ok(default),
                Some(text) => text.parse().map_err(|_| ConfigProblem::Value {
                    name,
                    value: text.to_owned(),
                }),
            }
        };

        // `Boolean.parseBoolean`: anything but "true" is false, and the default
        // applies only when the name is absent (`ConsensusModule.java:439-445`).
        let boolean = |name: &'static str, default: bool| -> bool {
            match properties.get(name) {
                None => default,
                Some(text) => text.eq_ignore_ascii_case("true"),
            }
        };

        let cluster_dir = PathBuf::from(
            properties
                .get(CLUSTER_DIR_PROP_NAME)
                .unwrap_or(CLUSTER_DIR_DEFAULT),
        );
        let mark_file_dir = properties.get(MARK_FILE_DIR_PROP_NAME).map(PathBuf::from);
        let aeron_dir = match properties.get(AERON_DIR_PROP_NAME) {
            Some(value) => PathBuf::from(value),
            None => PathBuf::from(deepmsg_archive::client::context::default_aeron_dir()),
        };

        let service_count = int(SERVICE_COUNT_PROP_NAME, SERVICE_COUNT_DEFAULT)?;
        if !(0..=MAX_SERVICE_COUNT).contains(&service_count) {
            return Err(ConfigProblem::Invalid(format!(
                "service count of range [0, {MAX_SERVICE_COUNT}]: {service_count}"
            )));
        }

        let leader_heartbeat_timeout_ns = properties.duration_ns(
            LEADER_HEARTBEAT_TIMEOUT_PROP_NAME,
            LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS,
        )?;
        let leader_heartbeat_interval_ns = properties.duration_ns(
            LEADER_HEARTBEAT_INTERVAL_PROP_NAME,
            LEADER_HEARTBEAT_INTERVAL_DEFAULT_NS,
        )?;
        let startup_canvass_timeout_ns = properties.duration_ns(
            STARTUP_CANVASS_TIMEOUT_PROP_NAME,
            STARTUP_CANVASS_TIMEOUT_DEFAULT_NS,
        )?;

        // `Conclude` (`:1786-1790`): the reference's own check.
        if leader_heartbeat_timeout_ns == 0
            || startup_canvass_timeout_ns / leader_heartbeat_timeout_ns < 2
        {
            return Err(ConfigProblem::Invalid(format!(
                "startupCanvassTimeoutNs={startup_canvass_timeout_ns} must be a multiple of \
                 leaderHeartbeatTimeoutNs={leader_heartbeat_timeout_ns}"
            )));
        }

        let cluster_id = int(CLUSTER_ID_PROP_NAME, CLUSTER_ID_DEFAULT)?;
        let mut archive = ArchiveContext::new();
        archive.aeron_dir = aeron_dir.to_string_lossy().into_owned();
        archive.control_request_channel = Some("aeron:ipc".to_owned());
        archive.control_response_channel = Some("aeron:ipc".to_owned());
        // `Context.conclude` (`ConsensusModule.java:2077-2084`): the response
        // stream is per-cluster so two clusters can share one archive.
        archive.control_response_stream_id += cluster_id * 100 + 100;

        // `aeron.cluster.clock` names a class the reference instantiates. Only
        // the default is implemented, and a deployment that asked for another
        // clock must hear about it (`common::config::names` has the row).
        if let Some(name) = properties.get(CLUSTER_CLOCK_PROP_NAME) {
            return Err(ConfigProblem::Invalid(format!(
                "{CLUSTER_CLOCK_PROP_NAME}={name} names a clock this build does not implement; \
                 only the millisecond clock exists"
            )));
        }

        let idle_strategy = properties
            .get(CLUSTER_IDLE_STRATEGY_PROP_NAME)
            .unwrap_or(CLUSTER_IDLE_STRATEGY_DEFAULT);
        if deepmsg_driver::idle::Strategy::by_agrona_name(idle_strategy).is_none() {
            return Err(ConfigProblem::Invalid(format!(
                "{CLUSTER_IDLE_STRATEGY_PROP_NAME}={idle_strategy} names no idle strategy this \
                 build has"
            )));
        }

        Ok(Self {
            cluster_dir,
            mark_file_dir: mark_file_dir.unwrap_or_else(|| PathBuf::from(CLUSTER_DIR_DEFAULT)),
            aeron_dir,
            member_id: int(CLUSTER_MEMBER_ID_PROP_NAME, CLUSTER_MEMBER_ID_DEFAULT)?,
            cluster_id,
            cluster_members: required(CLUSTER_MEMBERS_PROP_NAME)?,
            ingress_channel: required(INGRESS_CHANNEL_PROP_NAME)?,
            consensus_channel: properties
                .get(CONSENSUS_CHANNEL_PROP_NAME)
                .unwrap_or(CONSENSUS_CHANNEL_DEFAULT)
                .to_owned(),
            log_channel: properties
                .get(LOG_CHANNEL_PROP_NAME)
                .unwrap_or(LOG_CHANNEL_DEFAULT)
                .to_owned(),
            log_control_enabled: boolean(LOG_CONTROL_ENABLE_PROP_NAME, LOG_CONTROL_ENABLE_DEFAULT),
            egress_channel: properties.get(EGRESS_CHANNEL_PROP_NAME).map(str::to_owned),
            control_channel: properties
                .get(CONTROL_CHANNEL_PROP_NAME)
                .unwrap_or(CONTROL_CHANNEL_DEFAULT)
                .to_owned(),
            replication_channel: required(REPLICATION_CHANNEL_PROP_NAME)?,
            consensus_stream_id: int(CONSENSUS_STREAM_ID_PROP_NAME, CONSENSUS_STREAM_ID_DEFAULT)?,
            log_stream_id: int(LOG_STREAM_ID_PROP_NAME, LOG_STREAM_ID_DEFAULT)?,
            ingress_stream_id: int(INGRESS_STREAM_ID_PROP_NAME, INGRESS_STREAM_ID_DEFAULT)?,
            ingress_fragment_limit: usize::try_from(int(
                INGRESS_FRAGMENT_LIMIT_PROP_NAME,
                INGRESS_FRAGMENT_LIMIT_DEFAULT,
            )?)
            .map_err(|_| ConfigProblem::Value {
                name: INGRESS_FRAGMENT_LIMIT_PROP_NAME,
                value: properties
                    .get(INGRESS_FRAGMENT_LIMIT_PROP_NAME)
                    .unwrap_or_default()
                    .to_owned(),
            })?,
            ingress_ipc_allowed: boolean(
                INGRESS_IPC_ALLOWED_PROP_NAME,
                INGRESS_IPC_ALLOWED_DEFAULT,
            ),
            max_concurrent_sessions: usize::try_from(int(
                MAX_SESSIONS_PROP_NAME,
                MAX_CONCURRENT_SESSIONS_DEFAULT,
            )?)
            .map_err(|_| ConfigProblem::Value {
                name: MAX_SESSIONS_PROP_NAME,
                value: properties
                    .get(MAX_SESSIONS_PROP_NAME)
                    .unwrap_or_default()
                    .to_owned(),
            })?,
            authenticator_supplier: properties
                .get(AUTHENTICATOR_SUPPLIER_PROP_NAME)
                .filter(|name| !name.is_empty())
                .unwrap_or(DEFAULT_AUTHENTICATOR_SUPPLIER)
                .to_owned(),
            authorisation_service_supplier: properties
                .get(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME)
                .filter(|name| !name.is_empty())
                .map(str::to_owned),
            egress_stream_id: int(EGRESS_STREAM_ID_PROP_NAME, EGRESS_STREAM_ID_DEFAULT)?,
            snapshot_channel: properties
                .get(SNAPSHOT_CHANNEL_PROP_NAME)
                .unwrap_or(SNAPSHOT_CHANNEL_DEFAULT)
                .to_owned(),
            snapshot_stream_id: int(SNAPSHOT_STREAM_ID_PROP_NAME, SNAPSHOT_STREAM_ID_DEFAULT)?,
            replay_channel: properties
                .get(REPLAY_CHANNEL_PROP_NAME)
                .unwrap_or(REPLAY_CHANNEL_DEFAULT)
                .to_owned(),
            replay_stream_id: int(REPLAY_STREAM_ID_PROP_NAME, REPLAY_STREAM_ID_DEFAULT)?,
            log_fragment_limit: usize::try_from(int(
                LOG_FRAGMENT_LIMIT_PROP_NAME,
                LOG_FRAGMENT_LIMIT_DEFAULT,
            )?)
            .map_err(|_| ConfigProblem::Value {
                name: LOG_FRAGMENT_LIMIT_PROP_NAME,
                value: properties
                    .get(LOG_FRAGMENT_LIMIT_PROP_NAME)
                    .unwrap_or_default()
                    .to_owned(),
            })?,
            service_stream_id: int(SERVICE_STREAM_ID_PROP_NAME, SERVICE_STREAM_ID_DEFAULT)?,
            consensus_module_stream_id: int(
                CONSENSUS_MODULE_STREAM_ID_PROP_NAME,
                CONSENSUS_MODULE_STREAM_ID_DEFAULT,
            )?,
            service_count,
            archive,
            session_timeout_ns: properties
                .duration_ns(SESSION_TIMEOUT_PROP_NAME, SESSION_TIMEOUT_DEFAULT_NS)?,
            leader_heartbeat_timeout_ns,
            leader_heartbeat_interval_ns,
            startup_canvass_timeout_ns,
            termination_timeout_ns: properties.duration_ns(
                TERMINATION_TIMEOUT_PROP_NAME,
                TERMINATION_TIMEOUT_DEFAULT_NS,
            )?,
            election_timeout_ns: properties
                .duration_ns(ELECTION_TIMEOUT_PROP_NAME, ELECTION_TIMEOUT_DEFAULT_NS)?,
            election_status_interval_ns: properties.duration_ns(
                ELECTION_STATUS_INTERVAL_PROP_NAME,
                ELECTION_STATUS_INTERVAL_DEFAULT_NS,
            )?,
            appointed_leader_id: int(APPOINTED_LEADER_ID_PROP_NAME, -1)?,
            file_sync_level: int(FILE_SYNC_LEVEL_PROP_NAME, FILE_SYNC_LEVEL_DEFAULT)?,
            error_buffer_length: properties.size(
                ERROR_BUFFER_LENGTH_PROP_NAME,
                deepmsg_cluster_common::mark::ERROR_BUFFER_MIN_LENGTH as i32,
            )?,
            cycle_threshold_ns: properties
                .duration_ns(CYCLE_THRESHOLD_PROP_NAME, CYCLE_THRESHOLD_DEFAULT_NS)?,
            snapshot_duration_threshold_ns: properties.duration_ns(
                TOTAL_SNAPSHOT_DURATION_THRESHOLD_PROP_NAME,
                TOTAL_SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS,
            )?,
            mark_file_dir_property: properties.get(MARK_FILE_DIR_PROP_NAME).map(str::to_owned),
            delete_dir_on_start: false,
            app_version: APP_VERSION_DEFAULT,
            idle_strategy: idle_strategy.to_owned(),
            timer_service_supplier: {
                let name = properties
                    .get(crate::timer::TIMER_SERVICE_SUPPLIER_PROP_NAME)
                    .filter(|name| !name.is_empty())
                    .unwrap_or(crate::timer::TIMER_SERVICE_SUPPLIER_DEFAULT);
                // The reference's own whitelist: two class names, everything
                // else throws (`ConsensusModule.java:4589-4607`).
                crate::timer::timer_service_kind(name)
                    .map_err(|error| ConfigProblem::Invalid(error.to_string()))?;
                name.to_owned()
            },
            wheel_tick_resolution_ns: properties.duration_ns(
                crate::timer::WHEEL_TICK_RESOLUTION_PROP_NAME,
                crate::timer::WHEEL_TICK_RESOLUTION_DEFAULT_NS,
            )?,
            ticks_per_wheel: int(
                crate::timer::TICKS_PER_WHEEL_PROP_NAME,
                crate::timer::TICKS_PER_WHEEL_DEFAULT,
            )?,
        })
    }

    /// Settle the directories: make the relative absolute, delete when asked,
    /// and create both directory trees.
    ///
    /// The mark file's directory defaults to the cluster directory **unless
    /// `aeron.cluster.mark.file.dir` named one** — the reference takes the
    /// container's property when the CM's own `markFileDir` is unset
    /// (`ConsensusModule.java:1740-1744`).
    ///
    /// # Errors
    ///
    /// [`ConfigProblem::Directory`] when a path cannot be canonicalised or a
    /// directory cannot be created.
    pub fn conclude(&mut self) -> Result<(), ConfigProblem> {
        // Rust's `canonicalize` requires the path to exist, where Java's
        // `getCanonicalFile` does not; creating first and deleting after is the
        // same result with the same rule (`IoUtil.delete` then
        // `ensureDirectoryExists`).
        std::fs::create_dir_all(&self.cluster_dir).map_err(|error| {
            ConfigProblem::Directory(format!("could not create cluster directory: {error}"))
        })?;
        self.cluster_dir = canonical(&self.cluster_dir)?;

        if self.delete_dir_on_start {
            match std::fs::remove_dir_all(&self.cluster_dir) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(ConfigProblem::Directory(format!(
                        "could not delete {}: {error}",
                        self.cluster_dir.display()
                    )));
                }
            }
        }

        if let Some(name) = &self.mark_file_dir_property {
            std::fs::create_dir_all(name).map_err(|error| {
                ConfigProblem::Directory(format!("could not create mark file directory: {error}"))
            })?;
            self.mark_file_dir = canonical(Path::new(name))?;
        } else {
            self.mark_file_dir = self.cluster_dir.clone();
        }

        for (dir, what) in [
            (&self.cluster_dir, "cluster"),
            (&self.mark_file_dir, "mark file"),
        ] {
            std::fs::create_dir_all(dir).map_err(|error| {
                ConfigProblem::Directory(format!("could not create {what} directory: {error}"))
            })?;
        }

        Ok(())
    }
}

fn canonical(path: &Path) -> Result<PathBuf, ConfigProblem> {
    std::fs::canonicalize(path).map_err(|error| {
        ConfigProblem::Directory(format!(
            "could not canonicalise {}: {error}",
            path.display()
        ))
    })
}

impl From<ConfigProblem> for ClusterError {
    fn from(problem: ConfigProblem) -> Self {
        Self::Cluster(problem.to_string())
    }
}
