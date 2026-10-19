//! The container's configuration: the names `ClusteredServiceContainer` reads
//! and the defaults it attaches to them.
//!
//! The reference's `Context.conclude` (`ClusteredServiceContainer.java:824-1060`)
//! settles a much larger surface — an `Aeron` client, an archive context, two
//! trackers and five counters. P3-2a needs the names that shape the S2 seam and
//! the mark file; the counters and the archive belong to later slices and are
//! named as such below.
//!
//! Property names are the reference's (`ClusteredServiceContainer.java` unless
//! another file is named), because a deployment migrates by keeping its
//! properties file.

use std::path::PathBuf;

use deepmsg_archive::client::ArchiveContext;
use deepmsg_cluster_common::config::{ConfigError, Properties};

/// `aeron.cluster.id` (`ConsensusModule.java`).
pub const CLUSTER_ID_PROP_NAME: &str = "aeron.cluster.id";
/// `CLUSTER_ID_DEFAULT` (`ClusteredServiceContainer.java:217`).
pub const CLUSTER_ID_DEFAULT: i32 = 0;
/// `aeron.cluster.service.id` (`ClusteredServiceContainer.java:465`).
pub const SERVICE_ID_PROP_NAME: &str = "aeron.cluster.service.id";
/// `SERVICE_ID_DEFAULT` (`ClusteredServiceContainer.java:229`).
pub const SERVICE_ID_DEFAULT: i32 = 0;
/// `MAX_SERVICE_COUNT` (`ClusteredServiceContainer.java:234`).
pub const MAX_SERVICE_COUNT: i32 = 10;
/// `aeron.cluster.service.name` (`:240`).
pub const SERVICE_NAME_PROP_NAME: &str = "aeron.cluster.service.name";
/// `SERVICE_NAME_DEFAULT` (`:246`).
pub const SERVICE_NAME_DEFAULT: &str = "clustered-service";
/// `aeron.cluster.service.class.name` (`:252`) — names a Java class, refused.
pub const SERVICE_CLASS_NAME_PROP_NAME: &str = "aeron.cluster.service.class.name";
/// `aeron.cluster.control.channel` (`:282`).
pub const CONTROL_CHANNEL_PROP_NAME: &str = "aeron.cluster.control.channel";
/// `CONTROL_CHANNEL_DEFAULT` (`:289`).
pub const CONTROL_CHANNEL_DEFAULT: &str = "aeron:ipc?term-length=128k";
/// `aeron.cluster.service.stream.id` (`:294`).
pub const SERVICE_STREAM_ID_PROP_NAME: &str = "aeron.cluster.service.stream.id";
/// `SERVICE_STREAM_ID_DEFAULT` (`:301`) — the container reads 104.
pub const SERVICE_STREAM_ID_DEFAULT: i32 = 104;
/// `aeron.cluster.consensus.module.stream.id` (`:306`).
pub const CONSENSUS_MODULE_STREAM_ID_PROP_NAME: &str = "aeron.cluster.consensus.module.stream.id";
/// `CONSENSUS_MODULE_STREAM_ID_DEFAULT` (`:313`) — the container writes 105.
pub const CONSENSUS_MODULE_STREAM_ID_DEFAULT: i32 = 105;
/// `aeron.cluster.replay.channel` (`:264`).
pub const REPLAY_CHANNEL_PROP_NAME: &str = "aeron.cluster.replay.channel";
/// `REPLAY_CHANNEL_DEFAULT` (`:270`).
pub const REPLAY_CHANNEL_DEFAULT: &str = "aeron:ipc";
/// `aeron.cluster.replay.stream.id` (`:275`).
pub const REPLAY_STREAM_ID_PROP_NAME: &str = "aeron.cluster.replay.stream.id";
/// `REPLAY_STREAM_ID_DEFAULT` (`:282`).
pub const REPLAY_STREAM_ID_DEFAULT: i32 = 103;
/// `aeron.cluster.snapshot.channel` (`:318`).
pub const SNAPSHOT_CHANNEL_PROP_NAME: &str = "aeron.cluster.snapshot.channel";
/// `SNAPSHOT_CHANNEL_DEFAULT` (`:324`).
pub const SNAPSHOT_CHANNEL_DEFAULT: &str = "aeron:ipc?alias=snapshot";
/// `aeron.cluster.snapshot.stream.id` (`:330`).
pub const SNAPSHOT_STREAM_ID_PROP_NAME: &str = "aeron.cluster.snapshot.stream.id";
/// `SNAPSHOT_STREAM_ID_DEFAULT` (`:337`).
pub const SNAPSHOT_STREAM_ID_DEFAULT: i32 = 106;
/// `aeron.cluster.dir` (`:342`).
pub const CLUSTER_DIR_PROP_NAME: &str = "aeron.cluster.dir";
/// `CLUSTER_DIR_DEFAULT` (`:348`).
pub const CLUSTER_DIR_DEFAULT: &str = "aeron-cluster";
/// `aeron.cluster.mark.file.dir` (`:355`).
pub const MARK_FILE_DIR_PROP_NAME: &str = "aeron.cluster.mark.file.dir";
/// `aeron.cluster.service.error.buffer.length` (`:367`).
pub const ERROR_BUFFER_LENGTH_PROP_NAME: &str = "aeron.cluster.service.error.buffer.length";
/// `ERROR_BUFFER_LENGTH_DEFAULT` (`:373`) — 1 MiB.
pub const ERROR_BUFFER_LENGTH_DEFAULT: i32 = 1024 * 1024;
/// `aeron.cluster.service.responder` (`:379`).
pub const RESPONDER_SERVICE_PROP_NAME: &str = "aeron.cluster.service.responder";
/// `RESPONDER_SERVICE_DEFAULT` (`:385`).
pub const RESPONDER_SERVICE_DEFAULT: bool = true;
/// `aeron.cluster.log.fragment.limit` (`:392`).
pub const LOG_FRAGMENT_LIMIT_PROP_NAME: &str = "aeron.cluster.log.fragment.limit";
/// `LOG_FRAGMENT_LIMIT_DEFAULT` (`:398`).
pub const LOG_FRAGMENT_LIMIT_DEFAULT: i32 = 50;
/// `aeron.cluster.idle.strategy` (`:575`).
pub const IDLE_STRATEGY_PROP_NAME: &str = "aeron.cluster.idle.strategy";
/// `DEFAULT_IDLE_STRATEGY` (`:569`).
pub const IDLE_STRATEGY_DEFAULT: &str = "org.agrona.concurrent.BackoffIdleStrategy";
/// `aeron.dir`, the driver's directory (`CommonContext`).
pub const AERON_DIR_PROP_NAME: &str = "aeron.dir";
/// `AeronArchive.Configuration.LOCAL_CONTROL_CHANNEL_PROP_NAME`
/// (`AeronArchive.java:2716`); the container's archive is local, so this is
/// the channel it sends on.
pub const ARCHIVE_LOCAL_CONTROL_CHANNEL_PROP_NAME: &str = "aeron.archive.local.control.channel";
/// `LOCAL_CONTROL_CHANNEL_DEFAULT` (`AeronArchive.java:2722`).
pub const ARCHIVE_LOCAL_CONTROL_CHANNEL_DEFAULT: &str = "aeron:ipc?term-length=64k";
/// `LOCAL_CONTROL_STREAM_ID_PROP_NAME` (`AeronArchive.java:2728`).
pub const ARCHIVE_LOCAL_CONTROL_STREAM_ID_PROP_NAME: &str = "aeron.archive.local.control.stream.id";
/// `LOCAL_CONTROL_STREAM_ID_DEFAULT` (`AeronArchive.java:2735`).
pub const ARCHIVE_LOCAL_CONTROL_STREAM_ID_DEFAULT: i32 = 10;
/// `CONTROL_RESPONSE_STREAM_ID_PROP_NAME` (`AeronArchive.java:2758`).
pub const ARCHIVE_CONTROL_RESPONSE_STREAM_ID_PROP_NAME: &str =
    "aeron.archive.control.response.stream.id";
/// `CONTROL_RESPONSE_STREAM_ID_DEFAULT` (`AeronArchive.java:2764`).
pub const ARCHIVE_CONTROL_RESPONSE_STREAM_ID_DEFAULT: i32 = 20;
/// `SNAPSHOT_DURATION_THRESHOLD_PROP_NAME` (`ClusteredServiceContainer.java:430`).
pub const SNAPSHOT_DURATION_THRESHOLD_PROP_NAME: &str = "aeron.cluster.service.snapshot.threshold";
/// `SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS` (`:438`) — 1 s.
pub const SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS: i64 = 1_000_000_000;
/// The application version stamped into `NewLeadershipTermEvent.appVersion`.
pub const APP_VERSION_DEFAULT: i32 = 1;

/// Why the container's configuration could not be settled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigProblem {
    /// A name did not parse.
    Value {
        /// The property.
        name: &'static str,
        /// Its value.
        value: String,
    },
    /// A value parsed and is semantically wrong.
    Invalid(String),
    /// A property the reference refuses by name.
    Refused(String),
    /// A `Properties` read failed.
    Properties(ConfigError),
}

impl core::fmt::Display for ConfigProblem {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Value { name, value } => write!(f, "{name}={value} is not a value"),
            Self::Invalid(message) | Self::Refused(message) => f.write_str(message),
            Self::Properties(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ConfigProblem {}

impl From<ConfigError> for ConfigProblem {
    fn from(error: ConfigError) -> Self {
        Self::Properties(error)
    }
}

/// What P3-2a's container needs to start.
#[derive(Clone, Debug)]
pub struct ServiceContainerConfig {
    /// `aeron.cluster.id`.
    pub cluster_id: i32,
    /// `aeron.cluster.service.id`, `0..MAX_SERVICE_COUNT`.
    pub service_id: i32,
    /// `aeron.cluster.service.name`; the agent's role name.
    pub service_name: String,
    /// `aeron.dir`.
    pub aeron_dir: PathBuf,
    /// `aeron.cluster.dir`.
    pub cluster_dir: PathBuf,
    /// `aeron.cluster.mark.file.dir`, when it differs from the cluster dir.
    pub mark_file_dir: Option<PathBuf>,
    /// `aeron.cluster.control.channel`: both streams of S2 ride it.
    pub control_channel: String,
    /// `aeron.cluster.service.stream.id` (104): what the container reads.
    pub service_stream_id: i32,
    /// `aeron.cluster.consensus.module.stream.id` (105): what it writes.
    pub consensus_module_stream_id: i32,
    /// `aeron.cluster.log.fragment.limit`.
    pub log_fragment_limit: usize,
    /// `aeron.cluster.service.responder`: the one service that sends egress.
    pub responding_service: bool,
    /// `aeron.cluster.service.error.buffer.length`.
    pub error_buffer_length: i32,
    /// `aeron.cluster.idle.strategy`, by Agrona name.
    pub idle_strategy: String,
    /// The application version this container is (`NewLeadershipTermEvent`).
    pub app_version: i32,
    /// `aeron.cluster.replay.channel`.
    pub replay_channel: String,
    /// `aeron.cluster.replay.stream.id`.
    pub replay_stream_id: i32,
    /// `aeron.cluster.snapshot.channel`.
    pub snapshot_channel: String,
    /// `aeron.cluster.snapshot.stream.id`.
    pub snapshot_stream_id: i32,
    /// `aeron.archive.local.control.channel`: the archive this container uses
    /// is local to the same driver (`AeronArchive.java:2722`).
    pub archive_local_control_channel: String,
    /// `aeron.archive.local.control.stream.id`.
    pub archive_local_control_stream_id: i32,
    /// `aeron.archive.control.response.stream.id`, as the container computes it:
    /// `clusterId * 100 + 100 + controlResponseStreamId + (serviceId + 1)`
    /// (`ClusteredServiceContainer.java:1015-1020`).
    pub archive_control_response_stream_id: i32,
    /// `aeron.cluster.service.snapshot.threshold`: how long a snapshot may take
    /// before it is counted in counter 237.
    pub snapshot_duration_threshold_ns: i64,
}

impl ServiceContainerConfig {
    /// Read the names P3-2a honours, with the reference's defaults.
    ///
    /// # Errors
    ///
    /// [`ConfigProblem`] for an unparsable value, a service id outside
    /// `0..10`, or a Java service class name — which this build refuses by name
    /// instead of loading (spec §6).
    pub fn from_properties(properties: &Properties) -> Result<Self, ConfigProblem> {
        let int = |name: &'static str, default: i32| -> Result<i32, ConfigProblem> {
            match properties.get(name) {
                None => Ok(default),
                Some(text) => text.parse().map_err(|_| ConfigProblem::Value {
                    name,
                    value: text.to_owned(),
                }),
            }
        };
        let boolean = |name: &'static str, default: bool| -> bool {
            match properties.get(name) {
                None => default,
                Some(text) => text.eq_ignore_ascii_case("true"),
            }
        };

        if let Some(class) = properties.get(SERVICE_CLASS_NAME_PROP_NAME) {
            return Err(ConfigProblem::Refused(format!(
                "{SERVICE_CLASS_NAME_PROP_NAME}={class} names a Java class; this build selects its \
                 service from a compiled-in registry"
            )));
        }

        let service_id = int(SERVICE_ID_PROP_NAME, SERVICE_ID_DEFAULT)?;
        if !(0..MAX_SERVICE_COUNT).contains(&service_id) {
            return Err(ConfigProblem::Invalid(format!(
                "service id outside allowed range [0,{}]: {service_id}",
                MAX_SERVICE_COUNT - 1
            )));
        }

        let cluster_id = int(CLUSTER_ID_PROP_NAME, CLUSTER_ID_DEFAULT)?;
        let archive_control_response_stream_id = int(
            ARCHIVE_CONTROL_RESPONSE_STREAM_ID_PROP_NAME,
            ARCHIVE_CONTROL_RESPONSE_STREAM_ID_DEFAULT,
        )?;
        let archive_control_response_stream_id =
            cluster_id * 100 + 100 + archive_control_response_stream_id + (service_id + 1);
        let log_fragment_limit = int(LOG_FRAGMENT_LIMIT_PROP_NAME, LOG_FRAGMENT_LIMIT_DEFAULT)?;
        let log_fragment_limit =
            usize::try_from(log_fragment_limit).map_err(|_| ConfigProblem::Value {
                name: LOG_FRAGMENT_LIMIT_PROP_NAME,
                value: properties
                    .get(LOG_FRAGMENT_LIMIT_PROP_NAME)
                    .unwrap_or_default()
                    .to_owned(),
            })?;

        let aeron_dir = properties
            .get(AERON_DIR_PROP_NAME)
            .map_or_else(|| PathBuf::from(default_aeron_dir()), PathBuf::from);
        let cluster_dir = PathBuf::from(
            properties
                .get(CLUSTER_DIR_PROP_NAME)
                .unwrap_or(CLUSTER_DIR_DEFAULT),
        );

        Ok(Self {
            cluster_id,
            service_id,
            service_name: properties
                .get(SERVICE_NAME_PROP_NAME)
                .unwrap_or(SERVICE_NAME_DEFAULT)
                .to_owned(),
            aeron_dir,
            cluster_dir,
            mark_file_dir: properties.get(MARK_FILE_DIR_PROP_NAME).map(PathBuf::from),
            control_channel: properties
                .get(CONTROL_CHANNEL_PROP_NAME)
                .unwrap_or(CONTROL_CHANNEL_DEFAULT)
                .to_owned(),
            service_stream_id: int(SERVICE_STREAM_ID_PROP_NAME, SERVICE_STREAM_ID_DEFAULT)?,
            consensus_module_stream_id: int(
                CONSENSUS_MODULE_STREAM_ID_PROP_NAME,
                CONSENSUS_MODULE_STREAM_ID_DEFAULT,
            )?,
            log_fragment_limit,
            responding_service: boolean(RESPONDER_SERVICE_PROP_NAME, RESPONDER_SERVICE_DEFAULT),
            error_buffer_length: int(ERROR_BUFFER_LENGTH_PROP_NAME, ERROR_BUFFER_LENGTH_DEFAULT)?,
            idle_strategy: properties
                .get(IDLE_STRATEGY_PROP_NAME)
                .unwrap_or(IDLE_STRATEGY_DEFAULT)
                .to_owned(),
            app_version: APP_VERSION_DEFAULT,
            replay_channel: properties
                .get(REPLAY_CHANNEL_PROP_NAME)
                .unwrap_or(REPLAY_CHANNEL_DEFAULT)
                .to_owned(),
            replay_stream_id: int(REPLAY_STREAM_ID_PROP_NAME, REPLAY_STREAM_ID_DEFAULT)?,
            snapshot_channel: properties
                .get(SNAPSHOT_CHANNEL_PROP_NAME)
                .unwrap_or(SNAPSHOT_CHANNEL_DEFAULT)
                .to_owned(),
            snapshot_stream_id: int(SNAPSHOT_STREAM_ID_PROP_NAME, SNAPSHOT_STREAM_ID_DEFAULT)?,
            archive_local_control_channel: properties
                .get(ARCHIVE_LOCAL_CONTROL_CHANNEL_PROP_NAME)
                .unwrap_or(ARCHIVE_LOCAL_CONTROL_CHANNEL_DEFAULT)
                .to_owned(),
            archive_local_control_stream_id: int(
                ARCHIVE_LOCAL_CONTROL_STREAM_ID_PROP_NAME,
                ARCHIVE_LOCAL_CONTROL_STREAM_ID_DEFAULT,
            )?,
            archive_control_response_stream_id,
            snapshot_duration_threshold_ns: properties.duration_ns(
                SNAPSHOT_DURATION_THRESHOLD_PROP_NAME,
                SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS,
            )?,
        })
    }

    /// The archive this container records and replays with
    /// (`ClusteredServiceContainer.java:1011-1045`): a **local** archive, on
    /// IPC, with a response stream of its own so two services of one cluster do
    /// not share a control session.
    #[must_use]
    pub fn archive_context(&self) -> ArchiveContext {
        let mut context = ArchiveContext::new();
        context.control_request_channel = Some(self.archive_local_control_channel.clone());
        context.control_response_channel = Some(self.archive_local_control_channel.clone());
        context.control_request_stream_id = self.archive_local_control_stream_id;
        context.control_response_stream_id = self.archive_control_response_stream_id;
        context
    }

    /// Where this container's own mark file lives
    /// (`ClusterMarkFile.markFilenameForService`, `ClusterMarkFile.java:573-581`).
    pub fn mark_file_path(&self) -> PathBuf {
        let directory = self.mark_file_dir.as_ref().unwrap_or(&self.cluster_dir);
        directory.join(format!("cluster-mark-service-{}.dat", self.service_id))
    }

    /// The link name in the cluster directory (`:584-592`).
    pub fn mark_file_link_name(&self) -> String {
        format!("cluster-mark-service-{}.dat", self.service_id)
    }
}

/// Where a client looks for a driver when nothing says
/// (`aeron_default_path`, `aeron_utility.c`).
///
/// A copy of `deepmsg_archive::client::context::default_aeron_dir`, because
/// this crate must not depend on the archive: the container talks to S2, the
/// driver and the counters, and to nothing else.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_are_the_reference_defaults() {
        let config =
            ServiceContainerConfig::from_properties(&Properties::new()).expect("defaults parse");

        assert_eq!(config.cluster_id, 0);
        assert_eq!(config.service_id, 0);
        assert_eq!(config.service_name, "clustered-service");
        assert_eq!(config.control_channel, "aeron:ipc?term-length=128k");
        assert_eq!(config.service_stream_id, 104);
        assert_eq!(config.consensus_module_stream_id, 105);
        assert_eq!(config.log_fragment_limit, 50);
        assert!(config.responding_service);
        assert_eq!(config.error_buffer_length, 1024 * 1024);
        assert_eq!(
            config.idle_strategy,
            "org.agrona.concurrent.BackoffIdleStrategy"
        );
        assert_eq!(config.replay_stream_id, 103);
        assert_eq!(config.snapshot_stream_id, 106);
        assert_eq!(
            config.archive_local_control_channel,
            "aeron:ipc?term-length=64k"
        );
        assert_eq!(config.archive_local_control_stream_id, 10);
        // clusterId 0, serviceId 0, control response stream 20 ⇒ 121.
        assert_eq!(config.archive_control_response_stream_id, 121);
        assert_eq!(config.snapshot_duration_threshold_ns, 1_000_000_000);
    }

    #[test]
    fn a_service_id_outside_the_range_is_refused() {
        let mut properties = Properties::new();
        properties
            .apply_args(["-Daeron.cluster.service.id=10".to_owned()])
            .expect("a -D argument");

        assert!(ServiceContainerConfig::from_properties(&properties).is_err());
    }

    #[test]
    fn a_java_service_class_is_refused_by_name() {
        let mut properties = Properties::new();
        properties
            .apply_args(["-Daeron.cluster.service.class.name=com.example.Echo".to_owned()])
            .expect("a -D argument");

        assert!(matches!(
            ServiceContainerConfig::from_properties(&properties),
            Err(ConfigProblem::Refused(_))
        ));
    }

    #[test]
    fn the_mark_file_names_are_the_reference_names() {
        let config =
            ServiceContainerConfig::from_properties(&Properties::new()).expect("defaults parse");
        assert!(
            config
                .mark_file_path()
                .to_string_lossy()
                .ends_with("cluster-mark-service-0.dat")
        );
    }
}
