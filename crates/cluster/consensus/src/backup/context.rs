//! `ClusterBackup.Context` (`ClusterBackup.java:581-2091`) and its
//! `Configuration` (`:320-579`): the names a backup node reads, their defaults,
//! and `conclude()`.
//!
//! # What is a context here and what is a config
//!
//! The reference has one object with fifty setters, because a Java deployment
//! configures a component by calling them. This build's components separate the
//! two halves — [`ClusterBackupConfig`] is what a properties file says,
//! [`ClusterBackupContext`] is what `conclude()` resolved it to plus the things
//! it created (the mark file, the counters, the two archive contexts). The
//! split is the same one `clustered-service-container`'s `Config` and the
//! consensus module's `ConsensusModuleConfig` already make.
//!
//! # `conclude()`'s order is the contract
//!
//! `ClusterBackup.java:679-917` settles the defaults in an order that matters:
//! directories before the mark file, the mark file before the client, the
//! client before the counters, and `signalReady` last — because the ready
//! signal **is** "everything before it succeeded". The steps are numbered in
//! [`ClusterBackupContext::conclude`] the way the reference numbers them in
//! prose.

use std::fs;
use std::path::{Path, PathBuf};

use deepmsg_archive::server::config::ThreadNaming;
use deepmsg_cluster_common::config::Properties;
use deepmsg_cluster_common::mark::{self, ClusterMarkFile};
use deepmsg_cluster_common::member::{self, ClusterMember};
use deepmsg_codec_cluster_mark::cluster_component_type::ClusterComponentType;
use deepmsg_core::uri::ChannelUri;

use super::{ReplayStart, SourceType};
use crate::backup::agent::{
    AERON_CLUSTER_BACKUP_THREAD_NAME, AERON_CLUSTER_BACKUP_THREAD_NAME_CLASSIC,
};
use crate::config::ConfigProblem;
use crate::error::ClusterError;

/// `aeron.thread.naming` (`CommonContext.java:534`) — the same name the
/// consensus module reads (`config.rs:THREAD_NAMING_PROP_NAME`).
pub const THREAD_NAMING_PROP_NAME: &str = "aeron.thread.naming";

/// `CommonContext.ENDPOINT_PARAM_NAME`.
const ENDPOINT_PARAM_NAME: &str = "endpoint";

// ---------------------------------------------------------------------------
// Property names — `ClusterBackup.java` unless another file is named.
// ---------------------------------------------------------------------------

/// `aeron.cluster.backup.catchup.endpoint` (`:334`).
pub const CATCHUP_ENDPOINT_PROP_NAME: &str = "aeron.cluster.backup.catchup.endpoint";
/// `aeron.cluster.backup.catchup.channel` (`:340`).
pub const CATCHUP_CHANNEL_PROP_NAME: &str = "aeron.cluster.backup.catchup.channel";
/// `CLUSTER_BACKUP_CATCHUP_CHANNEL_DEFAULT` (`:346-347`).
pub const CATCHUP_CHANNEL_DEFAULT: &str =
    "aeron:udp?alias=backup|cc=cubic|so-sndbuf=512k|so-rcvbuf=512k|rcv-wnd=512k";
/// `aeron.cluster.backup.interval` (`:353`), default one hour (`:359`).
pub const INTERVAL_PROP_NAME: &str = "aeron.cluster.backup.interval";
/// `CLUSTER_BACKUP_INTERVAL_DEFAULT_NS` (`:359`).
pub const INTERVAL_DEFAULT_NS: i64 = 3_600_000_000_000;
/// `aeron.cluster.backup.response.timeout` (`:365`), default 5 s (`:371`).
pub const RESPONSE_TIMEOUT_PROP_NAME: &str = "aeron.cluster.backup.response.timeout";
/// `CLUSTER_BACKUP_RESPONSE_TIMEOUT_DEFAULT_NS` (`:371`).
pub const RESPONSE_TIMEOUT_DEFAULT_NS: i64 = 5_000_000_000;
/// `aeron.cluster.backup.progress.timeout` (`:377`), default 10 s (`:396`).
pub const PROGRESS_TIMEOUT_PROP_NAME: &str = "aeron.cluster.backup.progress.timeout";
/// `CLUSTER_BACKUP_PROGRESS_TIMEOUT_DEFAULT_NS` (`:396`).
pub const PROGRESS_TIMEOUT_DEFAULT_NS: i64 = 10_000_000_000;
/// `aeron.cluster.backup.cool.down.interval` (`:383-384`), default 30 s (`:390`).
pub const COOL_DOWN_INTERVAL_PROP_NAME: &str = "aeron.cluster.backup.cool.down.interval";
/// `CLUSTER_BACKUP_COOL_DOWN_INTERVAL_DEFAULT_NS` (`:390`).
pub const COOL_DOWN_INTERVAL_DEFAULT_NS: i64 = 30_000_000_000;
/// `aeron.cluster.backup.source.type` (`:402`), default `ANY` (`:408`).
pub const SOURCE_TYPE_PROP_NAME: &str = "aeron.cluster.backup.source.type";
/// `CLUSTER_BACKUP_SOURCE_TYPE_DEFAULT` (`:408`).
pub const SOURCE_TYPE_DEFAULT: &str = "ANY";
/// `cluster.backup.initial.replay.start` (`:561`) — **not** an `aeron.cluster.*`
/// name, so it is absent from the LC-6 table of eighty and has to be named
/// here.
pub const INITIAL_REPLAY_START_PROP_NAME: &str = "cluster.backup.initial.replay.start";
/// `CLUSTER_INITIAL_REPLAY_START_DEFAULT` (`:555`).
pub const INITIAL_REPLAY_START_DEFAULT: &str = "BEGINNING";

/// `Aeron`'s default directory name (`aeron.dir`, `CommonContext`).
pub const AERON_DIR_PROP_NAME: &str = "aeron.dir";
/// `aeron.cluster.id`.
pub const CLUSTER_ID_PROP_NAME: &str = "aeron.cluster.id";
/// `aeron.cluster.dir` — the cluster directory (`ClusteredServiceContainer.java:343`).
pub const CLUSTER_DIR_PROP_NAME: &str = "aeron.cluster.dir";
/// `CLUSTER_DIR_DEFAULT` (`ClusteredServiceContainer.java:349`).
pub const CLUSTER_DIR_DEFAULT: &str = "aeron-cluster";
/// `aeron.cluster.mark.file.dir` (`ClusteredServiceContainer.java:362`).
pub const MARK_FILE_DIR_PROP_NAME: &str = "aeron.cluster.mark.file.dir";
/// `aeron.cluster.error.buffer.length` (`ConsensusModule.java:828`).
pub const ERROR_BUFFER_LENGTH_PROP_NAME: &str = "aeron.cluster.error.buffer.length";
/// `aeron.cluster.consensus.channel` (`ConsensusModule.java:595`).
pub const CONSENSUS_CHANNEL_PROP_NAME: &str = "aeron.cluster.consensus.channel";
/// `CONSENSUS_CHANNEL_DEFAULT` (`ConsensusModule.java:602`).
pub const CONSENSUS_CHANNEL_DEFAULT: &str = "aeron:udp";
/// `aeron.cluster.consensus.stream.id` (`ConsensusModule.java:608`).
pub const CONSENSUS_STREAM_ID_PROP_NAME: &str = "aeron.cluster.consensus.stream.id";
/// `CONSENSUS_STREAM_ID_DEFAULT` (`ConsensusModule.java:614`).
pub const CONSENSUS_STREAM_ID_DEFAULT: i32 = 108;
/// `aeron.cluster.log.stream.id` (`ConsensusModule.java:557`).
pub const LOG_STREAM_ID_PROP_NAME: &str = "aeron.cluster.log.stream.id";
/// `LOG_STREAM_ID_DEFAULT` (`ConsensusModule.java:563`).
pub const LOG_STREAM_ID_DEFAULT: i32 = 100;
/// `aeron.cluster.snapshot.stream.id` — the CM snapshot replay stream
/// (`ConsensusModule.java:573`).
pub const CONSENSUS_MODULE_SNAPSHOT_STREAM_ID_PROP_NAME: &str = "aeron.cluster.snapshot.stream.id";
/// `SNAPSHOT_STREAM_ID_DEFAULT` for the CM (`ConsensusModule.java:573`).
pub const CONSENSUS_MODULE_SNAPSHOT_STREAM_ID_DEFAULT: i32 = 107;
/// The container's snapshot stream id (`ClusteredServiceContainer.java:331`).
pub const SERVICE_SNAPSHOT_STREAM_ID_PROP_NAME: &str = "aeron.cluster.snapshot.stream.id";
/// The container's default (`ClusteredServiceContainer.java:331`).
pub const SERVICE_SNAPSHOT_STREAM_ID_DEFAULT: i32 = 107;
/// `aeron.cluster.consensus.endpoints` (`ConsensusModule.java:514`).
pub const CONSENSUS_ENDPOINTS_PROP_NAME: &str = "aeron.cluster.consensus.endpoints";
/// `aeron.cluster.replication.progress.timeout` (`ConsensusModule.java:969`).
pub const REPLICATION_PROGRESS_TIMEOUT_PROP_NAME: &str =
    "aeron.cluster.replication.progress.timeout";
/// `REPLICATION_PROGRESS_TIMEOUT_DEFAULT_NS` (`ConsensusModule.java:976-977`).
pub const REPLICATION_PROGRESS_TIMEOUT_DEFAULT_NS: i64 = 10_000_000_000;
/// `aeron.cluster.replication.progress.interval` (`ConsensusModule.java:986`),
/// whose default is `Aeron.NULL_VALUE` — "derive it as timeout / 10".
pub const REPLICATION_PROGRESS_INTERVAL_PROP_NAME: &str =
    "aeron.cluster.replication.progress.interval";
/// `aeron.cluster.members` — only read to derive a catchup endpoint.
pub const MEMBERS_PROP_NAME: &str = "aeron.cluster.members";
/// `aeron.cluster.member.id`.
pub const MEMBER_ID_PROP_NAME: &str = "aeron.cluster.member.id";

/// The names a backup node reads, resolved from a [`Properties`].
///
/// One field per name the reference's `Configuration` has, with the reference's
/// defaults.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterBackupConfig {
    /// `aeron.cluster.id`.
    pub cluster_id: i32,
    /// `aeron.cluster.dir`.
    pub cluster_dir: PathBuf,
    /// `aeron.cluster.mark.file.dir`, or `None` for the cluster directory.
    pub mark_file_dir: Option<PathBuf>,
    /// `aeron.dir`.
    pub aeron_dir: PathBuf,
    /// `aeron.cluster.backup.catchup.endpoint`, before the member-string
    /// fallback. `None` when neither source gives one — `conclude` refuses that.
    pub catchup_endpoint: Option<String>,
    /// `aeron.cluster.backup.catchup.channel`.
    pub catchup_channel: String,
    /// `aeron.cluster.backup.interval`.
    pub backup_interval_ns: i64,
    /// `aeron.cluster.backup.response.timeout`.
    pub response_timeout_ns: i64,
    /// `aeron.cluster.backup.progress.timeout`.
    pub progress_timeout_ns: i64,
    /// `aeron.cluster.backup.cool.down.interval`.
    pub cool_down_interval_ns: i64,
    /// `aeron.cluster.backup.source.type`, still a string until `conclude`.
    pub source_type: String,
    /// `cluster.backup.initial.replay.start`.
    pub initial_replay_start: String,
    /// `aeron.cluster.error.buffer.length`.
    pub error_buffer_length: i32,
    /// `aeron.cluster.consensus.channel`.
    pub consensus_channel: String,
    /// `aeron.cluster.consensus.stream.id`.
    pub consensus_stream_id: i32,
    /// `aeron.cluster.snapshot.stream.id` as the CM reads it.
    pub consensus_module_snapshot_stream_id: i32,
    /// The same name as the container reads it.
    pub service_snapshot_stream_id: i32,
    /// `aeron.cluster.log.stream.id`.
    pub log_stream_id: i32,
    /// `aeron.cluster.consensus.endpoints`.
    pub cluster_consensus_endpoints: String,
    /// `aeron.cluster.replication.progress.timeout`, or `None` to use the
    /// default.
    pub replication_progress_timeout_ns: Option<i64>,
    /// `aeron.cluster.replication.progress.interval`, or `None` to derive it.
    pub replication_progress_interval_ns: Option<i64>,
    /// `aeron.cluster.members`, kept for the catchup-endpoint fallback.
    pub cluster_members: Option<String>,
    /// `aeron.cluster.member.id`.
    pub member_id: i32,
    /// `aeron.thread.naming`: which spelling of the role name applies
    /// (`CommonContext.threadName`).
    pub thread_naming: ThreadNaming,
    /// Delete the cluster directory before concluding (programmatic).
    pub delete_dir_on_start: bool,
}

impl ClusterBackupConfig {
    /// Read every name, with the reference's defaults.
    ///
    /// # Errors
    ///
    /// [`ConfigProblem`] for a duration, size or integer that does not parse.
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

        let aeron_dir = match properties.get(AERON_DIR_PROP_NAME) {
            Some(value) => PathBuf::from(value),
            None => PathBuf::from(deepmsg_archive::client::context::default_aeron_dir()),
        };

        Ok(Self {
            cluster_id: int(CLUSTER_ID_PROP_NAME, 0)?,
            cluster_dir: PathBuf::from(
                properties
                    .get(CLUSTER_DIR_PROP_NAME)
                    .unwrap_or(CLUSTER_DIR_DEFAULT),
            ),
            mark_file_dir: properties.get(MARK_FILE_DIR_PROP_NAME).map(PathBuf::from),
            aeron_dir,
            catchup_endpoint: properties
                .get(CATCHUP_ENDPOINT_PROP_NAME)
                .filter(|value| !value.is_empty())
                .map(str::to_owned),
            catchup_channel: properties
                .get(CATCHUP_CHANNEL_PROP_NAME)
                .unwrap_or(CATCHUP_CHANNEL_DEFAULT)
                .to_owned(),
            backup_interval_ns: properties.duration_ns(INTERVAL_PROP_NAME, INTERVAL_DEFAULT_NS)?,
            response_timeout_ns: properties
                .duration_ns(RESPONSE_TIMEOUT_PROP_NAME, RESPONSE_TIMEOUT_DEFAULT_NS)?,
            progress_timeout_ns: properties
                .duration_ns(PROGRESS_TIMEOUT_PROP_NAME, PROGRESS_TIMEOUT_DEFAULT_NS)?,
            cool_down_interval_ns: properties
                .duration_ns(COOL_DOWN_INTERVAL_PROP_NAME, COOL_DOWN_INTERVAL_DEFAULT_NS)?,
            source_type: properties
                .get(SOURCE_TYPE_PROP_NAME)
                .unwrap_or(SOURCE_TYPE_DEFAULT)
                .to_owned(),
            initial_replay_start: properties
                .get(INITIAL_REPLAY_START_PROP_NAME)
                .unwrap_or(INITIAL_REPLAY_START_DEFAULT)
                .to_owned(),
            error_buffer_length: properties.size(
                ERROR_BUFFER_LENGTH_PROP_NAME,
                deepmsg_cluster_common::mark::ERROR_BUFFER_MIN_LENGTH as i32,
            )?,
            consensus_channel: properties
                .get(CONSENSUS_CHANNEL_PROP_NAME)
                .unwrap_or(CONSENSUS_CHANNEL_DEFAULT)
                .to_owned(),
            consensus_stream_id: int(CONSENSUS_STREAM_ID_PROP_NAME, CONSENSUS_STREAM_ID_DEFAULT)?,
            consensus_module_snapshot_stream_id: CONSENSUS_MODULE_SNAPSHOT_STREAM_ID_DEFAULT,
            service_snapshot_stream_id: SERVICE_SNAPSHOT_STREAM_ID_DEFAULT,
            log_stream_id: int(LOG_STREAM_ID_PROP_NAME, LOG_STREAM_ID_DEFAULT)?,
            cluster_consensus_endpoints: properties
                .get(CONSENSUS_ENDPOINTS_PROP_NAME)
                .unwrap_or_default()
                .to_owned(),
            replication_progress_timeout_ns: properties
                .get(REPLICATION_PROGRESS_TIMEOUT_PROP_NAME)
                .map(|_| {
                    properties.duration_ns(
                        REPLICATION_PROGRESS_TIMEOUT_PROP_NAME,
                        REPLICATION_PROGRESS_TIMEOUT_DEFAULT_NS,
                    )
                })
                .transpose()?,
            replication_progress_interval_ns: properties
                .get(REPLICATION_PROGRESS_INTERVAL_PROP_NAME)
                .map(|_| properties.duration_ns(REPLICATION_PROGRESS_INTERVAL_PROP_NAME, -1))
                .transpose()?,
            cluster_members: properties.get(MEMBERS_PROP_NAME).map(str::to_owned),
            member_id: int(MEMBER_ID_PROP_NAME, -1)?,
            thread_naming: match properties.get(THREAD_NAMING_PROP_NAME) {
                None => ThreadNaming::DEFAULT,
                Some(value) => ThreadNaming::parse(value).ok_or_else(|| ConfigProblem::Value {
                    name: THREAD_NAMING_PROP_NAME,
                    value: value.to_owned(),
                })?,
            },
            delete_dir_on_start: false,
        })
    }

    /// `aeron.cluster.backup.source.type` as the enum (`:530-535`, validated in
    /// `conclude` at `:905-914`).
    ///
    /// # Errors
    ///
    /// The value is not one of the three names.
    pub fn source_type(&self) -> Result<SourceType, ClusterError> {
        SourceType::parse(&self.source_type)
    }

    /// `cluster.backup.initial.replay.start` as the enum (`:569-578`).
    ///
    /// # Errors
    ///
    /// The value is not one of the two names.
    pub fn initial_replay_start(&self) -> Result<ReplayStart, ClusterError> {
        ReplayStart::parse(&self.initial_replay_start)
    }

    /// `Configuration.catchupEndpoint()` (`:417-439`): the property, else the
    /// member string's own endpoint for this member id.
    ///
    /// The reference's third source, `aeron.cluster.member.endpoints`, is
    /// **not** consulted: this build recorded it as `AcceptedNotHonoured`
    /// (P3-8's LC-6), and the consensus module does not apply it either.
    #[must_use]
    pub fn resolved_catchup_endpoint(&self) -> Option<String> {
        if let Some(endpoint) = &self.catchup_endpoint {
            return Some(endpoint.clone());
        }

        let members = deepmsg_cluster_common::member::ClusterMember::parse_list(
            self.cluster_members.as_deref()?,
        )
        .ok()?;
        let member = deepmsg_cluster_common::member::find_member(&members, self.member_id)?;

        Some(member.catchup_endpoint.clone())
    }

    /// The name of the agent's thread (`ClusterBackup.java:225-226`, chosen by
    /// `CommonContext.threadName` from `aeron.thread.naming`).
    ///
    /// `classic` is the default and names the thread `cluster-backup`; `new`
    /// names it `aeron-cl-bu`. Neither carries the cluster id: the reference's
    /// backup thread does not, unlike the consensus module's.
    #[must_use]
    pub fn role_name(&self) -> String {
        match self.thread_naming {
            ThreadNaming::Classic => AERON_CLUSTER_BACKUP_THREAD_NAME_CLASSIC.to_owned(),
            ThreadNaming::New => AERON_CLUSTER_BACKUP_THREAD_NAME.to_owned(),
        }
    }

    /// The consensus channel the backup subscribes on, with its endpoint
    /// filled in from the member string when the property does not name one
    /// (`ClusterBackup.Configuration.consensusChannel()`, `:447-478`).
    ///
    /// The reference has a third source — `aeron.cluster.member.endpoints` —
    /// which this build does not apply (`P3-0e`), so a deployment that names
    /// neither the property nor the member string keeps the template as it is.
    #[must_use]
    pub fn resolved_consensus_channel(&self) -> String {
        let Ok(mut uri) = ChannelUri::parse(&self.consensus_channel) else {
            return self.consensus_channel.clone();
        };

        if uri.get(ENDPOINT_PARAM_NAME).is_some() {
            return self.consensus_channel.clone();
        }

        let Some(member) = self
            .cluster_members
            .as_deref()
            .and_then(|members| ClusterMember::parse_list(members).ok())
            .and_then(|members| {
                member::find_member(&members, self.member_id)
                    .map(|member| member.consensus_endpoint.clone())
            })
        else {
            return self.consensus_channel.clone();
        };

        uri.put(ENDPOINT_PARAM_NAME, member);
        uri.build()
    }

    /// The replication's progress timeout, defaulted (`:647`, default at
    /// `ConsensusModule.java:976-977`).
    #[must_use]
    pub fn replication_progress_timeout_ns(&self) -> i64 {
        self.replication_progress_timeout_ns
            .unwrap_or(REPLICATION_PROGRESS_TIMEOUT_DEFAULT_NS)
    }

    /// The interval between progress checks, derived when unset
    /// (`ClusterBackup.java:731-734`): `max(timeout / 10, 1)`.
    #[must_use]
    pub fn replication_progress_interval_ns(&self) -> i64 {
        self.replication_progress_interval_ns
            .unwrap_or_else(|| (self.replication_progress_timeout_ns() / 10).max(1))
    }
}

/// `ClusterBackup.Context` (`ClusterBackup.java:581-2091`): what `conclude()`
/// resolves, plus the two things it creates here — the directory layout and the
/// mark file.
///
/// The reference's `Context` also owns the Aeron client, the counters and the
/// two archive contexts. Those are the agent's ([`crate::backup::agent`])
/// because they are live objects with lifetimes; what a *context* has to settle
/// before an agent exists is the filesystem, and that is what this type is.
pub struct ClusterBackupContext {
    config: ClusterBackupConfig,
    /// The resolved cluster directory (`clusterDir`, `:1074-1098`).
    cluster_dir: PathBuf,
    /// The resolved mark-file directory (`markFileDir`, `:1100-1122`).
    mark_file_dir: PathBuf,
    /// The page size the mark file is aligned to.
    file_page_size: usize,
    /// Injected so a test does not have to move the wall clock.
    now_ms: i64,
    pid: i64,
    is_concluded: bool,
    mark_file: Option<ClusterMarkFile>,
}

impl ClusterBackupContext {
    /// A context over a resolved configuration.
    #[must_use]
    pub fn new(config: ClusterBackupConfig) -> Self {
        Self {
            cluster_dir: config.cluster_dir.clone(),
            mark_file_dir: config
                .mark_file_dir
                .clone()
                .unwrap_or_else(|| config.cluster_dir.clone()),
            file_page_size: 0,
            now_ms: deepmsg_core::clock::epoch_millis(),
            pid: i64::from(std::process::id()),
            is_concluded: false,
            mark_file: None,
            config,
        }
    }

    /// Replace the clock reading `conclude()` stamps into the mark file.
    #[must_use]
    pub fn with_now_ms(mut self, now_ms: i64) -> Self {
        self.now_ms = now_ms;
        self
    }

    /// Use `page_size` for the mark file instead of reading the driver's CnC.
    ///
    /// The reference does the same thing implicitly when a caller supplies an
    /// `Aeron` client (`ClusterBackup.java:735-736`): both paths end at the
    /// client's `filePageSize()`, and here they end at this number.
    #[must_use]
    pub fn with_file_page_size(mut self, page_size: usize) -> Self {
        self.file_page_size = page_size;
        self
    }

    /// The process id to stamp, for a test that wants a fixed one.
    #[must_use]
    pub fn with_pid(mut self, pid: i64) -> Self {
        self.pid = pid;
        self
    }

    /// The configuration.
    #[must_use]
    pub fn config(&self) -> &ClusterBackupConfig {
        &self.config
    }

    /// The cluster directory, as resolved.
    #[must_use]
    pub fn cluster_dir(&self) -> &Path {
        &self.cluster_dir
    }

    /// The mark-file directory, as resolved.
    #[must_use]
    pub fn mark_file_dir(&self) -> &Path {
        &self.mark_file_dir
    }

    /// The page size the mark file is aligned to, once concluded.
    #[must_use]
    pub fn file_page_size(&self) -> usize {
        self.file_page_size
    }

    /// The mark file, once concluded.
    #[must_use]
    pub fn mark_file(&self) -> Option<&ClusterMarkFile> {
        self.mark_file.as_ref()
    }

    /// Whether [`Self::conclude`] has run.
    #[must_use]
    pub fn is_concluded(&self) -> bool {
        self.is_concluded
    }

    /// `Context.conclude()` (`ClusterBackup.java:679-917`), steps 1–6 and 11.
    ///
    /// Steps 7–10 of the reference's `conclude` (Aeron client, error counter,
    /// the four cluster counters, the two archive contexts) belong to the
    /// agent, which is where the live objects are.
    ///
    /// # Errors
    ///
    /// * `ClusterError::Cluster("a ClusterBackup context is concluded once")`
    ///   for a second call on this value — the reference's
    ///   `ConcurrentConcludeException` (`:683-686`);
    /// * `ClusterError::Cluster("ClusterBackup.Context.catchupEndpoint must be
    ///   set")` when neither the property nor the member string gives one
    ///   (`:695-698`);
    /// * the mark file's own refusal, which is how a **live** mark file in the
    ///   directory stops a second backup (`:737-743`).
    pub fn conclude(&mut self) -> Result<(), ClusterError> {
        if self.is_concluded {
            return Err(ClusterError::Cluster(
                "a ClusterBackup context is concluded once".to_owned(),
            ));
        }

        // 1–2. `clusterDir` and `clusterDirectoryName` are one value.
        if self.config.cluster_dir.as_os_str().is_empty() {
            return Err(ClusterError::Cluster(
                "ClusterBackup.Context.clusterDir must be set".to_owned(),
            ));
        }
        self.cluster_dir.clone_from(&self.config.cluster_dir);
        self.mark_file_dir.clone_from(
            &self
                .config
                .mark_file_dir
                .clone()
                .unwrap_or_else(|| self.cluster_dir.clone()),
        );

        // 3. `deleteDirOnStart`.
        if self.config.delete_dir_on_start && self.cluster_dir.exists() {
            fs::remove_dir_all(&self.cluster_dir).map_err(|error| {
                ClusterError::Cluster(format!(
                    "failed to delete cluster dir {}: {error}",
                    self.cluster_dir.display()
                ))
            })?;
        }

        // 4. The catchup endpoint: there is no default, and the reference
        //    refuses to start without one (`:695-698`).
        if self.config.resolved_catchup_endpoint().is_none() {
            return Err(ClusterError::Cluster(
                "ClusterBackup.Context.catchupEndpoint must be set".to_owned(),
            ));
        }

        // 5. Both directories.
        for directory in [&self.cluster_dir, &self.mark_file_dir] {
            fs::create_dir_all(directory).map_err(|error| {
                ClusterError::Cluster(format!(
                    "failed to create directory {}: {error}",
                    directory.display()
                ))
            })?;
        }

        // 6. The page size, from the driver's CnC — which is also the client's,
        //    because a client reads the same file (`ClusterBackup.java:735-736`).
        if 0 == self.file_page_size {
            self.file_page_size = mark::driver_file_page_size(&self.config.aeron_dir);
        }

        // 6b. The mark file, of type BACKUP (`:737-743`), and its link
        //     (`:746-749`).
        let path = self.mark_file_dir.join(mark::FILENAME);
        let mark_file = ClusterMarkFile::create(
            &path,
            ClusterComponentType::BACKUP,
            usize::try_from(self.config.error_buffer_length).map_err(|_| {
                ClusterError::Cluster(format!(
                    "invalid error buffer length: {}",
                    self.config.error_buffer_length
                ))
            })?,
            self.file_page_size,
            self.now_ms,
            self.pid,
        )
        .map_err(|error| ClusterError::Cluster(error.to_string()))?;

        mark::ensure_mark_file_link(&self.cluster_dir, &path)
            .map_err(|error| ClusterError::Cluster(error.to_string()))?;

        self.mark_file = Some(mark_file);
        self.is_concluded = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! `ClusterBackupContextTest` (`ClusterBackupContextTest.java:61-260`).
    //!
    //! The reference's fixture builds a `ClusterBackup.Context` over mocked
    //! `Aeron`/`Aeron.Context` objects; here the two things those mocks supply —
    //! a page size and an error counter — are a real driver's CnC file, which is
    //! where this build reads the same numbers from anyway.

    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "deepmsg-p3-9-context-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).expect("a scratch directory");
        directory
    }

    fn config(cluster_dir: &Path, aeron_dir: &Path) -> ClusterBackupConfig {
        ClusterBackupConfig {
            cluster_id: 0,
            cluster_dir: cluster_dir.to_path_buf(),
            mark_file_dir: None,
            aeron_dir: aeron_dir.to_path_buf(),
            catchup_endpoint: Some("something".to_owned()),
            catchup_channel: CATCHUP_CHANNEL_DEFAULT.to_owned(),
            backup_interval_ns: INTERVAL_DEFAULT_NS,
            response_timeout_ns: RESPONSE_TIMEOUT_DEFAULT_NS,
            progress_timeout_ns: PROGRESS_TIMEOUT_DEFAULT_NS,
            cool_down_interval_ns: COOL_DOWN_INTERVAL_DEFAULT_NS,
            source_type: SOURCE_TYPE_DEFAULT.to_owned(),
            initial_replay_start: INITIAL_REPLAY_START_DEFAULT.to_owned(),
            error_buffer_length: 1_048_576,
            consensus_channel: CONSENSUS_CHANNEL_DEFAULT.to_owned(),
            consensus_stream_id: CONSENSUS_STREAM_ID_DEFAULT,
            consensus_module_snapshot_stream_id: CONSENSUS_MODULE_SNAPSHOT_STREAM_ID_DEFAULT,
            service_snapshot_stream_id: SERVICE_SNAPSHOT_STREAM_ID_DEFAULT,
            log_stream_id: LOG_STREAM_ID_DEFAULT,
            cluster_consensus_endpoints: String::new(),
            replication_progress_timeout_ns: None,
            replication_progress_interval_ns: None,
            cluster_members: None,
            member_id: -1,
            thread_naming: ThreadNaming::DEFAULT,
            delete_dir_on_start: false,
        }
    }

    const NOW_MS: i64 = 1_000_000;

    /// `throwsIllegalStateExceptionIfThereIsAnActiveMarkFile`
    /// (`ClusterBackupContextTest.java:95-105`): a second context over the same
    /// directory is refused, because the first one's mark file is alive.
    #[test]
    fn a_second_context_over_an_active_mark_file_is_refused() {
        let dir = scratch("active");
        let aeron = scratch("active-aeron");

        let mut context = ClusterBackupContext::new(config(&dir, &aeron))
            .with_file_page_size(4096)
            .with_now_ms(NOW_MS);
        context.conclude().expect("the first context concludes");

        let mut other = ClusterBackupContext::new(config(&dir, &aeron))
            .with_file_page_size(4096)
            .with_now_ms(NOW_MS);
        let error = other
            .conclude()
            .expect_err("a live mark file stops a second backup");
        assert!(
            error.to_string().contains("active"),
            "the refusal names the live mark file: {error}"
        );
    }

    /// `clusterDirectoryNameShouldMatchClusterDirWhenClusterDirSet`
    /// (`:107-115`).
    #[test]
    fn the_cluster_directory_is_what_the_context_resolves_to() {
        let dir = scratch("cluster-dir");
        let aeron = scratch("cluster-dir-aeron");

        let mut context = ClusterBackupContext::new(config(&dir, &aeron)).with_file_page_size(4096);
        context.conclude().expect("it concludes");

        assert_eq!(
            dir.canonicalize().unwrap(),
            context.cluster_dir().canonicalize().unwrap()
        );
    }

    /// `concludeShouldCreateMarkFileDirSetDirectly` (`:153-166`) and
    /// `concludeShouldCreateLinkFilePointingToTheParentDirectoryOfTheMarkFile`
    /// (`:184-209`) together: a mark file directory elsewhere is created, and
    /// the link in the cluster directory holds its canonical path.
    #[test]
    fn a_mark_file_directory_elsewhere_is_created_and_linked() {
        let root = scratch("mark-dir");
        let cluster_dir = root.join("cluster");
        let mark_file_dir = root.join("mark-file-dir");
        let aeron = scratch("mark-dir-aeron");

        let mut config = config(&cluster_dir, &aeron);
        config.mark_file_dir = Some(mark_file_dir.clone());
        let mut context = ClusterBackupContext::new(config).with_file_page_size(4096);
        context.conclude().expect("it concludes");

        assert!(mark_file_dir.exists());
        let link = cluster_dir.join(deepmsg_cluster_common::mark::LINK_FILENAME);
        assert!(link.exists(), "the link is written");
        assert_eq!(
            mark_file_dir.canonicalize().unwrap().to_string_lossy(),
            fs::read_to_string(&link).expect("the link's content")
        );
    }

    /// `shouldRemoveLinkIfMarkFileIsInClusterDir` (`:168-182`), both parameter
    /// values: with the mark-file directory set to the cluster directory and
    /// with it unset, a pre-existing link is removed.
    #[test]
    fn a_link_is_removed_when_the_mark_file_is_in_the_cluster_directory() {
        for (name, explicit) in [("explicit", true), ("unset", false)] {
            let dir = scratch(&format!("link-removed-{name}"));
            let aeron = scratch(&format!("link-removed-{name}-aeron"));

            // A stale link from a previous deployment that put the mark file
            // elsewhere.
            let link = dir.join(deepmsg_cluster_common::mark::LINK_FILENAME);
            fs::write(&link, b"/somewhere/else").unwrap();

            let mut config = config(&dir, &aeron);
            if explicit {
                config.mark_file_dir = Some(dir.clone());
            }
            let mut context = ClusterBackupContext::new(config).with_file_page_size(4096);
            context.conclude().expect("it concludes");

            assert!(!link.exists(), "{name}: the stale link is gone");
        }
    }

    /// `shouldAlignMarkFileBasedOnTheAeronClientFilePageSize` (`:211-233`) and
    /// `shouldAlignMarkFileBasedOnTheMediaDriverFilePageSize` (`:235-259`): the
    /// mark file's length is `align(header + error buffer, page size)`.
    #[test]
    fn the_mark_file_is_aligned_to_the_drivers_page_size() {
        let dir = scratch("align");
        let aeron = scratch("align-aeron");

        let mut config = config(&dir, &aeron);
        config.error_buffer_length = 1_919_191;
        let mut context = ClusterBackupContext::new(config).with_file_page_size(4096);
        context.conclude().expect("it concludes");

        let mark_file = context.mark_file().expect("a mark file");
        let page = context.file_page_size();
        assert_eq!(
            0,
            mark_file.length() % page,
            "length {} is a multiple of {page}",
            mark_file.length()
        );
        assert!(mark_file.length() >= 1_919_191);
    }

    /// The endpoint refusal (`ClusterBackup.java:695-698`): no property and no
    /// member string is a fatal conclusion.
    #[test]
    fn a_context_without_a_catchup_endpoint_is_refused() {
        let dir = scratch("no-endpoint");
        let aeron = scratch("no-endpoint-aeron");

        let mut config = config(&dir, &aeron);
        config.catchup_endpoint = None;
        let mut context = ClusterBackupContext::new(config).with_file_page_size(4096);
        let error = context.conclude().expect_err("no endpoint is fatal");
        assert!(
            error.to_string().contains("catchupEndpoint must be set"),
            "{error}"
        );
    }

    /// The two names the agent's thread may carry (`ClusterBackup.java:225-226`),
    /// chosen by `aeron.thread.naming` and by nothing else — unlike the
    /// consensus module's, which appends the cluster and member ids.
    #[test]
    fn the_configured_thread_name_follows_the_naming_mode() {
        let dir = scratch("role-name");
        let mut config = config(&dir, &dir);

        config.thread_naming = ThreadNaming::Classic;
        assert_eq!(AERON_CLUSTER_BACKUP_THREAD_NAME_CLASSIC, config.role_name());

        config.thread_naming = ThreadNaming::New;
        assert_eq!(AERON_CLUSTER_BACKUP_THREAD_NAME, config.role_name());
    }

    /// A second `conclude` on the same context is refused (`:683-686`).
    #[test]
    fn a_context_concludes_once() {
        let dir = scratch("once");
        let aeron = scratch("once-aeron");

        let mut context = ClusterBackupContext::new(config(&dir, &aeron)).with_file_page_size(4096);
        context.conclude().expect("it concludes");
        assert!(context.conclude().is_err());
    }
}
