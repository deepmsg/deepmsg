//! `ClusterBackupAgent` (`ClusterBackupAgent.java`): the backup's eight-state
//! machine.
//!
//! The loop is the reference's: a **slow tick** every ten milliseconds does the
//! things that do not need a deadline (drive the client, touch the mark file,
//! read the local archive's signals, notice the cluster archive failing), then
//! the consensus subscription is polled and the current state does its work.
//! Two conditions stop it: progress stalling for
//! `aeron.cluster.backup.progress.timeout` (a *reset*, not a failure) and an
//! `AgentTermination` (which runs the two hooks and leaves).
//!
//! ```text
//! BACKUP_QUERY ──answer──▶ SNAPSHOT_RETRIEVE ──done──▶ LIVE_LOG_RECORD
//!      ▲                        │                          │
//!      │                        └── no snapshots ──────────┤
//!      │                                                   ▼
//!      │                                            LIVE_LOG_REPLAY
//!      │                                                   │
//!      │                                                   ▼
//!      │                                          UPDATE_RECORDING_LOG
//!      │                                                   │
//!      │                                                   ▼
//!      │◀────────── deadline ────── BACKING_UP ◀───────────┘
//!      │
//!   RESET_BACKUP ──cool down──▶ BACKUP_QUERY
//! ```
//!
//! # What is deliberately different
//!
//! * **The cluster archive's connect and its replay are blocking calls.** The
//!   reference polls `AsyncConnect` and a bounded replay's control response one
//!   step per duty cycle because its agent shares a thread with nothing to
//!   spare. This build's archive client already has the blocking face — and
//!   `start_replay` answers the replay session id the reference has to read out
//!   of the response poller — so the backup uses it. The cost is that a turn can
//!   block for the archive's message timeout (10 s) instead of returning to the
//!   duty cycle.
//! * **No state-change logging.** The reference writes one line per transition
//!   through `ClusterLog`; this build has no cluster log, and inventing a
//!   format would be a new diagnostic artifact nobody asked for.
//! * **`aeron.cluster.member.endpoints` is not consulted** (`P3-0e`), so the
//!   seventh member attribute (`archiveResponseEndpoint`) is the only source of
//!   the cluster archive's MDC response channel.
//! * **An error answer on the local archive's control channel is returned, not
//!   thrown.** The reference throws the `ArchiveException` out of its duty
//!   cycle and ends the agent for `STORAGE_SPACE`
//!   (`ClusterBackupAgent.java:1063-1088`); this build's `poll` cannot throw, so
//!   [`Archive::poll_recording_event`] hands the refusal over and the cycle
//!   returns it as `ClusterError::ArchiveRefused`. `do_work` then applies the
//!   same two answers the reference does (`:1122-1128`): `STORAGE_SPACE` ends
//!   the backup, everything else is counted and resets it. A backup that used
//!   to report only a stalled progress now reports the archive's own words.

use deepmsg_archive::client::archive::RecordingEvent;
use deepmsg_archive::client::async_connect::{AsyncConnect, Credentials, Polled};
use deepmsg_archive::client::proxy::ReplayParams;
use deepmsg_archive::client::{Archive, ArchiveContext, Handlers, NoCredentials};
use deepmsg_client::client::Client;
use deepmsg_client::counter::Counter;
use deepmsg_client::counter::CounterEvent;
use deepmsg_cluster_common::member::{self, ClusterMember};
use deepmsg_cnc::error_log::{DistinctErrorLog, ErrorLogRegion};
use deepmsg_codec_cluster::backup_response_codec::BackupResponseDecoder;
use deepmsg_codec_cluster::challenge_codec::ChallengeDecoder;
use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_codec_cluster::message_header_codec::MessageHeaderDecoder;
use deepmsg_codec_cluster::session_event_codec::SessionEventDecoder;
use deepmsg_codec_cluster::{ReadBuf, SBE_SCHEMA_ID};
use deepmsg_core::uri::ChannelUri;

use crate::backup::consensus_publisher;
use crate::backup::context::ClusterBackupContext;
use crate::backup::log_source_validator::LogSourceValidator;
use crate::backup::publication_group::{COMMAND_TIMEOUT, PublicationGroup, randomised_session_id};
use crate::backup::snapshot_replication::{ArchiveClient, SnapshotReplication};
use crate::backup::{NULL_COUNTER_ID, NULL_LENGTH, NULL_POSITION, NULL_VALUE, ReplayStart, State};
use crate::config::PROTOCOL_SEMANTIC_VERSION;
use crate::error::ClusterError;
use crate::persist::recording_log::{CONSENSUS_MODULE_SERVICE_ID, RecordingLog, Snapshot};

/// `MARK_FILE_UPDATE_INTERVAL_MS` (`ClusterBackupAgent.java:77`).
pub const MARK_FILE_UPDATE_INTERVAL_MS: i64 = 1_000;
/// `SLOW_TICK_INTERVAL_MS` (`:78`).
const SLOW_TICK_INTERVAL_MS: i64 = 10;
/// `ConsensusAdapter.FRAGMENT_LIMIT` (`ConsensusAdapter.java:35`).
const FRAGMENT_LIMIT: usize = 10;
/// The cluster archive control channel's shape when a deployment supplies none
/// — the member's endpoint is put on it when an answer names one.
///
/// **A deviation.** The reference has no default at all: its
/// `AeronArchive.Configuration.controlChannel()` answers the system property or
/// `null`, and its own test fixture supplies both channels
/// (`TestCluster.java:606-610`). A shape with no destination is the smallest
/// value that still works once the endpoint is filled in.
const CLUSTER_ARCHIVE_CONTROL_CHANNEL_DEFAULT: &str = "aeron:udp";

/// `ArchiveException.STORAGE_SPACE` (`ArchiveException.java:53`).
const STORAGE_SPACE: i64 = 11;

/// Told about every counted error, after counter 211 moves
/// (`CountedErrorHandler.onError`).
pub type BackupErrorHandler = Box<dyn FnMut(&ClusterError)>;

/// The name of the thread the agent runs on (`ClusterBackup.java:225-226`).
pub const AERON_CLUSTER_BACKUP_THREAD_NAME: &str = "aeron-cl-bu";
/// The same, under `aeron.thread.naming=classic` — which is the default.
pub const AERON_CLUSTER_BACKUP_THREAD_NAME_CLASSIC: &str = "cluster-backup";

/// `ClusterBackupEventsListener` (`ClusterBackupEventsListener.java`): what a
/// deployment watches a backup through.
///
/// Every method has an empty default, as the reference's interface has no
/// abstract one and its agent checks the listener for null at each call.
#[allow(unused_variables)] // the defaults ignore their arguments, by design
pub trait ClusterBackupEventsListener {
    /// The backup has moved into the query state (`:28-32`).
    fn on_backup_query(&mut self) {}

    /// The leader may be failing (`:34-39`).
    fn on_possible_failure(&mut self, error: &ClusterError) {}

    /// A query was answered (`:41-49`).
    fn on_backup_response(
        &mut self,
        cluster_members: &[ClusterMember],
        log_source_member: &ClusterMember,
        snapshots_to_retrieve: &[Snapshot],
    ) {
    }

    /// The local recording log was updated (`:51-58`).
    fn on_updated_recording_log(
        &mut self,
        recording_log: &RecordingLog,
        snapshots_retrieved: &[Snapshot],
    ) {
    }

    /// The live log is being recorded locally (`:60-68`).
    fn on_live_log_progress(
        &mut self,
        recording_id: i64,
        recording_pos_counter_id: i32,
        log_position: i64,
    ) {
    }
}

/// What a caller supplies in process — the live objects the reference's
/// `ClusterBackup.Context` creates in `conclude`.
///
/// The two archive contexts are the deployment's. The reference's defaults are
/// `null` channels, which its own test fixture and every deployment fill in
/// (`TestCluster.java:606-610`: the archive's own control channel, and a
/// response channel per backup). The defaults here are the *shape* of the two
/// channels with no destination — the endpoint is replaced by the member that
/// answered — so a caller that sets nothing still gets a working backup.
pub struct ClusterBackupComponents {
    /// The credentials offered to the cluster and to its archive.
    pub credentials: Box<dyn Credentials>,
    /// What the deployment watches the backup through.
    pub events_listener: Option<Box<dyn ClusterBackupEventsListener>>,
    /// Run first on an `AgentTermination` (`terminationHook`).
    pub termination_hook: Box<dyn FnMut()>,
    /// Run before [`Self::termination_hook`], with the error
    /// (`extendedTerminationHook`).
    pub extended_termination_hook: Box<dyn FnMut(&ClusterError)>,
    /// Told about every counted error, after the counter moves.
    pub error_handler: Option<BackupErrorHandler>,
    /// The local archive's control channels: **IPC**, as the reference requires
    /// (`ClusterBackup.java:887-893`). `None` is the reference's default.
    pub local_archive_context: Option<ArchiveContext>,
    /// The cluster archive's control channel templates. `None` is the
    /// reference's default, which this build spells as a UDP shape with the
    /// endpoint taken from the answering member.
    pub cluster_archive_context: Option<ArchiveContext>,
}

impl Default for ClusterBackupComponents {
    fn default() -> Self {
        Self {
            credentials: Box::new(NoCredentials),
            events_listener: None,
            termination_hook: Box::new(|| {}),
            extended_termination_hook: Box::new(|_| {}),
            error_handler: None,
            local_archive_context: None,
            cluster_archive_context: None,
        }
    }
}

/// The five counters a backup allocates (`ClusterBackup.java:778-819`).
#[derive(Clone, Copy, Debug)]
pub struct BackupCounters {
    /// 211, versioned: `ClusterBackup Errors`.
    pub errors: Counter,
    /// 208: `ClusterBackup State`.
    pub state: Counter,
    /// 209: `ClusterBackup live log position`.
    pub live_log_position: Counter,
    /// 210: `ClusterBackup next query deadline in ms`.
    pub next_query_deadline_ms: Counter,
    /// 240: `ClusterBackup snapshots retrieved`.
    pub snapshots_retrieved: Counter,
}

/// A recording-log entry the answer named, kept until it can be appended —
/// the reference's `leaderLogEntry` / `leaderLastTermEntry`.
#[derive(Clone, Copy, Debug)]
struct LeaderEntry {
    leadership_term_id: i64,
    term_base_log_position: i64,
}

/// One message the backup's consensus subscription delivered.
enum BackupMessage {
    Response(BackupResponseMessage),
    Challenge {
        cluster_session_id: i64,
        encoded_challenge: Vec<u8>,
    },
    SessionEvent {
        correlation_id: i64,
        event_code: EventCode,
        detail: String,
    },
}

/// A `BackupResponse`, decoded.
struct BackupResponseMessage {
    correlation_id: i64,
    log_recording_id: i64,
    log_leadership_term_id: i64,
    log_term_base_log_position: i64,
    last_leadership_term_id: i64,
    last_term_base_log_position: i64,
    commit_position_counter_id: i32,
    leader_member_id: i32,
    /// `None` for the reference's null member id.
    member_id: Option<i32>,
    snapshots: Vec<Snapshot>,
    cluster_members: String,
}

/// The agent (`ClusterBackupAgent.java:74-1931`).
pub struct ClusterBackupAgent {
    ctx: ClusterBackupContext,
    client: Client,
    counters: BackupCounters,
    local_archive_context: ArchiveContext,
    cluster_archive_context: ArchiveContext,
    credentials: Box<dyn Credentials>,
    events_listener: Option<Box<dyn ClusterBackupEventsListener>>,
    termination_hook: Box<dyn FnMut()>,
    extended_termination_hook: Box<dyn FnMut(&ClusterError)>,
    error_handler: Option<BackupErrorHandler>,

    state: State,
    recording_log: Option<RecordingLog>,
    backup_archive: Option<Archive>,
    cluster_archive_async_connect: Option<AsyncConnect>,
    /// The context the connect in progress was started with: the member's
    /// endpoint is on its channels, and `Archive::from_connected` needs the
    /// same one back.
    cluster_archive_connect_context: Option<ArchiveContext>,
    cluster_archive: Option<Archive>,
    snapshot_replication: Option<SnapshotReplication>,
    consensus_subscription_id: i64,
    consensus_publication_group: PublicationGroup,
    log_source_validator: LogSourceValidator,
    role_name: String,

    cluster_members: Option<Vec<ClusterMember>>,
    log_supplier_member: Option<ClusterMember>,
    leader_log_entry: Option<LeaderEntry>,
    leader_last_term_entry: Option<LeaderEntry>,
    cluster_log_recording_id: i64,
    leader_commit_position_counter_id: i32,
    snapshots_to_retrieve: Vec<Snapshot>,
    snapshots_retrieved: Vec<Snapshot>,

    recording_subscription_id: i64,
    replay_channel: Option<String>,
    recording_channel: Option<String>,
    live_log_recording_subscription_id: i64,
    live_log_recording_session_id: i64,
    live_log_replay_session_id: i64,
    live_log_recording_id: i64,
    live_log_recording_counter_id: i32,

    /// The mark file's error log, remembered across sightings
    /// (`CountedErrorHandler`'s `DistinctErrorLog`).
    counted_error_log: DistinctErrorLog,
    slow_tick_deadline_ms: i64,
    mark_file_update_deadline_ms: i64,
    time_of_last_backup_query_ms: i64,
    time_of_last_progress_ms: i64,
    cool_down_deadline_ms: i64,
    correlation_id: i64,
}

impl ClusterBackupAgent {
    /// Build an agent over a concluded context. [`Self::on_start`] is what opens
    /// the recording log and the local archive.
    pub(crate) fn new(
        ctx: ClusterBackupContext,
        mut client: Client,
        counters: BackupCounters,
        mut components: ClusterBackupComponents,
    ) -> Result<Self, ClusterError> {
        let config = ctx.config();

        let local_archive_context = components
            .local_archive_context
            .take()
            .unwrap_or_else(|| default_local_archive_context(config));
        if !local_archive_context
            .control_request_channel
            .as_deref()
            .is_some_and(|channel| channel.starts_with("aeron:ipc"))
            || !local_archive_context
                .control_response_channel
                .as_deref()
                .is_some_and(|channel| channel.starts_with("aeron:ipc"))
        {
            // `ClusterBackup.java:887-893`.
            return Err(ClusterError::Cluster(
                "local archive control must be IPC".to_owned(),
            ));
        }

        let cluster_archive_context = components
            .cluster_archive_context
            .take()
            .unwrap_or_else(|| default_cluster_archive_context(config));

        let source_type = config.source_type()?;
        let role_name = config.role_name();

        let consensus_channel = config.resolved_consensus_channel();
        let consensus_subscription_id = client
            .add_subscription(
                &consensus_channel,
                config.consensus_stream_id,
                COMMAND_TIMEOUT,
            )
            .map_err(|error| {
                ClusterError::Cluster(format!(
                    "could not subscribe to the backup's consensus channel: {error}"
                ))
            })?;

        let mut agent = Self {
            client,
            counters,
            local_archive_context,
            cluster_archive_context,
            credentials: components.credentials,
            events_listener: components.events_listener,
            termination_hook: components.termination_hook,
            extended_termination_hook: components.extended_termination_hook,
            error_handler: components.error_handler,
            state: State::BackupQuery,
            recording_log: None,
            backup_archive: None,
            cluster_archive_async_connect: None,
            cluster_archive_connect_context: None,
            cluster_archive: None,
            snapshot_replication: None,
            consensus_subscription_id,
            consensus_publication_group: PublicationGroup::new(
                config
                    .cluster_consensus_endpoints
                    .split(',')
                    .map(str::to_owned)
                    .collect(),
                &consensus_channel,
                config.consensus_stream_id,
            ),
            log_source_validator: LogSourceValidator::new(source_type),
            role_name,
            cluster_members: None,
            log_supplier_member: None,
            leader_log_entry: None,
            leader_last_term_entry: None,
            cluster_log_recording_id: NULL_VALUE,
            leader_commit_position_counter_id: NULL_COUNTER_ID,
            snapshots_to_retrieve: Vec::new(),
            snapshots_retrieved: Vec::new(),
            recording_subscription_id: NULL_VALUE,
            replay_channel: None,
            recording_channel: None,
            live_log_recording_subscription_id: NULL_VALUE,
            live_log_recording_session_id: NULL_VALUE,
            live_log_replay_session_id: NULL_VALUE,
            live_log_recording_id: NULL_VALUE,
            live_log_recording_counter_id: NULL_COUNTER_ID,
            counted_error_log: DistinctErrorLog::default(),
            slow_tick_deadline_ms: 0,
            mark_file_update_deadline_ms: 0,
            time_of_last_backup_query_ms: 0,
            time_of_last_progress_ms: 0,
            cool_down_deadline_ms: NULL_VALUE,
            correlation_id: NULL_VALUE,
            ctx,
        };

        agent.consensus_publication_group.shuffle(&mut agent.client);

        Ok(agent)
    }

    /// `onStart` (`ClusterBackupAgent.java:196-211`): the recording log, the
    /// local archive, and the first query deadline.
    ///
    /// # Errors
    ///
    /// The recording log's file, or the local archive's connect.
    pub fn on_start(&mut self) -> Result<(), ClusterError> {
        let path = self
            .ctx
            .cluster_dir()
            .join(crate::persist::recording_log::FILENAME);
        self.recording_log = Some(RecordingLog::open(&path).map_err(|error| {
            ClusterError::Cluster(format!("the backup's recording log will not open: {error}"))
        })?);

        let context = self.local_archive_context.clone();
        let archive = Archive::connect(
            &context,
            &mut self.client,
            &mut *self.credentials,
            Handlers::default(),
        )
        .map_err(|error| {
            ClusterError::Cluster(format!("the local archive will not connect: {error}"))
        })?;
        self.backup_archive = Some(archive);

        let now_ms = deepmsg_core::clock::epoch_millis();
        write_counter(
            &self.client,
            self.counters.next_query_deadline_ms,
            now_ms - 1,
        );
        self.time_of_last_progress_ms = now_ms;

        Ok(())
    }

    /// The thread name the agent is meant to run under (`roleName`, `:296-300`).
    #[must_use]
    pub fn role_name(&self) -> &str {
        &self.role_name
    }

    /// The context the agent runs over — the fixture reads the mark file from it
    /// (`ClusterBackup.context()`, `:1964-1968`).
    #[must_use]
    pub fn context(&self) -> &ClusterBackupContext {
        &self.ctx
    }

    /// The state counter's current value, as the agent holds it.
    #[must_use]
    pub fn state(&self) -> State {
        self.state
    }

    /// The counters this agent writes.
    #[must_use]
    pub fn counters(&self) -> BackupCounters {
        self.counters
    }

    /// One duty cycle (`doWork`, `ClusterBackupAgent.java:234-293`).
    ///
    /// # Errors
    ///
    /// The error the cycle met, after the reset (or, for an `AgentTermination`,
    /// the termination hooks) the reference does with it.
    pub fn do_work(&mut self) -> Result<usize, ClusterError> {
        let now_ms = deepmsg_core::clock::epoch_millis();

        match self.do_work_inner(now_ms) {
            Ok(work) => Ok(work),
            Err(error) if error.is_termination() => {
                self.run_termination_hooks(&error);
                Err(error)
            }
            Err(error) => {
                // `:1122-1128`: a local archive that has run out of space ends
                // the backup, where every other refusal is a reset.
                if matches!(&error, ClusterError::ArchiveRefused { code, .. } if *code == STORAGE_SPACE)
                {
                    self.count_error(&error);
                    let termination = ClusterError::AgentTermination(format!(
                        "the local archive has run out of storage space: {error:?}"
                    ));
                    self.run_termination_hooks(&termination);
                    return Err(termination);
                }

                // `:275-285`: a possible failure is reported, the backup resets,
                // and the error still leaves the cycle. The runner's counted
                // error handler runs for every thrown error
                // (`AgentRunner`'s handler, `ClusterBackupAgent.java:248-319`),
                // which is what puts `AUTHENTICATION_REJECTED` in the mark
                // file's error log where a case reads it.
                self.count_error(&error);
                self.possible_failure(&error);
                self.set_state(State::ResetBackup);
                Err(error)
            }
        }
    }

    fn do_work_inner(&mut self, now_ms: i64) -> Result<usize, ClusterError> {
        let mut work = 0;

        if now_ms > self.slow_tick_deadline_ms {
            self.slow_tick_deadline_ms = now_ms + SLOW_TICK_INTERVAL_MS;
            work += self.slow_tick(now_ms)?;
        }

        work += self.poll_consensus_subscription()?;

        match self.state {
            State::BackupQuery => work += self.backup_query(now_ms)?,
            State::SnapshotRetrieve => work += self.snapshot_retrieve(now_ms)?,
            State::LiveLogRecord => work += self.live_log_record(now_ms)?,
            State::LiveLogReplay => work += self.live_log_replay(now_ms)?,
            State::UpdateRecordingLog => work += self.update_recording_log(now_ms)?,
            State::BackingUp => work += self.backing_up(now_ms)?,
            State::ResetBackup => work += self.reset_backup(now_ms)?,
            State::Closed => return Ok(work),
        }

        if self.has_progress_stalled(now_ms) {
            self.possible_failure(&ClusterError::Timeout("progress has stalled".to_owned()));
            self.set_state(State::ResetBackup);
        }

        Ok(work)
    }

    /// `onClose` (`ClusterBackupAgent.java:213-232`).
    ///
    /// # Errors
    ///
    /// Whatever closing the recording log or signalling the mark file found.
    pub fn close(&mut self) -> Result<(), ClusterError> {
        if !self.client.is_closed() {
            self.stop_replay();
            self.stop_recording();

            if let Some(mut replication) = self.snapshot_replication.take() {
                if let Some(archive) = self.backup_archive.as_mut() {
                    let mut archive_client = ArchiveClient {
                        client: &mut self.client,
                        archive,
                    };
                    replication.close(&mut archive_client);
                }
            }

            self.consensus_publication_group.close(&mut self.client);
            write_counter(&self.client, self.counters.state, State::Closed.code());
            self.state = State::Closed;
        }

        if let Some(mut archive) = self.backup_archive.take() {
            archive.close(&self.client);
        }
        self.drop_cluster_archive();

        if let Some(recording_log) = self.recording_log.take() {
            drop(recording_log);
        }

        if let Some(mark_file) = self.ctx.mark_file() {
            mark_file
                .signal_terminated()
                .map_err(|error| ClusterError::Cluster(format!("the mark file: {error}")))?;
        }

        Ok(())
    }

    // -- the slow tick -------------------------------------------------------

    /// `slowTick` (`:1014-1046`).
    fn slow_tick(&mut self, now_ms: i64) -> Result<usize, ClusterError> {
        let mut work = usize::from(self.client.poll());

        if self.client.is_closed() {
            return Err(ClusterError::AgentTermination(
                "unexpected Aeron close".to_owned(),
            ));
        }

        if now_ms >= self.mark_file_update_deadline_ms {
            self.mark_file_update_deadline_ms = now_ms + MARK_FILE_UPDATE_INTERVAL_MS;
            if let Some(mark_file) = self.ctx.mark_file() {
                mark_file.update_activity_timestamp(now_ms);
            }
        }

        work += self.poll_backup_archive_events()?;

        if NULL_VALUE == self.correlation_id {
            let error = match self.cluster_archive.as_mut() {
                Some(archive) => archive.check_for_error_response(&mut self.client).err(),
                None => None,
            };
            if let Some(error) = error {
                self.count_error(&ClusterError::Cluster(format!(
                    "cluster archive error: {error}"
                )));
                self.set_state(State::ResetBackup);
            }
        }

        self.drain_counter_events();
        Ok(work)
    }

    /// `onUnavailableCounter` (`:355-366`): the live log's recording counter is
    /// the one the backup reads its position from, and it going away means the
    /// recording did.
    fn drain_counter_events(&mut self) {
        for event in self.client.counter_events() {
            if let CounterEvent::Unavailable { counter_id, .. } = event {
                if counter_id == self.live_log_recording_counter_id {
                    self.possible_failure(&ClusterError::Cluster(
                        "log recording counter unexpectedly unavailable".to_owned(),
                    ));
                    self.set_state(State::ResetBackup);
                }
            }
        }
    }

    /// `pollBackupArchiveEvents` (`:1088-1122`): the local archive's recording
    /// signals, which is how a snapshot replication learns it is making
    /// progress.
    fn poll_backup_archive_events(&mut self) -> Result<usize, ClusterError> {
        let event = {
            let Some(archive) = self.backup_archive.as_mut() else {
                return Ok(0);
            };
            archive.poll_recording_event(&mut self.client)
        };

        let event = match event {
            Ok(event) => event,
            Err(error) => {
                self.count_error(&ClusterError::from(error));
                return Ok(0);
            }
        };

        let signal = match event {
            Some(RecordingEvent::Signal(signal)) => signal,
            // `ClusterBackupAgent.pollBackupArchiveEvents` (`:1063-1088`): a
            // control-response ERROR is an `ArchiveException` — counted, and
            // for `STORAGE_SPACE` the agent ends rather than carry on copying
            // into a filesystem that has none.
            Some(RecordingEvent::Refusal(refusal)) => {
                // `ClusterBackupAgent.pollBackupArchiveEvents` (`:1063-1088`): a
                // control-response ERROR is thrown as the `ArchiveException` it
                // is. Returning it is this build's throw, and `do_work` is where
                // the reference's two answers to one live (`:1122-1128` ends the
                // backup for `STORAGE_SPACE`, everything else resets it).
                return Err(ClusterError::ArchiveRefused {
                    code: refusal.error_code,
                    message: refusal.message,
                });
            }
            None => return Ok(0),
        };

        if let Some(replication) = self.snapshot_replication.as_mut() {
            if let Some(archive) = self.backup_archive.as_mut() {
                let archive_client = ArchiveClient {
                    client: &mut self.client,
                    archive,
                };
                replication.on_signal(
                    &archive_client,
                    signal.correlation_id,
                    signal.recording_id,
                    signal.position,
                    signal.signal,
                )?;
            }
        }

        Ok(1)
    }

    // -- the consensus subscription -----------------------------------------

    /// `consensusSubscription.poll(fragmentAssembler, …)` (`:243`), decoded in
    /// one turn and dispatched in the next.
    ///
    /// A separate vector because the handler inside `poll_subscription` cannot
    /// borrow the agent: the same split the consensus module's own adapter uses.
    fn poll_consensus_subscription(&mut self) -> Result<usize, ClusterError> {
        let mut messages = Vec::new();
        let polled = self.client.poll_subscription(
            self.consensus_subscription_id,
            FRAGMENT_LIMIT,
            |message| {
                if let Some(decoded) = decode_backup_message(message.payload) {
                    messages.push(decoded);
                }
            },
        );

        for message in messages {
            self.handle_backup_message(message)?;
        }

        Ok(polled)
    }

    fn handle_backup_message(&mut self, message: BackupMessage) -> Result<(), ClusterError> {
        match message {
            BackupMessage::Response(response) => self.on_backup_response(response),
            BackupMessage::Challenge {
                cluster_session_id,
                encoded_challenge,
            } => {
                self.on_challenge(cluster_session_id, &encoded_challenge)?;
                Ok(())
            }
            BackupMessage::SessionEvent {
                correlation_id,
                event_code,
                detail,
            } => self.on_session_event(correlation_id, event_code, &detail),
        }
    }

    /// `onBackupResponse` (`:463-595`): the answer to a query, which is the only
    /// way the backup learns where the cluster's log is and whose archive to
    /// read.
    #[allow(clippy::too_many_lines)] // one port of one reference method
    fn on_backup_response(&mut self, response: BackupResponseMessage) -> Result<(), ClusterError> {
        let Some(member_id) = response.member_id else {
            self.count_error(&ClusterError::Cluster(
                "onBackupResponse(): memberId is null, retrying for compatible node".to_owned(),
            ));
            self.set_state(State::ResetBackup);
            return Ok(());
        };

        if !self
            .log_source_validator
            .is_acceptable(i64::from(response.leader_member_id), i64::from(member_id))
        {
            self.consensus_publication_group
                .close_and_exclude_current(&mut self.client);
            self.set_state(State::ResetBackup);
            return Ok(());
        }

        if self
            .log_supplier_member
            .as_ref()
            .is_some_and(|supplier| supplier.id != member_id)
        {
            self.set_state(State::ResetBackup);
            return Ok(());
        }

        if State::BackupQuery != self.state || response.correlation_id != self.correlation_id {
            return Ok(());
        }

        // The snapshot set to ask for: every entry the answer named that this
        // log does not already hold at the same position (`:502-518`).
        {
            let recording_log = self.recording_log.as_ref();
            for snapshot in &response.snapshots {
                let already_held = recording_log
                    .and_then(|log| log.get_latest_snapshot(snapshot.service_id))
                    .is_some_and(|entry| entry.log_position == snapshot.log_position);
                if !already_held {
                    self.snapshots_to_retrieve.push(*snapshot);
                }
            }
        }

        let same_supplier = self
            .log_supplier_member
            .as_ref()
            .is_some_and(|supplier| supplier.id == member_id)
            && response.log_recording_id == self.cluster_log_recording_id;

        if !same_supplier {
            self.cluster_log_recording_id = response.log_recording_id;
            self.leader_log_entry = Some(LeaderEntry {
                leadership_term_id: response.log_leadership_term_id,
                term_base_log_position: response.log_term_base_log_position,
            });
        }

        let last_term_is_recorded = self
            .recording_log
            .as_ref()
            .and_then(RecordingLog::find_last_term)
            .is_some_and(|entry| entry.leadership_term_id == response.last_leadership_term_id);
        if !last_term_is_recorded {
            self.leader_last_term_entry = Some(LeaderEntry {
                leadership_term_id: response.last_leadership_term_id,
                term_base_log_position: response.last_term_base_log_position,
            });
        }

        self.time_of_last_backup_query_ms = 0;
        self.correlation_id = NULL_VALUE;
        self.leader_commit_position_counter_id = response.commit_position_counter_id;

        let mut cluster_members = ClusterMember::parse_list(&response.cluster_members)?;
        ClusterMember::set_is_leader(&mut cluster_members, response.leader_member_id);

        let Some(supplier) = member::find_member(&cluster_members, member_id).cloned() else {
            return Err(ClusterError::Cluster(format!(
                "{member_id} not found in {}",
                response.cluster_members
            )));
        };
        let mut supplier = supplier;
        supplier.leadership_term_id = response.log_leadership_term_id;

        self.cluster_members = Some(cluster_members);
        self.log_supplier_member = Some(supplier);

        if let Some(listener) = self.events_listener.as_mut() {
            let cluster_members = self.cluster_members.as_deref().unwrap_or(&[]);
            let supplier = self.log_supplier_member.as_ref().expect("just set");
            listener.on_backup_response(cluster_members, supplier, &self.snapshots_to_retrieve);
        }

        if self.cluster_archive.is_none() && self.cluster_archive_async_connect.is_none() {
            let supplier = self.log_supplier_member.as_ref().expect("just set");
            let mut context = self.cluster_archive_context.clone();
            let template = context
                .control_request_channel
                .clone()
                .unwrap_or_else(|| CLUSTER_ARCHIVE_CONTROL_CHANNEL_DEFAULT.to_owned());
            let mut request_uri = ChannelUri::parse(&template).map_err(|error| {
                ClusterError::Cluster(format!(
                    "the cluster archive's control channel does not parse: {template}: {error}"
                ))
            })?;
            request_uri.put("endpoint", supplier.archive_endpoint.clone());
            context.control_request_channel = Some(request_uri.build());

            if let Some(response_endpoint) = &supplier.archive_response_endpoint {
                let template = context
                    .control_response_channel
                    .clone()
                    .unwrap_or_else(|| "aeron:udp?endpoint=localhost:0".to_owned());
                let mut response_uri = ChannelUri::parse(&template).map_err(|error| {
                    ClusterError::Cluster(format!(
                        "the cluster archive's response channel does not parse: {template}: {error}"
                    ))
                })?;
                response_uri.put("control-mode", "response");
                response_uri.remove("endpoint");
                response_uri.put("control", response_endpoint.clone());
                context.control_response_channel = Some(response_uri.build());
            }

            let connect = AsyncConnect::new(&context, &mut self.client, &mut *self.credentials)
                .map_err(|error| {
                    ClusterError::Cluster(format!(
                        "could not start the cluster archive's connect: {error}"
                    ))
                })?;
            self.cluster_archive_async_connect = Some(connect);
            self.cluster_archive_connect_context = Some(context);
        }

        let now_ms = deepmsg_core::clock::epoch_millis();
        self.time_of_last_progress_ms = now_ms;
        self.set_state(if self.snapshots_to_retrieve.is_empty() {
            State::LiveLogRecord
        } else {
            State::SnapshotRetrieve
        });

        Ok(())
    }

    /// `onChallenge` (`:597-604`).
    fn on_challenge(
        &mut self,
        cluster_session_id: i64,
        encoded_challenge: &[u8],
    ) -> Result<(), ClusterError> {
        let response = self
            .credentials
            .on_challenge(encoded_challenge)
            .map_err(ClusterError::Cluster)?;
        let correlation_id = self.client.next_correlation_id()?;

        if let Some(registration_id) = self.consensus_publication_group.current() {
            if let (Some(position_limit), Some(publication)) = (
                self.read_position_limit(registration_id),
                self.client.exclusive_publication(registration_id),
            ) {
                consensus_publisher::challenge_response(
                    publication,
                    position_limit,
                    correlation_id,
                    cluster_session_id,
                    &response,
                );
            }
        }

        self.correlation_id = correlation_id;
        Ok(())
    }

    /// `onSessionEvent` (`:606-630`).
    fn on_session_event(
        &mut self,
        correlation_id: i64,
        event_code: EventCode,
        detail: &str,
    ) -> Result<(), ClusterError> {
        if self.correlation_id != correlation_id {
            return Ok(());
        }

        match event_code {
            EventCode::ERROR | EventCode::AUTHENTICATION_REJECTED => {
                Err(ClusterError::Cluster(format!("{event_code:?}: {detail}")))
            }
            EventCode::REDIRECT => {
                self.consensus_publication_group
                    .close_and_exclude_current(&mut self.client);
                self.set_state(State::ResetBackup);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    // -- BACKUP_QUERY --------------------------------------------------------

    /// `backupQuery` (`:679-706`): move to the next endpoint when the current
    /// one has not answered within the response timeout, and ask once the
    /// publication is connected.
    fn backup_query(&mut self, now_ms: i64) -> Result<usize, ClusterError> {
        let needs_next = self.consensus_publication_group.current().is_none()
            || now_ms > self.time_of_last_backup_query_ms + self.response_timeout_ms();

        if needs_next {
            self.consensus_publication_group.next(&mut self.client)?;
            self.correlation_id = NULL_VALUE;
            self.time_of_last_backup_query_ms = now_ms;
            return Ok(1);
        }

        if NULL_VALUE != self.correlation_id
            || !self.consensus_publication_group.is_connected(&self.client)
        {
            return Ok(0);
        }

        let correlation_id = self.client.next_correlation_id()?;
        let Some(registration_id) = self.consensus_publication_group.current() else {
            return Ok(0);
        };
        let Some(position_limit) = self.read_position_limit(registration_id) else {
            return Ok(0);
        };

        let credentials = self.credentials.encoded();
        let consensus_channel = self.ctx.config().resolved_consensus_channel();
        let sent = match self.client.exclusive_publication(registration_id) {
            Some(publication) => consensus_publisher::backup_query(
                publication,
                position_limit,
                correlation_id,
                self.ctx.config().consensus_stream_id,
                PROTOCOL_SEMANTIC_VERSION,
                &consensus_channel,
                &credentials,
            ),
            None => false,
        };

        if sent {
            self.time_of_last_backup_query_ms = now_ms;
            self.correlation_id = correlation_id;
            return Ok(1);
        }

        Ok(0)
    }

    /// A publication's position limit, read from the counter the driver keeps
    /// for it — the value every send in this crate passes to `offer`.
    fn read_position_limit(&self, registration_id: i64) -> Option<i64> {
        let publication = self.client.exclusive_publication(registration_id)?;
        self.client
            .counters_reader()?
            .value(publication.position_limit_counter_id())
    }

    // -- SNAPSHOT_RETRIEVE ---------------------------------------------------

    /// `snapshotRetrieve` (`:708-745`): wait for the cluster archive's connect,
    /// then copy the snapshots one at a time.
    fn snapshot_retrieve(&mut self, now_ms: i64) -> Result<usize, ClusterError> {
        let mut work = 0;

        if self.cluster_archive.is_none() {
            if !self.poll_cluster_archive_connect()? {
                return Ok(1);
            }
            work += 1;
        }

        if self.snapshot_replication.is_none() {
            let replication_channel = self.replication_channel()?;
            let (control_stream_id, control_channel) = match self.cluster_archive.as_ref() {
                Some(archive) => (
                    archive.context().control_request_stream_id,
                    archive
                        .context()
                        .control_request_channel
                        .clone()
                        .unwrap_or_default(),
                ),
                None => return Ok(work),
            };

            let mut replication = SnapshotReplication::with_timeouts(
                control_stream_id,
                control_channel,
                replication_channel,
                self.ctx.config().replication_progress_timeout_ns(),
                self.ctx.config().replication_progress_interval_ns(),
            );
            for snapshot in &self.snapshots_to_retrieve {
                replication.add_snapshot(*snapshot);
            }
            self.snapshot_replication = Some(replication);
            work += 1;
        }

        {
            let Some(replication) = self.snapshot_replication.as_mut() else {
                return Ok(work);
            };
            let Some(archive) = self.backup_archive.as_mut() else {
                return Ok(work);
            };
            let mut archive_client = ArchiveClient {
                client: &mut self.client,
                archive,
            };
            work += replication.poll(&mut archive_client, now_ms * 1_000_000)?;
        }

        work += self.poll_backup_archive_events()?;
        self.time_of_last_progress_ms = now_ms;

        let complete = self
            .snapshot_replication
            .as_ref()
            .is_some_and(SnapshotReplication::is_complete);
        if complete {
            if let Some(mut replication) = self.snapshot_replication.take() {
                self.snapshots_retrieved
                    .extend(replication.snapshots_retrieved());
                if let Some(archive) = self.backup_archive.as_mut() {
                    let mut archive_client = ArchiveClient {
                        client: &mut self.client,
                        archive,
                    };
                    replication.close(&mut archive_client);
                }
            }
            self.set_state(State::LiveLogRecord);
            work += 1;
        }

        Ok(work)
    }

    /// One step of the cluster archive's async connect (`:713-719`).
    fn poll_cluster_archive_connect(&mut self) -> Result<bool, ClusterError> {
        let Some(connect) = self.cluster_archive_async_connect.as_mut() else {
            return Ok(false);
        };

        let polled = connect.poll(&mut self.client, &mut *self.credentials);
        match polled {
            Ok(Polled::Awaiting) => Ok(false),
            Ok(Polled::Done(connected)) => {
                self.cluster_archive_async_connect = None;
                let context = self
                    .cluster_archive_connect_context
                    .take()
                    .unwrap_or_else(|| self.cluster_archive_context.clone());
                self.cluster_archive = Some(Archive::from_connected(
                    context,
                    *connected,
                    Handlers::default(),
                ));
                Ok(true)
            }
            Err(error) => Err(ClusterError::Cluster(format!(
                "the cluster archive will not connect: {error}"
            ))),
        }
    }

    /// `catchupChannel` with the cluster's endpoint on it (`:723-731`).
    fn replication_channel(&self) -> Result<String, ClusterError> {
        let catchup_endpoint = self
            .ctx
            .config()
            .resolved_catchup_endpoint()
            .ok_or_else(|| {
                ClusterError::Cluster(
                    "ClusterBackup.Context.catchupEndpoint must be set".to_owned(),
                )
            })?;
        let mut uri = ChannelUri::parse(&self.ctx.config().catchup_channel).map_err(|error| {
            ClusterError::Cluster(format!(
                "the backup's catchup channel does not parse: {error}"
            ))
        })?;
        uri.put("endpoint", catchup_endpoint);
        Ok(uri.build())
    }

    // -- LIVE_LOG_RECORD -----------------------------------------------------

    /// `liveLogRecord` (`:747-810`): set up the recording of the cluster's log
    /// in the local archive.
    fn live_log_record(&mut self, now_ms: i64) -> Result<usize, ClusterError> {
        if NULL_VALUE != self.live_log_recording_subscription_id {
            self.time_of_last_progress_ms = now_ms;
            self.set_state(State::LiveLogReplay);
            return Ok(1);
        }

        if NULL_VALUE == self.live_log_recording_session_id {
            self.live_log_recording_session_id = i64::from(randomised_session_id());
        }

        let catchup_endpoint = self
            .ctx
            .config()
            .resolved_catchup_endpoint()
            .ok_or_else(|| {
                ClusterError::Cluster(
                    "ClusterBackup.Context.catchupEndpoint must be set".to_owned(),
                )
            })?;
        let session_id = self.live_log_recording_session_id.to_string();

        if catchup_endpoint.ends_with(":0") {
            if NULL_VALUE == self.recording_subscription_id {
                let mut uri =
                    ChannelUri::parse(&self.ctx.config().catchup_channel).map_err(|error| {
                        ClusterError::Cluster(format!(
                            "the backup's catchup channel does not parse: {error}"
                        ))
                    })?;
                uri.remove("endpoint");
                let tag = self.client.next_correlation_id()?;
                let tag2 = self.client.next_correlation_id()?;
                uri.put("tags", format!("{tag},{tag2}"));
                uri.put("session-id", session_id.clone());
                self.recording_channel = Some(uri.build());

                uri.put("endpoint", catchup_endpoint);
                self.recording_subscription_id = self
                    .client
                    .add_subscription(
                        &uri.build(),
                        self.ctx.config().log_stream_id,
                        COMMAND_TIMEOUT,
                    )
                    .map_err(|error| {
                        ClusterError::Cluster(format!(
                            "could not subscribe to the backup's recording channel: {error}"
                        ))
                    })?;
                self.time_of_last_progress_ms = now_ms;
                return Ok(1);
            }

            let resolved = {
                let counters = self.client.counters_reader();
                let subscription = self.client.subscription(self.recording_subscription_id);
                match (counters, subscription) {
                    (Some(counters), Some(subscription)) => {
                        subscription.resolved_endpoint(&counters)
                    }
                    _ => None,
                }
            };
            let Some(resolved) = resolved else {
                return Ok(0);
            };

            let mut uri =
                ChannelUri::parse(&self.ctx.config().catchup_channel).map_err(|error| {
                    ClusterError::Cluster(format!(
                        "the backup's catchup channel does not parse: {error}"
                    ))
                })?;
            uri.put("endpoint", catchup_endpoint);
            uri.replace_endpoint_wildcard_port(&resolved)
                .map_err(|error| {
                    ClusterError::Cluster(format!(
                        "the resolved catchup endpoint {resolved} does not parse: {error}"
                    ))
                })?;
            uri.put("session-id", session_id);
            self.replay_channel = Some(uri.build());
        } else {
            let mut uri =
                ChannelUri::parse(&self.ctx.config().catchup_channel).map_err(|error| {
                    ClusterError::Cluster(format!(
                        "the backup's catchup channel does not parse: {error}"
                    ))
                })?;
            uri.put("endpoint", catchup_endpoint);
            uri.put("session-id", session_id);
            let channel = uri.build();
            self.replay_channel = Some(channel.clone());
            self.recording_channel = Some(channel);
        }

        self.live_log_recording_subscription_id = self.start_log_recording()?;
        self.time_of_last_progress_ms = now_ms;
        self.set_state(State::LiveLogReplay);

        Ok(1)
    }

    /// `startLogRecording` (`:1132-1145`): extend the recording this backup
    /// already has, or start one.
    fn start_log_recording(&mut self) -> Result<i64, ClusterError> {
        let recording_channel = self.recording_channel.clone().ok_or_else(|| {
            ClusterError::Cluster("the backup's recording channel is not set".to_owned())
        })?;
        let last_term = self
            .recording_log
            .as_ref()
            .and_then(RecordingLog::find_last_term)
            .map(|entry| entry.recording_id);

        let Some(archive) = self.backup_archive.as_mut() else {
            return Err(ClusterError::Cluster(
                "the local archive is not connected".to_owned(),
            ));
        };

        // `REMOTE` is `localSource = false`: the cluster's recording is not this
        // process's publication.
        let subscription_id = match last_term {
            Some(recording_id) => archive.extend_recording(
                &mut self.client,
                recording_id,
                &recording_channel,
                self.ctx.config().log_stream_id,
                false,
                true,
            )?,
            None => archive.start_recording(
                &mut self.client,
                &recording_channel,
                self.ctx.config().log_stream_id,
                false,
                true,
            )?,
        };

        if NULL_VALUE != self.recording_subscription_id {
            let _ = self
                .client
                .remove_subscription(self.recording_subscription_id, COMMAND_TIMEOUT);
        }
        self.recording_subscription_id = NULL_VALUE;
        self.recording_channel = None;

        Ok(subscription_id)
    }

    // -- LIVE_LOG_REPLAY -----------------------------------------------------

    /// `liveLogReplay` (`:818-900`): replay the cluster's log into the local
    /// archive's recording.
    fn live_log_replay(&mut self, now_ms: i64) -> Result<usize, ClusterError> {
        let mut work = 0;

        if NULL_VALUE != self.live_log_recording_id {
            self.time_of_last_progress_ms = now_ms;
            self.set_state(State::UpdateRecordingLog);
            return Ok(1);
        }

        if self.cluster_archive.is_none() {
            if !self.poll_cluster_archive_connect()? {
                return Ok(1);
            }
            work += 1;
        }

        if NULL_VALUE == self.correlation_id {
            let replay_channel = self.replay_channel.clone().ok_or_else(|| {
                ClusterError::Cluster("the backup's replay channel is not set".to_owned())
            })?;

            let start_position = {
                let Some(archive) = self.backup_archive.as_mut() else {
                    return Ok(work);
                };
                replay_start_position(
                    self.recording_log
                        .as_ref()
                        .and_then(RecordingLog::find_last_term),
                    &self.snapshots_retrieved,
                    self.ctx.config().initial_replay_start()?,
                    |recording_id| {
                        archive
                            .get_stop_position(&mut self.client, recording_id)
                            .map_err(ClusterError::from)
                    },
                )?
            };

            let params = ReplayParams {
                position: start_position,
                length: i64::from(NULL_LENGTH),
                bounding_limit_counter_id: self.leader_commit_position_counter_id,
                ..ReplayParams::default()
            };

            let Some(archive) = self.cluster_archive.as_mut() else {
                return Ok(work);
            };
            // **Blocking**, unlike the reference's bounded replay + poller: see
            // the module note. Answers the replay session id the reference reads
            // out of its control response.
            self.live_log_replay_session_id = archive.start_replay(
                &mut self.client,
                self.cluster_log_recording_id,
                &replay_channel,
                self.ctx.config().log_stream_id,
                &params,
            )?;
            // `correlationId = replayId` (`ClusterBackupAgent.java:848`):
            // a non-null correlation is what says "the replay has been asked
            // for". This build's `start_replay` is blocking and burns its own
            // correlation, so the session id is the marker; without it the
            // next turn sees a null correlation and asks again, and the
            // replay channel it no longer holds is the error that stops the
            // duty cycle.
            self.correlation_id = self.live_log_replay_session_id;
            self.replay_channel = None;
            self.time_of_last_progress_ms = now_ms;
            work += 1;
        }

        if NULL_COUNTER_ID == self.live_log_recording_counter_id {
            let found = {
                let counters = self.client.counters_reader();
                let archive_id = self.backup_archive.as_ref().map(Archive::archive_id);
                match (counters, archive_id) {
                    (Some(counters), Some(archive_id)) => {
                        let session_id = self.live_log_replay_session_id as i32;
                        deepmsg_archive::server::recording_pos::find_counter_id_by_session(
                            &counters, session_id, archive_id,
                        )
                        .and_then(|counter_id| {
                            let position = counters.value(counter_id)?;
                            let recording_id = counters
                                .key(counter_id)
                                .and_then(|key| {
                                    deepmsg_archive::server::recording_pos::parse_key(&key)
                                })
                                .map(|identity| identity.recording_id)?;
                            Some((counter_id, position, recording_id))
                        })
                    }
                    _ => None,
                }
            };

            match found {
                Some((counter_id, position, recording_id)) => {
                    write_counter(&self.client, self.counters.live_log_position, position);
                    self.live_log_recording_counter_id = counter_id;
                    self.live_log_recording_id = recording_id;
                    self.time_of_last_backup_query_ms = now_ms;
                    self.time_of_last_progress_ms = now_ms;
                    self.set_state(State::UpdateRecordingLog);
                    work += 1;
                }
                None => {
                    // `:884-897`: a replay that failed says so on the control
                    // channel even though no request is outstanding.
                    let error = match self.cluster_archive.as_mut() {
                        Some(archive) => archive.check_for_error_response(&mut self.client).err(),
                        None => None,
                    };
                    if let Some(error) = error {
                        return Err(ClusterError::Cluster(format!(
                            "Live log replay failed (replaySessionId={}): {error}",
                            self.live_log_replay_session_id
                        )));
                    }
                }
            }
        }

        Ok(work)
    }

    // -- UPDATE_RECORDING_LOG ------------------------------------------------

    /// `updateRecordingLog` (`:902-971`): write what the answer said into the
    /// local recording log, so a consensus module can start from this
    /// directory.
    fn update_recording_log(&mut self, now_ms: i64) -> Result<usize, ClusterError> {
        // The id the entries carry is the **backup's own** recording of the
        // replayed log (`liveLogRecordingId`, `ClusterBackupAgent.java:929`,
        // `:959`), not the cluster's: this recording log is what a member
        // started from the backup directory will replay, and a cluster
        // recording id names nothing in this archive.
        let recording_id = self.live_log_recording_id;
        let Some(recording_log) = self.recording_log.as_mut() else {
            return Err(ClusterError::Cluster(
                "the backup's recording log is not open".to_owned(),
            ));
        };

        let mut updated = false;
        let snapshot_term = self
            .snapshots_retrieved
            .first()
            .map_or(NULL_VALUE, |snapshot| snapshot.leadership_term_id);

        if let Some(entry) = self.leader_log_entry {
            if recording_log.is_unknown(entry.leadership_term_id)
                && entry.leadership_term_id <= snapshot_term
            {
                recording_log
                    .append_term_entry(
                        recording_id,
                        entry.leadership_term_id,
                        entry.term_base_log_position,
                        NULL_VALUE,
                    )
                    .map_err(|error| {
                        ClusterError::Cluster(format!("the recording log: {error}"))
                    })?;
                updated = true;
                self.leader_log_entry = None;
            }
        }

        if !self.snapshots_retrieved.is_empty() {
            for snapshot in self.snapshots_retrieved.iter().rev() {
                recording_log
                    .append_snapshot(
                        snapshot.leadership_term_id,
                        snapshot.term_base_log_position,
                        snapshot.log_position,
                        snapshot.timestamp,
                        snapshot.recording_id,
                        snapshot.service_id,
                    )
                    .map_err(|error| {
                        ClusterError::Cluster(format!("the recording log: {error}"))
                    })?;
            }
            updated = true;
        }

        if let Some(entry) = self.leader_last_term_entry {
            if recording_log.is_unknown(entry.leadership_term_id) {
                recording_log
                    .append_term_entry(
                        recording_id,
                        entry.leadership_term_id,
                        entry.term_base_log_position,
                        NULL_VALUE,
                    )
                    .map_err(|error| {
                        ClusterError::Cluster(format!("the recording log: {error}"))
                    })?;
                updated = true;
                self.leader_last_term_entry = None;
            }
        }

        if updated {
            recording_log.force(2).map_err(|error| {
                ClusterError::Cluster(format!("the recording log will not sync: {error}"))
            })?;

            if !self.snapshots_retrieved.is_empty() {
                let counter = self.counters.snapshots_retrieved;
                let value = read_counter(&self.client, counter).unwrap_or(0) + 1;
                write_counter(&self.client, counter, value);
            }

            if let Some(listener) = self.events_listener.as_mut() {
                let log = self.recording_log.as_ref().expect("just used");
                listener.on_updated_recording_log(log, &self.snapshots_retrieved);
            }
        }

        self.snapshots_retrieved.clear();
        self.snapshots_to_retrieve.clear();

        self.time_of_last_progress_ms = now_ms;
        write_counter(
            &self.client,
            self.counters.next_query_deadline_ms,
            now_ms + self.query_interval_ms(),
        );
        self.set_state(State::BackingUp);

        Ok(1)
    }

    // -- BACKING_UP ----------------------------------------------------------

    /// `backingUp` (`:973-1008`): until the next query deadline, report how far
    /// the local recording of the live log has got.
    fn backing_up(&mut self, now_ms: i64) -> Result<usize, ClusterError> {
        let mut work = 0;

        let deadline =
            read_counter(&self.client, self.counters.next_query_deadline_ms).unwrap_or(NULL_VALUE);
        if now_ms > deadline {
            self.time_of_last_backup_query_ms = now_ms;
            self.time_of_last_progress_ms = now_ms;
            self.set_state(State::BackupQuery);
            work += 1;
        }

        if NULL_COUNTER_ID != self.live_log_recording_counter_id {
            let position = self
                .client
                .counters_reader()
                .and_then(|counters| counters.value(self.live_log_recording_counter_id));

            if let Some(position) = position {
                let moved = self
                    .client
                    .counters_writable()
                    .and_then(|counters| {
                        self.counters
                            .live_log_position
                            .propose_max(&counters, position)
                    })
                    .unwrap_or(false);

                if moved {
                    if let Some(listener) = self.events_listener.as_mut() {
                        listener.on_live_log_progress(
                            self.live_log_recording_id,
                            self.live_log_recording_counter_id,
                            position,
                        );
                    }
                    work += 1;
                }
            }
        }

        Ok(work)
    }

    // -- RESET_BACKUP --------------------------------------------------------

    /// `resetBackup` (`:632-657`) and `reset` (`:302-334`).
    fn reset_backup(&mut self, now_ms: i64) -> Result<usize, ClusterError> {
        self.time_of_last_progress_ms = now_ms;

        if NULL_VALUE == self.cool_down_deadline_ms {
            self.cool_down_deadline_ms = now_ms + self.cool_down_interval_ms();
            self.reset();
            return Ok(1);
        }

        if now_ms > self.cool_down_deadline_ms {
            self.cool_down_deadline_ms = NULL_VALUE;
            self.set_state(State::BackupQuery);
            return Ok(1);
        }

        Ok(0)
    }

    /// `reset` (`:302-334`): everything the last attempt built, except the
    /// recording log and the local archive.
    fn reset(&mut self) {
        self.cluster_members = None;
        self.log_supplier_member = None;
        self.leader_log_entry = None;
        self.leader_last_term_entry = None;
        self.live_log_recording_counter_id = NULL_COUNTER_ID;
        self.live_log_recording_id = NULL_VALUE;
        self.live_log_recording_session_id = NULL_VALUE;

        self.snapshots_to_retrieve.clear();
        self.snapshots_retrieved.clear();

        if let Some(mut replication) = self.snapshot_replication.take() {
            if let Some(archive) = self.backup_archive.as_mut() {
                let mut archive_client = ArchiveClient {
                    client: &mut self.client,
                    archive,
                };
                replication.close(&mut archive_client);
            }
        }

        self.stop_recording();
        self.stop_replay();

        self.consensus_publication_group.close(&mut self.client);
        self.drop_cluster_archive();

        if NULL_VALUE != self.recording_subscription_id {
            let _ = self
                .client
                .remove_subscription(self.recording_subscription_id, COMMAND_TIMEOUT);
            self.recording_subscription_id = NULL_VALUE;
        }
    }

    fn drop_cluster_archive(&mut self) {
        self.cluster_archive_async_connect = None;
        self.cluster_archive_connect_context = None;
        if let Some(mut archive) = self.cluster_archive.take() {
            archive.close(&self.client);
        }
    }

    /// `stopReplay` (`:336-352`).
    fn stop_replay(&mut self) {
        if NULL_VALUE != self.live_log_replay_session_id {
            let replay_session_id = self.live_log_replay_session_id;
            let error = match self.cluster_archive.as_mut() {
                Some(archive) => archive
                    .stop_replay(&mut self.client, replay_session_id)
                    .err()
                    .map(|error| {
                        ClusterError::Cluster(format!("failed to stop log replay: {error}"))
                    }),
                None => None,
            };
            if let Some(error) = error {
                self.count_error(&error);
            }
            self.live_log_replay_session_id = NULL_VALUE;
        }
    }

    /// `stopRecording` (`:354-368`).
    fn stop_recording(&mut self) {
        if NULL_VALUE != self.live_log_recording_subscription_id {
            let subscription_id = self.live_log_recording_subscription_id;
            let error = match self.backup_archive.as_mut() {
                Some(archive) => archive
                    .try_stop_recording_subscription(&mut self.client, subscription_id)
                    .err()
                    .map(|error| {
                        ClusterError::Cluster(format!("failed to stop log recording: {error}"))
                    }),
                None => None,
            };
            if let Some(error) = error {
                self.count_error(&error);
            }
            self.live_log_recording_subscription_id = NULL_VALUE;
        }
    }

    // -- the plumbing the states share --------------------------------------

    /// `state(newState, nowMs)` (`:1010-1032`).
    fn set_state(&mut self, new_state: State) {
        if State::BackupQuery == new_state {
            if let Some(listener) = self.events_listener.as_mut() {
                listener.on_backup_query();
            }
        }

        write_counter(&self.client, self.counters.state, new_state.code());
        self.state = new_state;
        self.correlation_id = NULL_VALUE;
    }

    /// `hasProgressStalled` (`:1147-1151`).
    fn has_progress_stalled(&self, now_ms: i64) -> bool {
        NULL_COUNTER_ID == self.live_log_recording_counter_id
            && now_ms > self.time_of_last_progress_ms + self.progress_timeout_ms()
    }

    fn response_timeout_ms(&self) -> i64 {
        self.ctx.config().response_timeout_ns / 1_000_000
    }

    fn progress_timeout_ms(&self) -> i64 {
        self.ctx.config().progress_timeout_ns / 1_000_000
    }

    fn query_interval_ms(&self) -> i64 {
        self.ctx.config().backup_interval_ns / 1_000_000
    }

    fn cool_down_interval_ms(&self) -> i64 {
        self.ctx.config().cool_down_interval_ns / 1_000_000
    }

    /// `countedErrorHandler().onError` — counter 211, the mark file's distinct
    /// error log, then the caller's handler (`ClusterBackup.java:804`).
    fn count_error(&mut self, error: &ClusterError) {
        let current = read_counter(&self.client, self.counters.errors).unwrap_or(0);
        write_counter(&self.client, self.counters.errors, current + 1);

        if let Some(mark_file) = self.ctx.mark_file() {
            if let Some(buffer) = mark_file.error_buffer() {
                let region = ErrorLogRegion::new(buffer);
                let now_ms = deepmsg_core::clock::epoch_millis();
                let _ = self
                    .counted_error_log
                    .record(&region, now_ms, 0, &error.to_string());
            }
        }

        if let Some(handler) = self.error_handler.as_mut() {
            handler(error);
        }
    }

    fn possible_failure(&mut self, error: &ClusterError) {
        if let Some(listener) = self.events_listener.as_mut() {
            listener.on_possible_failure(error);
        }
    }

    /// `runTerminationHooks` (`:1169-1191`): the extended hook first, with the
    /// error, then the plain one.
    fn run_termination_hooks(&mut self, error: &ClusterError) {
        run_termination_hooks(
            &mut *self.extended_termination_hook,
            &mut *self.termination_hook,
            error,
        );
    }
}

/// `replayStartPosition` (`:1153-1167`) over anything that can answer a
/// recording's stop position — the archive in the agent, a closure in a test.
///
/// # Errors
///
/// The stop position's own failure.
fn replay_start_position<F>(
    last_term: Option<&crate::persist::recording_log::Entry>,
    snapshots_retrieved: &[Snapshot],
    replay_start: ReplayStart,
    get_stop_position: F,
) -> Result<i64, ClusterError>
where
    F: FnOnce(i64) -> Result<i64, ClusterError>,
{
    if let Some(entry) = last_term {
        return get_stop_position(entry.recording_id);
    }

    if ReplayStart::Beginning == replay_start {
        return Ok(NULL_POSITION);
    }

    let mut position = NULL_POSITION;
    for snapshot in snapshots_retrieved {
        if CONSENSUS_MODULE_SERVICE_ID == snapshot.service_id && position < snapshot.log_position {
            position = snapshot.log_position;
        }
    }

    Ok(position)
}

/// The reference's default local archive control channel
/// (`AeronArchive.Configuration.LOCAL_CONTROL_CHANNEL_DEFAULT`).
fn default_local_archive_context(
    config: &crate::backup::context::ClusterBackupConfig,
) -> ArchiveContext {
    let mut context = ArchiveContext::new();
    context.aeron_dir = config.aeron_dir.to_string_lossy().into_owned();
    context.client_name = format!("cluster-backup clusterId={}", config.cluster_id);
    context.control_request_channel = Some("aeron:ipc?term-length=64k".to_owned());
    context.control_response_channel = Some("aeron:ipc?term-length=64k".to_owned());
    context
}

/// The cluster archive's control channel **templates**: the shape, with the
/// member's endpoint laid over it when an answer names one.
fn default_cluster_archive_context(
    config: &crate::backup::context::ClusterBackupConfig,
) -> ArchiveContext {
    let mut context = ArchiveContext::new();
    context.aeron_dir = config.aeron_dir.to_string_lossy().into_owned();
    context.client_name = format!("cluster-backup clusterId={}", config.cluster_id);
    context.control_request_channel = Some("aeron:udp".to_owned());
    context.control_response_channel = Some("aeron:udp?endpoint=localhost:0".to_owned());
    context
}

fn read_counter(client: &Client, counter: Counter) -> Option<i64> {
    client
        .counters_reader()
        .and_then(|counters| counters.value(counter.counter_id()))
}

fn write_counter(client: &Client, counter: Counter, value: i64) {
    if let Some(counters) = client.counters_writable() {
        let _ = counter.set_value(&counters, value);
    }
}

/// Decode one fragment off the consensus subscription (`onFragment`, `:368-427`).
fn decode_backup_message(payload: &[u8]) -> Option<BackupMessage> {
    if payload.len() < deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH {
        return None;
    }

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
    if header.schema_id() != SBE_SCHEMA_ID {
        return None;
    }

    match header.template_id() {
        deepmsg_codec_cluster::backup_response_codec::SBE_TEMPLATE_ID => Some(
            BackupMessage::Response(decode_backup_response(payload, header)),
        ),
        deepmsg_codec_cluster::challenge_codec::SBE_TEMPLATE_ID => {
            let mut decoder = ChallengeDecoder::default().header(header, 0);
            let cluster_session_id = decoder.cluster_session_id();
            let coordinates = decoder.encoded_challenge_decoder();
            let encoded_challenge = decoder.encoded_challenge_slice(coordinates).to_vec();
            Some(BackupMessage::Challenge {
                cluster_session_id,
                encoded_challenge,
            })
        }
        deepmsg_codec_cluster::session_event_codec::SBE_TEMPLATE_ID => {
            let mut decoder = SessionEventDecoder::default().header(header, 0);
            let correlation_id = decoder.correlation_id();
            let event_code = decoder.code();
            let coordinates = decoder.detail_decoder();
            let detail = String::from_utf8_lossy(decoder.detail_slice(coordinates)).into_owned();
            Some(BackupMessage::SessionEvent {
                correlation_id,
                event_code,
                detail,
            })
        }
        _ => None,
    }
}

fn decode_backup_response(
    payload: &[u8],
    header: MessageHeaderDecoder<ReadBuf<'_>>,
) -> BackupResponseMessage {
    let decoder = BackupResponseDecoder::default().header(header, 0);
    let member_id = decoder.member_id();
    // `BackupResponseEncoder.memberIdNullValue()`: an SBE `int` null.
    let member_id = if member_id == i32::MIN {
        None
    } else {
        Some(member_id)
    };

    let mut snapshots = Vec::new();
    let mut group = decoder.snapshots_decoder();
    while group
        .advance()
        .expect("the snapshots group reads")
        .is_some()
    {
        snapshots.push(Snapshot {
            recording_id: group.recording_id(),
            leadership_term_id: group.leadership_term_id(),
            term_base_log_position: group.term_base_log_position(),
            log_position: group.log_position(),
            timestamp: group.timestamp(),
            service_id: group.service_id(),
        });
    }
    let mut decoder = group.parent().expect("the group has a parent");
    let coordinates = decoder.cluster_members_decoder();
    let cluster_members =
        String::from_utf8_lossy(decoder.cluster_members_slice(coordinates)).into_owned();

    let _ = payload;
    BackupResponseMessage {
        correlation_id: decoder.correlation_id(),
        log_recording_id: decoder.log_recording_id(),
        log_leadership_term_id: decoder.log_leadership_term_id(),
        log_term_base_log_position: decoder.log_term_base_log_position(),
        last_leadership_term_id: decoder.last_leadership_term_id(),
        last_term_base_log_position: decoder.last_term_base_log_position(),
        commit_position_counter_id: decoder.commit_position_counter_id(),
        leader_member_id: decoder.leader_member_id(),
        member_id,
        snapshots,
        cluster_members,
    }
}

/// The order [`ClusterBackupAgent::run_termination_hooks`] runs in, as a free
/// function so a test can watch both hooks without an Aeron client.
fn run_termination_hooks(
    extended: &mut dyn FnMut(&ClusterError),
    hook: &mut dyn FnMut(),
    error: &ClusterError,
) {
    extended(error);
    hook();
}

#[cfg(test)]
mod tests {
    //! `ClusterBackupAgentTest` (`ClusterBackupAgentTest.java:25-110`).

    use super::*;
    use crate::persist::recording_log::Entry;

    fn term_entry(recording_id: i64) -> Entry {
        Entry {
            position: 0,
            recording_id,
            leadership_term_id: 0,
            term_base_log_position: 0,
            log_position: -1,
            timestamp: 0,
            service_id: CONSENSUS_MODULE_SERVICE_ID,
            entry_type: crate::persist::recording_log::ENTRY_TYPE_TERM,
            is_valid: true,
            archive_endpoint: None,
            entry_index: 0,
        }
    }

    fn snapshot(log_position: i64, service_id: i32) -> Snapshot {
        Snapshot {
            recording_id: 1,
            leadership_term_id: 0,
            term_base_log_position: 0,
            log_position,
            timestamp: 0,
            service_id,
        }
    }

    /// `shouldReturnReplayStartPositionIfAlreadyExisting` (`:47-61`): a log
    /// that already has a term continues from where its recording stopped.
    #[test]
    fn should_return_replay_start_position_if_already_existing() {
        let expected = 892_374;
        let last_term = term_entry(234);

        let position = replay_start_position(
            Some(&last_term),
            &[],
            ReplayStart::Beginning,
            |recording_id| {
                assert_eq!(234, recording_id);
                Ok(expected)
            },
        )
        .expect("a stop position");

        assert_eq!(expected, position);
    }

    /// `shouldReturnNullPositionIfLastTermIsNullAndSnapshotsIsEmpty`
    /// (`:63-69`).
    #[test]
    fn should_return_null_position_if_last_term_is_null_and_snapshots_is_empty() {
        let position = replay_start_position(None, &[], ReplayStart::Beginning, |_| {
            panic!("no archive is asked without a last term")
        })
        .expect("a start position");

        assert_eq!(NULL_POSITION, position);
    }

    /// `shouldLargestPositionLessThanOrEqualToInitialReplayPosition`
    /// (`:71-90`): from the latest snapshot means the consensus module's
    /// largest position, and nothing at all means null.
    #[test]
    fn should_take_the_largest_consensus_module_snapshot_position() {
        let snapshots: Vec<Snapshot> = (1..=6)
            .map(|index| snapshot(index * 1000, CONSENSUS_MODULE_SERVICE_ID))
            .collect();
        // A service's snapshot at a larger position is not the answer: the
        // reference filters on `ConsensusModule.Configuration.SERVICE_ID`
        // (`:1160-1165`).
        let mut with_service = snapshots.clone();
        with_service.push(snapshot(7000, 0));

        let beginning = replay_start_position(None, &with_service, ReplayStart::Beginning, |_| {
            panic!("BEGINNING does not ask the archive")
        })
        .expect("a start position");
        assert_eq!(NULL_POSITION, beginning);

        let latest =
            replay_start_position(None, &with_service, ReplayStart::LatestSnapshot, |_| {
                panic!("the snapshots answer, not the archive")
            })
            .expect("a start position");
        assert_eq!(6000, latest);
    }

    /// `shouldRunTerminationHooksOnTermination` (`:92-109`): the extended hook
    /// runs first, with the error, then the plain one.
    #[test]
    fn should_run_termination_hooks_on_termination() {
        let order = std::cell::RefCell::new(Vec::new());

        let mut extended = |error: &ClusterError| {
            order.borrow_mut().push(format!("extended: {error:?}"));
        };
        let mut hook = || order.borrow_mut().push("hook".to_owned());

        let error = ClusterError::AgentTermination("unexpected Aeron close".to_owned());
        run_termination_hooks(&mut extended, &mut hook, &error);

        assert_eq!(
            vec![format!("extended: {error:?}"), "hook".to_owned()],
            *order.borrow()
        );
    }
}
