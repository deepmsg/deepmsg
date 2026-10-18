//! The consensus module agent: startup, the duty cycle, and the election host.
//!
//! Counterpart of `ConsensusModuleAgent` (`ConsensusModuleAgent.java`). P3-4a
//! builds the skeleton the card asks for:
//!
//! * `launch` — the reference's `Context.conclude` + `onStart` ordering: the
//!   mark file, `node-state.dat`, `recording.log`, the archive connection, the
//!   recovery plan, the counters, and the first election step.
//! * `do_work` — `ConsensusModuleAgent.doWork` (`:403-478`): the slow tick, the
//!   election (taken out of `self`, plan §4.5), and the steady state.
//! * the `ElectionHost` and `ConsensusSender` seams, so the election the
//!   previous slices built can actually run against a node.
//!
//! Everything that needs a session, an ingress image, a service or a snapshot
//! is a visible refusal naming the slice that brings it, not a silent no-op.
//! The one exception is the timer service: `consensus_work` treats an empty
//! timer table as the correct state, because `P3-4a`'s node has no timers and
//! `P3-4c` replaces the empty table with the wheel.
//!
//! # The field-splitting table (plan §4.5, public DoD 13)
//!
//! The agent is the **sole owner** of every component; nothing here is
//! `Rc<RefCell<_>>`, and no component points back at the agent. The borrows that
//! would otherwise alias are handled in three ways, and the table is frozen at
//! this card:
//!
//! | Interaction | Borrow shape | Why it is sound |
//! |---|---|---|
//! | election → host | `election` is **taken out** of `self.election`, then `e.do_work(self, &mut publisher)` | the election is not borrowed through `self` while `self` is `&mut`; on return it is dropped (Closed) or put back |
//! | host → sender | the publisher is taken out with `mem::take` for the same call | two disjoint `&mut`; the reference passes the same two objects, Rust cannot express that without the take-out |
//! | archive calls during a wait | `idle()` borrows `&mut client` only, never the archive, the log publisher or the election | a wait drives the client (plan §4.4 rule 1) and does no other work |
//! | log publication | **owned by `LogPublisher`, taken out of the client** | `P3-4b`'s ingress poll will borrow `&mut Client`; a publication left in the client's table could not be written in the same turn |
//! | counters | values are `Copy` handles; writes go through `client.counters_writable()` | the counter handle is not borrow-carrying, so writing one cannot alias a `&mut` field |
//!
//! There is no `Cluster<'_>`-style view in the agent: every method that runs a
//! callback takes the disjoint pieces it needs as arguments, and the only
//! callback that can re-enter the agent (`idle` from a wait) is a method, not a
//! user closure.

pub mod publisher;

use std::mem;
use std::time::Duration;

use deepmsg_archive::client::Handlers;
use deepmsg_archive::client::archive::Archive;
use deepmsg_archive::client::async_connect::NoCredentials;
use deepmsg_archive::client::descriptor_poller::RecordingDescriptor;
use deepmsg_archive::server::recording_pos::{find_counter_id_by_session, parse_key};
use deepmsg_client::client::Client;
use deepmsg_client::counter::Counter;
use deepmsg_client::publication::ExclusivePublication;
use deepmsg_cluster_common::counters::{
    self, CLUSTER_COMMIT_POSITION_TYPE_ID, CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID,
    CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID, CLUSTER_CONTROL_TOGGLE_TYPE_ID,
    CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID, CLUSTER_ELECTION_COUNT_TYPE_ID,
    CLUSTER_ELECTION_STATE_TYPE_ID, CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID,
    CLUSTER_MAX_CYCLE_TIME_TYPE_ID, CLUSTER_NODE_ROLE_TYPE_ID, CLUSTER_SNAPSHOT_COUNTER_TYPE_ID,
    CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID,
    CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID, NODE_CONTROL_TOGGLE_TYPE_ID,
};
use deepmsg_cluster_common::mark::{self, ClusterMarkFile};
use deepmsg_cluster_common::member::{self, ClusterMember};
use deepmsg_codec_cluster::admin_request_type::AdminRequestType;
use deepmsg_codec_cluster::admin_response_code::AdminResponseCode;
use deepmsg_codec_cluster::cluster_action::ClusterAction;
use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;
use deepmsg_codec_cluster_mark::cluster_component_type::ClusterComponentType;
use deepmsg_core::clock;
use deepmsg_core::uri::ChannelUri;
use deepmsg_driver::idle::Strategy;

use crate::clock::{ClusterClock, MillisecondClusterClock};
use crate::config::{CLUSTER_IDLE_STRATEGY_PROP_NAME, CLUSTER_TIME_UNIT, ConsensusModuleConfig};
use crate::consensus::{ConsensusMessage, decode_consensus_message};
use crate::control::cluster_control::ToggleState as ClusterControlToggle;
use crate::control::node_control::ToggleState as NodeControlToggle;
use crate::control::termination::ClusterTermination;
use crate::election::{APPEND_POSITION_FLAG_CATCHUP, Election, ElectionHost, ElectionState};
use crate::error::ClusterError;
use crate::handles::{LogReplay, RecordingReplication};
use crate::log::{AgentLogReplay, LogReplayHandler};
use crate::log_publisher::LogPublisher;
use crate::persist::node_state::NodeStateFile;
use crate::persist::recording_log::{self, RecordingExtent, RecordingLog, RecoveryPlan};
use crate::service_control::ack::{
    NULL_VALUE as ACK_NULL_VALUE, ServiceAck, ServiceAckQueues, has_reached, new_array_of_queues,
    remove_head,
};
use crate::service_control::adapter::{ServiceControlAdapter, ServiceControlHandler};
use crate::service_control::proxy::{ServiceProxy, ServiceSend};
use crate::service_control::tracker::{
    PendingServiceMessageTracker, service_id_from_service_message,
};
use crate::session::authenticator::{
    Authenticator, AuthorisationService, authenticator_for, authorisation_service_for,
};
use crate::session::cluster_session::{ClusterSession, SessionEgress};
use crate::session::egress::EgressPublisher;
use crate::session::ingress::{IngressAdapter, IngressHandler};
use crate::session::manager::{SessionConnectRequest, SessionContext, SessionManager};
use crate::snapshot::taker::{SnapshotAppend, SnapshotIdle, SnapshotSend};
use crate::snapshot::{ConsensusModuleSnapshotAdapter, SnapshotLoadHandler};
use crate::state::{ModuleState, Role};
use crate::timer::priority_heap::PriorityHeapTimerService;
use crate::timer::wheel::WheelTimerService;
use crate::timer::{
    TimerHandler, TimerService, TimerServiceKind, timer_service_kind, wheel_tick_resolution,
};
use deepmsg_core::logbuffer::append::Appended;
use std::sync::atomic::{AtomicBool, AtomicI64};

use publisher::ConsensusPublisher;

/// `ConsensusModuleAgent.SLOW_TICK_INTERVAL_NS` = 10 ms (`:124`).
pub const SLOW_TICK_INTERVAL_NS: i64 = 10_000_000;

/// `MARK_FILE_UPDATE_INTERVAL_NS` = 1 s
/// (`ClusteredServiceContainer.java:200`).
pub const MARK_FILE_UPDATE_INTERVAL_NS: i64 = 1_000_000_000;

/// How long a synchronous startup call waits for the driver.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// `ConsensusModule.Configuration.CLUSTER_INGRESS_FRAGMENT_LIMIT_DEFAULT` is 50;
/// the consensus adapter polls with the same bound.
const CONSENSUS_FRAGMENT_LIMIT: usize = 50;

/// The archive's answer for "no position".
const NULL_POSITION: i64 = -1;

/// `Aeron.NULL_VALUE`, and the `-1` a recording id takes when there is none.
const NULL_VALUE: i64 = -1;

/// `ConsensusModule.CLUSTER_ACTION_FLAGS_DEFAULT` (`:132`).
const CLUSTER_ACTION_FLAGS_DEFAULT: i32 = 0;

/// The counters a consensus module allocates (`ConsensusModule.java:1892-2046`).
///
/// They are `Copy` handles: the value lives in the CnC file, and every read and
/// write goes through a [`deepmsg_cnc::CountersReader`] built from the client.
struct ClusterCounters {
    error: Counter,
    module_state: Counter,
    election_state: Counter,
    election: Counter,
    leadership_term: Counter,
    node_role: Counter,
    commit_position: Counter,
    control_toggle: Counter,
    node_control_toggle: Counter,
    snapshot: Counter,
    timed_out_client: Counter,
    max_cycle_time: Counter,
    cycle_threshold_exceeded: Counter,
    total_snapshot_duration: Counter,
    total_snapshot_threshold_exceeded: Counter,
}

/// The append-position counter of the local archive recording.
///
/// The reference holds a `ReadableCounter` (`ConsensusModuleAgent.java:156`);
/// this is the same two numbers, read through the client's counters region.
#[derive(Clone, Copy, Debug)]
struct AppendPosition {
    counter_id: i32,
}

/// A cluster's duty-cycle tracker (`DutyCycleStallTracker`).
///
/// Two counters: the maximum cycle time seen, and how many cycles exceeded the
/// configured threshold (`:2020-2034`). The reference's tracker also keeps the
/// threshold; so does this.
struct DutyCycle {
    max_cycle_time: Counter,
    threshold_exceeded: Counter,
    threshold_ns: i64,
    cycle_start_ns: i64,
}

impl DutyCycle {
    fn measure(&mut self, client: &Client, now_ns: i64) {
        let duration_ns = now_ns.saturating_sub(self.cycle_start_ns);
        self.cycle_start_ns = now_ns;

        if duration_ns > read_counter(client, self.max_cycle_time).unwrap_or(0) {
            write_counter(client, self.max_cycle_time, duration_ns);
        }
        if duration_ns > self.threshold_ns {
            let exceeded = read_counter(client, self.threshold_exceeded).unwrap_or(0);
            write_counter(client, self.threshold_exceeded, exceeded + 1);
        }
    }
}

/// `SnapshotDurationTracker` (`service/SnapshotDurationTracker.java:25-97`):
/// the longest snapshot taken, and how many exceeded the configured threshold.
///
/// The reference's counter is named "total" but holds the **maximum**
/// (`proposeMax`, `:95`); the value is nanoseconds.
struct SnapshotDuration {
    max_duration: Counter,
    threshold_exceeded: Counter,
    threshold_ns: i64,
    /// `snapshotStartTimeNs`; `None` is the reference's `Long.MIN_VALUE`.
    start_ns: Option<i64>,
}

impl SnapshotDuration {
    /// `onSnapshotBegin` (`:74-77`).
    fn on_snapshot_begin(&mut self, time_ns: i64) {
        self.start_ns = Some(time_ns);
    }

    /// `onSnapshotEnd` (`:84-97`): propose the max, count a threshold breach.
    fn on_snapshot_end(&mut self, client: &Client, time_ns: i64) {
        let Some(start_ns) = self.start_ns else {
            return;
        };

        let duration_ns = time_ns.saturating_sub(start_ns);
        if duration_ns > self.threshold_ns {
            increment_counter(client, self.threshold_exceeded, 1);
        }
        if duration_ns > read_counter(client, self.max_duration).unwrap_or(0) {
            write_counter(client, self.max_duration, duration_ns);
        }
    }
}

/// One consensus module, and everything it owns.
pub struct ConsensusModuleAgent {
    config: ConsensusModuleConfig,
    clock: MillisecondClusterClock,
    idle_strategy: Strategy,
    client: Client,
    archive: Archive,
    recording_log: RecordingLog,
    node_state: NodeStateFile,
    mark_file: ClusterMarkFile,
    recovery_plan: RecoveryPlan,
    counters: ClusterCounters,
    duty_cycle: DutyCycle,

    members: Vec<ClusterMember>,
    this_member_index: usize,
    ranked_positions: Vec<i64>,
    publisher: ConsensusPublisher,
    log_publisher: LogPublisher,
    log_publication_channel_tag: i64,
    log_publication_session_id: i32,
    log_recording_id: i64,
    log_recording_stop_position: i64,
    log_subscription_id: i64,
    /// The channel of the manual-MDC subscription a follower joins the log
    /// through. `try_join_log_as_follower` records that channel, and the
    /// election's host accessors do not hand it back.
    log_subscription_channel: String,
    /// An image the election asked for and `try_join_log_as_follower` has not
    /// consumed yet. The reference hands the same `Image` object out every duty
    /// cycle; here the image is taken out of the subscription once
    /// (`Client::take_image`) and kept here until an adapter owns it.
    taken_log_image: Option<deepmsg_client::image::Image>,
    append_position: Option<AppendPosition>,

    state: ModuleState,
    role: Role,
    election: Option<Election>,
    /// Whether the current (first) election is the node's startup one
    /// (`Election.isNodeStartup`): sessions recovered from a snapshot are
    /// closed for TIMEOUT at the end of it (`prepareSessionsForNewTerm`).
    node_startup_election: bool,
    leadership_term_id: i64,

    last_append_position: i64,
    /// `timeOfLastAppendPositionSendNs` (`:152`): the follower's heartbeat.
    time_of_last_append_position_send_ns: i64,
    last_quorum_backtrack_commit_position: i64,
    time_of_last_log_update_ns: i64,
    time_of_last_leader_update_ns: i64,
    notified_commit_position: i64,
    slow_tick_deadline_ns: i64,
    mark_file_update_deadline_ns: i64,
    /// `ClusterMember.ingressEndpoints(activeMembers)` (`:2380`): the
    /// `id=endpoint,...` string a redirect, an admin response and a new session
    /// all carry. The members are fixed for the node's life, so it is made
    /// once — a steady-state duty cycle must not allocate (performance spec R1).
    ingress_endpoints: String,

    consensus_subscription_id: i64,
    /// Decoded consensus messages for one poll, reused so a poll that carries
    /// traffic does not allocate per message.
    consensus_messages: Vec<ConsensusMessage>,
    sessions: SessionManager,
    egress: EgressPublisher,
    ingress: IngressAdapter,
    service_proxy: ServiceProxy,
    service_publication_id: i64,
    service_control: ServiceControlAdapter,
    trackers: Vec<PendingServiceMessageTracker>,
    service_ack_queues: ServiceAckQueues,
    service_ack_id: i64,
    expected_ack_position: i64,
    service_client_ids: Vec<i64>,
    timer: Box<dyn TimerService>,
    log_replay: Option<AgentLogReplay>,
    uncommitted_timers: std::collections::VecDeque<(i64, i64)>,
    expired_timer_count: deepmsg_cluster_common::collections::HashMap<i64, i64>,
    live_log_destination: String,
    catchup_log_destination: String,
    closed: bool,

    // -- termination and the operator toggles (P3-4e) ----------------------
    /// `clusterTermination` (`:198`): `Some` while this leader is waiting for
    /// its services and its followers (`SHUTDOWN` and `ABORT`).
    cluster_termination: Option<ClusterTermination>,
    /// `terminationPosition` (`:134`): the log position termination stops at.
    termination_position: i64,
    /// `terminationLeadershipTermId` (`:135`): the term that position belongs
    /// to, so a stale `TerminationAck` cannot end the current one.
    termination_leadership_term_id: i64,
    /// `uncommittedPreviousState` (`:182`): the state before each
    /// `SUSPEND`/`RESUME`/`SNAPSHOT` action, written by
    /// `offerPositionAndPreviousState` (`:2664-2668`) and rolled back by
    /// `restoreUncommittedEntries` (`:3020-3057`) when the action turns out not
    /// to be committed.
    uncommitted_previous_state: std::collections::VecDeque<(i64, i64)>,
    /// `totalSnapshotDurationTracker` (`:191`), counters 234 and 235.
    snapshot_duration: SnapshotDuration,
}

/// The two objects a deployment names by class and a test supplies in process
/// (`ConsensusModule.Context.authenticatorSupplier` and
/// `authorisationServiceSupplier`, `ConsensusModule.java:1683-1690`).
#[derive(Default)]
pub struct LaunchComponents {
    /// `None` means the name in `aeron.cluster.authenticator.supplier`.
    pub authenticator: Option<Box<dyn Authenticator>>,
    /// `None` means the name in `aeron.cluster.authorisation.service.supplier`.
    pub authorisation_service: Option<Box<dyn AuthorisationService>>,
}

impl ConsensusModuleAgent {
    /// Start a consensus module: the reference's `Context.conclude()` followed by
    /// `onStart` (`ConsensusModuleAgent.java:328-401`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from any of the opening steps, or a refusal naming the
    /// slice that a configuration asks for and this one does not have (more than
    /// one member, services, a custom clock).
    pub fn launch(config: ConsensusModuleConfig) -> Result<Self, ClusterError> {
        Self::launch_with_components(config, LaunchComponents::default())
    }

    /// The same, with the two security objects supplied in process — the
    /// equivalent of the reference's `Context.authenticatorSupplier` /
    /// `authorisationServiceSupplier` fields (`ConsensusModule.java:1683-1690`).
    ///
    /// # Errors
    ///
    /// As [`Self::launch`], plus a refusal when a configured supplier name is
    /// one this build cannot make.
    pub fn launch_with_components(
        mut config: ConsensusModuleConfig,
        components: LaunchComponents,
    ) -> Result<Self, ClusterError> {
        config.conclude()?;

        let authenticator = match components.authenticator {
            Some(authenticator) => authenticator,
            None => authenticator_for(&config.authenticator_supplier)
                .map_err(|error| ClusterError::Cluster(error.to_string()))?,
        };
        let authorisation_service = match components.authorisation_service {
            Some(service) => service,
            None => authorisation_service_for(config.authorisation_service_supplier.as_deref())
                .map_err(|error| ClusterError::Cluster(error.to_string()))?,
        };

        let now_ms = clock::epoch_millis();
        let now_ns = MillisecondClusterClock.time_nanos();

        let mark_file = ClusterMarkFile::create(
            &config.mark_file_dir.join(mark::FILENAME),
            ClusterComponentType::CONSENSUS_MODULE,
            config.error_buffer_length as usize,
            deepmsg_cnc::create::FILE_PAGE_SIZE_DEFAULT,
            now_ms,
            i64::from(std::process::id()),
        )
        .map_err(cluster)?;

        mark::ensure_mark_file_link(
            &config.cluster_dir,
            &config.mark_file_dir.join(mark::FILENAME),
        )
        .map_err(cluster)?;

        mark_file.set_cluster_id(config.cluster_id);
        mark_file.set_member_id(config.member_id);
        mark_file.set_stream_ids(
            config.archive.control_request_stream_id,
            config.service_stream_id,
            config.consensus_module_stream_id,
            config.ingress_stream_id,
        );
        mark_file
            .write_var_data(&[
                config.aeron_dir.to_string_lossy().as_ref(),
                &config.control_channel,
                &config.ingress_channel,
                "",
                &config.authenticator_supplier,
                config.cluster_dir.to_string_lossy().as_ref(),
            ])
            .map_err(cluster)?;

        let node_state = NodeStateFile::open_or_create(&config.cluster_dir.join("node-state.dat"))
            .map_err(cluster)?;
        if node_state.candidate_term_id() == NULL_VALUE {
            if let Some(candidate_term_id) = mark_file.candidate_term_id() {
                if candidate_term_id != NULL_VALUE {
                    node_state
                        .update_candidate_term_id(candidate_term_id, NULL_VALUE, now_ms)
                        .map_err(cluster)?;
                }
            }
        }

        let recording_log = RecordingLog::open(&config.cluster_dir.join(recording_log::FILENAME))
            .map_err(cluster)?;

        let mut client = Client::connect(&config.aeron_dir).map_err(cluster)?;

        // The member table before the counters: the counters are per cluster,
        // not per member, so the order only matters for the two checks below.
        let members = ClusterMember::parse_list(&config.cluster_members).map_err(cluster)?;
        let this_member_index = members
            .iter()
            .position(|member| member.id == config.member_id)
            .ok_or_else(|| {
                ClusterError::Cluster(format!(
                    "member id {} is not in aeron.cluster.members",
                    config.member_id
                ))
            })?;

        let counters = allocate_counters(&mut client, &config)?;

        let mut archive = Archive::connect(
            &config.archive,
            &mut client,
            &mut NoCredentials,
            Handlers::default(),
        )
        .map_err(cluster)?;

        // A recording left running by a previous process is stopped before the
        // recovery plan is made (`ConsensusModuleAgent.java:336-341`).
        let last_term_recording_id = recording_log.find_last_term_recording_id();
        if last_term_recording_id != NULL_VALUE {
            archive
                .try_stop_recording_by_identity(&mut client, last_term_recording_id)
                .map_err(cluster)?;
        }

        let recovery_plan = create_recovery_plan(
            &recording_log,
            &mut archive,
            &mut client,
            config.service_count,
            NULL_VALUE,
        )?;

        let consensus_subscription_id =
            add_consensus_subscription(&mut client, &config, &members[this_member_index])?;

        // The service control channel (`ConsensusModuleAgent.java:263-266`):
        // one publication the CM writes JoinLog and the rest through, and one
        // subscription it reads the services' answers and requests from.
        let service_publication_id = client.add_publication(
            &config.control_channel,
            config.service_stream_id,
            COMMAND_TIMEOUT,
        )?;
        let service_control_subscription_id = client.add_subscription(
            &config.control_channel,
            config.consensus_module_stream_id,
            COMMAND_TIMEOUT,
        )?;

        let clock = MillisecondClusterClock;
        let idle_strategy = Strategy::by_agrona_name(&config.idle_strategy).ok_or_else(|| {
            ClusterError::Cluster(format!(
                "{CLUSTER_IDLE_STRATEGY_PROP_NAME}={} names no idle strategy this build has",
                config.idle_strategy
            ))
        })?;
        let max_cycle_time = counters.max_cycle_time;
        let cycle_threshold_exceeded = counters.cycle_threshold_exceeded;
        let total_snapshot_duration = counters.total_snapshot_duration;
        let total_snapshot_threshold_exceeded = counters.total_snapshot_threshold_exceeded;
        let sessions = SessionManager::new(
            config.member_id,
            config.cluster_id,
            config.session_timeout_ns,
            config.max_concurrent_sessions,
            authenticator,
            authorisation_service,
        );
        let egress = EgressPublisher::new(config.leader_heartbeat_timeout_ns);
        let ingress = IngressAdapter::new(config.ingress_fragment_limit);

        #[allow(clippy::cast_sign_loss)] // the reference validates [0, MAX_SERVICE_COUNT]
        let service_count = config.service_count as usize;
        let trackers = (0..service_count)
            .map(|service_id| {
                #[allow(clippy::cast_possible_truncation)] // bounded by MAX_SERVICE_COUNT
                let service_id = service_id as i32;
                PendingServiceMessageTracker::new(service_id)
            })
            .collect();
        let service_ack_queues = new_array_of_queues(service_count);
        let service_client_ids = vec![ACK_NULL_VALUE; service_count];

        let timer: Box<dyn TimerService> = match timer_service_kind(&config.timer_service_supplier)
            .map_err(|error| ClusterError::Cluster(error.to_string()))?
        {
            TimerServiceKind::Wheel => Box::new(WheelTimerService::new(
                CLUSTER_TIME_UNIT,
                0,
                wheel_tick_resolution(config.wheel_tick_resolution_ns, CLUSTER_TIME_UNIT),
                usize::try_from(config.ticks_per_wheel).unwrap_or(128),
            )),
            TimerServiceKind::PriorityHeap => Box::new(PriorityHeapTimerService::new()),
        };

        let endpoints = ingress_endpoints(&members);
        let member_count = members.len();
        let mut agent = Self {
            config,
            clock,
            idle_strategy,
            client,
            archive,
            recording_log,
            node_state,
            mark_file,
            recovery_plan,
            counters,
            duty_cycle: DutyCycle {
                max_cycle_time,
                threshold_exceeded: cycle_threshold_exceeded,
                threshold_ns: 0,
                cycle_start_ns: now_ns,
            },
            members,
            this_member_index,
            ranked_positions: vec![0; member::quorum_threshold(member_count)],
            publisher: ConsensusPublisher::new(),
            log_publisher: LogPublisher::new(),
            log_publication_channel_tag: 0,
            log_publication_session_id: -1,
            log_recording_id: NULL_VALUE,
            log_recording_stop_position: 0,
            log_subscription_id: NULL_VALUE,
            log_subscription_channel: String::new(),
            taken_log_image: None,
            append_position: None,
            state: ModuleState::Init,
            role: Role::Follower,
            election: None,
            node_startup_election: true,
            leadership_term_id: NULL_VALUE,
            last_append_position: NULL_VALUE,
            time_of_last_append_position_send_ns: now_ns,
            last_quorum_backtrack_commit_position: NULL_POSITION,
            time_of_last_log_update_ns: now_ns,
            time_of_last_leader_update_ns: now_ns,
            notified_commit_position: 0,
            slow_tick_deadline_ns: 0,
            mark_file_update_deadline_ns: 0,
            ingress_endpoints: endpoints,
            consensus_subscription_id,
            consensus_messages: Vec::with_capacity(64),
            sessions,
            egress,
            ingress,
            service_proxy: ServiceProxy::new(),
            service_publication_id,
            service_control: ServiceControlAdapter::new(Some(service_control_subscription_id)),
            trackers,
            service_ack_queues,
            service_ack_id: 0,
            // The reference initialises this to 0 (`ConsensusModuleAgent.java:132`):
            // a fresh cluster's containers ack position 0 at startup.
            expected_ack_position: 0,
            service_client_ids,
            timer,
            log_replay: None,
            uncommitted_timers: std::collections::VecDeque::new(),
            expired_timer_count: deepmsg_cluster_common::collections::HashMap::default(),
            live_log_destination: String::new(),
            catchup_log_destination: String::new(),
            closed: false,
            cluster_termination: None,
            termination_position: NULL_POSITION,
            termination_leadership_term_id: NULL_VALUE,
            uncommitted_previous_state: std::collections::VecDeque::new(),
            snapshot_duration: SnapshotDuration {
                max_duration: total_snapshot_duration,
                threshold_exceeded: total_snapshot_threshold_exceeded,
                threshold_ns: 0,
                start_ns: None,
            },
        };
        // Same shape as the duty-cycle threshold: the value is the configured
        // one, and both counters already exist.
        agent.snapshot_duration.threshold_ns = agent.config.snapshot_duration_threshold_ns;
        // The tracker's threshold is the configured one; set after the struct
        // exists because both counters were allocated by `allocate_counters`.
        agent.duty_cycle.threshold_ns = agent.config.cycle_threshold_ns;

        // `onStart`'s `recoverFromSnapshotAndLog` (`:342-346`), before the
        // election is built from the plan it returns.
        agent.recover_from_snapshot_and_log()?;

        agent.on_start(now_ns)?;
        Ok(agent)
    }

    /// `onStart` (`ConsensusModuleAgent.java:328-401`), after the archive and
    /// the recovery plan.
    fn on_start(&mut self, now_ns: i64) -> Result<(), ClusterError> {
        self.set_role(Role::Follower);
        self.set_counter_value(
            self.counters.module_state,
            i64::from(ModuleState::Init.code()),
        );

        let last_leadership_term_id = self.recovery_plan.last_leadership_term_id;
        let commit_position = self.commit_position_value();
        let appended_position = self.recovery_plan.appended_log_position;

        self.add_consensus_publications()?;
        let member_ids: Vec<i32> = self.members.iter().map(|member| member.id).collect();
        let election = Election::new(
            self.config.member_id,
            member_ids,
            true,
            -1,
            last_leadership_term_id,
            self.recovery_plan.last_term_base_log_position,
            commit_position,
            appended_position,
            now_ns,
            now_ns as u64,
        );
        self.election = Some(election);
        // `Election`'s constructor increments the election counter
        // (`Election.java:148`); the constructor here has no host, so the agent
        // that makes one counts it.
        increment_counter(&self.client, self.counters.election, 1);

        // `election.doWork(nowNs)` once, before the state is set ACTIVE: the
        // single-member short circuit leaves the election in
        // LEADER_LOG_REPLICATION, which is where the log publication is made.
        self.drive_election_once(now_ns)?;

        self.set_state(ModuleState::Active, "started");
        self.mark_file
            .signal_ready(clock::epoch_millis())
            .map_err(cluster)?;
        Ok(())
    }

    /// One `election.doWork` with the take-out pattern (plan §4.5).
    ///
    /// The reference's `doWork` catch (`ConsensusModuleAgent.java:458-465`)
    /// turns a recoverable error into `election.handleError(nowNs, ex)`, which
    /// counts it and puts the machine back in `INIT`; only an
    /// `AgentTerminationException` (or an error with no election running) leaves
    /// this method as an error.
    fn drive_election_once(&mut self, now_ns: i64) -> Result<usize, ClusterError> {
        let Some(mut election) = self.election.take() else {
            return Ok(0);
        };
        self.publisher.refresh_position_limits(&self.client);
        let mut publisher = mem::take(&mut self.publisher);
        let result = election.do_work(self, &mut publisher);
        self.publisher = publisher;

        let result = match result {
            Err(error) if error.is_election_recoverable() => {
                self.report_counted_error();
                let publisher = mem::take(&mut self.publisher);
                let recovered = election.handle_error(self, now_ns, error);
                self.publisher = publisher;
                recovered.map(|()| 1)
            }
            other => other,
        };

        if election.state() != ElectionState::Closed {
            self.election = Some(election);
        }
        result
    }

    /// One duty cycle. `ConsensusModuleAgent.doWork` (`:403-478`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the reference's three channels (`:439-466`): an
    /// [`ClusterError::AgentTermination`] ends the agent; every other error is
    /// returned because `P3-5b` owns `Election::handle_error`, which is what the
    /// reference calls to retry an election. Returning it here is the visible
    /// answer instead of a duty cycle that pretends to have recovered.
    pub fn do_work(&mut self) -> Result<usize, ClusterError> {
        let result = self.do_work_inner();

        // `doWork`'s catch block (`:439-466`): a termination passes through; a
        // storage-space refusal and a lost archive both end the agent with the
        // reference's counted error first. Anything else is returned, which is
        // what the reference rethrows when no election is in progress.
        let Err(error) = result else {
            return result;
        };
        if error.is_termination() {
            return Err(error);
        }
        if storage_space_refusal(&error) {
            self.report_counted_error();
            return Err(self.unexpected_termination(&error.to_string()));
        }
        if !self.archive.is_connected(&self.client) {
            self.report_counted_error();
            return Err(self.unexpected_termination("local archive is not connected"));
        }

        Err(error)
    }

    fn do_work_inner(&mut self) -> Result<usize, ClusterError> {
        let timestamp = self.clock.time();
        let now_ns = self.clock.convert_to_nanos(timestamp);
        self.duty_cycle.measure(&self.client, now_ns);

        let mut work = 0;
        if now_ns >= self.slow_tick_deadline_ns {
            let slow = self.slow_tick_work(now_ns)?;
            work += slow;
            self.slow_tick_deadline_ns = if slow > 0 {
                now_ns + 1
            } else {
                now_ns + SLOW_TICK_INTERVAL_NS
            };
        }

        work += self.consensus_adapter_poll()?;
        work += self.poll_log_replay()?;

        if self.election.is_some() {
            work += self.drive_election_once(now_ns)?;
        } else {
            work += self.consensus_work(timestamp, now_ns)?;
        }

        Ok(work)
    }

    /// `slowTickWork` (`:2384-2479`), with the session, control and service
    /// pieces left to the slices that own them.
    fn slow_tick_work(&mut self, now_ns: i64) -> Result<usize, ClusterError> {
        let mut work = 0;

        if self.client.poll() {
            work += 1;
        }
        if self.client.is_closed() {
            return Err(ClusterError::AgentTermination(
                "unexpected Aeron close".to_owned(),
            ));
        }
        if self.state == ModuleState::Closed {
            return Err(ClusterError::AgentTermination(
                "State.CLOSED == state".to_owned(),
            ));
        }

        if now_ns >= self.mark_file_update_deadline_ns {
            self.mark_file_update_deadline_ns = now_ns + MARK_FILE_UPDATE_INTERVAL_NS;
            self.mark_file
                .update_activity_timestamp(self.clock.time_millis());
        }

        work += self.poll_archive_events();

        // `ClusterSession.asyncConnect`'s two commands were deferred to here by
        // the ingress callback (see `session::ingress`).
        self.sessions.drive_async_connects(&mut self.client)?;

        // `sendRedirects` / `sendRejections` (`:2404-2405`): every role and
        // state sends them, because a client parked on a node that is not the
        // leader still has to be told where the leader is.
        {
            let leadership_term_id = self.leadership_term_id;
            let member_id = self.config.member_id;
            let leader_member_id = self.members[self.this_member_index].id;
            let ingress_endpoints = self.ingress_endpoints.as_str();
            let clock = self.clock;

            let Self {
                client,
                sessions,
                egress,
                log_publisher,
                counters,
                ..
            } = self;
            let mut ctx = SessionContext {
                client,
                log_publisher,
                egress,
                clock: &clock,
                timed_out_counter: counters.timed_out_client,
                leadership_term_id,
                member_id,
                leader_member_id,
                ingress_endpoints,
                now_ns,
            };

            work += sessions.send_redirects(&mut ctx)?;
            work += sessions.send_rejections(&mut ctx)?;
        }

        if self.election.is_none() {
            if self.role == Role::Leader {
                // `checkClusterControlToggle` (`:2411`) runs before the
                // session work: an operator's SUSPEND takes effect this tick.
                work += self.check_cluster_control_toggle(now_ns)?;

                if self.state == ModuleState::Active {
                    work += self.process_open_sessions(now_ns)?;

                    // `:2420-2425`: a leader that cannot see a quorum steps
                    // down.
                    if self.members.len() > 1
                        && !member::has_active_quorum(
                            &self.members,
                            now_ns,
                            self.config.leader_heartbeat_timeout_ns,
                        )
                    {
                        work += self.enter_election(false, "inactive follower quorum")?;
                    }
                } else if self.state == ModuleState::Terminating {
                    // `:2427-2434`: the leader closes once its services and
                    // every follower have finished (`SHUTDOWN`), or at the
                    // deadline.
                    let can_terminate =
                        self.cluster_termination
                            .as_ref()
                            .is_some_and(|termination| {
                                termination.can_terminate(&self.members, now_ns)
                            });
                    if can_terminate {
                        self.recording_log
                            .commit_log_position_for_term(
                                self.leadership_term_id,
                                self.termination_position,
                            )
                            .map_err(cluster)?;
                        return Err(self.close_and_terminate());
                    }
                }
            } else {
                // `:2438-2452`: a follower whose leader has gone quiet — and
                // which is not already terminating — starts an election.
                if (self.state == ModuleState::Active || self.state == ModuleState::Suspended)
                    && now_ns
                        >= self.time_of_last_log_update_ns + self.config.leader_heartbeat_timeout_ns
                    && self.termination_position == NULL_POSITION
                {
                    work += self.enter_election(false, "leader heartbeat timeout")?;
                }
            }
            // A follower's backup sessions (`processPendingBackupSessions`,
            // `:2440`) are ClusterBackup's (P3-9), and standby snapshot
            // notifications are premium; neither can be produced in this slice.

            if self.state == ModuleState::Active {
                work += self.check_node_control_toggle()?;
            }
        }

        Ok(work)
    }

    /// The leader's session work (`:2415-2418`), split out because the control
    /// toggle has to be read before it.
    fn process_open_sessions(&mut self, now_ns: i64) -> Result<usize, ClusterError> {
        let leadership_term_id = self.leadership_term_id;
        let member_id = self.config.member_id;
        let leader_member_id = self.members[self.this_member_index].id;
        let ingress_endpoints = self.ingress_endpoints.as_str();
        let clock = self.clock;

        let Self {
            client,
            sessions,
            egress,
            log_publisher,
            counters,
            ..
        } = self;
        let mut ctx = SessionContext {
            client,
            log_publisher,
            egress,
            clock: &clock,
            timed_out_counter: counters.timed_out_client,
            leadership_term_id,
            member_id,
            leader_member_id,
            ingress_endpoints,
            now_ns,
        };

        let mut work = sessions.process_all_pending_sessions(&mut ctx)?;
        work += sessions.check_sessions(&mut ctx)?;
        Ok(work)
    }

    // -- operator toggles and termination (P3-4e) --------------------------

    /// `checkClusterControlToggle` (`:2546-2662`).
    ///
    /// Only a leader calls this, and only in a slow tick. The three states the
    /// module can be in each answer one question: `ACTIVE` may start an action,
    /// `SNAPSHOT` waits for its own completion, `SUSPENDED` waits for `RESUME`.
    /// The switch is the operator's; the module resets it when the action is
    /// complete (`snapshot_on_service_ack`, the `RESUME` case) so the counter
    /// is `NEUTRAL` again for the next request.
    ///
    /// # Errors
    ///
    /// An invalid counter value is the reference's `ClusterException`; an
    /// append failure is the log's.
    fn check_cluster_control_toggle(&mut self, now_ns: i64) -> Result<usize, ClusterError> {
        match self.state {
            ModuleState::Active => {
                let toggle = ClusterControlToggle::get(&self.client, self.counters.control_toggle)?;
                match toggle {
                    ClusterControlToggle::Suspend => {
                        let timestamp = self.clock.time();
                        if self.append_action(ClusterAction::SUSPEND, timestamp)? {
                            self.offer_position_and_previous_state();
                            self.set_state(ModuleState::Suspended, "ClusterControl.SUSPEND");
                        }
                    }
                    ClusterControlToggle::Snapshot => {
                        let timestamp = self.clock.time();
                        if self.append_action(ClusterAction::SNAPSHOT, timestamp)? {
                            self.offer_position_and_previous_state();
                            self.set_state(ModuleState::Snapshot, "ClusterControl.SNAPSHOT");
                            self.snapshot_duration.on_snapshot_begin(now_ns);
                        }
                    }
                    ClusterControlToggle::StandbySnapshot => {
                        // A standby snapshot is premium's (`ClusterControl.java:80-83`)
                        // and a consensus module is never a standby: refuse it
                        // by name, count it, and put the counter back so the
                        // operator's next action is not shadowed by this one.
                        ClusterControlToggle::reset(&self.client, self.counters.control_toggle);
                        self.report_counted_error();
                    }
                    ClusterControlToggle::Shutdown => {
                        let timestamp = self.clock.time();
                        if self.append_action(ClusterAction::SNAPSHOT, timestamp)? {
                            let position = self.log_publisher.position();
                            self.begin_termination(now_ns, position);
                            self.offer_position_and_previous_state();
                            self.set_state(ModuleState::Snapshot, "ClusterControl.SHUTDOWN");
                            self.snapshot_duration.on_snapshot_begin(now_ns);
                        }
                    }
                    ClusterControlToggle::Abort => {
                        // No snapshot: the position is where the log is now
                        // (`:2610-2629`).
                        let position = self.log_publisher.position();
                        self.begin_termination(now_ns, position);
                        if self.config.service_count > 0 {
                            self.send_service_termination_position(true);
                        } else if let Some(termination) = self.cluster_termination.as_mut() {
                            termination.on_services_terminated();
                        }
                        self.set_state(ModuleState::Terminating, "ClusterControl.ABORT");
                    }
                    ClusterControlToggle::Inactive
                    | ClusterControlToggle::Neutral
                    | ClusterControlToggle::Resume => return Ok(0),
                }

                Ok(1)
            }
            ModuleState::Snapshot => {
                // `:2637-2644`: with no services the snapshot set is complete
                // as soon as the log has caught up with the commit position.
                // The position handed on is the **commit** position, as in the
                // reference.
                if self.config.service_count == 0
                    && self.log_publisher.position() <= self.commit_position_value()
                {
                    let log_position = self.commit_position_value();
                    let timestamp = self.clock.time();
                    self.snapshot_on_service_ack(log_position, timestamp, &[])?;
                }

                Ok(0)
            }
            ModuleState::Suspended => {
                // `:2645-2659`: only `RESUME` can move a suspended cluster, and
                // it is the one switch that is not a compare-and-set out of
                // `NEUTRAL` (the suspended module never resets the counter).
                if ClusterControlToggle::get(&self.client, self.counters.control_toggle)?
                    == ClusterControlToggle::Resume
                {
                    let timestamp = self.clock.time();
                    if self.append_action(ClusterAction::RESUME, timestamp)? {
                        self.offer_position_and_previous_state();
                        self.set_state(ModuleState::Active, "ClusterControl.RESUME");
                        ClusterControlToggle::reset(&self.client, self.counters.control_toggle);
                    }

                    return Ok(1);
                }

                Ok(0)
            }
            _ => Ok(0),
        }
    }

    /// `checkNodeControlToggle` (`:2670-2708`).
    ///
    /// Both OSS values name features this build does not have — the standby
    /// snapshot replicator (premium, `Q1`) and the recording-log validator
    /// (`P3-9`) — so each is refused by name: the toggle is reset and the
    /// refusal is counted, rather than the counter staying armed against a
    /// module that would never act on it.
    ///
    /// # Errors
    ///
    /// An invalid counter value, as the reference's `ClusterException`.
    fn check_node_control_toggle(&mut self) -> Result<usize, ClusterError> {
        match NodeControlToggle::get(&self.client, self.counters.node_control_toggle)? {
            NodeControlToggle::ReplicateStandbySnapshot
            | NodeControlToggle::ValidateRecordingLog => {
                NodeControlToggle::reset(&self.client, self.counters.node_control_toggle);
                self.report_counted_error();
                Ok(1)
            }
            NodeControlToggle::Inactive | NodeControlToggle::Neutral => Ok(0),
        }
    }

    /// `appendAction` (`:2710-2713`).
    fn append_action(
        &mut self,
        action: ClusterAction,
        timestamp: i64,
    ) -> Result<bool, ClusterError> {
        self.log_publisher.append_cluster_action(
            &self.client,
            self.leadership_term_id,
            timestamp,
            action,
            CLUSTER_ACTION_FLAGS_DEFAULT,
        )
    }

    /// `offerPositionAndPreviousState` (`:2664-2668`): the state this action is
    /// leaving, paired with the log position it will be committed at. If the
    /// action is never committed, `restore_uncommitted_entries` puts the state
    /// back (`:3056-3060`).
    fn offer_position_and_previous_state(&mut self) {
        let position = self.log_publisher.position();
        self.uncommitted_previous_state
            .push_back((position, i64::from(self.state.code())));
    }

    /// `snapshotOnServiceAck` (`:3104-3146`), after the snapshot itself.
    ///
    /// When this snapshot is the one a `SHUTDOWN` asked for, the termination
    /// half starts here: the services are told where to stop (or, with no
    /// services, are already done), the deadline restarts, and the module moves
    /// to `TERMINATING`. Otherwise it is an ordinary snapshot and the module
    /// returns to `ACTIVE`, resetting the operator's toggle.
    ///
    /// **Deviation:** the reference catches a snapshot failure, counts it, and
    /// only ends the agent for a termination or a storage-space error
    /// (`:3108-3121`). This build has no `ArchiveException.STORAGE_SPACE`
    /// distinction yet, so the error is propagated (counted by the caller of
    /// `do_work`) — recorded in the P3-4e delivery record.
    fn snapshot_on_service_ack(
        &mut self,
        log_position: i64,
        timestamp: i64,
        service_acks: &[ServiceAck],
    ) -> Result<(), ClusterError> {
        self.take_snapshot_with_acks(timestamp, log_position, service_acks)?;

        self.sessions
            .update_time_of_last_activity(self.clock.time_nanos());

        if self.cluster_termination.is_some() {
            if self.config.service_count > 0 {
                self.send_service_termination_position(true);
            } else if let Some(termination) = self.cluster_termination.as_mut() {
                termination.on_services_terminated();
            }

            let deadline_ns = self.clock.time_nanos() + self.config.termination_timeout_ns;
            if let Some(termination) = self.cluster_termination.as_mut() {
                termination.set_deadline_ns(deadline_ns);
            }
            self.set_state(ModuleState::Terminating, "null != clusterTermination");
        } else {
            self.set_state(ModuleState::Active, "snapshot complete");
            if self.role == Role::Leader {
                ClusterControlToggle::reset(&self.client, self.counters.control_toggle);
            }
        }

        Ok(())
    }

    /// `terminateOnServiceAck` (`:3309-3317`).
    fn terminate_on_service_ack(&mut self, log_position: i64) -> Result<(), ClusterError> {
        if let Some(termination) = self.cluster_termination.as_mut() {
            termination.on_services_terminated();
        }

        self.do_termination(log_position)
    }

    /// `doTermination` (`:3319-3344`).
    ///
    /// A follower acknowledges and closes; a leader closes when
    /// [`ClusterTermination::can_terminate`] says the cluster is ready. Both
    /// paths record the termination position as the term's commit position
    /// before stopping the recording, so a restart resumes from there.
    fn do_termination(&mut self, log_position: i64) -> Result<(), ClusterError> {
        if self.cluster_termination.is_none() {
            if self.termination_leadership_term_id == self.leadership_term_id {
                // The reference ignores `terminationAck`'s result (`:3325`):
                // this is a best-effort answer from a node about to close.
                let leader_member_id = self.leader_member_id();
                let _sent = self.publisher.termination_ack(
                    leader_member_id,
                    self.leadership_term_id,
                    log_position,
                    self.config.member_id,
                );
            } else {
                // `:3330-3332`: the reference's ERROR `ClusterEvent` for an ack
                // that belongs to a previous term.
                self.report_counted_error();
            }

            self.recording_log
                .commit_log_position_for_term(self.leadership_term_id, log_position)
                .map_err(cluster)?;
            return Err(self.close_and_terminate());
        }

        let can_terminate = self
            .cluster_termination
            .as_ref()
            .is_some_and(|termination| {
                termination.can_terminate(&self.members, self.clock.time_nanos())
            });
        if can_terminate {
            self.recording_log
                .commit_log_position_for_term(self.leadership_term_id, log_position)
                .map_err(cluster)?;
            return Err(self.close_and_terminate());
        }

        Ok(())
    }

    /// `unexpectedTermination` (`:3297-3307`): a counted error has already
    /// happened; tell this node's services where to stop, stop the recording,
    /// mark the state closed, and end the duty cycle.
    fn unexpected_termination(&mut self, reason: &str) -> ClusterError {
        if self.config.service_count > 0 {
            self.send_service_termination_position_at(0, true);
        }
        if self.try_stop_log_recording().is_err() {
            self.report_counted_error();
        }
        self.set_state(ModuleState::Closed, reason);
        ClusterError::AgentTermination(reason.to_owned())
    }

    /// `closeAndTerminate` (`:3290-3295`): stop the recording, mark the state,
    /// and hand the duty cycle the one error that ends it.
    ///
    /// The reference's `tryStopLogRecording` catches its own archive errors and
    /// reports them (`:3346-3370`); this one returns them, so the failure is
    /// counted and the close continues — a node being told to stop must still
    /// stop.
    fn close_and_terminate(&mut self) -> ClusterError {
        if self.try_stop_log_recording().is_err() {
            self.report_counted_error();
        }

        self.set_state(ModuleState::Closed, "expected termination");
        ClusterError::AgentTermination("expected termination".to_owned())
    }

    /// `ClusterTermination.terminationPosition` (`ClusterTermination.java:63-84`)
    /// wired to this agent's members and publisher, then the three fields a
    /// leader keeps (`:2592-2601`, `:2614-2618`).
    fn begin_termination(&mut self, now_ns: i64, position: i64) {
        let deadline_ns = now_ns + self.config.termination_timeout_ns;
        let mut termination = ClusterTermination::new(deadline_ns);

        {
            let Self {
                members,
                publisher,
                client,
                counters,
                ..
            } = self;
            let this_member_id = self.config.member_id;
            let error_counter = counters.error;
            let mut on_send_failure = |_member_id: i32| {
                increment_counter(client, error_counter, 1);
            };

            termination.termination_position(
                members,
                this_member_id,
                publisher,
                &mut on_send_failure,
                self.leadership_term_id,
                position,
            );
        }

        self.cluster_termination = Some(termination);
        self.termination_position = position;
        self.termination_leadership_term_id = self.leadership_term_id;
    }

    /// `serviceProxy.terminationPosition(terminationPosition, ackRequired)`
    /// (`:2621`, `:2508`, `:3129`): tell this node's services where to stop.
    fn send_service_termination_position(&mut self, ack_required: bool) -> bool {
        self.send_service_termination_position_at(self.termination_position, ack_required)
    }

    /// The same at an explicit position: `unexpectedTermination` sends
    /// **position 0** (`:3302`), not the termination position.
    fn send_service_termination_position_at(&mut self, position: i64, ack_required: bool) -> bool {
        let send = PublicationSender {
            client: &self.client,
            registration_id: self.service_publication_id,
        };
        self.service_proxy
            .termination_position(&send, position, ack_required)
    }

    /// The member the reference calls `leaderMember` (`:2369-2381`).
    ///
    /// A one-member cluster is its own leader once the election completes; a
    /// follower learns the id from the log replication (`P3-6b`), so until
    /// then this answers the local id — the only member there is.
    fn leader_member_id(&self) -> i32 {
        self.members
            .iter()
            .find(|member| member.is_leader)
            .map_or(self.config.member_id, |member| member.id)
    }

    /// The reference's `ctx.countedErrorHandler().onError(...)`: one on counter
    /// 210. The text forms of those events need the error log (`P3-4f`), so
    /// what is visible here is the count.
    fn report_counted_error(&self) {
        increment_counter(&self.client, self.counters.error, 1);
    }

    /// `onTerminationPosition` (`:1183-1193`): a follower records where the
    /// leader will stop. The consensus dispatcher that will call this is
    /// `P3-5b`/`P3-6a`; until it exists a one-member cluster has no leader to
    /// send one.
    pub fn on_termination_position(&mut self, leadership_term_id: i64, log_position: i64) {
        if leadership_term_id == self.leadership_term_id && self.role == Role::Follower {
            self.termination_position = log_position;
            self.termination_leadership_term_id = leadership_term_id;
            self.time_of_last_log_update_ns = self.clock.time_nanos();
        }
    }

    /// `onTerminationAck` (`:1195-1216`): the leader marks the follower that
    /// answered and closes once every one of them has.
    ///
    /// # Errors
    ///
    /// The recording log's, when the termination position is committed.
    pub fn on_termination_ack(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        member_id: i32,
    ) -> Result<(), ClusterError> {
        if leadership_term_id == self.leadership_term_id
            && log_position >= self.termination_position
            && self.role == Role::Leader
        {
            let mut found = false;
            if let Some(member) = self
                .members
                .iter_mut()
                .find(|member| member.id == member_id)
            {
                member.has_terminated = true;
                found = true;
            }

            if found {
                let can_terminate = self
                    .cluster_termination
                    .as_ref()
                    .is_some_and(|termination| {
                        termination.can_terminate(&self.members, self.clock.time_nanos())
                    });
                if can_terminate {
                    self.recording_log
                        .commit_log_position_for_term(leadership_term_id, self.termination_position)
                        .map_err(cluster)?;
                    return Err(self.close_and_terminate());
                }
            }
        }

        Ok(())
    }

    /// `consensusAdapter.poll()` (`:423`): read the members' consensus
    /// messages.
    ///
    /// The dispatcher is `P3-5b`/`P3-6a`; a message arriving here is a member
    /// speaking to a node that cannot answer, so it is a refusal rather than a
    /// dropped frame. A one-member cluster has nobody to hear from.
    fn consensus_adapter_poll(&mut self) -> Result<usize, ClusterError> {
        let mut messages = std::mem::take(&mut self.consensus_messages);
        messages.clear();
        let mut decode_error = None;
        {
            let subscription_id = self.consensus_subscription_id;
            self.client
                .poll_subscription(subscription_id, CONSENSUS_FRAGMENT_LIMIT, |message| {
                    match decode_consensus_message(message.payload) {
                        Ok(Some(decoded)) => messages.push(decoded),
                        Ok(None) => {}
                        Err(error) => {
                            if decode_error.is_none() {
                                decode_error = Some(error);
                            }
                        }
                    }
                });
        }
        let mut work = 0;
        let mut dispatch_error = None;
        for message in messages.drain(..) {
            match self.dispatch_consensus_message(message) {
                Ok(count) => work += count,
                Err(error) => {
                    dispatch_error = Some(error);
                    break;
                }
            }
        }
        self.consensus_messages = messages;

        if let Some(error) = decode_error {
            return Err(error);
        }
        match dispatch_error {
            Some(error) => Err(error),
            None => Ok(work),
        }
    }

    /// `consensusWork` (`:2481-2543`), leader half.
    ///
    /// The follower half needs the log adapter and the follower position
    /// publisher, both `P3-4b`/`P3-6a`; a node that reaches this method as a
    /// follower is one the reference would not produce in this slice, so it is a
    /// refusal rather than a silent `0`.
    fn consensus_work(&mut self, timestamp: i64, now_ns: i64) -> Result<usize, ClusterError> {
        let mut work = 0;

        if self.role == Role::Leader {
            // The reference's order and state gate (`:2487-2497`): timers, then
            // the pending service message trackers, then ingress — **only while
            // ACTIVE**, which is what makes `SUSPEND` a suspension — then the
            // leader position, which every state updates.
            if self.state == ModuleState::Active {
                work += self.poll_timers(timestamp)?;
                work += self.poll_trackers(timestamp)?;
                work += self.poll_ingress()?;
            }
            self.publisher.refresh_position_limits(&self.client);
            work += self.update_leader_position_internal(now_ns)?;
        } else {
            // `:2499-2533`, without the log adapter: the follower's half of the
            // stream is `P3-6b`. What is here is ingress forwarding (a client
            // parked on a follower is redirected by the session, not the log)
            // and the append position that is the follower's heartbeat.
            if self.state == ModuleState::Active || self.state == ModuleState::Suspended {
                work += self.poll_ingress()?;
            }
            work += self.update_follower_position_internal(now_ns)?;
        }

        // Both roles poll the service control channel (`:2535`).
        work += self.poll_service_control()?;

        Ok(work)
    }

    /// The `ConsensusModuleAgent.updateLeaderPosition(nowNs)` one-argument form
    /// (`:2885-2895`), which reads the append counter and the quorum position.
    fn update_leader_position_internal(&mut self, now_ns: i64) -> Result<usize, ClusterError> {
        let Some(_) = self.append_position else {
            return Ok(0);
        };
        let leader_append_position = self.append_position_value();
        let quorum_position =
            self.quorum_position_bounded_by_leader_log(leader_append_position, now_ns);
        self.update_leader_position(now_ns, leader_append_position, quorum_position)
    }

    /// Close the agent: `onClose` (`:291-326`) without the pieces this slice
    /// does not own.
    ///
    /// # Errors
    ///
    /// The archive's answer when stopping the recording fails. A shutdown that
    /// cannot stop its recording says so; the mark file is still marked
    /// terminated, because the next process must be able to take the directory.
    pub fn close(mut self) -> Result<(), ClusterError> {
        self.close_inner()
    }

    fn close_inner(&mut self) -> Result<(), ClusterError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        let stop_result = self.try_stop_log_recording();

        // A client's counters are its own to give back: `Aeron.close()` closes
        // every `AtomicCounter` it owns (`ClientConductor.java:184`), which is
        // what a known directory needs — a restart must not find the previous
        // process's module-state counter and refuse itself
        // (`ConsensusModule.java:1941-1945`). Closing the client alone only
        // tells the driver the client is gone; the reaper frees what it owned
        // later, and a restart in the same directory cannot wait for that.
        let mut remove_error = None;
        for counter in self.counters() {
            if let Err(error) = self.client.remove_counter(&counter, COMMAND_TIMEOUT) {
                remove_error = remove_error.or(Some(cluster(format!(
                    "could not remove counter {}: {error}",
                    counter.counter_id()
                ))));
            }
        }
        self.client.close();

        self.set_state(ModuleState::Closed, "closed");
        self.mark_file.signal_terminated().map_err(cluster)?;

        stop_result?;
        match remove_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    // -- state and counters ------------------------------------------------

    /// `state(...)` (`:1310`): the field and counter 200.
    fn set_state(&mut self, state: ModuleState, _reason: &str) {
        self.state = state;
        self.set_counter_value(self.counters.module_state, i64::from(state.code()));
    }

    /// `role(...)` (`:1337`), counter 201.
    fn set_role(&mut self, role: Role) {
        self.role = role;
        self.set_counter_value(self.counters.node_role, i64::from(role.code()));
    }

    fn set_counter_value(&self, counter: Counter, value: i64) {
        write_counter(&self.client, counter, value);
    }

    fn commit_position_value(&self) -> i64 {
        read_counter(&self.client, self.counters.commit_position).unwrap_or(0)
    }

    fn append_position_value(&self) -> i64 {
        let Some(position) = self.append_position else {
            return self.log_recording_stop_position;
        };
        self.client
            .counters_reader()
            .and_then(|counters| counters.value(position.counter_id))
            .unwrap_or(self.log_recording_stop_position)
    }

    fn position_limit(&self, publication: &ExclusivePublication) -> i64 {
        self.client
            .counters_reader()
            .and_then(|counters| counters.value(publication.position_limit_counter_id()))
            .unwrap_or(0)
    }

    // -- the log -----------------------------------------------------------

    /// `addLogPublication` (`:1639-1686`). The publication is taken out of the
    /// client and given to [`LogPublisher`].
    fn add_log_publication(&mut self, append_position: i64) -> Result<usize, ClusterError> {
        let log_publication_tag = self.client.next_correlation_id().map_err(cluster)?;
        self.log_publication_channel_tag = self.client.next_correlation_id().map_err(cluster)?;

        let mut channel = ChannelUri::parse(&self.config.log_channel).map_err(cluster)?;
        channel.put("alias", "log");
        channel.put(
            "tags",
            format!(
                "{},{}",
                self.log_publication_channel_tag, log_publication_tag
            ),
        );

        if channel.media() == "udp" {
            // `addLogPublication` (`:1650-1656`): under a manual MDC log the
            // publication says so itself, and — unless the configuration turns
            // it off — binds the control endpoint at this member's own log
            // address with a wildcard port. The reference puts these on the
            // publication regardless of what the configured channel says.
            if self.log_is_mdc() {
                channel.put("control-mode", "manual");
                if self.config.log_control_enabled {
                    if let Some(control) = self
                        .member_log_endpoint(self.config.member_id)
                        .as_deref()
                        .and_then(replace_port_with_wildcard)
                    {
                        channel.put("control", control);
                    }
                }
            }

            channel.put("ssc", (self.members.len() == 1).to_string());
        }

        if let Some(log) = &self.recovery_plan.log {
            put_initial_position(
                &mut channel,
                append_position,
                log.initial_term_id,
                log.term_buffer_length,
            );
            channel.put("mtu", log.mtu_length.to_string());
        } else {
            // `ensureConsistentInitialTermId` (`:3395-3400`).
            channel.put("init-term-id", "0");
            channel.put("term-id", "0");
            channel.put("term-offset", "0");
        }

        let registration_id = self
            .client
            .add_exclusive_publication(&channel.build(), self.config.log_stream_id, COMMAND_TIMEOUT)
            .map_err(cluster)?;
        let publication = self
            .client
            .take_exclusive_publication(registration_id)
            .ok_or_else(|| {
                ClusterError::Cluster("the log publication was not mapped".to_owned())
            })?;

        self.log_publication_session_id = publication.session_id();
        self.log_publisher.set_publication(publication);

        // `addLogPublication` (`:1674-1683`): under MDC the leader is the only
        // writer, so every follower is a destination of its log publication.
        if channel_uri_media_is_udp(&self.config.log_channel) && self.log_is_mdc() {
            for index in 0..self.members.len() {
                if self.members[index].id == self.config.member_id {
                    continue;
                }
                let endpoint = self.members[index].log_endpoint.clone();
                let log_channel = self.config.log_channel.clone();

                let mut log_publisher = std::mem::take(&mut self.log_publisher);
                let added =
                    log_publisher.add_destination(&mut self.client, &log_channel, &endpoint);
                self.log_publisher = log_publisher;
                if added.is_err() {
                    self.report_counted_error();
                }
            }
        }

        Ok(1)
    }

    /// `startLogRecording` (`:2360-2371`).
    fn start_log_recording(
        &mut self,
        channel: &str,
        stream_id: i32,
        local_source: bool,
    ) -> Result<(), ClusterError> {
        let last_recording_id = self.recording_log.find_last_term_recording_id();
        let subscription_id = if last_recording_id == NULL_VALUE {
            self.archive
                .start_recording(&mut self.client, channel, stream_id, local_source, true)
                .map_err(cluster)?
        } else {
            self.archive
                .extend_recording(
                    &mut self.client,
                    last_recording_id,
                    channel,
                    stream_id,
                    local_source,
                    true,
                )
                .map_err(cluster)?
        };

        self.log_subscription_id = subscription_id;
        Ok(())
    }

    /// `tryCreateAppendPosition` (`:2726-2751`): the archive's recording
    /// position counter for this session, or `false` while it does not exist.
    fn try_create_append_position(&mut self, log_session_id: i32) -> Result<bool, ClusterError> {
        let Some(counters) = self.client.counters_reader() else {
            return Ok(false);
        };

        let Some(counter_id) =
            find_counter_id_by_session(&counters, log_session_id, self.archive.archive_id())
        else {
            return Ok(false);
        };

        let Some(descriptor) = counters.get(counter_id) else {
            return Ok(false);
        };
        if 0 == descriptor.registration_id {
            return Ok(false);
        }

        let Some(key) = counters.key(counter_id) else {
            return Ok(false);
        };
        let Some(identity) = parse_key(&key) else {
            return Ok(false);
        };
        if identity.recording_id == NULL_VALUE {
            return Ok(false);
        }

        self.log_recording_id = identity.recording_id;
        self.append_position = Some(AppendPosition { counter_id });
        Ok(true)
    }

    /// `tryStopLogRecording` (`:3346-3378`), keeping the id like the reference
    /// does (the recovery plan still needs it).
    fn try_stop_log_recording(&mut self) -> Result<(), ClusterError> {
        self.append_position = None;

        if self.log_subscription_id != NULL_VALUE {
            let subscription_id = mem::replace(&mut self.log_subscription_id, NULL_VALUE);
            self.archive
                .try_stop_recording_subscription(&mut self.client, subscription_id)
                .map_err(cluster)?;
        } else if self.log_recording_id != NULL_VALUE {
            self.archive
                .try_stop_recording_by_identity(&mut self.client, self.log_recording_id)
                .map_err(cluster)?;
        }

        Ok(())
    }

    // -- waiting -----------------------------------------------------------

    /// The reference's `idle()` (`:529-557`): drive this agent's own client,
    /// idle, and poll archive events. Nothing else — plan §4.4 rule 1.
    fn idle(&mut self) -> Result<(), ClusterError> {
        self.idle_strategy.idle(0);
        if self.client.poll() {
            // Work is the caller's loop condition, not this method's.
        }
        if self.client.is_closed() {
            return Err(ClusterError::AgentTermination(
                "unexpected Aeron close".to_owned(),
            ));
        }
        let _ = self.poll_archive_events();
        Ok(())
    }

    // -- accessors the tests and the next slices read ---------------------

    /// The module state (counter 200's twin).
    pub const fn state(&self) -> ModuleState {
        self.state
    }

    /// The role (counter 201's twin).
    pub const fn role(&self) -> Role {
        self.role
    }

    /// The election's state, or `None` when no election is in progress.
    pub fn election_state(&self) -> Option<ElectionState> {
        self.election.as_ref().map(Election::state)
    }

    /// The term the module is in.
    pub const fn leadership_term_id(&self) -> i64 {
        self.leadership_term_id
    }

    /// The recovery plan this launch made.
    pub fn recovery_plan(&self) -> &RecoveryPlan {
        &self.recovery_plan
    }

    /// The mark file.
    pub const fn mark_file(&self) -> &ClusterMarkFile {
        &self.mark_file
    }

    /// The recording id of the local log, or `-1`.
    pub const fn log_recording_id(&self) -> i64 {
        self.log_recording_id
    }

    /// The log's publication session, or `-1` before the publication exists.
    pub const fn log_publication_session_id(&self) -> i32 {
        self.log_publication_session_id
    }

    /// The committed sessions (P3-4b).
    pub fn sessions(&self) -> &[crate::session::ClusterSession] {
        self.sessions.sessions()
    }

    /// How many sessions are open right now.
    pub fn session_count(&self) -> usize {
        self.sessions.sessions().len()
    }

    /// Whether a log replay is still in progress (P3-4d).
    pub fn is_replaying(&self) -> bool {
        self.log_replay.is_some()
    }

    /// How far the log publication has got, which advances when a message is
    /// appended even before it is committed.
    pub fn log_position(&self) -> i64 {
        self.log_publisher.position()
    }

    /// The counter ids this module allocated, for a test that checks all of them
    /// were made with the reference's keys and labels.
    pub fn counter_ids(&self) -> Vec<i32> {
        self.counters().iter().map(Counter::counter_id).collect()
    }

    /// Every counter this module owns, in allocation order.
    fn counters(&self) -> [Counter; 15] {
        [
            self.counters.error,
            self.counters.module_state,
            self.counters.election_state,
            self.counters.election,
            self.counters.leadership_term,
            self.counters.node_role,
            self.counters.commit_position,
            self.counters.control_toggle,
            self.counters.node_control_toggle,
            self.counters.snapshot,
            self.counters.timed_out_client,
            self.counters.max_cycle_time,
            self.counters.cycle_threshold_exceeded,
            self.counters.total_snapshot_duration,
            self.counters.total_snapshot_threshold_exceeded,
        ]
    }

    /// The client, for a test that reads the CnC counters directly.
    pub const fn client(&self) -> &Client {
        &self.client
    }

    /// The client, mutably — a test's way to make the driver go away under the
    /// agent and check that the duty cycle notices.
    pub fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    /// The archive client, likewise.
    pub const fn archive(&self) -> &Archive {
        &self.archive
    }

    /// The counter a value lives in, by its type id and cluster id — the
    /// reference's `ClusterCounters.find`.
    pub fn find_counter(&self, type_id: i32) -> Option<i32> {
        let counters = self.client.counters_reader()?;
        counters::find_cluster_counter(&counters, type_id, self.config.cluster_id)
    }
}

impl Drop for ConsensusModuleAgent {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.close_inner();
        }
    }
}

// ---------------------------------------------------------------------------
// The election seams.
// ---------------------------------------------------------------------------

impl ConsensusModuleAgent {
    // -- consensus messages and the position engine (P3-6a) ----------------

    /// `ConsensusAdapter.onFragment`'s switch (`ConsensusAdapter.java:92-340`),
    /// dispatched on the agent: every handler is an agent method, and most of
    /// them route to the election while one is in progress.
    fn dispatch_consensus_message(
        &mut self,
        message: ConsensusMessage,
    ) -> Result<usize, ClusterError> {
        let now_ns = self.clock.time_nanos();
        match message {
            ConsensusMessage::CanvassPosition {
                log_leadership_term_id,
                log_position,
                leadership_term_id,
                follower_member_id,
                protocol_version,
            } => self.on_canvass_position(
                log_leadership_term_id,
                log_position,
                leadership_term_id,
                follower_member_id,
                protocol_version,
                now_ns,
            ),
            ConsensusMessage::RequestVote {
                log_leadership_term_id,
                log_position,
                candidate_term_id,
                candidate_member_id,
                protocol_version,
            } => self.on_request_vote(
                log_leadership_term_id,
                log_position,
                candidate_term_id,
                candidate_member_id,
                protocol_version,
                now_ns,
            ),
            ConsensusMessage::Vote {
                log_leadership_term_id,
                log_position,
                candidate_term_id,
                candidate_member_id,
                follower_member_id,
                vote,
            } => self.on_vote(
                log_leadership_term_id,
                log_position,
                candidate_term_id,
                candidate_member_id,
                follower_member_id,
                vote,
            ),
            ConsensusMessage::NewLeadershipTerm {
                log_leadership_term_id,
                next_leadership_term_id,
                next_term_base_log_position,
                next_log_position,
                leadership_term_id,
                term_base_log_position,
                log_position,
                commit_position,
                leader_recording_id,
                timestamp,
                leader_member_id,
                log_session_id,
                app_version,
                is_startup,
            } => self.on_new_leadership_term(
                log_leadership_term_id,
                next_leadership_term_id,
                next_term_base_log_position,
                next_log_position,
                leadership_term_id,
                term_base_log_position,
                log_position,
                commit_position,
                leader_recording_id,
                timestamp,
                leader_member_id,
                log_session_id,
                app_version,
                is_startup,
                now_ns,
            ),
            ConsensusMessage::AppendPosition {
                leadership_term_id,
                log_position,
                follower_member_id,
                flags,
            } => self.on_append_position(
                leadership_term_id,
                log_position,
                follower_member_id,
                flags,
                now_ns,
            ),
            ConsensusMessage::CommitPosition {
                leadership_term_id,
                log_position,
                leader_member_id,
            } => {
                self.on_commit_position(leadership_term_id, log_position, leader_member_id, now_ns)
            }
            ConsensusMessage::CatchupPosition {
                leadership_term_id,
                log_position,
                follower_member_id,
                catchup_endpoint,
            } => self.on_catchup_position(
                leadership_term_id,
                log_position,
                follower_member_id,
                &catchup_endpoint,
            ),
            ConsensusMessage::StopCatchup {
                leadership_term_id,
                follower_member_id,
            } => self.on_stop_catchup(leadership_term_id, follower_member_id),
            ConsensusMessage::TerminationPosition {
                leadership_term_id,
                log_position,
            } => {
                self.on_termination_position(leadership_term_id, log_position);
                Ok(1)
            }
            ConsensusMessage::TerminationAck {
                leadership_term_id,
                log_position,
                member_id,
            } => self
                .on_termination_ack(leadership_term_id, log_position, member_id)
                .map(|()| 1),
            ConsensusMessage::BackupQuery { .. }
            | ConsensusMessage::ChallengeResponse { .. }
            | ConsensusMessage::HeartbeatRequest { .. }
            | ConsensusMessage::StandbySnapshot { .. } => {
                // Backup queries, the challenge response and the standby
                // snapshot are ClusterBackup's (P3-9) and premium's; a member
                // that hears one is heard, counted and not answered.
                self.report_counted_error();
                Ok(0)
            }
        }
    }

    /// Run `f` with the election taken out, the publisher taken out, and both
    /// put back — the take-and-put-back pattern the duty cycle uses, for the
    /// handlers the election owns while it runs.
    fn with_election<F>(&mut self, f: F) -> Result<usize, ClusterError>
    where
        F: FnOnce(
            &mut Election,
            &mut ConsensusModuleAgent,
            &mut ConsensusPublisher,
        ) -> Result<usize, ClusterError>,
    {
        let Some(mut election) = self.election.take() else {
            return Ok(0);
        };
        self.publisher.refresh_position_limits(&self.client);
        let mut publisher = std::mem::take(&mut self.publisher);
        let result = f(&mut election, self, &mut publisher);
        self.publisher = publisher;
        if election.state() != ElectionState::Closed {
            self.election = Some(election);
        }
        result
    }

    /// `onCanvassPosition` (`:902-960`).
    fn on_canvass_position(
        &mut self,
        log_leadership_term_id: i64,
        log_position: i64,
        leadership_term_id: i64,
        follower_member_id: i32,
        protocol_version: i32,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        let _ = now_ns;
        self.check_follower_for_consensus_publication(follower_member_id);

        if self.election.is_some() {
            return self.with_election(|election, host, sender| {
                election.on_canvass_position(
                    host,
                    sender,
                    log_leadership_term_id,
                    log_position,
                    leadership_term_id,
                    follower_member_id,
                    protocol_version,
                )?;
                Ok(1)
            });
        }

        if self.role != Role::Leader
            || !self
                .members
                .iter()
                .any(|member| member.id == follower_member_id)
            || log_leadership_term_id > self.leadership_term_id
        {
            return Ok(0);
        }

        self.update_member_log_position(follower_member_id, log_leadership_term_id, log_position);
        // `stopExistingCatchupReplay` is the catchup path (P3-6b).

        if log_leadership_term_id < self.leadership_term_id {
            let term_base_log_position = self
                .recording_log
                .find_term_entry(self.leadership_term_id)
                .map_or(0, |entry| entry.term_base_log_position);
            let next = self
                .recording_log
                .find_term_entry(log_leadership_term_id + 1)
                .map(|entry| {
                    (
                        entry.leadership_term_id,
                        entry.term_base_log_position,
                        entry.log_position,
                    )
                });
            let (next_leadership_term_id, next_term_base, next_log_position) = match next {
                Some((term, base, position)) => (term, base, position),
                None => (
                    self.leadership_term_id,
                    term_base_log_position,
                    NULL_POSITION,
                ),
            };
            let log_position_now = self.log_publisher.position();
            let commit_position = self.commit_position_value();
            let leadership_term_id = self.leadership_term_id;
            let member_id = self.config.member_id;
            let timestamp = self.clock.time();
            let log_session_id = self.log_publication_session_id;
            let log_recording_id = self.log_recording_id;
            let app_version = self.config.app_version;

            self.publisher.refresh_position_limits(&self.client);
            self.publisher.new_leadership_term(
                follower_member_id,
                log_leadership_term_id,
                next_leadership_term_id,
                next_term_base,
                next_log_position,
                leadership_term_id,
                term_base_log_position,
                log_position_now,
                commit_position,
                log_recording_id,
                timestamp,
                member_id,
                log_session_id,
                app_version,
                false,
            );
            if let Some(error) = self.publisher.take_error() {
                return Err(error);
            }
        }

        Ok(1)
    }

    /// `onRequestVote` (`:962-980`).
    fn on_request_vote(
        &mut self,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_term_id: i64,
        candidate_id: i32,
        protocol_version: i32,
        _now_ns: i64,
    ) -> Result<usize, ClusterError> {
        if self.election.is_some() {
            return self.with_election(|election, host, sender| {
                election.on_request_vote(
                    host,
                    sender,
                    log_leadership_term_id,
                    log_position,
                    candidate_term_id,
                    candidate_id,
                    protocol_version,
                )?;
                Ok(1)
            });
        }

        if candidate_term_id > self.leadership_term_id {
            self.enter_election(
                false,
                &format!(
                    "unexpected vote request: this.leadershipTermId={} candidateTermId={candidate_term_id}",
                    self.leadership_term_id
                ),
            )
        } else {
            Ok(0)
        }
    }

    /// `onVote` (`:982-997`).
    fn on_vote(
        &mut self,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_term_id: i64,
        candidate_id: i32,
        voter_id: i32,
        vote: bool,
    ) -> Result<usize, ClusterError> {
        if self.election.is_some() {
            return self.with_election(|election, host, _sender| {
                election.on_vote(
                    host,
                    log_leadership_term_id,
                    log_position,
                    candidate_term_id,
                    candidate_id,
                    voter_id,
                    vote,
                )?;
                Ok(1)
            });
        }
        Ok(0)
    }

    /// `onNewLeadershipTerm` (`:999-1076`).
    #[allow(clippy::too_many_arguments)]
    fn on_new_leadership_term(
        &mut self,
        log_leadership_term_id: i64,
        next_leadership_term_id: i64,
        next_term_base_log_position: i64,
        next_log_position: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        commit_position: i64,
        leader_recording_id: i64,
        timestamp: i64,
        leader_member_id: i32,
        log_session_id: i32,
        _app_version: i32,
        is_startup: bool,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        let leader_member_id_changed = self.leader_member_id();

        if self.election.is_some() {
            return self.with_election(|election, host, _sender| {
                election.on_new_leadership_term(
                    host,
                    log_leadership_term_id,
                    next_leadership_term_id,
                    next_term_base_log_position,
                    next_log_position,
                    leadership_term_id,
                    term_base_log_position,
                    log_position,
                    commit_position,
                    leader_recording_id,
                    timestamp,
                    leader_member_id,
                    log_session_id,
                    is_startup,
                )?;
                Ok(1)
            });
        }

        if self.role == Role::Follower
            && leadership_term_id == self.leadership_term_id
            && leader_member_id == leader_member_id_changed
        {
            self.notified_commit_position = self.notified_commit_position.max(commit_position);
            self.time_of_last_log_update_ns = now_ns;
            Ok(1)
        } else if leadership_term_id > self.leadership_term_id {
            self.enter_election(
                false,
                &format!(
                    "unexpected new leadership term event: this.leadershipTermId={} newLeadershipTermId={leadership_term_id}",
                    self.leadership_term_id
                ),
            )
        } else {
            Ok(0)
        }
    }

    /// `onAppendPosition` (`:1078-1098`).
    fn on_append_position(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        flags: i16,
        _now_ns: i64,
    ) -> Result<usize, ClusterError> {
        if self.election.is_some() {
            return self.with_election(|election, host, _sender| {
                election.on_append_position(
                    host,
                    leadership_term_id,
                    log_position,
                    follower_member_id,
                    flags,
                )?;
                Ok(1)
            });
        }

        if leadership_term_id <= self.leadership_term_id
            && self.role == Role::Leader
            && self
                .members
                .iter()
                .any(|member| member.id == follower_member_id)
        {
            self.update_member_log_position(follower_member_id, leadership_term_id, log_position);
            self.track_catchup_completion(follower_member_id, leadership_term_id, flags)?;
            return Ok(1);
        }
        Ok(0)
    }

    /// `onCommitPosition` (`:1108-1145`).
    fn on_commit_position(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        leader_member_id: i32,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        if leadership_term_id >= self.leadership_term_id {
            self.time_of_last_leader_update_ns = now_ns;
        }

        if self.election.is_some() {
            return self.with_election(|election, host, _sender| {
                election.on_commit_position(
                    host,
                    leadership_term_id,
                    log_position,
                    leader_member_id,
                )?;
                Ok(1)
            });
        }

        if leadership_term_id == self.leadership_term_id {
            if self.leader_member_id() == leader_member_id && self.role == Role::Follower {
                self.notified_commit_position = self.notified_commit_position.max(log_position);
                self.time_of_last_log_update_ns = now_ns;
                return Ok(1);
            }
            Ok(0)
        } else if leadership_term_id > self.leadership_term_id {
            self.enter_election(
                false,
                &format!(
                    "unexpected commit position from new leader - this.leadershipTermId={} newLeadershipTermId={leadership_term_id}",
                    self.leadership_term_id
                ),
            )
        } else {
            Ok(0)
        }
    }

    /// `onCatchupPosition` (`:1147-1168`): the leader starts an archive replay
    /// for the follower. That replay stream is the catchup path, `P3-6b`;
    /// hearing the message is counted rather than silently assumed.
    fn on_catchup_position(
        &mut self,
        leadership_term_id: i64,
        _log_position: i64,
        _follower_member_id: i32,
        _catchup_endpoint: &str,
    ) -> Result<usize, ClusterError> {
        if leadership_term_id <= self.leadership_term_id && self.role == Role::Leader {
            self.report_counted_error();
            return Ok(0);
        }
        Ok(0)
    }

    /// `onStopCatchup` (`:1170-1181`): removing a catchup destination is
    /// `P3-6b`'s.
    fn on_stop_catchup(
        &mut self,
        _leadership_term_id: i64,
        _follower_member_id: i32,
    ) -> Result<usize, ClusterError> {
        Ok(0)
    }

    /// `updateFollowerPosition` (`:2753-2785`): the follower's append position
    /// is also its heartbeat — there is no separate ping.
    fn update_follower_position_internal(&mut self, now_ns: i64) -> Result<usize, ClusterError> {
        let recorded_position = match self.append_position {
            Some(_) => self.append_position_value(),
            None => self.log_recording_stop_position,
        };
        let position = recorded_position.max(self.last_append_position);

        if position > self.last_append_position
            || now_ns
                >= self.time_of_last_append_position_send_ns
                    + self.config.leader_heartbeat_interval_ns
        {
            self.publisher.refresh_position_limits(&self.client);
            let leader_member_id = self.leader_member_id();
            let sent = self.publisher.append_position(
                leader_member_id,
                self.leadership_term_id,
                position,
                self.config.member_id,
                0,
            );
            if let Some(error) = self.publisher.take_error() {
                return Err(error);
            }
            if sent {
                if position > self.last_append_position {
                    self.last_append_position = position;
                }
                self.time_of_last_append_position_send_ns = now_ns;
                return Ok(1);
            }
        }
        Ok(0)
    }

    /// `enterElection` (`:3059-3094`): the four triggers all land here.
    fn enter_election(
        &mut self,
        is_log_end_of_stream: bool,
        reason: &str,
    ) -> Result<usize, ClusterError> {
        if self.election.is_some() {
            return Err(ClusterError::Cluster("election in progress".to_owned()));
        }

        self.set_role(Role::Follower);

        let leadership_term_id = self.leadership_term_id;
        let term_base_log_position = self
            .recording_log
            .find_term_entry(leadership_term_id)
            .map_or(self.recovery_plan.last_term_base_log_position, |entry| {
                entry.term_base_log_position
            });
        let appended_position = match self.append_position {
            Some(_) => self.append_position_value(),
            None => self
                .recovery_plan
                .appended_log_position
                .max(self.log_recording_stop_position),
        };
        let commit_position = self.commit_position_value();

        // `ctx.countedErrorHandler().onError(new ClusterEvent(reason))`.
        self.report_counted_error();

        let member_ids: Vec<i32> = self.members.iter().map(|member| member.id).collect();
        let graceful_closed_leader_id = if is_log_end_of_stream {
            self.leader_member_id()
        } else {
            -1
        };
        let now_ns = self.clock.time_nanos();
        let election = Election::new(
            self.config.member_id,
            member_ids,
            false,
            graceful_closed_leader_id,
            leadership_term_id,
            term_base_log_position,
            commit_position,
            appended_position,
            now_ns,
            now_ns as u64,
        );
        let _ = reason;

        self.election = Some(election);
        increment_counter(&self.client, self.counters.election, 1);
        self.drive_election_once(now_ns)
    }
}

impl ConsensusModuleAgent {
    /// `ClusterMember.addConsensusPublications` (`ClusterMember.java:927-953`):
    /// one exclusive publication per other member, on the consensus channel
    /// with that member's endpoint.
    fn add_consensus_publications(&mut self) -> Result<(), ClusterError> {
        let this_member_id = self.config.member_id;
        for index in 0..self.members.len() {
            let member_id = self.members[index].id;
            if member_id == this_member_id {
                continue;
            }
            let channel = self.consensus_channel_for(member_id)?;
            self.ensure_consensus_publication(member_id, &channel)?;
        }
        Ok(())
    }

    /// The channel a publication to `member_id` goes on
    /// (`ClusterMember.addConsensusPublications`'s `channelUri.put(endpoint)`).
    fn consensus_channel_for(&self, member_id: i32) -> Result<String, ClusterError> {
        let endpoint = self
            .members
            .iter()
            .find(|member| member.id == member_id)
            .map(|member| member.consensus_endpoint.clone())
            .ok_or_else(|| {
                ClusterError::Cluster(format!("member id {member_id} is not in the member list"))
            })?;
        let mut uri = ChannelUri::parse(&self.config.consensus_channel).map_err(cluster)?;
        uri.put("endpoint", endpoint);
        Ok(uri.build())
    }

    fn ensure_consensus_publication(
        &mut self,
        member_id: i32,
        channel: &str,
    ) -> Result<(), ClusterError> {
        if self.publisher.has_publication(member_id) {
            return Ok(());
        }
        let registration_id = self.client.add_exclusive_publication(
            channel,
            self.config.consensus_stream_id,
            COMMAND_TIMEOUT,
        )?;
        let publication = self
            .client
            .take_exclusive_publication(registration_id)
            .ok_or_else(|| {
                ClusterError::Cluster("the consensus publication did not arrive".to_owned())
            })?;
        self.publisher.add_publication(member_id, publication);
        Ok(())
    }

    /// `checkFollowerForConsensusPublication` (`:3423-3440`): a member heard
    /// from for the first time gets a publication before it is answered. A
    /// failure is the reference's counted error, not a fatal one.
    fn check_follower_for_consensus_publication(&mut self, member_id: i32) {
        if member_id == self.config.member_id
            || self.publisher.has_publication(member_id)
            || !self.members.iter().any(|member| member.id == member_id)
        {
            return;
        }
        match self.consensus_channel_for(member_id) {
            Ok(channel) => {
                if self
                    .ensure_consensus_publication(member_id, &channel)
                    .is_err()
                {
                    self.report_counted_error();
                }
            }
            Err(_) => self.report_counted_error(),
        }
    }
}

impl ElectionHost for ConsensusModuleAgent {
    fn set_role(&mut self, new_role: Role) {
        self.set_role(new_role);
    }

    fn role(&self) -> Role {
        self.role
    }

    fn state(&self) -> ModuleState {
        self.state
    }

    fn log_recording_id(&self) -> i64 {
        self.log_recording_id
    }

    fn set_log_recording_id(&mut self, recording_id: i64) {
        if recording_id != NULL_VALUE {
            self.log_recording_id = recording_id;
        }
    }

    fn time_of_last_leader_update_ns(&self) -> i64 {
        self.time_of_last_leader_update_ns
    }

    fn quorum_position_bounded_by_leader_log(
        &mut self,
        leader_append_position: i64,
        now_ns: i64,
    ) -> i64 {
        let quorum_position = ClusterMember::quorum_position(
            &self.members,
            &mut self.ranked_positions,
            now_ns,
            self.config.leader_heartbeat_timeout_ns,
        );
        quorum_position.min(leader_append_position)
    }

    fn update_member_log_position(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        log_position: i64,
    ) {
        let now_ns = self.clock.time_nanos();
        if let Some(member) = self
            .members
            .iter_mut()
            .find(|member| member.id == member_id)
        {
            member.leadership_term_id = leadership_term_id;
            member.log_position = log_position;
            member.time_of_last_append_position_ns = now_ns;
        }
    }

    fn update_leader_position(
        &mut self,
        now_ns: i64,
        append_position: i64,
        quorum_position: i64,
    ) -> Result<usize, ClusterError> {
        let this_member_id = self.members[self.this_member_index].id;
        self.update_member_log_position(this_member_id, self.leadership_term_id, append_position);

        let leader_commit_position = self.commit_position_value();
        if quorum_position <= leader_commit_position
            && now_ns < self.time_of_last_log_update_ns + self.config.leader_heartbeat_interval_ns
        {
            return Ok(0);
        }

        if quorum_position < leader_commit_position
            && leader_commit_position > self.last_quorum_backtrack_commit_position
        {
            self.last_quorum_backtrack_commit_position = leader_commit_position;
            // The reference logs a `ClusterEvent` here and carries on
            // (`:2921-2926`); a WARN is not a control-flow error.
        }

        self.publish_commit_position(quorum_position, self.leadership_term_id)?;

        // `commitPosition.proposeMaxRelease(quorumPosition)`: the commit
        // position never moves backwards, even when the quorum answer does.
        let commit_position = self.counters.commit_position;
        let current = read_counter(&self.client, commit_position).unwrap_or(0);
        if quorum_position > current {
            write_counter(&self.client, commit_position, quorum_position);
        }
        self.sweep_uncommitted_entries_to(quorum_position);
        self.time_of_last_log_update_ns = now_ns;
        self.time_of_last_leader_update_ns = now_ns;
        Ok(1)
    }

    fn publish_commit_position(
        &mut self,
        commit_position: i64,
        leadership_term_id: i64,
    ) -> Result<(), ClusterError> {
        // `publishCommitPosition` (`:2938-2946`): every other member, with the
        // election's term and this member as the leader.
        let member_id = self.config.member_id;
        for index in 0..self.members.len() {
            let other = self.members[index].id;
            if other != member_id {
                self.publisher.commit_position(
                    other,
                    leadership_term_id,
                    commit_position,
                    member_id,
                );
            }
        }

        match self.publisher.take_error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn join_log_as_leader(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        log_session_id: i32,
        is_startup: bool,
    ) -> Result<(), ClusterError> {
        self.leadership_term_id = leadership_term_id;
        self.set_counter_value(self.counters.leadership_term, leadership_term_id);
        for tracker in self.trackers.iter_mut() {
            tracker.set_leadership_term_id(leadership_term_id);
        }

        let is_ipc = self.config.log_channel.starts_with("aeron:ipc");
        let channel = format!(
            "{}?tags={}|session-id={}|alias=log",
            if is_ipc { "aeron:ipc" } else { "aeron:udp" },
            self.log_publication_channel_tag,
            log_session_id
        );

        self.start_log_recording(&channel, self.config.log_stream_id, true)?;

        while !self.try_create_append_position(log_session_id)? {
            self.idle()?;
        }

        // `awaitServicesReady` (`:1801-1838`): tell the services where the log
        // is and wait for every one of them to answer at that position.
        self.await_services_ready(
            &channel,
            self.config.log_stream_id,
            log_session_id,
            log_position,
            i64::MAX,
            is_startup,
            Role::Leader,
        )?;

        self.live_log_destination = if is_ipc {
            channel
        } else {
            format!("aeron-spy:{channel}")
        };
        self.last_append_position = log_position;

        Ok(())
    }

    /// `tryJoinLogAsFollower` (`:1771-1799`): record the subscription the
    /// image arrived on, find the recording's position counter, and tell the
    /// services where the log starts.
    ///
    /// The `LogAdapter` the reference attaches here — the reader that applies
    /// the stream to the module's state — is `P3-6b`; the recording and the
    /// append position it makes are what a follower needs to take part in the
    /// quorum, and that is this slice.
    fn try_join_log_as_follower(
        &mut self,
        image: deepmsg_client::image::Image,
        is_leader_startup: bool,
        now_ns: i64,
    ) -> Result<bool, ClusterError> {
        let session_id = image.session_id();
        let join_position = image.join_position();

        if self.log_subscription_id == NULL_VALUE {
            let channel = self.log_subscription_channel.clone();
            let stream_id = self.config.log_stream_id;
            self.start_log_recording(&channel, stream_id, false)?;
        }

        // The image is kept **before** the append position is looked for: the
        // client hands the image over once (`take_image` removes it from the
        // subscription), and the archive's recording counter appears a cycle or
        // two after the recording starts — so a failed attempt that dropped the
        // image would leave `followerLogAwait` waiting for an image it already
        // had. The reference keeps the same `Image` object in the subscription
        // for the life of the term; this is that object held here.
        self.taken_log_image = Some(image);

        if self.try_create_append_position(session_id)? {
            self.last_append_position = join_position;
            self.time_of_last_append_position_send_ns = now_ns;

            let channel = self.log_subscription_channel.clone();
            let stream_id = self.config.log_stream_id;
            self.await_services_ready(
                &channel,
                stream_id,
                session_id,
                join_position,
                i64::MAX,
                is_leader_startup,
                Role::Follower,
            )?;
            return Ok(true);
        }

        Ok(false)
    }

    fn add_log_publication(&mut self, append_position: i64) -> Result<usize, ClusterError> {
        ConsensusModuleAgent::add_log_publication(self, append_position)
    }

    fn log_session_id(&self) -> i32 {
        self.log_publication_session_id
    }

    fn new_log_replay(
        &mut self,
        log_position: i64,
        append_position: i64,
    ) -> Result<LogReplay, ClusterError> {
        ConsensusModuleAgent::start_log_replay(self, log_position, append_position)
    }

    fn new_log_replication(
        &mut self,
        _leader_archive_endpoint: &str,
        _response_archive_endpoint: Option<&str>,
        _leader_recording_id: i64,
        _stop_position: i64,
        _now_ns: i64,
    ) -> Result<RecordingReplication, ClusterError> {
        Err(ClusterError::Cluster("log replication is P3-6b".to_owned()))
    }

    fn truncate_log_entry(
        &mut self,
        _leadership_term_id: i64,
        _log_position: i64,
    ) -> Result<(), ClusterError> {
        Err(ClusterError::Cluster(
            "truncating a log entry is P3-6b".to_owned(),
        ))
    }

    fn append_new_leadership_term_event(&mut self, now_ns: i64) -> Result<bool, ClusterError> {
        let Some(publication) = self.log_publisher.publication() else {
            return Err(ClusterError::Cluster(
                "the log publication has not been created".to_owned(),
            ));
        };
        let position_limit = self.position_limit(publication);
        let term_base_log_position = self.append_position_value();

        self.log_publisher.append_new_leadership_term_event(
            position_limit,
            self.leadership_term_id,
            self.clock.timestamp_from_nanos(now_ns),
            term_base_log_position,
            self.members[self.this_member_index].id,
            self.log_publication_session_id,
            ClusterTimeUnit::MILLIS,
            self.config.app_version,
        )
    }

    fn prepare_for_new_leadership(
        &mut self,
        log_position: i64,
        now_ns: i64,
    ) -> Result<i64, ClusterError> {
        self.set_role(Role::Follower);
        self.live_log_destination.clear();
        self.catchup_log_destination.clear();
        self.last_quorum_backtrack_commit_position = NULL_POSITION;

        // `prepareForNewLeadership` (`:1382`): an operator switch is only
        // honoured by a leader, so losing leadership disarms it. (The
        // reference also disconnects the ingress and log adapters here; those
        // are P3-6b's.)
        ClusterControlToggle::deactivate(&self.client, self.counters.control_toggle);

        let had_recording = self.log_recording_id != NULL_VALUE;
        let last_append_position = self.append_position_value();
        self.try_stop_log_recording()?;

        if had_recording {
            self.recovery_plan = create_recovery_plan(
                &self.recording_log,
                &mut self.archive,
                &mut self.client,
                self.config.service_count,
                self.log_recording_id,
            )?;
            write_counter(&self.client, self.counters.commit_position, log_position);
            self.last_append_position = last_append_position;
            self.time_of_last_leader_update_ns = now_ns;
        }

        self.restore_uncommitted_entries(log_position);

        Ok(self.last_append_position)
    }

    fn catchup_initiated(&mut self, _now_ns: i64) -> Result<(), ClusterError> {
        Err(ClusterError::Cluster("catchup is P3-6b".to_owned()))
    }

    fn catchup_poll(&mut self, _limit_position: i64, _now_ns: i64) -> Result<usize, ClusterError> {
        Err(ClusterError::Cluster("catchup is P3-6b".to_owned()))
    }

    fn is_catchup_near_live(&mut self, _position: i64) -> bool {
        false
    }

    fn track_catchup_completion(
        &mut self,
        member_id: i32,
        _leadership_term_id: i64,
        append_position_flags: i16,
    ) -> Result<(), ClusterError> {
        // `trackCatchupCompletion` (`ConsensusModuleAgent.java:1946-1949`) is a
        // no-op unless this follower has a replay in progress or its append
        // position carries the catchup flag — every ordinary heartbeat goes
        // through here, so an unconditional refusal would fail the steady
        // state. Starting and stopping the replay is P3-6b.
        let has_catchup_session = self
            .members
            .iter()
            .any(|member| member.id == member_id && member.catchup_replay_session_id != NULL_VALUE);
        let is_catchup = 0 != (APPEND_POSITION_FLAG_CATCHUP & append_position_flags);

        if has_catchup_session || is_catchup {
            return Err(ClusterError::Cluster("catchup is P3-6b".to_owned()));
        }

        Ok(())
    }

    fn stop_all_catchups(&mut self) -> Result<(), ClusterError> {
        // `stopAllCatchups` (`ConsensusModuleAgent.java:1900-1912`): stop the
        // replay for every member that has one. Nothing starts one until
        // P3-6b, so the list is empty; the members' fields are cleared so a
        // later slice starts from the reference's state.
        for member in &mut self.members {
            member.catchup_replay_session_id = NULL_VALUE;
        }
        Ok(())
    }

    fn live_log_destination(&self) -> &str {
        &self.live_log_destination
    }

    fn set_live_log_destination(&mut self, destination: &str) -> Result<(), ClusterError> {
        self.live_log_destination = destination.to_owned();
        Ok(())
    }

    fn catchup_log_destination(&self) -> &str {
        &self.catchup_log_destination
    }

    fn set_catchup_log_destination(&mut self, destination: &str) -> Result<(), ClusterError> {
        self.catchup_log_destination = destination.to_owned();
        Ok(())
    }

    fn await_local_sockets_closed(&mut self, _registration_id: i64) -> Result<(), ClusterError> {
        Err(ClusterError::Cluster(
            "waiting for local sockets to close is P3-6b".to_owned(),
        ))
    }

    fn set_leader_member_id(&mut self, leader_member_id: i32) {
        // `updateMemberDetails(election.leader())` (`:1930`, `:2370-2381`).
        ClusterMember::set_is_leader(&mut self.members, leader_member_id);
    }

    fn set_leadership_term_id(&mut self, leadership_term_id: i64) {
        // `electionComplete`'s first line (`ConsensusModuleAgent.java:1907`).
        // For a leader `join_log_as_leader` has already set it; for a follower
        // this is the only place the steady state ever learns its term.
        self.leadership_term_id = leadership_term_id;
    }

    fn election_complete(&mut self, now_ns: i64) -> Result<(), ClusterError> {
        self.set_counter_value(self.counters.leadership_term, self.leadership_term_id);

        if self.role == Role::Leader {
            self.time_of_last_log_update_ns = now_ns - self.config.leader_heartbeat_interval_ns;
            // `electionComplete` (`:1909`): the wheel's tick moves to the
            // cluster's clock in one step, not one tick per poll.
            self.timer.current_time(self.clock.time());
            for tracker in self.trackers.iter_mut() {
                tracker.set_leadership_term_id(self.leadership_term_id);
            }
            // `updateMemberDetails` (`:2370-2381`) marks the leader on every
            // member; `ClusterTermination.canTerminate` reads that flag to know
            // it is not waiting for itself. A follower learns the leader id
            // from the log replication (P3-6b) and has nothing to terminate.
            ClusterMember::set_is_leader(&mut self.members, self.config.member_id);
            ClusterControlToggle::activate(&self.client, self.counters.control_toggle);
        } else {
            self.time_of_last_log_update_ns = now_ns;
            self.live_log_destination.clear();
        }
        NodeControlToggle::activate(&self.client, self.counters.node_control_toggle);

        self.recovery_plan = create_recovery_plan(
            &self.recording_log,
            &mut self.archive,
            &mut self.client,
            self.config.service_count,
            self.log_recording_id,
        )?;

        // The reference uses `election.logPosition()` here; at LEADER_READY the
        // election has already set it to its append position (`Election.java:864`),
        // which is the same number as the append counter.
        let log_position = self.append_position_value();
        self.notified_commit_position = self.notified_commit_position.max(log_position);
        write_counter(&self.client, self.counters.commit_position, log_position);

        self.this_member_index = self
            .members
            .iter()
            .position(|member| member.id == self.config.member_id)
            .unwrap_or(self.this_member_index);

        // `electionComplete` (`ConsensusModuleAgent.java:1905-1944`):
        // `prepareSessionsForNewTerm(isLeaderStartup)` and then `connectIngress`.
        // There are no sessions to prepare before P3-4d loads a snapshot, but
        // the order is kept so that slice has the call where the reference has
        // it.
        let is_leader_startup = self.node_startup_election;
        self.node_startup_election = false;
        self.sessions
            .prepare_sessions_for_new_term(is_leader_startup, now_ns);
        self.connect_ingress()?;

        Ok(())
    }

    fn poll_archive_events(&mut self) -> usize {
        // The reference polls its `RecordingSignalPoller` here (`:2061-2075`).
        // This build's archive client delivers descriptors inline; the recording
        // position the election waits for is read from its counter
        // (`try_create_append_position`), so there is no signal backlog to
        // drain. `client.poll()` still runs, because the driver's answers and
        // the heartbeat both need it.
        usize::from(self.client.poll())
    }

    fn node_state(&mut self) -> &mut NodeStateFile {
        &mut self.node_state
    }

    fn recording_log(&mut self) -> &mut RecordingLog {
        &mut self.recording_log
    }

    fn now_ns(&mut self) -> i64 {
        self.clock.time_nanos()
    }

    fn epoch_ms(&self) -> i64 {
        self.clock.time_millis()
    }

    fn nanos_from_timestamp(&self, timestamp: i64) -> i64 {
        match self.clock.time_unit() {
            ClusterTimeUnit::MILLIS | ClusterTimeUnit::NullVal => {
                timestamp.saturating_mul(1_000_000)
            }
            ClusterTimeUnit::MICROS => timestamp.saturating_mul(1_000),
            ClusterTimeUnit::NANOS => timestamp,
        }
    }

    fn timestamp_from_nanos(&self, now_ns: i64) -> i64 {
        self.clock.timestamp_from_nanos(now_ns)
    }

    fn appointed_leader_id(&self) -> i32 {
        self.config.appointed_leader_id
    }

    fn startup_canvass_timeout_ns(&self) -> i64 {
        self.config.startup_canvass_timeout_ns
    }

    fn leader_heartbeat_timeout_ns(&self) -> i64 {
        self.config.leader_heartbeat_timeout_ns
    }

    fn leader_heartbeat_interval_ns(&self) -> i64 {
        self.config.leader_heartbeat_interval_ns
    }

    fn election_timeout_ns(&self) -> i64 {
        self.config.election_timeout_ns
    }

    fn election_status_interval_ns(&self) -> i64 {
        self.config.election_status_interval_ns
    }

    fn has_quorum_at_position(
        &mut self,
        leadership_term_id: i64,
        position: i64,
        now_ns: i64,
        timeout_ns: i64,
    ) -> bool {
        member::has_quorum_at_position(
            &self.members,
            leadership_term_id,
            position,
            now_ns,
            timeout_ns,
        )
    }

    // ---- P3-5b: the member table and the log subscription ----------------

    fn reset_members(&mut self, leadership_term_id: i64, append_position: i64) {
        ClusterMember::reset_all(&mut self.members);
        let index = self.this_member_index;
        self.members[index].leadership_term_id = leadership_term_id;
        self.members[index].log_position = append_position;
    }

    fn become_candidate(&mut self, candidate_term_id: i64, candidate_member_id: i32) {
        ClusterMember::become_candidate(&mut self.members, candidate_term_id, candidate_member_id);
    }

    fn member_ballot_sent(&self, member_id: i32) -> bool {
        self.members
            .iter()
            .find(|member| member.id == member_id)
            .is_some_and(|member| member.is_ballot_sent)
    }

    fn set_member_ballot_sent(&mut self, member_id: i32, sent: bool) {
        if let Some(member) = self
            .members
            .iter_mut()
            .find(|member| member.id == member_id)
        {
            member.is_ballot_sent = sent;
        }
    }

    fn set_member_vote(&mut self, member_id: i32, candidate_term_id: i64, vote: Option<bool>) {
        if let Some(member) = self
            .members
            .iter_mut()
            .find(|member| member.id == member_id)
        {
            member.candidate_term_id = candidate_term_id;
            member.vote = vote;
        }
    }

    fn unanimous_candidate(
        &self,
        candidate_member_id: i32,
        graceful_closed_leader_id: i32,
    ) -> bool {
        self.members
            .iter()
            .find(|member| member.id == candidate_member_id)
            .is_some_and(|candidate| {
                ClusterMember::is_unanimous_candidate(
                    &self.members,
                    candidate,
                    graceful_closed_leader_id,
                )
            })
    }

    fn quorum_candidate(&self, candidate_member_id: i32) -> bool {
        self.members
            .iter()
            .find(|member| member.id == candidate_member_id)
            .is_some_and(|candidate| member::is_quorum_candidate(&self.members, candidate))
    }

    fn unanimous_leader(&self, candidate_term_id: i64, graceful_closed_leader_id: i32) -> bool {
        ClusterMember::is_unanimous_leader(
            &self.members,
            candidate_term_id,
            graceful_closed_leader_id,
        )
    }

    fn quorum_leader(&self, candidate_term_id: i64) -> bool {
        member::is_quorum_leader(&self.members, candidate_term_id)
    }

    fn member_has_publication(&self, member_id: i32) -> bool {
        self.publisher.has_publication(member_id)
    }

    fn try_add_member_publication(&mut self, member_id: i32) -> Result<(), ClusterError> {
        // `ClusterMember.tryAddPublication` (`:777-800`): a registration that
        // fails is the reference's **counted** error, not a fatal one — the
        // next canvass round tries again. The `Result` is the frozen seam's,
        // and it is always `Ok`.
        match self.consensus_channel_for(member_id) {
            Ok(channel) => {
                if self
                    .ensure_consensus_publication(member_id, &channel)
                    .is_err()
                {
                    self.report_counted_error();
                }
            }
            Err(_) => self.report_counted_error(),
        }
        Ok(())
    }

    fn consensus_stream_id(&self) -> i32 {
        self.config.consensus_stream_id
    }

    fn app_version(&self) -> i32 {
        self.config.app_version
    }

    fn file_sync_level(&self) -> i32 {
        self.config.file_sync_level
    }

    fn log_channel(&self) -> &str {
        &self.config.log_channel
    }

    fn log_stream_id(&self) -> i32 {
        self.config.log_stream_id
    }

    fn log_is_mdc(&self) -> bool {
        // `ConsensusModule.Context.isLogMdc` (`:2145`): a UDP log is manual
        // multi-destination control unless the channel names one endpoint.
        // This is the configured channel's shape, not a property the caller
        // writes: `add_log_publication` puts `control-mode=manual` on the
        // publication itself, the way `addLogPublication` does (`:1650-1653`).
        ChannelUri::parse(&self.config.log_channel)
            .ok()
            .is_some_and(|uri| uri.media() == "udp" && uri.get("endpoint").is_none())
    }

    fn member_log_endpoint(&self, member_id: i32) -> Option<String> {
        self.members
            .iter()
            .find(|member| member.id == member_id)
            .map(|member| member.log_endpoint.clone())
    }

    fn member_catchup_endpoint(&self, member_id: i32) -> Option<String> {
        self.members
            .iter()
            .find(|member| member.id == member_id)
            .map(|member| member.catchup_endpoint.clone())
    }

    fn member_archive_endpoint(&self, member_id: i32) -> Option<String> {
        self.members
            .iter()
            .find(|member| member.id == member_id)
            .map(|member| member.archive_endpoint.clone())
    }

    fn member_archive_response_endpoint(&self, member_id: i32) -> Option<String> {
        self.members
            .iter()
            .find(|member| member.id == member_id)
            .and_then(|member| member.archive_response_endpoint.clone())
    }

    fn add_log_subscription(&mut self, log_session_id: i32) -> Result<i64, ClusterError> {
        let first = self.client.next_correlation_id().map_err(cluster)?;
        let second = self.client.next_correlation_id().map_err(cluster)?;
        // `Election.addFollowerSubscription` (`:1366-1383`): UDP media, manual
        // control mode, the leader's log session id, one group, no rejoin.
        let channel = format!(
            "aeron:udp?tags={first},{second}|control-mode=manual|session-id={log_session_id}|group=true|rejoin=false|alias=log-cm"
        );
        let registration_id =
            self.client
                .add_subscription(&channel, self.config.log_stream_id, COMMAND_TIMEOUT)?;
        self.log_subscription_channel = channel;
        Ok(registration_id)
    }

    fn remove_log_subscription(&mut self, registration_id: i64) -> Result<(), ClusterError> {
        self.client
            .remove_subscription(registration_id, COMMAND_TIMEOUT)
            .map_err(cluster)
    }

    fn add_log_destination(
        &mut self,
        registration_id: i64,
        destination: &str,
    ) -> Result<(), ClusterError> {
        self.client
            .add_rcv_destination(registration_id, destination, COMMAND_TIMEOUT)
            .map(|_| ())
            .map_err(cluster)
    }

    fn log_subscription_resolved_endpoint(&self, registration_id: i64) -> Option<String> {
        let counters = self.client.counters_reader()?;
        self.client
            .subscription(registration_id)?
            .resolved_endpoint(&counters)
    }

    fn log_subscription_channel_errored(&self, registration_id: i64) -> bool {
        let Some(counters) = self.client.counters_reader() else {
            return false;
        };
        let Some(status_id) = self
            .client
            .subscription(registration_id)
            .and_then(deepmsg_client::subscription::Subscription::channel_status_indicator_id)
        else {
            return false;
        };
        counters.value(status_id) == Some(-1) // `CHANNEL_ENDPOINT_STATUS_ERRORED`
    }

    fn log_image_by_session(
        &mut self,
        registration_id: i64,
        session_id: i32,
    ) -> Option<deepmsg_client::image::Image> {
        if let Some(image) = self.taken_log_image.take() {
            return Some(image);
        }
        let publication_id = self
            .client
            .subscription(registration_id)?
            .image_by_session_id(session_id)?
            .registration_id();
        self.client.take_image(registration_id, publication_id)
    }

    fn commit_position_value(&self) -> i64 {
        self.commit_position_value()
    }

    fn set_election_state(&mut self, state: ElectionState) {
        self.set_counter_value(self.counters.election_state, i64::from(state.code()));
    }
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// `ArchiveException.STORAGE_SPACE` (`ArchiveException.java:84`): the one
/// archive refusal `doWork` treats as terminal (`:446-450`).
/// Whether a channel's media is UDP (`ChannelUri.isUdp()`).
fn channel_uri_media_is_udp(channel: &str) -> bool {
    ChannelUri::parse(channel).is_ok_and(|uri| uri.media() == "udp")
}

/// `ChannelUri.replacePortWithWildcard` (`ChannelUri.java:664-678`): the
/// endpoint with its port replaced by `0`, or `None` when it names no port.
fn replace_port_with_wildcard(endpoint: &str) -> Option<String> {
    let index = endpoint.rfind(':')?;
    Some(format!("{}0", &endpoint[..=index]))
}

fn storage_space_refusal(error: &ClusterError) -> bool {
    matches!(error, ClusterError::ArchiveRefused { code: 11, .. })
}

fn cluster(error: impl std::fmt::Display) -> ClusterError {
    ClusterError::Cluster(error.to_string())
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

/// `ClusterCounters.allocate*` for the counters a module owns.
fn allocate_counters(
    client: &mut Client,
    config: &ConsensusModuleConfig,
) -> Result<ClusterCounters, ClusterError> {
    let cluster_id = config.cluster_id;

    // The reference refuses to start a second module for the same cluster id
    // (`ConsensusModule.java:1941-1945`), by the counter that names it.
    if let Some(counters) = client.counters_reader() {
        if counters::find_cluster_counter(
            &counters,
            CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
            cluster_id,
        )
        .is_some()
        {
            return Err(ClusterError::Cluster(format!(
                "existing consensus module detected for clusterId={cluster_id}"
            )));
        }
    }

    let allocate = |client: &mut Client,
                    type_id: i32,
                    spec: counters::CounterSpec|
     -> Result<Counter, ClusterError> {
        client
            .add_counter(type_id, spec.key(), spec.label(), COMMAND_TIMEOUT)
            .map_err(cluster)
    };

    let error = allocate(
        client,
        CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID,
        counters::versioned_cluster_counter("Cluster Errors", cluster_id, "1.53.2", "deepmsg")?,
    )?;

    let module_state = allocate(
        client,
        CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
        counters::cluster_counter("Consensus Module state", cluster_id)?,
    )?;
    let election_state = allocate(
        client,
        CLUSTER_ELECTION_STATE_TYPE_ID,
        counters::cluster_counter("Cluster election state", cluster_id)?,
    )?;
    let election = allocate(
        client,
        CLUSTER_ELECTION_COUNT_TYPE_ID,
        counters::cluster_counter("Cluster election count", cluster_id)?,
    )?;
    let leadership_term = allocate(
        client,
        CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID,
        counters::cluster_counter("Cluster leadership term id", cluster_id)?,
    )?;
    let node_role = allocate(
        client,
        CLUSTER_NODE_ROLE_TYPE_ID,
        counters::cluster_counter("Cluster node role", cluster_id)?,
    )?;
    let commit_position = allocate(
        client,
        CLUSTER_COMMIT_POSITION_TYPE_ID,
        counters::cluster_counter("Cluster commit-pos:", cluster_id)?,
    )?;
    let control_toggle = allocate(
        client,
        CLUSTER_CONTROL_TOGGLE_TYPE_ID,
        counters::cluster_counter("Cluster control toggle", cluster_id)?,
    )?;
    let node_control_toggle = allocate(
        client,
        NODE_CONTROL_TOGGLE_TYPE_ID,
        counters::cluster_counter("Node control toggle", cluster_id)?,
    )?;
    let snapshot = allocate(
        client,
        CLUSTER_SNAPSHOT_COUNTER_TYPE_ID,
        counters::cluster_counter("Cluster snapshot count", cluster_id)?,
    )?;
    let timed_out_client = allocate(
        client,
        deepmsg_cluster_common::counters::CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID,
        counters::cluster_counter("Cluster timed out client count", cluster_id)?,
    )?;

    let max_cycle_time = allocate(
        client,
        CLUSTER_MAX_CYCLE_TIME_TYPE_ID,
        counters::cluster_counter("Cluster max cycle time in ns", cluster_id)?,
    )?;
    let cycle_threshold_exceeded = allocate(
        client,
        CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
        counters::cluster_counter(
            &format!(
                "Cluster work cycle time exceeded count: threshold={}",
                format_duration_ns(config.cycle_threshold_ns)
            ),
            cluster_id,
        )?,
    )?;

    let total_snapshot_duration = allocate(
        client,
        CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID,
        counters::cluster_counter("Total max snapshot duration in ns", cluster_id)?,
    )?;
    let total_snapshot_threshold_exceeded = allocate(
        client,
        CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID,
        counters::cluster_counter(
            &format!(
                "Total max snapshot duration exceeded count: threshold={}",
                format_duration_ns(config.snapshot_duration_threshold_ns)
            ),
            cluster_id,
        )?,
    )?;

    Ok(ClusterCounters {
        error,
        module_state,
        election_state,
        election,
        leadership_term,
        node_role,
        commit_position,
        control_toggle,
        node_control_toggle,
        snapshot,
        timed_out_client,
        max_cycle_time,
        cycle_threshold_exceeded,
        total_snapshot_duration,
        total_snapshot_threshold_exceeded,
    })
}

/// `SystemUtil.formatDuration`: the largest unit that divides the value exactly.
fn format_duration_ns(duration_ns: i64) -> String {
    for (unit, scale) in [("s", 1_000_000_000_i64), ("ms", 1_000_000), ("us", 1_000)] {
        if duration_ns % scale == 0 {
            return format!("{}{unit}", duration_ns / scale);
        }
    }
    format!("{duration_ns}ns")
}

/// The consensus channel's endpoint is the member's own when the channel does
/// not name one (`ConsensusModuleAgent.java:257-262`).
fn add_consensus_subscription(
    client: &mut Client,
    config: &ConsensusModuleConfig,
    this_member: &ClusterMember,
) -> Result<i64, ClusterError> {
    let mut channel = ChannelUri::parse(&config.consensus_channel).map_err(cluster)?;
    if channel.get("endpoint").is_none() {
        channel.put("endpoint", this_member.consensus_endpoint.clone());
    }
    client
        .add_subscription(
            &channel.build(),
            config.consensus_stream_id,
            COMMAND_TIMEOUT,
        )
        .map_err(cluster)
}

/// The archive's answer to `listRecording`, as the plan's six numbers.
fn list_recording(
    archive: &mut Archive,
    client: &mut Client,
    recording_id: i64,
) -> Option<RecordingExtent> {
    let mut extent = None;
    let _ = archive.list_recording(
        client,
        recording_id,
        &mut |descriptor: &RecordingDescriptor| {
            extent = Some(RecordingExtent {
                recording_id: descriptor.recording_id,
                start_position: descriptor.start_position,
                stop_position: descriptor.stop_position,
                initial_term_id: descriptor.initial_term_id,
                term_buffer_length: descriptor.term_buffer_length,
                mtu_length: descriptor.mtu_length,
                session_id: descriptor.session_id,
            });
        },
    );
    extent
}

fn create_recovery_plan(
    recording_log: &RecordingLog,
    archive: &mut Archive,
    client: &mut Client,
    service_count: i32,
    replicated_recording_id: i64,
) -> Result<RecoveryPlan, ClusterError> {
    recording_log
        .create_recovery_plan(
            |recording_id| list_recording(archive, client, recording_id),
            service_count,
            replicated_recording_id,
        )
        .map_err(cluster)
}

/// `ChannelUri.initialPosition` (`ChannelUri.java:349-363`).
fn put_initial_position(
    channel: &mut ChannelUri,
    position: i64,
    initial_term_id: i32,
    term_length: i32,
) {
    let bits_to_shift = term_length.trailing_zeros();
    let term_id = ((position >> bits_to_shift) as i32) + initial_term_id;
    let term_offset = (position & (i64::from(term_length) - 1)) as i32;

    channel.put("init-term-id", initial_term_id.to_string());
    channel.put("term-id", term_id.to_string());
    channel.put("term-offset", term_offset.to_string());
    channel.put("term-length", term_length.to_string());
}

/// `id=endpoint,...` for the active members (`ClusterMember.ingressEndpoints`,
/// `ClusterMember.java:1239-1255`), which is what a redirect or a new-leader
/// event carries.
fn increment_counter(client: &Client, counter: Counter, delta: i64) {
    if let Some(counters) = client.counters_writable() {
        let value = counters.value(counter.counter_id()).unwrap_or(0);
        counters.set_value(counter.counter_id(), value + delta);
    }
}

fn ingress_endpoints(members: &[ClusterMember]) -> String {
    let mut out = String::new();
    for (index, member) in members.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&member.id.to_string());
        out.push('=');
        out.push_str(&member.ingress_endpoint);
    }
    out
}

/// `UdpChannel.isMulticastDestinationAddress` for the endpoint a URI names.
fn is_multicast_endpoint(uri: &ChannelUri) -> bool {
    let Some(endpoint) = uri.get("endpoint") else {
        return false;
    };
    let host = endpoint
        .rsplit_once(':')
        .map_or(endpoint, |(host, _port)| host);
    host.parse::<std::net::Ipv4Addr>()
        .map(|address| address.is_multicast())
        .unwrap_or(false)
}

/// `ConsensusModuleAgent.refineResponseChannel` (`ConsensusModuleAgent.java:3459-3475`).
///
/// `aeron.cluster.egress.channel`, when set, is a template: its parameters are
/// put onto a client's response channel. An IPC response channel is special —
/// it is replaced whole by the egress channel unless IPC ingress is allowed.
fn refine_response_channel(
    response_channel: &str,
    ipc_ingress_allowed: bool,
    egress_channel: Option<&str>,
) -> String {
    let Some(template) = egress_channel else {
        return response_channel.to_owned();
    };

    if response_channel.starts_with("aeron:ipc") {
        if ipc_ingress_allowed {
            response_channel.to_owned()
        } else {
            template.to_owned()
        }
    } else {
        let Ok(mut uri) = ChannelUri::parse(response_channel) else {
            return response_channel.to_owned();
        };
        if let Ok(template_uri) = ChannelUri::parse(template) {
            for (key, value) in template_uri.params() {
                uri.put(key.as_str(), value.clone());
            }
        }
        uri.build()
    }
}

/// An admin request read from ingress, carried out after the poll because the
/// response goes out on the client ([`AdminRequestType`]).
struct DeferredAdminRequest {
    cluster_session_id: i64,
    correlation_id: i64,
    request_type: AdminRequestType,
}

/// The consensus module as the ingress adapter sees it
/// (`ConsensusModuleAgent.on*`).
///
/// It borrows the agent's disjoint fields: the client is held by the poll, so
/// the two handlers that need it — a session close and an admin request, both of
/// which answer on the client — are queued and run immediately after the poll,
/// in the same duty cycle.
struct AgentIngressHandler<'a> {
    sessions: &'a mut SessionManager,
    log_publisher: &'a mut LogPublisher,
    role: Role,
    leadership_term_id: i64,
    position_limit: i64,
    now_ns: i64,
    ingress_ipc_allowed: bool,
    egress_channel: Option<&'a str>,
    ingress_endpoints: &'a str,
    deferred_closes: Vec<(i64, i64)>,
    deferred_admins: Vec<DeferredAdminRequest>,
    extension_messages: usize,
}

impl IngressHandler for AgentIngressHandler<'_> {
    fn on_extension_message(
        &mut self,
        _block_length: u16,
        _template_id: u16,
        _schema_id: u16,
        _version: u16,
        _payload: &[u8],
    ) -> deepmsg_client::fragment_assembler::Action {
        // The reference records a `ClusterEvent` and continues
        // (`ConsensusModuleAgent.java:650-672`); the count is added to counter
        // 212 after the poll, because writing it needs the client.
        self.extension_messages += 1;
        deepmsg_client::fragment_assembler::Action::Continue
    }

    fn on_ingress_message(
        &mut self,
        leadership_term_id: i64,
        cluster_session_id: i64,
        payload: &[u8],
    ) -> Result<deepmsg_client::fragment_assembler::Action, ClusterError> {
        use deepmsg_client::fragment_assembler::Action;

        if leadership_term_id != self.leadership_term_id || self.role != Role::Leader {
            return Ok(Action::Continue);
        }
        if !self
            .sessions
            .find_by_session_id(cluster_session_id)
            .is_some_and(|session| session.is_open())
        {
            return Ok(Action::Continue);
        }

        let timestamp = MillisecondClusterClock.time();
        let position = self.log_publisher.append_message(
            self.position_limit,
            leadership_term_id,
            cluster_session_id,
            timestamp,
            payload,
        )?;
        if position > 0 {
            if let Some(session) = self.sessions.find_by_session_id_mut(cluster_session_id) {
                session.set_time_of_last_activity_ns(
                    MillisecondClusterClock.convert_to_nanos(timestamp),
                );
            }
            Ok(Action::Continue)
        } else {
            Ok(Action::Abort)
        }
    }

    fn on_session_connect(&mut self, request: SessionConnectRequest) -> Result<(), ClusterError> {
        let mut request = request;
        request.response_channel = refine_response_channel(
            &request.response_channel,
            self.ingress_ipc_allowed,
            self.egress_channel,
        );
        self.sessions.on_session_connect(
            request,
            self.role,
            self.ingress_endpoints,
            MillisecondClusterClock.time_nanos(),
        );
        Ok(())
    }

    fn on_session_close(
        &mut self,
        leadership_term_id: i64,
        cluster_session_id: i64,
    ) -> Result<(), ClusterError> {
        if leadership_term_id == self.leadership_term_id && self.role == Role::Leader {
            self.deferred_closes
                .push((leadership_term_id, cluster_session_id));
        }
        Ok(())
    }

    fn on_session_keep_alive(
        &mut self,
        leadership_term_id: i64,
        cluster_session_id: i64,
        image_session_id: i32,
    ) -> Result<(), ClusterError> {
        if leadership_term_id == self.leadership_term_id && self.role == Role::Leader {
            if let Some(session) = self.sessions.find_by_session_id_mut(cluster_session_id) {
                if session.is_open() {
                    session.link_ingress_image(image_session_id);
                    session.set_time_of_last_activity_ns(self.now_ns);
                }
            }
        }
        Ok(())
    }

    fn on_challenge_response(
        &mut self,
        correlation_id: i64,
        cluster_session_id: i64,
        encoded_credentials: &[u8],
    ) -> Result<(), ClusterError> {
        if self.role == Role::Leader {
            self.sessions.on_challenge_response_for_user_session(
                correlation_id,
                cluster_session_id,
                encoded_credentials,
                self.now_ns,
            );
        } else {
            return Err(ClusterError::Cluster(
                "sending a challenge response to the leader is P3-6b".to_owned(),
            ));
        }
        Ok(())
    }

    fn on_admin_request(
        &mut self,
        leadership_term_id: i64,
        cluster_session_id: i64,
        correlation_id: i64,
        request_type: AdminRequestType,
        _payload: &[u8],
    ) -> Result<(), ClusterError> {
        if self.role == Role::Leader && leadership_term_id == self.leadership_term_id {
            self.deferred_admins.push(DeferredAdminRequest {
                cluster_session_id,
                correlation_id,
                request_type,
            });
        }
        Ok(())
    }
}

impl ConsensusModuleAgent {
    /// `connectIngress` (`ConsensusModuleAgent.java:3387-3415`): the ingress
    /// subscription, and the IPC one when a leader allows IPC ingress.
    fn connect_ingress(&mut self) -> Result<(), ClusterError> {
        let this_member = &self.members[self.this_member_index];
        let mut uri = ChannelUri::parse(&self.config.ingress_channel)
            .map_err(|error| ClusterError::Cluster(error.to_string()))?;
        if uri.get("endpoint").is_none() {
            uri.put("endpoint", this_member.ingress_endpoint.clone());
        }

        if self.role != Role::Leader && is_multicast_endpoint(&uri) {
            // The reference does not subscribe a follower to a multicast
            // ingress channel (`:3395-3398`).
            return Ok(());
        }

        uri.put("rejoin", "false");
        uri.put("reliable-stream", "true");

        let udp = self.client.add_subscription(
            &uri.build(),
            self.config.ingress_stream_id,
            COMMAND_TIMEOUT,
        )?;
        let ipc = if self.role == Role::Leader && self.config.ingress_ipc_allowed {
            Some(self.client.add_subscription(
                "aeron:ipc",
                self.config.ingress_stream_id,
                COMMAND_TIMEOUT,
            )?)
        } else {
            None
        };

        self.ingress.connect(Some(udp), ipc);
        Ok(())
    }

    /// `consensusWork`'s `ingressAdapter.poll()` (`ConsensusModuleAgent.java:2494`):
    /// the client poll and then the deferred work that needs the client back.
    fn poll_ingress(&mut self) -> Result<usize, ClusterError> {
        if !self.ingress.is_connected() {
            return Ok(0);
        }

        let position_limit = self.log_publisher.position_limit(&self.client);
        let now_ns = self.clock.time_nanos();
        let ingress_endpoints = self.ingress_endpoints.as_str();
        let role = self.role;
        let leadership_term_id = self.leadership_term_id;
        let ingress_ipc_allowed = self.config.ingress_ipc_allowed;

        let Self {
            client,
            ingress,
            sessions,
            log_publisher,
            config,
            ..
        } = self;
        let mut handler = AgentIngressHandler {
            sessions,
            log_publisher,
            role,
            leadership_term_id,
            position_limit,
            now_ns,
            ingress_ipc_allowed,
            egress_channel: config.egress_channel.as_deref(),
            ingress_endpoints,
            deferred_closes: Vec::new(),
            deferred_admins: Vec::new(),
            extension_messages: 0,
        };

        let result = ingress.poll(client, &mut handler);
        let work = result?;
        let deferred_closes = std::mem::take(&mut handler.deferred_closes);
        let deferred_admins = std::mem::take(&mut handler.deferred_admins);
        let extension_messages = handler.extension_messages;
        drop(handler);

        if extension_messages > 0 {
            let error_counter = self.counters.error;
            let current = read_counter(&self.client, error_counter).unwrap_or(0);
            write_counter(
                &self.client,
                error_counter,
                current + i64::try_from(extension_messages).unwrap_or(1),
            );
        }

        // The two deferred handlers both need `&mut Client`; they run in the
        // same duty cycle, right after the poll.
        self.sessions.drive_async_connects(&mut self.client)?;

        if !deferred_closes.is_empty() || !deferred_admins.is_empty() {
            let leadership_term_id = self.leadership_term_id;
            let member_id = self.config.member_id;
            let leader_member_id = self.members[self.this_member_index].id;
            let clock = self.clock;
            let Self {
                client,
                sessions,
                egress,
                log_publisher,
                counters,
                ..
            } = self;
            let mut ctx = SessionContext {
                client,
                log_publisher,
                egress,
                clock: &clock,
                timed_out_counter: counters.timed_out_client,
                leadership_term_id,
                member_id,
                leader_member_id,
                ingress_endpoints,
                now_ns,
            };
            for (term, cluster_session_id) in deferred_closes {
                sessions.on_session_close(term, cluster_session_id, &mut ctx)?;
            }
            for request in deferred_admins {
                handle_admin_request(&mut ctx, &*sessions, request)?;
            }
        }

        Ok(work)
    }
}

/// `ConsensusModuleAgent.onAdminRequest` (`ConsensusModuleAgent.java:780-826`).
fn handle_admin_request(
    ctx: &mut SessionContext<'_>,
    sessions: &SessionManager,
    request: DeferredAdminRequest,
) -> Result<(), ClusterError> {
    let Some(session) = sessions
        .find_by_session_id(request.cluster_session_id)
        .filter(|session| session.is_open())
    else {
        return Ok(());
    };

    let authorised = sessions.authorisation_service().is_authorised(
        i32::from(deepmsg_codec_cluster::SBE_SCHEMA_ID),
        i32::from(deepmsg_codec_cluster::admin_request_codec::SBE_TEMPLATE_ID),
        Some(i32::from(request.request_type)),
        session.encoded_principal(),
    );
    if !authorised {
        let message = format!(
            "Execution of the {} request was not authorised",
            admin_request_type_name(request.request_type)
        );
        return send_admin_response(
            ctx,
            session,
            request.correlation_id,
            request.request_type,
            AdminResponseCode::UNAUTHORISED_ACCESS,
            &message,
        );
    }

    if request.request_type == AdminRequestType::SNAPSHOT {
        // The reference switches the control toggle (`:806-816`); the toggle
        // is P3-4e, so this answers with the reference's own failure text
        // instead of pretending the snapshot was scheduled.
        return send_admin_response(
            ctx,
            session,
            request.correlation_id,
            request.request_type,
            AdminResponseCode::ERROR,
            "Failed to switch ClusterControl to the ToggleState.SNAPSHOT state",
        );
    }

    let message = format!(
        "Unknown request type: {}",
        admin_request_type_name(request.request_type)
    );
    send_admin_response(
        ctx,
        session,
        request.correlation_id,
        request.request_type,
        AdminResponseCode::ERROR,
        &message,
    )
}

fn send_admin_response(
    ctx: &mut SessionContext<'_>,
    session: &ClusterSession,
    correlation_id: i64,
    request_type: AdminRequestType,
    response_code: AdminResponseCode,
    message: &str,
) -> Result<(), ClusterError> {
    ctx.egress.send_admin_response(
        &SessionEgress {
            client: ctx.client,
            session,
        },
        correlation_id,
        request_type,
        response_code,
        message,
    )?;
    Ok(())
}

fn admin_request_type_name(request_type: AdminRequestType) -> &'static str {
    match request_type {
        AdminRequestType::SNAPSHOT => "SNAPSHOT",
        AdminRequestType::NullVal => "NULL_VAL",
    }
}

/// The service publication seen through a client, for [`ServiceProxy`].
struct PublicationSender<'a> {
    client: &'a Client,
    registration_id: i64,
}

impl ServiceSend for PublicationSender<'_> {
    fn try_claim(&self, length: usize, bytes: &[u8]) -> bool {
        let Some(publication) = self.client.publication(self.registration_id) else {
            return false;
        };
        let Some(counters) = self.client.counters_reader() else {
            return false;
        };
        let limit = counters
            .value(publication.position_limit_counter_id())
            .unwrap_or(0);
        let Ok(claim) = publication.try_claim(limit, length) else {
            return false;
        };
        let frame = claim.frame();
        if frame.write_payload(bytes).is_none() {
            return false;
        }
        let frame_length = deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH + length;
        #[allow(clippy::cast_possible_truncation)] // bounded by the payload maximum
        let frame_length = frame_length as i32;
        frame.publish(frame_length).is_some()
    }

    fn offer(&self, bytes: &[u8]) -> bool {
        matches!(
            self.client.offer(self.registration_id, bytes),
            Some(deepmsg_core::logbuffer::append::Appended::Ok { .. })
        )
    }

    fn is_closed(&self) -> bool {
        self.client.publication(self.registration_id).is_none()
    }

    fn term_buffer_length(&self) -> i32 {
        self.client.publication(self.registration_id).map_or(
            0,
            deepmsg_client::publication::Publication::term_buffer_length,
        )
    }
}

/// The agent as the timer service's expiry callback (`onTimerEvent`,
/// `ConsensusModuleAgent.java:889-900`).
struct AgentTimerHandler<'a> {
    log_publisher: &'a mut LogPublisher,
    leadership_term_id: i64,
    position_limit: i64,
    timestamp: i64,
    uncommitted_timers: &'a mut std::collections::VecDeque<(i64, i64)>,
}

impl TimerHandler for AgentTimerHandler<'_> {
    fn on_timer_event(&mut self, correlation_id: i64) -> bool {
        match self.log_publisher.append_timer(
            self.position_limit,
            correlation_id,
            self.leadership_term_id,
            self.timestamp,
        ) {
            Ok(position) if position > 0 => {
                self.uncommitted_timers
                    .push_back((position, correlation_id));
                true
            }
            _ => false,
        }
    }
}

/// A timer control handler for the service channel.
struct DeferredQuery {
    correlation_id: i64,
    extended: bool,
}

struct AgentServiceHandler<'a> {
    trackers: &'a mut [PendingServiceMessageTracker],
    timer: &'a mut dyn TimerService,
    service_ack_queues: &'a mut ServiceAckQueues,
    service_ack_id: &'a mut i64,
    service_client_ids: &'a [i64],
    state: ModuleState,
    expired_timer_count: &'a mut deepmsg_cluster_common::collections::HashMap<i64, i64>,
    deferred_closes: Vec<i64>,
    deferred_queries: Vec<DeferredQuery>,
    /// A snapshot every service has acked, carried out after the poll.
    deferred_snapshot: Option<(i64, i64, Vec<ServiceAck>)>,
    /// A `TERMINATING` ack: `terminateOnServiceAck(logPosition)`, carried out
    /// after the poll because it needs the archive and the recording log.
    deferred_termination: Option<i64>,
    /// A `QUITTING` ack: `closeAndTerminate()`, after the poll. Nothing in
    /// 1.53.2 sets `QUITTING` (`ConsensusModule.java:168`), so this branch is
    /// transcribed and unreachable, as in the reference.
    deferred_close: bool,
}

impl ServiceControlHandler for AgentServiceHandler<'_> {
    fn on_service_message(
        &mut self,
        cluster_session_id: i64,
        message: &[u8],
    ) -> Result<(), ClusterError> {
        let service_id = service_id_from_service_message(cluster_session_id);
        let Some(tracker) = usize::try_from(service_id)
            .ok()
            .and_then(|index| self.trackers.get_mut(index))
        else {
            return Err(ClusterError::Cluster(format!(
                "service message for unknown serviceId={service_id}"
            )));
        };
        tracker.enqueue_message(message)
    }

    fn on_service_close_session(&mut self, cluster_session_id: i64) -> Result<(), ClusterError> {
        self.deferred_closes.push(cluster_session_id);
        Ok(())
    }

    fn on_schedule_timer(
        &mut self,
        correlation_id: i64,
        deadline: i64,
    ) -> Result<(), ClusterError> {
        // A timer the replayed log already expired is not scheduled again; the
        // count is what `onReplayTimerEvent` left behind
        // (`ConsensusModuleAgent.java:1449-1459`).
        let expired = self
            .expired_timer_count
            .get(&correlation_id)
            .copied()
            .unwrap_or(0);
        if expired == 0 {
            self.timer
                .schedule_timer_for_correlation_id(correlation_id, deadline);
        } else {
            self.expired_timer_count.insert(correlation_id, expired - 1);
        }
        Ok(())
    }

    fn on_cancel_timer(&mut self, correlation_id: i64) -> Result<(), ClusterError> {
        self.timer.cancel_timer_by_correlation_id(correlation_id);
        Ok(())
    }

    fn on_service_ack(
        &mut self,
        log_position: i64,
        _timestamp: i64,
        ack_id: i64,
        relevant_id: i64,
        service_id: i32,
    ) -> Result<deepmsg_client::fragment_assembler::Action, ClusterError> {
        let Some(queue) = usize::try_from(service_id)
            .ok()
            .and_then(|index| self.service_ack_queues.get_mut(index))
        else {
            return Err(ClusterError::Cluster(format!(
                "ack for unknown serviceId={service_id}"
            )));
        };

        // `captureServiceAck` (`:2715-2723`).
        if ack_id == 0 {
            let already_received = self
                .service_client_ids
                .get(service_id as usize)
                .is_some_and(|client_id| *client_id != ACK_NULL_VALUE);
            if already_received {
                return Err(ClusterError::Cluster(format!(
                    "initial ack already received from service: possible duplicate serviceId={service_id}"
                )));
            }
        }
        queue.push_back(ServiceAck::new(ack_id, log_position, relevant_id));

        if has_reached(log_position, *self.service_ack_id, self.service_ack_queues)? {
            match self.state {
                ModuleState::Active => {}
                ModuleState::Snapshot => {
                    // `onServiceAck` (`:1472-1480`): the next ack id, then the
                    // collected acks; a null relevant id is reported and the
                    // snapshot is not taken.
                    *self.service_ack_id += 1;
                    let acks = crate::service_control::ack::poll_service_acks_at(
                        log_position,
                        service_id as usize,
                        self.service_ack_queues,
                    )?;
                    let null_ids = crate::service_control::ack::null_relevant_ids(
                        "failed to take snapshot",
                        &acks,
                    );
                    if null_ids.is_empty() {
                        self.deferred_snapshot = Some((log_position, _timestamp, acks));
                    }
                }
                ModuleState::Quitting => {
                    self.deferred_close = true;
                }
                ModuleState::Terminating => {
                    // `onServiceAck` (`:1486-1487`): the last service ack for
                    // this position ends the wait.
                    self.deferred_termination = Some(log_position);
                }
                _ => {}
            }
        }

        // `ConsensusModuleAdapter` always breaks after a `ServiceAck`.
        Ok(deepmsg_client::fragment_assembler::Action::Break)
    }

    fn on_cluster_members_query(
        &mut self,
        correlation_id: i64,
        is_extended_request: bool,
    ) -> Result<(), ClusterError> {
        self.deferred_queries.push(DeferredQuery {
            correlation_id,
            extended: is_extended_request,
        });
        Ok(())
    }
}

impl ConsensusModuleAgent {
    /// `sweepUncommittedEntriesTo` (`ConsensusModuleAgent.java:2998-3018`):
    /// timers, then the pending service messages, then the sessions.
    fn sweep_uncommitted_entries_to(&mut self, commit_position: i64) {
        while let Some(&(append_position, _correlation_id)) = self.uncommitted_timers.front() {
            if append_position > commit_position {
                break;
            }
            self.uncommitted_timers.pop_front();
            self.uncommitted_timers.pop_front();
        }

        for tracker in self.trackers.iter_mut() {
            tracker.sweep_leader_messages(commit_position);
        }

        let _ = self.sessions.sweep_uncommitted_sessions(commit_position);

        // `:3013-3017`: a previous state whose action has now been committed
        // is no longer a rollback candidate.
        while let Some(&(append_position, _state)) = self.uncommitted_previous_state.front() {
            if append_position > commit_position {
                break;
            }
            self.uncommitted_previous_state.pop_front();
            self.uncommitted_previous_state.pop_front();
        }
    }

    /// `restoreUncommittedEntries` (`ConsensusModuleAgent.java:3020-3052`):
    /// timers the commit position did not reach expire at once (the replay
    /// will produce their events again), and the service messages go back to
    /// "not appended".
    fn restore_uncommitted_entries(&mut self, commit_position: i64) {
        while let Some((append_position, correlation_id)) = self.uncommitted_timers.pop_front() {
            if append_position > commit_position {
                self.timer
                    .schedule_timer_for_correlation_id(correlation_id, 0);
            }
        }

        for tracker in self.trackers.iter_mut() {
            tracker.restore_uncommitted_messages(commit_position);
        }

        self.sessions.restore_uncommitted_sessions(commit_position);

        // `:3041-3057`: committed previous states are dropped; if one is left,
        // its action never made it, so the state before it is the committed
        // one — the reference's "rollback".
        while let Some(&(append_position, _state)) = self.uncommitted_previous_state.front() {
            if append_position > commit_position {
                break;
            }
            self.uncommitted_previous_state.pop_front();
            self.uncommitted_previous_state.pop_front();
        }

        if !self.uncommitted_previous_state.is_empty() {
            self.uncommitted_previous_state.pop_front();
            if let Some(committed_state) = self
                .uncommitted_previous_state
                .pop_front()
                .and_then(|(_, code)| i32::try_from(code).ok())
                .and_then(ModuleState::from_code)
            {
                if self.state != ModuleState::Closed {
                    self.set_state(committed_state, "rollback");
                }
            }
        }
        self.uncommitted_previous_state.clear();
    }

    /// `awaitServicesReady` (`ConsensusModuleAgent.java:1801-1838`): a no-op
    /// without services, otherwise send `JoinLog` and wait for every service's
    /// ack at `log_position`.
    #[allow(clippy::too_many_arguments)]
    fn await_services_ready(
        &mut self,
        log_channel: &str,
        stream_id: i32,
        log_session_id: i32,
        log_position: i64,
        max_log_position: i64,
        is_startup: bool,
        role: Role,
    ) -> Result<(), ClusterError> {
        if self.config.service_count <= 0 {
            return Ok(());
        }

        {
            let send = PublicationSender {
                client: &self.client,
                registration_id: self.service_publication_id,
            };
            self.service_proxy.join_log(
                &send,
                log_position,
                max_log_position,
                self.config.member_id,
                log_session_id,
                stream_id,
                is_startup,
                role.code(),
                log_channel,
                false,
            )?;
        }

        self.expected_ack_position = log_position;
        while !has_reached(log_position, self.service_ack_id, &self.service_ack_queues)? {
            self.poll_service_control()?;
            self.idle()?;
            if self.state == ModuleState::Closed {
                return Err(ClusterError::AgentTermination(
                    "State.CLOSED == state".to_owned(),
                ));
            }
        }
        remove_head(&mut self.service_ack_queues);
        self.service_ack_id += 1;
        Ok(())
    }

    /// `timerService.poll(timestamp)` (`:2489`).
    fn poll_timers(&mut self, timestamp: i64) -> Result<usize, ClusterError> {
        let position_limit = self.log_publisher.position_limit(&self.client);
        let leadership_term_id = self.leadership_term_id;
        let Self {
            timer,
            log_publisher,
            uncommitted_timers,
            ..
        } = self;
        let mut handler = AgentTimerHandler {
            log_publisher,
            leadership_term_id,
            position_limit,
            timestamp,
            uncommitted_timers,
        };
        Ok(timer.poll(timestamp, &mut handler))
    }

    /// Each `PendingServiceMessageTracker.poll()` (`:2490-2493`).
    fn poll_trackers(&mut self, timestamp: i64) -> Result<usize, ClusterError> {
        if self.trackers.is_empty() {
            return Ok(0);
        }
        let position_limit = self.log_publisher.position_limit(&self.client);
        let Self {
            trackers,
            log_publisher,
            ..
        } = self;
        let mut work = 0;
        for tracker in trackers.iter_mut() {
            work += tracker.poll(log_publisher, position_limit, timestamp)?;
        }
        Ok(work)
    }

    /// `consensusModuleAdapter.poll()` (`:2535`) and the deferred work that
    /// needs the client and the sessions back.
    fn poll_service_control(&mut self) -> Result<usize, ClusterError> {
        if !self.service_control.is_connected() {
            return Ok(0);
        }

        let leadership_term_id = self.leadership_term_id;
        let member_id = self.config.member_id;
        let leader_member_id = self.members[self.this_member_index].id;
        let state = self.state;
        let now_ns = self.clock.time_nanos();
        let clock = self.clock;
        let ingress_endpoints = self.ingress_endpoints.as_str();

        let (work, deferred_snapshot, deferred_termination, deferred_close) = {
            let Self {
                client,
                service_control,
                trackers,
                timer,
                service_ack_queues,
                service_ack_id,
                service_client_ids,
                expired_timer_count,
                sessions,
                egress,
                log_publisher,
                counters,
                service_proxy,
                service_publication_id,
                members,
                ..
            } = self;

            let mut handler = AgentServiceHandler {
                trackers,
                timer: &mut **timer,
                service_ack_queues,
                service_ack_id,
                service_client_ids,
                state,
                expired_timer_count,
                deferred_closes: Vec::new(),
                deferred_queries: Vec::new(),
                deferred_snapshot: None,
                deferred_termination: None,
                deferred_close: false,
            };

            let result = service_control.poll(client, &mut handler);
            let deferred_closes = std::mem::take(&mut handler.deferred_closes);
            let deferred_queries = std::mem::take(&mut handler.deferred_queries);
            let deferred_snapshot = handler.deferred_snapshot.take();
            let deferred_termination = handler.deferred_termination.take();
            let deferred_close = handler.deferred_close;
            let work = result?;
            drop(handler);

            if !deferred_closes.is_empty() {
                let mut ctx = SessionContext {
                    client,
                    log_publisher,
                    egress,
                    clock: &clock,
                    timed_out_counter: counters.timed_out_client,
                    leadership_term_id,
                    member_id,
                    leader_member_id,
                    ingress_endpoints,
                    now_ns,
                };
                for cluster_session_id in deferred_closes {
                    sessions.on_session_close(leadership_term_id, cluster_session_id, &mut ctx)?;
                }
            }

            if !deferred_queries.is_empty() {
                let send = PublicationSender {
                    client,
                    registration_id: *service_publication_id,
                };
                let members = &*members;
                for query in deferred_queries {
                    let sent = if query.extended {
                        service_proxy.cluster_members_extended_response(
                            &send,
                            query.correlation_id,
                            now_ns,
                            leader_member_id,
                            member_id,
                            members,
                        )
                    } else {
                        service_proxy.cluster_members_response(
                            &send,
                            query.correlation_id,
                            leader_member_id,
                            &member::encode_as_string(members),
                        )
                    };
                    if !sent {
                        let error_counter = counters.error;
                        let current = read_counter(&*client, error_counter).unwrap_or(0);
                        write_counter(&*client, error_counter, current + 1);
                    }
                }
            }

            (
                work,
                deferred_snapshot,
                deferred_termination,
                deferred_close,
            )
        };

        if let Some((log_position, timestamp, acks)) = deferred_snapshot {
            // `snapshotOnServiceAck` (`:3104-3146`): take it, then ACTIVE — or,
            // for a `SHUTDOWN`'s snapshot, start the termination half.
            self.snapshot_on_service_ack(log_position, timestamp, &acks)?;
        }

        if let Some(log_position) = deferred_termination {
            self.terminate_on_service_ack(log_position)?;
        }

        if deferred_close {
            return Err(self.close_and_terminate());
        }

        Ok(work)
    }
}

/// The snapshot publication and the idle that drives the client between
/// retries, as one borrow (`ConsensusModuleSnapshotTaker`'s publication +
/// `IdleStrategy` + agent invoker).
struct ExclusivePublicationSink<'a> {
    client: &'a mut Client,
    publication: &'a ExclusivePublication,
    idle_strategy: &'a mut Strategy,
}

impl SnapshotSend for ExclusivePublicationSink<'_> {
    fn try_claim(&self, length: usize, bytes: &[u8]) -> SnapshotAppend {
        let limit = self.client.counters_reader().map_or(0, |counters| {
            counters
                .value(self.publication.position_limit_counter_id())
                .unwrap_or(0)
        });
        match self.publication.try_claim(limit, length) {
            Ok(claim) => {
                let frame = claim.frame();
                if frame.write_payload(bytes).is_none() {
                    return SnapshotAppend::Fatal("failed to write into the claim".to_owned());
                }
                let frame_length = deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH + length;
                #[allow(clippy::cast_possible_truncation)] // bounded by the payload maximum
                let frame_length = frame_length as i32;
                if frame.publish(frame_length).is_some() {
                    SnapshotAppend::Ok
                } else {
                    SnapshotAppend::Fatal("failed to publish the claim".to_owned())
                }
            }
            Err(Appended::BackPressured | Appended::EndOfLog | Appended::MidRotation) => {
                SnapshotAppend::Retry
            }
            Err(Appended::NotConnected) => {
                SnapshotAppend::Fatal("publication is not connected".to_owned())
            }
            Err(Appended::MaxPositionExceeded) => SnapshotAppend::Fatal(format!(
                "publication at max position: term-length={}",
                self.publication.term_buffer_length()
            )),
            Err(other) => SnapshotAppend::Fatal(format!("{other:?}")),
        }
    }

    fn offer(&self, bytes: &[u8]) -> SnapshotAppend {
        let limit = self.client.counters_reader().map_or(0, |counters| {
            counters
                .value(self.publication.position_limit_counter_id())
                .unwrap_or(0)
        });
        match self.publication.offer(limit, bytes) {
            Appended::Ok { .. } => SnapshotAppend::Ok,
            Appended::BackPressured | Appended::EndOfLog | Appended::MidRotation => {
                SnapshotAppend::Retry
            }
            Appended::NotConnected => {
                SnapshotAppend::Fatal("publication is not connected".to_owned())
            }
            Appended::MaxPositionExceeded => SnapshotAppend::Fatal(format!(
                "publication at max position: term-length={}",
                self.publication.term_buffer_length()
            )),
            other => SnapshotAppend::Fatal(format!("{other:?}")),
        }
    }

    fn max_payload_length(&self) -> usize {
        self.publication.max_payload_length().unwrap_or(0)
    }
}

impl SnapshotIdle for ExclusivePublicationSink<'_> {
    fn reset(&mut self) {
        // The strategy in this build has no separate reset; `idle(0)` after
        // work does what the reference's `reset` does for backoff.
    }

    fn idle(&mut self) {
        let _ = self.client.poll();
        self.idle_strategy.idle(0);
    }
}

/// The agent as the snapshot loader's listener (`ConsensusModuleAgent.onLoad*`).
struct AgentSnapshotLoadHandler<'a> {
    sessions: &'a mut SessionManager,
    trackers: &'a mut [PendingServiceMessageTracker],
    timer: &'a mut dyn TimerService,
}

impl SnapshotLoadHandler for AgentSnapshotLoadHandler<'_> {
    fn on_load_begin_snapshot(
        &mut self,
        app_version: i32,
        time_unit: ClusterTimeUnit,
    ) -> Result<(), ClusterError> {
        // `onLoadBeginSnapshot` (`:632-647`): an incompatible app version or a
        // time unit this build does not have is fatal in the reference.
        if app_version != crate::config::APP_VERSION_DEFAULT {
            return Err(ClusterError::AgentTermination(format!(
                "incompatible appVersion={app_version}, expected={}",
                crate::config::APP_VERSION_DEFAULT
            )));
        }
        if time_unit != CLUSTER_TIME_UNIT {
            return Err(ClusterError::AgentTermination(format!(
                "timeUnit={time_unit:?} does not match {CLUSTER_TIME_UNIT:?}"
            )));
        }
        Ok(())
    }

    fn on_load_end_snapshot(&mut self) -> Result<(), ClusterError> {
        Ok(())
    }

    fn on_load_cluster_session(
        &mut self,
        session: crate::snapshot::SnapshotSession,
    ) -> Result<(), ClusterError> {
        self.sessions.on_load_cluster_session(
            session.cluster_session_id,
            session.correlation_id,
            session.opened_log_position,
            session.time_of_last_activity,
            session.close_reason,
            session.response_stream_id,
            &session.response_channel,
        )
    }

    fn on_load_timer(&mut self, correlation_id: i64, deadline: i64) -> Result<(), ClusterError> {
        // `onLoadTimer` (`:744-748`) delegates to `onScheduleTimer`.
        self.timer
            .schedule_timer_for_correlation_id(correlation_id, deadline);
        Ok(())
    }

    fn on_load_consensus_module_state(
        &mut self,
        next_session_id: i64,
        next_service_session_id: i64,
        log_service_session_id: i64,
        pending_message_capacity: i32,
    ) -> Result<(), ClusterError> {
        self.sessions.load_next_session_id(next_session_id);
        if let Some(tracker) = self.trackers.first_mut() {
            #[allow(clippy::cast_sign_loss)] // a capacity from a snapshot
            let capacity = pending_message_capacity as usize;
            tracker.load_state(next_service_session_id, log_service_session_id, capacity);
        }
        Ok(())
    }

    fn on_load_pending_message_tracker(
        &mut self,
        next_service_session_id: i64,
        log_service_session_id: i64,
        pending_message_capacity: i32,
        service_id: i32,
    ) -> Result<(), ClusterError> {
        let Some(tracker) = usize::try_from(service_id)
            .ok()
            .and_then(|index| self.trackers.get_mut(index))
        else {
            return Err(ClusterError::Cluster(format!(
                "snapshot has a tracker for unknown serviceId={service_id}"
            )));
        };
        #[allow(clippy::cast_sign_loss)] // a capacity from a snapshot
        let capacity = pending_message_capacity as usize;
        tracker.load_state(next_service_session_id, log_service_session_id, capacity);
        Ok(())
    }

    fn on_load_pending_message(
        &mut self,
        cluster_session_id: i64,
        message: &[u8],
    ) -> Result<(), ClusterError> {
        let service_id = crate::service_control::service_id_from_log_message(cluster_session_id);
        let Some(tracker) = usize::try_from(service_id)
            .ok()
            .and_then(|index| self.trackers.get_mut(index))
        else {
            return Err(ClusterError::Cluster(format!(
                "pending service message for unknown serviceId={service_id}"
            )));
        };
        if !tracker.append_message(message) {
            return Err(ClusterError::Cluster(
                "pending service message ring is at capacity during load".to_owned(),
            ));
        }
        Ok(())
    }
}

impl ConsensusModuleAgent {
    /// `takeSnapshot` (`ConsensusModuleAgent.java:3153-3191`), with no service
    /// snapshots (the 0-service path). The trigger itself — ClusterControl and
    /// admin requests — is P3-4e; this is the seam it will call.
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the archive, a refused record, or a recording that
    /// stops early.
    pub fn take_snapshot(&mut self, timestamp: i64) -> Result<(), ClusterError> {
        if self.config.service_count > 0 {
            return Err(ClusterError::Cluster(
                "taking a snapshot with services needs their acks (P3-4f)".to_owned(),
            ));
        }
        let log_position = read_counter(&self.client, self.counters.commit_position).unwrap_or(0);
        self.take_snapshot_with_acks(timestamp, log_position, &[])
    }

    /// The same with the service acks whose `relevantId`s name their snapshot
    /// recordings (`:3177-3185`).
    ///
    /// # Errors
    ///
    /// As [`Self::take_snapshot`].
    pub fn take_snapshot_with_acks(
        &mut self,
        timestamp: i64,
        log_position: i64,
        service_acks: &[ServiceAck],
    ) -> Result<(), ClusterError> {
        let registration_id = self.client.add_exclusive_publication(
            &self.config.snapshot_channel,
            self.config.snapshot_stream_id,
            COMMAND_TIMEOUT,
        )?;
        let publication = self
            .client
            .take_exclusive_publication(registration_id)
            .ok_or_else(|| {
                ClusterError::Cluster("the snapshot publication did not arrive".to_owned())
            })?;
        let log_session_id = publication.session_id();

        let mut channel_uri = ChannelUri::parse(&self.config.snapshot_channel)
            .map_err(|error| ClusterError::Cluster(error.to_string()))?;
        channel_uri.put("session-id", log_session_id.to_string());
        self.archive.start_recording(
            &mut self.client,
            &channel_uri.build(),
            self.config.snapshot_stream_id,
            true,
            true,
        )?;

        let counter_id = self.await_recording_counter(log_session_id)?;
        let recording_id = {
            let identity = self
                .client
                .counters_reader()
                .and_then(|counters| counters.key(counter_id))
                .and_then(|key| deepmsg_archive::server::recording_pos::parse_key(&key))
                .ok_or_else(|| {
                    ClusterError::Cluster("the snapshot recording has no identity".to_owned())
                })?;
            identity.recording_id
        };

        self.snapshot_state(&publication, log_position)?;
        let position = publication.position().unwrap_or(0);
        self.await_recording_complete(recording_id, position, counter_id)?;

        let term_base_log_position = self
            .recording_log
            .entries()
            .iter()
            .find(|entry| {
                entry.leadership_term_id == self.leadership_term_id
                    && entry.entry_type == recording_log::ENTRY_TYPE_TERM
            })
            .map_or(0, |entry| entry.term_base_log_position);

        for (service_id, ack) in service_acks.iter().enumerate().rev() {
            #[allow(clippy::cast_possible_truncation)] // bounded by MAX_SERVICE_COUNT
            let service_id = service_id as i32;
            self.recording_log
                .append_snapshot(
                    self.leadership_term_id,
                    term_base_log_position,
                    log_position,
                    timestamp,
                    ack.relevant_id(),
                    service_id,
                )
                .map_err(cluster)?;
        }
        self.recording_log
            .append_snapshot(
                self.leadership_term_id,
                term_base_log_position,
                log_position,
                timestamp,
                recording_id,
                -1,
            )
            .map_err(cluster)?;
        self.recording_log
            .force(self.config.file_sync_level)
            .map_err(cluster)?;
        self.recovery_plan = create_recovery_plan(
            &self.recording_log,
            &mut self.archive,
            &mut self.client,
            self.config.service_count,
            -1,
        )?;

        let _ = self
            .client
            .remove_publication(registration_id, COMMAND_TIMEOUT);
        // `takeSnapshot` (`:3189`): the duration the operator sees on counters
        // 234/235, measured from the trigger (the replayed action or the
        // toggle), not from this call.
        let now_ns = self.clock.time_nanos();
        self.snapshot_duration.on_snapshot_end(&self.client, now_ns);
        increment_counter(&self.client, self.counters.snapshot, 1);
        Ok(())
    }

    /// `snapshotState` (`:3221-3253`): the record order and the iteration
    /// containers are documented in [`crate::snapshot`].
    fn snapshot_state(
        &mut self,
        publication: &ExclusivePublication,
        log_position: i64,
    ) -> Result<(), ClusterError> {
        let Self {
            client,
            idle_strategy,
            sessions,
            timer,
            trackers,
            leadership_term_id,
            config,
            counters,
            ..
        } = self;
        let mut sink = ExclusivePublicationSink {
            client,
            publication,
            idle_strategy,
        };
        let mut taker = crate::snapshot::ConsensusModuleSnapshotTaker::new(
            &mut sink,
            CLUSTER_TIME_UNIT,
            config.app_version,
        );

        taker.mark_begin(log_position, *leadership_term_id)?;
        let (next_service_session_id, log_service_session_id, pending_message_capacity) =
            match trackers.first() {
                Some(tracker) => (
                    tracker.next_service_session_id(),
                    tracker.log_service_session_id(),
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        tracker.size() as i32
                    },
                ),
                None => (0, 0, 0),
            };
        taker.snapshot_consensus_module_state(
            sessions.next_committed_session_id(),
            next_service_session_id,
            log_service_session_id,
            pending_message_capacity,
        )?;
        sessions.snapshot_sessions(&mut taker)?;
        timer.snapshot(&mut taker);
        if taker.failed() {
            return Err(ClusterError::Cluster(
                "a timer record was refused by the snapshot publication".to_owned(),
            ));
        }
        let mut corrections = 0usize;
        for tracker in trackers.iter() {
            if taker.snapshot_tracker(tracker)?.is_some() {
                corrections += 1;
            }
        }
        taker.mark_end(log_position, *leadership_term_id)?;
        drop(taker);

        // The reference reports each missed-message correction through its
        // counted error handler; the module error counter is the same fact.
        if corrections > 0 {
            let error_counter = counters.error;
            let current = read_counter(&*client, error_counter).unwrap_or(0);
            #[allow(clippy::cast_possible_wrap)]
            write_counter(&*client, error_counter, current + corrections as i64);
        }
        Ok(())
    }

    /// `awaitRecordingCounter` (`:3208-3219`).
    fn await_recording_counter(&mut self, session_id: i32) -> Result<i32, ClusterError> {
        let archive_id = self.archive.archive_id();
        loop {
            if let Some(counters) = self.client.counters_reader() {
                if let Some(counter_id) =
                    deepmsg_archive::server::recording_pos::find_counter_id_by_session(
                        &counters, session_id, archive_id,
                    )
                {
                    return Ok(counter_id);
                }
            }
            self.idle()?;
        }
    }

    /// `awaitRecordingComplete` (`:3193-3206`).
    fn await_recording_complete(
        &mut self,
        recording_id: i64,
        position: i64,
        counter_id: i32,
    ) -> Result<(), ClusterError> {
        loop {
            let value = self
                .client
                .counters_reader()
                .and_then(|counters| counters.value(counter_id))
                .unwrap_or(0);
            if value >= position {
                return Ok(());
            }
            let active = self
                .client
                .counters_reader()
                .and_then(|counters| counters.key(counter_id))
                .and_then(|key| deepmsg_archive::server::recording_pos::parse_key(&key))
                .is_some_and(|identity| identity.recording_id == recording_id);
            if !active {
                return Err(ClusterError::Cluster(format!(
                    "recording has stopped unexpectedly: {recording_id}"
                )));
            }
            self.idle()?;
        }
    }

    /// `loadSnapshot` (`:2787-2836`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] when the replay cannot start, the image closes before
    /// the END marker, or a record is inconsistent.
    fn load_snapshot(&mut self, snapshot: &recording_log::Snapshot) -> Result<(), ClusterError> {
        let replay_channel = self.config.replay_channel.clone();
        let replay_stream_id = self.config.replay_stream_id;
        let subscription_id = self.archive.replay(
            &mut self.client,
            snapshot.recording_id,
            &replay_channel,
            replay_stream_id,
            &deepmsg_archive::client::proxy::ReplayParams {
                position: 0,
                // Replay the whole recording and stop at its end (the shape the
                // archive suite uses for a bounded replay).
                length: -2,
                ..deepmsg_archive::client::proxy::ReplayParams::default()
            },
        )?;

        let image_registration_id = self.await_replay_image(subscription_id)?;
        let mut adapter = ConsensusModuleSnapshotAdapter::new();

        loop {
            let mut handler = AgentSnapshotLoadHandler {
                sessions: &mut self.sessions,
                trackers: &mut self.trackers,
                timer: &mut *self.timer,
            };
            let work = adapter.poll(
                &mut self.client,
                subscription_id,
                image_registration_id,
                &mut handler,
            )?;
            if adapter.is_done() {
                break;
            }
            match work {
                Some(0) | None => {
                    self.poll_archive_events();
                    self.idle()?;
                }
                Some(_) => {
                    self.idle()?;
                }
            }
        }

        for tracker in self.trackers.iter_mut() {
            tracker.verify()?;
            tracker.reset();
        }
        self.timer.current_time(self.clock.time());
        write_counter(
            &self.client,
            self.counters.commit_position,
            snapshot.log_position,
        );
        self.leadership_term_id = snapshot.leadership_term_id;
        for tracker in self.trackers.iter_mut() {
            tracker.set_leadership_term_id(snapshot.leadership_term_id);
        }
        self.expected_ack_position = snapshot.log_position;
        let _ = self
            .client
            .remove_subscription(subscription_id, COMMAND_TIMEOUT);
        Ok(())
    }

    fn await_replay_image(&mut self, subscription_id: i64) -> Result<i64, ClusterError> {
        loop {
            if let Some(subscription) = self.client.subscription(subscription_id) {
                if let Some(image) = subscription.images().first() {
                    return Ok(image.registration_id());
                }
            }
            self.idle()?;
        }
    }

    /// `recoverFromSnapshotAndLog` (`:3495-3533`) for a module with no
    /// extension and (in this slice) no services.
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the plan, the snapshot load, or an ack that is
    /// missing its relevant id.
    fn recover_from_snapshot_and_log(&mut self) -> Result<(), ClusterError> {
        let plan = create_recovery_plan(
            &self.recording_log,
            &mut self.archive,
            &mut self.client,
            self.config.service_count,
            self.log_recording_id,
        )?;
        if let Some(log) = &plan.log {
            self.log_recording_id = log.recording_id;
        }
        self.recovery_plan = plan;

        let recovery_counter = self.add_recovery_state_counter();

        if let Some(snapshot) = self.recovery_plan.snapshots.first().copied() {
            self.load_snapshot(&snapshot)?;
        }

        // `recoverFromSnapshotAndLog`'s ack wait: vacuous at zero services.
        while !has_reached(
            self.expected_ack_position,
            self.service_ack_id,
            &self.service_ack_queues,
        )? {
            self.poll_service_control()?;
            self.idle()?;
        }
        let acks = crate::service_control::ack::poll_service_acks(&mut self.service_ack_queues)?;
        let null_ids = crate::service_control::ack::null_relevant_ids(
            "failed to start clustered service",
            &acks,
        );
        if !null_ids.is_empty() {
            return Err(ClusterError::AgentTermination(
                "failed to start clustered service(s)".to_owned(),
            ));
        }
        self.capture_service_client_ids(&acks);
        self.service_ack_id += 1;

        if let Some(counter) = recovery_counter {
            let _ = self.client.remove_counter(&counter, COMMAND_TIMEOUT);
        }
        Ok(())
    }

    /// `addRecoveryStateCounter` (`:2850-2875`): a counter the container finds
    /// its snapshot ids through, alive only for the recovery.
    fn add_recovery_state_counter(&mut self) -> Option<Counter> {
        let snapshot = self.recovery_plan.snapshots.first();
        let (term, log_position, timestamp) = snapshot.map_or(
            (self.recovery_plan.last_leadership_term_id, 0, 0),
            |snapshot| {
                (
                    snapshot.leadership_term_id,
                    snapshot.log_position,
                    snapshot.timestamp,
                )
            },
        );
        let snapshot_recording_ids = self
            .recovery_plan
            .snapshots
            .iter()
            .filter(|snapshot| snapshot.service_id >= 0)
            .map(|snapshot| snapshot.recording_id)
            .collect();
        let key = deepmsg_cluster_common::counters::RecoveryStateKey::new(
            term,
            log_position,
            timestamp,
            self.config.cluster_id,
            snapshot_recording_ids,
        )
        .ok()?;
        let spec = key.encode().ok()?;
        self.client
            .add_counter(
                deepmsg_cluster_common::counters::CLUSTER_RECOVERY_STATE_TYPE_ID,
                spec.key(),
                spec.label(),
                COMMAND_TIMEOUT,
            )
            .ok()
    }

    /// `captureServiceClientIds` (`:2877-2883`).
    fn capture_service_client_ids(&mut self, acks: &[ServiceAck]) {
        for (index, ack) in acks.iter().enumerate() {
            if let Some(slot) = self.service_client_ids.get_mut(index) {
                *slot = ack.relevant_id();
            }
        }
    }
}

/// The replay's record handlers, borrowing the agent's fields.
struct AgentLogReplayHandler<'a> {
    sessions: &'a mut SessionManager,
    timer: &'a mut dyn TimerService,
    trackers: &'a mut [PendingServiceMessageTracker],
    expired_timer_count: &'a mut deepmsg_cluster_common::collections::HashMap<i64, i64>,
    leadership_term_id: i64,
    service_count: i32,
    state_change: Option<ModuleState>,
    new_leadership_term_id: Option<i64>,
    deferred_snapshot: Option<(i64, i64)>,
    /// A `SNAPSHOT` action with default flags was replayed:
    /// `totalSnapshotDurationTracker.onSnapshotBegin` (`:1584`).
    snapshot_began: bool,
}

impl LogReplayHandler for AgentLogReplayHandler<'_> {
    fn on_replay_session_message(
        &mut self,
        cluster_session_id: i64,
        timestamp: i64,
    ) -> Result<(), ClusterError> {
        if let Some(session) = self.sessions.find_by_session_id_mut(cluster_session_id) {
            session.set_time_of_last_activity_ns(timestamp.saturating_mul(1_000_000));
        } else if cluster_session_id < 0 {
            // A service's own message: the follower sweeps its tracker.
            let service_id =
                crate::service_control::service_id_from_log_message(cluster_session_id);
            if let Some(tracker) = usize::try_from(service_id)
                .ok()
                .and_then(|index| self.trackers.get_mut(index))
            {
                tracker.sweep_follower_messages(cluster_session_id);
            }
        }
        Ok(())
    }

    fn on_replay_timer_event(&mut self, correlation_id: i64) -> Result<(), ClusterError> {
        if !self.timer.cancel_timer_by_correlation_id(correlation_id) {
            // The timer was already replayed as expired; a later schedule for
            // the same id is absorbed (`ConsensusModuleAgent.java:1539-1545`).
            let count = self
                .expired_timer_count
                .get(&correlation_id)
                .copied()
                .unwrap_or(0);
            self.expired_timer_count.insert(correlation_id, count + 1);
        }
        Ok(())
    }

    fn on_replay_session_open(
        &mut self,
        log_position: i64,
        correlation_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        response_stream_id: i32,
        response_channel: &str,
    ) -> Result<(), ClusterError> {
        self.sessions.on_replay_session_open(
            log_position,
            correlation_id,
            cluster_session_id,
            timestamp,
            response_stream_id,
            response_channel,
        )
    }

    fn on_replay_session_close(
        &mut self,
        cluster_session_id: i64,
        close_reason: deepmsg_codec_cluster::close_reason::CloseReason,
    ) -> Result<(), ClusterError> {
        self.sessions
            .on_replay_session_close(cluster_session_id, close_reason);
        Ok(())
    }

    fn on_replay_cluster_action(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        timestamp: i64,
        action: deepmsg_codec_cluster::cluster_action::ClusterAction,
        flags: i32,
    ) -> Result<(), ClusterError> {
        if leadership_term_id != self.leadership_term_id {
            return Ok(());
        }
        match action {
            deepmsg_codec_cluster::cluster_action::ClusterAction::SUSPEND => {
                self.state_change = Some(ModuleState::Suspended);
            }
            deepmsg_codec_cluster::cluster_action::ClusterAction::RESUME => {
                self.state_change = Some(ModuleState::Active);
            }
            deepmsg_codec_cluster::cluster_action::ClusterAction::SNAPSHOT if flags == 0 => {
                self.state_change = Some(ModuleState::Snapshot);
                self.snapshot_began = true;
                if self.service_count == 0 {
                    // `onReplayClusterAction` (`:1587`): with no services the
                    // snapshot set is complete immediately.
                    self.deferred_snapshot = Some((log_position, timestamp));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn on_replay_new_leadership_term_event(
        &mut self,
        leadership_term_id: i64,
        _log_position: i64,
        _timestamp: i64,
        _term_base_log_position: i64,
        _time_unit: i32,
        _app_version: i32,
    ) -> Result<(), ClusterError> {
        self.new_leadership_term_id = Some(leadership_term_id);
        Ok(())
    }

    fn on_replay_extension_message(
        &mut self,
        schema_id: u16,
        _template_id: u16,
        _version: u16,
        _message: &[u8],
    ) -> Result<(), ClusterError> {
        Err(ClusterError::Cluster(format!(
            "expected schemaId={}, actual={schema_id} \
             (no extension is configured)",
            deepmsg_codec_cluster::SBE_SCHEMA_ID
        )))
    }
}

impl ConsensusModuleAgent {
    /// Start replaying our own log tail (`newLogReplay`, `:1840-1843`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] when the archive will not start the replay or the
    /// subscription cannot be made.
    fn start_log_replay(
        &mut self,
        log_position: i64,
        append_position: i64,
    ) -> Result<crate::handles::LogReplay, ClusterError> {
        let replay_channel = self.config.replay_channel.clone();
        let replay_stream_id = self.config.replay_stream_id;
        let replay_session_id = self.archive.start_replay(
            &mut self.client,
            self.log_recording_id,
            &replay_channel,
            replay_stream_id,
            &deepmsg_archive::client::proxy::ReplayParams {
                position: log_position,
                length: append_position - log_position,
                ..deepmsg_archive::client::proxy::ReplayParams::default()
            },
        )?;

        // The subscription must name the replay's session id, which is the low
        // 32 bits of the replay id (`ArchiveConductor.java:956`).
        let log_session_id = replay_session_id as i32;
        let mut channel = ChannelUri::parse(&replay_channel)
            .map_err(|error| ClusterError::Cluster(error.to_string()))?;
        channel.put("session-id", log_session_id.to_string());
        let subscription_id =
            self.client
                .add_subscription(&channel.build(), replay_stream_id, COMMAND_TIMEOUT)?;

        let done = std::sync::Arc::new(AtomicBool::new(false));
        let position = std::sync::Arc::new(AtomicI64::new(log_position));
        self.log_replay = Some(AgentLogReplay {
            subscription_id,
            replay_session_id,
            log_session_id,
            start_position: log_position,
            stop_position: append_position,
            adapter: crate::log::LogAdapter::new(),
            done: std::sync::Arc::clone(&done),
            position: std::sync::Arc::clone(&position),
        });

        Ok(crate::handles::LogReplay::shared(done, position))
    }

    /// The agent's `replayLogPoll` (`:1845-1867`): advance the replay, commit
    /// what it has reached, and finish it at the stop position.
    ///
    /// # Errors
    ///
    /// [`ClusterError::ClusterEvent`] when the image closes before the stop
    /// position — the reference's "unexpected image close".
    fn poll_log_replay(&mut self) -> Result<usize, ClusterError> {
        let state_change;
        let new_leadership_term_id;
        let deferred_snapshot;
        let snapshot_began;
        let work;

        {
            let Self {
                client,
                archive,
                log_replay,
                sessions,
                timer,
                trackers,
                expired_timer_count,
                leadership_term_id,
                config,
                counters,
                ..
            } = self;

            let Some(replay) = log_replay.as_mut() else {
                return Ok(0);
            };

            if !replay.adapter.has_image() {
                let Some(image) =
                    client
                        .subscription(replay.subscription_id)
                        .and_then(|subscription| {
                            subscription.image_by_session_id(replay.log_session_id)
                        })
                else {
                    return Ok(0);
                };
                let join_position = image.join_position();
                if join_position != replay.start_position {
                    return Err(ClusterError::Cluster(format!(
                        "joinPosition={join_position} expected startPosition={}",
                        replay.start_position
                    )));
                }
                replay.adapter.set_image(image.registration_id());
                return Ok(1);
            }

            let mut handler = AgentLogReplayHandler {
                sessions,
                timer: &mut **timer,
                trackers,
                expired_timer_count,
                leadership_term_id: *leadership_term_id,
                service_count: config.service_count,
                state_change: None,
                new_leadership_term_id: None,
                deferred_snapshot: None,
                snapshot_began: false,
            };

            work = replay.adapter.poll(
                client,
                replay.subscription_id,
                &mut handler,
                config.log_fragment_limit,
            )?;
            state_change = handler.state_change;
            new_leadership_term_id = handler.new_leadership_term_id;
            deferred_snapshot = handler.deferred_snapshot;
            snapshot_began = handler.snapshot_began;

            let position = replay.adapter.position();
            let commit_position = counters.commit_position;
            let current = read_counter(&*client, commit_position).unwrap_or(0);
            if position > current {
                write_counter(&*client, commit_position, position);
            }
            replay
                .position
                .store(position, std::sync::atomic::Ordering::Release);

            if position >= replay.stop_position {
                // `LogReplay.close` (`:62-75`): stop the replay and give the
                // subscription back.
                let _ = archive.stop_replay(client, replay.replay_session_id);
                let _ = client.remove_subscription(replay.subscription_id, COMMAND_TIMEOUT);
                replay
                    .done
                    .store(true, std::sync::atomic::Ordering::Release);
                *log_replay = None;
            } else {
                let image_closed = client
                    .subscription(replay.subscription_id)
                    .and_then(|subscription| {
                        subscription.images().iter().find(|image| {
                            Some(image.registration_id()) == replay.adapter.image_registration_id()
                        })
                    })
                    .is_none_or(deepmsg_client::image::Image::is_closed);
                if image_closed {
                    return Err(ClusterError::ClusterEvent(format!(
                        "unexpected image close when replaying log: position={position}"
                    )));
                }
            }
        }

        if let Some(state) = state_change {
            self.set_state(state, "replayed cluster action");
        }
        if let Some(term) = new_leadership_term_id {
            self.leadership_term_id = term;
            for tracker in self.trackers.iter_mut() {
                tracker.set_leadership_term_id(term);
            }
        }
        if snapshot_began {
            // `onReplayClusterAction` (`:1584`): the replayed action starts the
            // snapshot-duration clock, as the live trigger does.
            self.snapshot_duration
                .on_snapshot_begin(self.clock.time_nanos());
        }
        if let Some((log_position, timestamp)) = deferred_snapshot {
            self.snapshot_on_service_ack(log_position, timestamp, &[])?;
        }

        Ok(work.unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ConsensusModuleAgentTest.shouldTerminateOnArchiveStorageError`: the
    /// classification is the part of that case that does not need the driver.
    #[test]
    fn storage_space_is_the_one_archive_refusal_that_ends_the_agent() {
        let storage_space = ClusterError::ArchiveRefused {
            code: 11,
            message: "low storage".to_owned(),
        };
        assert!(storage_space_refusal(&storage_space));

        for code in [0, 10, 12, 16] {
            assert!(!storage_space_refusal(&ClusterError::ArchiveRefused {
                code,
                message: String::new(),
            }));
        }
        assert!(!storage_space_refusal(&ClusterError::Cluster(
            "anything else".to_owned()
        )));
    }

    #[test]
    fn a_response_channel_is_unchanged_without_an_egress_template() {
        assert_eq!(
            "aeron:udp?endpoint=localhost:1234",
            refine_response_channel("aeron:udp?endpoint=localhost:1234", false, None)
        );
    }

    #[test]
    fn an_ipc_response_channel_becomes_the_egress_channel_unless_ipc_ingress_is_allowed() {
        let egress = "aeron:udp?endpoint=localhost:20000";
        assert_eq!(
            egress,
            refine_response_channel("aeron:ipc", false, Some(egress))
        );
        assert_eq!(
            "aeron:ipc",
            refine_response_channel("aeron:ipc", true, Some(egress))
        );
    }

    #[test]
    fn the_egress_channels_parameters_are_put_onto_a_udp_response_channel() {
        let refined = refine_response_channel(
            "aeron:udp?endpoint=localhost:1234",
            false,
            Some("aeron:udp?interface=127.0.0.1|ttl=3"),
        );
        let uri = ChannelUri::parse(&refined).expect("a URI");
        assert_eq!(Some("127.0.0.1"), uri.get("interface"));
        assert_eq!(Some("3"), uri.get("ttl"));
        assert_eq!(Some("localhost:1234"), uri.get("endpoint"));
    }

    #[test]
    fn ingress_endpoints_are_id_equals_endpoint_separated_by_commas() {
        let members = ClusterMember::parse_list(
            "0,localhost:1000,localhost:2000,localhost:3000,localhost:4000,localhost:5000",
        )
        .expect("two members");
        assert_eq!("0=localhost:1000", ingress_endpoints(&members));
    }
}
