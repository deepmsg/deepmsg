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
use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;
use deepmsg_codec_cluster_mark::cluster_component_type::ClusterComponentType;
use deepmsg_core::clock;
use deepmsg_core::uri::ChannelUri;
use deepmsg_driver::idle::Strategy;

use crate::clock::{ClusterClock, MillisecondClusterClock};
use crate::config::{CLUSTER_IDLE_STRATEGY_PROP_NAME, ConsensusModuleConfig};
use crate::election::{Election, ElectionHost, ElectionState};
use crate::error::ClusterError;
use crate::handles::{LogReplay, RecordingReplication};
use crate::log_publisher::LogPublisher;
use crate::persist::node_state::NodeStateFile;
use crate::persist::recording_log::{self, RecordingExtent, RecordingLog, RecoveryPlan};
use crate::state::{ModuleState, Role};

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

/// The default authenticator's class name, which the marker's var-data carries
/// (`ConsensusModule.java:807`). P3-4b instantiates it; P3-4a only records it.
const DEFAULT_AUTHENTICATOR: &str = "io.aeron.cluster.DefaultAuthenticatorSupplier";

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
    append_position: Option<AppendPosition>,

    state: ModuleState,
    role: Role,
    election: Option<Election>,
    leadership_term_id: i64,

    last_append_position: i64,
    last_quorum_backtrack_commit_position: i64,
    time_of_last_log_update_ns: i64,
    time_of_last_leader_update_ns: i64,
    notified_commit_position: i64,
    slow_tick_deadline_ns: i64,
    mark_file_update_deadline_ns: i64,

    consensus_subscription_id: i64,
    live_log_destination: String,
    catchup_log_destination: String,
    closed: bool,
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
    pub fn launch(mut config: ConsensusModuleConfig) -> Result<Self, ClusterError> {
        config.conclude()?;

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
                DEFAULT_AUTHENTICATOR,
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
        if members.len() != 1 {
            return Err(ClusterError::Cluster(format!(
                "a cluster of {} members is P3-6b; this slice runs one member",
                members.len()
            )));
        }
        let this_member_index = members
            .iter()
            .position(|member| member.id == config.member_id)
            .ok_or_else(|| {
                ClusterError::Cluster(format!(
                    "member id {} is not in aeron.cluster.members",
                    config.member_id
                ))
            })?;

        if config.service_count > 0 {
            return Err(ClusterError::Cluster(
                "service_count > 0 needs the service proxy (P3-4c)".to_owned(),
            ));
        }

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

        let clock = MillisecondClusterClock;
        let idle_strategy = Strategy::by_agrona_name(&config.idle_strategy).ok_or_else(|| {
            ClusterError::Cluster(format!(
                "{CLUSTER_IDLE_STRATEGY_PROP_NAME}={} names no idle strategy this build has",
                config.idle_strategy
            ))
        })?;
        let max_cycle_time = counters.max_cycle_time;
        let cycle_threshold_exceeded = counters.cycle_threshold_exceeded;
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
            ranked_positions: vec![0; member::quorum_threshold(1)],
            publisher: ConsensusPublisher::new(0),
            log_publisher: LogPublisher::new(),
            log_publication_channel_tag: 0,
            log_publication_session_id: -1,
            log_recording_id: NULL_VALUE,
            log_recording_stop_position: 0,
            log_subscription_id: NULL_VALUE,
            append_position: None,
            state: ModuleState::Init,
            role: Role::Follower,
            election: None,
            leadership_term_id: NULL_VALUE,
            last_append_position: NULL_VALUE,
            last_quorum_backtrack_commit_position: NULL_POSITION,
            time_of_last_log_update_ns: now_ns,
            time_of_last_leader_update_ns: now_ns,
            notified_commit_position: 0,
            slow_tick_deadline_ns: 0,
            mark_file_update_deadline_ns: 0,
            consensus_subscription_id,
            live_log_destination: String::new(),
            catchup_log_destination: String::new(),
            closed: false,
        };
        // The tracker's threshold is the configured one; set after the struct
        // exists because both counters were allocated by `allocate_counters`.
        agent.duty_cycle.threshold_ns = agent.config.cycle_threshold_ns;

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

        let election = Election::new(
            self.config.member_id,
            vec![self.config.member_id],
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
    fn drive_election_once(&mut self, now_ns: i64) -> Result<usize, ClusterError> {
        let _ = now_ns;
        let Some(mut election) = self.election.take() else {
            return Ok(0);
        };
        let mut publisher = mem::take(&mut self.publisher);
        let result = election.do_work(self, &mut publisher);
        self.publisher = publisher;

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

        // Sessions (`SessionManager.sendRedirects/sendRejections/checkSessions`)
        // are P3-4b; the cluster control toggle is P3-4e.
        if self.election.is_none() && self.role == Role::Leader && self.state == ModuleState::Active
        {
            // One member has a quorum by definition, so `hasActiveQuorum`
            // cannot fail here; the check belongs to P3-6b with the member table.
        }

        Ok(work)
    }

    /// `consensusAdapter.poll()` (`:423`): read the members' consensus
    /// messages.
    ///
    /// The dispatcher is `P3-5b`/`P3-6a`; a message arriving here is a member
    /// speaking to a node that cannot answer, so it is a refusal rather than a
    /// dropped frame. A one-member cluster has nobody to hear from.
    fn consensus_adapter_poll(&mut self) -> Result<usize, ClusterError> {
        let mut received = 0;
        self.client.poll_subscription(
            self.consensus_subscription_id,
            CONSENSUS_FRAGMENT_LIMIT,
            |_message| received += 1,
        );
        if received > 0 {
            return Err(ClusterError::Cluster(
                "consensus message handling is P3-5b".to_owned(),
            ));
        }
        Ok(0)
    }

    /// `consensusWork` (`:2481-2543`), leader half.
    ///
    /// The follower half needs the log adapter and the follower position
    /// publisher, both `P3-4b`/`P3-6a`; a node that reaches this method as a
    /// follower is one the reference would not produce in this slice, so it is a
    /// refusal rather than a silent `0`.
    fn consensus_work(&mut self, timestamp: i64, now_ns: i64) -> Result<usize, ClusterError> {
        let _ = timestamp;
        if self.role != Role::Leader {
            return Err(ClusterError::Cluster(
                "follower steady state needs the log adapter (P3-4b)".to_owned(),
            ));
        }

        let mut work = 0;
        // Timers are P3-4c; the reference's `timerService.poll(timestamp)` on an
        // empty table is zero work, which is what a P3-4a node has.
        work += self.update_leader_position_internal(now_ns)?;

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
        self.time_of_last_log_update_ns = now_ns;
        self.time_of_last_leader_update_ns = now_ns;
        Ok(1)
    }

    fn publish_commit_position(
        &mut self,
        _commit_position: i64,
        _leadership_term_id: i64,
    ) -> Result<(), ClusterError> {
        if self.members.len() > 1 {
            return Err(ClusterError::Cluster(
                "publishing a commit position needs consensus publications (P3-6a)".to_owned(),
            ));
        }
        Ok(())
    }

    fn join_log_as_leader(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        log_session_id: i32,
        _is_startup: bool,
    ) -> Result<(), ClusterError> {
        self.leadership_term_id = leadership_term_id;
        self.set_counter_value(self.counters.leadership_term, leadership_term_id);

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

        if self.config.service_count > 0 {
            return Err(ClusterError::Cluster(
                "waiting for service acks needs the service proxy (P3-4c)".to_owned(),
            ));
        }

        self.live_log_destination = if is_ipc {
            channel
        } else {
            format!("aeron-spy:{channel}")
        };
        self.last_append_position = log_position;

        Ok(())
    }

    fn try_join_log_as_follower(
        &mut self,
        _image: deepmsg_client::image::Image,
        _is_leader_startup: bool,
        _now_ns: i64,
    ) -> Result<bool, ClusterError> {
        Err(ClusterError::Cluster(
            "joining a log as a follower is P3-6b".to_owned(),
        ))
    }

    fn add_log_publication(&mut self, append_position: i64) -> Result<usize, ClusterError> {
        ConsensusModuleAgent::add_log_publication(self, append_position)
    }

    fn log_session_id(&self) -> i32 {
        self.log_publication_session_id
    }

    fn new_log_replay(
        &mut self,
        _log_position: i64,
        _append_position: i64,
    ) -> Result<LogReplay, ClusterError> {
        Err(ClusterError::Cluster("replaying a log is P3-4d".to_owned()))
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
        _member_id: i32,
        _leadership_term_id: i64,
        _append_position_flags: i16,
    ) -> Result<(), ClusterError> {
        Err(ClusterError::Cluster("catchup is P3-6b".to_owned()))
    }

    fn stop_all_catchups(&mut self) -> Result<(), ClusterError> {
        Err(ClusterError::Cluster("catchup is P3-6b".to_owned()))
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

    fn election_complete(&mut self, now_ns: i64) -> Result<(), ClusterError> {
        self.set_counter_value(self.counters.leadership_term, self.leadership_term_id);

        if self.role == Role::Leader {
            self.time_of_last_log_update_ns = now_ns - self.config.leader_heartbeat_interval_ns;
            activate_toggle(&self.client, self.counters.control_toggle);
        } else {
            self.time_of_last_log_update_ns = now_ns;
            self.live_log_destination.clear();
        }
        activate_toggle(&self.client, self.counters.node_control_toggle);

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

        // `connectIngress()` is P3-4b; the leader member is this one by the
        // single-member rule.
        self.this_member_index = self
            .members
            .iter()
            .position(|member| member.id == self.config.member_id)
            .unwrap_or(self.this_member_index);

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
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

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

/// `ClusterControl.ToggleState.activate`: INACTIVE (0) becomes NEUTRAL (1).
fn activate_toggle(client: &Client, counter: Counter) {
    if read_counter(client, counter).unwrap_or(0) == 0 {
        write_counter(client, counter, 1);
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
