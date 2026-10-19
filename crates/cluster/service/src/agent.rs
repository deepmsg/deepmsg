//! The container agent: `launch`, the duty cycle, and the log-to-business
//! dispatch.
//!
//! Counterpart of `ClusteredServiceAgent` (`ClusteredServiceAgent.java`,
//! `CSA`). P3-2a is the container **without snapshots**: the recovery read that
//! finds a snapshot recording id, and a `ClusterActionRequest(SNAPSHOT)`, both
//! end the container with an error naming `P3-2b`, rather than pretending the
//! service has no state to save.
//!
//! # The duty cycle (`CSA:247-281`)
//!
//! ```text
//! now = monotonic clock
//! if clock tick (1 ms)          -> poll the service stream (104) and act on it
//! if the log image exists       -> read up to the commit counter, dispatch
//!                                  (when a poll reads nothing and the image is
//!                                   done, close it and drop to follower)
//! service.do_background_work
//! ```
//!
//! # Where the borrows go
//!
//! `do_work` destructures `self` into disjoint fields before it polls the log,
//! because the log's handler needs `&mut Client` (to add or remove an egress
//! publication) while the image is being read; the image itself is owned by the
//! log adapter. The field table is frozen in the plan and restated in
//! `cluster.rs`.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use deepmsg_archive::client::{Archive, Handlers, NoCredentials, ReplayParams};
use deepmsg_client::client::Client;
use deepmsg_client::image::Image;
use deepmsg_cluster_common::counters;
use deepmsg_cluster_common::counters::recovery::RecoveryStateKey;
use deepmsg_cluster_common::mark::{self, ClusterMarkFile};
use deepmsg_codec_cluster::cluster_action::ClusterAction;
use deepmsg_codec_cluster_mark::cluster_component_type::ClusterComponentType;
use deepmsg_driver::idle::Strategy;

use crate::adapter::{JoinLogEvent, ServiceAdapter, ServiceEvents};
use crate::cluster::{AgentContext, Cluster, ClusterTime, CommitPosition, LifecycleCallback};
use crate::config::ServiceContainerConfig;
use crate::counters::{COMMAND_TIMEOUT, ErrorReporter};
use crate::error::ContainerError;
use crate::log_adapter::{
    BoundedLogAdapter, CLUSTER_ACTION_FLAGS_DEFAULT, LogEvent, LogHandler, LogMessage,
};
use crate::proxy::ConsensusModuleProxy;
use crate::role::{Role, TimeUnit};
use crate::service::{
    ClusteredService, LogHeader, NewLeadershipTermEvent, SessionId, SnapshotReader,
};
use crate::session::{ContainerClientSession, EGRESS_COMMAND_TIMEOUT, SessionTable};
use crate::snapshot::{ServiceSnapshotLoader, SnapshotLoadHandler, SnapshotSession};

/// `Aeron.NULL_VALUE` / `AeronArchive.NULL_POSITION`: what the reference uses
/// for "unset" in the three positions the service stream carries.
pub const NULL_POSITION: i64 = -1;

/// `AeronArchive.Configuration.localControlStreamId()`'s default, which the
/// container's mark file names as `archiveStreamId`
/// (`ClusteredServiceContainer.java:2094`).
pub const ARCHIVE_CONTROL_STREAM_ID: i32 = 10;

/// The handle a caller (a test, or P3-2c's `SIGTERM` hook) stops the container
/// with. The flag is read by the 1 ms clock tick.
#[derive(Clone, Debug)]
pub struct ShutdownHandle {
    flag: Arc<AtomicBool>,
}

impl ShutdownHandle {
    /// Ask the agent to leave its duty cycle at the next tick.
    pub fn stop(&self) {
        self.flag.store(true, Ordering::Release);
    }

    /// Whether a stop has been asked for.
    pub fn is_stopped(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }
}

/// `ClusteredServiceAgent<S>` (`CSA:119-132`).
pub struct ClusteredServiceAgent<S: ClusteredService> {
    config: ServiceContainerConfig,
    client: Client,
    /// The local archive, connected the first time a snapshot is taken or
    /// loaded. **A deviation from the reference**, which connects and closes one
    /// per operation (`CSA:938`, `:990`): this build's `Archive` has no
    /// `Drop`, so a per-operation connect would leak the control subscription
    /// and publication in the client. Holding one keeps the number of control
    /// sessions at one. Recorded in the plan (§7, D1).
    archive: Option<Archive>,
    service: S,
    context: AgentContext,
    error_reporter: ErrorReporter,
    cm_proxy: ConsensusModuleProxy,
    service_adapter: ServiceAdapter,
    log_adapter: BoundedLogAdapter,
    sessions: SessionTable,
    active_log_event: Option<JoinLogEvent>,
    termination_position: i64,
    termination_ack_required: bool,
    requested_ack_position: i64,
    ack_id: i64,
    log_position: i64,
    cluster_time: i64,
    time_unit: TimeUnit,
    role: Role,
    member_id: i32,
    is_service_active: bool,
    shutdown: ShutdownHandle,
}

impl<S: ClusteredService> ClusteredServiceAgent<S> {
    /// `ClusteredServiceContainer` construction plus `CSA.onStart`
    /// (`CSA:197-206`, `:765-815`).
    ///
    /// # Errors
    ///
    /// [`ContainerError`] when the client cannot reach the driver, the mark
    /// file cannot be created, a counter cannot be allocated, or the recovery
    /// state names a snapshot (P3-2b).
    pub fn launch(config: ServiceContainerConfig, service: S) -> Result<Self, ContainerError> {
        let mut client = Client::connect(&config.aeron_dir)?;

        let shutdown = ShutdownHandle {
            flag: Arc::new(AtomicBool::new(false)),
        };
        let abort = Arc::new(AtomicBool::new(false));
        {
            let abort = Arc::clone(&abort);
            client.add_close_handler(move || abort.store(true, Ordering::Release))?;
        }

        let now_ms = deepmsg_core::clock::epoch_millis();
        let mark_file_path = config.mark_file_path();
        if let Some(parent) = mark_file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mark_file = ClusterMarkFile::create(
            &mark_file_path,
            ClusterComponentType::CONTAINER,
            usize::try_from(config.error_buffer_length)
                .map_err(|_| ContainerError::Cluster("error buffer length".to_owned()))?,
            deepmsg_cnc::create::FILE_PAGE_SIZE_DEFAULT,
            now_ms,
            i64::from(std::process::id()),
        )?;
        mark_file.set_stream_ids(
            ARCHIVE_CONTROL_STREAM_ID,
            config.service_stream_id,
            config.consensus_module_stream_id,
            -1,
        );
        mark_file.set_service_id(config.service_id);
        mark_file.set_cluster_id(config.cluster_id);
        mark_file.set_member_id(-1);
        ensure_service_mark_link(&config.cluster_dir, &mark_file_path, config.service_id)?;
        mark_file.signal_ready(now_ms)?;

        let error_reporter =
            ErrorReporter::allocate(&mut client, config.cluster_id, config.service_id)?;

        let idle = Strategy::by_agrona_name(&config.idle_strategy).ok_or_else(|| {
            ContainerError::Cluster(format!(
                "{} names no idle strategy this build has",
                config.idle_strategy
            ))
        })?;

        let cm_publication = client.add_publication(
            &config.control_channel,
            config.consensus_module_stream_id,
            COMMAND_TIMEOUT,
        )?;
        let service_subscription = client.add_subscription(
            &config.control_channel,
            config.service_stream_id,
            COMMAND_TIMEOUT,
        )?;

        let mut cm_proxy = ConsensusModuleProxy::new(config.service_id);
        cm_proxy.set_publication(cm_publication);

        let context = AgentContext::new(
            idle,
            config.idle_strategy.clone(),
            mark_file,
            Arc::clone(&shutdown.flag),
            abort,
        );

        let mut agent = Self {
            log_adapter: BoundedLogAdapter::new(config.log_fragment_limit),
            config,
            client,
            archive: None,
            service,
            context,
            error_reporter,
            cm_proxy,
            service_adapter: ServiceAdapter::new(service_subscription),
            sessions: SessionTable::new(),
            active_log_event: None,
            termination_position: NULL_POSITION,
            termination_ack_required: true,
            requested_ack_position: NULL_POSITION,
            ack_id: 0,
            log_position: NULL_POSITION,
            cluster_time: 0,
            time_unit: TimeUnit::Milliseconds,
            role: Role::Follower,
            member_id: -1,
            is_service_active: false,
            shutdown,
        };

        agent.await_commit_position_counter()?;
        agent.recover_state()?;
        agent.is_service_active = true;
        Ok(agent)
    }

    /// How a caller stops the agent.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown.clone()
    }

    /// The container's service id.
    pub const fn service_id(&self) -> i32 {
        self.config.service_id
    }

    /// The role the node believes it has.
    pub const fn role(&self) -> Role {
        self.role
    }

    /// Whether every start-up step has completed.
    pub const fn is_service_active(&self) -> bool {
        self.is_service_active
    }

    // -- start-up ----------------------------------------------------------

    /// `awaitCommitPositionCounter` (`CSA:924-935`): the counter is the first
    /// thing the container and the module agree on.
    fn await_commit_position_counter(&mut self) -> Result<(), ContainerError> {
        self.context.reset_idle();
        loop {
            let found = self.client.counters_reader().and_then(|counters| {
                counters::find_cluster_counter(
                    &counters,
                    counters::CLUSTER_COMMIT_POSITION_TYPE_ID,
                    self.config.cluster_id,
                )
                .and_then(|counter_id| {
                    counters.get(counter_id).map(|descriptor| CommitPosition {
                        counter_id,
                        registration_id: descriptor.registration_id,
                    })
                })
            });
            if let Some(commit) = found {
                self.context
                    .set_commit_position(commit.counter_id, commit.registration_id);
                return Ok(());
            }
            self.context.idle(&mut self.client, 0)?;
        }
    }

    /// `recoverState` (`CSA:765-815`).
    fn recover_state(&mut self) -> Result<(), ContainerError> {
        let counter_id = self.await_recovery_counter()?;
        let key = self
            .client
            .counters_reader()
            .and_then(|counters| counters.key(counter_id))
            .ok_or_else(|| {
                ContainerError::Cluster(format!("recovery counter {counter_id} has no key"))
            })?;
        let recovery = RecoveryStateKey::decode(&key)?;

        self.log_position = recovery.log_position;
        self.cluster_time = recovery.timestamp;
        self.cm_proxy
            .set_leadership_term_id(recovery.leadership_term_id);

        // `loadSnapshot` (`CSA:767-775`): only a recovery state that names a
        // leadership term can name a snapshot, and only a snapshot id that is
        // not `NULL_POSITION` is loaded.
        let mut snapshot = None;
        let mut snapshot_error = None;
        if recovery.leadership_term_id != NULL_POSITION {
            match recovery.snapshot_recording_id(self.config.service_id) {
                Ok(recording_id) if recording_id != NULL_POSITION => {
                    match self.load_snapshot(recording_id) {
                        Ok(loaded) => snapshot = Some(loaded),
                        Err(error) => snapshot_error = Some(error),
                    }
                }
                Ok(_) => {}
                Err(error) => snapshot_error = Some(ContainerError::Cluster(error.to_string())),
            }
        }
        if let Some((_, unit)) = &snapshot {
            self.time_unit = *unit;
        }

        let result;
        self.context.set_lifecycle(LifecycleCallback::OnStart);
        {
            let Self {
                client,
                sessions,
                cm_proxy,
                context,
                service,
                cluster_time,
                time_unit,
                log_position,
                role,
                member_id,
                ..
            } = self;
            let mut cluster = Cluster::new(
                client,
                sessions,
                cm_proxy,
                context,
                ClusterTime {
                    time: *cluster_time,
                    unit: *time_unit,
                },
                *log_position,
                *role,
                *member_id,
            );
            result = match snapshot_error {
                Some(error) => Err(error),
                None => {
                    let mut reader = snapshot
                        .as_mut()
                        .map(|(image, _)| SnapshotReader::new(image));
                    service.on_start(&mut cluster, reader.as_mut())
                }
            };
        }
        self.context.set_lifecycle(LifecycleCallback::None);

        let ack_id = self.ack_id;
        self.ack_id += 1;
        let relevant_id = if result.is_ok() {
            self.client.client_id()
        } else {
            NULL_POSITION
        };
        while !self.cm_proxy.ack(
            &self.client,
            self.log_position,
            self.cluster_time,
            ack_id,
            relevant_id,
        )? {
            self.context.idle(&mut self.client, 0)?;
        }

        result
    }

    /// `awaitRecoveryCounter` (`CSA:816-827`).
    fn await_recovery_counter(&mut self) -> Result<i32, ContainerError> {
        self.context.reset_idle();
        loop {
            if let Some(counter_id) = self.client.counters_reader().and_then(|counters| {
                counters::find_recovery_state(&counters, self.config.cluster_id)
            }) {
                return Ok(counter_id);
            }
            self.context.idle(&mut self.client, 0)?;
        }
    }

    /// `loadSnapshot` + `loadState` (`CSA:936-986`): replay the recording from
    /// position 0, rebuild the session table, stop at END, and hand the same
    /// image to the business.
    ///
    /// The image is left positioned just after END, which is where the
    /// reference leaves it: the business's own records are what it reads.
    fn load_snapshot(&mut self, recording_id: i64) -> Result<(Image, TimeUnit), ContainerError> {
        let Self {
            client,
            archive,
            config,
            context,
            sessions,
            ..
        } = self;

        let archive = ensure_archive(archive, client, config)?;
        let subscription_id = archive
            .replay(
                client,
                recording_id,
                &config.replay_channel,
                config.replay_stream_id,
                // `startReplay(recordingId, 0, NULL_VALUE, …)` (`CSA:941`): the
                // default length, which is the NULL value — not the `-2` the
                // consensus module uses, which asks for an end-of-stream
                // marker this side does not wait for.
                &ReplayParams::default(),
            )
            .map_err(|error| {
                ContainerError::Cluster(format!(
                    "the snapshot replay for recordingId={recording_id} could not start: {error}"
                ))
            })?;

        let image_registration_id = loop {
            let image = client
                .subscription(subscription_id)
                .and_then(|subscription| subscription.images().first())
                .map(Image::registration_id);
            if let Some(registration_id) = image {
                break registration_id;
            }
            context.idle(client, 0)?;
        };

        let mut loader = ServiceSnapshotLoader::new();
        loop {
            let mut handler = SessionLoadHandler { sessions };
            let work = loader.poll(client, subscription_id, image_registration_id, &mut handler)?;
            if loader.is_done() {
                break;
            }

            match work {
                None => {
                    return Err(ContainerError::Cluster(
                        "snapshot ended unexpectedly".to_owned(),
                    ));
                }
                Some(0) => {
                    archive.check_for_error_response(client)?;
                    context.idle(client, 0)?;
                }
                Some(work) => {
                    archive.check_for_error_response(client)?;
                    context.idle(client, work)?;
                }
            }
        }

        // `ctx.appVersionValidator().isVersionCompatible` (`CSA:977-985`), whose
        // default is semantic-version **major** equality
        // (`AppVersionValidator.java:29-51`).
        if major_version(loader.app_version()) != major_version(config.app_version) {
            return Err(ContainerError::Cluster(format!(
                "incompatible app version: {} snapshot={}",
                semantic_version(config.app_version),
                semantic_version(loader.app_version())
            )));
        }

        let image = client
            .take_image(subscription_id, image_registration_id)
            .ok_or_else(|| {
                ContainerError::Cluster("the snapshot image ended with the snapshot".to_owned())
            })?;
        client.remove_subscription(subscription_id, COMMAND_TIMEOUT)?;

        Ok((image, loader.time_unit()))
    }

    // -- the duty cycle ----------------------------------------------------

    /// `doWork` (`CSA:247-281`).
    ///
    /// # Errors
    ///
    /// [`ContainerError::AgentTermination`] when the agent must stop; the
    /// caller closes it. Any other error also ends the duty cycle.
    pub fn do_work(&mut self) -> Result<usize, ContainerError> {
        let now_ns = deepmsg_core::clock::monotonic_nano_time();
        let mut work = 0;

        if self
            .context
            .check_for_clock_tick(&mut self.client, now_ns)?
        {
            work += self.poll_service_adapter()?;
        }

        if self.log_adapter.has_image() {
            work += self.poll_log()?;
        }

        work += self.invoke_background_work(now_ns);
        Ok(work)
    }

    fn poll_log(&mut self) -> Result<usize, ContainerError> {
        let commit = self.commit_position_value()?;
        let role = self.role;
        let member_id = self.member_id;
        let time_unit = self.time_unit;
        let log_position = self.log_position;
        let cluster_time = self.cluster_time;

        let (read, new_log_position, new_cluster_time) = {
            let Self {
                client,
                context,
                cm_proxy,
                sessions,
                service,
                log_adapter,
                error_reporter,
                config,
                ..
            } = self;

            let mut handler = EventHandler {
                service,
                client,
                sessions,
                cm_proxy,
                context,
                error_reporter,
                config: &*config,
                role,
                member_id,
                time_unit,
                log_position,
                cluster_time,
            };

            let read = log_adapter.poll(commit, &mut handler)?;
            let position = log_adapter
                .image()
                .map(deepmsg_client::image::Image::position);
            let subscriber = log_adapter
                .image()
                .map(deepmsg_client::image::Image::subscriber_position_id);
            let result = (read, handler.log_position, handler.cluster_time);

            if let (Some(position), Some(subscriber)) = (position, subscriber) {
                if let Some(counters) = client.counters_writable() {
                    counters.set_value(subscriber, position);
                }
            }
            result
        };

        self.log_position = new_log_position;
        self.cluster_time = new_cluster_time;

        if read == 0 && self.log_adapter.is_done() {
            self.close_log()?;
        }
        Ok(read)
    }

    fn commit_position_value(&self) -> Result<i64, ContainerError> {
        let Some(commit) = self.context.commit_position() else {
            return Ok(0);
        };
        self.client
            .counters_reader()
            .and_then(|counters| counters.value(commit.counter_id))
            .ok_or_else(|| {
                ContainerError::AgentTermination(
                    "commit-pos counter unexpectedly closed, terminating".to_owned(),
                )
            })
    }

    /// `pollServiceAdapter` (`CSA:1157-1199`).
    fn poll_service_adapter(&mut self) -> Result<usize, ContainerError> {
        let mut events = ServiceEvents::default();
        let work = self.service_adapter.poll(&mut self.client, &mut events)?;

        if let Some((position, ack_required)) = events.termination_position {
            self.termination_position = position;
            self.termination_ack_required = ack_required;
        }
        if let Some(position) = events.requested_ack_position {
            self.requested_ack_position = position;
        }
        if let Some(event) = events.join_log {
            // `onJoinLog` (`CSA:449-471`): the max is set to the event's
            // **logPosition** first, and the real maximum arrives with
            // `joinActiveLog`. That is the reference's order, quirk included.
            self.log_adapter.set_max_log_position(event.log_position);
            self.active_log_event = Some(event);
        }

        if self.active_log_event.is_some() && !self.log_adapter.has_image() {
            let event = self.active_log_event.take().expect("checked");
            self.join_active_log(event)?;
        }

        if self.termination_position != NULL_POSITION
            && self.log_position >= self.termination_position
        {
            if self.log_position > self.termination_position {
                self.report(ContainerError::ClusterEvent(format!(
                    "service terminate: logPosition={} > terminationPosition={}",
                    self.log_position, self.termination_position
                )));
            }
            return Err(self.terminate(
                self.log_position == self.termination_position,
                self.termination_ack_required,
            ));
        }

        if self.requested_ack_position != NULL_POSITION
            && self.log_position >= self.requested_ack_position
        {
            if self.log_position > self.requested_ack_position {
                self.report(ContainerError::ClusterEvent(format!(
                    "invalid ack request: logPosition={} > requestedAckPosition={}",
                    self.log_position, self.requested_ack_position
                )));
            }
            let ack_id = self.ack_id;
            self.ack_id += 1;
            while !self.cm_proxy.ack(
                &self.client,
                self.log_position,
                self.cluster_time,
                ack_id,
                NULL_POSITION,
            )? {
                self.context.idle(&mut self.client, 0)?;
            }
            self.requested_ack_position = NULL_POSITION;
        }

        Ok(work)
    }

    /// `joinActiveLog` (`CSA:845-909`).
    fn join_active_log(&mut self, event: JoinLogEvent) -> Result<(), ContainerError> {
        if self.role != Role::Leader {
            self.disconnect_egress();
        }

        let channel = format!(
            "{}|alias=log-sc-{}",
            event.log_channel, self.config.service_id
        );
        let subscription_id =
            self.client
                .add_subscription(&channel, event.log_stream_id, COMMAND_TIMEOUT)?;

        let image_registration_id = self.await_image(subscription_id, event.log_session_id)?;
        let join_position = {
            let subscription = self
                .client
                .subscription(subscription_id)
                .ok_or_else(|| ContainerError::Cluster("log subscription vanished".to_owned()))?;
            subscription
                .images()
                .iter()
                .find(|image| image.registration_id() == image_registration_id)
                .map(deepmsg_client::image::Image::join_position)
                .ok_or_else(|| ContainerError::Cluster("log image vanished".to_owned()))?
        };

        if join_position != self.log_position {
            return Err(ContainerError::AgentTermination(format!(
                "Cluster log must be contiguous for joining image: expectedPosition={} \
                 joinPosition={join_position}",
                self.log_position
            )));
        }
        if event.log_position != self.log_position {
            return Err(ContainerError::AgentTermination(format!(
                "Cluster log must be contiguous for active log event: expectedPosition={} \
                 eventPosition={}",
                self.log_position, event.log_position
            )));
        }

        let image = self
            .client
            .take_image(subscription_id, image_registration_id)
            .ok_or_else(|| ContainerError::Cluster("log image could not be taken".to_owned()))?;
        self.log_adapter.set_image(subscription_id, image);
        self.log_adapter
            .set_max_log_position(event.max_log_position);

        self.context.reset_idle();
        let ack_id = self.ack_id;
        self.ack_id += 1;
        while !self.cm_proxy.ack(
            &self.client,
            event.log_position,
            self.cluster_time,
            ack_id,
            NULL_POSITION,
        )? {
            self.context.idle(&mut self.client, 0)?;
        }

        self.member_id = event.member_id;
        self.context.mark_file().set_member_id(self.member_id);

        if event.role == Role::Leader.code() {
            let responding = self.config.responding_service;
            let startup = event.is_startup;
            let mut failures: Vec<String> = Vec::new();
            {
                let Self {
                    client, sessions, ..
                } = self;
                for session in sessions.iter_mut() {
                    if responding && !startup {
                        if let Err(error) = session.connect(client) {
                            failures.push(format!(
                                "failed to connect session response publication: {error}"
                            ));
                        }
                    }
                    session.reset_closing();
                }
            }
            for failure in failures {
                self.report(ContainerError::ClusterEvent(failure));
            }
        }

        let new_role = Role::get(i64::from(event.role))?;
        self.set_role(new_role)
    }

    /// `awaitImage(sessionId, subscription)` (`CSA:911-921`).
    fn await_image(
        &mut self,
        subscription_id: i64,
        session_id: i32,
    ) -> Result<i64, ContainerError> {
        self.context.reset_idle();
        loop {
            let _ = self.client.poll();
            let registration_id = self.client.subscription(subscription_id).and_then(|s| {
                s.image_by_session_id(session_id)
                    .map(deepmsg_client::image::Image::registration_id)
            });
            if let Some(registration_id) = registration_id {
                return Ok(registration_id);
            }
            self.context.idle(&mut self.client, 0)?;
        }
    }

    /// `closeLog` (`CSA:829-835`).
    fn close_log(&mut self) -> Result<(), ContainerError> {
        let position = self
            .log_adapter
            .image()
            .map(deepmsg_client::image::Image::position)
            .unwrap_or(0);
        self.log_position = position.max(self.log_position);

        if let Some(subscription_id) = self.log_adapter.subscription_registration_id() {
            self.client
                .remove_subscription(subscription_id, EGRESS_COMMAND_TIMEOUT)?;
        }
        self.log_adapter.clear_image();
        self.disconnect_egress();
        self.set_role(Role::Follower)
    }

    /// `disconnectEgress` (`CSA:837-843`).
    fn disconnect_egress(&mut self) {
        let Self {
            client,
            sessions,
            error_reporter: _,
            ..
        } = self;
        for session in sessions.iter_mut() {
            if session.publication_registration_id().is_some() {
                if let Err(error) = session.disconnect(client) {
                    eprintln!("failed to disconnect session response publication: {error}");
                }
            }
        }
    }

    /// `role(newRole)` (`CSA:748-760`): only a change is a callback.
    fn set_role(&mut self, new_role: Role) -> Result<(), ContainerError> {
        if new_role == self.role {
            return Ok(());
        }
        self.role = new_role;
        self.context.set_lifecycle(LifecycleCallback::OnRoleChange);
        let result = self.service.on_role_change(new_role);
        self.context.set_lifecycle(LifecycleCallback::None);
        result
    }

    /// `terminate(...)` (`CSA:1201-1241`).
    fn terminate(&mut self, expected: bool, ack_required: bool) -> ContainerError {
        self.is_service_active = false;
        self.context.set_lifecycle(LifecycleCallback::OnTerminate);
        let result = {
            let Self {
                client,
                sessions,
                cm_proxy,
                context,
                service,
                cluster_time,
                time_unit,
                log_position,
                role,
                member_id,
                ..
            } = self;
            let mut cluster = Cluster::new(
                client,
                sessions,
                cm_proxy,
                context,
                ClusterTime {
                    time: *cluster_time,
                    unit: *time_unit,
                },
                *log_position,
                *role,
                *member_id,
            );
            service.on_terminate(&mut cluster)
        };
        self.context.set_lifecycle(LifecycleCallback::None);
        if let Err(error) = result {
            self.report(error);
        }

        if ack_required {
            let mut attempts = 5;
            let ack_id = self.ack_id;
            self.ack_id += 1;
            while attempts > 0 {
                match self.cm_proxy.ack(
                    &self.client,
                    self.log_position,
                    self.cluster_time,
                    ack_id,
                    NULL_POSITION,
                ) {
                    Ok(true) => break,
                    Ok(false) | Err(_) => {
                        attempts -= 1;
                        if attempts == 0 {
                            break;
                        }
                        let _ = self.context.idle(&mut self.client, 0);
                    }
                }
            }
        }

        self.termination_position = NULL_POSITION;
        ContainerError::AgentTermination(format!("service terminated, isExpected={expected}"))
    }

    fn invoke_background_work(&mut self, now_ns: i64) -> usize {
        self.context
            .set_lifecycle(LifecycleCallback::DoBackgroundWork);
        let work = self.service.do_background_work(now_ns);
        self.context.set_lifecycle(LifecycleCallback::None);
        work
    }

    fn report(&mut self, error: ContainerError) {
        let now_ms = deepmsg_core::clock::epoch_millis();
        let Self {
            client,
            context,
            error_reporter,
            ..
        } = self;
        error_reporter.report(client, context.mark_file(), now_ms, &error);
    }

    // -- close -------------------------------------------------------------

    /// `onClose` (`CSA:209-245`) and `Context.close`.
    ///
    /// Consumes the agent: after this there is no client to speak through.
    /// # Errors
    ///
    /// [`ContainerError`] only from the egress removals, which are reported
    /// rather than raised in the reference.
    pub fn close(mut self) -> Result<(), ContainerError> {
        if self.is_service_active {
            self.is_service_active = false;
            self.context.set_lifecycle(LifecycleCallback::OnTerminate);
            let result = {
                let Self {
                    client,
                    sessions,
                    cm_proxy,
                    context,
                    service,
                    cluster_time,
                    time_unit,
                    log_position,
                    role,
                    member_id,
                    ..
                } = &mut self;
                let mut cluster = Cluster::new(
                    client,
                    sessions,
                    cm_proxy,
                    context,
                    ClusterTime {
                        time: *cluster_time,
                        unit: *time_unit,
                    },
                    *log_position,
                    *role,
                    *member_id,
                );
                service.on_terminate(&mut cluster)
            };
            self.context.set_lifecycle(LifecycleCallback::None);
            if let Err(error) = result {
                self.report(error);
            }
        }

        // Every step below is best effort, as the reference's `CloseHelper`
        // calls are: this runs on the way out, and one refused removal must not
        // keep the mark file from saying the container is done.
        if let Some(subscription_id) = self.log_adapter.subscription_registration_id() {
            if let Err(error) = self
                .client
                .remove_subscription(subscription_id, EGRESS_COMMAND_TIMEOUT)
            {
                eprintln!("failed to remove the log subscription on close: {error}");
            }
        }
        self.log_adapter.clear_image();
        self.disconnect_egress();
        if let Err(error) = self.context.mark_file().signal_terminated() {
            eprintln!("failed to signal the mark file terminated: {error}");
        }
        self.client.close();
        Ok(())
    }
}

/// The log's handler: one method per `BoundedLogAdapter.onMessage` arm
/// (`BoundedLogAdapter.java:136-260`), all with the same disjoint borrows.
struct EventHandler<'a, S: ClusteredService> {
    service: &'a mut S,
    client: &'a mut Client,
    sessions: &'a mut SessionTable,
    cm_proxy: &'a mut ConsensusModuleProxy,
    context: &'a mut AgentContext,
    error_reporter: &'a mut ErrorReporter,
    config: &'a ServiceContainerConfig,
    role: Role,
    member_id: i32,
    time_unit: TimeUnit,
    log_position: i64,
    cluster_time: i64,
}

impl<S: ClusteredService> EventHandler<'_, S> {
    fn report(&mut self, error: ContainerError) {
        let now_ms = deepmsg_core::clock::epoch_millis();
        self.error_reporter
            .report(&*self.client, self.context.mark_file(), now_ms, &error);
    }
}

impl<S: ClusteredService> LogHandler for EventHandler<'_, S> {
    fn on_log_event(&mut self, event: LogEvent<'_>) -> Result<(), ContainerError> {
        self.log_position = event.position;
        let header = LogHeader {
            position: event.position,
            session_id: event.session_id,
            stream_id: event.stream_id,
        };

        match event.message {
            LogMessage::SessionMessage {
                cluster_session_id,
                timestamp,
            } => {
                self.cluster_time = timestamp;
                let session = self
                    .sessions
                    .get(cluster_session_id)
                    .map(|_| SessionId(cluster_session_id));
                let mut cluster = Cluster::new(
                    &mut *self.client,
                    &mut *self.sessions,
                    &mut *self.cm_proxy,
                    &mut *self.context,
                    ClusterTime {
                        time: self.cluster_time,
                        unit: self.time_unit,
                    },
                    self.log_position,
                    self.role,
                    self.member_id,
                );
                self.service.on_session_message(
                    &mut cluster,
                    session,
                    timestamp,
                    event.payload,
                    &header,
                )
            }
            LogMessage::TimerEvent {
                correlation_id,
                timestamp,
            } => {
                self.cluster_time = timestamp;
                let mut cluster = Cluster::new(
                    &mut *self.client,
                    &mut *self.sessions,
                    &mut *self.cm_proxy,
                    &mut *self.context,
                    ClusterTime {
                        time: self.cluster_time,
                        unit: self.time_unit,
                    },
                    self.log_position,
                    self.role,
                    self.member_id,
                );
                self.service
                    .on_timer_event(&mut cluster, correlation_id, timestamp)
            }
            LogMessage::SessionOpen {
                leadership_term_id,
                cluster_session_id,
                timestamp,
                response_stream_id,
                response_channel,
                encoded_principal,
            } => {
                self.cluster_time = timestamp;
                if self.sessions.get(cluster_session_id).is_some() {
                    return Err(ContainerError::AgentTermination(format!(
                        "clashing open clusterSessionId={cluster_session_id} \
                         leadershipTermId={leadership_term_id} logPosition={}",
                        self.log_position
                    )));
                }

                let mut session = ContainerClientSession::new(
                    cluster_session_id,
                    response_stream_id,
                    response_channel,
                    encoded_principal,
                );
                if self.role == Role::Leader && self.config.responding_service {
                    if let Err(error) = session.connect(self.client) {
                        self.report(ContainerError::ClusterEvent(format!(
                            "failed to connect session response publication: {error}"
                        )));
                    }
                }
                self.sessions.add(session);

                let mut cluster = Cluster::new(
                    &mut *self.client,
                    &mut *self.sessions,
                    &mut *self.cm_proxy,
                    &mut *self.context,
                    ClusterTime {
                        time: self.cluster_time,
                        unit: self.time_unit,
                    },
                    self.log_position,
                    self.role,
                    self.member_id,
                );
                self.service
                    .on_session_open(&mut cluster, SessionId(cluster_session_id), timestamp)
            }
            LogMessage::SessionClose {
                leadership_term_id,
                cluster_session_id,
                timestamp,
                close_reason,
            } => {
                self.cluster_time = timestamp;
                let Some(mut session) = self.sessions.remove(cluster_session_id) else {
                    self.report(ContainerError::ClusterEvent(format!(
                        "unknown session close: clusterSessionId={cluster_session_id} \
                         closeReason={close_reason:?} leadershipTermId={leadership_term_id} \
                         logPosition={}",
                        self.log_position
                    )));
                    return Ok(());
                };

                if let Err(error) = session.disconnect(self.client) {
                    self.report(ContainerError::ClusterEvent(format!(
                        "failed to disconnect session response publication: {error}"
                    )));
                }

                let mut cluster = Cluster::new(
                    &mut *self.client,
                    &mut *self.sessions,
                    &mut *self.cm_proxy,
                    &mut *self.context,
                    ClusterTime {
                        time: self.cluster_time,
                        unit: self.time_unit,
                    },
                    self.log_position,
                    self.role,
                    self.member_id,
                );
                self.service.on_session_close(
                    &mut cluster,
                    SessionId(cluster_session_id),
                    timestamp,
                    close_reason,
                )
            }
            LogMessage::ClusterAction {
                leadership_term_id,
                log_position,
                timestamp,
                action,
                flags,
            } => {
                self.cluster_time = timestamp;
                if action == ClusterAction::SNAPSHOT && flags == CLUSTER_ACTION_FLAGS_DEFAULT {
                    return Err(ContainerError::AgentTermination(format!(
                        "taking the service snapshot at logPosition={log_position} \
                         leadershipTermId={leadership_term_id} is P3-2b"
                    )));
                }
                Ok(())
            }
            LogMessage::NewLeadershipTerm {
                leadership_term_id,
                log_position,
                timestamp,
                term_base_log_position,
                leader_member_id,
                log_session_id,
                time_unit,
                app_version,
            } => {
                if major_version(app_version) != major_version(self.config.app_version) {
                    return Err(ContainerError::AgentTermination(format!(
                        "incompatible version: {}.{}.{} log={app_version}",
                        major_version(self.config.app_version),
                        (self.config.app_version >> 8) & 0xff,
                        self.config.app_version & 0xff,
                    )));
                }

                self.cm_proxy.set_leadership_term_id(leadership_term_id);
                self.log_position = log_position;
                self.cluster_time = timestamp;
                let unit = TimeUnit::from_cluster_time_unit(time_unit);
                self.time_unit = unit;

                let event = NewLeadershipTermEvent {
                    leadership_term_id,
                    log_position,
                    timestamp,
                    term_base_log_position,
                    leader_member_id,
                    log_session_id,
                    time_unit: unit,
                    app_version,
                };
                let mut cluster = Cluster::new(
                    &mut *self.client,
                    &mut *self.sessions,
                    &mut *self.cm_proxy,
                    &mut *self.context,
                    ClusterTime {
                        time: self.cluster_time,
                        unit: self.time_unit,
                    },
                    self.log_position,
                    self.role,
                    self.member_id,
                );
                self.service
                    .on_new_leadership_term_event(&mut cluster, &event)
            }
        }
    }
}

/// `SemanticVersion.major`.
const fn major_version(version: i32) -> i32 {
    (version >> 16) & 0xff
}

/// `SemanticVersion.toString`.
fn semantic_version(version: i32) -> String {
    format!(
        "{}.{}.{}",
        (version >> 16) & 0xff,
        (version >> 8) & 0xff,
        version & 0xff
    )
}

/// The container's archive, connected on first use (deviation D1, see the
/// field's comment).
fn ensure_archive<'a>(
    archive: &'a mut Option<Archive>,
    client: &mut Client,
    config: &ServiceContainerConfig,
) -> Result<&'a mut Archive, ContainerError> {
    if archive.is_none() {
        let context = config.archive_context();
        let connected = Archive::connect(&context, client, &mut NoCredentials, Handlers::default())
            .map_err(|error| {
                ContainerError::Cluster(format!("the archive could not be connected: {error}"))
            })?;
        *archive = Some(connected);
    }

    Ok(archive.as_mut().expect("connected above"))
}

/// The agent as the loader's listener (`CSA.addSession`, `:628-652`): a session
/// is rebuilt with no egress publication, because the join that follows decides
/// the role and connects it there.
struct SessionLoadHandler<'a> {
    sessions: &'a mut SessionTable,
}

impl SnapshotLoadHandler for SessionLoadHandler<'_> {
    fn on_load_begin_snapshot(
        &mut self,
        _app_version: i32,
        _time_unit: TimeUnit,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_load_cluster_session(
        &mut self,
        session: SnapshotSession<'_>,
    ) -> Result<(), ContainerError> {
        self.sessions.add(ContainerClientSession::new(
            session.cluster_session_id,
            session.response_stream_id,
            session.response_channel.to_owned(),
            session.encoded_principal.to_vec(),
        ));
        Ok(())
    }

    fn on_load_end_snapshot(&mut self) -> Result<(), ContainerError> {
        Ok(())
    }
}

/// The `.lnk` the cluster directory gets when the mark file lives elsewhere
/// (`MarkFile.ensureMarkFileLink`, `ClusterMarkFile.linkFilenameForService`).
fn ensure_service_mark_link(
    cluster_dir: &Path,
    mark_file: &Path,
    service_id: i32,
) -> Result<(), ContainerError> {
    std::fs::create_dir_all(cluster_dir)?;
    let link = cluster_dir.join(format!(
        "{}{service_id}{}",
        mark::SERVICE_FILENAME_PREFIX,
        mark::LINK_FILE_EXTENSION
    ));

    let mark_directory = mark_file
        .parent()
        .ok_or_else(|| ContainerError::Cluster("the mark file has no directory".to_owned()))?;
    let cluster_directory = cluster_dir.canonicalize()?;
    let mark_directory = mark_directory.canonicalize()?;

    if cluster_directory == mark_directory {
        if link.exists() {
            std::fs::remove_file(&link)?;
        }
        return Ok(());
    }

    std::fs::write(&link, mark_directory.to_string_lossy().as_bytes())?;
    Ok(())
}
