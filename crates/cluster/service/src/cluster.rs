//! `Cluster<'_>`, the context a business callback is given.
//!
//! The reference's `Cluster` is an interface the container itself implements
//! (`CSA implements Cluster`), so a callback receives the container and can
//! call anything on it, including `idle()` that re-enters the agent. Rust has
//! no such self-reference: this is a **value assembled from disjoint field
//! borrows** for the duration of one callback (plan §3.1, the frozen field
//! table). Its eight fields are exactly the ones the plan froze:
//!
//! | Field | Why it is borrowed |
//! |---|---|
//! | `client` | `idle` polls the conductor; egress offers read publications by id |
//! | `sessions` | `session()` looks one up; `close_client_session()` marks it |
//! | `cm_proxy` | `offer`/`schedule_timer`/`cancel_timer`/`close_session` write stream 105 |
//! | `context` | the idle strategy, clocks, commit counter, mark file and lifecycle gate |
//! | `time` | `time()`/`time_unit()`: a value copied from the current log event |
//! | `log_position` | `logPosition()`: a value copied from the current log event |
//! | `role` | `role()` |
//! | `member_id` | `memberId()` |
//!
//! `session()` borrows `cm_proxy` mutably because an egress offer writes the
//! sticky `SessionMessageHeader` (`proxy.rs`); a callback therefore takes a
//! session view with `&mut Cluster`, exactly as it would in Java through a
//! shared reference.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use deepmsg_client::client::Client;
use deepmsg_cluster_common::mark::ClusterMarkFile;
use deepmsg_driver::idle::Strategy;

use crate::error::ContainerError;
use crate::proxy::{ConsensusModuleProxy, MOCKED_OFFER, OFFER_NOT_CONNECTED};
use crate::role::{Role, TimeUnit};
use crate::service::SessionId;
use crate::session::{ContainerClientSession, SessionTable};

/// How often the container's clock tick runs (`CSA.ONE_MILLISECOND_NS`).
pub const CLOCK_TICK_INTERVAL_NS: i64 = 1_000_000;

/// How often the mark file is stamped (`MARK_FILE_UPDATE_INTERVAL_NS`, `:200`).
pub const MARK_FILE_UPDATE_INTERVAL_NS: i64 = 1_000_000_000;

/// Which callback the container is inside, for `checkForValidInvocation`
/// (`CSA:1297-1304`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LifecycleCallback {
    /// Not inside a lifecycle callback.
    #[default]
    None,
    /// `onStart`.
    OnStart,
    /// `onTerminate`.
    OnTerminate,
    /// `onRoleChange`.
    OnRoleChange,
    /// `doBackgroundWork`.
    DoBackgroundWork,
}

impl LifecycleCallback {
    /// `lifecycleName` (`CSA:104-118`).
    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::OnStart => "onStart",
            Self::OnTerminate => "onTerminate",
            Self::OnRoleChange => "onRoleChange",
            Self::DoBackgroundWork => "doBackgroundWork",
        }
    }
}

/// The commit-position counter, remembered by both ids so a reclaimed counter
/// is recognised (`ReadableCounter.isClosed`, `CSA:1136-1142`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitPosition {
    /// The counter's id.
    pub counter_id: i32,
    /// The registration id it was allocated under.
    pub registration_id: i64,
}

/// The state a business may not see directly but `Cluster::idle` must drive:
/// the idle strategy, the clocks, the commit counter, the mark file and the
/// lifecycle gate.
///
/// Grouping them in one struct is the field table's answer to "how many
/// borrows does `idle` take": one.
pub struct AgentContext {
    idle: Strategy,
    idle_name: String,
    mark_file: ClusterMarkFile,
    commit_position: Option<CommitPosition>,
    last_slow_tick_ns: i64,
    mark_file_update_deadline_ms: i64,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    lifecycle: LifecycleCallback,
}

impl AgentContext {
    /// A context over the container's mark file.
    pub fn new(
        idle: Strategy,
        idle_name: String,
        mark_file: ClusterMarkFile,
        shutdown: Arc<AtomicBool>,
        abort: Arc<AtomicBool>,
    ) -> Self {
        Self {
            idle,
            idle_name,
            mark_file,
            commit_position: None,
            last_slow_tick_ns: 0,
            mark_file_update_deadline_ms: 0,
            shutdown,
            abort,
            lifecycle: LifecycleCallback::None,
        }
    }

    /// The mark file, for the error reporter and the termination path.
    pub const fn mark_file(&self) -> &ClusterMarkFile {
        &self.mark_file
    }

    /// Remember the commit counter once it exists.
    pub fn set_commit_position(&mut self, counter_id: i32, registration_id: i64) {
        self.commit_position = Some(CommitPosition {
            counter_id,
            registration_id,
        });
    }

    /// The commit counter, once found.
    pub const fn commit_position(&self) -> Option<CommitPosition> {
        self.commit_position
    }

    /// Enter a lifecycle callback; `Cluster` refuses sends while this is not
    /// [`LifecycleCallback::None`] (`CSA:1297-1304`).
    pub fn set_lifecycle(&mut self, callback: LifecycleCallback) {
        self.lifecycle = callback;
    }

    /// The callback currently running.
    pub const fn lifecycle(&self) -> LifecycleCallback {
        self.lifecycle
    }

    /// The shared flag a `SIGTERM` (or a test) sets.
    pub fn shutdown(&self) -> &Arc<AtomicBool> {
        &self.shutdown
    }

    /// Record that the client is gone, which is the reference's `abort`.
    pub fn abort(&self) {
        self.abort.store(true, Ordering::Release);
    }

    /// `checkForClockTick` (`CSA:1114-1155`).
    ///
    /// # Errors
    ///
    /// `AgentTermination` when the agent is asked to stop, the client closed,
    /// or the commit counter was reclaimed.
    pub fn check_for_clock_tick(
        &mut self,
        client: &mut Client,
        now_ns: i64,
    ) -> Result<bool, ContainerError> {
        if self.abort.load(Ordering::Acquire) {
            return Err(ContainerError::AgentTermination(
                "unexpected Aeron close".to_owned(),
            ));
        }
        if self.shutdown.load(Ordering::Acquire) {
            return Err(ContainerError::AgentTermination(
                "shutdown requested".to_owned(),
            ));
        }

        if now_ns - self.last_slow_tick_ns <= CLOCK_TICK_INTERVAL_NS {
            return Ok(false);
        }
        self.last_slow_tick_ns = now_ns;

        // The reference's `aeronAgentInvoker.invoke()`: the conductor runs here
        // and nowhere else in the container.
        let _ = client.poll();

        if self.abort.load(Ordering::Acquire) || client.is_closed() {
            self.abort();
            return Err(ContainerError::AgentTermination(
                "unexpected Aeron close".to_owned(),
            ));
        }

        if let Some(commit) = self.commit_position {
            let alive = client
                .counters_reader()
                .and_then(|counters| counters.get(commit.counter_id))
                .is_some_and(|descriptor| descriptor.registration_id == commit.registration_id);
            if !alive {
                return Err(ContainerError::AgentTermination(
                    "commit-pos counter unexpectedly closed, terminating".to_owned(),
                ));
            }
        }

        let now_ms = deepmsg_core::clock::epoch_millis();
        if now_ms >= self.mark_file_update_deadline_ms {
            self.mark_file_update_deadline_ms = now_ms + MARK_FILE_UPDATE_INTERVAL_NS / 1_000_000;
            self.mark_file.update_activity_timestamp(now_ms);
        }

        Ok(true)
    }

    /// `idle(workCount)` (`CSA:423-430`): the strategy, then the clock tick.
    ///
    /// The reference's `doIdleWork` also calls `doBackgroundWork`
    /// (`CSA:432-446`); a callback that calls `Cluster::idle` already holds
    /// `&mut service`, so this one cannot (plan deviation 1). Background work
    /// runs at the end of the next `do_work` instead.
    ///
    /// # Errors
    ///
    /// As [`Self::check_for_clock_tick`].
    pub fn idle(&mut self, client: &mut Client, work: usize) -> Result<(), ContainerError> {
        self.idle.idle(work);
        self.check_for_clock_tick(client, deepmsg_core::clock::monotonic_nano_time())?;
        Ok(())
    }

    /// `idleStrategy.reset()` (`CSA:1132` etc.).
    ///
    /// `Strategy` has no `reset`; a fresh strategy of the same kind is the same
    /// state the reference resets to.
    pub fn reset_idle(&mut self) {
        if let Some(strategy) = Strategy::by_agrona_name(&self.idle_name) {
            self.idle = strategy;
        }
    }
}

impl core::fmt::Debug for AgentContext {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AgentContext")
            .field("commit_position", &self.commit_position)
            .field("lifecycle", &self.lifecycle)
            .finish()
    }
}

/// The cluster time and its unit, copied out of the current log event.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClusterTime {
    /// `Cluster.time()`.
    pub time: i64,
    /// `Cluster.timeUnit()`.
    pub unit: TimeUnit,
}

/// `Cluster` (`Cluster.java:130-387`), assembled per callback.
pub struct Cluster<'a> {
    client: &'a mut Client,
    sessions: &'a mut SessionTable,
    cm_proxy: &'a mut ConsensusModuleProxy,
    context: &'a mut AgentContext,
    time: ClusterTime,
    log_position: i64,
    role: Role,
    member_id: i32,
}

impl<'a> Cluster<'a> {
    /// Assemble the view for one callback.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: &'a mut Client,
        sessions: &'a mut SessionTable,
        cm_proxy: &'a mut ConsensusModuleProxy,
        context: &'a mut AgentContext,
        time: ClusterTime,
        log_position: i64,
        role: Role,
        member_id: i32,
    ) -> Self {
        Self {
            client,
            sessions,
            cm_proxy,
            context,
            time,
            log_position,
            role,
            member_id,
        }
    }

    /// `memberId()`.
    pub const fn member_id(&self) -> i32 {
        self.member_id
    }

    /// `role()`.
    pub const fn role(&self) -> Role {
        self.role
    }

    /// `logPosition()`.
    pub const fn log_position(&self) -> i64 {
        self.log_position
    }

    /// `time()`: the log's time for the current message, never a wall clock.
    pub const fn time(&self) -> i64 {
        self.time.time
    }

    /// `timeUnit()`.
    pub const fn time_unit(&self) -> TimeUnit {
        self.time.unit
    }

    /// `ClusterClock.timeMicros()`.
    pub const fn time_micros(&self) -> i64 {
        self.time.unit.to_micros(self.time.time)
    }

    /// `ClusterClock.timeNanos()`.
    pub const fn time_nanos(&self) -> i64 {
        self.time.unit.to_nanos(self.time.time)
    }

    /// `getClientSession(id)` (`Cluster.java:166-192`).
    ///
    /// The view borrows the proxy mutably: an egress offer writes the sticky
    /// session header (`proxy.rs`).
    pub fn session(&mut self, id: SessionId) -> Option<ClientSession<'_>> {
        let Self {
            client,
            sessions,
            cm_proxy,
            context,
            time,
            role,
            ..
        } = self;

        sessions.get(id.0).map(|session| ClientSession {
            session,
            client,
            cm_proxy,
            time: *time,
            role: *role,
            lifecycle: context.lifecycle(),
        })
    }

    /// `clientSessions()` (`Cluster.java:176-180`), as read-only facts.
    ///
    /// The reference hands out a collection of live session objects; a view
    /// that can offer needs `&mut Cluster` (one at a time), so enumerating
    /// every session yields [`SessionInfo`] values instead — the same fields,
    /// no offer. Recorded in the plan's deviations.
    pub fn sessions(&self) -> impl Iterator<Item = SessionInfo<'_>> {
        self.sessions.iter().map(|session| SessionInfo {
            id: SessionId(session.id()),
            response_stream_id: session.response_stream_id(),
            response_channel: session.response_channel(),
            encoded_principal: session.encoded_principal(),
            is_closing: session.is_closing(),
        })
    }

    /// `closeClientSession(id)` (`Cluster.java:192-230`): three send attempts,
    /// `idle()` between them.
    ///
    /// # Errors
    ///
    /// [`ContainerError::Cluster`] for an unknown session — the reference's
    /// `ClusterException` — or a send the publication refused.
    pub fn close_client_session(&mut self, id: SessionId) -> Result<bool, ContainerError> {
        self.check_for_valid_invocation()?;

        let closing = {
            let Some(session) = self.sessions.get(id.0) else {
                return Err(ContainerError::Cluster(format!(
                    "unknown clusterSessionId: {}",
                    id.0
                )));
            };
            session.is_closing()
        };
        if closing {
            return Ok(true);
        }

        let mut attempts = 3;
        loop {
            if self.cm_proxy.close_session(self.client, id.0)? {
                if let Some(session) = self.sessions.get_mut(id.0) {
                    session.mark_closing();
                }
                return Ok(true);
            }
            self.context.idle(self.client, 0)?;
            attempts -= 1;
            if attempts == 0 {
                return Ok(false);
            }
        }
    }

    /// `scheduleTimer(correlationId, deadline)` (`Cluster.java:246-275`).
    ///
    /// # Errors
    ///
    /// [`ContainerError::Cluster`] from a refused send.
    pub fn schedule_timer(
        &mut self,
        correlation_id: i64,
        deadline: i64,
    ) -> Result<bool, ContainerError> {
        self.check_for_valid_invocation()?;
        self.cm_proxy
            .schedule_timer(self.client, correlation_id, deadline)
    }

    /// `cancelTimer(correlationId)` (`Cluster.java:277-314`).
    ///
    /// # Errors
    ///
    /// [`ContainerError::Cluster`] from a refused send.
    pub fn cancel_timer(&mut self, correlation_id: i64) -> Result<bool, ContainerError> {
        self.check_for_valid_invocation()?;
        self.cm_proxy.cancel_timer(self.client, correlation_id)
    }

    /// A **service message** (`Cluster.offer`, `:316-379`): `clusterSessionId`
    /// is this container's service id.
    ///
    /// # Errors
    ///
    /// [`ContainerError`] from the lifecycle gate or a refused send.
    pub fn offer(&mut self, payload: &[u8]) -> Result<i64, ContainerError> {
        self.check_for_valid_invocation()?;
        self.cm_proxy.offer_service_message(self.client, payload)
    }

    /// The cluster-members query (`ConsensusModuleProxy.clusterMembersQuery`).
    ///
    /// # Errors
    ///
    /// [`ContainerError`] from the lifecycle gate or a refused send.
    pub fn cluster_members_query(&mut self, correlation_id: i64) -> Result<bool, ContainerError> {
        self.check_for_valid_invocation()?;
        self.cm_proxy
            .cluster_members_query(self.client, correlation_id)
    }

    /// `idle(workCount)` (`Cluster.java:387-395`): poll the client and idle.
    ///
    /// # Errors
    ///
    /// [`ContainerError::AgentTermination`] when the agent must stop.
    pub fn idle(&mut self, work: usize) -> Result<(), ContainerError> {
        self.context.idle(self.client, work)
    }

    /// `checkForValidInvocation` (`CSA:1297-1304`).
    ///
    /// # Errors
    ///
    /// [`ContainerError::Cluster`] when called from a lifecycle callback.
    pub fn check_for_valid_invocation(&self) -> Result<(), ContainerError> {
        let lifecycle = self.context.lifecycle();
        if lifecycle == LifecycleCallback::None {
            Ok(())
        } else {
            Err(ContainerError::Cluster(format!(
                "sending messages or scheduling timers is not allowed from {}",
                lifecycle.name()
            )))
        }
    }
}

/// The read-only facts of a session, for `Cluster::sessions`.
#[derive(Clone, Copy, Debug)]
pub struct SessionInfo<'a> {
    /// `ClientSession.id()`.
    pub id: SessionId,
    /// `ClientSession.responseStreamId()`.
    pub response_stream_id: i32,
    /// `ClientSession.responseChannel()`.
    pub response_channel: &'a str,
    /// `ClientSession.encodedPrincipal()`.
    pub encoded_principal: &'a [u8],
    /// `ClientSession.isClosing()`.
    pub is_closing: bool,
}

/// `ClientSession` (`ClientSession.java:30-134`), for one callback.
pub struct ClientSession<'a> {
    session: &'a ContainerClientSession,
    client: &'a Client,
    cm_proxy: &'a mut ConsensusModuleProxy,
    time: ClusterTime,
    role: Role,
    lifecycle: LifecycleCallback,
}

impl ClientSession<'_> {
    /// `id()`.
    pub const fn id(&self) -> SessionId {
        SessionId(self.session.id())
    }

    /// `responseStreamId()`.
    pub const fn response_stream_id(&self) -> i32 {
        self.session.response_stream_id()
    }

    /// `responseChannel()`.
    pub fn response_channel(&self) -> &str {
        self.session.response_channel()
    }

    /// `encodedPrincipal()`.
    pub fn encoded_principal(&self) -> &[u8] {
        self.session.encoded_principal()
    }

    /// `isClosing()`.
    pub const fn is_closing(&self) -> bool {
        self.session.is_closing()
    }

    /// `offer(buffer, offset, length)` (`ClientSession.java:78-91`).
    ///
    /// Returns the reference's codes: a positive position, [`MOCKED_OFFER`] on
    /// a follower, [`OFFER_NOT_CONNECTED`] with no publication, or one of the
    /// negative `Publication` codes.
    ///
    /// # Errors
    ///
    /// [`ContainerError::Cluster`] when called from a lifecycle callback — the
    /// reference's `ClusterException`.
    pub fn offer(&mut self, payload: &[u8]) -> Result<i64, ContainerError> {
        self.check_for_valid_invocation()?;

        if self.role != Role::Leader {
            return Ok(MOCKED_OFFER);
        }
        let Some(publication_registration_id) = self.session.publication_registration_id() else {
            return Ok(OFFER_NOT_CONNECTED);
        };

        Ok(self.cm_proxy.offer_session_message(
            self.client,
            publication_registration_id,
            self.session.id(),
            self.time.time,
            payload,
        ))
    }

    /// `checkForValidInvocation` (`CSA:1297-1304`).
    ///
    /// # Errors
    ///
    /// As [`Cluster::check_for_valid_invocation`].
    pub fn check_for_valid_invocation(&self) -> Result<(), ContainerError> {
        if self.lifecycle == LifecycleCallback::None {
            Ok(())
        } else {
            Err(ContainerError::Cluster(format!(
                "sending messages or scheduling timers is not allowed from {}",
                self.lifecycle.name()
            )))
        }
    }
}
