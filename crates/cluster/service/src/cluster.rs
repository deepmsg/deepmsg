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
use deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH;
use deepmsg_driver::idle::Strategy;

use crate::config::ServiceContainerConfig;
use crate::error::ContainerError;
use crate::proxy::{
    Claimed, ConsensusModuleProxy, MOCKED_OFFER, OFFER_NOT_CONNECTED, SESSION_HEADER_LENGTH,
};
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
        // `CSA.idle(int)`: the clock tick only when the caller reports no work
        // (`:423-430`); `idle()` with no argument always runs it.
        if work == 0 {
            self.check_for_clock_tick(client, deepmsg_core::clock::monotonic_nano_time())?;
        }
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
    /// What `Cluster.context()` hands a business. The third borrow of the
    /// container's own state, and the only **shared** one: nothing writes a
    /// `ServiceContainerConfig` after launch, so it cannot conflict with the
    /// three above (`lib.rs`'s field table records the reason, P3-12).
    config: &'a ServiceContainerConfig,
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
        config: &'a ServiceContainerConfig,
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
            config,
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

        let lifecycle = context.lifecycle();

        sessions.get_mut(id.0).map(|session| ClientSession {
            session,
            client,
            cm_proxy,
            context,
            time: *time,
            role: *role,
            lifecycle,
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

    /// `forEachClientSession(Consumer)` (`Cluster.java:183`): every session as a
    /// view that can offer, one at a time.
    ///
    /// The write-side counterpart of [`Self::sessions`], and the reason that one
    /// can stay read-only: a fan-out walks the table directly instead of
    /// collecting ids first, so it costs no allocation and each view is the
    /// session it names.
    pub fn for_each_client_session<F>(&mut self, mut action: F)
    where
        F: FnMut(&mut ClientSession<'_>),
    {
        let Self {
            client,
            sessions,
            cm_proxy,
            context,
            time,
            role,
            ..
        } = self;
        let lifecycle = context.lifecycle();

        for session in sessions.iter_mut() {
            action(&mut ClientSession {
                session,
                client,
                cm_proxy,
                context,
                time: *time,
                role: *role,
                lifecycle,
            });
        }
    }

    /// `client()` (the reference's `aeron()`, `Cluster.java:151`): the client
    /// the container runs on.
    ///
    /// The reference returns the `Aeron` it owns and names the method after it;
    /// this build has no owner object — the client is the one the agent threads
    /// through every call — so the corresponding thing is a borrow of it, named
    /// after what it hands out.
    ///
    /// `&mut` because what a business does with it is `addPublication` /
    /// `addExclusivePublication` (`crates/client/src/client.rs:1499`, `:1525`),
    /// and both need it.
    ///
    /// A publication taken from here is addressed by **registration id** and
    /// must be looked up again with `client.publication(id)` after the callback
    /// returns: the handle borrows the client, and the client is borrowed for
    /// one callback at a time.
    ///
    /// The snapshot callback gets no `Cluster`; its counterpart is
    /// [`SnapshotWriter::client`](crate::service::SnapshotWriter::client).
    pub fn client(&mut self) -> &mut Client {
        self.client
    }

    /// `context()` (`Cluster.java:158`): the container's own configuration.
    ///
    /// The reference returns `ClusteredServiceContainer.Context`; this build's
    /// counterpart is [`ServiceContainerConfig`], which is the same facts and
    /// the only place they live. What the reference's Context has and this
    /// does not — `errorHandler`, `threadFactory`, `epochClock`,
    /// `ownsAeronClient`, `clusteredService` — is listed in `docs/compat.md`
    /// with why each has no counterpart.
    pub fn context(&self) -> &ServiceContainerConfig {
        self.config
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

        let Some(session) = self.sessions.get_mut(id.0) else {
            return Err(ContainerError::Cluster(format!(
                "unknown clusterSessionId: {}",
                id.0
            )));
        };
        if session.is_closing() {
            return Ok(true);
        }

        close_session(self.cm_proxy, self.client, self.context, session)
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

    /// The same service message, made of parts that must land as one message
    /// (`Cluster.offer(DirectBufferVector[])`, `Cluster.java:332`).
    ///
    /// The parts are joined by the term, not by a buffer of ours: the sticky
    /// header goes out as the leading slice and `parts` follow it. The
    /// reference does the same, by putting its header vector at the front of
    /// the caller's array (`CSA:382-397`).
    ///
    /// # Errors
    ///
    /// As [`Self::offer`].
    pub fn offer_vectored(&mut self, parts: &[&[u8]]) -> Result<i64, ContainerError> {
        self.check_for_valid_invocation()?;
        self.cm_proxy
            .offer_service_message_vectored(self.client, parts)
    }

    /// Claim a service message of `length` payload bytes
    /// (`Cluster.tryClaim(int, BufferClaim)`, `Cluster.java:379`).
    ///
    /// The sticky header is written by the container, so the caller writes only
    /// the payload — `length` bytes of it — into
    /// [`ClaimedMessage::payload_mut`], and [`ClaimedMessage::commit`] publishes
    /// the frame.
    ///
    /// # Errors
    ///
    /// [`ContainerError`] from the lifecycle gate, and the reference's three
    /// throwing `checkResult` states (not connected, closed, at maximum
    /// position). Back-pressure and mid-rotation are **not** errors: they come
    /// back as [`TryClaim::NotClaimed`], as they do in the reference.
    pub fn try_claim(&mut self, length: usize) -> Result<TryClaim<'_>, ContainerError> {
        self.check_for_valid_invocation()?;

        match self.cm_proxy.claim_service_message(self.client, length) {
            Ok(Claimed::Term(claim)) => {
                let position = claim.position();
                Ok(TryClaim::Claimed(ClaimedMessage::new(
                    Claimed::Term(claim),
                    length,
                    position,
                )))
            }
            Ok(Claimed::Mocked(_)) => unreachable!("a service message is never mocked"),
            // The reference's split (`ConsensusModuleProxy.java:227-235`): two
            // codes are the caller's to retry, three are thrown.
            Err(code) if crate::proxy::is_retryable_claim(code) => Ok(TryClaim::NotClaimed(code)),
            Err(code) => Err(crate::proxy::check_result_code(self.client, code)),
        }
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

/// `ClusteredServiceAgent.closeClientSession` (`CSA:323-351`): three send
/// attempts with `idle()` between them, and the session marked closing when one
/// lands.
///
/// Shared by [`Cluster::close_client_session`] and [`ClientSession::close`],
/// which the reference routes through the **same** method
/// (`ContainerClientSession.java:73-79`) — so there is one loop here, and one
/// place where it can be wrong.
fn close_session(
    cm_proxy: &mut ConsensusModuleProxy,
    client: &mut Client,
    context: &mut AgentContext,
    session: &mut ContainerClientSession,
) -> Result<bool, ContainerError> {
    let id = session.id();

    let mut attempts = 3;
    loop {
        if cm_proxy.close_session(client, id)? {
            session.mark_closing();
            return Ok(true);
        }
        context.idle(client, 0)?;
        attempts -= 1;
        if attempts == 0 {
            return Ok(false);
        }
    }
}

/// What `Cluster::try_claim` and `ClientSession::try_claim` answered.
///
/// The reference returns a `long` from `Publication.tryClaim` and puts three of
/// its values in `BufferClaim`; two of them are **not** errors and are handed
/// back rather than thrown (`ConsensusModuleProxy.java:227-235`).
pub enum TryClaim<'a> {
    /// A claimed message: write it through [`ClaimedMessage::payload_mut`], then
    /// [`ClaimedMessage::commit`].
    Claimed(ClaimedMessage<'a>),
    /// `Publication.BACK_PRESSURED` or `ADMIN_ACTION`: no window right now, or
    /// another producer is mid-rotation. The reference hands both back.
    NotClaimed(i64),
}

/// A message claimed in the term, or in a follower's scratch buffer.
///
/// The sticky header is already written; the caller writes the payload and
/// commits. **Nothing is published until `commit`**, which is the reference's
/// contract too — a claimed frame carries a negative length, so a reader stops
/// at it (`Claim::frame`, `aeron_exclusive_publication.c:41`).
pub struct ClaimedMessage<'a> {
    claimed: Claimed<'a>,
    payload_length: usize,
    position: i64,
}

impl<'a> ClaimedMessage<'a> {
    fn new(claimed: Claimed<'a>, payload_length: usize, position: i64) -> Self {
        Self {
            claimed,
            payload_length,
            position,
        }
    }

    /// The position the claim answered with: just past the frame, or
    /// [`MOCKED_OFFER`] for a follower's scratch claim.
    pub const fn position(&self) -> i64 {
        self.position
    }

    /// How many payload bytes the caller may write.
    pub const fn len(&self) -> usize {
        self.payload_length
    }

    /// Whether the caller wrote nothing (`len()` of zero).
    pub const fn is_empty(&self) -> bool {
        self.payload_length == 0
    }

    /// The payload window: write here, then commit.
    ///
    /// # Errors
    ///
    /// `None` if the claim is shorter than it says, which the proxy already
    /// ruled out; the option exists because the window is bounds-checked.
    pub fn payload_mut(&mut self) -> Option<&mut [u8]> {
        match &mut self.claimed {
            Claimed::Term(claim) => claim.payload_mut(SESSION_HEADER_LENGTH, self.payload_length),
            Claimed::Mocked(bytes) => Some(bytes),
        }
    }

    /// Publish the frame, and answer with the position.
    ///
    /// For a follower's mocked claim there is nothing to publish and the
    /// position is [`MOCKED_OFFER`] — the reference's `BufferClaim` wraps a
    /// scratch buffer there and no publication is touched (`CSA:713-731`).
    ///
    /// # Errors
    ///
    /// [`ContainerError::Cluster`] if the frame's length cannot be published,
    /// which would mean the claim was not inside its own term.
    pub fn commit(self) -> Result<i64, ContainerError> {
        let Self {
            claimed,
            payload_length,
            position,
        } = self;

        let Claimed::Term(claim) = claimed else {
            return Ok(position);
        };

        let total = SESSION_HEADER_LENGTH + payload_length + DATA_HEADER_LENGTH;
        let frame_length = i32::try_from(total).map_err(|_| {
            ContainerError::Cluster(format!("a claimed message of {total} bytes is not a frame"))
        })?;

        let Some(()) = claim.frame().publish(frame_length) else {
            return Err(ContainerError::Cluster(
                "the claimed frame is not inside its term".to_owned(),
            ));
        };

        Ok(position)
    }
}

impl core::fmt::Debug for TryClaim<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Claimed(message) => f
                .debug_struct("Claimed")
                .field("position", &message.position())
                .finish(),
            Self::NotClaimed(code) => f.debug_tuple("NotClaimed").field(code).finish(),
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
    session: &'a mut ContainerClientSession,
    client: &'a mut Client,
    cm_proxy: &'a mut ConsensusModuleProxy,
    context: &'a mut AgentContext,
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
    /// `close()` (`ClientSession.java:67`): the same close
    /// `Cluster::close_client_session` asks for, from a session that already
    /// holds itself.
    ///
    /// The reference delegates to `ClusteredServiceAgent.closeClientSession`
    /// (`ContainerClientSession.java:73-79`), which is the method
    /// `Cluster.closeClientSession` also calls — so the two are one path here
    /// too, three send attempts with `idle()` between them.
    ///
    /// # Errors
    ///
    /// As [`Cluster::close_client_session`], minus the unknown session: a view
    /// cannot exist for one.
    pub fn close(&mut self) -> Result<bool, ContainerError> {
        self.check_for_valid_invocation()?;

        if self.session.is_closing() {
            return Ok(true);
        }

        close_session(self.cm_proxy, self.client, self.context, self.session)
    }

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

    /// The same egress message, made of parts that must land as one message
    /// (`ClientSession.offer(DirectBufferVector[])`, `ClientSession.java:96`).
    ///
    /// Returns the reference's codes, as [`Self::offer`] does; the sticky
    /// header goes out as the leading slice.
    ///
    /// # Errors
    ///
    /// As [`Self::offer`].
    pub fn offer_vectored(&mut self, parts: &[&[u8]]) -> Result<i64, ContainerError> {
        self.check_for_valid_invocation()?;

        if self.role != Role::Leader {
            return Ok(MOCKED_OFFER);
        }
        let Some(publication_registration_id) = self.session.publication_registration_id() else {
            return Ok(OFFER_NOT_CONNECTED);
        };

        Ok(self.cm_proxy.offer_session_message_vectored(
            self.client,
            publication_registration_id,
            self.session.id(),
            self.time.time,
            parts,
        ))
    }

    /// Claim an egress message of `length` payload bytes
    /// (`ClientSession.tryClaim(int, BufferClaim)`, `ClientSession.java:133`).
    ///
    /// A **follower** gets a scratch claim instead of a log one — the caller
    /// writes it, commits it, and nothing is sent (`CSA:713-731`), with
    /// [`MOCKED_OFFER`] as the position. That check comes first, as it does in
    /// the reference: a follower has no egress publication to miss.
    ///
    /// # Errors
    ///
    /// As [`Cluster::try_claim`].
    pub fn try_claim(&mut self, length: usize) -> Result<TryClaim<'_>, ContainerError> {
        self.check_for_valid_invocation()?;

        if self.role != Role::Leader {
            return match self.cm_proxy.mock_session_message(
                self.session.id(),
                self.time.time,
                length,
            ) {
                Ok(Claimed::Mocked(bytes)) => Ok(TryClaim::Claimed(ClaimedMessage::new(
                    Claimed::Mocked(bytes),
                    length,
                    MOCKED_OFFER,
                ))),
                Ok(Claimed::Term(_)) => unreachable!("a follower's claim is never in the log"),
                Err(error) => Err(error),
            };
        }

        let Some(publication_registration_id) = self.session.publication_registration_id() else {
            return Err(crate::proxy::check_result_code(
                self.client,
                OFFER_NOT_CONNECTED,
            ));
        };

        match self.cm_proxy.claim_session_message(
            self.client,
            publication_registration_id,
            self.session.id(),
            self.time.time,
            length,
        ) {
            Ok(Claimed::Term(claim)) => {
                let position = claim.position();
                Ok(TryClaim::Claimed(ClaimedMessage::new(
                    Claimed::Term(claim),
                    length,
                    position,
                )))
            }
            Ok(Claimed::Mocked(_)) => unreachable!("a leader's claim is never mocked"),
            Err(code) if crate::proxy::is_retryable_claim(code) => Ok(TryClaim::NotClaimed(code)),
            Err(code) => Err(crate::proxy::check_result_code(self.client, code)),
        }
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
