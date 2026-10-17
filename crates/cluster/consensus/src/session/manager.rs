//! The session table: connect, authenticate, open, time out, close.
//!
//! Counterpart of `SessionManager.java`. Three lists are live at any moment —
//! pending user sessions, rejected sessions waiting to hear about it, and
//! sessions waiting to be redirected — plus the committed sessions, kept in id
//! order. The processing order inside [`SessionManager::process_pending_sessions`]
//! and [`SessionManager::check_sessions`] is the reference's and is part of the
//! behaviour: an authenticator that authenticates synchronously sees its
//! session opened in the same pass, and a session that timed out is closed
//! before its open event is retried.

use std::collections::VecDeque;

use deepmsg_client::client::Client;
use deepmsg_client::counter::Counter;
use deepmsg_codec_cluster::close_reason::CloseReason;
use deepmsg_codec_cluster::event_code::EventCode;

use crate::clock::{ClusterClock, MillisecondClusterClock};
use crate::config::{
    PROTOCOL_MAJOR_VERSION, PROTOCOL_SEMANTIC_VERSION, SESSION_INVALID_VERSION_MSG,
    SESSION_LIMIT_MSG, semantic_version_major,
};
use crate::error::ClusterError;
use crate::log_publisher::LogPublisher;
use crate::session::authenticator::{Authenticator, AuthorisationService};
use crate::session::cluster_session::{
    ClusterSession, ClusterSessionProxy, ClusterSessionState, NULL_VALUE, SessionEgress,
    session_info,
};
use crate::session::egress::EgressPublisher;
use crate::state::Role;

/// One connect request, as the ingress adapter read it
/// (`SessionManager.onSessionConnect`, `SessionManager.java:240-289`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionConnectRequest {
    /// The client's correlation id.
    pub correlation_id: i64,
    /// The stream the client wants its answers on.
    pub response_stream_id: i32,
    /// The protocol version the client named; absent in old messages.
    pub version: Option<i32>,
    /// The channel the client wants its answers on, already refined against
    /// `aeron.cluster.egress.channel`.
    pub response_channel: String,
    /// The credentials the client sent.
    pub encoded_credentials: Vec<u8>,
    /// The client's free-form description of itself.
    pub client_info: String,
    /// The ingress image's `sourceIdentity`.
    pub source_identity: String,
    /// The ingress image's session id.
    pub image_session_id: i32,
}

/// The collaborators a session pass needs, borrowed apart from the manager
/// itself (plan §4.5's field-splitting table).
pub struct SessionContext<'a> {
    /// The client: async adds, publications, counter writes.
    pub client: &'a mut Client,
    /// The log the open/close events go into.
    pub log_publisher: &'a mut LogPublisher,
    /// The egress publisher.
    pub egress: &'a mut EgressPublisher,
    /// The cluster clock.
    pub clock: &'a MillisecondClusterClock,
    /// Counter 213, the timed-out client count.
    pub timed_out_counter: Counter,
    /// The term the log records carry.
    pub leadership_term_id: i64,
    /// This member's id.
    pub member_id: i32,
    /// The leader's id (this member, for a cluster of one).
    pub leader_member_id: i32,
    /// `id=endpoint,...` for the active members (`ClusterMember.ingressEndpoints`).
    pub ingress_endpoints: &'a str,
    /// The cycle's nanosecond time.
    pub now_ns: i64,
}

/// The session table (`SessionManager.java:68-191`).
pub struct SessionManager {
    cluster_member_id: i32,
    cluster_id: i32,
    session_timeout_ns: i64,
    max_concurrent_sessions: usize,
    /// Committed sessions, id-sorted (`SessionManager.java:69`).
    sessions: Vec<ClusterSession>,
    /// User sessions that have not been opened yet (`:71`).
    pending_user_sessions: Vec<ClusterSession>,
    /// Sessions that have to hear they were rejected (`:72`).
    rejected_user_sessions: Vec<ClusterSession>,
    /// Sessions that have to hear who the leader is (`:73`).
    redirect_user_sessions: Vec<ClusterSession>,
    /// Sessions whose close event is not committed yet (`:78`).
    uncommitted_closed_sessions: VecDeque<ClusterSession>,
    /// `(logPosition, nextSessionId)` pairs (`:82`).
    uncommitted_next_committed_session_ids: VecDeque<(i64, i64)>,
    next_session_id: i64,
    next_committed_session_id: i64,
    authenticator: Box<dyn Authenticator>,
    authorisation_service: Box<dyn AuthorisationService>,
}

impl SessionManager {
    /// `new SessionManager(...)` (`SessionManager.java:113-191`).
    pub fn new(
        cluster_member_id: i32,
        cluster_id: i32,
        session_timeout_ns: i64,
        max_concurrent_sessions: usize,
        authenticator: Box<dyn Authenticator>,
        authorisation_service: Box<dyn AuthorisationService>,
    ) -> Self {
        Self {
            cluster_member_id,
            cluster_id,
            session_timeout_ns,
            max_concurrent_sessions,
            sessions: Vec::new(),
            pending_user_sessions: Vec::new(),
            rejected_user_sessions: Vec::new(),
            redirect_user_sessions: Vec::new(),
            uncommitted_closed_sessions: VecDeque::new(),
            uncommitted_next_committed_session_ids: VecDeque::new(),
            next_session_id: 1,
            next_committed_session_id: 1,
            authenticator,
            authorisation_service,
        }
    }

    /// The authorisation service (`SessionManager.java:208-211`).
    pub fn authorisation_service(&self) -> &dyn AuthorisationService {
        self.authorisation_service.as_ref()
    }

    /// The id the next session on this leader will get (`:213-216`).
    pub const fn next_committed_session_id(&self) -> i64 {
        self.next_committed_session_id
    }

    /// The committed sessions, id order.
    pub fn sessions(&self) -> &[ClusterSession] {
        &self.sessions
    }

    /// A committed session by id (`:193-196`).
    pub fn find_by_session_id(&self, cluster_session_id: i64) -> Option<&ClusterSession> {
        self.sessions
            .iter()
            .find(|session| session.id() == cluster_session_id)
    }

    /// A committed session by id, mutably.
    pub fn find_by_session_id_mut(
        &mut self,
        cluster_session_id: i64,
    ) -> Option<&mut ClusterSession> {
        self.sessions
            .iter_mut()
            .find(|session| session.id() == cluster_session_id)
    }

    /// The pending sessions, for tests and diagnostics.
    pub fn pending_sessions(&self) -> &[ClusterSession] {
        &self.pending_user_sessions
    }

    /// The pending sessions, mutably — what a snapshot load (P3-4d) and the
    /// authentication tests need.
    pub fn pending_sessions_mut(&mut self) -> &mut Vec<ClusterSession> {
        &mut self.pending_user_sessions
    }

    /// The rejected sessions still waiting to hear about it.
    pub fn rejected_sessions(&self) -> &[ClusterSession] {
        &self.rejected_user_sessions
    }

    /// The sessions waiting to be redirected.
    pub fn redirect_sessions(&self) -> &[ClusterSession] {
        &self.redirect_user_sessions
    }

    /// `onSessionConnect` (`SessionManager.java:240-289`).
    ///
    /// Returns `true` when the session was created and has to have its response
    /// publication added. The reference does that inside this call; this build
    /// does it right after the ingress poll, because the client is borrowed by
    /// the poll ([`SessionManager::drive_async_connects`]).
    ///
    /// # Errors
    ///
    /// Nothing today; the shape is here because the ingress path propagates with
    /// `?`.
    pub fn on_session_connect(
        &mut self,
        request: SessionConnectRequest,
        role: Role,
        ingress_endpoints: &str,
        now_ns: i64,
    ) -> bool {
        let cluster_session_id = if role == Role::Leader {
            let id = self.next_session_id;
            self.next_session_id += 1;
            id
        } else {
            NULL_VALUE
        };

        let mut session = ClusterSession::new(
            self.cluster_member_id,
            cluster_session_id,
            request.response_stream_id,
            &request.response_channel,
            session_info(
                &request.client_info,
                &request.source_identity,
                request.image_session_id,
            ),
        );
        session.last_activity_ns(now_ns, request.correlation_id);

        if role != Role::Leader {
            session.redirect(ingress_endpoints);
            self.redirect_user_sessions.push(session);
            return true;
        }

        // `SemanticVersion.major` of an absent optional is the null value, 0,
        // whose major is 0 — old clients pass.
        let version = request.version.unwrap_or(0);
        if PROTOCOL_MAJOR_VERSION != semantic_version_major(version) {
            let detail = format!(
                "{SESSION_INVALID_VERSION_MSG} {}, cluster is {}",
                semantic_version_string(version),
                semantic_version_string(PROTOCOL_SEMANTIC_VERSION)
            );
            session.reject(EventCode::ERROR, &detail);
            self.rejected_user_sessions.push(session);
        } else if self.pending_user_sessions.len() + self.sessions.len()
            >= self.max_concurrent_sessions
        {
            session.reject(EventCode::ERROR, SESSION_LIMIT_MSG);
            self.rejected_user_sessions.push(session);
        } else {
            session.link_ingress_image(request.image_session_id);
            self.authenticator.on_connect_request(
                session.id(),
                &request.encoded_credentials,
                now_ns / 1_000_000,
            );
            self.pending_user_sessions.push(session);
        }

        true
    }

    /// Add the response publication and counter for every session that has not
    /// got one yet (`ClusterSession.asyncConnect`, `ClusterSession.java:190-194`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] when either async add is refused by the command ring.
    pub fn drive_async_connects(&mut self, client: &mut Client) -> Result<(), ClusterError> {
        for session in self
            .pending_user_sessions
            .iter_mut()
            .chain(self.rejected_user_sessions.iter_mut())
            .chain(self.redirect_user_sessions.iter_mut())
        {
            if session.response_publication_id() == NULL_VALUE {
                session.async_connect(client, self.cluster_id)?;
            }
        }
        Ok(())
    }

    /// `onSessionClose` (`SessionManager.java:291-309`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the close append.
    pub fn on_session_close(
        &mut self,
        leadership_term_id: i64,
        cluster_session_id: i64,
        ctx: &mut SessionContext<'_>,
    ) -> Result<(), ClusterError> {
        let Some(index) = self
            .sessions
            .iter()
            .position(|session| session.id() == cluster_session_id)
        else {
            return Ok(());
        };
        if !self.sessions[index].is_open() {
            return Ok(());
        }

        self.sessions[index].closing(CloseReason::CLIENT_ACTION);
        self.sessions[index].disconnect(ctx.client);

        let timestamp = ctx.clock.time();
        let session = &mut self.sessions[index];
        if ctx.log_publisher.append_session_close(
            &*ctx.client,
            ctx.member_id,
            session,
            leadership_term_id,
            timestamp,
        )? {
            session.set_closed_log_position(ctx.log_publisher.position());
            let closed = self.sessions.remove(index);
            self.uncommitted_closed_sessions.push_back(closed);
        }
        Ok(())
    }

    /// `onChallengeResponseForUserSession` (`SessionManager.java:416-442`).
    pub fn on_challenge_response_for_user_session(
        &mut self,
        correlation_id: i64,
        cluster_session_id: i64,
        encoded_credentials: &[u8],
        now_ns: i64,
    ) {
        let now_ms = now_ns / 1_000_000;
        if let Some(session) = self.pending_user_sessions.iter_mut().rev().find(|session| {
            session.id() == cluster_session_id && session.state() == ClusterSessionState::Challenged
        }) {
            session.last_activity_ns(now_ns, correlation_id);
            self.authenticator.on_challenge_response(
                cluster_session_id,
                encoded_credentials,
                now_ms,
            );
        }
    }

    /// `processAllPendingSessions` (`SessionManager.java:517-538`): user
    /// sessions, then backup sessions (which this slice never creates).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from authentication or from the log append.
    pub fn process_all_pending_sessions(
        &mut self,
        ctx: &mut SessionContext<'_>,
    ) -> Result<usize, ClusterError> {
        self.process_pending_sessions(ctx)
    }

    /// `processPendingSessions` (`SessionManager.java:583-761`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the authenticator (a principal over the limit), the
    /// log append or the egress send. The reference throws out of the same
    /// places, and a swallowed error would let the cycle keep working.
    pub fn process_pending_sessions(
        &mut self,
        ctx: &mut SessionContext<'_>,
    ) -> Result<usize, ClusterError> {
        let mut work_count = 0;
        let mut index = self.pending_user_sessions.len();
        while index > 0 {
            index -= 1;
            let mut session = self.pending_user_sessions.swap_remove(index);

            if session.state() == ClusterSessionState::Invalid {
                session.close(ctx.client, "invalid session");
                continue;
            }

            if ctx.now_ns > session.time_of_last_activity_ns() + self.session_timeout_ns
                && session.state() != ClusterSessionState::Init
            {
                session.close(ctx.client, "session timed out");
                increment_counter(ctx.client, ctx.timed_out_counter);
                continue;
            }

            if matches!(
                session.state(),
                ClusterSessionState::Init
                    | ClusterSessionState::Connecting
                    | ClusterSessionState::Connected
            ) && session.is_response_publication_connected(ctx.client, ctx.now_ns)?
            {
                session.set_state(ClusterSessionState::Connected, "connected");
                let now_ms = ctx.clock.time_millis();
                let mut proxy = ClusterSessionProxy::new(&mut session, ctx.egress, &*ctx.client);
                self.authenticator
                    .on_connected_session(&mut proxy, now_ms)?;
            }

            if session.state() == ClusterSessionState::Challenged
                && session.is_response_publication_connected(ctx.client, ctx.now_ns)?
            {
                let now_ms = ctx.clock.time_millis();
                let mut proxy = ClusterSessionProxy::new(&mut session, ctx.egress, &*ctx.client);
                self.authenticator
                    .on_challenged_session(&mut proxy, now_ms)?;
            }

            if session.state() == ClusterSessionState::Authenticated {
                match session.action() {
                    crate::session::cluster_session::ClusterSessionAction::Client => {
                        let timestamp = ctx.clock.time();
                        if session.append_session_to_log_and_send_open(
                            ctx.client,
                            ctx.log_publisher,
                            ctx.egress,
                            ctx.leadership_term_id,
                            ctx.member_id,
                            ctx.now_ns,
                            timestamp,
                        )? {
                            self.uncommitted_next_committed_session_ids
                                .push_back((ctx.log_publisher.position(), session.id() + 1));
                            self.add_session(session);
                            work_count += 1;
                        } else {
                            self.pending_user_sessions.push(session);
                        }
                    }
                    other => {
                        return Err(ClusterError::Cluster(format!(
                            "a {} session needs the backup path (P3-9)",
                            other.name()
                        )));
                    }
                }
            } else if session.state() == ClusterSessionState::Rejected {
                self.rejected_user_sessions.push(session);
            } else {
                self.pending_user_sessions.push(session);
            }
        }
        Ok(work_count)
    }

    /// `checkSessions` (`SessionManager.java:841-930`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the close append or an egress send.
    pub fn check_sessions(&mut self, ctx: &mut SessionContext<'_>) -> Result<usize, ClusterError> {
        let mut work_count = 0;
        let mut index = self.sessions.len();
        while index > 0 {
            index -= 1;
            let timed_out = ctx.now_ns
                > self.sessions[index].time_of_last_activity_ns() + self.session_timeout_ns;
            if timed_out {
                match self.sessions[index].state() {
                    ClusterSessionState::Open => {
                        self.sessions[index].closing(CloseReason::TIMEOUT);
                        let timestamp = ctx.clock.time();
                        let session = &mut self.sessions[index];
                        if ctx.log_publisher.append_session_close(
                            &*ctx.client,
                            ctx.member_id,
                            session,
                            ctx.leadership_term_id,
                            timestamp,
                        )? {
                            let detail = close_reason_name(CloseReason::TIMEOUT);
                            egress_session_event(ctx, session, EventCode::CLOSED, &detail)?;
                            session.set_closed_log_position(ctx.log_publisher.position());
                            increment_counter(ctx.client, ctx.timed_out_counter);
                            let closed = self.sessions.remove(index);
                            self.uncommitted_closed_sessions.push_back(closed);
                        }
                        work_count += 1;
                    }
                    ClusterSessionState::Closing => {
                        let timestamp = ctx.clock.time();
                        let session = &mut self.sessions[index];
                        if ctx.log_publisher.append_session_close(
                            &*ctx.client,
                            ctx.member_id,
                            session,
                            ctx.leadership_term_id,
                            timestamp,
                        )? {
                            let detail = close_reason_name(session.close_reason());
                            egress_session_event(ctx, session, EventCode::CLOSED, &detail)?;
                            session.set_closed_log_position(ctx.log_publisher.position());
                            if session.close_reason() == CloseReason::TIMEOUT {
                                increment_counter(ctx.client, ctx.timed_out_counter);
                            }
                            let closed = self.sessions.remove(index);
                            self.uncommitted_closed_sessions.push_back(closed);
                            work_count += 1;
                        }
                    }
                    _ => {
                        self.sessions.remove(index);
                        work_count += 1;
                    }
                }
            } else if self.sessions[index].has_open_event_pending() {
                let session = &mut self.sessions[index];
                if egress_session_event(ctx, session, EventCode::OK, "")? {
                    session.clear_open_event_pending();
                    work_count += 1;
                }
            } else if self.sessions[index].has_new_leader_event_pending() {
                let leadership_term_id = ctx.leadership_term_id;
                let leader_member_id = ctx.leader_member_id;
                let ingress_endpoints = ctx.ingress_endpoints;
                let session = &mut self.sessions[index];
                let sent = ctx.egress.new_leader(
                    &SessionEgress {
                        client: ctx.client,
                        session,
                    },
                    leadership_term_id,
                    leader_member_id,
                    ingress_endpoints,
                )?;
                if sent {
                    session.set_new_leader_event_pending(false);
                    work_count += 1;
                }
            }
        }
        Ok(work_count)
    }

    /// `sendRedirects` (`SessionManager.java:969-975`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from an egress send.
    pub fn send_redirects(&mut self, ctx: &mut SessionContext<'_>) -> Result<usize, ClusterError> {
        let mut redirects = std::mem::take(&mut self.redirect_user_sessions);
        let work = self.send_for_all(&mut redirects, ctx)?;
        self.redirect_user_sessions = redirects;
        Ok(work)
    }

    /// `sendRejections` (`SessionManager.java:977-988`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from an egress send.
    pub fn send_rejections(&mut self, ctx: &mut SessionContext<'_>) -> Result<usize, ClusterError> {
        let mut rejected = std::mem::take(&mut self.rejected_user_sessions);
        let work = self.send_for_all(&mut rejected, ctx)?;
        self.rejected_user_sessions = rejected;
        Ok(work)
    }

    /// `sendForAll` (`SessionManager.java:990-1026`).
    fn send_for_all(
        &mut self,
        sessions: &mut Vec<ClusterSession>,
        ctx: &mut SessionContext<'_>,
    ) -> Result<usize, ClusterError> {
        let mut work_count = 0;
        let mut index = sessions.len();
        while index > 0 {
            index -= 1;
            let mut session = sessions.swap_remove(index);
            let event_code = session.event_code().unwrap_or(EventCode::ERROR);
            let detail = session.response_detail().unwrap_or_default().to_owned();

            if session.is_response_publication_connected(ctx.client, ctx.now_ns)? {
                let sent = ctx.egress.send_event(
                    &SessionEgress {
                        client: ctx.client,
                        session: &session,
                    },
                    ctx.leadership_term_id,
                    ctx.leader_member_id,
                    event_code,
                    &detail,
                )?;
                if sent {
                    session.close(ctx.client, &format!("{event_code:?}"));
                    work_count += 1;
                    continue;
                }
            }

            if session.has_timed_out(ctx.now_ns, self.session_timeout_ns) {
                session.close(ctx.client, "session timed out");
                increment_counter(ctx.client, ctx.timed_out_counter);
                work_count += 1;
            } else if session.state() == ClusterSessionState::Invalid {
                session.close(ctx.client, "invalid");
                work_count += 1;
            } else {
                sessions.push(session);
            }
        }
        Ok(work_count)
    }

    /// `prepareSessionsForNewTerm` (`SessionManager.java:932-967`).
    pub fn prepare_sessions_for_new_term(&mut self, is_startup: bool, now_ns: i64) {
        if is_startup {
            for session in self.sessions.iter_mut() {
                if session.is_open() {
                    session.closing(CloseReason::TIMEOUT);
                }
            }
        } else {
            for session in self.sessions.iter_mut() {
                if session.is_open() {
                    session.set_time_of_last_activity_ns(now_ns);
                    session.set_new_leader_event_pending(true);
                }
            }
        }
    }

    /// `clearSessionsAfter` (`SessionManager.java:1028-1047`).
    ///
    /// The egress sends are the reference's; a session whose publication is gone
    /// simply does not hear about the election, as in the reference.
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from an egress send.
    pub fn clear_sessions_after(
        &mut self,
        log_position: i64,
        ctx: &mut SessionContext<'_>,
    ) -> Result<(), ClusterError> {
        let mut index = self.sessions.len();
        while index > 0 {
            index -= 1;
            if self.sessions[index].opened_log_position() > log_position {
                let session = &mut self.sessions[index];
                let _ = ctx.egress.send_event(
                    &SessionEgress {
                        client: ctx.client,
                        session,
                    },
                    ctx.leadership_term_id,
                    ctx.member_id,
                    EventCode::CLOSED,
                    "election",
                );
                self.sessions.remove(index);
            }
        }

        for mut session in self.pending_user_sessions.drain(..) {
            let _ = ctx.egress.send_event(
                &SessionEgress {
                    client: ctx.client,
                    session: &session,
                },
                ctx.leadership_term_id,
                ctx.member_id,
                EventCode::CLOSED,
                "election",
            );
            session.close(ctx.client, "election");
        }
        Ok(())
    }

    /// `closeSessions` (`SessionManager.java:1049-1055`).
    pub fn close_sessions(&mut self, client: &mut Client) {
        for session in self.sessions.iter_mut() {
            session.close(client, "Cluster node terminated");
        }
        self.sessions.clear();
    }

    /// `sweepUncommittedSessions` (`SessionManager.java:1057-1075`).
    ///
    /// # Errors
    ///
    /// Nothing; the shape matches the caller's `?`.
    pub fn sweep_uncommitted_sessions(&mut self, commit_position: i64) -> Result<(), ClusterError> {
        while let Some(session) = self.uncommitted_closed_sessions.front() {
            if session.closed_log_position() > commit_position {
                break;
            }
            self.uncommitted_closed_sessions.pop_front();
        }

        while let Some(&(position, next_session_id)) =
            self.uncommitted_next_committed_session_ids.front()
        {
            if position > commit_position {
                break;
            }
            self.uncommitted_next_committed_session_ids.pop_front();
            self.next_committed_session_id = self.next_committed_session_id.max(next_session_id);
        }
        Ok(())
    }

    /// `restoreUncommittedSessions` (`SessionManager.java:1077-1096`).
    pub fn restore_uncommitted_sessions(&mut self, commit_position: i64) {
        while let Some(mut session) = self.uncommitted_closed_sessions.pop_front() {
            if session.closed_log_position() > commit_position {
                session.reset_close_reason();
                session.set_state(ClusterSessionState::Closing, "uncommitted session");
                self.add_session(session);
            }
        }

        while self
            .uncommitted_next_committed_session_ids
            .pop_front()
            .is_some()
        {}
    }

    /// `snapshotSessions` (`SessionManager.java:1107-1119`).
    ///
    /// **The container is `sessions`, the id-sorted `Vec`** — not a map — and
    /// only `OPEN` and `CLOSING` sessions are written. The taker writes each
    /// session's activity stamp as `Aeron.NULL_VALUE`; see
    /// [`crate::snapshot::taker`].
    ///
    /// # Errors
    ///
    /// [`ClusterError`] when the publication refuses a record.
    pub fn snapshot_sessions(
        &self,
        taker: &mut crate::snapshot::ConsensusModuleSnapshotTaker<'_>,
    ) -> Result<(), ClusterError> {
        for session in self.sessions.iter() {
            if matches!(
                session.state(),
                ClusterSessionState::Open | ClusterSessionState::Closing
            ) {
                taker.snapshot_session(session)?;
            }
        }
        Ok(())
    }

    /// `onLoadClusterSession` (`SessionManager.java:540-562`).
    ///
    /// `session_info` is not serialised, so the response channel stands in for
    /// it, as in the reference.
    ///
    /// # Errors
    ///
    /// Nothing today; the shape matches the loader's `?`.
    #[allow(clippy::too_many_arguments)]
    pub fn on_load_cluster_session(
        &mut self,
        cluster_session_id: i64,
        correlation_id: i64,
        opened_log_position: i64,
        time_of_last_activity: i64,
        close_reason: CloseReason,
        response_stream_id: i32,
        response_channel: &str,
    ) -> Result<(), ClusterError> {
        let mut session = ClusterSession::new(
            self.cluster_member_id,
            cluster_session_id,
            response_stream_id,
            response_channel,
            response_channel.to_owned(),
        );
        session.load_snapshot_state(
            correlation_id,
            opened_log_position,
            time_of_last_activity,
            close_reason,
        );
        self.add_session(session);

        if cluster_session_id >= self.next_session_id {
            self.next_session_id = cluster_session_id + 1;
            self.next_committed_session_id = self.next_session_id;
        }
        Ok(())
    }

    /// `onReplaySessionOpen` (`SessionManager.java:478-505`): a session the
    /// replayed log says was opened.
    ///
    /// # Errors
    ///
    /// Nothing today; the shape matches the replay handler's `?`.
    #[allow(clippy::too_many_arguments)]
    pub fn on_replay_session_open(
        &mut self,
        log_position: i64,
        correlation_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        response_stream_id: i32,
        response_channel: &str,
    ) -> Result<(), ClusterError> {
        let mut session = ClusterSession::new(
            self.cluster_member_id,
            cluster_session_id,
            response_stream_id,
            response_channel,
            response_channel.to_owned(),
        );
        session.load_snapshot_state(
            correlation_id,
            log_position,
            timestamp.saturating_mul(1_000_000),
            CloseReason::NullVal,
        );
        self.add_session(session);

        if cluster_session_id >= self.next_session_id {
            self.next_session_id = cluster_session_id + 1;
            self.next_committed_session_id = self.next_session_id;
        }
        Ok(())
    }

    /// `onReplaySessionClose` (`SessionManager.java:507-515`).
    pub fn on_replay_session_close(&mut self, cluster_session_id: i64, close_reason: CloseReason) {
        if let Some(index) = self
            .sessions
            .iter()
            .position(|session| session.id() == cluster_session_id)
        {
            self.sessions[index].closing(close_reason);
            self.sessions.remove(index);
        }
    }

    /// `loadNextSessionId` (`SessionManager.java:563-567`).
    pub fn load_next_session_id(&mut self, next_session_id: i64) {
        self.next_session_id = next_session_id;
        self.next_committed_session_id = next_session_id;
    }

    /// `updateTimeOfLastActivity` (`SessionManager.java:1098-1105`).
    pub fn update_time_of_last_activity(&mut self, now_ns: i64) {
        for session in self.sessions.iter_mut() {
            session.set_time_of_last_activity_ns(now_ns);
        }
    }

    /// `addSession` (`SessionManager.java:1134-1157`): insert in id order.
    fn add_session(&mut self, session: ClusterSession) {
        let position = self
            .sessions
            .iter()
            .rposition(|existing| existing.id() < session.id())
            .map_or(0, |index| index + 1);
        self.sessions.insert(position, session);
    }
}

/// Emit one `SessionEvent` for a committed session.
fn egress_session_event(
    ctx: &mut SessionContext<'_>,
    session: &ClusterSession,
    event_code: EventCode,
    detail: &str,
) -> Result<bool, ClusterError> {
    ctx.egress.send_event(
        &SessionEgress {
            client: ctx.client,
            session,
        },
        ctx.leadership_term_id,
        ctx.member_id,
        event_code,
        detail,
    )
}

/// `CloseReason.name()` as the reference writes it into a detail string.
fn close_reason_name(reason: CloseReason) -> String {
    match reason {
        CloseReason::CLIENT_ACTION => "CLIENT_ACTION".to_owned(),
        CloseReason::SERVICE_ACTION => "SERVICE_ACTION".to_owned(),
        CloseReason::TIMEOUT => "TIMEOUT".to_owned(),
        CloseReason::NullVal => "NULL_VAL".to_owned(),
    }
}

/// `SemanticVersion.toString`.
fn semantic_version_string(version: i32) -> String {
    format!(
        "{}.{}.{}",
        (version >> 16) & 0xFFFF,
        (version >> 8) & 0xFF,
        version & 0xFF
    )
}

/// `counter.incrementRelease()`.
fn increment_counter(client: &Client, counter: Counter) {
    if let Some(counters) = client.counters_writable() {
        let value = counters.value(counter.counter_id()).unwrap_or(0);
        counters.set_value(counter.counter_id(), value + 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_semantic_version_prints_as_major_dot_minor_dot_patch() {
        assert_eq!("0.3.0", semantic_version_string(PROTOCOL_SEMANTIC_VERSION));
        assert_eq!("0.0.0", semantic_version_string(0));
    }

    #[test]
    fn close_reason_names_are_the_references() {
        assert_eq!(
            "CLIENT_ACTION",
            close_reason_name(CloseReason::CLIENT_ACTION)
        );
        assert_eq!("TIMEOUT", close_reason_name(CloseReason::TIMEOUT));
    }
}
