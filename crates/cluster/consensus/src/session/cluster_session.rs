//! One client session, from the connect request to the close event.
//!
//! Counterpart of `ClusterSession.java` and `ClusterSessionProxy.java`. The
//! state machine has ten states and four actions, and both are the reference's
//! values, in the reference's order — they are logged and, through the session
//! counter, visible outside this process.

use std::time::Duration;

use deepmsg_client::client::{AsyncAdd, AsyncAddPoll, Client, CommandError};
use deepmsg_client::publication::Publication;
use deepmsg_cluster_common::counters::{self, CLUSTER_SESSION_TYPE_ID};
use deepmsg_cnc::command::ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE;
use deepmsg_codec_cluster::close_reason::CloseReason;
use deepmsg_codec_cluster::event_code::EventCode;

use crate::error::ClusterError;
use crate::session::authenticator::{
    AuthorisationService, SessionProxy, check_encoded_principal_length,
};
use crate::session::egress::EgressPublisher;

/// `Aeron.NULL_VALUE`.
pub const NULL_VALUE: i64 = -1;

/// `AeronArchive.NULL_POSITION`.
pub const NULL_POSITION: i64 = -1;

/// How long a synchronous client command waits; the same bound the agent uses.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// `ClusterSession.State` (`ClusterSession.java:53-56`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum ClusterSessionState {
    /// A connect request has been read and nothing has been said about it.
    #[default]
    Init,
    /// The response publication is being added.
    Connecting,
    /// The response publication is connected.
    Connected,
    /// A challenge went out; the credentials are on their way back.
    Challenged,
    /// The credentials passed; the open event has not been appended yet.
    Authenticated,
    /// The session will not be opened.
    Rejected,
    /// The session open is in the log.
    Open,
    /// A close is in progress.
    Closing,
    /// The response publication could not be added.
    Invalid,
    /// The session is gone.
    Closed,
}

impl ClusterSessionState {
    /// The reference's `name()` in the state log line.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Init => "INIT",
            Self::Connecting => "CONNECTING",
            Self::Connected => "CONNECTED",
            Self::Challenged => "CHALLENGED",
            Self::Authenticated => "AUTHENTICATED",
            Self::Rejected => "REJECTED",
            Self::Open => "OPEN",
            Self::Closing => "CLOSING",
            Self::Invalid => "INVALID",
            Self::Closed => "CLOSED",
        }
    }
}

/// `ClusterSession.Action` (`ClusterSession.java:59-62`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum ClusterSessionAction {
    /// An ordinary client.
    #[default]
    Client,
    /// A backup query.
    Backup,
    /// A heartbeat request.
    Heartbeat,
    /// A standby snapshot notification (premium; never produced here).
    StandbySnapshot,
}

impl ClusterSessionAction {
    /// The reference's `name()`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Client => "CLIENT",
            Self::Backup => "BACKUP",
            Self::Heartbeat => "HEARTBEAT",
            Self::StandbySnapshot => "STANDBY_SNAPSHOT",
        }
    }
}

/// `SessionManager.sessionInfo(clientInfo, header)` (`SessionManager.java:1174-1179`):
/// the client's own words plus the image it arrived on, which is what the
/// session counter's label is built from.
pub fn session_info(client_info: &str, source_identity: &str, image_session_id: i32) -> String {
    let tail = format!("sourceIdentity={source_identity} sessionId={image_session_id}");
    if client_info.is_empty() {
        tail
    } else {
        format!("{client_info} {tail}")
    }
}

/// One client session (`ClusterSession.java`).
#[derive(Clone, Debug)]
pub struct ClusterSession {
    id: i64,
    cluster_member_id: i32,
    response_stream_id: i32,
    response_channel: String,
    session_info: String,
    action: ClusterSessionAction,
    state: ClusterSessionState,
    correlation_id: i64,
    opened_log_position: i64,
    closed_log_position: i64,
    time_of_last_activity_ns: i64,
    /// The ingress image the session's messages arrive on, by the image's
    /// session id (`ingressImageCorrelationId` in the reference, which keeps the
    /// correlation id; this build's client does not expose one, so the image's
    /// session id is what [`super::ingress`] can match on).
    ingress_image_session_id: i32,
    /// The registration id of the async add of the response publication.
    response_publication_id: i64,
    /// The registration id of the async add of the session counter.
    counter_registration_id: i64,
    /// The adopted counter's slot, once the driver has answered.
    session_counter_id: Option<i32>,
    has_new_leader_event_pending: bool,
    has_open_event_pending: bool,
    response_detail: Option<String>,
    event_code: Option<EventCode>,
    close_reason: CloseReason,
    encoded_principal: Vec<u8>,
    /// What the session's own request carried besides its identity
    /// (`ClusterSession.requestInput`): for a `BACKUP` session, the log position
    /// the query asked about (`SessionManager.java:666-668`). `None` is the
    /// reference's `null`.
    request_input: Option<i64>,
}

impl ClusterSession {
    /// `new ClusterSession(...)` (`ClusterSession.java:88-101`).
    pub fn new(
        cluster_member_id: i32,
        id: i64,
        response_stream_id: i32,
        response_channel: &str,
        session_info: String,
    ) -> Self {
        Self {
            id,
            cluster_member_id,
            response_stream_id,
            response_channel: response_channel.to_owned(),
            session_info,
            action: ClusterSessionAction::Client,
            state: ClusterSessionState::Init,
            correlation_id: 0,
            opened_log_position: NULL_POSITION,
            closed_log_position: NULL_POSITION,
            time_of_last_activity_ns: 0,
            ingress_image_session_id: -1,
            response_publication_id: NULL_VALUE,
            counter_registration_id: NULL_VALUE,
            session_counter_id: None,
            has_new_leader_event_pending: false,
            has_open_event_pending: true,
            response_detail: None,
            event_code: None,
            close_reason: CloseReason::NullVal,
            encoded_principal: Vec::new(),
            request_input: None,
        }
    }

    /// The session's id, as the log and the client know it.
    pub const fn id(&self) -> i64 {
        self.id
    }

    /// `requestInput()` (`ClusterSession.java:329-332`).
    pub const fn request_input(&self) -> Option<i64> {
        self.request_input
    }

    /// `requestInput(Object)` (`:325-328`).
    pub fn set_request_input(&mut self, request_input: Option<i64>) {
        self.request_input = request_input;
    }

    /// The member this session is being served by.
    pub const fn cluster_member_id(&self) -> i32 {
        self.cluster_member_id
    }

    /// The stream the response publication is on.
    pub const fn response_stream_id(&self) -> i32 {
        self.response_stream_id
    }

    /// The channel the response publication is on.
    pub fn response_channel(&self) -> &str {
        &self.response_channel
    }

    /// The diagnostic string the session counter's label carries.
    pub fn session_info(&self) -> &str {
        &self.session_info
    }

    /// The state.
    pub const fn state(&self) -> ClusterSessionState {
        self.state
    }

    /// Move to a state; the reference logs the transition
    /// (`ClusterSession.java:326-330`), which is a diagnostic and not a wire
    /// contract.
    pub fn set_state(&mut self, state: ClusterSessionState, _reason: &str) {
        self.state = state;
    }

    /// The action this session performs (`ClusterSession.java:459-467`).
    pub const fn action(&self) -> ClusterSessionAction {
        self.action
    }

    /// Set the action.
    pub fn set_action(&mut self, action: ClusterSessionAction) {
        self.action = action;
    }

    /// Whether the session is open (`ClusterSession.java:119-122`).
    pub const fn is_open(&self) -> bool {
        matches!(self.state, ClusterSessionState::Open)
    }

    /// The correlation id the client's connect request carried.
    pub const fn correlation_id(&self) -> i64 {
        self.correlation_id
    }

    /// The log position the open event was appended at.
    pub const fn opened_log_position(&self) -> i64 {
        self.opened_log_position
    }

    /// The log position the close event was appended at.
    pub const fn closed_log_position(&self) -> i64 {
        self.closed_log_position
    }

    /// Record where the close event was appended.
    pub fn set_closed_log_position(&mut self, position: i64) {
        self.closed_log_position = position;
    }

    /// Forget a close position the commit position has not reached.
    pub fn reset_closed_log_position(&mut self) {
        self.closed_log_position = NULL_POSITION;
    }

    /// The last time the session did anything, in nanoseconds.
    pub const fn time_of_last_activity_ns(&self) -> i64 {
        self.time_of_last_activity_ns
    }

    /// Set the last-activity stamp.
    pub fn set_time_of_last_activity_ns(&mut self, now_ns: i64) {
        self.time_of_last_activity_ns = now_ns;
    }

    /// Record activity and the correlation id that carried it
    /// (`ClusterSession.java:385-389`).
    pub fn last_activity_ns(&mut self, now_ns: i64, correlation_id: i64) {
        self.time_of_last_activity_ns = now_ns;
        self.correlation_id = correlation_id;
    }

    /// Whether the session has timed out (`ClusterSession.java:497-500`).
    pub fn has_timed_out(&self, now_ns: i64, session_timeout_ns: i64) -> bool {
        !matches!(self.state, ClusterSessionState::Init)
            && self.time_of_last_activity_ns + session_timeout_ns < now_ns
    }

    /// The ingress image this session arrived on.
    pub const fn ingress_image_session_id(&self) -> i32 {
        self.ingress_image_session_id
    }

    /// Link the ingress image, first one wins (`ClusterSession.java:479-485`).
    pub fn link_ingress_image(&mut self, image_session_id: i32) {
        if self.ingress_image_session_id == -1 {
            self.ingress_image_session_id = image_session_id;
        }
    }

    /// Forget the ingress image, so the next term links its own
    /// (`ClusterSession.unlinkIngressImage`, `ClusterSession.java:487-490`).
    ///
    /// The reference drops a reference to the image object; this build holds the
    /// image's session id, which is the same question asked of a different
    /// value — the next term's ingress image is a different session.
    pub fn unlink_ingress_image(&mut self) {
        self.ingress_image_session_id = -1;
    }

    /// The principal the authenticator accepted.
    pub fn encoded_principal(&self) -> &[u8] {
        &self.encoded_principal
    }

    /// The event code a rejected or redirected session carries.
    pub const fn event_code(&self) -> Option<EventCode> {
        self.event_code
    }

    /// The detail string a rejected or redirected session carries.
    pub fn response_detail(&self) -> Option<&str> {
        self.response_detail.as_deref()
    }

    /// The reason a closing session was closed (`ClusterSession.java:170-177`).
    pub fn closing(&mut self, reason: CloseReason) {
        self.close_reason = reason;
        self.has_new_leader_event_pending = false;
        self.has_open_event_pending = false;
        self.time_of_last_activity_ns = 0;
        self.set_state(ClusterSessionState::Closing, "");
    }

    /// The close reason (`ClusterSession.java:179-182`).
    pub const fn close_reason(&self) -> CloseReason {
        self.close_reason
    }

    /// Clear a stale close reason on a session being restored.
    pub fn reset_close_reason(&mut self) {
        self.closed_log_position = NULL_POSITION;
        self.close_reason = CloseReason::NullVal;
    }

    /// The authenticator's `authenticate` (`ClusterSession.java:332-340`).
    ///
    /// # Errors
    ///
    /// [`ClusterError::Cluster`] when the principal is longer than
    /// `MAX_ENCODED_PRINCIPAL_LENGTH`; the reference throws the same
    /// `ClusterException` out of `checkEncodedPrincipalLength`.
    pub fn authenticate(&mut self, encoded_principal: &[u8]) -> Result<(), ClusterError> {
        check_encoded_principal_length(encoded_principal.len())?;
        self.encoded_principal = encoded_principal.to_vec();
        self.set_state(ClusterSessionState::Authenticated, "authenticated");
        Ok(())
    }

    /// The session open is in the log (`ClusterSession.java:342-346`).
    pub fn open(&mut self, opened_log_position: i64) {
        self.opened_log_position = opened_log_position;
        self.set_state(ClusterSessionState::Open, "");
    }

    /// Reject the session (`ClusterSession.java:391-401`).
    pub fn reject(&mut self, event_code: EventCode, detail: &str) {
        self.event_code = Some(event_code);
        self.response_detail = (!detail.is_empty()).then(|| detail.to_owned());
        let reason = if detail.is_empty() {
            format!("{event_code:?}")
        } else {
            format!("{event_code:?}: {detail}")
        };
        self.set_state(ClusterSessionState::Rejected, &reason);
    }

    /// Redirect the session to the leader (`ClusterSession.java:403-407`): the
    /// state does not change until the redirect has been sent.
    pub fn redirect(&mut self, ingress_endpoints: &str) {
        self.event_code = Some(EventCode::REDIRECT);
        self.response_detail = Some(ingress_endpoints.to_owned());
    }

    /// Close the session (`ClusterSession.java:103-107`).
    pub fn close(&mut self, client: &mut Client, reason: &str) {
        self.disconnect(client);
        self.set_state(ClusterSessionState::Closed, reason);
    }

    /// Give the response publication and the session counter back
    /// (`ClusterSession.java:220-242`).
    ///
    /// The reference sends an async removal for the publication and a removal
    /// for the counter, and reports a failure through its error handler. Here
    /// the publication's removal is async and the counter's waits for its
    /// answer — the reference's `asyncRemoveCounter` has no counterpart in this
    /// client — and a removal the driver refuses is **not** control flow: the
    /// session is going away either way, and the client closes both with
    /// itself, so the only thing a `?` could do is abort a teardown.
    pub fn disconnect(&mut self, client: &mut Client) {
        if self.response_publication_id != NULL_VALUE {
            let _ = client.async_remove_publication(self.response_publication_id, COMMAND_TIMEOUT);
        }
        if self.counter_registration_id != NULL_VALUE {
            let counter = client.counter(self.counter_registration_id).copied();
            if let Some(counter) = counter {
                let _ = client.remove_counter(&counter, COMMAND_TIMEOUT);
            }
        }
        self.response_publication_id = NULL_VALUE;
        self.counter_registration_id = NULL_VALUE;
        self.session_counter_id = None;
    }

    /// The response publication's registration id.
    pub const fn response_publication_id(&self) -> i64 {
        self.response_publication_id
    }

    /// Whether the open event still has to reach the client
    /// (`ClusterSession.java:449-457`).
    pub const fn has_open_event_pending(&self) -> bool {
        self.has_open_event_pending
    }

    /// The open event went out.
    pub fn clear_open_event_pending(&mut self) {
        self.has_open_event_pending = false;
    }

    /// Whether a new-leader event is owed to this session
    /// (`ClusterSession.java:439-447`).
    pub const fn has_new_leader_event_pending(&self) -> bool {
        self.has_new_leader_event_pending
    }

    /// Set or clear the new-leader-event flag.
    pub fn set_new_leader_event_pending(&mut self, pending: bool) {
        self.has_new_leader_event_pending = pending;
    }

    /// The session counter's slot once the driver has answered.
    pub const fn session_counter_id(&self) -> Option<i32> {
        self.session_counter_id
    }

    /// `ClusterSession.clearOpenEventPending` and the rest of what a snapshot
    /// load restores (`ClusterSession.java:139-158`): here, just the state.
    pub fn load_snapshot_state(
        &mut self,
        correlation_id: i64,
        opened_log_position: i64,
        time_of_last_activity_ns: i64,
        close_reason: CloseReason,
    ) {
        self.correlation_id = correlation_id;
        self.opened_log_position = opened_log_position;
        self.time_of_last_activity_ns = time_of_last_activity_ns;
        if close_reason == CloseReason::NullVal {
            self.set_state(ClusterSessionState::Open, "");
        } else {
            self.close_reason = close_reason;
            self.set_state(ClusterSessionState::Closing, "");
        }
    }

    /// `ClusterSession.asyncConnect` (`ClusterSession.java:190-194`): ask the
    /// driver for the session counter and the response publication.
    ///
    /// The reference issues both from inside the ingress callback; this build
    /// issues them right after the poll, which is the same duty cycle and the
    /// same two commands (see [`super::ingress`]).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] when the counter's key or label cannot be built, or when
    /// the command ring refuses either add.
    pub fn async_connect(
        &mut self,
        client: &mut Client,
        cluster_id: i32,
    ) -> Result<(), ClusterError> {
        let spec = counters::session_counter(cluster_id, self.id, &self.session_info)?;
        let counter = client.async_add_counter(
            CLUSTER_SESSION_TYPE_ID,
            spec.key(),
            spec.label(),
            COMMAND_TIMEOUT,
        )?;
        self.counter_registration_id = counter.registration_id();

        let publication = client.async_add_publication(
            &self.response_channel,
            self.response_stream_id,
            COMMAND_TIMEOUT,
        )?;
        self.response_publication_id = publication.registration_id();
        Ok(())
    }

    /// `ClusterSession.connect` (`ClusterSession.java:196-219`): a session that
    /// outlives a term re-creates the counter and the response publication it
    /// gave back when the term ended.
    ///
    /// Synchronous where [`Self::async_connect`] is not, because the reference's
    /// is, and because of what runs right after it: `prepareSessionsForNewTerm`
    /// calls this and then marks the new-leader event pending
    /// (`SessionManager.java:932-967`), so the event that goes out on the next
    /// duty cycle has to have a publication to go out on. Without one the send
    /// reports success into nothing: the client never learns where the cluster
    /// moved to, keeps offering to the old leader, and gets `CLOSED` (-4) from
    /// a publication the conductor took away from it — measured on the LC-2
    /// partition case, for a whole minute (`records/P3-6c.md`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] when the counter's key or label cannot be built or the
    /// counter itself is refused. A refused **publication** is not an error
    /// here: the reference reports it through its error handler as a counted
    /// `WARN` and leaves the session open, and every send path already treats
    /// "no response publication" as "not sent yet"
    /// ([`Self::is_response_publication_connected`]).
    pub fn connect(&mut self, client: &mut Client, cluster_id: i32) -> Result<(), ClusterError> {
        if self.response_publication_id != NULL_VALUE {
            return Err(ClusterError::Cluster(
                "response publication already added".to_owned(),
            ));
        }

        let spec = counters::session_counter(cluster_id, self.id, &self.session_info)?;
        let counter = client.add_counter(
            CLUSTER_SESSION_TYPE_ID,
            spec.key(),
            spec.label(),
            COMMAND_TIMEOUT,
        )?;
        self.counter_registration_id = counter.registration_id();

        if let Ok(registration_id) = client.add_publication(
            &self.response_channel,
            self.response_stream_id,
            COMMAND_TIMEOUT,
        ) {
            self.response_publication_id = registration_id;
        }

        Ok(())
    }

    /// `ClusterSession.isResponsePublicationConnected`
    /// (`ClusterSession.java:244-295`), as a decision over what the client
    /// answered.
    ///
    /// # Errors
    ///
    /// [`ClusterError::Cluster`] with the reference's words when the publication
    /// add failed for a reason other than
    /// `RESOURCE_TEMPORARILY_UNAVAILABLE`.
    pub fn is_response_publication_connected(
        &mut self,
        client: &mut Client,
        now_ns: i64,
    ) -> Result<bool, ClusterError> {
        if self.response_publication_id == NULL_VALUE {
            return Err(ClusterError::Cluster(
                "response publication already added".to_owned(),
            ));
        }

        // The publication, if the driver has answered the async add.
        if client.publication(self.response_publication_id).is_none() {
            match client.async_add_poll(AsyncAdd::publication(self.response_publication_id)) {
                AsyncAddPoll::Awaiting => return Ok(false),
                AsyncAddPoll::Ready => {}
                AsyncAddPoll::Failed(error) => match response_publication_retry(&error) {
                    ResponsePublicationOutcome::Retry => {
                        let add = client.async_add_publication(
                            &self.response_channel,
                            self.response_stream_id,
                            COMMAND_TIMEOUT,
                        )?;
                        self.response_publication_id = add.registration_id();
                        return Ok(false);
                    }
                    ResponsePublicationOutcome::Invalid => {
                        self.response_publication_id = NULL_VALUE;
                        self.set_state(ClusterSessionState::Invalid, "responsePublication is null");
                        return Ok(false);
                    }
                    ResponsePublicationOutcome::Fatal(message) => {
                        self.response_publication_id = NULL_VALUE;
                        return Err(ClusterError::Cluster(format!(
                            "failed to connect session response publication: {message}"
                        )));
                    }
                },
                AsyncAddPoll::Unknown => {
                    self.response_publication_id = NULL_VALUE;
                    self.set_state(ClusterSessionState::Invalid, "responsePublication is null");
                    return Ok(false);
                }
            }
        }

        // The publication is adopted. Its counter is the session counter; the
        // reference waits for it here (ClusterSession.java:270-292) and sets its
        // reference id to the publication's registration id.
        if self.session_counter_id.is_none() {
            if client.counter(self.counter_registration_id).is_none() {
                match client.async_add_poll(AsyncAdd::counter(self.counter_registration_id)) {
                    AsyncAddPoll::Awaiting => return Ok(false),
                    AsyncAddPoll::Ready => {}
                    // The reference's `aeron.getCounter` waits for the counter
                    // rather than re-issuing its add, so a transient refusal is
                    // "look again next turn" and anything else is fatal
                    // (`ClusterSession.java:270-292`).
                    AsyncAddPoll::Failed(error) => match response_publication_retry(&error) {
                        ResponsePublicationOutcome::Retry => return Ok(false),
                        ResponsePublicationOutcome::Invalid => {
                            self.set_state(
                                ClusterSessionState::Invalid,
                                "responsePublication is null",
                            );
                            return Ok(false);
                        }
                        ResponsePublicationOutcome::Fatal(message) => {
                            return Err(ClusterError::Cluster(format!(
                                "failed to connect session counter: {message}"
                            )));
                        }
                    },
                    AsyncAddPoll::Unknown => {
                        self.set_state(ClusterSessionState::Invalid, "responsePublication is null");
                        return Ok(false);
                    }
                }
            }

            let Some(counter) = client.counter(self.counter_registration_id) else {
                return Ok(false);
            };
            let counter_id = counter.counter_id();
            if let Some(counters) = client.counters_writable() {
                counters.set_reference_id(counter_id, self.response_publication_id);
                counters.set_value(counter_id, self.id);
            }
            self.session_counter_id = Some(counter_id);
        }

        self.time_of_last_activity_ns = now_ns;
        if matches!(
            self.state,
            ClusterSessionState::Init
                | ClusterSessionState::Connecting
                | ClusterSessionState::Connected
        ) {
            self.set_state(ClusterSessionState::Connecting, "connecting");
        }

        let publication = client
            .publication(self.response_publication_id)
            .ok_or_else(|| {
                ClusterError::Cluster("response publication vanished after its add".to_owned())
            })?;
        Ok(publication.is_connected().unwrap_or(false))
    }

    /// `ClusterSession.appendSessionToLogAndSendOpen`
    /// (`ClusterSession.java:348-369`): the open event goes into the log only
    /// when the client can still receive the answer.
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the log append or the egress send.
    #[allow(clippy::too_many_arguments)]
    pub fn append_session_to_log_and_send_open(
        &mut self,
        client: &Client,
        log_publisher: &mut crate::log_publisher::LogPublisher,
        egress: &mut EgressPublisher,
        leadership_term_id: i64,
        member_id: i32,
        now_ns: i64,
        timestamp: i64,
    ) -> Result<bool, ClusterError> {
        let Some(publication) = client.publication(self.response_publication_id) else {
            return Ok(false);
        };
        let position_limit = position_limit(client, publication);
        if publication.available_window(position_limit).unwrap_or(0) <= 0 {
            return Ok(false);
        }

        let position =
            log_publisher.append_session_open(client, self, leadership_term_id, timestamp)?;
        if position > 0 {
            self.open(position);
            self.time_of_last_activity_ns = now_ns;
            let sent = egress.send_event(
                &SessionEgress {
                    client,
                    session: self,
                },
                leadership_term_id,
                member_id,
                EventCode::OK,
                "",
            )?;
            if sent {
                self.clear_open_event_pending();
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// What to do with a failed response-publication or counter add
/// (`ClusterSession.java:264-293`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResponsePublicationOutcome {
    /// `RESOURCE_TEMPORARILY_UNAVAILABLE`: re-issue the add and try again.
    Retry,
    /// The command is no longer in flight and produced nothing: the session is
    /// `INVALID` and will be closed.
    Invalid,
    /// Anything else: the reference throws and the session is torn down.
    Fatal(String),
}

/// The decision half of `isResponsePublicationConnected`, kept separate so the
/// `RESOURCE_TEMPORARILY_UNAVAILABLE` branch is testable without a driver — the
/// `SessionManagerTest.shouldRetryCreatingResponsePublicationWhenResourceTemporarilyUnavailable`
/// case (`SessionManagerTest.java:170-207`).
pub fn response_publication_retry(error: &CommandError) -> ResponsePublicationOutcome {
    match error {
        CommandError::Driver { code, .. }
            if *code == ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE =>
        {
            ResponsePublicationOutcome::Retry
        }
        CommandError::TimedOut { .. } => ResponsePublicationOutcome::Invalid,
        other => ResponsePublicationOutcome::Fatal(other.to_string()),
    }
}

/// The response publication's window limit, read from its counter.
fn position_limit(client: &Client, publication: &Publication) -> i64 {
    let Some(counters) = client.counters_reader() else {
        return 0;
    };
    counters
        .value(publication.position_limit_counter_id())
        .unwrap_or(0)
}

/// A session seen as an egress destination (`EgressPublisher`'s collaborator).
pub(crate) struct SessionEgress<'a> {
    /// The client that owns the response publication.
    pub client: &'a Client,
    /// The session whose publication it is.
    pub session: &'a ClusterSession,
}

impl crate::session::egress::EgressSession for SessionEgress<'_> {
    fn id(&self) -> i64 {
        self.session.id
    }

    fn correlation_id(&self) -> i64 {
        self.session.correlation_id
    }

    fn max_payload_length(&self) -> Option<usize> {
        self.client
            .publication(self.session.response_publication_id)
            .and_then(Publication::max_payload_length)
    }

    fn try_claim(&self, length: usize, bytes: &[u8]) -> bool {
        let Some(publication) = self
            .client
            .publication(self.session.response_publication_id)
        else {
            return false;
        };
        let limit = position_limit(self.client, publication);
        let Ok(claim) = publication.try_claim(limit, length) else {
            return false;
        };
        let frame = claim.frame();
        if frame.write_payload(bytes).is_none() {
            return false;
        }
        // `publish` writes the **unaligned** frame length, data header
        // included: what `Frame::begin` already put there and what a reader
        // reports. Publishing the aligned length instead would make the
        // padding part of the payload.
        let frame_length = deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH + length;
        #[allow(clippy::cast_possible_truncation)] // bounded by max payload length
        let frame_length = frame_length as i32;
        frame.publish(frame_length).is_some()
    }

    fn offer(&self, bytes: &[u8]) -> bool {
        let Some(publication) = self
            .client
            .publication(self.session.response_publication_id)
        else {
            return false;
        };
        let limit = position_limit(self.client, publication);
        matches!(
            publication.offer(limit, bytes),
            deepmsg_core::logbuffer::append::Appended::Ok { .. }
        )
    }
}

/// The reference's `ClusterSessionProxy`: one session, one egress publisher and
/// the authorisation service, bound for the duration of one authenticator call.
pub struct ClusterSessionProxy<'a> {
    session: &'a mut ClusterSession,
    egress: &'a mut EgressPublisher,
    client: &'a Client,
}

impl<'a> ClusterSessionProxy<'a> {
    /// Bind the proxy (`ClusterSessionProxy.java:36-40`).
    pub fn new(
        session: &'a mut ClusterSession,
        egress: &'a mut EgressPublisher,
        client: &'a Client,
    ) -> Self {
        Self {
            session,
            egress,
            client,
        }
    }
}

impl SessionProxy for ClusterSessionProxy<'_> {
    fn session_id(&self) -> i64 {
        self.session.id
    }

    fn challenge(&mut self, encoded_challenge: &[u8]) -> Result<bool, ClusterError> {
        let sent = self.egress.send_challenge(
            &SessionEgress {
                client: self.client,
                session: self.session,
            },
            encoded_challenge,
        )?;
        if sent {
            self.session
                .set_state(ClusterSessionState::Challenged, "challenged");
        }
        Ok(sent)
    }

    fn authenticate(&mut self, encoded_principal: &[u8]) -> Result<bool, ClusterError> {
        self.session.authenticate(encoded_principal)?;
        Ok(true)
    }

    fn reject(&mut self) {
        self.session.reject(
            EventCode::AUTHENTICATION_REJECTED,
            crate::config::SESSION_REJECTED_MSG,
        );
    }
}

/// What a `SessionProxy::reject` records, exposed for the tests that assert it.
pub fn rejected_session_state(
    session: &ClusterSession,
) -> (ClusterSessionState, Option<EventCode>) {
    (session.state(), session.event_code())
}

/// The authorisation service the session manager holds, for the agent's
/// `onAdminRequest`.
pub fn is_authorised(
    service: &dyn AuthorisationService,
    protocol_id: i32,
    action_id: i32,
    action_type: Option<i32>,
    encoded_principal: &[u8],
) -> bool {
    service.is_authorised(protocol_id, action_id, action_type, encoded_principal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_id_is_a_plain_long_and_the_counter_key_is_cluster_then_id() {
        let spec =
            counters::session_counter(7, 3, "client sourceIdentity=(ipc) sessionId=1").unwrap();
        assert_eq!(12, spec.key().len());
        assert_eq!(7_i32.to_le_bytes(), spec.key()[..4]);
        assert_eq!(3_i64.to_le_bytes(), spec.key()[4..]);
        assert_eq!(
            "cluster-session: client sourceIdentity=(ipc) sessionId=1 - clusterId=7",
            spec.label()
        );
    }

    #[test]
    fn the_resource_temporarily_unavailable_answer_reissues_the_add() {
        let error = CommandError::Driver {
            code: ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE,
            message: "temporarily unavailable".to_owned(),
        };
        assert_eq!(
            ResponsePublicationOutcome::Retry,
            response_publication_retry(&error)
        );
    }

    #[test]
    fn any_other_driver_refusal_is_fatal() {
        let error = CommandError::Driver {
            code: 7,
            message: "nope".to_owned(),
        };
        assert!(matches!(
            response_publication_retry(&error),
            ResponsePublicationOutcome::Fatal(_)
        ));
    }

    #[test]
    fn a_principal_over_the_limit_is_refused_with_the_references_words() {
        let mut session = ClusterSession::new(0, 1, 102, "aeron:ipc", String::new());
        let error = session
            .authenticate(&vec![0_u8; crate::config::MAX_ENCODED_PRINCIPAL_LENGTH + 1])
            .unwrap_err();
        assert!(error.to_string().contains("encoded principal max length"));
    }
}
