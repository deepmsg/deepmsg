//! A connected cluster session (`AeronCluster.java:85-1127`).
//!
//! `P3-3a` built what a *successful connect* hands back and a `close`; this
//! card gives the handle its two directions: sending (`offer`, `try_claim`, the
//! keep-alive, the admin request, the session close) and receiving
//! ([`AeronCluster::poll_egress`]), plus what a leadership change does to both.
//!
//! # The session header
//!
//! Every ingress message starts with a 32-byte `SessionMessageHeader` — the SBE
//! header (8) and a block of 24 — carrying the session id and the leadership
//! term (`AC:99-100`, `:259-262`). The header is built **once**; a leader
//! change rewrites one field of it (`AC:687`). A message is offered as two
//! parts, header and payload, so nothing joins them in a buffer of their own
//! (`AC:451-458`) — the reference's two-buffer `offer`, which this build's
//! `Publication` has as `offer_two_slices`.
//!
//! # The write path never sees a `Client` of its own
//!
//! The spec's rule (§3): one caller-owned client can carry several sessions.
//! Every method that has to touch the driver takes `&mut Client` — the same
//! shape as the archive's client.

use std::time::Duration;

use deepmsg_client::client::{AsyncAdd, AsyncAddPoll, Client};
use deepmsg_client::fragment_assembler::{Action, Message};
use deepmsg_client::publication::Claim;
use deepmsg_codec::cluster::WriteBuf;
use deepmsg_codec::cluster::admin_request_codec::{
    AdminRequestEncoder, SBE_BLOCK_LENGTH as ADMIN_REQUEST_BLOCK_LENGTH,
};
use deepmsg_codec::cluster::admin_request_type::AdminRequestType;
use deepmsg_codec::cluster::event_code::EventCode;
use deepmsg_codec::cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec::cluster::session_close_request_codec::{
    SBE_BLOCK_LENGTH as SESSION_CLOSE_REQUEST_BLOCK_LENGTH, SessionCloseRequestEncoder,
};
use deepmsg_codec::cluster::session_keep_alive_codec::{
    SBE_BLOCK_LENGTH as SESSION_KEEP_ALIVE_BLOCK_LENGTH, SessionKeepAliveEncoder,
};
use deepmsg_codec::cluster::session_message_header_codec::SessionMessageHeaderEncoder;
use deepmsg_core::logbuffer::append::Appended;

use crate::async_connect::{
    async_add_ingress, ingress_kind, is_temporarily_unavailable, member_channel,
    parse_ingress_endpoints,
};
use crate::client_ops::{ClientOps, PublicationKind};
use crate::context::AeronClusterContext;
use crate::egress::{
    ControlledEgressListenerExtension, Egress, EgressListenerExtension, SESSION_HEADER_LENGTH,
    decode,
};
use crate::error::ClusterClientError;

/// `SEND_ATTEMPTS` (`AC:102`): how many times a claim-based send tries before
/// it gives up and says so.
const SEND_ATTEMPTS: usize = 3;

/// The ingress publication a session publishes through.
///
/// The reference has one `Publication` type with two implementations behind it
/// (`ExclusivePublication` and `ConcurrentPublication`); the choice is made by
/// `Context.isIngressExclusive`, which defaults to `true` (`AC:1436`,
/// `:795-813`). This build's client keeps the two in separate lists, so the
/// kind travels with the id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngressPublication {
    /// `Client::publication` — `isIngressExclusive == false`.
    Concurrent(i64),
    /// `Client::exclusive_publication` — the reference's default.
    Exclusive(i64),
}

impl IngressPublication {
    /// The publication's registration id.
    #[must_use]
    pub const fn registration_id(self) -> i64 {
        match self {
            Self::Concurrent(id) | Self::Exclusive(id) => id,
        }
    }

    /// The list it is in, for this crate's client seam.
    pub(crate) const fn kind(self) -> PublicationKind {
        match self {
            Self::Concurrent(_) => PublicationKind::Concurrent,
            Self::Exclusive(_) => PublicationKind::Exclusive,
        }
    }
}

/// Where a session is (`AC:1170-1193`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    /// Connected to a leader.
    Connected,
    /// Disconnected from a leader, waiting for a new one (`AC:836-840`). The
    /// deadline is `newLeaderTimeoutNs`; past it the session closes.
    AwaitNewLeader,
    /// A new leader has been elected, and the ingress publication is waiting to
    /// connect (`AC:683`). The deadline is `messageTimeoutNs`.
    AwaitNewLeaderConnection,
    /// The cluster closed the session, or the ingress publication reached its
    /// maximum position. The next state-change poll closes the client.
    PendingClose,
    /// Closed; terminal.
    Closed,
}

/// Claim `length` bytes on a publication of either kind.
fn claim<'c>(
    client: &'c Client,
    ingress: IngressPublication,
    length: usize,
) -> Result<Claim<'c>, Appended> {
    match ingress {
        IngressPublication::Concurrent(registration_id) => client
            .try_claim(registration_id, length)
            .unwrap_or(Err(Appended::NotConnected)),
        IngressPublication::Exclusive(registration_id) => {
            let publication = client
                .exclusive_publication(registration_id)
                .ok_or(Appended::NotConnected)?;
            let limit = client
                .counters_reader()
                .and_then(|counters| counters.value(publication.position_limit_counter_id()))
                .unwrap_or(0);

            publication.try_claim(limit, length)
        }
    }
}

/// A claimed ingress message: the session header is already written, and the
/// caller writes the payload and publishes it (`BufferClaim`, `AC:393-439`).
///
/// The reference hands over a mutable window; this build's buffer API
/// deliberately hands out no such slice, so the payload is written through
/// [`SessionClaim::write_payload`] instead.
pub struct SessionClaim<'a> {
    claim: Claim<'a>,
    length: usize,
}

impl SessionClaim<'_> {
    /// The payload length the claim was made for.
    #[must_use]
    pub const fn payload_length(&self) -> usize {
        self.length
    }

    /// Write the payload, after the session header.
    ///
    /// `None` when `payload` is not the length the claim was made for.
    pub fn write_payload(&self, payload: &[u8]) -> Option<()> {
        if payload.len() != self.length {
            return None;
        }

        self.claim
            .frame()
            .write_payload_slice_from(SESSION_HEADER_LENGTH, payload)
    }

    /// Publish the frame, which is what makes it visible to the cluster.
    pub fn commit(&self) -> Option<()> {
        let frame_length = i32::try_from(self.length + SESSION_HEADER_LENGTH).ok()?;

        self.claim.frame().publish(frame_length)
    }
}

/// A session a completed connect established.
pub struct AeronCluster {
    context: AeronClusterContext,
    /// `None` once the publication has been given back (disconnect, close).
    ingress: Option<IngressPublication>,
    egress_subscription: i64,
    egress_image: Option<i64>,
    cluster_session_id: i64,
    leadership_term_id: i64,
    leader_member_id: i32,
    state: SessionState,
    /// When the state's deadline expires, in `nano_clock` nanoseconds; `0` for
    /// the states that have none.
    state_deadline_ns: i64,
    /// The session message header, written once and re-termed on a leader
    /// change (`AC:259-262`, `:687`).
    header: [u8; SESSION_HEADER_LENGTH],
    /// The bytes of the small messages (keep-alive, admin request, session
    /// close), reused rather than rebuilt.
    scratch: Vec<u8>,
    /// Where another schema's egress messages go (`AC:272-285`).
    egress_listener_extension: Option<Box<dyn EgressListenerExtension>>,
    /// The same, for the controlled poll.
    controlled_egress_listener_extension: Option<Box<dyn ControlledEgressListenerExtension>>,
}

/// `FRAGMENT_LIMIT` (`AC:103`).
const FRAGMENT_LIMIT: usize = 10;

/// A new leader seen during a poll, acted on when the poll returns.
struct PendingLeader {
    session_id: i64,
    leadership_term_id: i64,
    leader_member_id: i32,
    ingress_endpoints: String,
}

/// The reference's default listener throws when it is called
/// (`AC:1529-1552`); this is the same answer as an error.
fn missing_egress_listener() -> ClusterClientError {
    ClusterClientError::Configuration(
        "egressListener must be specified on AeronCluster.Context".to_owned(),
    )
}

impl core::fmt::Debug for AeronCluster {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AeronCluster")
            .field("cluster_session_id", &self.cluster_session_id)
            .field("leadership_term_id", &self.leadership_term_id)
            .field("leader_member_id", &self.leader_member_id)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl AeronCluster {
    /// The synchronous connect (`AC:141-200`): an [`crate::AsyncConnect`] in a
    /// loop, with the caller's client driven.
    ///
    /// **The client is driven here, and nowhere else.** `AsyncConnect::poll`
    /// asks the client questions; the `client.poll()` in this loop is what lets
    /// the driver answer them. A connect written without that call is a
    /// connect that waits for answers nobody reads.
    ///
    /// # Errors
    ///
    /// Whatever the connect fails with.
    pub fn connect(
        context: AeronClusterContext,
        client: &mut Client,
    ) -> Result<Self, crate::error::ClusterClientError> {
        crate::async_connect::connect(context, client)
    }

    /// Assemble a session from what a connect established.
    pub(crate) fn new(
        context: AeronClusterContext,
        ingress: IngressPublication,
        egress_subscription: i64,
        egress_image: Option<i64>,
        cluster_session_id: i64,
        leadership_term_id: i64,
        leader_member_id: i32,
    ) -> Self {
        let mut session = Self {
            context,
            ingress: Some(ingress),
            egress_subscription,
            egress_image,
            cluster_session_id,
            leadership_term_id,
            leader_member_id,
            state: SessionState::Connected,
            state_deadline_ns: 0,
            header: [0u8; SESSION_HEADER_LENGTH],
            scratch: Vec::new(),
            egress_listener_extension: None,
            controlled_egress_listener_extension: None,
        };

        session.write_header();
        session
    }

    /// The SBE session header, written once at construction
    /// (`AC:259-262`).
    fn write_header(&mut self) {
        let encoder = SessionMessageHeaderEncoder::default()
            .wrap(WriteBuf::new(&mut self.header), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().expect("the encoder the header wrapped");

        encoder
            .cluster_session_id(self.cluster_session_id)
            .leadership_term_id(self.leadership_term_id)
            // The reference writes zero and leaves it: the cluster stamps the
            // timestamp when it sequences the message.
            .timestamp(0);
    }

    /// The context the session was opened with.
    ///
    /// The values a completed connect settled on — `new_leader_timeout_ns`,
    /// which is otherwise `None` — are read here. The reference keeps the
    /// caller's `Context` object alive and updates it in place; this context
    /// moved, so the session is where its settled values live.
    #[must_use]
    pub fn context(&self) -> &AeronClusterContext {
        &self.context
    }

    /// The ingress publication, with its kind, or `None` while the session has
    /// none (a disconnect, or a new leader not yet connected).
    #[must_use]
    pub const fn ingress_publication(&self) -> Option<IngressPublication> {
        self.ingress
    }

    /// The subscription the cluster's answers arrive on.
    #[must_use]
    pub const fn egress_subscription(&self) -> i64 {
        self.egress_subscription
    }

    /// The image the last session event or new leader arrived on.
    #[must_use]
    pub const fn egress_image(&self) -> Option<i64> {
        self.egress_image
    }

    /// The session the cluster assigned.
    #[must_use]
    pub const fn cluster_session_id(&self) -> i64 {
        self.cluster_session_id
    }

    /// The leadership term the session is in.
    #[must_use]
    pub const fn leadership_term_id(&self) -> i64 {
        self.leadership_term_id
    }

    /// The member that leads.
    #[must_use]
    pub const fn leader_member_id(&self) -> i32 {
        self.leader_member_id
    }

    /// Where the session is (`AC:1170-1193`).
    #[must_use]
    pub const fn state(&self) -> SessionState {
        self.state
    }

    /// How long the session waits for a new leader, now that a connect has
    /// settled it (`AC:2574-2578`, `:1604-1616`).
    #[must_use]
    pub const fn new_leader_timeout_ns(&self) -> Option<i64> {
        self.context.new_leader_timeout_ns
    }

    /// Whether the session is closed (`AC:319-322`).
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        matches!(self.state, SessionState::Closed)
    }

    /// Offer `payload` to the cluster (`AC:451-458`).
    ///
    /// Two parts, not one: the resident session header and the caller's
    /// payload, with nothing joining them in a buffer of their own.
    ///
    /// The result is tracked (`AC:455`), so a `NOT_CONNECTED` moves the session
    /// to `AWAIT_NEW_LEADER` and a successful offer after a new leader moves it
    /// back to `CONNECTED`.
    pub fn offer(&mut self, client: &mut Client, payload: &[u8]) -> Option<Appended> {
        self.offer_with(client, payload)
    }

    /// The same, over the crate's client seam.
    pub(crate) fn offer_with(
        &mut self,
        client: &mut impl ClientOps,
        payload: &[u8],
    ) -> Option<Appended> {
        let Some(ingress) = self.ingress else {
            self.track_ingress_result(client, None);
            return None;
        };

        let header = self.header;
        let result =
            client.offer_two_slices(ingress.kind(), ingress.registration_id(), &header, payload);
        self.track_ingress_result(client, result.as_ref());
        result
    }

    /// Claim `length` payload bytes and hand them back to be written in place
    /// (`AC:427-439`).
    ///
    /// The session header is written into the claim **now**; the caller writes
    /// the payload through [`SessionClaim::write_payload`] and publishes it
    /// with [`SessionClaim::commit`].
    ///
    /// # Errors
    ///
    /// [`Appended`] as the publication gives it, or `NotConnected` when the
    /// session has no publication.
    pub fn try_claim<'c>(
        &mut self,
        client: &'c Client,
        length: usize,
    ) -> Result<SessionClaim<'c>, Appended> {
        let Some(ingress) = self.ingress else {
            return Err(Appended::NotConnected);
        };
        let total = length
            .checked_add(SESSION_HEADER_LENGTH)
            .ok_or(Appended::Malformed)?;

        let claim = claim(client, ingress, total)?;

        let header = self.header;
        if claim.frame().write_payload(&header).is_none() {
            return Err(Appended::Malformed);
        }

        // A claim that succeeded is the reference's `0 < result` branch: a
        // session waiting for its new leader is connected again (`AC:727-733`).
        if matches!(self.state, SessionState::AwaitNewLeaderConnection) {
            self.state = SessionState::Connected;
            self.state_deadline_ns = 0;
        }

        Ok(SessionClaim { claim, length })
    }

    /// Send a keep-alive for the session (`AC:487-526`).
    ///
    /// The caller owns the rhythm: the reference does not time it either — a
    /// session times out after ten seconds without one.
    pub fn send_keep_alive(&mut self, client: &mut Client) -> bool {
        self.send_keep_alive_with(client)
    }

    /// The same, over the crate's client seam.
    pub(crate) fn send_keep_alive_with(&mut self, client: &mut impl ClientOps) -> bool {
        let (leadership_term_id, cluster_session_id) =
            (self.leadership_term_id, self.cluster_session_id);
        let length = self.encode(
            MESSAGE_HEADER_LENGTH + usize::from(SESSION_KEEP_ALIVE_BLOCK_LENGTH),
            move |buffer| {
                let encoder = SessionKeepAliveEncoder::default()
                    .wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
                let mut header = encoder.header(0);
                let mut encoder = header.parent().expect("the encoder the header wrapped");

                encoder
                    .leadership_term_id(leadership_term_id)
                    .cluster_session_id(cluster_session_id);

                MESSAGE_HEADER_LENGTH + encoder.encoded_length()
            },
        );

        self.send_with_attempts(client, length)
    }

    /// Ask the cluster to take a snapshot (`AC:536-586`).
    pub fn send_admin_request_to_take_a_snapshot(
        &mut self,
        client: &mut Client,
        correlation_id: i64,
    ) -> bool {
        self.send_admin_request_to_take_a_snapshot_with(client, correlation_id)
    }

    /// The same, over the crate's client seam.
    pub(crate) fn send_admin_request_to_take_a_snapshot_with(
        &mut self,
        client: &mut impl ClientOps,
        correlation_id: i64,
    ) -> bool {
        let (leadership_term_id, cluster_session_id) =
            (self.leadership_term_id, self.cluster_session_id);
        let length = self.encode(
            // The block, plus the four-byte length prefix its (empty) payload
            // carries.
            MESSAGE_HEADER_LENGTH + usize::from(ADMIN_REQUEST_BLOCK_LENGTH) + 4,
            move |buffer| {
                let encoder = AdminRequestEncoder::default()
                    .wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
                let mut header = encoder.header(0);
                let mut encoder = header.parent().expect("the encoder the header wrapped");

                encoder
                    .leadership_term_id(leadership_term_id)
                    .cluster_session_id(cluster_session_id)
                    .correlation_id(correlation_id)
                    .request_type(AdminRequestType::SNAPSHOT)
                    .payload(&[]);

                MESSAGE_HEADER_LENGTH + encoder.encoded_length()
            },
        );

        self.send_with_attempts(client, length)
    }

    /// Track what an ingress publication returned (`AC:713-734`).
    ///
    /// The one place a send's answer is turned into a state: a publication that
    /// lost its subscriber or closed moves the session to `AWAIT_NEW_LEADER`;
    /// a publication at its maximum position is closed and the session moves to
    /// `PENDING_CLOSE`; a successful send while waiting for the new leader's
    /// connection moves the session back to `CONNECTED`.
    pub(crate) fn track_ingress_result(
        &mut self,
        client: &mut impl ClientOps,
        result: Option<&Appended>,
    ) {
        match self.state {
            SessionState::Connected => match result {
                None | Some(Appended::NotConnected) => self.on_disconnected(client),
                Some(Appended::MaxPositionExceeded) => {
                    self.release_ingress(client);
                    self.state = SessionState::PendingClose;
                    self.state_deadline_ns = 0;
                }
                _ => {}
            },
            SessionState::AwaitNewLeaderConnection => {
                if matches!(result, Some(Appended::Ok { .. })) {
                    self.state = SessionState::Connected;
                    self.state_deadline_ns = 0;
                }
            }
            _ => {}
        }
    }

    /// `onDisconnected` (`AC:836-840`): give the publication back and wait for
    /// a new leader, for as long as the context's `newLeaderTimeoutNs` says.
    pub(crate) fn on_disconnected(&mut self, client: &mut impl ClientOps) {
        if matches!(self.state, SessionState::AwaitNewLeader) {
            return;
        }

        self.release_ingress(client);
        let timeout = self.context.new_leader_timeout_ns.unwrap_or(0);
        self.state = SessionState::AwaitNewLeader;
        self.state_deadline_ns = self.now() + timeout;
    }

    /// Close the session if it is due (`AC:648-660`).
    ///
    /// Needed explicitly by a caller that reads [`AeronCluster::egress_subscription`]
    /// directly instead of calling [`AeronCluster::poll_egress`].
    pub fn poll_state_changes(&mut self, client: &mut Client) -> usize {
        self.poll_state_changes_with(client)
    }

    /// The same, over the crate's client seam.
    pub(crate) fn poll_state_changes_with(&mut self, client: &mut impl ClientOps) -> usize {
        let due = matches!(self.state, SessionState::PendingClose)
            || (matches!(
                self.state,
                SessionState::AwaitNewLeader | SessionState::AwaitNewLeaderConnection
            ) && self.now() >= self.state_deadline_ns);

        if due {
            self.close_with(client);
            return 1;
        }

        0
    }

    /// Close the session and release its resources (`AC:290-311`).
    ///
    /// A connected session tells the cluster first — `SessionCloseRequest`, so
    /// the service sees `on_session_close` at once rather than waiting out the
    /// session timeout — and then gives the subscription and the publication
    /// back.
    pub fn close(&mut self, client: &mut Client) {
        self.close_with(client);
    }

    /// The same, over the crate's client seam.
    pub(crate) fn close_with(&mut self, client: &mut impl ClientOps) {
        if self.is_closed() {
            return;
        }

        if matches!(self.state, SessionState::Connected) {
            if let Some(ingress) = self.ingress {
                if client.publication_is_connected(ingress.kind(), ingress.registration_id()) {
                    self.close_session(client);
                }
            }
        }

        let timeout = self.timeout();
        let _ = client.async_remove_subscription(self.egress_subscription, timeout);
        self.release_ingress(client);

        self.state = SessionState::Closed;
        self.state_deadline_ns = 0;
    }

    /// `closeSession` (`AC:1129-1160`): tell the cluster the session is over.
    fn close_session(&mut self, client: &mut impl ClientOps) {
        let (leadership_term_id, cluster_session_id) =
            (self.leadership_term_id, self.cluster_session_id);
        let length = self.encode(
            MESSAGE_HEADER_LENGTH + usize::from(SESSION_CLOSE_REQUEST_BLOCK_LENGTH),
            move |buffer| {
                let encoder = SessionCloseRequestEncoder::default()
                    .wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
                let mut header = encoder.header(0);
                let mut encoder = header.parent().expect("the encoder the header wrapped");

                encoder
                    .leadership_term_id(leadership_term_id)
                    .cluster_session_id(cluster_session_id);

                MESSAGE_HEADER_LENGTH + encoder.encoded_length()
            },
        );

        self.send_with_attempts(client, length);
    }

    /// Offer the message in `scratch`, up to `SEND_ATTEMPTS` times, tracking
    /// each result (`AC:487-526`, `:536-586`, `:1129-1160`).
    fn send_with_attempts(&mut self, client: &mut impl ClientOps, length: usize) -> bool {
        for attempt in 0..SEND_ATTEMPTS {
            let result = self.ingress.and_then(|ingress| {
                client.offer(
                    ingress.kind(),
                    ingress.registration_id(),
                    &self.scratch[..length],
                )
            });

            self.track_ingress_result(client, result.as_ref());

            if matches!(result, Some(Appended::Ok { .. })) {
                return true;
            }

            if attempt + 1 < SEND_ATTEMPTS {
                // The reference runs its idle strategy here, whose first act
                // is to yield; this build's client has no strategy to run.
                std::thread::yield_now();
            }
        }

        false
    }

    /// Encode a message whose block is `block_length`, into `scratch`, and
    /// answer its total length.
    ///
    /// The closure is handed the encoder after the SBE header has been written,
    /// writes the block's fields, and answers the encoded length.
    fn encode<F>(&mut self, capacity: usize, write: F) -> usize
    where
        F: FnOnce(&mut [u8]) -> usize,
    {
        self.scratch.resize(capacity, 0);
        write(&mut self.scratch)
    }

    /// Poll the egress subscription and dispatch what it holds
    /// (`AC:599-612`).
    ///
    /// This, or [`Self::controlled_poll_egress`], is what notices a leader
    /// change: without it a session that lost its leader waits for nothing.
    ///
    /// # Errors
    ///
    /// [`ClusterClientError::Configuration`] when a message arrives and the
    /// context has no listener; [`ClusterClientError::Cluster`] for another
    /// schema with no extension listener. The reference throws from inside the
    /// poll for both.
    pub fn poll_egress(&mut self, client: &mut Client) -> Result<usize, ClusterClientError> {
        self.poll_egress_with(client)
    }

    /// The same, over the crate's client seam.
    pub(crate) fn poll_egress_with(
        &mut self,
        client: &mut impl ClientOps,
    ) -> Result<usize, ClusterClientError> {
        let subscription = self.egress_subscription;
        let session_id = self.cluster_session_id;
        let mut pending_leader = None;
        let mut error = None;

        let mut work = {
            let mut handler = |message: Message<'_>| -> Action {
                if error.is_some() {
                    return Action::Break;
                }

                match decode(&message) {
                    Some(Egress::Message {
                        cluster_session_id,
                        timestamp,
                        payload,
                    }) => {
                        if cluster_session_id == session_id {
                            match self.context.egress_listener.as_mut() {
                                Some(listener) => listener.on_message(
                                    cluster_session_id,
                                    timestamp,
                                    payload,
                                    &message.header,
                                ),
                                None => {
                                    error = Some(missing_egress_listener());
                                    return Action::Break;
                                }
                            }
                        }
                        Action::Continue
                    }
                    Some(Egress::SessionEvent {
                        correlation_id,
                        cluster_session_id,
                        leadership_term_id,
                        leader_member_id,
                        code,
                        detail,
                    }) => {
                        if cluster_session_id == session_id {
                            if EventCode::CLOSED == code {
                                self.state = SessionState::PendingClose;
                                self.state_deadline_ns = 0;
                            }

                            // The reference's default listener implements
                            // only `onMessage` (`AC:1529-1552`); the other
                            // three callbacks are default no-ops, so an event
                            // with no listener is consumed and nothing more.
                            if let Some(listener) = self.context.egress_listener.as_mut() {
                                listener.on_session_event(
                                    correlation_id,
                                    cluster_session_id,
                                    leadership_term_id,
                                    leader_member_id,
                                    code,
                                    &String::from_utf8_lossy(detail),
                                );
                            }
                        }
                        Action::Continue
                    }
                    Some(Egress::NewLeader {
                        cluster_session_id,
                        leadership_term_id,
                        leader_member_id,
                        ingress_endpoints,
                    }) => {
                        if cluster_session_id == session_id {
                            self.egress_image = Some(message.header.image_registration_id);
                            pending_leader = Some(PendingLeader {
                                session_id: cluster_session_id,
                                leadership_term_id,
                                leader_member_id,
                                ingress_endpoints: String::from_utf8_lossy(ingress_endpoints)
                                    .into_owned(),
                            });
                        }
                        Action::Continue
                    }
                    Some(Egress::AdminResponse {
                        cluster_session_id,
                        correlation_id,
                        request_type,
                        response_code,
                        message: admin_message,
                        payload,
                    }) => {
                        if cluster_session_id == session_id {
                            if let Some(listener) = self.context.egress_listener.as_mut() {
                                listener.on_admin_response(
                                    cluster_session_id,
                                    correlation_id,
                                    request_type,
                                    response_code,
                                    &String::from_utf8_lossy(admin_message),
                                    payload,
                                );
                            }
                        }
                        Action::Continue
                    }
                    Some(Egress::Extension {
                        acting_block_length,
                        template_id,
                        schema_id,
                        acting_version,
                        body,
                    }) => match self.egress_listener_extension.as_mut() {
                        Some(extension) => {
                            extension.on_extension_message(
                                acting_block_length,
                                template_id,
                                schema_id,
                                acting_version,
                                body,
                            );
                            Action::Continue
                        }
                        None => {
                            error = Some(ClusterClientError::Cluster(format!(
                                "expected cluster egress schemaId={} actual={schema_id}",
                                deepmsg_codec::cluster::SBE_SCHEMA_ID
                            )));
                            Action::Break
                        }
                    },
                    None => Action::Continue,
                }
            };

            client.poll_egress(subscription, FRAGMENT_LIMIT, &mut handler)
        };

        // The reference acts on a new leader **inside** the poll; here it is
        // acted on as soon as the poll returns, because adding the new
        // publication needs the client the poll was borrowing. Everything in
        // the same poll was dispatched against the old leader either way.
        if let Some(leader) = pending_leader {
            self.on_new_leader_with(
                client,
                leader.session_id,
                leader.leadership_term_id,
                leader.leader_member_id,
                &leader.ingress_endpoints,
            )?;
        }

        // `AC:603-607`: an egress image that has closed is a lost leader. The
        // reference checks it after the poll and before the state changes.
        if matches!(
            self.state,
            SessionState::Connected | SessionState::AwaitNewLeaderConnection
        ) && self
            .egress_image
            .is_some_and(|image| client.image_is_closed(subscription, image))
        {
            self.on_disconnected(client);
            work += 1;
        }

        work += self.poll_state_changes_with(client);

        match error {
            Some(error) => Err(error),
            None => Ok(work),
        }
    }

    /// The controlled poll (`AC:626-639`): the session message's consumption is
    /// the listener's to decide.
    ///
    /// # Errors
    ///
    /// As [`Self::poll_egress`].
    pub fn controlled_poll_egress(
        &mut self,
        client: &mut Client,
    ) -> Result<usize, ClusterClientError> {
        self.controlled_poll_egress_with(client)
    }

    /// The same, over the crate's client seam.
    pub(crate) fn controlled_poll_egress_with(
        &mut self,
        client: &mut impl ClientOps,
    ) -> Result<usize, ClusterClientError> {
        let subscription = self.egress_subscription;
        let session_id = self.cluster_session_id;
        let mut pending_leader = None;
        let mut error = None;

        let mut work = {
            let mut handler = |message: Message<'_>| -> Action {
                if error.is_some() {
                    return Action::Break;
                }

                match decode(&message) {
                    Some(Egress::Message {
                        cluster_session_id,
                        timestamp,
                        payload,
                    }) => {
                        if cluster_session_id == session_id {
                            return match self.context.controlled_egress_listener.as_mut() {
                                Some(listener) => listener.on_message(
                                    cluster_session_id,
                                    timestamp,
                                    payload,
                                    &message.header,
                                ),
                                None => {
                                    error = Some(ClusterClientError::Configuration(
                                        "controlledEgressListener must be specified on \
                                     AeronCluster.Context"
                                            .to_owned(),
                                    ));
                                    Action::Break
                                }
                            };
                        }
                        Action::Continue
                    }
                    Some(Egress::SessionEvent {
                        correlation_id,
                        cluster_session_id,
                        leadership_term_id,
                        leader_member_id,
                        code,
                        detail,
                    }) => {
                        if cluster_session_id == session_id {
                            if EventCode::CLOSED == code {
                                self.state = SessionState::PendingClose;
                                self.state_deadline_ns = 0;
                            }

                            if let Some(listener) = self.context.controlled_egress_listener.as_mut()
                            {
                                listener.on_session_event(
                                    correlation_id,
                                    cluster_session_id,
                                    leadership_term_id,
                                    leader_member_id,
                                    code,
                                    &String::from_utf8_lossy(detail),
                                );
                            }
                        }
                        Action::Continue
                    }
                    Some(Egress::NewLeader {
                        cluster_session_id,
                        leadership_term_id,
                        leader_member_id,
                        ingress_endpoints,
                    }) => {
                        if cluster_session_id == session_id {
                            self.egress_image = Some(message.header.image_registration_id);
                            pending_leader = Some(PendingLeader {
                                session_id: cluster_session_id,
                                leadership_term_id,
                                leader_member_id,
                                ingress_endpoints: String::from_utf8_lossy(ingress_endpoints)
                                    .into_owned(),
                            });
                        }
                        // `AC:1082`: a new leader is committed, never re-read.
                        Action::Commit
                    }
                    Some(Egress::AdminResponse {
                        cluster_session_id,
                        correlation_id,
                        request_type,
                        response_code,
                        message: admin_message,
                        payload,
                    }) => {
                        if cluster_session_id == session_id {
                            if let Some(listener) = self.context.controlled_egress_listener.as_mut()
                            {
                                listener.on_admin_response(
                                    cluster_session_id,
                                    correlation_id,
                                    request_type,
                                    response_code,
                                    &String::from_utf8_lossy(admin_message),
                                    payload,
                                );
                            }
                        }
                        Action::Continue
                    }
                    Some(Egress::Extension {
                        acting_block_length,
                        template_id,
                        schema_id,
                        acting_version,
                        body,
                    }) => match self.controlled_egress_listener_extension.as_mut() {
                        Some(extension) => extension.on_extension_message(
                            acting_block_length,
                            template_id,
                            schema_id,
                            acting_version,
                            body,
                        ),
                        None => {
                            error = Some(ClusterClientError::Cluster(format!(
                                "expected cluster egress schemaId={} actual={schema_id}",
                                deepmsg_codec::cluster::SBE_SCHEMA_ID
                            )));
                            Action::Break
                        }
                    },
                    None => Action::Continue,
                }
            };

            client.poll_egress(subscription, FRAGMENT_LIMIT, &mut handler)
        };

        if let Some(leader) = pending_leader {
            self.on_new_leader_with(
                client,
                leader.session_id,
                leader.leadership_term_id,
                leader.leader_member_id,
                &leader.ingress_endpoints,
            )?;
        }

        if matches!(
            self.state,
            SessionState::Connected | SessionState::AwaitNewLeaderConnection
        ) && self
            .egress_image
            .is_some_and(|image| client.image_is_closed(subscription, image))
        {
            self.on_disconnected(client);
            work += 1;
        }

        work += self.poll_state_changes_with(client);

        match error {
            Some(error) => Err(error),
            None => Ok(work),
        }
    }

    /// A new leader has been elected (`AC:671-704`).
    ///
    /// Writes the new term into the session header, gives the old publication
    /// back, and adds the new leader's — by the configured channel when there
    /// are no endpoints, or by the endpoint the event named.
    ///
    /// # Errors
    ///
    /// [`ClusterClientError::Cluster`] for an event about another session or an
    /// endpoint list without the leader; [`ClusterClientError::Timeout`] when
    /// the new publication does not arrive in time.
    pub fn on_new_leader(
        &mut self,
        client: &mut Client,
        cluster_session_id: i64,
        leadership_term_id: i64,
        leader_member_id: i32,
        ingress_endpoints: &str,
    ) -> Result<(), ClusterClientError> {
        self.on_new_leader_with(
            client,
            cluster_session_id,
            leadership_term_id,
            leader_member_id,
            ingress_endpoints,
        )
    }

    /// The same, over the crate's client seam.
    pub(crate) fn on_new_leader_with(
        &mut self,
        client: &mut impl ClientOps,
        cluster_session_id: i64,
        leadership_term_id: i64,
        leader_member_id: i32,
        ingress_endpoints: &str,
    ) -> Result<(), ClusterClientError> {
        if cluster_session_id != self.cluster_session_id {
            return Err(ClusterClientError::Cluster(format!(
                "invalid clusterSessionId={cluster_session_id} expected={}",
                self.cluster_session_id
            )));
        }

        self.state = SessionState::AwaitNewLeaderConnection;
        self.state_deadline_ns = self.now() + self.context.message_timeout_ns;
        self.leadership_term_id = leadership_term_id;
        self.leader_member_id = leader_member_id;
        // The one field of the session header a leader change rewrites
        // (`AC:687`).
        self.write_header();

        self.release_ingress(client);

        let channel = if self.context.ingress_endpoints.is_none() {
            self.context.ingress_channel.clone().ok_or_else(|| {
                ClusterClientError::Configuration("ingressChannel must be specified".to_owned())
            })?
        } else {
            let members = parse_ingress_endpoints(Some(ingress_endpoints))?;
            let leader = members
                .iter()
                .find(|member| member.member_id() == leader_member_id)
                .ok_or_else(|| {
                    ClusterClientError::Cluster(format!(
                        "the redirect names member {leader_member_id}, which                          {ingress_endpoints:?} does not"
                    ))
                })?;

            member_channel(&self.context, leader.endpoint())
        };

        let ingress = self.add_new_leader_ingress_publication(client, &channel)?;
        self.ingress = Some(ingress);

        // Both listeners hear about it, whichever poll is in use
        // (`AC:702-703`).
        if let Some(listener) = self.context.egress_listener.as_mut() {
            listener.on_new_leader(
                cluster_session_id,
                leadership_term_id,
                leader_member_id,
                ingress_endpoints,
            );
        }
        if let Some(listener) = self.context.controlled_egress_listener.as_mut() {
            listener.on_new_leader(
                cluster_session_id,
                leadership_term_id,
                leader_member_id,
                ingress_endpoints,
            );
        }

        Ok(())
    }

    /// `addNewLeaderIngressPublication` (`AC:758-793`): add the new leader's
    /// publication, retrying until the context's `messageTimeoutNs` runs out.
    ///
    /// This loop **drives the client**: the add's answer arrives in a turn of
    /// the client, and the reference's loop runs the agent invoker for the same
    /// reason (`AC:786`).
    fn add_new_leader_ingress_publication(
        &mut self,
        client: &mut impl ClientOps,
        channel: &str,
    ) -> Result<IngressPublication, ClusterClientError> {
        let deadline = self.now() + self.context.message_timeout_ns;
        let mut add: Option<AsyncAdd> = None;

        loop {
            if add.is_none() {
                add = Some(async_add_ingress(client, &self.context, channel)?);
            }

            let handle = add.expect("just added");
            match client.async_add_poll(handle) {
                AsyncAddPoll::Ready => {
                    return Ok(ingress_kind(&self.context, handle.registration_id()));
                }
                AsyncAddPoll::Unknown => {
                    let kind = ingress_kind(&self.context, handle.registration_id()).kind();
                    if client.publication_present(kind, handle.registration_id()) {
                        return Ok(ingress_kind(&self.context, handle.registration_id()));
                    }
                    add = None;
                }
                AsyncAddPoll::Failed(error) => {
                    add = None;
                    if !is_temporarily_unavailable(&error) {
                        return Err(ClusterClientError::Client(error.to_string()));
                    }
                }
                AsyncAddPoll::Awaiting => {}
            }

            if self.now() >= deadline {
                return Err(ClusterClientError::Timeout(format!(
                    "failed to add new leader ingress publication (leaderMemberId={}, \
                     leadershipTermId={}, channel={channel}, streamId={}) within {}ns",
                    self.leader_member_id,
                    self.leadership_term_id,
                    self.context.ingress_stream_id,
                    self.context.message_timeout_ns
                )));
            }

            client.poll();
            std::thread::yield_now();
        }
    }

    /// The extension listener for the plain poll (`AC:272-275`).
    pub fn egress_listener_extension(&mut self, extension: Box<dyn EgressListenerExtension>) {
        self.egress_listener_extension = Some(extension);
    }

    /// The extension listener for the controlled poll (`AC:282-285`).
    pub fn controlled_egress_listener_extension(
        &mut self,
        extension: Box<dyn ControlledEgressListenerExtension>,
    ) {
        self.controlled_egress_listener_extension = Some(extension);
    }

    fn now(&mut self) -> i64 {
        self.context.nano_clock.nano_time()
    }

    fn timeout(&self) -> Duration {
        Duration::from_nanos(u64::try_from(self.context.message_timeout_ns).unwrap_or(0))
    }

    fn release_ingress(&mut self, client: &mut impl ClientOps) {
        if let Some(ingress) = self.ingress.take() {
            let _ = client.async_remove_publication(ingress.registration_id(), self.timeout());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    use deepmsg_client::fragment_assembler::MessageHeader;
    use deepmsg_codec::cluster::admin_request_codec::AdminRequestDecoder;
    use deepmsg_codec::cluster::event_code::EventCode;
    use deepmsg_codec::cluster::message_header_codec::MessageHeaderDecoder;
    use deepmsg_codec::cluster::new_leader_event_codec::NewLeaderEventEncoder;
    use deepmsg_codec::cluster::session_close_request_codec::SessionCloseRequestDecoder;
    use deepmsg_codec::cluster::session_event_codec::SessionEventEncoder;
    use deepmsg_codec::cluster::session_keep_alive_codec::SessionKeepAliveDecoder;
    use deepmsg_codec::cluster::session_message_header_codec::SessionMessageHeaderDecoder;
    use deepmsg_codec::cluster::{ReadBuf, SBE_SCHEMA_ID, WriteBuf};

    use super::*;
    use crate::context::NanoClock;
    use crate::test_client::RecordingClient;

    const EGRESS_SUBSCRIPTION: i64 = 9;
    const IMAGE: i64 = 11;
    const CLUSTER_SESSION_ID: i64 = 123;
    const NEW_LEADER_TIMEOUT_NS: i64 = 1_000_000_000;
    const INGRESS_ENDPOINTS: &str = "0=localhost:10000,1=localhost:20000";

    struct TestClock(Rc<Cell<i64>>);

    impl NanoClock for TestClock {
        fn nano_time(&mut self) -> i64 {
            self.0.get()
        }
    }

    #[derive(Default)]
    struct RecordingListener {
        calls: Vec<String>,
    }

    impl crate::egress::EgressListener for RecordingListener {
        fn on_message(
            &mut self,
            cluster_session_id: i64,
            timestamp: i64,
            payload: &[u8],
            _header: &MessageHeader,
        ) {
            self.calls.push(format!(
                "message({cluster_session_id}, {timestamp}, {:?})",
                payload
            ));
        }

        fn on_session_event(
            &mut self,
            _correlation_id: i64,
            cluster_session_id: i64,
            _leadership_term_id: i64,
            _leader_member_id: i32,
            code: EventCode,
            detail: &str,
        ) {
            self.calls.push(format!(
                "session_event({cluster_session_id}, {code:?}, {detail})"
            ));
        }

        fn on_new_leader(
            &mut self,
            cluster_session_id: i64,
            leadership_term_id: i64,
            leader_member_id: i32,
            ingress_endpoints: &str,
        ) {
            self.calls.push(format!(
                "new_leader({cluster_session_id}, {leadership_term_id}, {leader_member_id}, \
                 {ingress_endpoints})"
            ));
        }
    }

    struct SharedListener(Rc<RefCell<RecordingListener>>);

    impl crate::egress::EgressListener for SharedListener {
        fn on_message(
            &mut self,
            cluster_session_id: i64,
            timestamp: i64,
            payload: &[u8],
            header: &MessageHeader,
        ) {
            self.0
                .borrow_mut()
                .on_message(cluster_session_id, timestamp, payload, header);
        }

        fn on_session_event(
            &mut self,
            correlation_id: i64,
            cluster_session_id: i64,
            leadership_term_id: i64,
            leader_member_id: i32,
            code: EventCode,
            detail: &str,
        ) {
            let _ = (correlation_id, leadership_term_id, leader_member_id);
            self.0
                .borrow_mut()
                .on_session_event(0, cluster_session_id, 0, 0, code, detail);
        }

        fn on_new_leader(
            &mut self,
            cluster_session_id: i64,
            leadership_term_id: i64,
            leader_member_id: i32,
            ingress_endpoints: &str,
        ) {
            self.0.borrow_mut().on_new_leader(
                cluster_session_id,
                leadership_term_id,
                leader_member_id,
                ingress_endpoints,
            );
        }
    }

    struct Harness {
        session: AeronCluster,
        client: RecordingClient,
        clock: Rc<Cell<i64>>,
        listener: Rc<RefCell<RecordingListener>>,
        ingress: i64,
    }

    fn harness() -> Harness {
        let mut client = RecordingClient::new();
        let ingress = client.add_publication();
        client.open_image(IMAGE);
        client.adds_ready();

        let clock = Rc::new(Cell::new(1_000));
        let listener = Rc::new(RefCell::new(RecordingListener::default()));
        let context = AeronClusterContext::default()
            .ingress_channel(Some("aeron:ipc"))
            .egress_channel(Some("aeron:ipc"))
            .new_leader_timeout_ns(Some(NEW_LEADER_TIMEOUT_NS))
            .nano_clock(Box::new(TestClock(Rc::clone(&clock))))
            .egress_listener(Box::new(SharedListener(Rc::clone(&listener))));

        let session = AeronCluster::new(
            context,
            IngressPublication::Exclusive(ingress),
            EGRESS_SUBSCRIPTION,
            Some(IMAGE),
            CLUSTER_SESSION_ID,
            2,
            1,
        );

        Harness {
            session,
            client,
            clock,
            listener,
            ingress,
        }
    }

    fn advance(clock: &Cell<i64>, by: i64) {
        clock.set(clock.get() + by);
    }

    fn new_leader_event() -> Vec<u8> {
        let mut buffer = vec![0u8; 256];
        let length = {
            let encoder = NewLeaderEventEncoder::default()
                .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .cluster_session_id(CLUSTER_SESSION_ID)
                .leadership_term_id(3)
                .leader_member_id(2)
                .ingress_endpoints(INGRESS_ENDPOINTS.as_bytes());

            MESSAGE_HEADER_LENGTH + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    fn session_event(session_id: i64, code: EventCode) -> Vec<u8> {
        let mut buffer = vec![0u8; 256];
        let length = {
            let encoder = SessionEventEncoder::default()
                .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .cluster_session_id(session_id)
                .correlation_id(1)
                .leadership_term_id(2)
                .leader_member_id(1)
                .code(code)
                .leader_heartbeat_timeout_ns_opt(None)
                .detail(b"");

            MESSAGE_HEADER_LENGTH + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    fn session_message(session_id: i64, payload: &[u8]) -> Vec<u8> {
        use deepmsg_codec::cluster::session_message_header_codec::SessionMessageHeaderEncoder;

        let mut buffer = vec![0u8; SESSION_HEADER_LENGTH + payload.len()];
        {
            let encoder = SessionMessageHeaderEncoder::default()
                .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .cluster_session_id(session_id)
                .leadership_term_id(2)
                .timestamp(77);
        }
        buffer[SESSION_HEADER_LENGTH..].copy_from_slice(payload);
        buffer
    }

    fn poll(session: &mut AeronCluster, client: &mut RecordingClient, bytes: Vec<u8>) -> usize {
        client.push_egress(bytes);
        session.poll_egress_with(client).expect("the poll")
    }

    /// `shouldStayConnectedAfterSuccessfulFailover` (four combinations).
    #[test]
    fn a_session_stays_connected_across_a_successful_failover() {
        for (ingress_disconnects, app_messages) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let mut h = harness();
            if ingress_disconnects {
                h.client.disconnect(h.ingress);
            }

            let sent = if app_messages {
                h.session
                    .offer_with(&mut h.client, b"12345678")
                    .is_some_and(|result| matches!(result, Appended::Ok { .. }))
            } else {
                h.session.send_keep_alive_with(&mut h.client)
            };

            assert_eq!(!ingress_disconnects, sent);
            assert_eq!(
                if ingress_disconnects {
                    SessionState::AwaitNewLeader
                } else {
                    SessionState::Connected
                },
                h.session.state()
            );

            advance(&h.clock, NEW_LEADER_TIMEOUT_NS - 1);
            assert_eq!(
                1,
                poll(&mut h.session, &mut h.client, new_leader_event()),
                "the event is one fragment, and the poll counts fragments"
            );
            assert_eq!(SessionState::AwaitNewLeaderConnection, h.session.state());
            assert_eq!(
                vec![format!(
                    "new_leader({CLUSTER_SESSION_ID}, 3, 2, {INGRESS_ENDPOINTS})"
                )],
                h.listener.borrow().calls
            );

            assert_eq!(0, h.session.poll_egress_with(&mut h.client).expect("idle"));

            advance(&h.clock, 5_000_000_000 - 1);

            // The publication the new leader's add made is the last one.
            let new_publication = h.client.last_registration_id();
            h.client.connect(new_publication);

            let sent = if app_messages {
                h.session
                    .offer_with(&mut h.client, b"12345678")
                    .is_some_and(|result| matches!(result, Appended::Ok { .. }))
            } else {
                h.session.send_keep_alive_with(&mut h.client)
            };
            assert!(sent);
            assert_eq!(SessionState::Connected, h.session.state());

            advance(&h.clock, 1);
            assert_eq!(0, h.session.poll_egress_with(&mut h.client).expect("idle"));
            assert!(!h.session.is_closed());
        }
    }

    /// `shouldRetryNewLeaderIngressPublicationWhenSendChannelEndpointIsClosing`.
    #[test]
    fn a_closing_send_channel_endpoint_is_retried() {
        let mut h = harness();
        h.client.fail_next_add_polls(1);

        assert_eq!(1, poll(&mut h.session, &mut h.client, new_leader_event()));

        assert_eq!(SessionState::AwaitNewLeaderConnection, h.session.state());
        assert!(!h.session.is_closed());
        assert_eq!(
            vec![format!(
                "new_leader({CLUSTER_SESSION_ID}, 3, 2, {INGRESS_ENDPOINTS})"
            )],
            h.listener.borrow().calls
        );
    }

    /// `shouldCloseItselfWhenDisconnectedForLongerThanNewLeaderTimeout`.
    #[test]
    fn a_session_closes_when_disconnected_past_the_new_leader_timeout() {
        for app_messages in [false, true] {
            let mut h = harness();
            h.client.disconnect(h.ingress);

            if app_messages {
                h.session.offer_with(&mut h.client, b"12345678");
            } else {
                h.session.send_keep_alive_with(&mut h.client);
            }
            assert_eq!(SessionState::AwaitNewLeader, h.session.state());

            advance(&h.clock, NEW_LEADER_TIMEOUT_NS - 1);
            assert_eq!(0, h.session.poll_state_changes_with(&mut h.client));
            assert!(!h.session.is_closed());

            advance(&h.clock, 1);
            assert_eq!(1, h.session.poll_state_changes_with(&mut h.client));
            assert!(h.session.is_closed());
        }
    }

    /// `shouldCloseItselfWhenUnableToSendMessageForLongerThanNewLeaderConnectionTimeout`.
    #[test]
    fn a_session_closes_when_the_new_leader_never_connects() {
        for app_messages in [false, true] {
            let mut h = harness();
            h.client.disconnect(h.ingress);

            if app_messages {
                h.session.offer_with(&mut h.client, b"12345678");
            } else {
                h.session.send_keep_alive_with(&mut h.client);
            }

            advance(&h.clock, NEW_LEADER_TIMEOUT_NS / 2);
            poll(&mut h.session, &mut h.client, new_leader_event());
            assert_eq!(SessionState::AwaitNewLeaderConnection, h.session.state());
            assert!(!h.session.is_closed());

            advance(&h.clock, 5_000_000_000 - 1);
            if app_messages {
                h.session.offer_with(&mut h.client, b"12345678");
            } else {
                h.session.send_keep_alive_with(&mut h.client);
            }
            assert_eq!(SessionState::AwaitNewLeaderConnection, h.session.state());

            advance(&h.clock, 1);
            assert_eq!(1, h.session.poll_egress_with(&mut h.client).expect("due"));
            assert!(h.session.is_closed());
        }
    }

    /// `shouldCloseIngressPublicationWhenEgressImageCloses`: a lost image is a
    /// lost leader, in `CONNECTED` and in `AWAIT_NEW_LEADER_CONNECTION`.
    #[test]
    fn a_closed_egress_image_closes_the_ingress_publication() {
        let mut h = harness();

        // Connected.
        h.client.close_image(IMAGE);
        assert_eq!(1, h.session.poll_egress_with(&mut h.client).expect("lost"));
        assert_eq!(SessionState::AwaitNewLeader, h.session.state());
        assert_eq!(
            1,
            h.client
                .calls()
                .iter()
                .filter(|call| call.as_str() == format!("async_remove_publication({})", h.ingress))
                .count()
        );

        // The new leader's publication connects nothing yet, and its image
        // closes too.
        h.client.open_image(IMAGE);
        poll(&mut h.session, &mut h.client, new_leader_event());
        assert_eq!(SessionState::AwaitNewLeaderConnection, h.session.state());
        let new_publication = h.client.last_registration_id();

        h.client.close_image(IMAGE);
        assert_eq!(1, h.session.poll_egress_with(&mut h.client).expect("lost"));
        assert_eq!(SessionState::AwaitNewLeader, h.session.state());
        assert!(
            h.client
                .calls()
                .iter()
                .any(|call| call.as_str() == format!("async_remove_publication({new_publication})")),
            "the new leader's publication was given back: {:?}",
            h.client.calls()
        );
    }

    /// `shouldCloseItselfAfterReachingMaxPositionOnTheIngressPublication`.
    #[test]
    fn a_publication_at_its_maximum_position_pends_close() {
        let mut h = harness();
        h.client.offer_returns(Appended::MaxPositionExceeded);

        let result = h.session.offer_with(&mut h.client, b"12345678");

        assert_eq!(Some(Appended::MaxPositionExceeded), result);
        assert_eq!(SessionState::PendingClose, h.session.state());
        assert!(
            h.client
                .calls()
                .iter()
                .any(|call| call.as_str() == format!("async_remove_publication({})", h.ingress))
        );

        assert_eq!(1, h.session.poll_state_changes_with(&mut h.client));
        assert!(h.session.is_closed());
    }

    /// An offer is the session header and the payload, in that order, and the
    /// header carries the session and its term.
    #[test]
    fn an_offer_carries_the_session_header_before_the_payload() {
        let mut h = harness();

        h.session.offer_with(&mut h.client, b"hello");

        let offered = h.client.offered();
        assert_eq!(1, offered.len());
        let bytes = &offered[0];
        assert_eq!(SESSION_HEADER_LENGTH + 5, bytes.len());

        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);
        assert_eq!(SBE_SCHEMA_ID, header.schema_id());
        assert_eq!(1, header.template_id(), "SessionMessageHeader");

        let decoder = SessionMessageHeaderDecoder::default().header(header, 0);
        assert_eq!(CLUSTER_SESSION_ID, decoder.cluster_session_id());
        assert_eq!(2, decoder.leadership_term_id());
        assert_eq!(0, decoder.timestamp(), "the cluster stamps it");
        assert_eq!(b"hello", &bytes[SESSION_HEADER_LENGTH..]);
    }

    /// A keep-alive names the term and the session (`AC:492-505`).
    #[test]
    fn a_keep_alive_carries_the_term_and_the_session() {
        let mut h = harness();

        assert!(h.session.send_keep_alive_with(&mut h.client));

        let offered = h.client.offered();
        let bytes = &offered[0];
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);
        assert_eq!(
            deepmsg_codec::cluster::session_keep_alive_codec::SBE_TEMPLATE_ID,
            header.template_id(),
            "SessionKeepAlive"
        );

        let decoder = SessionKeepAliveDecoder::default().header(header, 0);
        assert_eq!(2, decoder.leadership_term_id());
        assert_eq!(CLUSTER_SESSION_ID, decoder.cluster_session_id());
    }

    /// A snapshot request carries the correlation id and the request type
    /// (`AC:541-560`).
    #[test]
    fn an_admin_request_asks_for_a_snapshot() {
        let mut h = harness();

        assert!(
            h.session
                .send_admin_request_to_take_a_snapshot_with(&mut h.client, 77)
        );

        let offered = h.client.offered();
        let bytes = &offered[0];
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);
        assert_eq!(
            deepmsg_codec::cluster::admin_request_codec::SBE_TEMPLATE_ID,
            header.template_id(),
            "AdminRequest"
        );

        let decoder = AdminRequestDecoder::default().header(header, 0);
        assert_eq!(2, decoder.leadership_term_id());
        assert_eq!(CLUSTER_SESSION_ID, decoder.cluster_session_id());
        assert_eq!(77, decoder.correlation_id());
        assert_eq!(AdminRequestType::SNAPSHOT, decoder.request_type());
    }

    /// A close tells the cluster first (`AC:290-311`, `:1129-1160`).
    #[test]
    fn a_close_sends_the_close_request_before_giving_the_resources_back() {
        let mut h = harness();

        h.session.close_with(&mut h.client);

        let offered = h.client.offered();
        assert_eq!(1, offered.len(), "the close request was offered");
        let bytes = &offered[0];
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);
        assert_eq!(
            deepmsg_codec::cluster::session_close_request_codec::SBE_TEMPLATE_ID,
            header.template_id(),
            "SessionCloseRequest"
        );

        let decoder = SessionCloseRequestDecoder::default().header(header, 0);
        assert_eq!(2, decoder.leadership_term_id());
        assert_eq!(CLUSTER_SESSION_ID, decoder.cluster_session_id());

        assert!(
            h.client
                .calls()
                .iter()
                .any(|call| call.as_str() == format!("async_remove_publication({})", h.ingress))
        );
        assert!(h.client.calls().iter().any(
            |call| call.as_str() == format!("async_remove_subscription({EGRESS_SUBSCRIPTION})")
        ));
        assert!(h.session.is_closed());
    }

    /// A service message is dispatched with its payload after the session
    /// header (`AC:872-892`).
    #[test]
    fn a_session_message_reaches_the_listener_with_its_payload() {
        let mut h = harness();

        poll(
            &mut h.session,
            &mut h.client,
            session_message(CLUSTER_SESSION_ID, b"from the service"),
        );

        assert_eq!(
            vec![format!(
                "message({CLUSTER_SESSION_ID}, 77, [102, 114, 111, 109, 32, 116, 104, 101, 32, \
                 115, 101, 114, 118, 105, 99, 101])"
            )],
            h.listener.borrow().calls
        );
    }

    /// A message for another session is not delivered (`AC:880-890`).
    #[test]
    fn a_session_message_for_another_session_is_dropped() {
        let mut h = harness();

        poll(
            &mut h.session,
            &mut h.client,
            session_message(CLUSTER_SESSION_ID + 1, b"not ours"),
        );

        assert!(h.listener.borrow().calls.is_empty());
    }

    /// `SessionEvent.CLOSED` pends the close, and the next state-change poll
    /// closes the session (`AC:906-909`, `:648-660`).
    #[test]
    fn a_closed_session_event_pends_the_close() {
        let mut h = harness();

        poll(
            &mut h.session,
            &mut h.client,
            session_event(CLUSTER_SESSION_ID, EventCode::CLOSED),
        );

        // The same poll runs the state-change check after the dispatch
        // (`AC:609`), so the close is due and taken in this call.
        assert!(h.session.is_closed());
        assert_eq!(0, h.session.poll_state_changes_with(&mut h.client));
    }

    /// A message with no listener to go to is the reference's configuration
    /// error, not a silent drop.
    #[test]
    fn a_message_with_no_listener_is_a_configuration_error() {
        let mut h = harness();
        let context = AeronClusterContext::default()
            .ingress_channel(Some("aeron:ipc"))
            .egress_channel(Some("aeron:ipc"));
        let mut session = AeronCluster::new(
            context,
            IngressPublication::Exclusive(h.ingress),
            EGRESS_SUBSCRIPTION,
            Some(IMAGE),
            CLUSTER_SESSION_ID,
            2,
            1,
        );

        h.client
            .push_egress(session_message(CLUSTER_SESSION_ID, b"hello"));

        let error = session
            .poll_egress_with(&mut h.client)
            .expect_err("no listener");

        assert_eq!(
            "ERROR - egressListener must be specified on AeronCluster.Context",
            error.to_string()
        );

        // A session event is not: the reference's default listener inherits
        // no-op callbacks for everything but `onMessage`.
        h.client
            .push_egress(session_event(CLUSTER_SESSION_ID, EventCode::OK));
        session
            .poll_egress_with(&mut h.client)
            .expect("an event with no listener is consumed");
    }
}
