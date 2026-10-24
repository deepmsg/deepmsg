//! What a service tells the consensus module (`ConsensusModuleProxy.java`).
//!
//! Seven messages on the control channel's consensus-module stream, and the
//! **sticky `SessionMessageHeader`** every message that carries one shares:
//!
//! | Here | Reference |
//! |---|---|
//! | [`ConsensusModuleProxy::schedule_timer`] | `ScheduleTimer` (31) — `:62-81` |
//! | [`ConsensusModuleProxy::cancel_timer`] | `CancelTimer` (32) — `:83-101` |
//! | [`ConsensusModuleProxy::offer_session_message`] | `Publication.offer` in `CSA.offer` — `CSA:659-707` |
//! | [`ConsensusModuleProxy::offer_service_message`] | `offer` — `CSA:382-397` |
//! | [`ConsensusModuleProxy::ack`] | `ServiceAck` (33) — `:147-170` |
//! | [`ConsensusModuleProxy::close_session`] | `CloseSession` (30) — `:172-196` |
//! | [`ConsensusModuleProxy::cluster_members_query`] | `ClusterMembersQuery` (34) — `:198-235` |
//!
//! # The sticky header
//!
//! The reference wraps **one** `SessionMessageHeaderEncoder` over one buffer in
//! the constructor (`CSA:191-193`) and only ever sets the fields the message
//! needs: `leadershipTermId` at recovery and on every `NewLeadershipTermEvent`
//! (`CSA:582-614`), `timestamp` when an egress message is offered
//! (`CSA:694-706`), `clusterSessionId` for both kinds. Fields a message does not
//! set therefore carry the byte value the previous message left, and that is
//! observable: a service message's timestamp is the last egress timestamp (0
//! after startup). This struct keeps the three values and re-encodes the 32-byte
//! header from them, which is the same bytes with a visible source of truth.
//!
//! # Failure semantics
//!
//! `ConsensusModuleProxy.checkResult` (`:227-235`) throws for `NOT_CONNECTED`,
//! `CLOSED` and `MAX_POSITION_EXCEEDED`; everything else is returned to the
//! caller. That split survives here as `Err` versus `Ok(false)`.

use deepmsg_client::client::Client;
use deepmsg_client::publication::{Claim, Publication};
use deepmsg_codec_cluster::WriteBuf;
use deepmsg_codec_cluster::cancel_timer_codec::{
    CancelTimerEncoder, SBE_BLOCK_LENGTH as CANCEL_TIMER_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::close_session_codec::{
    CloseSessionEncoder, SBE_BLOCK_LENGTH as CLOSE_SESSION_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::cluster_members_query_codec::{
    ClusterMembersQueryEncoder, SBE_BLOCK_LENGTH as MEMBERS_QUERY_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::schedule_timer_codec::{
    SBE_BLOCK_LENGTH as SCHEDULE_TIMER_BLOCK_LENGTH, ScheduleTimerEncoder,
};
use deepmsg_codec_cluster::service_ack_codec::{
    SBE_BLOCK_LENGTH as SERVICE_ACK_BLOCK_LENGTH, ServiceAckEncoder,
};
use deepmsg_codec_cluster::session_message_header_codec::{
    SBE_BLOCK_LENGTH as SESSION_MESSAGE_HEADER_BLOCK_LENGTH, SessionMessageHeaderEncoder,
};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH;
use deepmsg_driver::publication_params::MAX_UDP_PAYLOAD_LENGTH;

use crate::error::ContainerError;

/// `ServiceProxy.SEND_ATTEMPTS` (`ServiceProxy.java:28`) — the container uses
/// the same five for every send.
pub const SEND_ATTEMPTS: usize = 5;

/// `AeronCluster.SESSION_HEADER_LENGTH`: one SBE header plus
/// `SessionMessageHeader`'s block.
pub const SESSION_HEADER_LENGTH: usize =
    MESSAGE_HEADER_LENGTH + SESSION_MESSAGE_HEADER_BLOCK_LENGTH as usize;

/// What a follower's mocked claim may hold
/// (`ConsensusModuleProxy` has no such bound; the agent's) — the reference's
/// `headerBuffer.capacity() - SESSION_HEADER_LENGTH`, where that buffer is the
/// message buffer less one frame header (`CSA:152-154`).
const MAX_CLAIMED_PAYLOAD: usize =
    MAX_UDP_PAYLOAD_LENGTH as usize - DATA_HEADER_LENGTH - SESSION_HEADER_LENGTH;

/// The bytes of a `SessionMessageHeader`, and the values still to be written.
///
/// The reference's buffer starts zeroed and is then written in place; the
/// initial values here are therefore `0`, and `leadershipTermId` is
/// `NULL_VALUE` only after a recovery read says so.
#[derive(Clone, Copy, Debug, Default)]
pub struct SessionHeader {
    /// `SessionMessageHeader.leadershipTermId`.
    pub leadership_term_id: i64,
    /// `SessionMessageHeader.clusterSessionId`.
    pub cluster_session_id: i64,
    /// `SessionMessageHeader.timestamp`.
    pub timestamp: i64,
}

/// A claimed message, as the proxy hands it over.
///
/// The sticky header is already written into it; what follows is the caller's.
pub enum Claimed<'a> {
    /// A claim in the term. [`Claimed::Term`] is published by the caller's
    /// `Frame::publish`, and until then a reader stops at its negative length.
    Term(Claim<'a>),
    /// A follower's scratch window (`CSA:713-731`): the caller writes it and
    /// **nothing is sent**. The reference's mocked claim, whose position is
    /// `ClientSession.MOCKED_OFFER` (`ClientSession.java:32`).
    Mocked(&'a mut [u8]),
}

impl core::fmt::Debug for Claimed<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Term(claim) => f
                .debug_struct("Term")
                .field("offset", &claim.offset())
                .field("position", &claim.position())
                .finish(),
            Self::Mocked(bytes) => f.debug_tuple("Mocked").field(&bytes.len()).finish(),
        }
    }
}

/// `ConsensusModuleProxy` (`ConsensusModuleProxy.java`), on a non-exclusive
/// publication the client owns.
pub struct ConsensusModuleProxy {
    publication_registration_id: Option<i64>,
    service_id: i32,
    header: SessionHeader,
    buffer: Vec<u8>,
}

impl ConsensusModuleProxy {
    /// A proxy that has not been given its publication yet.
    pub fn new(service_id: i32) -> Self {
        Self {
            publication_registration_id: None,
            service_id,
            header: SessionHeader::default(),
            buffer: vec![0_u8; SESSION_HEADER_LENGTH],
        }
    }

    /// The publication the client made on stream 105
    /// (`CSA:192` — `aeron.addPublication(controlChannel, consensusModuleStreamId)`).
    pub fn set_publication(&mut self, registration_id: i64) {
        self.publication_registration_id = Some(registration_id);
    }

    /// The sticky header, for the byte tests and the agent's recovery read.
    pub const fn header(&self) -> SessionHeader {
        self.header
    }

    /// `sessionMessageHeaderEncoder.leadershipTermId(...)` (`CSA:590`, `:775`).
    pub fn set_leadership_term_id(&mut self, leadership_term_id: i64) {
        self.header.leadership_term_id = leadership_term_id;
    }

    /// `scheduleTimer` (`:62-81`).
    ///
    /// # Errors
    ///
    /// [`ContainerError::Cluster`] for a publication that is not connected,
    /// closed or at its maximum position — the reference's `checkResult`.
    pub fn schedule_timer(
        &mut self,
        client: &Client,
        correlation_id: i64,
        deadline: i64,
    ) -> Result<bool, ContainerError> {
        let length = MESSAGE_HEADER_LENGTH + usize::from(SCHEDULE_TIMER_BLOCK_LENGTH);
        self.resize(length);
        {
            let encoder = ScheduleTimerEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message.correlation_id(correlation_id).deadline(deadline);
        }
        self.send(client, length)
    }

    /// `cancelTimer` (`:83-101`).
    ///
    /// # Errors
    ///
    /// As [`Self::schedule_timer`].
    pub fn cancel_timer(
        &mut self,
        client: &Client,
        correlation_id: i64,
    ) -> Result<bool, ContainerError> {
        let length = MESSAGE_HEADER_LENGTH + usize::from(CANCEL_TIMER_BLOCK_LENGTH);
        self.resize(length);
        {
            let encoder = CancelTimerEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message.correlation_id(correlation_id);
        }
        self.send(client, length)
    }

    /// `ServiceAck` (`:147-170`).
    ///
    /// # Errors
    ///
    /// As [`Self::schedule_timer`].
    pub fn ack(
        &mut self,
        client: &Client,
        log_position: i64,
        timestamp: i64,
        ack_id: i64,
        relevant_id: i64,
    ) -> Result<bool, ContainerError> {
        let length = MESSAGE_HEADER_LENGTH + usize::from(SERVICE_ACK_BLOCK_LENGTH);
        self.resize(length);
        {
            let encoder = ServiceAckEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .log_position(log_position)
                .timestamp(timestamp)
                .ack_id(ack_id)
                .relevant_id(relevant_id)
                .service_id(self.service_id);
        }
        self.send(client, length)
    }

    /// `closeSession` (`:172-196`).
    ///
    /// # Errors
    ///
    /// As [`Self::schedule_timer`].
    pub fn close_session(
        &mut self,
        client: &Client,
        cluster_session_id: i64,
    ) -> Result<bool, ContainerError> {
        let length = MESSAGE_HEADER_LENGTH + usize::from(CLOSE_SESSION_BLOCK_LENGTH);
        self.resize(length);
        {
            let encoder = CloseSessionEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message.cluster_session_id(cluster_session_id);
        }
        self.send(client, length)
    }

    /// `clusterMembersQuery` (`:198-235`). The reference marks it `extended`;
    /// nothing in the container consumes the answer (`ServiceAdapter` has no
    /// arm for 41/43), and that is the reference's shape too.
    ///
    /// # Errors
    ///
    /// As [`Self::schedule_timer`].
    pub fn cluster_members_query(
        &mut self,
        client: &Client,
        correlation_id: i64,
    ) -> Result<bool, ContainerError> {
        let length = MESSAGE_HEADER_LENGTH + usize::from(MEMBERS_QUERY_BLOCK_LENGTH);
        self.resize(length);
        {
            let encoder = ClusterMembersQueryEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .correlation_id(correlation_id)
                .extended(deepmsg_codec_cluster::boolean_type::BooleanType::TRUE);
        }
        self.send(client, length)
    }

    /// A **service message**: the payload goes to the consensus module with
    /// `clusterSessionId = serviceId` and the sticky timestamp untouched
    /// (`CSA:382-397`).
    ///
    /// # Errors
    ///
    /// As [`Self::schedule_timer`] for the send; a payload too large for one
    /// message is `MAX_POSITION_EXCEEDED`, which the reference reports the same
    /// way from `Publication.offer`.
    pub fn offer_service_message(
        &mut self,
        client: &Client,
        payload: &[u8],
    ) -> Result<i64, ContainerError> {
        self.header.cluster_session_id = i64::from(self.service_id);
        self.offer(client, payload)
    }

    /// The same service message, made of parts that must land as one message
    /// (`Cluster.offer(DirectBufferVector[])`, `Cluster.java:332`).
    ///
    /// The sticky header is offered as the leading slice, exactly as the
    /// reference puts its header vector at the front of the caller's array
    /// (`CSA:382-397`), and the parts follow it with no buffer joining them.
    ///
    /// # Errors
    ///
    /// As [`Self::offer_service_message`].
    pub fn offer_service_message_vectored(
        &mut self,
        client: &Client,
        parts: &[&[u8]],
    ) -> Result<i64, ContainerError> {
        self.header.cluster_session_id = i64::from(self.service_id);
        let position = self.offer_vectored(client, parts);
        if position < 0 {
            check_result(client, self.publication_registration_id, position)?;
        }
        Ok(position)
    }

    /// An **egress message**: `clusterSessionId` is the session and `timestamp`
    /// is the container's log time (`CSA:694-706`).
    ///
    /// The sticky header lives here, but the **publication does not**: egress
    /// goes out on the session's own publication (102 in the smoke), which the
    /// caller names by registration id. Writing it here and not on the
    /// consensus module's stream (105) is the difference between an echo and a
    /// service message.
    pub fn offer_session_message(
        &mut self,
        client: &Client,
        egress_publication_registration_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        payload: &[u8],
    ) -> i64 {
        self.header.cluster_session_id = cluster_session_id;
        self.header.timestamp = timestamp;
        let length = self.encode_session_message(payload);

        let Some(publication) = client.publication(egress_publication_registration_id) else {
            return OFFER_NOT_CONNECTED;
        };

        self.offer_on(client, publication, length)
    }

    /// The same egress message, made of parts that must land as one message
    /// (`ClientSession.offer(DirectBufferVector[])`, `ClientSession.java:96`).
    ///
    /// The sticky header goes out as the leading slice, as it does in the
    /// single-buffer path — `encode_header` writes it into the resident buffer,
    /// which is what makes this an offer of parts rather than a join.
    pub fn offer_session_message_vectored(
        &mut self,
        client: &Client,
        egress_publication_registration_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        parts: &[&[u8]],
    ) -> i64 {
        self.header.cluster_session_id = cluster_session_id;
        self.header.timestamp = timestamp;
        self.encode_header();

        let Some(publication) = client.publication(egress_publication_registration_id) else {
            return OFFER_NOT_CONNECTED;
        };

        self.offer_slices_on(client, publication, parts)
    }

    /// Claim a **service message** of `length` payload bytes
    /// (`Cluster.tryClaim`, `Cluster.java:379`).
    ///
    /// The claim is `length + SESSION_HEADER_LENGTH`, and the sticky header is
    /// written into its front — the reference's
    /// `tryClaim(length + SESSION_HEADER_LENGTH, claim, header)`
    /// (`ConsensusModuleProxy.java:132-146`).
    ///
    /// # Errors
    ///
    /// One of the negative `Publication` codes, exactly as the reference's
    /// `tryClaim` answers. A "try again" answer is not retried here: the
    /// reference does not retry a caller's claim either.
    pub fn claim_service_message<'c>(
        &'c mut self,
        client: &'c Client,
        length: usize,
    ) -> Result<Claimed<'c>, i64> {
        self.header.cluster_session_id = i64::from(self.service_id);
        self.claim_on(client, length)
    }

    /// Claim an **egress message** of `length` payload bytes on the session's
    /// own publication (`CSA:708-736`, `ClientSession.java:133`).
    ///
    /// # Errors
    ///
    /// As [`Self::claim_service_message`], plus [`OFFER_NOT_CONNECTED`] for a
    /// session whose publication is not there yet.
    pub fn claim_session_message<'c>(
        &'c mut self,
        client: &'c Client,
        egress_publication_registration_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        length: usize,
    ) -> Result<Claimed<'c>, i64> {
        self.header.cluster_session_id = cluster_session_id;
        self.header.timestamp = timestamp;

        let Some(publication) = client.publication(egress_publication_registration_id) else {
            return Err(OFFER_NOT_CONNECTED);
        };

        self.claim_on_publication(client, publication, length)
    }

    /// A follower's claim: a window over the resident buffer, and nothing sent
    /// (`CSA:713-731`).
    ///
    /// # Errors
    ///
    /// [`ContainerError::Cluster`] for a claim longer than the reference's
    /// buffer allows — its `IllegalArgumentException`, with its words.
    pub fn mock_session_message(
        &mut self,
        cluster_session_id: i64,
        timestamp: i64,
        length: usize,
    ) -> Result<Claimed<'_>, ContainerError> {
        if length > MAX_CLAIMED_PAYLOAD {
            return Err(ContainerError::Cluster(format!(
                "claim exceeds maxPayloadLength={MAX_CLAIMED_PAYLOAD}, length={length}"
            )));
        }

        self.header.cluster_session_id = cluster_session_id;
        self.header.timestamp = timestamp;
        self.resize(SESSION_HEADER_LENGTH + length);
        self.encode_header();

        let start = SESSION_HEADER_LENGTH;
        Ok(Claimed::Mocked(&mut self.buffer[start..start + length]))
    }

    fn claim_on<'c>(&'c mut self, client: &'c Client, length: usize) -> Result<Claimed<'c>, i64> {
        let Some(publication) = self.publication(client) else {
            return Err(OFFER_NOT_CONNECTED);
        };

        self.claim_on_publication(client, publication, length)
    }

    fn claim_on_publication<'c>(
        &'c mut self,
        client: &'c Client,
        publication: &'c Publication,
        length: usize,
    ) -> Result<Claimed<'c>, i64> {
        let Some(total) = SESSION_HEADER_LENGTH.checked_add(length) else {
            return Err(OFFER_MAX_POSITION_EXCEEDED);
        };

        let mut claim = match publication.try_claim(position_limit(client, publication), total) {
            Ok(claim) => claim,
            Err(appended) => return Err(claim_refusal(appended)),
        };

        if let Some(slot) = claim.payload_mut(0, SESSION_HEADER_LENGTH) {
            encode_session_header(self.header, slot);
        }

        Ok(Claimed::Term(claim))
    }

    /// Encode the sticky header into `buffer[..SESSION_HEADER_LENGTH]`.
    fn encode_header(&mut self) {
        encode_session_header(self.header, &mut self.buffer[..SESSION_HEADER_LENGTH]);
    }

    fn offer(&mut self, client: &Client, payload: &[u8]) -> Result<i64, ContainerError> {
        let position = self.offer_position(client, payload);
        if position < 0 {
            check_result(client, self.publication_registration_id, position)?;
        }
        Ok(position)
    }

    /// Encode the sticky header and the payload; returns the message length.
    ///
    /// Separated from the offer so the bytes can be asserted without a driver
    /// (the sticky-header contract is the thing P3-2b's snapshot compares).
    fn encode_session_message(&mut self, payload: &[u8]) -> usize {
        let length = SESSION_HEADER_LENGTH + payload.len();
        self.resize(length);
        self.encode_header();
        self.buffer[SESSION_HEADER_LENGTH..length].copy_from_slice(payload);
        length
    }

    fn offer_position(&mut self, client: &Client, payload: &[u8]) -> i64 {
        let length = self.encode_session_message(payload);

        let Some(publication) = self.publication(client) else {
            return OFFER_NOT_CONNECTED;
        };

        self.offer_on(client, publication, length)
    }

    /// The sticky header, then `parts`, offered on the consensus module's
    /// publication as one message.
    ///
    /// Nothing is encoded past the header: the parts are the caller's, and the
    /// term receives them back to back with it (`Publication.offer_slices`).
    fn offer_vectored(&mut self, client: &Client, parts: &[&[u8]]) -> i64 {
        self.encode_header();

        let Some(publication) = self.publication(client) else {
            return OFFER_NOT_CONNECTED;
        };

        self.offer_slices_on(client, publication, parts)
    }

    /// Offer the first `length` bytes of the buffer on `publication`, retried
    /// [`SEND_ATTEMPTS`] times, and answer with the reference's code.
    fn offer_on(&self, client: &Client, publication: &Publication, length: usize) -> i64 {
        self.offer_with(client, publication, |limit| {
            publication.offer(limit, &self.buffer[..length])
        })
    }

    /// The same, with the message made of the sticky header and `parts`.
    fn offer_slices_on(&self, client: &Client, publication: &Publication, parts: &[&[u8]]) -> i64 {
        self.offer_with(client, publication, |limit| {
            publication.offer_slices(limit, &self.buffer[..SESSION_HEADER_LENGTH], parts)
        })
    }

    /// The retry the three offers share: the limit is read fresh on every
    /// attempt, and the answer is the reference's code.
    fn offer_with<F>(&self, client: &Client, publication: &Publication, append: F) -> i64
    where
        F: Fn(i64) -> Appended,
    {
        for _ in 0..SEND_ATTEMPTS {
            match append(position_limit(client, publication)) {
                Appended::Ok { position, .. } => return position.raw(),
                Appended::EndOfLog | Appended::MidRotation | Appended::BackPressured => {}
                Appended::NotConnected => return OFFER_NOT_CONNECTED,
                Appended::MaxPositionExceeded => return OFFER_MAX_POSITION_EXCEEDED,
                Appended::MessageTooLarge | Appended::Malformed => {
                    return OFFER_MAX_POSITION_EXCEEDED;
                }
            }
        }

        OFFER_BACK_PRESSURED
    }

    /// One of the fixed messages: encode-then-offer, retried five times
    /// (`ConsensusModuleProxy.java`'s `tryClaim` loop).
    fn send(&self, client: &Client, length: usize) -> Result<bool, ContainerError> {
        let Some(publication) = self.publication(client) else {
            return Err(ContainerError::Cluster(
                "publication is not connected".to_owned(),
            ));
        };

        for _ in 0..SEND_ATTEMPTS {
            match publication.offer(position_limit(client, publication), &self.buffer[..length]) {
                Appended::Ok { .. } => return Ok(true),
                Appended::EndOfLog | Appended::MidRotation | Appended::BackPressured => {}
                Appended::NotConnected => {
                    return Err(ContainerError::Cluster(
                        "publication is not connected".to_owned(),
                    ));
                }
                Appended::MaxPositionExceeded => {
                    return Err(ContainerError::Cluster(format!(
                        "publication at max position: term-length={}",
                        publication.term_buffer_length()
                    )));
                }
                Appended::MessageTooLarge | Appended::Malformed => {
                    return Err(ContainerError::Cluster(
                        "publication refused the message".to_owned(),
                    ));
                }
            }
        }

        Ok(false)
    }

    fn publication<'a>(&self, client: &'a Client) -> Option<&'a Publication> {
        client.publication(self.publication_registration_id?)
    }

    fn resize(&mut self, length: usize) {
        if self.buffer.len() < length {
            self.buffer.resize(length, 0);
        }
    }
}

/// `Publication.positionLimit`'s counter, read fresh.
pub fn position_limit(client: &Client, publication: &Publication) -> i64 {
    client
        .counters_reader()
        .and_then(|counters| counters.value(publication.position_limit_counter_id()))
        .unwrap_or(0)
}

/// Write the sticky header into `slot`, which must be exactly
/// [`SESSION_HEADER_LENGTH`] bytes.
///
/// Free of the proxy so a **claimed** message can carry the same header the
/// offered one does: the claim hands back term memory, and the header goes into
/// it where the message will lie rather than through the resident buffer.
fn encode_session_header(header: SessionHeader, slot: &mut [u8]) {
    let encoder =
        SessionMessageHeaderEncoder::default().wrap(WriteBuf::new(slot), MESSAGE_HEADER_LENGTH);
    let mut encoder_header = encoder.header(0);
    let mut message = encoder_header.parent().expect("the encoder just wrapped");
    message
        .leadership_term_id(header.leadership_term_id)
        .cluster_session_id(header.cluster_session_id)
        .timestamp(header.timestamp);
}

/// What a refused claim answers: the reference's `Publication.tryClaim` codes.
fn claim_refusal(appended: Appended) -> i64 {
    match appended {
        Appended::NotConnected => OFFER_NOT_CONNECTED,
        Appended::Malformed | Appended::MessageTooLarge | Appended::MaxPositionExceeded => {
            OFFER_MAX_POSITION_EXCEEDED
        }
        Appended::EndOfLog | Appended::MidRotation => OFFER_ADMIN_ACTION,
        Appended::BackPressured | Appended::Ok { .. } => OFFER_BACK_PRESSURED,
    }
}

/// The reference's `checkResult` (`:227-235`): the three states that throw.
fn check_result(
    client: &Client,
    registration_id: Option<i64>,
    position: i64,
) -> Result<(), ContainerError> {
    match position {
        OFFER_NOT_CONNECTED => Err(ContainerError::Cluster(
            "publication is not connected".to_owned(),
        )),
        OFFER_CLOSED => Err(ContainerError::Cluster("publication is closed".to_owned())),
        OFFER_MAX_POSITION_EXCEEDED => {
            let term_length = registration_id
                .and_then(|id| client.publication(id))
                .map_or(0, Publication::term_buffer_length);
            Err(ContainerError::Cluster(format!(
                "publication at max position: term-length={term_length}"
            )))
        }
        _ => Ok(()),
    }
}

/// The two `Publication.tryClaim` answers the reference **returns** rather
/// than throws (`ConsensusModuleProxy.java:227-235`): no window right now, and
/// another producer is mid-rotation. Both mean "try again".
pub(crate) const fn is_retryable_claim(code: i64) -> bool {
    code == OFFER_BACK_PRESSURED || code == OFFER_ADMIN_ACTION
}

/// The same three states as a value, for the caller that has already
/// classified the other two as retries — a claim's `BACK_PRESSURED` and
/// `ADMIN_ACTION` (`ConsensusModuleProxy.java:227-235`).
pub(crate) fn check_result_code(client: &Client, code: i64) -> ContainerError {
    match check_result(client, None, code) {
        Err(error) => error,
        Ok(()) => ContainerError::Cluster(format!("publication refused the claim: {code}")),
    }
}

/// `Publication.NOT_CONNECTED`.
pub const OFFER_NOT_CONNECTED: i64 = -1;
/// `Publication.BACK_PRESSURED`.
pub const OFFER_BACK_PRESSURED: i64 = -2;
/// `Publication.ADMIN_ACTION`.
pub const OFFER_ADMIN_ACTION: i64 = -3;
/// `Publication.CLOSED`.
pub const OFFER_CLOSED: i64 = -4;
/// `Publication.MAX_POSITION_EXCEEDED`.
pub const OFFER_MAX_POSITION_EXCEEDED: i64 = -5;
/// `ClientSession.MOCKED_OFFER` (`ClientSession.java:32`).
pub const MOCKED_OFFER: i64 = 1;

#[cfg(test)]
mod tests {
    use super::*;

    fn i64_at(bytes: &[u8], offset: usize) -> i64 {
        i64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("eight bytes"))
    }

    fn u16_at(bytes: &[u8], offset: usize) -> u16 {
        u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("two bytes"))
    }

    /// The vectored path writes the same sticky header as the single-buffer
    /// one, and **copies none of the parts**: the header is offered as the
    /// leading slice (`CSA:382-397`), which is what makes this an offer of
    /// parts rather than a join.
    #[test]
    fn the_vectored_offer_prepares_the_header_and_copies_no_part() {
        let mut proxy = ConsensusModuleProxy::new(4);
        proxy.set_leadership_term_id(9);
        proxy.header.cluster_session_id = 21;
        proxy.header.timestamp = 5_000;

        proxy.encode_header();

        assert_eq!(
            SESSION_HEADER_LENGTH,
            proxy.buffer.len(),
            "the parts are offered from the caller's slices, not copied in"
        );
        assert_eq!(i64_at(&proxy.buffer, 8), 9, "leadershipTermId");
        assert_eq!(i64_at(&proxy.buffer, 16), 21, "clusterSessionId");
        assert_eq!(i64_at(&proxy.buffer, 24), 5_000, "timestamp");
    }

    /// A follower's claim is a window over the resident buffer: the sticky
    /// header is in front of it, the caller's bytes follow, and nothing was
    /// offered (`CSA:713-731`).
    #[test]
    fn a_follower_claim_writes_into_the_resident_buffer() {
        let mut proxy = ConsensusModuleProxy::new(4);
        proxy.set_leadership_term_id(9);

        let mut claimed = proxy
            .mock_session_message(21, 5_000, 6)
            .expect("six bytes fit the resident buffer");

        {
            let Claimed::Mocked(window) = &mut claimed else {
                panic!("a follower's claim is never in the log");
            };
            assert_eq!(6, window.len());
            window.copy_from_slice(b"abcdef");
        }

        assert_eq!(i64_at(&proxy.buffer, 8), 9, "leadershipTermId");
        assert_eq!(i64_at(&proxy.buffer, 16), 21, "clusterSessionId");
        assert_eq!(i64_at(&proxy.buffer, 24), 5_000, "timestamp");
        assert_eq!(
            b"abcdef",
            &proxy.buffer[SESSION_HEADER_LENGTH..SESSION_HEADER_LENGTH + 6],
            "the payload lies just past the header"
        );
    }

    /// Past what the reference's resident buffer can hold, its words.
    #[test]
    fn a_follower_claim_past_the_buffer_is_refused_with_the_references_words() {
        let mut proxy = ConsensusModuleProxy::new(4);

        let error = proxy
            .mock_session_message(21, 5_000, MAX_CLAIMED_PAYLOAD + 1)
            .expect_err("one byte too many");

        let ContainerError::Cluster(message) = error else {
            panic!("a claim past the buffer is the reference's ClusterException");
        };
        assert_eq!(
            format!(
                "claim exceeds maxPayloadLength={MAX_CLAIMED_PAYLOAD}, length={}",
                MAX_CLAIMED_PAYLOAD + 1
            ),
            message
        );
    }

    /// The sticky header's bytes: one schema-111 message header and the three
    /// fields, in the order `SessionMessageHeaderEncoder` writes them.
    #[test]
    fn the_session_header_is_one_sbe_header_and_three_fields() {
        let mut proxy = ConsensusModuleProxy::new(3);
        proxy.set_leadership_term_id(7);
        proxy.header.cluster_session_id = 42;
        proxy.header.timestamp = 99;

        let length = proxy.encode_session_message(b"hi");
        assert_eq!(length, SESSION_HEADER_LENGTH + 2);

        let bytes = &proxy.buffer[..length];
        assert_eq!(u16_at(bytes, 0), SESSION_MESSAGE_HEADER_BLOCK_LENGTH);
        assert_eq!(u16_at(bytes, 2), 1, "SessionMessageHeader's template id");
        assert_eq!(u16_at(bytes, 4), 111, "schema 111");
        assert_eq!(u16_at(bytes, 6), 17, "schema version");
        assert_eq!(i64_at(bytes, 8), 7);
        assert_eq!(i64_at(bytes, 16), 42);
        assert_eq!(i64_at(bytes, 24), 99);
        assert_eq!(&bytes[SESSION_HEADER_LENGTH..], b"hi");
    }

    /// A service message sets `clusterSessionId` to the service id and **does
    /// not touch the timestamp** (`CSA:382-397`), so it carries the last egress
    /// timestamp — 0 after startup, and that is observable.
    #[test]
    fn a_service_message_keeps_the_last_egress_timestamp() {
        let mut proxy = ConsensusModuleProxy::new(5);
        proxy.set_leadership_term_id(2);

        proxy.header.cluster_session_id = 11;
        proxy.header.timestamp = 1_234;
        let _ = proxy.encode_session_message(b"egress");

        proxy.header.cluster_session_id = i64::from(proxy.service_id);
        let length = proxy.encode_session_message(b"service");
        let bytes = &proxy.buffer[..length];

        assert_eq!(i64_at(bytes, 16), 5, "clusterSessionId is the service id");
        assert_eq!(
            i64_at(bytes, 24),
            1_234,
            "the timestamp was not written by the service message"
        );
    }

    /// `ServiceAck`'s five fields, in the reference's order
    /// (`ConsensusModuleProxy.java:147-170`).
    #[test]
    fn the_ack_carries_its_five_fields_in_order() {
        let mut proxy = ConsensusModuleProxy::new(0);
        proxy.resize(MESSAGE_HEADER_LENGTH + usize::from(SERVICE_ACK_BLOCK_LENGTH));
        let length = {
            let encoder = ServiceAckEncoder::default().wrap(
                WriteBuf::new(
                    &mut proxy.buffer
                        [..MESSAGE_HEADER_LENGTH + usize::from(SERVICE_ACK_BLOCK_LENGTH)],
                ),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .log_position(101)
                .timestamp(202)
                .ack_id(3)
                .relevant_id(404)
                .service_id(5);
            MESSAGE_HEADER_LENGTH + message.encoded_length()
        };

        let bytes = &proxy.buffer[..length];
        assert_eq!(u16_at(bytes, 2), 33, "ServiceAck's template id");
        assert_eq!(i64_at(bytes, 8), 101);
        assert_eq!(i64_at(bytes, 16), 202);
        assert_eq!(i64_at(bytes, 24), 3);
        assert_eq!(i64_at(bytes, 32), 404);
        assert_eq!(
            i32::from_le_bytes(bytes[40..44].try_into().expect("four")),
            5
        );
    }
}
