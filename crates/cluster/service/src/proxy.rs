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
use deepmsg_client::publication::Publication;
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

use crate::error::ContainerError;

/// `ServiceProxy.SEND_ATTEMPTS` (`ServiceProxy.java:28`) — the container uses
/// the same five for every send.
pub const SEND_ATTEMPTS: usize = 5;

/// `AeronCluster.SESSION_HEADER_LENGTH`: one SBE header plus
/// `SessionMessageHeader`'s block.
pub const SESSION_HEADER_LENGTH: usize =
    MESSAGE_HEADER_LENGTH + SESSION_MESSAGE_HEADER_BLOCK_LENGTH as usize;

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

    /// Encode the sticky header into `buffer[..SESSION_HEADER_LENGTH]`.
    fn encode_header(&mut self) {
        let encoder = SessionMessageHeaderEncoder::default().wrap(
            WriteBuf::new(&mut self.buffer[..SESSION_HEADER_LENGTH]),
            MESSAGE_HEADER_LENGTH,
        );
        let mut header = encoder.header(0);
        let mut message = header.parent().expect("the encoder just wrapped");
        message
            .leadership_term_id(self.header.leadership_term_id)
            .cluster_session_id(self.header.cluster_session_id)
            .timestamp(self.header.timestamp);
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

    /// Offer the first `length` bytes of the buffer on `publication`, retried
    /// [`SEND_ATTEMPTS`] times, and answer with the reference's code.
    fn offer_on(&self, client: &Client, publication: &Publication, length: usize) -> i64 {
        for _ in 0..SEND_ATTEMPTS {
            match publication.offer(position_limit(client, publication), &self.buffer[..length]) {
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
