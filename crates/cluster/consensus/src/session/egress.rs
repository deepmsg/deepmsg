//! What the cluster says back to a client (`EgressPublisher.java`).
//!
//! Four messages (`SessionEvent`, `Challenge`, `NewLeaderEvent`,
//! `AdminResponse`), each with two paths and three attempts:
//!
//! * if the encoded message fits the response publication's maximum payload, it
//!   is claimed (zero copy in the reference; a claim and one write here);
//! * otherwise it is encoded into a reusable buffer and offered.
//!
//! A negative answer is simply retried; when the three attempts are gone the
//! call reports `false` and the session manager keeps the session where it is,
//! so the same message is tried again on the next slow tick.
//!
//! # Why a trait for the session
//!
//! `EgressPublisherTest` drives the publisher with a mocked `ClusterSession`
//! (`EgressPublisherTest.java:64-296`) and asserts **which** path was taken and
//! how many times. [`EgressSession`] is that mock surface: the id, the
//! correlation id, the maximum payload length, and the two writes.

use deepmsg_codec_cluster::WriteBuf;
use deepmsg_codec_cluster::admin_request_type::AdminRequestType;
use deepmsg_codec_cluster::admin_response_code::AdminResponseCode;
use deepmsg_codec_cluster::admin_response_codec::{
    AdminResponseEncoder, SBE_BLOCK_LENGTH as ADMIN_RESPONSE_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::challenge_codec::{
    ChallengeEncoder, SBE_BLOCK_LENGTH as CHALLENGE_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::new_leader_event_codec::{
    NewLeaderEventEncoder, SBE_BLOCK_LENGTH as NEW_LEADER_EVENT_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::session_event_codec::{
    SBE_BLOCK_LENGTH as SESSION_EVENT_BLOCK_LENGTH, SessionEventEncoder,
};

/// `EgressPublisher.SEND_ATTEMPTS` (`EgressPublisher.java:38`).
const SEND_ATTEMPTS: usize = 3;

/// `EgressPublisher`'s reusable buffer starts at
/// `MAX_ENCODED_MEMBERSHIP_QUERY_LENGTH` = 4 KiB (`ClusterSession.java:50`,
/// `EgressPublisher.java:41`).
const INITIAL_BUFFER_LENGTH: usize = 4 * 1024;

/// Every variable-length field in schema 111 is a 4-byte length followed by the
/// bytes (`varAsciiEncoding` / `varDataEncoding`).
const VAR_DATA_HEADER_LENGTH: usize = 4;

/// A session seen as an egress destination (`ClusterSession.responsePublication`,
/// `tryClaim`, `offer`).
pub trait EgressSession {
    /// The cluster session id the messages carry.
    fn id(&self) -> i64;

    /// The correlation id the client's last request carried.
    fn correlation_id(&self) -> i64;

    /// The response publication's maximum payload, or `None` when there is no
    /// publication.
    fn max_payload_length(&self) -> Option<usize>;

    /// Write a claimed message of `length` bytes; `false` is a retryable
    /// negative answer.
    fn try_claim(&self, length: usize, bytes: &[u8]) -> bool;

    /// Offer an encoded message; `false` is a retryable negative answer.
    fn offer(&self, bytes: &[u8]) -> bool;
}

/// The cluster's egress publisher (`EgressPublisher.java`).
pub struct EgressPublisher {
    /// The reference's `ExpandableArrayBuffer`.
    buffer: Vec<u8>,
    leader_heartbeat_timeout_ns: i64,
}

impl EgressPublisher {
    /// `new EgressPublisher(leaderHeartbeatTimeoutNs)` (`EgressPublisher.java:49-52`).
    pub fn new(leader_heartbeat_timeout_ns: i64) -> Self {
        Self {
            buffer: vec![0_u8; INITIAL_BUFFER_LENGTH],
            leader_heartbeat_timeout_ns,
        }
    }

    /// `sendEvent` (`EgressPublisher.java:54-109`).
    pub fn send_event(
        &mut self,
        session: &dyn EgressSession,
        leadership_term_id: i64,
        leader_member_id: i32,
        code: EventCode,
        detail: &str,
    ) -> Result<bool, crate::ClusterError> {
        let Some(max_payload_length) = session.max_payload_length() else {
            return Ok(false);
        };

        let length = MESSAGE_HEADER_LENGTH
            + usize::from(SESSION_EVENT_BLOCK_LENGTH)
            + VAR_DATA_HEADER_LENGTH
            + detail.len();
        self.size_buffer(length);

        {
            let encoder = SessionEventEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .cluster_session_id(session.id())
                .correlation_id(session.correlation_id())
                .leadership_term_id(leadership_term_id)
                .leader_member_id(leader_member_id)
                .code(code)
                .version(crate::config::PROTOCOL_SEMANTIC_VERSION)
                .leader_heartbeat_timeout_ns(self.leader_heartbeat_timeout_ns)
                .detail(detail.as_bytes());
        }

        Ok(self.send(session, length, max_payload_length))
    }

    /// `sendChallenge` (`EgressPublisher.java:111-144`).
    pub fn send_challenge(
        &mut self,
        session: &dyn EgressSession,
        encoded_challenge: &[u8],
    ) -> Result<bool, crate::ClusterError> {
        let Some(max_payload_length) = session.max_payload_length() else {
            return Ok(false);
        };

        let length = MESSAGE_HEADER_LENGTH
            + usize::from(CHALLENGE_BLOCK_LENGTH)
            + VAR_DATA_HEADER_LENGTH
            + encoded_challenge.len();
        self.size_buffer(length);

        {
            let encoder = ChallengeEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .correlation_id(session.correlation_id())
                .cluster_session_id(session.id())
                .encoded_challenge(encoded_challenge);
        }

        Ok(self.send(session, length, max_payload_length))
    }

    /// `newLeader` (`EgressPublisher.java:146-193`).
    pub fn new_leader(
        &mut self,
        session: &dyn EgressSession,
        leadership_term_id: i64,
        leader_member_id: i32,
        ingress_endpoints: &str,
    ) -> Result<bool, crate::ClusterError> {
        let Some(max_payload_length) = session.max_payload_length() else {
            return Ok(false);
        };

        let length = MESSAGE_HEADER_LENGTH
            + usize::from(NEW_LEADER_EVENT_BLOCK_LENGTH)
            + VAR_DATA_HEADER_LENGTH
            + ingress_endpoints.len();
        self.size_buffer(length);

        {
            let encoder = NewLeaderEventEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .leadership_term_id(leadership_term_id)
                .cluster_session_id(session.id())
                .leader_member_id(leader_member_id)
                .ingress_endpoints(ingress_endpoints.as_bytes());
        }

        Ok(self.send(session, length, max_payload_length))
    }

    /// `sendAdminResponse` (`EgressPublisher.java:195-245`).
    pub fn send_admin_response(
        &mut self,
        session: &dyn EgressSession,
        correlation_id: i64,
        request_type: AdminRequestType,
        response_code: AdminResponseCode,
        message: &str,
    ) -> Result<bool, crate::ClusterError> {
        let Some(max_payload_length) = session.max_payload_length() else {
            return Ok(false);
        };

        // The `payload` variable-length field is written empty, header
        // included (`EgressPublisher.java:203-206`).
        let length = MESSAGE_HEADER_LENGTH
            + usize::from(ADMIN_RESPONSE_BLOCK_LENGTH)
            + VAR_DATA_HEADER_LENGTH
            + message.len()
            + VAR_DATA_HEADER_LENGTH;
        self.size_buffer(length);

        {
            let encoder = AdminResponseEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message_encoder = header.parent().expect("the encoder just wrapped");
            message_encoder
                .cluster_session_id(session.id())
                .correlation_id(correlation_id)
                .request_type(request_type)
                .response_code(response_code)
                .message(message.as_bytes())
                .payload(&[]);
        }

        Ok(self.send(session, length, max_payload_length))
    }

    /// Claim when it fits, offer when it does not
    /// (`EgressPublisher.java:80-109`).
    fn send(&self, session: &dyn EgressSession, length: usize, max_payload_length: usize) -> bool {
        if length <= max_payload_length {
            for _ in 0..SEND_ATTEMPTS {
                if session.try_claim(length, &self.buffer[..length]) {
                    return true;
                }
            }
            false
        } else {
            for _ in 0..SEND_ATTEMPTS {
                if session.offer(&self.buffer[..length]) {
                    return true;
                }
            }
            false
        }
    }

    /// The reference's `ExpandableArrayBuffer` grows once and is reused.
    fn size_buffer(&mut self, length: usize) {
        if self.buffer.len() < length {
            self.buffer.resize(length, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    // The ported `EgressPublisherTest` cases live in `tests/egress_publisher.rs`,
    // because they need a session mock that records the path taken.
}
