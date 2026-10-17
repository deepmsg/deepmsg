//! What the consensus module tells a service (`ServiceProxy.java`).
//!
//! Five messages on the control channel's service stream, every one of them
//! with five send attempts:
//!
//! | Here | Reference |
//! |---|---|
//! | [`ServiceProxy::join_log`] | `JoinLog` (40) — `:54-101` |
//! | [`ServiceProxy::cluster_members_response`] | `ClusterMembersResponse` (41) — `:103-139` |
//! | [`ServiceProxy::cluster_members_extended_response`] | `ClusterMembersExtendedResponse` (43) — `:141-194` |
//! | [`ServiceProxy::termination_position`] | `ServiceTerminationPosition` (42) — `:196-229` |
//! | [`ServiceProxy::request_service_ack`] | `RequestServiceAck` (108) — `:231-259` |
//!
//! The failure semantics are the reference's and are not uniform: `joinLog` and
//! `requestServiceAck` throw when the attempts are gone (the startup wait cannot
//! proceed), a closed publication is silently ignored by `terminationPosition`,
//! and the two membership responses report through the error handler and carry
//! on. A `NOT_CONNECTED` or a closed publication is a `ClusterException` for
//! `join_log` — "publication is not connected" / "publication is closed".

use deepmsg_cluster_common::member::ClusterMember;
use deepmsg_codec_cluster::WriteBuf;
use deepmsg_codec_cluster::boolean_type::BooleanType;
use deepmsg_codec_cluster::cluster_members_extended_response_codec::encoder::{
    ActiveMembersEncoder, PassiveMembersEncoder,
};
use deepmsg_codec_cluster::cluster_members_extended_response_codec::{
    ClusterMembersExtendedResponseEncoder, SBE_BLOCK_LENGTH as EXTENDED_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::cluster_members_response_codec::{
    ClusterMembersResponseEncoder, SBE_BLOCK_LENGTH as RESPONSE_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::join_log_codec::{
    JoinLogEncoder, SBE_BLOCK_LENGTH as JOIN_LOG_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::request_service_ack_codec::{
    RequestServiceAckEncoder, SBE_BLOCK_LENGTH as REQUEST_ACK_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::service_termination_position_codec::{
    SBE_BLOCK_LENGTH as TERMINATION_BLOCK_LENGTH, ServiceTerminationPositionEncoder,
};

use crate::error::ClusterError;

/// `ServiceProxy.SEND_ATTEMPTS` (`ServiceProxy.java:28`).
const SEND_ATTEMPTS: usize = 5;

/// Every variable-length field in schema 111 is a 4-byte length and the bytes.
const VAR_DATA_HEADER_LENGTH: usize = 4;

/// The send surface the proxy needs, so `ServiceProxyTest` can mock the
/// publication the way the reference mocks `ConcurrentPublication`.
pub trait ServiceSend {
    /// Write a claimed message of `length` bytes.
    fn try_claim(&self, length: usize, bytes: &[u8]) -> bool;

    /// Offer an encoded message.
    fn offer(&self, bytes: &[u8]) -> bool;

    /// `Publication.isClosed()`.
    fn is_closed(&self) -> bool;

    /// `Publication.termBufferLength()`, for the max-position error.
    fn term_buffer_length(&self) -> i32;
}

/// `ServiceProxy` (`ServiceProxy.java`).
pub struct ServiceProxy {
    buffer: Vec<u8>,
}

impl Default for ServiceProxy {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceProxy {
    /// The proxy's reusable encoding buffer.
    pub fn new() -> Self {
        Self {
            buffer: vec![0_u8; 512],
        }
    }

    /// `joinLog(...)` (`:54-101`).
    ///
    /// # Errors
    ///
    /// [`ClusterError::Cluster`] when the publication is not connected or
    /// closed, is at its maximum position, or the five attempts are gone —
    /// the reference throws from `checkResult` and from the exhaustion branch.
    #[allow(clippy::too_many_arguments)]
    pub fn join_log(
        &mut self,
        send: &dyn ServiceSend,
        log_position: i64,
        max_log_position: i64,
        member_id: i32,
        log_session_id: i32,
        log_stream_id: i32,
        is_startup: bool,
        role: i32,
        channel: &str,
        is_standby: bool,
    ) -> Result<(), ClusterError> {
        let length = MESSAGE_HEADER_LENGTH
            + usize::from(JOIN_LOG_BLOCK_LENGTH)
            + VAR_DATA_HEADER_LENGTH
            + channel.len();
        self.size_buffer(length);

        {
            let encoder = JoinLogEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .log_position(log_position)
                .max_log_position(max_log_position)
                .member_id(member_id)
                .log_session_id(log_session_id)
                .log_stream_id(log_stream_id)
                .is_startup(boolean(is_startup))
                .role(role)
                .is_standby(boolean(is_standby))
                .log_channel(channel.as_bytes());
        }

        for _ in 0..SEND_ATTEMPTS {
            if send.try_claim(length, &self.buffer[..length]) {
                return Ok(());
            }
        }

        Err(ClusterError::Cluster(format!(
            "failed to send join log request: {}",
            publication_state(send)
        )))
    }

    /// `clusterMembersResponse(...)` (`:103-139`). `false` means the five
    /// attempts were gone and a `ClusterEvent` was recorded by the reference's
    /// error handler.
    pub fn cluster_members_response(
        &mut self,
        send: &dyn ServiceSend,
        correlation_id: i64,
        leader_member_id: i32,
        active_members: &str,
    ) -> bool {
        let length = MESSAGE_HEADER_LENGTH
            + usize::from(RESPONSE_BLOCK_LENGTH)
            + VAR_DATA_HEADER_LENGTH
            + active_members.len()
            + VAR_DATA_HEADER_LENGTH;
        self.size_buffer(length);

        {
            let encoder = ClusterMembersResponseEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .correlation_id(correlation_id)
                .leader_member_id(leader_member_id)
                .active_members(active_members.as_bytes())
                .passive_followers(b"");
        }

        for _ in 0..SEND_ATTEMPTS {
            if send.try_claim(length, &self.buffer[..length]) {
                return true;
            }
        }
        false
    }

    /// `clusterMembersExtendedResponse(...)` (`:141-194`). The reference
    /// encodes into an expandable buffer and **offers**; `false` means the five
    /// attempts were gone.
    pub fn cluster_members_extended_response(
        &mut self,
        send: &dyn ServiceSend,
        correlation_id: i64,
        current_time_ns: i64,
        leader_member_id: i32,
        member_id: i32,
        active_members: &[ClusterMember],
    ) -> bool {
        let length = self.encode_extended(
            correlation_id,
            current_time_ns,
            leader_member_id,
            member_id,
            active_members,
        );
        self.size_buffer(length);

        for _ in 0..SEND_ATTEMPTS {
            if send.offer(&self.buffer[..length]) {
                return true;
            }
        }
        false
    }

    /// `terminationPosition(...)` (`:196-229`): a closed publication is
    /// silently ignored.
    pub fn termination_position(
        &mut self,
        send: &dyn ServiceSend,
        log_position: i64,
        ack_required: bool,
    ) -> bool {
        if send.is_closed() {
            return true;
        }

        let length = MESSAGE_HEADER_LENGTH + usize::from(TERMINATION_BLOCK_LENGTH);
        self.size_buffer(length);

        {
            let encoder = ServiceTerminationPositionEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .log_position(log_position)
                .ack_required(boolean(ack_required));
        }

        for _ in 0..SEND_ATTEMPTS {
            if send.try_claim(length, &self.buffer[..length]) {
                return true;
            }
        }
        false
    }

    /// `requestServiceAck(logPosition)` (`:231-259`).
    ///
    /// # Errors
    ///
    /// [`ClusterError::Cluster`] when the five attempts are gone — the
    /// reference throws here.
    pub fn request_service_ack(
        &mut self,
        send: &dyn ServiceSend,
        log_position: i64,
    ) -> Result<(), ClusterError> {
        let length = MESSAGE_HEADER_LENGTH + usize::from(REQUEST_ACK_BLOCK_LENGTH);
        self.size_buffer(length);

        {
            let encoder = RequestServiceAckEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message.log_position(log_position);
        }

        for _ in 0..SEND_ATTEMPTS {
            if send.try_claim(length, &self.buffer[..length]) {
                return Ok(());
            }
        }

        Err(ClusterError::Cluster(format!(
            "failed to send request for service ack: {}",
            publication_state(send)
        )))
    }

    /// Encode the extended response and return its length.
    fn encode_extended(
        &mut self,
        correlation_id: i64,
        current_time_ns: i64,
        leader_member_id: i32,
        member_id: i32,
        active_members: &[ClusterMember],
    ) -> usize {
        // A generous bound: the block plus five endpoints per member.
        let per_member = 2048;
        let capacity = MESSAGE_HEADER_LENGTH
            + usize::from(EXTENDED_BLOCK_LENGTH)
            + active_members.len() * per_member
            + 64;
        if self.buffer.len() < capacity {
            self.buffer.resize(capacity, 0);
        }

        let encoder = ClusterMembersExtendedResponseEncoder::default().wrap(
            WriteBuf::new(&mut self.buffer[..capacity]),
            MESSAGE_HEADER_LENGTH,
        );
        let mut header = encoder.header(0);
        let mut message = header.parent().expect("the encoder just wrapped");
        message
            .correlation_id(correlation_id)
            .current_time_ns(current_time_ns)
            .leader_member_id(leader_member_id)
            .member_id(member_id);

        #[allow(clippy::cast_possible_truncation)] // at most MAX_SERVICE_COUNT members
        let count = active_members.len() as u16;
        let mut group = message.active_members_encoder(count, ActiveMembersEncoder::default());

        for m in active_members {
            group
                .advance()
                .expect("the group has room")
                .expect("an index");
            group
                .leadership_term_id(m.leadership_term_id)
                .log_position(m.log_position)
                .time_of_last_append_ns(m.time_of_last_append_position_ns)
                .member_id(m.id)
                .ingress_endpoint(m.ingress_endpoint.as_bytes())
                .consensus_endpoint(m.consensus_endpoint.as_bytes())
                .log_endpoint(m.log_endpoint.as_bytes())
                .catchup_endpoint(m.catchup_endpoint.as_bytes())
                .archive_endpoint(m.archive_endpoint.as_bytes());
        }

        let message = group.parent().expect("the group has a parent");
        let mut passive = message.passive_members_encoder(0, PassiveMembersEncoder::default());
        let message = passive.parent().expect("the group has a parent");
        MESSAGE_HEADER_LENGTH + message.encoded_length()
    }

    fn size_buffer(&mut self, length: usize) {
        if self.buffer.len() < length {
            self.buffer.resize(length, 0);
        }
    }
}

fn boolean(value: bool) -> BooleanType {
    if value {
        BooleanType::TRUE
    } else {
        BooleanType::FALSE
    }
}

/// The reference's `Publication.errorString`, for the exhaustion messages.
fn publication_state(send: &dyn ServiceSend) -> String {
    if send.is_closed() {
        format!(
            "publication is closed, term-length={}",
            send.term_buffer_length()
        )
    } else {
        "not connected".to_owned()
    }
}
