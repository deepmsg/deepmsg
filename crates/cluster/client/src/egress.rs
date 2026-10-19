//! What a cluster sends back: the two listeners, the two extension hooks, and
//! the adapters that dispatch to them (`EgressListener.java`,
//! `ControlledEgressListener.java`, `EgressAdapter.java`).
//!
//! # One decode, two doors
//!
//! A cluster's egress carries four messages that matter to a client — a
//! service message, a session event, a new leader, an admin response — and any
//! number of others it does not. [`decode`] turns the four into an
//! [`Egress`] and everything else into `None` (or, for another schema, an
//! [`Egress::Extension`]).
//!
//! Two doors read it. [`EgressAdapter`] is for a caller that wants the egress
//! stream and not the session: it holds the session id it cares about, drops
//! everything addressed to another session, and hands the rest to its
//! listener — which is exactly what `EgressAdapterTest`'s ten cases pin down.
//! [`crate::cluster::AeronCluster`] reads the same decode and adds the
//! session's own reactions (`SessionEvent.CLOSED`, a new leader, the egress
//! image). The parse is shared so there is one answer to "what is this
//! message"; the reactions are not, because the session's are its own.
//!
//! # Unknown schemas are a question, not an error
//!
//! A message from another schema is a message this client's schema does not
//! describe — an application's extension, usually. With an extension listener
//! it is delivered; without one it is a [`ClusterClientError::Cluster`] with
//! the reference's text (`EgressAdapter.java:141-146`), because a client that
//! silently dropped it would make a schema mismatch look like an idle stream.

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, Message, MessageHeader};
use deepmsg_codec::cluster::SBE_SCHEMA_ID;
use deepmsg_codec::cluster::admin_request_type::AdminRequestType;
use deepmsg_codec::cluster::admin_response_code::AdminResponseCode;
use deepmsg_codec::cluster::admin_response_codec::{
    AdminResponseDecoder, SBE_BLOCK_LENGTH as ADMIN_RESPONSE_BLOCK_LENGTH,
};
use deepmsg_codec::cluster::event_code::EventCode;
use deepmsg_codec::cluster::message_header_codec;
use deepmsg_codec::cluster::new_leader_event_codec::{
    NewLeaderEventDecoder, SBE_BLOCK_LENGTH as NEW_LEADER_EVENT_BLOCK_LENGTH,
};
use deepmsg_codec::cluster::session_event_codec::{
    SBE_BLOCK_LENGTH as SESSION_EVENT_BLOCK_LENGTH, SessionEventDecoder,
};
use deepmsg_codec::cluster::session_message_header_codec::{
    SBE_BLOCK_LENGTH as SESSION_MESSAGE_HEADER_BLOCK_LENGTH, SessionMessageHeaderDecoder,
};

use crate::error::ClusterClientError;
use crate::sbe::{holds_var, sbe_header, var_slice};

/// Length of a session message's header, on ingress and on egress
/// (`AeronCluster.SESSION_HEADER_LENGTH`, `AC:99-100`): the SBE header (8) plus
/// the `SessionMessageHeader` block (24).
pub const SESSION_HEADER_LENGTH: usize =
    message_header_codec::ENCODED_LENGTH + SESSION_MESSAGE_HEADER_BLOCK_LENGTH as usize;

/// A template that reaches a listener.
const SESSION_MESSAGE_HEADER_TEMPLATE_ID: u16 = 1;
const SESSION_EVENT_TEMPLATE_ID: u16 = 2;
const NEW_LEADER_EVENT_TEMPLATE_ID: u16 = 6;
const ADMIN_RESPONSE_TEMPLATE_ID: u16 = 27;

/// `EgressListener` (`EgressListener.java:36-116`): the four callbacks a
/// session message and the three administrative events arrive on.
///
/// The reference's `onMessage` hands over the buffer, an offset and a length;
/// this hands over the payload slice, which is the same bytes with the whole
/// message already delimited, plus the fragment's header for a caller that
/// wants to know where they came from.
pub trait EgressListener {
    /// A message a clustered service sent to this session.
    fn on_message(
        &mut self,
        cluster_session_id: i64,
        timestamp: i64,
        payload: &[u8],
        header: &MessageHeader,
    );

    /// A session event (an error, a close, an authentication answer, …).
    fn on_session_event(
        &mut self,
        _correlation_id: i64,
        _cluster_session_id: i64,
        _leadership_term_id: i64,
        _leader_member_id: i32,
        _code: EventCode,
        _detail: &str,
    ) {
    }

    /// A new leader has been elected.
    fn on_new_leader(
        &mut self,
        _cluster_session_id: i64,
        _leadership_term_id: i64,
        _leader_member_id: i32,
        _ingress_endpoints: &str,
    ) {
    }

    /// The cluster's answer to an admin request.
    fn on_admin_response(
        &mut self,
        _cluster_session_id: i64,
        _correlation_id: i64,
        _request_type: AdminRequestType,
        _response_code: AdminResponseCode,
        _message: &str,
        _payload: &[u8],
    ) {
    }
}

/// `ControlledEgressListener` (`ControlledEgressListener.java:35-125`): the
/// same four callbacks, with the session message's consumption the listener's
/// to decide.
pub trait ControlledEgressListener {
    /// A message a clustered service sent to this session, and what the poll
    /// should do with it.
    fn on_message(
        &mut self,
        cluster_session_id: i64,
        timestamp: i64,
        payload: &[u8],
        header: &MessageHeader,
    ) -> Action;

    /// A session event.
    fn on_session_event(
        &mut self,
        _correlation_id: i64,
        _cluster_session_id: i64,
        _leadership_term_id: i64,
        _leader_member_id: i32,
        _code: EventCode,
        _detail: &str,
    ) {
    }

    /// A new leader has been elected.
    fn on_new_leader(
        &mut self,
        _cluster_session_id: i64,
        _leadership_term_id: i64,
        _leader_member_id: i32,
        _ingress_endpoints: &str,
    ) {
    }

    /// The cluster's answer to an admin request.
    fn on_admin_response(
        &mut self,
        _cluster_session_id: i64,
        _correlation_id: i64,
        _request_type: AdminRequestType,
        _response_code: AdminResponseCode,
        _message: &str,
        _payload: &[u8],
    ) {
    }
}

/// `EgressListenerExtension` (`EgressListenerExtension.java:24-50`): a message
/// from a schema this client does not know.
pub trait EgressListenerExtension {
    /// The message's SBE header fields and the body after the header.
    fn on_extension_message(
        &mut self,
        acting_block_length: u16,
        template_id: u16,
        schema_id: u16,
        acting_version: u16,
        body: &[u8],
    );
}

/// `ControlledEgressListenerExtension`: the same, controlled.
pub trait ControlledEgressListenerExtension {
    /// The message's SBE header fields and the body after the header, and what
    /// the poll should do with it.
    fn on_extension_message(
        &mut self,
        acting_block_length: u16,
        template_id: u16,
        schema_id: u16,
        acting_version: u16,
        body: &[u8],
    ) -> Action;
}

/// One egress message, decoded.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Egress<'a> {
    /// A service message: `timestamp` is when the correlated ingress was
    /// sequenced, `payload` is what the service sent (the session header is
    /// already behind us).
    Message {
        /// The session it belongs to.
        cluster_session_id: i64,
        /// When the ingress was sequenced in the cluster.
        timestamp: i64,
        /// The service's bytes.
        payload: &'a [u8],
    },
    /// A session event.
    SessionEvent {
        correlation_id: i64,
        cluster_session_id: i64,
        leadership_term_id: i64,
        leader_member_id: i32,
        code: EventCode,
        detail: &'a [u8],
    },
    /// A new leader.
    NewLeader {
        cluster_session_id: i64,
        leadership_term_id: i64,
        leader_member_id: i32,
        ingress_endpoints: &'a [u8],
    },
    /// An admin response.
    AdminResponse {
        cluster_session_id: i64,
        correlation_id: i64,
        request_type: AdminRequestType,
        response_code: AdminResponseCode,
        message: &'a [u8],
        payload: &'a [u8],
    },
    /// A message from another schema.
    Extension {
        acting_block_length: u16,
        template_id: u16,
        schema_id: u16,
        acting_version: u16,
        body: &'a [u8],
    },
}

/// Decode one egress message.
///
/// `Ok(None)` is "not a message this client reads": an unknown template, or a
/// message too short to hold what it claims. `Ok(Some(Egress::Extension(_)))`
/// is another schema's message, which is a question for the extension
/// listener rather than an error here.
pub(crate) fn decode<'a>(message: &Message<'a>) -> Option<Egress<'a>> {
    let payload = message.payload;
    let header = sbe_header(payload)?;

    if header.schema_id() != SBE_SCHEMA_ID {
        return Some(Egress::Extension {
            acting_block_length: header.block_length(),
            template_id: header.template_id(),
            schema_id: header.schema_id(),
            acting_version: header.version(),
            body: payload.get(message_header_codec::ENCODED_LENGTH..)?,
        });
    }

    match header.template_id() {
        SESSION_MESSAGE_HEADER_TEMPLATE_ID => {
            if payload.len() < SESSION_HEADER_LENGTH {
                return None;
            }

            let decoder = SessionMessageHeaderDecoder::default().header(header, 0);

            Some(Egress::Message {
                cluster_session_id: decoder.cluster_session_id(),
                timestamp: decoder.timestamp(),
                payload: &payload[SESSION_HEADER_LENGTH..],
            })
        }
        SESSION_EVENT_TEMPLATE_ID => {
            if !holds_var(payload, SESSION_EVENT_BLOCK_LENGTH) {
                return None;
            }

            let mut decoder = SessionEventDecoder::default().header(header, 0);
            let coordinates = decoder.detail_decoder();
            let detail = var_slice(payload, coordinates)?;

            Some(Egress::SessionEvent {
                correlation_id: decoder.correlation_id(),
                cluster_session_id: decoder.cluster_session_id(),
                leadership_term_id: decoder.leadership_term_id(),
                leader_member_id: decoder.leader_member_id(),
                code: decoder.code(),
                detail,
            })
        }
        NEW_LEADER_EVENT_TEMPLATE_ID => {
            if !holds_var(payload, NEW_LEADER_EVENT_BLOCK_LENGTH) {
                return None;
            }

            let mut decoder = NewLeaderEventDecoder::default().header(header, 0);
            let coordinates = decoder.ingress_endpoints_decoder();
            let ingress_endpoints = var_slice(payload, coordinates)?;

            Some(Egress::NewLeader {
                cluster_session_id: decoder.cluster_session_id(),
                leadership_term_id: decoder.leadership_term_id(),
                leader_member_id: decoder.leader_member_id(),
                ingress_endpoints,
            })
        }
        ADMIN_RESPONSE_TEMPLATE_ID => {
            if !holds_var(payload, ADMIN_RESPONSE_BLOCK_LENGTH) {
                return None;
            }

            let mut decoder = AdminResponseDecoder::default().header(header, 0);
            let coordinates = decoder.message_decoder();
            let message = var_slice(payload, coordinates)?;

            // The payload's length prefix lies after the message, so the bound
            // for it is the one the decoder has just advanced past.
            let after_message = message_header_codec::ENCODED_LENGTH
                + usize::from(ADMIN_RESPONSE_BLOCK_LENGTH)
                + 4
                + message.len();
            if payload.len() < after_message + 4 {
                return None;
            }

            let coordinates = decoder.payload_decoder();
            let admin_payload = var_slice(payload, coordinates)?;

            Some(Egress::AdminResponse {
                cluster_session_id: decoder.cluster_session_id(),
                correlation_id: decoder.correlation_id(),
                request_type: decoder.request_type(),
                response_code: decoder.response_code(),
                message,
                payload: admin_payload,
            })
        }
        _ => None,
    }
}

/// `EgressAdapter` (`EA:31-230`): dispatch an egress subscription to an
/// [`EgressListener`].
///
/// The subscription is the caller's (a id, as everywhere in this crate), and
/// [`poll`](Self::poll) reads it through the caller's client.
pub struct EgressAdapter {
    cluster_session_id: i64,
    subscription: i64,
    fragment_limit: usize,
    listener: Box<dyn EgressListener>,
    extension: Option<Box<dyn EgressListenerExtension>>,
}

impl EgressAdapter {
    /// An adapter for `subscription`, delivering messages for
    /// `cluster_session_id` to `listener`.
    pub fn new(
        listener: Box<dyn EgressListener>,
        cluster_session_id: i64,
        subscription: i64,
        fragment_limit: usize,
    ) -> Self {
        Self {
            cluster_session_id,
            subscription,
            fragment_limit,
            listener,
            extension: None,
        }
    }

    /// The same, with somewhere to send another schema's messages.
    #[must_use]
    pub fn with_extension(mut self, extension: Box<dyn EgressListenerExtension>) -> Self {
        self.extension = Some(extension);
        self
    }

    /// The subscription it reads.
    #[must_use]
    pub const fn subscription(&self) -> i64 {
        self.subscription
    }

    /// Read the subscription and dispatch what it holds (`EA:114-119`).
    ///
    /// # Errors
    ///
    /// [`ClusterClientError::Cluster`] for another schema with no extension
    /// listener — the reference throws from its handler (`EA:141-146`).
    pub fn poll(&mut self, client: &mut Client) -> Result<usize, ClusterClientError> {
        let subscription = self.subscription;
        let fragment_limit = self.fragment_limit;
        let mut error = None;
        let mut handler = |message: Message<'_>| match self.on_message(message) {
            Ok(()) => Action::Continue,
            Err(failure) => {
                error = Some(failure);
                Action::Break
            }
        };

        let fragments =
            client.poll_subscription_controlled(subscription, fragment_limit, &mut handler);

        match error {
            Some(error) => Err(error),
            None => Ok(fragments),
        }
    }

    /// Dispatch one message (`EA:124-230`).
    ///
    /// # Errors
    ///
    /// As [`Self::poll`].
    pub fn on_message(&mut self, message: Message<'_>) -> Result<(), ClusterClientError> {
        match decode(&message) {
            Some(Egress::Extension {
                acting_block_length,
                template_id,
                schema_id,
                acting_version,
                body,
            }) => match self.extension.as_mut() {
                Some(extension) => {
                    extension.on_extension_message(
                        acting_block_length,
                        template_id,
                        schema_id,
                        acting_version,
                        body,
                    );
                    Ok(())
                }
                None => Err(ClusterClientError::Cluster(format!(
                    "expected schemaId={SBE_SCHEMA_ID}, actual={schema_id}"
                ))),
            },
            Some(Egress::Message {
                cluster_session_id,
                timestamp,
                payload,
            }) => {
                if cluster_session_id == self.cluster_session_id {
                    self.listener.on_message(
                        cluster_session_id,
                        timestamp,
                        payload,
                        &message.header,
                    );
                }
                Ok(())
            }
            Some(Egress::SessionEvent {
                correlation_id,
                cluster_session_id,
                leadership_term_id,
                leader_member_id,
                code,
                detail,
            }) => {
                if cluster_session_id == self.cluster_session_id {
                    self.listener.on_session_event(
                        correlation_id,
                        cluster_session_id,
                        leadership_term_id,
                        leader_member_id,
                        code,
                        &String::from_utf8_lossy(detail),
                    );
                }
                Ok(())
            }
            Some(Egress::NewLeader {
                cluster_session_id,
                leadership_term_id,
                leader_member_id,
                ingress_endpoints,
            }) => {
                if cluster_session_id == self.cluster_session_id {
                    self.listener.on_new_leader(
                        cluster_session_id,
                        leadership_term_id,
                        leader_member_id,
                        &String::from_utf8_lossy(ingress_endpoints),
                    );
                }
                Ok(())
            }
            Some(Egress::AdminResponse {
                cluster_session_id,
                correlation_id,
                request_type,
                response_code,
                message: admin_message,
                payload,
            }) => {
                if cluster_session_id == self.cluster_session_id {
                    self.listener.on_admin_response(
                        cluster_session_id,
                        correlation_id,
                        request_type,
                        response_code,
                        &String::from_utf8_lossy(admin_message),
                        payload,
                    );
                }
                Ok(())
            }
            None => Ok(()),
        }
    }
}

/// `ControlledEgressAdapter`: the same dispatch, with the session message's
/// consumption the listener's to decide.
pub struct ControlledEgressAdapter {
    cluster_session_id: i64,
    subscription: i64,
    fragment_limit: usize,
    listener: Box<dyn ControlledEgressListener>,
    extension: Option<Box<dyn ControlledEgressListenerExtension>>,
}

impl ControlledEgressAdapter {
    /// An adapter for `subscription`, delivering messages for
    /// `cluster_session_id` to `listener`.
    pub fn new(
        listener: Box<dyn ControlledEgressListener>,
        cluster_session_id: i64,
        subscription: i64,
        fragment_limit: usize,
    ) -> Self {
        Self {
            cluster_session_id,
            subscription,
            fragment_limit,
            listener,
            extension: None,
        }
    }

    /// The same, with somewhere to send another schema's messages.
    #[must_use]
    pub fn with_extension(mut self, extension: Box<dyn ControlledEgressListenerExtension>) -> Self {
        self.extension = Some(extension);
        self
    }

    /// The subscription it reads.
    #[must_use]
    pub const fn subscription(&self) -> i64 {
        self.subscription
    }

    /// Read the subscription and dispatch what it holds.
    ///
    /// # Errors
    ///
    /// As [`EgressAdapter::poll`].
    pub fn poll(&mut self, client: &mut Client) -> Result<usize, ClusterClientError> {
        let subscription = self.subscription;
        let fragment_limit = self.fragment_limit;
        let mut error = None;
        let mut handler = |message: Message<'_>| match self.on_message(message) {
            Ok(action) => action,
            Err(failure) => {
                error = Some(failure);
                Action::Break
            }
        };

        let fragments =
            client.poll_subscription_controlled(subscription, fragment_limit, &mut handler);

        match error {
            Some(error) => Err(error),
            None => Ok(fragments),
        }
    }

    /// Dispatch one message and say what the poll should do next.
    ///
    /// # Errors
    ///
    /// As [`EgressAdapter::poll`].
    pub fn on_message(&mut self, message: Message<'_>) -> Result<Action, ClusterClientError> {
        match decode(&message) {
            Some(Egress::Extension {
                acting_block_length,
                template_id,
                schema_id,
                acting_version,
                body,
            }) => match self.extension.as_mut() {
                Some(extension) => Ok(extension.on_extension_message(
                    acting_block_length,
                    template_id,
                    schema_id,
                    acting_version,
                    body,
                )),
                None => Err(ClusterClientError::Cluster(format!(
                    "expected schemaId={SBE_SCHEMA_ID}, actual={schema_id}"
                ))),
            },
            Some(Egress::Message {
                cluster_session_id,
                timestamp,
                payload,
            }) => {
                if cluster_session_id == self.cluster_session_id {
                    return Ok(self.listener.on_message(
                        cluster_session_id,
                        timestamp,
                        payload,
                        &message.header,
                    ));
                }
                Ok(Action::Continue)
            }
            Some(Egress::SessionEvent {
                correlation_id,
                cluster_session_id,
                leadership_term_id,
                leader_member_id,
                code,
                detail,
            }) => {
                if cluster_session_id == self.cluster_session_id {
                    self.listener.on_session_event(
                        correlation_id,
                        cluster_session_id,
                        leadership_term_id,
                        leader_member_id,
                        code,
                        &String::from_utf8_lossy(detail),
                    );
                }
                Ok(Action::Continue)
            }
            Some(Egress::NewLeader {
                cluster_session_id,
                leadership_term_id,
                leader_member_id,
                ingress_endpoints,
            }) => {
                if cluster_session_id == self.cluster_session_id {
                    self.listener.on_new_leader(
                        cluster_session_id,
                        leadership_term_id,
                        leader_member_id,
                        &String::from_utf8_lossy(ingress_endpoints),
                    );
                }
                // A new leader is a message the session must not see twice
                // (`AC:1082`).
                Ok(Action::Commit)
            }
            Some(Egress::AdminResponse {
                cluster_session_id,
                correlation_id,
                request_type,
                response_code,
                message: admin_message,
                payload,
            }) => {
                if cluster_session_id == self.cluster_session_id {
                    self.listener.on_admin_response(
                        cluster_session_id,
                        correlation_id,
                        request_type,
                        response_code,
                        &String::from_utf8_lossy(admin_message),
                        payload,
                    );
                }
                Ok(Action::Continue)
            }
            None => Ok(Action::Continue),
        }
    }
}

#[cfg(test)]
mod tests {
    use deepmsg_codec::cluster::WriteBuf;
    use deepmsg_codec::cluster::admin_response_codec::AdminResponseEncoder;
    use deepmsg_codec::cluster::new_leader_event_codec::NewLeaderEventEncoder;
    use deepmsg_codec::cluster::session_event_codec::SessionEventEncoder;
    use deepmsg_codec::cluster::session_message_header_codec::SessionMessageHeaderEncoder;

    use super::*;
    use crate::PROTOCOL_SEMANTIC_VERSION;

    const SESSION_ID: i64 = 42;

    fn message(bytes: &[u8]) -> Message<'_> {
        Message {
            header: MessageHeader {
                session_id: 0,
                stream_id: 102,
                term_offset: 0,
                flags: 0,
                position: 0,
                frame_length: 0,
                fragmented_frame_length: 0,
                image_registration_id: 11,
            },
            payload: bytes,
        }
    }

    /// Encode one cluster message at the front of a buffer.
    fn encode<F>(length: usize, write: F) -> Vec<u8>
    where
        F: FnOnce(&mut [u8]) -> usize,
    {
        let mut buffer = vec![0u8; length];
        let written = write(&mut buffer);
        buffer.truncate(written);
        buffer
    }

    fn session_message(app_payload: &[u8]) -> Vec<u8> {
        encode(SESSION_HEADER_LENGTH + app_payload.len(), |buffer| {
            let encoder = SessionMessageHeaderEncoder::default()
                .wrap(WriteBuf::new(buffer), message_header_codec::ENCODED_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .cluster_session_id(SESSION_ID)
                .leadership_term_id(5)
                .timestamp(1_000);

            let length = SESSION_HEADER_LENGTH + app_payload.len();
            buffer[SESSION_HEADER_LENGTH..length].copy_from_slice(app_payload);
            length
        })
    }

    fn session_event(session_id: i64) -> Vec<u8> {
        encode(256, |buffer| {
            let encoder = SessionEventEncoder::default()
                .wrap(WriteBuf::new(buffer), message_header_codec::ENCODED_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .cluster_session_id(session_id)
                .correlation_id(77)
                .leadership_term_id(5)
                .leader_member_id(2)
                .code(EventCode::CLOSED)
                .version(PROTOCOL_SEMANTIC_VERSION)
                .leader_heartbeat_timeout_ns_opt(None)
                .detail(b"closed politely");

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        })
    }

    fn new_leader(session_id: i64) -> Vec<u8> {
        encode(256, |buffer| {
            let encoder = NewLeaderEventEncoder::default()
                .wrap(WriteBuf::new(buffer), message_header_codec::ENCODED_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .cluster_session_id(session_id)
                .leadership_term_id(6)
                .leader_member_id(1)
                .ingress_endpoints(b"0=localhost:10000,1=localhost:20000");

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        })
    }

    fn admin_response(session_id: i64) -> Vec<u8> {
        encode(256, |buffer| {
            let encoder = AdminResponseEncoder::default()
                .wrap(WriteBuf::new(buffer), message_header_codec::ENCODED_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .cluster_session_id(session_id)
                .correlation_id(88)
                .request_type(AdminRequestType::SNAPSHOT)
                .response_code(AdminResponseCode::OK)
                .message(b"taken")
                .payload(b"payload bytes");

            message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
        })
    }

    /// A foreign schema, built by hand: the header's four fields and nothing
    /// else. This is the reference's "another schema's message" — its own test
    /// writes an archive `ControlResponse` (schema 101) to get one.
    fn foreign_schema(schema_id: u16, template_id: u16) -> Vec<u8> {
        let mut bytes = vec![0u8; message_header_codec::ENCODED_LENGTH];
        bytes[0..2].copy_from_slice(&16u16.to_le_bytes()); // block length
        bytes[2..4].copy_from_slice(&template_id.to_le_bytes());
        bytes[4..6].copy_from_slice(&schema_id.to_le_bytes());
        bytes[6..8].copy_from_slice(&7u16.to_le_bytes()); // acting version
        bytes
    }

    #[derive(Default)]
    struct RecordingListener {
        calls: Vec<String>,
    }

    impl EgressListener for RecordingListener {
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
            correlation_id: i64,
            cluster_session_id: i64,
            leadership_term_id: i64,
            leader_member_id: i32,
            code: EventCode,
            detail: &str,
        ) {
            self.calls.push(format!(
                "session_event({correlation_id}, {cluster_session_id}, {leadership_term_id}, \
                 {leader_member_id}, {code:?}, {detail})"
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

        fn on_admin_response(
            &mut self,
            cluster_session_id: i64,
            correlation_id: i64,
            request_type: AdminRequestType,
            response_code: AdminResponseCode,
            message: &str,
            payload: &[u8],
        ) {
            self.calls.push(format!(
                "admin_response({cluster_session_id}, {correlation_id}, {request_type:?}, \
                 {response_code:?}, {message}, {payload:?})"
            ));
        }
    }

    #[derive(Default)]
    struct RecordingExtension {
        calls: Vec<String>,
    }

    impl EgressListenerExtension for RecordingExtension {
        fn on_extension_message(
            &mut self,
            acting_block_length: u16,
            template_id: u16,
            schema_id: u16,
            acting_version: u16,
            body: &[u8],
        ) {
            self.calls.push(format!(
                "extension({acting_block_length}, {template_id}, {schema_id}, {acting_version}, \
                 {})",
                body.len()
            ));
        }
    }

    /// A listener the test can read while the adapter owns it.
    type Shared = std::rc::Rc<std::cell::RefCell<RecordingListener>>;

    struct SharedListener(Shared);

    impl EgressListener for SharedListener {
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
            self.0.borrow_mut().on_session_event(
                correlation_id,
                cluster_session_id,
                leadership_term_id,
                leader_member_id,
                code,
                detail,
            );
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

        fn on_admin_response(
            &mut self,
            cluster_session_id: i64,
            correlation_id: i64,
            request_type: AdminRequestType,
            response_code: AdminResponseCode,
            message: &str,
            payload: &[u8],
        ) {
            self.0.borrow_mut().on_admin_response(
                cluster_session_id,
                correlation_id,
                request_type,
                response_code,
                message,
                payload,
            );
        }
    }

    fn adapter() -> (EgressAdapter, Shared) {
        let listener = Shared::default();
        let adapter = EgressAdapter::new(
            Box::new(SharedListener(Shared::clone(&listener))),
            SESSION_ID,
            1,
            10,
        );

        (adapter, listener)
    }

    /// `onFragmentShouldDelegateToEgressListenerOnUnknownSchemaId`.
    #[test]
    fn an_unknown_schema_goes_to_the_extension_listener() {
        let extension = std::rc::Rc::new(std::cell::RefCell::new(RecordingExtension::default()));
        struct SharedExtension(std::rc::Rc<std::cell::RefCell<RecordingExtension>>);
        impl EgressListenerExtension for SharedExtension {
            fn on_extension_message(
                &mut self,
                acting_block_length: u16,
                template_id: u16,
                schema_id: u16,
                acting_version: u16,
                body: &[u8],
            ) {
                self.0.borrow_mut().on_extension_message(
                    acting_block_length,
                    template_id,
                    schema_id,
                    acting_version,
                    body,
                );
            }
        }

        let (adapter, listener) = adapter();
        let mut adapter =
            adapter.with_extension(Box::new(SharedExtension(std::rc::Rc::clone(&extension))));

        let bytes = foreign_schema(17, 19);
        adapter.on_message(message(&bytes)).expect("an extension");

        assert!(listener.borrow().calls.is_empty());
        assert_eq!(
            vec!["extension(16, 19, 17, 7, 0)".to_owned()],
            extension.borrow().calls
        );
    }

    /// `defaultEgressListenerBehaviourShouldThrowClusterExceptionOnUnknownSchemaId`:
    /// no extension listener, and the message is not this schema's.
    #[test]
    fn an_unknown_schema_with_no_extension_is_the_references_error() {
        let (mut adapter, _listener) = adapter();

        let error = adapter
            .on_message(message(&[0u8; 64]))
            .expect_err("schema 0");

        assert_eq!("ERROR - expected schemaId=111, actual=0", error.to_string());
    }

    /// `onFragmentShouldInvokeOnMessageCallbackIfSessionIdMatches`.
    #[test]
    fn a_session_message_reaches_the_listener() {
        let (mut adapter, listener) = adapter();
        let bytes = session_message(b"hello");

        adapter.on_message(message(&bytes)).expect("a message");

        assert_eq!(
            vec![format!(
                "message({SESSION_ID}, 1000, [104, 101, 108, 108, 111])"
            )],
            listener.borrow().calls
        );
    }

    /// `onFragmentIsANoOpIfSessionIdDoesNotMatchOnSessionMessage`.
    #[test]
    fn a_session_message_for_another_session_is_a_no_op() {
        let (mut adapter, listener) = adapter();
        let bytes = session_message_for(SESSION_ID + 1, b"hello");

        adapter
            .on_message(message(&bytes))
            .expect("another session");

        assert!(listener.borrow().calls.is_empty());
    }

    /// `onFragmentShouldInvokeOnSessionEventCallbackIfSessionIdMatches`.
    #[test]
    fn a_session_event_reaches_the_listener() {
        let (mut adapter, listener) = adapter();
        let bytes = session_event(SESSION_ID);

        adapter.on_message(message(&bytes)).expect("an event");

        assert_eq!(
            vec![format!(
                "session_event(77, {SESSION_ID}, 5, 2, CLOSED, closed politely)"
            )],
            listener.borrow().calls
        );
    }

    /// `onFragmentIsANoOpIfSessionIdDoesNotMatchOnSessionEvent`.
    #[test]
    fn a_session_event_for_another_session_is_a_no_op() {
        let (mut adapter, listener) = adapter();

        adapter
            .on_message(message(&session_event(SESSION_ID + 1)))
            .expect("another session");

        assert!(listener.borrow().calls.is_empty());
    }

    /// `onFragmentShouldInvokeOnNewLeaderCallbackIfSessionIdMatches`.
    #[test]
    fn a_new_leader_reaches_the_listener() {
        let (mut adapter, listener) = adapter();

        adapter
            .on_message(message(&new_leader(SESSION_ID)))
            .expect("a new leader");

        assert_eq!(
            vec![format!(
                "new_leader({SESSION_ID}, 6, 1, 0=localhost:10000,1=localhost:20000)"
            )],
            listener.borrow().calls
        );
    }

    /// `onFragmentIsANoOpIfSessionIdDoesNotMatchOnNewLeader`.
    #[test]
    fn a_new_leader_for_another_session_is_a_no_op() {
        let (mut adapter, listener) = adapter();

        adapter
            .on_message(message(&new_leader(SESSION_ID + 1)))
            .expect("another session");

        assert!(listener.borrow().calls.is_empty());
    }

    /// `onFragmentShouldInvokeOnAdminResponseCallbackIfSessionIdMatches`.
    #[test]
    fn an_admin_response_reaches_the_listener() {
        let (mut adapter, listener) = adapter();

        adapter
            .on_message(message(&admin_response(SESSION_ID)))
            .expect("an admin response");

        assert_eq!(
            vec![format!(
                "admin_response({SESSION_ID}, 88, SNAPSHOT, OK, taken, \
                 [112, 97, 121, 108, 111, 97, 100, 32, 98, 121, 116, 101, 115])"
            )],
            listener.borrow().calls
        );
    }

    /// `onFragmentIsANoOpIfSessionIdDoesNotMatchOnAdminResponse`.
    #[test]
    fn an_admin_response_for_another_session_is_a_no_op() {
        let (mut adapter, listener) = adapter();

        adapter
            .on_message(message(&admin_response(SESSION_ID + 1)))
            .expect("another session");

        assert!(listener.borrow().calls.is_empty());
    }

    /// The same session message, for a session the adapter does not serve.
    fn session_message_for(session_id: i64, app_payload: &[u8]) -> Vec<u8> {
        encode(SESSION_HEADER_LENGTH + app_payload.len(), |buffer| {
            let encoder = SessionMessageHeaderEncoder::default()
                .wrap(WriteBuf::new(buffer), message_header_codec::ENCODED_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");

            encoder
                .cluster_session_id(session_id)
                .leadership_term_id(5)
                .timestamp(1_000);

            let length = SESSION_HEADER_LENGTH + app_payload.len();
            buffer[SESSION_HEADER_LENGTH..length].copy_from_slice(app_payload);
            length
        })
    }
}

#[cfg(test)]
mod compatibility_tests {
    use deepmsg_codec::cluster::ReadBuf;
    use deepmsg_codec::cluster::message_header_codec::MessageHeaderDecoder;
    use deepmsg_codec::cluster::session_event_codec::SessionEventDecoder;

    use super::*;

    /// `readingVersion12UsingVersion5Codec`: a message written for an older
    /// schema is read by the current decoder.
    ///
    /// The reference compares two generated codecs (v12 writes, v5 reads). This
    /// build generates only the current schema, so the property is stated on
    /// the one decoder it has: a `SessionEvent` whose header says **acting
    /// block length 32 and acting version 5** — the shape a v5 writer
    /// produced — reads its required fields, and answers `None` for the two
    /// optional fields that version did not have. That is exactly what the
    /// generated decoder switches on (`version()` below acting version 6,
    /// `leader_heartbeat_timeout_ns()` below 13).
    #[test]
    fn an_older_acting_version_reads_the_required_fields_and_skips_the_optional_ones() {
        const V5_BLOCK_LENGTH: u16 = 32;
        const V5_ACTING_VERSION: u16 = 5;
        const CLUSTER_SESSION_ID: i64 = -4_623_823;
        const CORRELATION_ID: i64 = 3_583_456_348_756_843;
        const LEADERSHIP_TERM_ID: i64 = 10_000_000_000;
        const LEADER_MEMBER_ID: i32 = 2;
        let detail = b"some very detailed message";

        let mut bytes = vec![0u8; 8 + V5_BLOCK_LENGTH as usize + 4 + detail.len()];
        // The SBE header: the acting block length and version are the old
        // writer's, and they are what the decoder acts on.
        bytes[0..2].copy_from_slice(&V5_BLOCK_LENGTH.to_le_bytes());
        bytes[2..4].copy_from_slice(&SESSION_EVENT_TEMPLATE_ID.to_le_bytes());
        bytes[4..6].copy_from_slice(&SBE_SCHEMA_ID.to_le_bytes());
        bytes[6..8].copy_from_slice(&V5_ACTING_VERSION.to_le_bytes());

        // The v5 block: through `code`, and no further.
        bytes[8..16].copy_from_slice(&CLUSTER_SESSION_ID.to_le_bytes());
        bytes[16..24].copy_from_slice(&CORRELATION_ID.to_le_bytes());
        bytes[24..32].copy_from_slice(&LEADERSHIP_TERM_ID.to_le_bytes());
        bytes[32..36].copy_from_slice(&LEADER_MEMBER_ID.to_le_bytes());
        bytes[36..40].copy_from_slice(&i32::from(EventCode::REDIRECT).to_le_bytes());
        // The variable field starts where the **acting** block ends.
        let at = 8 + V5_BLOCK_LENGTH as usize;
        bytes[at..at + 4].copy_from_slice(&(detail.len() as u32).to_le_bytes());
        bytes[at + 4..].copy_from_slice(detail);

        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
        let mut decoder = SessionEventDecoder::default().header(header, 0);

        assert_eq!(CLUSTER_SESSION_ID, decoder.cluster_session_id());
        assert_eq!(CORRELATION_ID, decoder.correlation_id());
        assert_eq!(LEADERSHIP_TERM_ID, decoder.leadership_term_id());
        assert_eq!(LEADER_MEMBER_ID, decoder.leader_member_id());
        assert_eq!(EventCode::REDIRECT, decoder.code());
        assert_eq!(None, decoder.version(), "a v5 writer had no version field");
        assert_eq!(
            None,
            decoder.leader_heartbeat_timeout_ns(),
            "a v5 writer had no heartbeat field"
        );

        let coordinates = decoder.detail_decoder();
        assert_eq!(Some(detail.as_slice()), var_slice(&bytes, coordinates));

        // And the client's own decode reads the same message.
        match decode(&Message {
            header: deepmsg_client::fragment_assembler::MessageHeader {
                session_id: 0,
                stream_id: 102,
                term_offset: 0,
                flags: 0,
                position: 0,
                frame_length: 0,
                fragmented_frame_length: 0,
                image_registration_id: 0,
            },
            payload: &bytes,
        }) {
            Some(Egress::SessionEvent {
                cluster_session_id,
                correlation_id,
                leadership_term_id,
                leader_member_id,
                code,
                detail: decoded_detail,
            }) => {
                assert_eq!(CLUSTER_SESSION_ID, cluster_session_id);
                assert_eq!(CORRELATION_ID, correlation_id);
                assert_eq!(LEADERSHIP_TERM_ID, leadership_term_id);
                assert_eq!(LEADER_MEMBER_ID, leader_member_id);
                assert_eq!(EventCode::REDIRECT, code);
                assert_eq!(detail.as_slice(), decoded_detail);
            }
            other => panic!("the older message decoded as {other:?}"),
        }
    }
}
