//! What the consensus module tells a service (`ServiceAdapter.java`).
//!
//! One subscription on the control channel's service stream (104), polled with
//! a fragment limit of [`FRAGMENT_LIMIT`] = 1, dispatching three message kinds:
//!
//! | Template | Message | Handler |
//! |---|---|---|
//! | 40 | `JoinLog` | [`ServiceEvents::join_log`] |
//! | 42 | `ServiceTerminationPosition` | [`ServiceEvents::termination_position`] |
//! | 108 | `RequestServiceAck` | [`ServiceEvents::requested_ack_position`] |
//!
//! The container does not read `ClusterMembersResponse(41/43)`: the reference's
//! `ServiceAdapter` has no arm for them either (`ServiceAdapter.java:56-108`),
//! and the query is sent from this side with nobody waiting for the answer.
//!
//! # Two phases, held in values
//!
//! The reference's adapter calls straight into `ClusteredServiceAgent`, which
//! then does its work in `pollServiceAdapter` (`CSA:1157-1199`). Here the three
//! facts land in [`ServiceEvents`] and the agent applies them after the poll
//! returns, for the reason the consensus module's own adapter does the same:
//! `poll_subscription_controlled` borrows the client, and the work that follows
//! needs it too.

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message};
use deepmsg_cluster_common::decode::at_least;
use deepmsg_codec_cluster::boolean_type::BooleanType;
use deepmsg_codec_cluster::join_log_codec::JoinLogDecoder;
use deepmsg_codec_cluster::message_header_codec::{
    ENCODED_LENGTH as MESSAGE_HEADER_LENGTH, MessageHeaderDecoder,
};
use deepmsg_codec_cluster::request_service_ack_codec::RequestServiceAckDecoder;
use deepmsg_codec_cluster::service_termination_position_codec::ServiceTerminationPositionDecoder;
use deepmsg_codec_cluster::{ReadBuf, SBE_SCHEMA_ID};

use crate::error::ContainerError;

/// `ServiceAdapter.FRAGMENT_LIMIT`.
pub const FRAGMENT_LIMIT: usize = 1;

/// One `JoinLog(40)` (`CSA.onJoinLog`, `:449-471`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinLogEvent {
    /// `JoinLog.logPosition`: where the image must begin.
    pub log_position: i64,
    /// `JoinLog.maxLogPosition`: the end the log is known to reach.
    pub max_log_position: i64,
    /// The member this container is.
    pub member_id: i32,
    /// The image's session id — the key to wait for.
    pub log_session_id: i32,
    /// The stream the log rides.
    pub log_stream_id: i32,
    /// Whether this is a startup join rather than a role change.
    pub is_startup: bool,
    /// The member's role, as a code (`Cluster.Role`).
    pub role: i32,
    /// The standby flag; P3 keeps it and does not act on it.
    pub is_standby: bool,
    /// The channel to subscribe to.
    pub log_channel: String,
}

/// What a poll over the service stream found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ServiceEvents {
    /// The last `JoinLog` in the poll, if one arrived.
    pub join_log: Option<JoinLogEvent>,
    /// `(logPosition, ackRequired)` from `ServiceTerminationPosition(42)`.
    pub termination_position: Option<(i64, bool)>,
    /// `logPosition` from `RequestServiceAck(108)`.
    pub requested_ack_position: Option<i64>,
}

/// `ServiceAdapter` (`ServiceAdapter.java:27-110`).
#[derive(Clone, Copy, Debug, Default)]
pub struct ServiceAdapter {
    subscription_registration_id: Option<i64>,
}

impl ServiceAdapter {
    /// The subscription the client made on stream 104 (`CSA:193`).
    pub const fn new(subscription_registration_id: i64) -> Self {
        Self {
            subscription_registration_id: Some(subscription_registration_id),
        }
    }

    /// An adapter with no subscription, so a launch that has not made one yet
    /// polls nothing rather than panicking.
    pub const fn disconnected() -> Self {
        Self {
            subscription_registration_id: None,
        }
    }

    /// `ServiceAdapter.poll()` (`:51-54`).
    ///
    /// # Errors
    ///
    /// [`ContainerError`] from a message too short for its own header, or a
    /// foreign schema id — the reference's `ClusterException` out of the
    /// callback.
    pub fn poll(
        &self,
        client: &mut Client,
        events: &mut ServiceEvents,
    ) -> Result<usize, ContainerError> {
        let Some(subscription_id) = self.subscription_registration_id else {
            return Ok(0);
        };

        let mut dispatch = Dispatch {
            events,
            error: None,
        };
        let work =
            client.poll_subscription_controlled(subscription_id, FRAGMENT_LIMIT, &mut dispatch);
        if let Some(error) = dispatch.error {
            return Err(error);
        }
        Ok(work)
    }

    /// Decode one whole message into `events` — the body of `onFragment`, and
    /// what the dispatch tests drive directly.
    ///
    /// # Errors
    ///
    /// As [`Self::poll`].
    pub fn dispatch_message(
        events: &mut ServiceEvents,
        message: &[u8],
    ) -> Result<Action, ContainerError> {
        at_least(
            message,
            MESSAGE_HEADER_LENGTH,
            "cluster service control message header",
        )?;
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(message), 0);
        let template_id = header.template_id();
        let schema_id = header.schema_id();

        if schema_id != SBE_SCHEMA_ID {
            return Err(ContainerError::Cluster(format!(
                "expected schemaId={SBE_SCHEMA_ID}, actual={schema_id}"
            )));
        }

        match template_id {
            deepmsg_codec_cluster::join_log_codec::SBE_TEMPLATE_ID => {
                let mut decoder = JoinLogDecoder::default().header(header, 0);
                let coords = decoder.log_channel_decoder();
                let log_channel =
                    String::from_utf8_lossy(decoder.log_channel_slice(coords)).into_owned();
                events.join_log = Some(JoinLogEvent {
                    log_position: decoder.log_position(),
                    max_log_position: decoder.max_log_position(),
                    member_id: decoder.member_id(),
                    log_session_id: decoder.log_session_id(),
                    log_stream_id: decoder.log_stream_id(),
                    is_startup: decoder.is_startup() == BooleanType::TRUE,
                    role: decoder.role(),
                    is_standby: decoder.is_standby() == BooleanType::TRUE,
                    log_channel,
                });
                Ok(Action::Continue)
            }
            deepmsg_codec_cluster::service_termination_position_codec::SBE_TEMPLATE_ID => {
                let decoder = ServiceTerminationPositionDecoder::default().header(header, 0);
                events.termination_position = Some((
                    decoder.log_position(),
                    decoder.ack_required() == BooleanType::TRUE,
                ));
                Ok(Action::Continue)
            }
            deepmsg_codec_cluster::request_service_ack_codec::SBE_TEMPLATE_ID => {
                let decoder = RequestServiceAckDecoder::default().header(header, 0);
                events.requested_ack_position = Some(decoder.log_position());
                Ok(Action::Continue)
            }
            // The reference's switch has no default arm.
            _ => Ok(Action::Continue),
        }
    }
}

/// The controlled-poll callback.
struct Dispatch<'a> {
    events: &'a mut ServiceEvents,
    error: Option<ContainerError>,
}

impl ControlledHandler for Dispatch<'_> {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        match ServiceAdapter::dispatch_message(self.events, message.payload) {
            Ok(action) => action,
            Err(error) => {
                self.error = Some(error);
                Action::Abort
            }
        }
    }
}
