//! What a service tells the consensus module (`ConsensusModuleAdapter.java`).
//!
//! One subscription on the control channel's consensus-module stream, polled
//! with a fragment limit of [`FRAGMENT_LIMIT`] = 25, dispatching six message
//! kinds:
//!
//! | Template | Message | Handler |
//! |---|---|---|
//! | 1 | `SessionMessageHeader` | `on_service_message` |
//! | 30 | `CloseSession` | `on_service_close_session` |
//! | 31 | `ScheduleTimer` | `on_schedule_timer` |
//! | 32 | `CancelTimer` | `on_cancel_timer` |
//! | 33 | `ServiceAck` | `on_service_ack` — and the poll **stops** (`BREAK`) |
//! | 34 | `ClusterMembersQuery` | `on_cluster_members_query` |
//!
//! The ack's `BREAK` is not an optimisation: a service ack changes the module's
//! state (snapshot, termination), and the reference stops reading the stream at
//! that point so the rest of the messages are handled against the new state.

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message};
use deepmsg_cluster_common::decode::at_least;
use deepmsg_codec_cluster::cancel_timer_codec::CancelTimerDecoder;
use deepmsg_codec_cluster::close_session_codec::CloseSessionDecoder;
use deepmsg_codec_cluster::cluster_members_query_codec::ClusterMembersQueryDecoder;
use deepmsg_codec_cluster::message_header_codec::{
    ENCODED_LENGTH as MESSAGE_HEADER_LENGTH, MessageHeaderDecoder,
};
use deepmsg_codec_cluster::schedule_timer_codec::ScheduleTimerDecoder;
use deepmsg_codec_cluster::service_ack_codec::ServiceAckDecoder;
use deepmsg_codec_cluster::session_message_header_codec::SessionMessageHeaderDecoder;
use deepmsg_codec_cluster::{ReadBuf, SBE_SCHEMA_ID};

use crate::error::ClusterError;

/// `ConsensusModuleAdapter.FRAGMENT_LIMIT` (`ConsensusModuleAdapter.java:29`).
pub const FRAGMENT_LIMIT: usize = 25;

/// The six dispatches a service can make (`ConsensusModuleAdapter.java:59-151`).
pub trait ServiceControlHandler {
    /// Template 1: a message the service wants in the log. `message` is the
    /// whole queued message (SBE header + `SessionMessageHeader` + payload).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] when the tracker cannot queue it.
    fn on_service_message(
        &mut self,
        cluster_session_id: i64,
        message: &[u8],
    ) -> Result<(), ClusterError>;

    /// Template 30.
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the session close.
    fn on_service_close_session(&mut self, cluster_session_id: i64) -> Result<(), ClusterError>;

    /// Template 31.
    ///
    /// # Errors
    ///
    /// Nothing today; the reference's schedule cannot fail.
    fn on_schedule_timer(&mut self, correlation_id: i64, deadline: i64)
    -> Result<(), ClusterError>;

    /// Template 32.
    ///
    /// # Errors
    ///
    /// Nothing today.
    fn on_cancel_timer(&mut self, correlation_id: i64) -> Result<(), ClusterError>;

    /// Template 33: the poll stops after this one.
    ///
    /// # Errors
    ///
    /// [`ClusterError`] when an ack is out of sequence.
    fn on_service_ack(
        &mut self,
        log_position: i64,
        timestamp: i64,
        ack_id: i64,
        relevant_id: i64,
        service_id: i32,
    ) -> Result<Action, ClusterError>;

    /// Template 34.
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the membership response.
    fn on_cluster_members_query(
        &mut self,
        correlation_id: i64,
        is_extended_request: bool,
    ) -> Result<(), ClusterError>;
}

/// `ConsensusModuleAdapter` (`ConsensusModuleAdapter.java:27-151`).
#[derive(Clone, Copy, Debug, Default)]
pub struct ServiceControlAdapter {
    subscription_id: Option<i64>,
}

impl ServiceControlAdapter {
    /// `new ConsensusModuleAdapter(Subscription, ConsensusModuleAgent)`.
    pub const fn new(subscription_id: Option<i64>) -> Self {
        Self { subscription_id }
    }

    /// Whether the control subscription exists.
    pub const fn is_connected(&self) -> bool {
        self.subscription_id.is_some()
    }

    /// `poll()` (`:53-56`).
    ///
    /// # Errors
    ///
    /// The first error a handler raised; the controlled poll itself cannot
    /// fail.
    pub fn poll(
        &self,
        client: &mut Client,
        handler: &mut dyn ServiceControlHandler,
    ) -> Result<usize, ClusterError> {
        let Some(subscription_id) = self.subscription_id else {
            return Ok(0);
        };

        let mut dispatch = Dispatch {
            handler,
            error: None,
        };
        let work =
            client.poll_subscription_controlled(subscription_id, FRAGMENT_LIMIT, &mut dispatch);
        if let Some(error) = dispatch.error {
            return Err(error);
        }
        Ok(work)
    }

    /// Decode and dispatch one whole message — the body of `onFragment`, and
    /// what the dispatch tests drive directly.
    ///
    /// # Errors
    ///
    /// [`ClusterError`] for a foreign schema, a message too short for its own
    /// header, or a handler's own error.
    pub fn dispatch_message(
        &self,
        handler: &mut dyn ServiceControlHandler,
        message: &[u8],
    ) -> Result<Action, ClusterError> {
        at_least(
            message,
            MESSAGE_HEADER_LENGTH,
            "cluster control message header",
        )?;
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(message), 0);
        let template_id = header.template_id();
        let schema_id = header.schema_id();

        if schema_id != SBE_SCHEMA_ID {
            return Err(ClusterError::Cluster(format!(
                "expected schemaId={SBE_SCHEMA_ID}, actual={schema_id}"
            )));
        }

        match template_id {
            deepmsg_codec_cluster::session_message_header_codec::SBE_TEMPLATE_ID => {
                let decoder = SessionMessageHeaderDecoder::default().header(header, 0);
                let cluster_session_id = decoder.cluster_session_id();
                handler.on_service_message(cluster_session_id, message)?;
                Ok(Action::Continue)
            }
            deepmsg_codec_cluster::close_session_codec::SBE_TEMPLATE_ID => {
                let decoder = CloseSessionDecoder::default().header(header, 0);
                handler.on_service_close_session(decoder.cluster_session_id())?;
                Ok(Action::Continue)
            }
            deepmsg_codec_cluster::schedule_timer_codec::SBE_TEMPLATE_ID => {
                let decoder = ScheduleTimerDecoder::default().header(header, 0);
                handler.on_schedule_timer(decoder.correlation_id(), decoder.deadline())?;
                Ok(Action::Continue)
            }
            deepmsg_codec_cluster::cancel_timer_codec::SBE_TEMPLATE_ID => {
                let decoder = CancelTimerDecoder::default().header(header, 0);
                handler.on_cancel_timer(decoder.correlation_id())?;
                Ok(Action::Continue)
            }
            deepmsg_codec_cluster::service_ack_codec::SBE_TEMPLATE_ID => {
                let decoder = ServiceAckDecoder::default().header(header, 0);
                handler.on_service_ack(
                    decoder.log_position(),
                    decoder.timestamp(),
                    decoder.ack_id(),
                    decoder.relevant_id(),
                    decoder.service_id(),
                )
            }
            deepmsg_codec_cluster::cluster_members_query_codec::SBE_TEMPLATE_ID => {
                let decoder = ClusterMembersQueryDecoder::default().header(header, 0);
                let extended =
                    decoder.extended() == deepmsg_codec_cluster::boolean_type::BooleanType::TRUE;
                handler.on_cluster_members_query(decoder.correlation_id(), extended)?;
                Ok(Action::Continue)
            }
            // The reference has no default arm: an unknown template is
            // silently continued.
            _ => Ok(Action::Continue),
        }
    }
}

/// The controlled-poll callback.
struct Dispatch<'a> {
    handler: &'a mut dyn ServiceControlHandler,
    error: Option<ClusterError>,
}

impl ControlledHandler for Dispatch<'_> {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        let adapter = ServiceControlAdapter::default();
        match adapter.dispatch_message(self.handler, message.payload) {
            Ok(action) => action,
            Err(error) => {
                self.error = Some(error);
                Action::Abort
            }
        }
    }
}
