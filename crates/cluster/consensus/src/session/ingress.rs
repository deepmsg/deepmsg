//! The client-facing edge of a cluster node (`IngressAdapter.java`).
//!
//! One subscription per transport — the cluster ingress channel, and an IPC one
//! when `aeron.cluster.ingress.ipc.allowed` lets clients use IPC — each polled
//! through the client's **controlled** face with a fragment limit of
//! `aeron.cluster.ingress.fragment.limit`. The assembler the client keeps per
//! subscription turns fragments into whole messages; this adapter turns whole
//! messages into calls on [`IngressHandler`].
//!
//! The seven dispatches are the reference's (`IngressAdapter.java:72-209`):
//! schema 111 template 1 is an application message and its handler's `Action`
//! is the poll's answer (so back-pressure can say `ABORT` and leave the message
//! unread); templates 3, 4, 5, 8 and 26 are the session messages; anything else
//! is an extension message. The reference returns `CONTINUE` for all of those.
//!
//! # Where the session's diagnostics come from
//!
//! A session's counter label contains the ingress image's `sourceIdentity` and
//! session id (`SessionManager.java:1174-1179`), which the reference reads from
//! the fragment's `Header.context()` — the image. The whole-message face here
//! hands over a `MessageHeader`, not an image, so the adapter reads the
//! subscription's images **before** polling and matches on the message header's
//! session id. That is the same number the image has, and it is the only thing
//! one image differs from another by on this channel.

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message};
use deepmsg_cluster_common::decode::at_least;
use deepmsg_codec_cluster::admin_request_codec::AdminRequestDecoder;
use deepmsg_codec_cluster::admin_request_type::AdminRequestType;
use deepmsg_codec_cluster::challenge_response_codec::ChallengeResponseDecoder;
use deepmsg_codec_cluster::message_header_codec::{
    ENCODED_LENGTH as MESSAGE_HEADER_LENGTH, MessageHeaderDecoder,
};
use deepmsg_codec_cluster::session_close_request_codec::SessionCloseRequestDecoder;
use deepmsg_codec_cluster::session_connect_request_codec::SessionConnectRequestDecoder;
use deepmsg_codec_cluster::session_keep_alive_codec::SessionKeepAliveDecoder;
use deepmsg_codec_cluster::session_message_header_codec::SessionMessageHeaderDecoder;
use deepmsg_codec_cluster::{ReadBuf, SBE_SCHEMA_ID};

use crate::error::ClusterError;
use crate::session::manager::SessionConnectRequest;

/// How long a driver command waits for its answer, as the agent's own
/// `COMMAND_TIMEOUT` does.
const COMMAND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// `AeronCluster.SESSION_HEADER_LENGTH`: the SBE header plus
/// `SessionMessageHeader`'s block (`AeronCluster.java:99-100`).
pub const SESSION_HEADER_LENGTH: usize = deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH
    + deepmsg_codec_cluster::session_message_header_codec::SBE_BLOCK_LENGTH as usize;

/// What the ingress poll asks of the consensus module — the reference's
/// `ConsensusModuleAgent` methods, as a trait so the dispatch can be driven by
/// `IngressAdapterTest`'s mock.
pub trait IngressHandler {
    /// `onExtensionMessage` (`ConsensusModuleAgent.java:650-672`).
    fn on_extension_message(
        &mut self,
        block_length: u16,
        template_id: u16,
        schema_id: u16,
        version: u16,
        payload: &[u8],
    ) -> Action;

    /// `onIngressMessage` (`ConsensusModuleAgent.java:828-854`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the log append. The reference throws out of
    /// `onFragment`, which aborts the poll; this propagates the same way.
    fn on_ingress_message(
        &mut self,
        leadership_term_id: i64,
        cluster_session_id: i64,
        payload: &[u8],
    ) -> Result<Action, ClusterError>;

    /// `onSessionConnect` (`ConsensusModuleAgent.java:750-770`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] when the session cannot be made.
    fn on_session_connect(&mut self, request: SessionConnectRequest) -> Result<(), ClusterError>;

    /// `onSessionClose` (`ConsensusModuleAgent.java:772-778`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the close append.
    fn on_session_close(
        &mut self,
        leadership_term_id: i64,
        cluster_session_id: i64,
    ) -> Result<(), ClusterError>;

    /// `onSessionKeepAlive` (`ConsensusModuleAgent.java:856-867`).
    ///
    /// # Errors
    ///
    /// Nothing today; the reference's body cannot fail.
    fn on_session_keep_alive(
        &mut self,
        leadership_term_id: i64,
        cluster_session_id: i64,
        image_session_id: i32,
    ) -> Result<(), ClusterError>;

    /// `onIngressChallengeResponse` (`ConsensusModuleAgent.java:869-881`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] when the challenge response cannot be handed on.
    fn on_challenge_response(
        &mut self,
        correlation_id: i64,
        cluster_session_id: i64,
        encoded_credentials: &[u8],
    ) -> Result<(), ClusterError>;

    /// `onAdminRequest` (`ConsensusModuleAgent.java:780-826`).
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the admin response.
    fn on_admin_request(
        &mut self,
        leadership_term_id: i64,
        cluster_session_id: i64,
        correlation_id: i64,
        request_type: AdminRequestType,
        payload: &[u8],
    ) -> Result<(), ClusterError>;
}

/// The ingress subscriptions (`IngressAdapter.java:29-41`).
#[derive(Clone, Debug, Default)]
pub struct IngressAdapter {
    udp_subscription_id: Option<i64>,
    ipc_subscription_id: Option<i64>,
    fragment_limit: usize,
    /// The subscription the two fields below describe, if any.
    cached_subscription_id: Option<i64>,
    /// `(registration id, session id)` of each image at the time the map was
    /// made: a comparison against the current images costs no allocation, and
    /// the map below is only rebuilt when one of these changed.
    cached_image_keys: Vec<(i64, i32)>,
    /// The image identities for `cached_subscription_id`: each image's
    /// `sourceIdentity`, by the image's session id.
    cached_image_identities: deepmsg_cluster_common::collections::HashMap<i32, String>,
}

impl IngressAdapter {
    /// `IngressAdapter(int fragmentPollLimit, ConsensusModuleAgent)`.
    #[must_use]
    pub fn new(fragment_limit: usize) -> Self {
        Self {
            udp_subscription_id: None,
            ipc_subscription_id: None,
            fragment_limit,
            cached_subscription_id: None,
            cached_image_keys: Vec::new(),
            cached_image_identities: deepmsg_cluster_common::collections::HashMap::default(),
        }
    }

    /// `connect(Subscription, Subscription)` (`IngressAdapter.java:211-215`).
    pub fn connect(&mut self, udp_subscription_id: Option<i64>, ipc_subscription_id: Option<i64>) {
        self.udp_subscription_id = udp_subscription_id;
        self.ipc_subscription_id = ipc_subscription_id;
    }

    /// Whether any subscription exists.
    pub const fn is_connected(&self) -> bool {
        self.udp_subscription_id.is_some() || self.ipc_subscription_id.is_some()
    }

    /// `CloseHelper.close(ingressAdapter)` (`ConsensusModuleAgent.java:1363`):
    /// give both subscriptions back. A new election's `connectIngress` makes
    /// its own.
    ///
    /// The reference closes the adapter objects and does not wait; a
    /// `REMOVE_SUBSCRIPTION` here waits for the driver's answer, which is this
    /// client's face for it.
    pub fn close(&mut self, client: &mut Client) {
        for subscription_id in [
            self.udp_subscription_id.take(),
            self.ipc_subscription_id.take(),
        ]
        .into_iter()
        .flatten()
        {
            let _ = client.remove_subscription(subscription_id, COMMAND_TIMEOUT);
        }
        self.cached_subscription_id = None;
        self.cached_image_keys.clear();
        self.cached_image_identities.clear();
    }

    /// The fragment limit.
    pub const fn fragment_limit(&self) -> usize {
        self.fragment_limit
    }

    /// `IngressAdapter.poll` (`IngressAdapter.java:217-232`): both subscriptions,
    /// UDP first, each polled once with the same limit.
    ///
    /// # Errors
    ///
    /// The first [`ClusterError`] a handler raised. The controlled poll itself
    /// cannot fail; a handler's error is carried out of the callback and
    /// propagated here, which is what the reference's exception does.
    pub fn poll(
        &mut self,
        client: &mut Client,
        handler: &mut dyn IngressHandler,
    ) -> Result<usize, ClusterError> {
        let mut work = 0;
        for subscription_id in [self.udp_subscription_id, self.ipc_subscription_id]
            .into_iter()
            .flatten()
        {
            self.sync_image_identities(client, subscription_id);
            let mut dispatch = Dispatch {
                handler,
                images: &self.cached_image_identities,
                error: None,
            };
            work += client.poll_subscription_controlled(
                subscription_id,
                self.fragment_limit,
                &mut dispatch,
            );
            if let Some(error) = dispatch.error {
                return Err(error);
            }
        }
        Ok(work)
    }

    /// Bring the cached image identities up to date, without allocating when
    /// nothing changed.
    ///
    /// A steady-state poll runs this every duty cycle, so the comparison walks
    /// the current images against the cached `(registration id, session id)`
    /// pairs and answers early; only a new or departed image rebuilds the map
    /// (session establishment is one of the allocation exceptions, performance
    /// spec §2).
    fn sync_image_identities(&mut self, client: &Client, subscription_id: i64) {
        let mut matches = self.cached_subscription_id == Some(subscription_id);
        let mut count = 0;

        if let Some(subscription) = client.subscription(subscription_id) {
            for image in subscription.images() {
                if matches
                    && self.cached_image_keys.get(count)
                        != Some(&(image.registration_id(), image.session_id()))
                {
                    matches = false;
                }
                count += 1;
            }
        }

        if matches && count == self.cached_image_keys.len() {
            return;
        }

        self.cached_subscription_id = Some(subscription_id);
        self.cached_image_keys.clear();
        self.cached_image_identities.clear();
        if let Some(subscription) = client.subscription(subscription_id) {
            for image in subscription.images() {
                self.cached_image_keys
                    .push((image.registration_id(), image.session_id()));
                self.cached_image_identities
                    .entry(image.session_id())
                    .or_insert_with(|| image.source_identity().to_owned());
            }
        }
    }
}

/// The controlled-poll callback: decode a whole message and hand it on.
struct Dispatch<'a> {
    handler: &'a mut dyn IngressHandler,
    /// The adapter's cached image identities; read only by a session
    /// connect, and kept across polls so a steady-state poll allocates nothing
    /// (performance spec R1).
    images: &'a deepmsg_cluster_common::collections::HashMap<i32, String>,
    error: Option<ClusterError>,
}

impl ControlledHandler for Dispatch<'_> {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        match self.dispatch(message) {
            // The handler's own action is the back-pressure contract
            // (`ConsensusModuleAgent.onIngressMessage`'s `else { return ABORT; }`,
            // `:849-852`). An `ABORT` leaves the fragment **unconsumed**, so the
            // next poll reads it again — which is how a message the leader
            // cannot append *yet* (a log publication momentarily out of room,
            // `aeron_network_publication.c:995-1001`) survives instead of being
            // dropped on the floor. Answering `Continue` here consumed it.
            Ok(action) => action,
            Err(error) => {
                self.error = Some(error);
                Action::Abort
            }
        }
    }
}

impl Dispatch<'_> {
    fn dispatch(&mut self, message: Message<'_>) -> Result<Action, ClusterError> {
        dispatch_message(
            message.payload,
            message.header.session_id,
            self.images,
            self.handler,
        )
    }
}

/// Decode one whole message and hand it on (`IngressAdapter.onMessage`,
/// `IngressAdapter.java:72-209`).
///
/// `images` carries the `sourceIdentity` of each image on the subscription, by
/// the image's session id; an application message's diagnostic text uses the
/// one its image is known by.
///
/// # Errors
///
/// [`ClusterError`] from a handler or from a message too short to hold its own
/// header — the reference's exception out of the callback, in the same place.
pub fn dispatch_message(
    payload: &[u8],
    image_session_id: i32,
    images: &deepmsg_cluster_common::collections::HashMap<i32, String>,
    handler: &mut dyn IngressHandler,
) -> Result<Action, ClusterError> {
    at_least(payload, MESSAGE_HEADER_LENGTH, "cluster message header")?;
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
    let block_length = header.block_length();
    let template_id = header.template_id();
    let schema_id = header.schema_id();
    let version = header.version();

    if schema_id != SBE_SCHEMA_ID {
        return Ok(handler.on_extension_message(
            block_length,
            template_id,
            schema_id,
            version,
            payload,
        ));
    }

    match template_id {
        deepmsg_codec_cluster::session_message_header_codec::SBE_TEMPLATE_ID => {
            at_least(payload, SESSION_HEADER_LENGTH, "SessionMessageHeader")?;
            let decoder = SessionMessageHeaderDecoder::default().header(header, 0);
            let leadership_term_id = decoder.leadership_term_id();
            let cluster_session_id = decoder.cluster_session_id();
            let _timestamp = decoder.timestamp();
            handler.on_ingress_message(
                leadership_term_id,
                cluster_session_id,
                &payload[SESSION_HEADER_LENGTH..],
            )
        }
        deepmsg_codec_cluster::session_connect_request_codec::SBE_TEMPLATE_ID => {
            let mut decoder = SessionConnectRequestDecoder::default().header(header, 0);
            let correlation_id = decoder.correlation_id();
            let response_stream_id = decoder.response_stream_id();
            let client_version = decoder.version();
            let channel_coords = decoder.response_channel_decoder();
            let credentials_coords = decoder.encoded_credentials_decoder();
            let info_coords = decoder.client_info_decoder();
            let response_channel = decoder.response_channel_slice(channel_coords);
            let encoded_credentials = decoder.encoded_credentials_slice(credentials_coords);
            let client_info = decoder.client_info_slice(info_coords);
            let response_channel = String::from_utf8_lossy(response_channel).into_owned();
            let client_info = String::from_utf8_lossy(client_info).into_owned();
            let encoded_credentials = encoded_credentials.to_vec();
            let source_identity = images.get(&image_session_id).cloned().unwrap_or_default();
            handler.on_session_connect(SessionConnectRequest {
                correlation_id,
                response_stream_id,
                version: client_version,
                response_channel,
                encoded_credentials,
                client_info,
                source_identity,
                image_session_id,
            })?;
            Ok(Action::Continue)
        }
        deepmsg_codec_cluster::session_close_request_codec::SBE_TEMPLATE_ID => {
            let decoder = SessionCloseRequestDecoder::default().header(header, 0);
            let leadership_term_id = decoder.leadership_term_id();
            let cluster_session_id = decoder.cluster_session_id();
            handler.on_session_close(leadership_term_id, cluster_session_id)?;
            Ok(Action::Continue)
        }
        deepmsg_codec_cluster::session_keep_alive_codec::SBE_TEMPLATE_ID => {
            let decoder = SessionKeepAliveDecoder::default().header(header, 0);
            let leadership_term_id = decoder.leadership_term_id();
            let cluster_session_id = decoder.cluster_session_id();
            handler.on_session_keep_alive(
                leadership_term_id,
                cluster_session_id,
                image_session_id,
            )?;
            Ok(Action::Continue)
        }
        deepmsg_codec_cluster::challenge_response_codec::SBE_TEMPLATE_ID => {
            let mut decoder = ChallengeResponseDecoder::default().header(header, 0);
            let correlation_id = decoder.correlation_id();
            let cluster_session_id = decoder.cluster_session_id();
            let credentials_coords = decoder.encoded_credentials_decoder();
            let encoded_credentials = decoder.encoded_credentials_slice(credentials_coords);
            handler.on_challenge_response(
                correlation_id,
                cluster_session_id,
                encoded_credentials,
            )?;
            Ok(Action::Continue)
        }
        deepmsg_codec_cluster::admin_request_codec::SBE_TEMPLATE_ID => {
            let mut decoder = AdminRequestDecoder::default().header(header, 0);
            let leadership_term_id = decoder.leadership_term_id();
            let cluster_session_id = decoder.cluster_session_id();
            let correlation_id = decoder.correlation_id();
            let request_type = decoder.request_type();
            let payload_coords = decoder.payload_decoder();
            let payload = decoder.payload_slice(payload_coords);
            handler.on_admin_request(
                leadership_term_id,
                cluster_session_id,
                correlation_id,
                request_type,
                payload,
            )?;
            Ok(Action::Continue)
        }
        _ => Ok(handler.on_extension_message(
            block_length,
            template_id,
            schema_id,
            version,
            payload,
        )),
    }
}

#[cfg(test)]
mod tests {
    //! What the dispatch layer is told to do with a message is what happens to
    //! that message. The reference's contract, in one line
    //! (`ConsensusModuleAgent.java:849-852`): a leader that cannot append answers
    //! `ABORT`, and `ABORT` means the frame is **not** consumed.

    use super::*;
    use deepmsg_client::fragment_assembler::MessageHeader;
    use deepmsg_codec_cluster::WriteBuf;
    use deepmsg_codec_cluster::session_message_header_codec::SessionMessageHeaderEncoder;

    /// A handler that answers every ingress message the same way, and remembers
    /// what it was told.
    struct Answers {
        action: Action,
        seen: Vec<(i64, i64)>,
    }

    impl IngressHandler for Answers {
        fn on_extension_message(&mut self, _: u16, _: u16, _: u16, _: u16, _: &[u8]) -> Action {
            Action::Continue
        }

        fn on_ingress_message(
            &mut self,
            leadership_term_id: i64,
            cluster_session_id: i64,
            _payload: &[u8],
        ) -> Result<Action, ClusterError> {
            self.seen.push((leadership_term_id, cluster_session_id));
            Ok(self.action)
        }

        fn on_session_connect(&mut self, _: SessionConnectRequest) -> Result<(), ClusterError> {
            Ok(())
        }

        fn on_session_close(&mut self, _: i64, _: i64) -> Result<(), ClusterError> {
            Ok(())
        }

        fn on_session_keep_alive(&mut self, _: i64, _: i64, _: i32) -> Result<(), ClusterError> {
            Ok(())
        }

        fn on_challenge_response(&mut self, _: i64, _: i64, _: &[u8]) -> Result<(), ClusterError> {
            Ok(())
        }

        fn on_admin_request(
            &mut self,
            _: i64,
            _: i64,
            _: i64,
            _: AdminRequestType,
            _: &[u8],
        ) -> Result<(), ClusterError> {
            Ok(())
        }
    }

    /// One whole `SessionMessageHeader` message, header and all — the shape the
    /// ingress subscription hands the dispatcher.
    fn a_session_message(leadership_term_id: i64, cluster_session_id: i64) -> Vec<u8> {
        let payload = b"echo me";
        let mut buffer = vec![0_u8; SESSION_HEADER_LENGTH + payload.len()];
        {
            let encoder = SessionMessageHeaderEncoder::default()
                .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().expect("the encoder the header wrapped");
            encoder
                .cluster_session_id(cluster_session_id)
                .leadership_term_id(leadership_term_id)
                .timestamp(77);
        }
        buffer[SESSION_HEADER_LENGTH..].copy_from_slice(payload);
        buffer
    }

    /// Dispatch one message through a handler that answers `answer`, and report
    /// what the dispatcher answered the reader.
    fn dispatched(answer: Action) -> Action {
        let mut handler = Answers {
            action: answer,
            seen: Vec::new(),
        };
        let images = deepmsg_cluster_common::collections::HashMap::default();
        let message = a_session_message(2, 7);

        let had_error = {
            let mut dispatch = Dispatch {
                handler: &mut handler,
                images: &images,
                error: None,
            };
            let answered = dispatch.on_message(Message {
                header: MessageHeader {
                    session_id: 1,
                    stream_id: 100,
                    term_offset: 0,
                    flags: 0,
                    position: 0,
                    frame_length: 0,
                    fragmented_frame_length: 0,
                    image_registration_id: 1,
                },
                payload: &message,
            });
            assert!(dispatch.error.is_none(), "an answer is not an error");
            answered
        };

        assert_eq!(
            vec![(2, 7)],
            handler.seen,
            "the handler saw the message the message header named"
        );
        had_error
    }

    #[test]
    fn an_aborted_ingress_message_is_not_consumed() {
        // The whole of `ABORT`: the reader's position stays where it was, so the
        // same frame arrives again. Answering `Continue` consumed a message the
        // leader never appended — that is a lost client message, not back
        // pressure.
        assert_eq!(Action::Abort, dispatched(Action::Abort));
    }

    #[test]
    fn a_continued_ingress_message_is_consumed() {
        assert_eq!(Action::Continue, dispatched(Action::Continue));
    }
}
