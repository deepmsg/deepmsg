//! `IngressAdapterTest.shouldDelegateToConsensusModuleAgent`
//! (`IngressAdapterTest.java:57-163`), all seven of its parameterised cases.
//!
//! The reference builds each message with the generated encoder and calls
//! `adapter.onMessage(buffer, 0, length, header)`, then verifies the one method
//! on the mocked `ConsensusModuleAgent` that should have been called. Here the
//! same messages go through [`dispatch_message`] — the body of the adapter's
//! `onMessage` — with a handler that records the calls.

use std::cell::RefCell;

use deepmsg_client::fragment_assembler::Action;
use deepmsg_cluster::session::ingress::{IngressHandler, dispatch_message};
use deepmsg_cluster::session::manager::SessionConnectRequest;
use deepmsg_codec_cluster::WriteBuf;
use deepmsg_codec_cluster::admin_request_codec::AdminRequestEncoder;
use deepmsg_codec_cluster::admin_request_type::AdminRequestType;
use deepmsg_codec_cluster::challenge_response_codec::ChallengeResponseEncoder;
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH;
use deepmsg_codec_cluster::session_close_request_codec::SessionCloseRequestEncoder;
use deepmsg_codec_cluster::session_connect_request_codec::SessionConnectRequestEncoder;
use deepmsg_codec_cluster::session_keep_alive_codec::SessionKeepAliveEncoder;
use deepmsg_codec_cluster::session_message_header_codec::SessionMessageHeaderEncoder;

/// What the mock saw.
#[derive(Debug, PartialEq, Eq)]
enum Call {
    Extension(u16, u16, u16, u16),
    Ingress(i64, i64, Vec<u8>),
    Connect(SessionConnectRequest),
    Close(i64, i64),
    KeepAlive(i64, i64, i32),
    Challenge(i64, i64, Vec<u8>),
    Admin(i64, i64, i64, AdminRequestType, Vec<u8>),
}

#[derive(Default)]
struct MockHandler {
    calls: RefCell<Vec<Call>>,
    action: Option<Action>,
}

impl IngressHandler for MockHandler {
    fn on_extension_message(
        &mut self,
        block_length: u16,
        template_id: u16,
        schema_id: u16,
        version: u16,
        _payload: &[u8],
    ) -> Action {
        self.calls.borrow_mut().push(Call::Extension(
            block_length,
            template_id,
            schema_id,
            version,
        ));
        Action::Continue
    }

    fn on_ingress_message(
        &mut self,
        leadership_term_id: i64,
        cluster_session_id: i64,
        payload: &[u8],
    ) -> Result<Action, deepmsg_cluster::ClusterError> {
        self.calls.borrow_mut().push(Call::Ingress(
            leadership_term_id,
            cluster_session_id,
            payload.to_vec(),
        ));
        Ok(self.action.unwrap_or(Action::Continue))
    }

    fn on_session_connect(
        &mut self,
        request: SessionConnectRequest,
    ) -> Result<(), deepmsg_cluster::ClusterError> {
        self.calls.borrow_mut().push(Call::Connect(request));
        Ok(())
    }

    fn on_session_close(
        &mut self,
        leadership_term_id: i64,
        cluster_session_id: i64,
    ) -> Result<(), deepmsg_cluster::ClusterError> {
        self.calls
            .borrow_mut()
            .push(Call::Close(leadership_term_id, cluster_session_id));
        Ok(())
    }

    fn on_session_keep_alive(
        &mut self,
        leadership_term_id: i64,
        cluster_session_id: i64,
        image_session_id: i32,
    ) -> Result<(), deepmsg_cluster::ClusterError> {
        self.calls.borrow_mut().push(Call::KeepAlive(
            leadership_term_id,
            cluster_session_id,
            image_session_id,
        ));
        Ok(())
    }

    fn on_challenge_response(
        &mut self,
        correlation_id: i64,
        cluster_session_id: i64,
        encoded_credentials: &[u8],
    ) -> Result<(), deepmsg_cluster::ClusterError> {
        self.calls.borrow_mut().push(Call::Challenge(
            correlation_id,
            cluster_session_id,
            encoded_credentials.to_vec(),
        ));
        Ok(())
    }

    fn on_admin_request(
        &mut self,
        leadership_term_id: i64,
        cluster_session_id: i64,
        correlation_id: i64,
        request_type: AdminRequestType,
        payload: &[u8],
    ) -> Result<(), deepmsg_cluster::ClusterError> {
        self.calls.borrow_mut().push(Call::Admin(
            leadership_term_id,
            cluster_session_id,
            correlation_id,
            request_type,
            payload.to_vec(),
        ));
        Ok(())
    }
}

fn dispatch(payload: &[u8]) -> Vec<Call> {
    let mut handler = MockHandler::default();
    let images = deepmsg_cluster_common::collections::HashMap::default();
    dispatch_message(payload, 1, &images, &mut handler).expect("the message dispatches");
    handler.calls.into_inner()
}

#[test]
fn should_delegate_unknown_schema_to_the_extension_hook() {
    // A message header for schema 17, template 19, acting block length 0.
    let mut buffer = [0_u8; ENCODED_LENGTH];
    buffer[0..2].copy_from_slice(&0_u16.to_le_bytes());
    buffer[2..4].copy_from_slice(&19_u16.to_le_bytes());
    buffer[4..6].copy_from_slice(&17_u16.to_le_bytes());
    buffer[6..8].copy_from_slice(&0_u16.to_le_bytes());

    assert_eq!(vec![Call::Extension(0, 19, 17, 0)], dispatch(&buffer));
}

#[test]
fn should_delegate_session_message_header_to_on_ingress_message() {
    // The action the handler answers is the poll's action; `ABORT` is what a
    // full log answer looks like on this path (`ConsensusModuleAgent.java:848`,
    // `onIngressMessage` returning `ABORT`).
    let mut buffer = [0_u8; 128];
    let encoder =
        SessionMessageHeaderEncoder::default().wrap(WriteBuf::new(&mut buffer), ENCODED_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .leadership_term_id(17)
        .cluster_session_id(19)
        .timestamp(23);
    let length = ENCODED_LENGTH + message.encoded_length();
    let mut payload = buffer[..length].to_vec();
    payload.extend_from_slice(b"body");

    assert_eq!(
        vec![Call::Ingress(17, 19, b"body".to_vec())],
        dispatch(&payload)
    );
}

#[test]
fn an_ingress_message_the_handler_aborts_is_aborted_by_the_adapter() {
    let mut buffer = [0_u8; 128];
    let encoder =
        SessionMessageHeaderEncoder::default().wrap(WriteBuf::new(&mut buffer), ENCODED_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .leadership_term_id(17)
        .cluster_session_id(19)
        .timestamp(23);
    let length = ENCODED_LENGTH + message.encoded_length();

    let mut handler = MockHandler {
        action: Some(Action::Abort),
        ..MockHandler::default()
    };
    let images = deepmsg_cluster_common::collections::HashMap::default();
    assert_eq!(
        Action::Abort,
        dispatch_message(&buffer[..length], 1, &images, &mut handler).expect("it dispatches")
    );
}

#[test]
fn should_delegate_session_connect_request_to_on_session_connect() {
    let mut buffer = [0_u8; 256];
    let encoder =
        SessionConnectRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), ENCODED_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .correlation_id(17)
        .response_stream_id(19)
        .version(23)
        .response_channel(b"x")
        .encoded_credentials(b"")
        .client_info(b"test");
    let length = ENCODED_LENGTH + message.encoded_length();

    let calls = dispatch(&buffer[..length]);
    match &calls[..] {
        [Call::Connect(request)] => {
            assert_eq!(17, request.correlation_id);
            assert_eq!(19, request.response_stream_id);
            assert_eq!(Some(23), request.version);
            assert_eq!("x", request.response_channel);
            assert!(request.encoded_credentials.is_empty());
            assert_eq!("test", request.client_info);
        }
        other => panic!("unexpected calls: {other:?}"),
    }
}

#[test]
fn should_delegate_session_close_request_to_on_session_close() {
    let mut buffer = [0_u8; 128];
    let encoder =
        SessionCloseRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), ENCODED_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message.leadership_term_id(17).cluster_session_id(19);
    let length = ENCODED_LENGTH + message.encoded_length();

    assert_eq!(vec![Call::Close(17, 19)], dispatch(&buffer[..length]));
}

#[test]
fn should_delegate_session_keep_alive_to_on_session_keep_alive() {
    let mut buffer = [0_u8; 128];
    let encoder =
        SessionKeepAliveEncoder::default().wrap(WriteBuf::new(&mut buffer), ENCODED_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message.leadership_term_id(17).cluster_session_id(19);
    let length = ENCODED_LENGTH + message.encoded_length();

    assert_eq!(
        vec![Call::KeepAlive(17, 19, 1)],
        dispatch(&buffer[..length])
    );
}

#[test]
fn should_delegate_challenge_response_to_on_ingress_challenge_response() {
    let mut buffer = [0_u8; 128];
    let encoder =
        ChallengeResponseEncoder::default().wrap(WriteBuf::new(&mut buffer), ENCODED_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .correlation_id(17)
        .cluster_session_id(19)
        .encoded_credentials(b"foo");
    let length = ENCODED_LENGTH + message.encoded_length();

    assert_eq!(
        vec![Call::Challenge(17, 19, b"foo".to_vec())],
        dispatch(&buffer[..length])
    );
}

#[test]
fn should_delegate_admin_request_to_on_admin_request() {
    let mut buffer = [0_u8; 256];
    let encoder = AdminRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), ENCODED_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .leadership_term_id(17)
        .cluster_session_id(19)
        .correlation_id(23)
        .request_type(AdminRequestType::SNAPSHOT)
        .payload(b"abc");
    let length = ENCODED_LENGTH + message.encoded_length();

    assert_eq!(
        vec![Call::Admin(
            17,
            19,
            23,
            AdminRequestType::SNAPSHOT,
            b"abc".to_vec()
        )],
        dispatch(&buffer[..length])
    );
}
