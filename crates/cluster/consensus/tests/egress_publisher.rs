//! `EgressPublisherTest` (`EgressPublisherTest.java:54-370`), one test per test.
//!
//! Each case drives the publisher with a session mock that answers a **planned
//! sequence** of claim/offer results, and then decodes what came out. That is
//! exactly what the reference's Mockito fixture does: the point of the test is
//! which path was taken (`tryClaim` or `offer`), how many attempts it took, and
//! that the bytes carry the fields the encoder was given.

use std::cell::{Cell, RefCell};

use deepmsg_cluster::session::egress::{EgressPublisher, EgressSession};
use deepmsg_codec_cluster::ReadBuf;
use deepmsg_codec_cluster::admin_request_type::AdminRequestType;
use deepmsg_codec_cluster::admin_response_code::AdminResponseCode;
use deepmsg_codec_cluster::admin_response_codec::AdminResponseDecoder;
use deepmsg_codec_cluster::challenge_codec::ChallengeDecoder;
use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_codec_cluster::message_header_codec::MessageHeaderDecoder;
use deepmsg_codec_cluster::new_leader_event_codec::NewLeaderEventDecoder;
use deepmsg_codec_cluster::session_event_codec::SessionEventDecoder;

const LEADER_HEARTBEAT_TIMEOUT_NS: i64 = 1_000_000_000;

/// A session whose two writes answer from a scripted list and record the bytes.
struct MockSession {
    id: i64,
    correlation_id: i64,
    max_payload_length: usize,
    claim_results: RefCell<Vec<bool>>,
    offer_results: RefCell<Vec<bool>>,
    claims: RefCell<Vec<Vec<u8>>>,
    offers: RefCell<Vec<Vec<u8>>>,
    claim_attempts: Cell<usize>,
    offer_attempts: Cell<usize>,
}

impl MockSession {
    fn new(id: i64, correlation_id: i64, max_payload_length: usize) -> Self {
        Self {
            id,
            correlation_id,
            max_payload_length,
            claim_results: RefCell::new(Vec::new()),
            offer_results: RefCell::new(Vec::new()),
            claims: RefCell::new(Vec::new()),
            offers: RefCell::new(Vec::new()),
            claim_attempts: Cell::new(0),
            offer_attempts: Cell::new(0),
        }
    }

    fn claims(mut self, results: &[bool]) -> Self {
        self.claim_results = RefCell::new(results.to_vec());
        self
    }

    fn offers(mut self, results: &[bool]) -> Self {
        self.offer_results = RefCell::new(results.to_vec());
        self
    }

    fn claim_count(&self) -> usize {
        self.claim_attempts.get()
    }

    fn offer_count(&self) -> usize {
        self.offer_attempts.get()
    }

    fn last_claim(&self) -> Vec<u8> {
        self.claims.borrow().last().cloned().expect("a claim")
    }

    fn last_offer(&self) -> Vec<u8> {
        self.offers.borrow().last().cloned().expect("an offer")
    }
}

impl EgressSession for MockSession {
    fn id(&self) -> i64 {
        self.id
    }

    fn correlation_id(&self) -> i64 {
        self.correlation_id
    }

    fn max_payload_length(&self) -> Option<usize> {
        Some(self.max_payload_length)
    }

    fn try_claim(&self, length: usize, bytes: &[u8]) -> bool {
        assert_eq!(length, bytes.len());
        self.claim_attempts.set(self.claim_attempts.get() + 1);
        let next = {
            let mut results = self.claim_results.borrow_mut();
            if results.is_empty() {
                false
            } else {
                results.remove(0)
            }
        };
        if next {
            self.claims.borrow_mut().push(bytes.to_vec());
        }
        next
    }

    fn offer(&self, bytes: &[u8]) -> bool {
        self.offer_attempts.set(self.offer_attempts.get() + 1);
        let next = {
            let mut results = self.offer_results.borrow_mut();
            if results.is_empty() {
                false
            } else {
                results.remove(0)
            }
        };
        if next {
            self.offers.borrow_mut().push(bytes.to_vec());
        }
        next
    }
}

fn session_event(bytes: &[u8]) -> SessionEventDecoder<'_> {
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);
    SessionEventDecoder::default().header(header, 0)
}

#[test]
fn send_event_should_use_try_claim_if_event_is_small() {
    let session = MockSession::new(17, 23, 2000).claims(&[false, true]);
    let mut publisher = EgressPublisher::new(LEADER_HEARTBEAT_TIMEOUT_NS);

    assert!(
        publisher
            .send_event(
                &session,
                7,
                1,
                EventCode::AUTHENTICATION_REJECTED,
                "a detail",
            )
            .unwrap()
    );

    assert_eq!(2, session.claim_count());
    assert_eq!(0, session.offer_count());

    let bytes = session.last_claim();
    let mut decoder = session_event(&bytes);
    assert_eq!(17, decoder.cluster_session_id());
    assert_eq!(23, decoder.correlation_id());
    assert_eq!(7, decoder.leadership_term_id());
    assert_eq!(1, decoder.leader_member_id());
    assert_eq!(EventCode::AUTHENTICATION_REJECTED, decoder.code());
    assert_eq!(
        Some(deepmsg_cluster::config::PROTOCOL_SEMANTIC_VERSION),
        decoder.version()
    );
    assert_eq!(
        Some(LEADER_HEARTBEAT_TIMEOUT_NS),
        decoder.leader_heartbeat_timeout_ns()
    );
    let detail = decoder.detail_decoder();
    assert_eq!(b"a detail", decoder.detail_slice(detail));
}

#[test]
fn send_event_should_use_offer_when_event_is_too_big() {
    let detail = "x".repeat(1000);
    let session = MockSession::new(17, 23, 100).offers(&[false, false, true]);
    let mut publisher = EgressPublisher::new(LEADER_HEARTBEAT_TIMEOUT_NS);

    assert!(
        publisher
            .send_event(&session, 7, 1, EventCode::ERROR, &detail)
            .unwrap()
    );

    assert_eq!(0, session.claim_count());
    assert_eq!(3, session.offer_count());

    let bytes = session.last_offer();
    let mut decoder = session_event(&bytes);
    assert_eq!(17, decoder.cluster_session_id());
    assert_eq!(EventCode::ERROR, decoder.code());
    let decoded = decoder.detail_decoder();
    assert_eq!(detail.as_bytes(), decoder.detail_slice(decoded));
}

#[test]
fn should_send_challenge_response_using_try_claim_if_small() {
    let challenge = vec![7_u8; 16];
    let session = MockSession::new(17, 23, 500).claims(&[false, false, true]);
    let mut publisher = EgressPublisher::new(LEADER_HEARTBEAT_TIMEOUT_NS);

    assert!(publisher.send_challenge(&session, &challenge).unwrap());

    assert_eq!(3, session.claim_count());
    assert_eq!(0, session.offer_count());

    let bytes = session.last_claim();
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    let mut decoder = ChallengeDecoder::default().header(header, 0);
    assert_eq!(23, decoder.correlation_id());
    assert_eq!(17, decoder.cluster_session_id());
    let decoded = decoder.encoded_challenge_decoder();
    assert_eq!(
        challenge.as_slice(),
        decoder.encoded_challenge_slice(decoded)
    );
}

#[test]
fn should_send_challenge_response_using_offer_if_large() {
    let challenge = vec![7_u8; 100];
    let session = MockSession::new(17, 23, 100).offers(&[false, true]);
    let mut publisher = EgressPublisher::new(LEADER_HEARTBEAT_TIMEOUT_NS);

    assert!(publisher.send_challenge(&session, &challenge).unwrap());

    assert_eq!(0, session.claim_count());
    assert_eq!(2, session.offer_count());

    let bytes = session.last_offer();
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    let mut decoder = ChallengeDecoder::default().header(header, 0);
    let decoded = decoder.encoded_challenge_decoder();
    assert_eq!(
        challenge.as_slice(),
        decoder.encoded_challenge_slice(decoded)
    );
}

#[test]
fn should_send_new_leader_event_using_try_claim_if_small() {
    let session = MockSession::new(17, 23, 500).claims(&[false, true]);
    let mut publisher = EgressPublisher::new(LEADER_HEARTBEAT_TIMEOUT_NS);

    assert!(
        publisher
            .new_leader(&session, 7, 1, "0=localhost:20123")
            .unwrap()
    );

    assert_eq!(2, session.claim_count());
    assert_eq!(0, session.offer_count());

    let bytes = session.last_claim();
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    let mut decoder = NewLeaderEventDecoder::default().header(header, 0);
    assert_eq!(7, decoder.leadership_term_id());
    assert_eq!(17, decoder.cluster_session_id());
    assert_eq!(1, decoder.leader_member_id());
    let endpoints = decoder.ingress_endpoints_decoder();
    assert_eq!(
        b"0=localhost:20123",
        decoder.ingress_endpoints_slice(endpoints)
    );
}

#[test]
fn should_send_new_leader_event_using_offer_if_too_big() {
    let endpoints = "e".repeat(1000);
    let session = MockSession::new(17, 23, 500).offers(&[false, false, true]);
    let mut publisher = EgressPublisher::new(LEADER_HEARTBEAT_TIMEOUT_NS);

    assert!(publisher.new_leader(&session, 7, 1, &endpoints).unwrap());

    assert_eq!(0, session.claim_count());
    assert_eq!(3, session.offer_count());

    let bytes = session.last_offer();
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    let mut decoder = NewLeaderEventDecoder::default().header(header, 0);
    let decoded = decoder.ingress_endpoints_decoder();
    assert_eq!(
        endpoints.as_bytes(),
        decoder.ingress_endpoints_slice(decoded)
    );
}

#[test]
fn should_send_admin_response_using_try_claim_if_small() {
    let session = MockSession::new(17, 23, 500).claims(&[false, false, true]);
    let mut publisher = EgressPublisher::new(LEADER_HEARTBEAT_TIMEOUT_NS);

    assert!(
        publisher
            .send_admin_response(
                &session,
                23,
                AdminRequestType::SNAPSHOT,
                AdminResponseCode::OK,
                "ok",
            )
            .unwrap()
    );

    assert_eq!(3, session.claim_count());
    assert_eq!(0, session.offer_count());

    let bytes = session.last_claim();
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    let mut decoder = AdminResponseDecoder::default().header(header, 0);
    assert_eq!(17, decoder.cluster_session_id());
    assert_eq!(23, decoder.correlation_id());
    assert_eq!(AdminRequestType::SNAPSHOT, decoder.request_type());
    assert_eq!(AdminResponseCode::OK, decoder.response_code());
    let message = decoder.message_decoder();
    assert_eq!(b"ok", decoder.message_slice(message));
    let payload = decoder.payload_decoder();
    assert!(decoder.payload_slice(payload).is_empty());
}

#[test]
fn should_send_admin_response_using_offer_if_too_big() {
    let message = "m".repeat(200);
    let session = MockSession::new(17, 23, 50).offers(&[false, true]);
    let mut publisher = EgressPublisher::new(LEADER_HEARTBEAT_TIMEOUT_NS);

    assert!(
        publisher
            .send_admin_response(
                &session,
                23,
                AdminRequestType::SNAPSHOT,
                AdminResponseCode::ERROR,
                &message,
            )
            .unwrap()
    );

    assert_eq!(0, session.claim_count());
    assert_eq!(2, session.offer_count());

    let bytes = session.last_offer();
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    let mut decoder = AdminResponseDecoder::default().header(header, 0);
    let decoded = decoder.message_decoder();
    assert_eq!(message.as_bytes(), decoder.message_slice(decoded));
    let payload = decoder.payload_decoder();
    assert!(decoder.payload_slice(payload).is_empty());
}
