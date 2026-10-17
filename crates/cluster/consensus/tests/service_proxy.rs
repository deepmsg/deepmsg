//! `ServiceProxyTest` (`ServiceProxyTest.java:51-168`) — the four send cases.
//!
//! The reference mocks `ConcurrentPublication` and the error handler; here a
//! [`MockSend`] answers a scripted sequence of claim/offer results and records
//! what was written. The failure cases assert what the reference asserts: five
//! attempts and a `ClusterEvent` (here a `false` from the response methods,
//! which the agent turns into the error-counter increment).

use std::cell::RefCell;

use deepmsg_cluster::service_control::proxy::{ServiceProxy, ServiceSend};
use deepmsg_cluster_common::member::ClusterMember;
use deepmsg_codec_cluster::ReadBuf;
use deepmsg_codec_cluster::cluster_members_extended_response_codec::ClusterMembersExtendedResponseDecoder;
use deepmsg_codec_cluster::cluster_members_response_codec::ClusterMembersResponseDecoder;
use deepmsg_codec_cluster::message_header_codec::MessageHeaderDecoder;

#[derive(Default)]
struct MockSend {
    /// The answer each attempt gets; the last one repeats.
    answers: RefCell<Vec<bool>>,
    claims: RefCell<Vec<Vec<u8>>>,
    offers: RefCell<Vec<Vec<u8>>>,
    claim_attempts: std::cell::Cell<usize>,
    offer_attempts: std::cell::Cell<usize>,
    closed: bool,
}

impl MockSend {
    fn answering(answers: &[bool]) -> Self {
        Self {
            answers: RefCell::new(answers.to_vec()),
            ..Self::default()
        }
    }

    fn attempt(&self) -> bool {
        let mut answers = self.answers.borrow_mut();
        if answers.len() > 1 {
            answers.remove(0)
        } else {
            answers.first().copied().unwrap_or(false)
        }
    }

    fn claim_count(&self) -> usize {
        self.claim_attempts.get()
    }

    fn offer_count(&self) -> usize {
        self.offer_attempts.get()
    }
}

impl ServiceSend for MockSend {
    fn try_claim(&self, length: usize, bytes: &[u8]) -> bool {
        assert_eq!(length, bytes.len());
        self.claim_attempts.set(self.claim_attempts.get() + 1);
        if self.attempt() {
            self.claims.borrow_mut().push(bytes.to_vec());
            true
        } else {
            false
        }
    }

    fn offer(&self, bytes: &[u8]) -> bool {
        self.offer_attempts.set(self.offer_attempts.get() + 1);
        if self.attempt() {
            self.offers.borrow_mut().push(bytes.to_vec());
            true
        } else {
            false
        }
    }

    fn is_closed(&self) -> bool {
        self.closed
    }

    fn term_buffer_length(&self) -> i32 {
        64 * 1024
    }
}

fn members() -> Vec<ClusterMember> {
    vec![
        ClusterMember::parse(
            "0,localhost:1000,localhost:2000,localhost:3000,localhost:4000,localhost:5000",
        )
        .expect("a member"),
    ]
}

#[test]
fn should_send_cluster_member_response() {
    let send = MockSend::answering(&[true]);
    let mut proxy = ServiceProxy::new();

    assert!(proxy.cluster_members_response(&send, 7, 1, "0,localhost:1000"));

    assert_eq!(1, send.claim_count());
    let bytes = send.claims.borrow()[0].clone();
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    let mut decoder = ClusterMembersResponseDecoder::default().header(header, 0);
    assert_eq!(7, decoder.correlation_id());
    assert_eq!(1, decoder.leader_member_id());
    let active = decoder.active_members_decoder();
    assert_eq!(b"0,localhost:1000", decoder.active_members_slice(active));
    let passive = decoder.passive_followers_decoder();
    assert!(decoder.passive_followers_slice(passive).is_empty());
}

#[test]
fn should_log_warning_if_cluster_member_response_fails() {
    let send = MockSend::answering(&[false]);
    let mut proxy = ServiceProxy::new();

    assert!(!proxy.cluster_members_response(&send, 7, 1, "0,localhost:1000"));
    assert_eq!(5, send.claim_count());
}

#[test]
fn should_send_cluster_member_extended_response() {
    let send = MockSend::answering(&[true]);
    let mut proxy = ServiceProxy::new();
    let members = members();

    assert!(proxy.cluster_members_extended_response(&send, 7, 1_000, 1, 0, &members));

    assert_eq!(1, send.offer_count());
    let bytes = send.offers.borrow()[0].clone();
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    let decoder = ClusterMembersExtendedResponseDecoder::default().header(header, 0);
    assert_eq!(7, decoder.correlation_id());
    assert_eq!(1_000, decoder.current_time_ns());
    assert_eq!(1, decoder.leader_member_id());
    assert_eq!(0, decoder.member_id());

    let mut active = decoder.active_members_decoder();
    assert_eq!(Some(0), active.advance().expect("the group has room"));
    assert_eq!(0, active.member_id());
    let ingress = active.ingress_endpoint_decoder();
    assert_eq!(b"localhost:1000", active.ingress_endpoint_slice(ingress));
    assert_eq!(None, active.advance().expect("the group ends"));

    let message = active.parent().expect("the group has a parent");
    let mut passive = message.passive_members_decoder();
    assert_eq!(None, passive.advance().expect("no passive members"));
}

#[test]
fn should_log_warning_if_cluster_member_extended_response_fails() {
    let send = MockSend::answering(&[false]);
    let mut proxy = ServiceProxy::new();

    assert!(!proxy.cluster_members_extended_response(&send, 7, 1_000, 1, 0, &members()));
    assert_eq!(5, send.offer_count());
}
