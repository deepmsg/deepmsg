//! `ConsensusModuleSnapshotTakerTest` (`ConsensusModuleSnapshotTakerTest.java:57-253`),
//! one test per reference test.
//!
//! The reference mocks `ExclusivePublication` and the idle strategy; here the
//! publication is a [`MockSnapshotSend`] with a scripted claim/offer sequence,
//! and the idle-strategy interactions (a `reset()` before the first claim and
//! an `idle()` between attempts) have no counterpart — this taker has no idle
//! strategy, so those assertions are dropped (recorded in the ledger).

use std::cell::{Cell, RefCell};

use deepmsg_cluster::service_control::PendingServiceMessageTracker;
use deepmsg_cluster::session::{ClusterSession, ClusterSessionState};
use deepmsg_cluster::snapshot::taker::{
    ConsensusModuleSnapshotTaker, NULL_VALUE, SNAPSHOT_TYPE_ID, SnapshotAppend, SnapshotIdle,
    SnapshotSend,
};
use deepmsg_cluster::timer::TimerSnapshotTaker;
use deepmsg_codec_cluster::ReadBuf;
use deepmsg_codec_cluster::close_reason::CloseReason;
use deepmsg_codec_cluster::cluster_session_codec::ClusterSessionDecoder;
use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;
use deepmsg_codec_cluster::consensus_module_codec::ConsensusModuleDecoder;
use deepmsg_codec_cluster::message_header_codec::MessageHeaderDecoder;
use deepmsg_codec_cluster::pending_message_tracker_codec::PendingMessageTrackerDecoder;
use deepmsg_codec_cluster::snapshot_mark::SnapshotMark;
use deepmsg_codec_cluster::snapshot_marker_codec::SnapshotMarkerDecoder;
use deepmsg_codec_cluster::timer_codec::TimerDecoder;

/// A publication whose two write paths answer from a scripted list.
struct MockSnapshotSend {
    /// `reset()` calls, for the reference's idle assertions.
    resets: Cell<usize>,
    /// `idle()` calls.
    idles: Cell<usize>,
    claim_answers: RefCell<Vec<SnapshotAppend>>,
    offer_answers: RefCell<Vec<SnapshotAppend>>,
    claims: RefCell<Vec<Vec<u8>>>,
    offers: RefCell<Vec<Vec<u8>>>,
    claim_attempts: Cell<usize>,
    offer_attempts: Cell<usize>,
    max_payload_length: usize,
}

impl MockSnapshotSend {
    fn answering(claims: &[SnapshotAppend], offers: &[SnapshotAppend]) -> Self {
        Self {
            resets: Cell::new(0),
            idles: Cell::new(0),
            claim_answers: RefCell::new(claims.to_vec()),
            offer_answers: RefCell::new(offers.to_vec()),
            claims: RefCell::new(Vec::new()),
            offers: RefCell::new(Vec::new()),
            claim_attempts: Cell::new(0),
            offer_attempts: Cell::new(0),
            max_payload_length: usize::MAX,
        }
    }

    fn with_max_payload_length(mut self, length: usize) -> Self {
        self.max_payload_length = length;
        self
    }

    fn next(answers: &RefCell<Vec<SnapshotAppend>>) -> SnapshotAppend {
        let mut answers = answers.borrow_mut();
        if answers.len() > 1 {
            answers.remove(0)
        } else {
            answers.first().cloned().unwrap_or(SnapshotAppend::Retry)
        }
    }

    fn last_claim(&self) -> Vec<u8> {
        self.claims.borrow().last().cloned().expect("a claim")
    }

    fn last_offer(&self) -> Vec<u8> {
        self.offers.borrow().last().cloned().expect("an offer")
    }
}

impl SnapshotSend for MockSnapshotSend {
    fn try_claim(&self, length: usize, bytes: &[u8]) -> SnapshotAppend {
        assert_eq!(length, bytes.len());
        self.claim_attempts.set(self.claim_attempts.get() + 1);
        let answer = Self::next(&self.claim_answers);
        if answer == SnapshotAppend::Ok {
            self.claims.borrow_mut().push(bytes.to_vec());
        }
        answer
    }

    fn offer(&self, bytes: &[u8]) -> SnapshotAppend {
        self.offer_attempts.set(self.offer_attempts.get() + 1);
        let answer = Self::next(&self.offer_answers);
        if answer == SnapshotAppend::Ok {
            self.offers.borrow_mut().push(bytes.to_vec());
        }
        answer
    }

    fn max_payload_length(&self) -> usize {
        self.max_payload_length
    }
}

impl SnapshotIdle for MockSnapshotSend {
    fn reset(&mut self) {
        self.resets.set(self.resets.get() + 1);
    }

    fn idle(&mut self) {
        self.idles.set(self.idles.get() + 1);
    }
}

fn taker<'a>(send: &'a mut MockSnapshotSend) -> ConsensusModuleSnapshotTaker<'a> {
    ConsensusModuleSnapshotTaker::new(send, ClusterTimeUnit::MILLIS, 1)
}

const RETRY: SnapshotAppend = SnapshotAppend::Retry;
const OK: SnapshotAppend = SnapshotAppend::Ok;

#[test]
fn snapshot_consensus_module_state() {
    let mut send = MockSnapshotSend::answering(&[RETRY, OK], &[]);
    let mut taker = taker(&mut send);

    taker
        .snapshot_consensus_module_state(17, 23, 42, 64)
        .expect("the third claim succeeds");

    assert_eq!(2, send.claim_attempts.get());
    let bytes = send.last_claim();
    assert_eq!(36, bytes.len());
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    assert_eq!(
        deepmsg_codec_cluster::consensus_module_codec::SBE_TEMPLATE_ID,
        header.template_id()
    );
    let decoder = ConsensusModuleDecoder::default().header(header, 0);
    assert_eq!(17, decoder.next_session_id());
    assert_eq!(Some(23), decoder.next_service_session_id());
    assert_eq!(Some(42), decoder.log_service_session_id());
    assert_eq!(Some(64), decoder.pending_message_capacity());
}

#[test]
fn snapshot_pending_service_message_tracker() {
    let mut tracker = PendingServiceMessageTracker::new(6);
    tracker.enqueue_message(&[0_u8; 64]).expect("it fits");
    let expected_log_service_session_id = tracker.log_service_session_id();
    let expected_next = tracker.next_service_session_id();
    let expected_capacity = tracker.size();

    let mut send = MockSnapshotSend::answering(&[RETRY, OK], &[RETRY, OK]);
    let mut taker = taker(&mut send);
    let correction = taker
        .snapshot_tracker(&tracker)
        .expect("the tracker record and its message go out");
    assert!(correction.is_none(), "nothing was missed");

    let bytes = send.last_claim();
    assert_eq!(32, bytes.len());
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    let decoder = PendingMessageTrackerDecoder::default().header(header, 0);
    assert_eq!(expected_next, decoder.next_service_session_id());
    assert_eq!(
        expected_log_service_session_id,
        decoder.log_service_session_id()
    );
    assert_eq!(
        Some(i32::try_from(expected_capacity).expect("a small ring")),
        decoder.pending_message_capacity()
    );
    assert_eq!(6, decoder.service_id());

    // The queued message is offered verbatim (two attempts: one refused).
    assert_eq!(2, send.offer_attempts.get());
    assert_eq!(64, send.last_offer().len());

    // The reference's exact numbers for service id 6.
    assert_eq!(-8_791_026_472_627_208_192, expected_log_service_session_id);
    assert_eq!(-8_791_026_472_627_208_190, expected_next);
}

#[test]
fn snapshot_pending_service_message_tracker_with_service_messages_missed_by_follower() {
    let mut tracker = PendingServiceMessageTracker::new(6);
    tracker.enqueue_message(&[0_u8; 64]).expect("it fits");

    // A follower whose log is further ahead than the ring: everything queued is
    // swept, and the tracker's next id lags behind what the snapshot must say.
    let expected_log_service_session_id = tracker.log_service_session_id() + 51;
    tracker.sweep_follower_messages(expected_log_service_session_id);
    assert!(tracker.next_service_session_id() < expected_log_service_session_id + 1);

    let mut send = MockSnapshotSend::answering(&[RETRY, OK], &[]);
    let mut taker = taker(&mut send);
    let correction = taker
        .snapshot_tracker(&tracker)
        .expect("the tracker record goes out");
    assert!(correction.is_some(), "the correction is reported");

    let bytes = send.last_claim();
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    let decoder = PendingMessageTrackerDecoder::default().header(header, 0);
    assert_eq!(
        expected_log_service_session_id + 1,
        decoder.next_service_session_id(),
        "an empty ring writes logServiceSessionId + 1"
    );
    assert_eq!(
        expected_log_service_session_id,
        decoder.log_service_session_id()
    );
}

#[test]
fn snapshot_timer() {
    let mut send = MockSnapshotSend::answering(&[RETRY, RETRY, OK], &[]);
    let mut taker = taker(&mut send);

    taker.snapshot_timer(7, 9_000);
    assert!(!taker.failed());

    assert_eq!(3, send.claim_attempts.get());
    let bytes = send.last_claim();
    assert_eq!(24, bytes.len());
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    let decoder = TimerDecoder::default().header(header, 0);
    assert_eq!(7, decoder.correlation_id());
    assert_eq!(9_000, decoder.deadline());
}

#[test]
fn snapshot_session_should_use_try_claim_if_data_fits_into_max_payload_size() {
    let session = snapshot_session();
    let length = 8 + 40 + 4 + session.response_channel().len();
    let mut send = MockSnapshotSend::answering(&[RETRY, OK], &[]).with_max_payload_length(length);
    let mut taker = taker(&mut send);

    taker
        .snapshot_session(&session)
        .expect("the second claim succeeds");
    assert_eq!(2, send.claim_attempts.get());
    assert_eq!(0, send.offer_attempts.get());

    verify_session_record(&send.last_claim());
}

#[test]
fn snapshot_session_should_use_offer_if_data_does_not_fit_into_max_payload_size() {
    let session = snapshot_session();
    let length = 8 + 40 + 4 + session.response_channel().len();
    let mut send =
        MockSnapshotSend::answering(&[], &[RETRY, OK]).with_max_payload_length(length - 1);
    let mut taker = taker(&mut send);

    taker
        .snapshot_session(&session)
        .expect("the second offer succeeds");
    assert_eq!(0, send.claim_attempts.get());
    assert_eq!(2, send.offer_attempts.get());

    verify_session_record(&send.last_offer());
}

fn snapshot_session() -> ClusterSession {
    let mut session = ClusterSession::new(0, 2, 556, "aeron:ipc", String::new());
    session.load_snapshot_state(13, 1024, 800, CloseReason::CLIENT_ACTION);
    session
}

fn verify_session_record(bytes: &[u8]) {
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);
    assert_eq!(
        deepmsg_codec_cluster::cluster_session_codec::SBE_TEMPLATE_ID,
        header.template_id()
    );
    let mut decoder = ClusterSessionDecoder::default().header(header, 0);
    assert_eq!(2, decoder.cluster_session_id());
    assert_eq!(13, decoder.correlation_id());
    assert_eq!(1024, decoder.opened_log_position());
    assert_eq!(
        NULL_VALUE,
        decoder.time_of_last_activity(),
        "the live activity stamp is never serialised"
    );
    assert_eq!(CloseReason::CLIENT_ACTION, decoder.close_reason());
    assert_eq!(556, decoder.response_stream_id());
    let channel = decoder.response_channel_decoder();
    assert_eq!(b"aeron:ipc", decoder.response_channel_slice(channel));
}

#[test]
fn the_snapshot_markers_carry_the_type_and_position() {
    let mut send = MockSnapshotSend::answering(&[OK, OK], &[]);
    let mut taker = taker(&mut send);

    taker.mark_begin(1_024, 5).expect("BEGIN");
    taker.mark_end(1_024, 5).expect("END");

    for (index, bytes) in send.claims.borrow().iter().enumerate() {
        assert_eq!(48, bytes.len());
        let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(bytes), 0);
        let decoder = SnapshotMarkerDecoder::default().header(header, 0);
        assert_eq!(SNAPSHOT_TYPE_ID, decoder.type_id());
        assert_eq!(1_024, decoder.log_position());
        assert_eq!(5, decoder.leadership_term_id());
        assert_eq!(0, decoder.index());
        assert_eq!(
            if index == 0 {
                SnapshotMark::BEGIN
            } else {
                SnapshotMark::END
            },
            decoder.mark()
        );
        assert_eq!(ClusterTimeUnit::MILLIS, decoder.time_unit());
    }
}

#[test]
fn a_snapshot_only_writes_open_and_closing_sessions() {
    use deepmsg_cluster::session::SessionManager;

    let mut manager = SessionManager::new(
        0,
        7,
        10_000_000_000,
        10,
        Box::new(deepmsg_cluster::session::authenticator::DefaultAuthenticator),
        deepmsg_cluster::session::authenticator::authorisation_service_for(None).expect("default"),
    );
    manager
        .on_load_cluster_session(
            1,
            1,
            100,
            NULL_VALUE,
            CloseReason::NullVal,
            102,
            "aeron:ipc",
        )
        .expect("open session");
    manager
        .on_load_cluster_session(
            2,
            2,
            200,
            NULL_VALUE,
            CloseReason::TIMEOUT,
            102,
            "aeron:ipc",
        )
        .expect("closing session");
    assert_eq!(ClusterSessionState::Open, manager.sessions()[0].state());
    assert_eq!(ClusterSessionState::Closing, manager.sessions()[1].state());

    let mut send = MockSnapshotSend::answering(&[OK, OK], &[]);
    let mut taker = taker(&mut send);
    manager.snapshot_sessions(&mut taker).expect("two records");

    assert_eq!(2, send.claims.borrow().len());
    let first = send.claims.borrow()[0].clone();
    let second = send.claims.borrow()[1].clone();
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&first), 0);
    let decoder = ClusterSessionDecoder::default().header(header, 0);
    assert_eq!(1, decoder.cluster_session_id());
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&second), 0);
    let decoder = ClusterSessionDecoder::default().header(header, 0);
    assert_eq!(2, decoder.cluster_session_id());
    assert_eq!(CloseReason::TIMEOUT, decoder.close_reason());
}
