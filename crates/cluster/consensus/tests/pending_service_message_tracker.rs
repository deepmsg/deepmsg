//! `PendingServiceMessageTrackerTest` (`PendingServiceMessageTrackerTest.java:51-250`),
//! one test per reference test.
//!
//! The reference mocks `LogPublisher.appendMessage` to answer `64` and drives
//! the tracker with a 64-byte message (a 32-byte `SessionMessageHeader` and 32
//! bytes of payload). The same mock is `RecordingAppender` here. The snapshot
//! helpers are the tracker's own fields and pending messages, as the reference's
//! are: the full snapshot taker is P3-4d, but the tracker is what those tests
//! exercise.

use std::cell::RefCell;
use std::rc::Rc;

use deepmsg_cluster::ClusterError;
use deepmsg_cluster::service_control::tracker::{
    LogAppender, NOT_APPENDED_YET, PendingServiceMessageTracker, SESSION_HEADER_LENGTH,
};

const APPEND_POSITION: i64 = 64;
const CLUSTER_SESSION_ID_OFFSET: usize = SESSION_HEADER_LENGTH - 16;
const APPEND_POSITION_OFFSET: usize = SESSION_HEADER_LENGTH - 8;

#[derive(Default)]
struct RecordingAppender {
    appends: RefCell<Vec<(i64, Vec<u8>)>>,
    answer: i64,
}

impl LogAppender for RecordingAppender {
    fn append_message(
        &mut self,
        _position_limit: i64,
        _leadership_term_id: i64,
        cluster_session_id: i64,
        _timestamp: i64,
        payload: &[u8],
    ) -> Result<i64, ClusterError> {
        self.appends
            .borrow_mut()
            .push((cluster_session_id, payload.to_vec()));
        Ok(self.answer)
    }
}

fn appender() -> Rc<RefCell<RecordingAppender>> {
    Rc::new(RefCell::new(RecordingAppender {
        appends: RefCell::new(Vec::new()),
        answer: APPEND_POSITION,
    }))
}

fn poll(tracker: &mut PendingServiceMessageTracker, appender: &Rc<RefCell<RecordingAppender>>) {
    tracker
        .poll(&mut *appender.borrow_mut(), 1024, 0)
        .expect("the append succeeds");
}

fn message() -> Vec<u8> {
    vec![0_u8; 64]
}

struct Snapshot {
    next_service_session_id: i64,
    log_service_session_id: i64,
    size: usize,
    messages: Vec<Vec<u8>>,
}

fn take_snapshot(tracker: &PendingServiceMessageTracker) -> Snapshot {
    let mut messages = Vec::new();
    tracker.for_each_pending(
        &mut |message: &[u8], _head_offset: usize| {
            messages.push(message.to_vec());
            true
        },
        10,
    );
    Snapshot {
        next_service_session_id: tracker.next_service_session_id(),
        log_service_session_id: tracker.log_service_session_id(),
        size: tracker.size(),
        messages,
    }
}

fn load_snapshot(tracker: &mut PendingServiceMessageTracker, snapshot: &Snapshot) {
    tracker.load_state(
        snapshot.next_service_session_id,
        snapshot.log_service_session_id,
        snapshot.size,
    );
    for message in &snapshot.messages {
        assert!(tracker.append_message(message));
    }
}

fn fresh_tracker() -> PendingServiceMessageTracker {
    PendingServiceMessageTracker::new(0)
}

#[test]
fn snapshot_empty() {
    let tracker = fresh_tracker();
    let snapshot = take_snapshot(&tracker);

    let mut loaded = fresh_tracker();
    load_snapshot(&mut loaded, &snapshot);
    loaded.verify().expect("an empty snapshot verifies");
}

#[test]
fn snapshot_after_enqueue_before_poll_and_sweep() {
    let mut tracker = fresh_tracker();
    tracker.enqueue_message(&message()).expect("it fits");
    let snapshot = take_snapshot(&tracker);

    let mut loaded = fresh_tracker();
    load_snapshot(&mut loaded, &snapshot);
    loaded.verify().expect("one queued message verifies");
}

#[test]
fn snapshot_after_enqueue_and_poll_before_sweep() {
    let appender = appender();
    let mut tracker = fresh_tracker();
    tracker.enqueue_message(&message()).expect("it fits");
    poll(&mut tracker, &appender);
    assert_eq!(1, appender.borrow().appends.borrow().len());

    let snapshot = take_snapshot(&tracker);
    let mut loaded = fresh_tracker();
    load_snapshot(&mut loaded, &snapshot);
    loaded.verify().expect("an appended message verifies");
}

#[test]
fn snapshot_after_enqueue_poll_and_sweep_for_leader() {
    let appender = appender();
    let mut tracker = fresh_tracker();
    tracker.enqueue_message(&message()).expect("it fits");
    poll(&mut tracker, &appender);

    // The commit position is 0, so nothing is swept: 64 is not committed.
    tracker.sweep_leader_messages(0);

    let snapshot = take_snapshot(&tracker);
    let mut loaded = fresh_tracker();
    load_snapshot(&mut loaded, &snapshot);
    loaded.verify().expect("the unswept message verifies");
}

#[test]
fn snapshot_after_enqueue_poll_and_sweep_for_follower() {
    let appender = appender();
    let mut tracker = fresh_tracker();
    tracker.enqueue_message(&message()).expect("it fits");

    // Java mutates the caller's buffer in `enqueueMessage`, so its test reads
    // the rewritten id back out of the message it passed in. This tracker
    // stores a copy, so the id is read from the queued message instead.
    let mut cluster_session_id = 0;
    tracker.for_each_pending(
        &mut |message: &[u8], _| {
            cluster_session_id = i64::from_le_bytes(
                message[CLUSTER_SESSION_ID_OFFSET..][..8]
                    .try_into()
                    .unwrap(),
            );
            true
        },
        1,
    );

    poll(&mut tracker, &appender);
    tracker.sweep_follower_messages(cluster_session_id);

    let snapshot = take_snapshot(&tracker);
    let mut loaded = fresh_tracker();
    load_snapshot(&mut loaded, &snapshot);
    loaded.verify().expect("the follower-swept state verifies");
}

#[test]
fn load_invalid() {
    let mut tracker = fresh_tracker();
    tracker.load_state(-9223372036854774166, -9223372036854774166, 0);
    assert!(tracker.verify().is_err());
}

#[test]
fn load_valid() {
    let mut tracker = fresh_tracker();
    tracker.load_state(-9223372036854774165, -9223372036854774166, 0);
    tracker.verify().expect("next == log + 1 verifies");
}

#[test]
fn an_enqueued_message_carries_the_next_session_id_and_no_append_position_yet() {
    let mut tracker = fresh_tracker();
    let cluster_session_id = tracker.next_service_session_id();
    tracker.enqueue_message(&message()).expect("it fits");

    let mut stored = Vec::new();
    tracker.for_each_pending(
        &mut |message: &[u8], _| {
            stored.extend_from_slice(message);
            true
        },
        1,
    );
    assert_eq!(
        cluster_session_id,
        i64::from_le_bytes(stored[CLUSTER_SESSION_ID_OFFSET..][..8].try_into().unwrap())
    );
    assert_eq!(
        NOT_APPENDED_YET,
        i64::from_le_bytes(stored[APPEND_POSITION_OFFSET..][..8].try_into().unwrap())
    );
}
