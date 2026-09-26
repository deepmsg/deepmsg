//! The client's command/response loop, without a driver.
//!
//! The interop suite proves this against a real `aeronmd` and is the stronger
//! evidence, but it cannot run in CI — so the matching loop and the deadline
//! are exercised here against replies the test fabricates byte for byte. That
//! is the same technique the reference's own client test uses
//! (`aeron-client/src/test/c/aeron_client_conductor_test.cpp:338-355`).
//!
//! What is *not* here: the encoding and decoding of a message. Those are pure
//! functions with their own tests in `deepmsg-cnc`; what needs a client is the
//! part that decides whether an arriving response is the one it was waiting
//! for, and what happens when none arrives.

use std::path::Path;
use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, CommandError};
use deepmsg_cnc::command::{ON_ERROR_TYPE_ID, ON_SUBSCRIPTION_READY_TYPE_ID};
use deepmsg_cnc::layout;
use deepmsg_core::pal::MappedFile;
use deepmsg_tests::synthetic::{self, EVENT_CAPACITY, SyntheticCnc};

/// The correlation id the client's `add_subscription` will use.
///
/// Read from the ring's counter rather than assumed: connecting draws one id
/// and the command draws the next, so the command's id is one past whatever
/// the counter holds now. Reading it means a change in how many ids the client
/// draws fails *here*, loudly, instead of producing a reply that matches
/// nothing and a confusing timeout somewhere else.
fn subscription_correlation_id(cnc: &Path) -> i64 {
    let mapping = MappedFile::open_readonly(cnc).expect("map the CnC file");
    let offsets = synthetic::offsets();
    let field =
        offsets.to_driver + synthetic::COMMAND_CAPACITY + layout::MPSC_CORRELATION_COUNTER_OFFSET;

    let region = mapping.region(field, 8).expect("the counter is in range");
    region.load_i64_acquire(0).expect("readable") + 1
}

/// Write one fabricated reply into the to-clients ring.
fn publish(cnc: &Path, type_id: i32, payload: &[u8]) {
    let mapping = MappedFile::open_readwrite(cnc).expect("map the CnC file writable");
    let offsets = synthetic::offsets();
    let region = mapping
        .region_mut(offsets.to_clients, synthetic::TO_CLIENTS)
        .expect("the to-clients region is writable");

    // One reply per test, so the writer's cursor starts at zero. The receiver
    // starts at `latest_counter`, which is zero in a fresh file too, so it
    // lands on this record either way.
    let mut next = 0i64;
    synthetic::publish_broadcast(&region, EVENT_CAPACITY, &mut next, type_id, payload)
        .expect("the reply fits the ring");
}

fn ready_payload(correlation_id: i64) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&correlation_id.to_le_bytes());
    // What an IPC subscription reports: no channel-status counter exists for
    // it, and that is not an error.
    payload.extend_from_slice(&(-1i32).to_le_bytes());
    payload
}

fn error_payload(correlation_id: i64, code: i32, message: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&correlation_id.to_le_bytes());
    payload.extend_from_slice(&code.to_le_bytes());
    payload.extend_from_slice(&(message.len() as i32).to_le_bytes());
    payload.extend_from_slice(message.as_bytes());
    payload
}

#[test]
fn matches_a_ready_response_by_correlation_id() {
    let cnc = SyntheticCnc::new(deepmsg_core::version::CNC_VERSION);
    let expected = subscription_correlation_id(&cnc.cnc_path());
    publish(
        &cnc.cnc_path(),
        ON_SUBSCRIPTION_READY_TYPE_ID,
        &ready_payload(expected),
    );

    let mut client = Client::connect(cnc.path()).expect("connect");
    let id = client
        .add_subscription("aeron:ipc", 1001, Duration::from_secs(1))
        .expect("the fabricated reply must match");

    assert_eq!(expected, id, "the id is the correlation id, echoed");
}

#[test]
fn a_response_for_someone_else_is_ignored_not_mistaken() {
    // Every client reads the whole broadcast ring, so a reply addressed to a
    // different request is the normal case rather than an edge one.
    let cnc = SyntheticCnc::new(deepmsg_core::version::CNC_VERSION);
    let ours = subscription_correlation_id(&cnc.cnc_path());
    publish(
        &cnc.cnc_path(),
        ON_SUBSCRIPTION_READY_TYPE_ID,
        &ready_payload(ours + 1000),
    );

    let mut client = Client::connect(cnc.path()).expect("connect");
    let error = client
        .add_subscription("aeron:ipc", 1001, Duration::from_millis(200))
        .expect_err("a reply for another request must not complete ours");

    assert!(
        matches!(error, CommandError::TimedOut { correlation_id } if correlation_id == ours),
        "got {error:?}"
    );
}

#[test]
fn surfaces_a_driver_error_against_the_command_that_caused_it() {
    let cnc = SyntheticCnc::new(deepmsg_core::version::CNC_VERSION);
    let expected = subscription_correlation_id(&cnc.cnc_path());
    publish(
        &cnc.cnc_path(),
        ON_ERROR_TYPE_ID,
        &error_payload(expected, -1004, "unknown subscription"),
    );

    let mut client = Client::connect(cnc.path()).expect("connect");
    let error = client
        .add_subscription("aeron:ipc", 1001, Duration::from_secs(1))
        .expect_err("the driver refused it");

    match error {
        CommandError::Driver { code, message } => {
            assert_eq!(-1004, code);
            assert_eq!("unknown subscription", message);
        }
        other => panic!("expected a driver error, got {other:?}"),
    }
}

#[test]
fn a_command_with_no_reply_expires_rather_than_hanging() {
    let cnc = SyntheticCnc::new(deepmsg_core::version::CNC_VERSION);
    let expected = subscription_correlation_id(&cnc.cnc_path());

    let mut client = Client::connect(cnc.path()).expect("connect");
    let started = Instant::now();
    let error = client
        .add_subscription("aeron:ipc", 1001, Duration::from_millis(150))
        .expect_err("nothing will answer");

    assert!(
        matches!(error, CommandError::TimedOut { correlation_id } if correlation_id == expected),
        "got {error:?}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "it must actually wait for the deadline, not give up early"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "and it must not wait much longer than the deadline either"
    );
}
