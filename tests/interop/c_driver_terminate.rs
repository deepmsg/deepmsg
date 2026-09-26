//! Termination against a live driver — the two-sided oracle.
//!
//! A deny-only test cannot tell "the command arrived and was refused" apart
//! from "the command was never sent at all", because both look like a driver
//! that keeps running. With `allow` the driver actually stops, which is
//! observable, so the pair together says something neither says alone.
//!
//! The reference's own happy-path test (`aeron_c_terminate_test.cpp:51-58`)
//! asserts only that the client got `1` — a value it returns on a successful
//! ring write, entirely independent of whether the driver did anything. These
//! tests assert the consequence instead.

use std::time::{Duration, Instant};

use deepmsg_client::terminate::{TerminationOutcome, request_driver_termination};
use deepmsg_cnc::layout;
use deepmsg_tests::driver::{self, READY_TIMEOUT, ReferenceDriver};

/// How long to wait for a driver to finish shutting down.
const STOP_DEADLINE: Duration = Duration::from_secs(10);

/// The property that turns the driver's default refusal into acceptance.
///
/// `aeronmd` uppercases and de-underscores `-D` names, so this sets
/// `AERON_DRIVER_TERMINATION_VALIDATOR=allow`
/// (`aeron-driver/src/main/c/aeronmd.c:72-81`).
const ALLOW_TERMINATION: &str = "-Daeron.driver.termination.validator=allow";

fn start(
    test_name: &str,
    extra_properties: &[&str],
) -> Option<(ReferenceDriver, deepmsg_cnc::CncFile)> {
    let Some(binary) = driver::locate_verified() else {
        driver::announce_skip();
        return None;
    };

    let mut reference =
        ReferenceDriver::start_with(&binary, test_name, extra_properties).expect("start driver");
    let cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the driver must publish a readable CnC file");

    Some((reference, cnc))
}

#[test]
fn a_refused_termination_is_still_a_sent_one() {
    // The default validator is `deny` (`aeron-driver/src/main/c/aeron_driver_context.c:1247`),
    // so a stock driver refuses every termination request whatever the token.
    let Some((reference, cnc)) = start("terminate-denied", &[]) else {
        return;
    };

    let outcome = request_driver_termination(reference.aeron_dir(), b"let me in")
        .expect("sending must succeed even though the driver will refuse");

    assert_eq!(
        TerminationOutcome::Committed,
        outcome,
        "Committed means the record reached the ring, not that it was accepted"
    );

    // And the driver is unmoved. A short wait first, so a driver that *did*
    // shut down has time to say so rather than being caught mid-flight.
    std::thread::sleep(Duration::from_millis(250));

    let heartbeat = cnc
        .consumer_heartbeat_ms()
        .expect("the ring trailer stays readable");
    assert_ne!(
        layout::NULL_VALUE,
        heartbeat,
        "a refused request must leave the driver running"
    );

    let timeout_ms = cnc.metadata().client_liveness_timeout_ns / 1_000_000;
    assert!(
        cnc.driver_is_active(now_ms(), timeout_ms),
        "the heartbeat should still be fresh"
    );
}

#[test]
fn an_accepted_termination_actually_stops_the_driver() {
    let Some((mut reference, cnc)) = start("terminate-allowed", &[ALLOW_TERMINATION]) else {
        return;
    };

    let outcome = request_driver_termination(reference.aeron_dir(), b"let me in")
        .expect("sending must succeed");

    assert_eq!(TerminationOutcome::Committed, outcome);

    // The observable consequence. A clean close release-stores `-1` into the
    // to-driver ring's consumer heartbeat
    // (`aeron-driver/src/main/c/aeron_driver_conductor.c:3493`); it is the only
    // external evidence that a terminate did anything at all, and the reference
    // never asserts on it.
    let deadline = Instant::now() + STOP_DEADLINE;
    loop {
        if Some(layout::NULL_VALUE) == cnc.consumer_heartbeat_ms() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the heartbeat never became NULL_VALUE, so the driver did not close cleanly; \
             its output was:\n{}",
            reference.log_tail(20)
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // The mapping outlives the file, so this reads the last thing the driver
    // wrote rather than failing.
    assert!(
        !cnc.driver_is_active(now_ms(), i64::MAX),
        "a heartbeat of -1 means deliberately stopped, not merely stale"
    );

    // And it exited through the hook rather than through a signal, which is a
    // different route to the same teardown: the hook sets EXIT_SUCCESS, while
    // the signal handler records the signal number as the exit status
    // (`aeron-driver/src/main/c/aeronmd.c:39-42,165-185`).
    let status = reference.stop().expect("reaping must succeed");
    assert_eq!(
        Some(0),
        status.code(),
        "an accepted termination exits 0; a signalled one exits with the signal number"
    );

    assert!(
        !reference
            .aeron_dir()
            .join(deepmsg_cnc::CNC_FILE_NAME)
            .exists(),
        "the directory goes with it when delete.on.shutdown is set"
    );
}

fn now_ms() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after the epoch");

    #[allow(clippy::cast_possible_truncation)]
    let millis = elapsed.as_millis() as i64;
    millis
}
