//! Connecting to a driver that is not ready yet.
//!
//! There is a window between a driver creating its CnC file and publishing the
//! version, and for a 46 MB file it is not a small one. A client that arrives
//! inside it used to fail hard — deterministically, for any orchestration that
//! starts both at once — so this pins the wait, and the giving up, that the
//! reference's own client does (`aeron_client_connect_to_driver`, `aeronc.c:70-125`).
//!
//! The tests use a driver-shaped thread rather than a driver process: creating
//! and publishing a CnC file is the whole of what the window is about, and a
//! real driver would add a subprocess and a 46 MB file to every run of this.

use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cnc::create::COUNTERS_VALUES_BUFFER_LENGTH_MIN;
use deepmsg_cnc::{CncFile, CncIdentity, CncLayout};
use deepmsg_core::clock;
use deepmsg_tests::synthetic::SyntheticCnc;
use deepmsg_tests::temp::TempDir;

/// The smallest CnC file a driver can be configured to write, so the test
/// exercises the window without allocating the default 46 MB.
fn layout() -> CncLayout {
    CncLayout {
        counters_values_length: COUNTERS_VALUES_BUFFER_LENGTH_MIN,
        ..CncLayout::default()
    }
}

fn identity() -> CncIdentity {
    CncIdentity {
        liveness_timeout_ns: deepmsg_cnc::CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT,
        start_timestamp_ms: clock::epoch_millis(),
        pid: i64::from(std::process::id()),
    }
}

#[test]
fn connecting_waits_for_a_driver_that_is_still_starting() {
    let dir = TempDir::new("deepmsg-connect-window");
    let path = dir.path().to_owned();

    // The driver, arriving late: the file is created and published after the
    // client has already asked for it.
    let publisher = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(40));
        let mut cnc = CncFile::create(&path, &layout(), &identity()).expect("create the CnC file");
        cnc.write_consumer_heartbeat(clock::epoch_millis())
            .expect("the ring is writable");
        cnc.publish().expect("publish");
    });

    let started = Instant::now();
    let client = Client::connect(dir.path()).expect("connect");
    publisher.join().expect("the driver thread");

    assert!(
        started.elapsed() >= Duration::from_millis(40),
        "it waited for the file instead of failing on the first look"
    );
    assert!(client.client_id() >= 0, "and it is a usable client");
}

#[test]
fn connecting_gives_up_when_no_driver_ever_arrives() {
    let dir = TempDir::new("deepmsg-connect-timeout");

    // No driver at all: the wait is bounded, and the bound is the caller's.
    let started = Instant::now();
    let error = Client::connect_with_timeout(dir.path(), Duration::from_millis(50))
        .expect_err("nobody is going to create a CnC file here");

    assert!(
        started.elapsed() >= Duration::from_millis(50),
        "it waited out its window before giving up"
    );
    assert!(
        matches!(error, deepmsg_client::client::ConnectError::Cnc(_)),
        "and the failure names the file it was waiting for: {error}"
    );
}

/// A file is not a driver: one that a driver left behind is refused, and it is
/// refused as *"no driver"* rather than as *"no file"*.
///
/// Two shapes of "gone", because the reference's fourth step has two ways to
/// arrive there (`aeronc.c:149-167`): an orderly shutdown writes
/// `NULL_VALUE` into the heartbeat on its way out
/// (`aeron-driver/src/main/c/aeron_driver_conductor.c:3493`), and a driver that
/// was killed leaves a timestamp that simply ages out of the window.
#[test]
fn connecting_does_not_take_a_file_a_dead_driver_left_behind() {
    for (shape, heartbeat) in [
        ("an orderly shutdown", deepmsg_cnc::layout::NULL_VALUE),
        ("a driver that was killed", clock::epoch_millis() - 60_000),
    ] {
        let cnc = SyntheticCnc::new(deepmsg_core::version::CNC_VERSION).with_heartbeat(heartbeat);

        let started = Instant::now();
        let error = Client::connect_with_timeout(cnc.path(), Duration::from_millis(150))
            .expect_err("the driver behind this file is gone");

        assert!(
            matches!(
                error,
                deepmsg_client::client::ConnectError::NoLiveDriver { .. }
            ),
            "{shape}: {error}"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(150),
            "{shape}: it waited out its window rather than deciding on the first look"
        );
    }
}

/// And the other half of that step: what replaces a dead driver is a **new file
/// at the same path**, so a client that is waiting has to unmap and look again
/// (`aeronc.c:161-167`).
///
/// A client that kept the old mapping would wait out its whole window for an
/// inode nothing writes to any more — which is what the failing case on CI was,
/// seen from the archive's side.
#[test]
fn connecting_waits_for_a_driver_that_replaces_a_dead_one() {
    let cnc = SyntheticCnc::new(deepmsg_core::version::CNC_VERSION)
        .with_heartbeat(deepmsg_cnc::layout::NULL_VALUE);
    let dir = cnc.path().to_owned();
    let path = cnc.cnc_path();

    let replacement_dir = dir.clone();
    let replacement = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(80));

        // Unlinked first, which is what a driver started with
        // `aeron.dir.delete.on.start` leaves behind: a new file, a new inode.
        std::fs::remove_file(&path).expect("remove the dead driver's file");
        let mut fresh =
            CncFile::create(&replacement_dir, &layout(), &identity()).expect("create the CnC file");
        fresh
            .write_consumer_heartbeat(clock::epoch_millis())
            .expect("the ring is writable");
        fresh.publish().expect("publish");
    });

    let started = Instant::now();
    let client = Client::connect_with_timeout(&dir, Duration::from_secs(3))
        .expect("connect to the driver that arrived");

    // Measured **before** the join, and that is the whole assertion: the join
    // waits out the replacement's own 80 ms, so an elapsed read after it holds
    // whether or not the connect waited at all. Measured on the first version of
    // this test, which passed against a client that took the dead file at once.
    let waited = started.elapsed();
    replacement.join().expect("the replacement thread");

    assert!(
        waited >= Duration::from_millis(80),
        "it waited for the replacement rather than taking the dead driver's file: {waited:?}"
    );
    assert!(client.client_id() >= 0, "and it is a usable client");
}
