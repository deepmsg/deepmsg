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
