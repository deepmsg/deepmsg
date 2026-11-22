//! Termination handshake: clean client close and driver termination, plus
//! the kill -9 path (driver death -> client conductor liveness -> forced
//! close -> error callback, process survives — see ADR-0003).
//!
//! The window is shortened to 500 ms here: the reference's default is ten
//! seconds (`aeron-context.c:35`), which is right for a caller and wrong for a
//! test that has to wait a driver out.

use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, ClientError, ClientOptions, DEFAULT_TIMEOUT};
use deepmsg_tests::driver::OwnDriver;

/// A killed driver is discovered by the client's duty cycle, the error
/// callback runs exactly once, and the calling process stays alive.
#[test]
fn a_killed_driver_terminates_the_client_once() {
    let Some(driver) = OwnDriver::start("termination-handshake") else {
        deepmsg_tests::driver::announce_own_skip();
        return;
    };
    let aeron_dir = driver.aeron_dir().to_path_buf();

    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut client = Client::connect_with_options(
        &aeron_dir,
        ClientOptions {
            driver_timeout_ms: 500,
            error_handler: Some(Box::new(move |_error| {
                observed.fetch_add(1, Ordering::AcqRel);
            })),
            ..ClientOptions::default()
        },
    )
    .expect("the client connects");

    // A real client, not a bare connection: both resources are on the driver
    // whose death is the subject.
    client
        .add_publication("aeron:ipc", 1001, DEFAULT_TIMEOUT)
        .expect("a publication");
    client
        .add_subscription("aeron:ipc", 1002, DEFAULT_TIMEOUT)
        .expect("a subscription");

    let status = Command::new("kill")
        .args(["-9", &driver.pid().to_string()])
        .status()
        .expect("kill runs");
    assert!(status.success(), "the driver was signalled");

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !client.is_terminated() {
        client.poll();
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(client.is_terminated(), "the client noticed the driver");
    assert!(
        matches!(client.error(), Some(ClientError::DriverTimeout { .. })),
        "the reason is the liveness timeout: {:?}",
        client.error()
    );
    assert_eq!(
        1,
        calls.load(Ordering::Acquire),
        "the error callback ran exactly once"
    );

    // The process is alive because the assertions above ran; a client that
    // panicked its way out would have taken the test with it.
}
