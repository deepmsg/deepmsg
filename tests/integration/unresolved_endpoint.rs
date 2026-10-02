//! A channel whose name does not resolve.
//!
//! `AsyncResourceTest.shouldDetectUnknownHost` is the reference's case: a
//! client adds a publication on `aeron:udp?endpoint=wibble:1234`, the driver
//! cannot resolve the name, and the client has to be **told** — otherwise it
//! waits for a publication that will never be ready, which is what the Java
//! test turns into a ten-second hang and what this one turns into a timeout.
//!
//! Our client against our driver, so it runs in CI and needs no reference
//! checkout.

use std::time::Duration;

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream every test here asks on.
const STREAM_ID: i32 = 1001;

/// A name that cannot resolve, and that fails **without asking a nameserver**.
///
/// The reference's own case uses a bare `wibble`, and RFC 2606 would suggest
/// `something.invalid`; both were tried here and both turn this test into a
/// measure of the host's DNS. A name glibc has to *ask* about costs whatever
/// the resolver costs — this machine's nameserver is unreachable and takes
/// anywhere from 0.2 s to 20 s to say so — while the resolution is
/// **synchronous in the conductor**, so a slow answer stops the driver's
/// heartbeat and the client declares the driver dead at ten seconds
/// (`AsyncResourceTest.shouldDetectUnknownHost` fails here for the same
/// reason, against any driver that resolves where the reference C driver
/// does).
///
/// An address that is not an address is refused by `getaddrinfo` itself, in
/// microseconds, which is the same failure — a name that does not resolve —
/// with the network taken out of it.
const CHANNEL: &str = "aeron:udp?endpoint=999.999.999.999:1234";

#[test]
fn a_publication_whose_name_does_not_resolve_is_refused_by_name() {
    let Some(mut own) = OwnDriver::start("unresolved-publication") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let error = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect_err("a name that does not resolve is not a publication");

    // The words are the default resolver's own, and they name the host: a
    // client that is told `Unable to resolve host=(...)` knows which part of
    // its channel to change.
    assert!(
        error.to_string().contains("999.999.999.999"),
        "the refusal does not name the host it could not resolve: {error}\n\
         the driver said:\n{}",
        own.log_tail(40)
    );
}

#[test]
fn a_subscription_whose_name_does_not_resolve_is_refused_by_name() {
    let Some(mut own) = OwnDriver::start("unresolved-subscription") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let error = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect_err("a name that does not resolve is not a subscription");

    assert!(
        error.to_string().contains("999.999.999.999"),
        "the refusal does not name the host it could not resolve: {error}\n\
         the driver said:\n{}",
        own.log_tail(40)
    );
}
