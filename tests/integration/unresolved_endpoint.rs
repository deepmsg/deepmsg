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

/// A name that cannot resolve, and that is refused **without a nameserver being
/// asked** — which is the whole of why it looks like this.
///
/// Every name that is *asked about* costs whatever the host's resolver costs,
/// and this host's nameserver is unreachable: measured here, `wibble` takes
/// 0.4 s to 15 s, `wibble.invalid` 10 s to 20 s, `wibble.` a steady 5 s, and a
/// dotted quad that is not a quad — `999.999.999.999`, the first thing tried —
/// 0.17 s most runs and 5.78 s in others. That matters because the resolution
/// is **synchronous in the conductor** on this build: a slow answer stops the
/// driver's heartbeat, and the client declares the driver dead after ten
/// seconds of it. A test written with any of those names fails here one run in
/// five, and the driver is not what is slow.
///
/// An empty label is refused by the resolver's own syntax in microseconds on
/// every run measured, which is the same failure — a name that does not
/// resolve — with the network taken out of it.
///
/// The divergence underneath is real and belongs to a slice of its own: the
/// reference's add-publication command is a state machine that waits while the
/// **native resource agent** parses the channel
/// (`aeron_driver_conductor.c:4113-4131`,
/// `AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_PENDING`), so its
/// conductor never waits on a nameserver, and `AsyncResourceTest` passes there
/// on this same host. It is written up in `docs/compat.md`.
const CHANNEL: &str = "aeron:udp?endpoint=nothing..invalid:1234";

/// The host of [`CHANNEL`], which every refusal is expected to name.
const HOST: &str = "nothing..invalid";

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

    // The words are the resolver's own, and they name the host: a client told
    // which name could not be resolved knows which part of its channel to
    // change.
    assert!(
        error.to_string().contains(HOST),
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
        error.to_string().contains(HOST),
        "the refusal does not name the host it could not resolve: {error}\n\
         the driver said:\n{}",
        own.log_tail(40)
    );
}
