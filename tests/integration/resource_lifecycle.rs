//! G2-2: a client giving a resource back.
//!
//! Three ways to let go of a publication or a subscription, and they are not
//! the same:
//!
//! * `remove_*` asks the driver and waits for its answer — the resource is gone
//!   from both sides when the call returns;
//! * `revoke_publication` is the same command with the revoke flag, and the
//!   difference is what it does to *readers*: their images are revoked and
//!   reported unavailable with `is_publication_revoked` true;
//! * `force_remove_*` tells the driver nothing, which is what a client about to
//!   die does — the driver keeps its link until the client's own liveness
//!   timeout reaps it.
//!
//! Everything here is our own client against our own driver, so it needs no
//! reference checkout and runs in CI. What it asserts that a unit test cannot:
//! the driver's answer is what removes the link, and what the client is left
//! holding afterwards is nothing.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream every test here publishes on.
const STREAM_ID: i32 = 1001;

/// Shared memory: a removed subscription's image is a *network* one, but the
/// shape of a removal — the link going, the counters coming back, the
/// acknowledgement arriving — is the same, and this needs no sockets.
const CHANNEL: &str = "aeron:ipc";

/// How long a test may take before it is a failure rather than a slow machine.
const DEADLINE: Duration = Duration::from_secs(30);

fn await_images(client: &mut Client, registration_id: i64) {
    let start = Instant::now();

    while start.elapsed() < DEADLINE {
        let _ = client.poll();

        if client
            .subscription(registration_id)
            .is_some_and(|subscription| !subscription.images().is_empty())
        {
            return;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    panic!("no image arrived");
}

/// A publication and a subscription that are talking, so that a removal has
/// something to remove.
fn talking_pair(name: &str) -> Option<(OwnDriver, Client, i64, i64)> {
    let Some(mut own) = OwnDriver::start(name) else {
        driver::announce_own_skip();
        return None;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_images(&mut client, subscription);

    Some((own, client, publication, subscription))
}

#[test]
fn a_subscription_and_a_publication_can_be_given_back() {
    let Some((_own, mut client, publication, subscription)) = talking_pair("lifecycle-remove")
    else {
        return;
    };

    assert!(client.publication(publication).is_some());
    assert!(client.subscription(subscription).is_some());

    // The subscription first: its image is what a receiver feeds, and the
    // acknowledgement is what makes the removal this client's too.
    client
        .remove_subscription(subscription, DEFAULT_TIMEOUT)
        .expect("the driver answers a removal it made");

    assert!(
        client.subscription(subscription).is_none(),
        "the client is not holding it any more"
    );
    assert!(
        client.subscription(subscription).is_none() && client.publication(publication).is_some(),
        "and the publication next to it is untouched"
    );

    client
        .remove_publication(publication, DEFAULT_TIMEOUT)
        .expect("the driver answers a removal it made");

    assert!(client.publication(publication).is_none());
}

/// A removal the driver refuses is a removal this client does not make: the
/// resource is still its own, and it is still usable.
#[test]
fn a_removal_the_driver_refuses_leaves_the_resource_alone() {
    let Some((_own, mut client, publication, _subscription)) = talking_pair("lifecycle-refused")
    else {
        return;
    };

    // A registration id nobody has: the driver answers `ON_ERROR` with the
    // unknown-publication code rather than an acknowledgement.
    let result = client.remove_publication(i64::MAX, DEFAULT_TIMEOUT);

    assert!(result.is_err(), "the driver refused it");
    assert!(
        client.publication(publication).is_some(),
        "and the publication it does own is still here"
    );
}

/// `force_remove_*` asks nobody: the client lets go, and the driver is left
/// holding a link that its own client timeout will reap.
#[test]
fn a_forced_removal_does_not_ask_the_driver() {
    let Some((_own, mut client, publication, subscription)) = talking_pair("lifecycle-force")
    else {
        return;
    };

    assert!(client.force_remove_subscription(subscription));
    assert!(client.subscription(subscription).is_none());
    assert!(
        !client.force_remove_subscription(subscription),
        "and asking twice is not a removal"
    );

    assert!(client.force_remove_publication(publication));
    assert!(client.publication(publication).is_none());

    // The driver is still there and still serving: a forced removal is a
    // client's own business.
    let _ = client.poll();
}

/// A revoked publication takes its readers' images with it
/// (`REMOVE_PUBLICATION_FLAG_REVOKE`), which is a different ending from the
/// quiet removal above — and the one `PublicationRevokeTest` measures from the
/// far side.
#[test]
fn a_revoked_publication_takes_its_readers_image_with_it() {
    let Some((_own, mut client, publication, subscription)) = talking_pair("lifecycle-revoke")
    else {
        return;
    };

    client
        .revoke_publication(publication, DEFAULT_TIMEOUT)
        .expect("the driver answers a revoke it made");

    assert!(
        client.publication(publication).is_none(),
        "a revocation is a removal, with a message to the readers"
    );

    // The image goes with it: the driver tells the subscriber, and the client
    // drops the image when it hears.
    let start = Instant::now();

    while start.elapsed() < DEADLINE {
        let _ = client.poll();

        if client
            .subscription(subscription)
            .is_none_or(|subscription| subscription.images().is_empty())
        {
            return;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    panic!("the revoked stream's image was never reported unavailable");
}
