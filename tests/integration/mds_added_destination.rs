//! G3-1b: a destination added to a multi-destination subscription connects the
//! far end.
//!
//! The shape is `BusySocketTest.mdsSubscriptionShouldConnectToASocketOnceItIsFree`,
//! in our own harness: a publisher and a subscription on the same address in one
//! driver, and a `control-mode=manual` MDS subscription in a **second** driver
//! that is later given that address as a destination. The image appears on the
//! MDS side — which is what an arriving frame alone is enough for — and the
//! publisher has to end up connected, which needs the other half: status
//! messages coming back from the destination that was added.
//!
//! Everything here is our own, so it needs no reference checkout.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream the publisher and both subscriptions use.
const STREAM_ID: i32 = 1001;

/// The port the first driver listens on, and the one the second is later given.
const PORT: u16 = 24_914;

/// The second stream's port: the MDS subscription reads from this one first, so
/// that the address added later is the **second** destination on one endpoint —
/// which is the shape the reference's own test has, and the one where the
/// answer has to leave through the socket the frames arrived on.
const OTHER_PORT: u16 = 24_915;

/// How long a test may take before it is a failure rather than a slow machine.
const DEADLINE: Duration = Duration::from_secs(20);

fn publisher_channel(port: u16) -> String {
    format!("aeron:udp?endpoint=127.0.0.1:{port}|term-length=64k")
}

fn subscriber_channel(port: u16) -> String {
    format!("aeron:udp?endpoint=127.0.0.1:{port}")
}

/// The MDS subscription: manual control, so it reads from the destinations its
/// client names and from nothing else.
const MDS_CHANNEL: &str = "aeron:udp?control-mode=manual";

fn await_connected(client: &mut Client, registration_id: i64) -> bool {
    let start = Instant::now();

    while start.elapsed() < DEADLINE {
        let _ = client.poll();

        if client
            .exclusive_publication(registration_id)
            .and_then(deepmsg_client::publication::ExclusivePublication::is_connected)
            == Some(true)
        {
            return true;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    false
}

/// A destination added to a manual subscription is what connects the publisher
/// on the far side: the image is built from the frames that arrive, and the
/// publisher only calls itself connected once it has heard a status message
/// from a reader.
///
/// The second stream is the point of the shape. A manual subscription that
/// already reads from one address has an image whose answer leaves through the
/// socket the frames arrived on, and the destination added afterwards has a
/// socket of its own — a status message sent through the wrong one is a status
/// message the publisher never sees.
#[test]
#[ignore = "known failure: a second destination on one endpoint makes no image \
            (analysis/aeron/deepmsg-endpoint-lifecycle-plan.md §10)"]
fn a_destination_added_to_an_mds_subscription_connects_the_publisher() {
    let Some(mut first) = OwnDriver::start("mds-added-destination-publisher") else {
        driver::announce_own_skip();
        return;
    };
    let Some(mut second) = OwnDriver::start("mds-added-destination-reader") else {
        driver::announce_own_skip();
        return;
    };

    first
        .await_cnc(Duration::from_secs(10))
        .expect("the first driver publishes its CnC file");
    second
        .await_cnc(Duration::from_secs(10))
        .expect("the second driver publishes its CnC file");

    let mut publisher_client = Client::connect(first.aeron_dir()).expect("connect to the first");
    let mut reader_client = Client::connect(second.aeron_dir()).expect("connect to the second");

    let late = publisher_client
        .add_exclusive_publication(&publisher_channel(PORT), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the publication whose address is added later");
    let early = publisher_client
        .add_exclusive_publication(&publisher_channel(OTHER_PORT), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the publication the MDS reads from first");

    let local_late = publisher_client
        .add_subscription(&subscriber_channel(PORT), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the subscription holding the late address");
    assert!(
        await_connected(&mut publisher_client, late),
        "the publication whose address is held by this driver's own subscription \
         is connected to it"
    );

    // The MDS subscription reads from nowhere until it is told where, and the
    // first address it is told is free: nothing on the other driver holds it.
    let mds = reader_client
        .add_subscription(MDS_CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a manual subscription");

    reader_client
        .add_rcv_destination(mds, &subscriber_channel(OTHER_PORT), DEFAULT_TIMEOUT)
        .expect("the first destination");

    assert!(
        await_connected(&mut publisher_client, early),
        "the first destination connects the publisher it reads from"
    );

    // And the second address only becomes free when the subscription holding it
    // goes — the release is two-phase, so the add is retried until it is.
    local_subscription_close(&mut publisher_client, local_late);

    let start = Instant::now();
    let mut added = false;

    while start.elapsed() < DEADLINE {
        let _ = reader_client.poll();

        if reader_client
            .add_rcv_destination(mds, &subscriber_channel(PORT), DEFAULT_TIMEOUT)
            .is_ok()
        {
            added = true;
            break;
        }

        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(added, "the address never came free for the other driver");

    // The evidence, from both drivers, before the assertion that says whether
    // the status messages made it: what one side sent and what the other
    // received are two independent readings.
    let publisher_counters = interesting_counters(&publisher_client);
    let reader_counters = interesting_counters(&reader_client);

    // And what each side thinks it can read: the reader's images are how the
    // frames that arrived were made sense of, and the publisher's connected
    // byte is what its own driver believes.
    let reader_images = reader_client
        .subscription(mds)
        .map(|subscription| subscription.images().len());
    let late_connected = publisher_client
        .exclusive_publication(late)
        .and_then(deepmsg_client::publication::ExclusivePublication::is_connected);
    let early_connected = publisher_client
        .exclusive_publication(early)
        .and_then(deepmsg_client::publication::ExclusivePublication::is_connected);

    assert!(
        await_connected(&mut publisher_client, late),
        "and the publisher behind the address that was just added is connected\n\
         reader images: {reader_images:?} late_connected: {late_connected:?} \
         early_connected: {early_connected:?}\n\
         publisher driver: {publisher_counters:?}\n\
         reader driver: {reader_counters:?}"
    );

    let _ = early;
}

/// The counters that say whether a status message travelled, by label — the
/// system counters share a type id and are told apart by what they are called
/// (`aeron-client/src/main/c/aeron_counters.h`).
fn interesting_counters(client: &Client) -> Vec<(String, i64)> {
    let Some(reader) = client.counters_reader() else {
        return Vec::new();
    };

    let mut found = Vec::new();
    let _ = reader.for_each(|descriptor| {
        if [
            "Status Messages sent",
            "Status Messages received",
            "Status Messages rejected",
            "Heartbeats sent",
            "Heartbeats received",
            "NAKs sent",
            "NAKs received",
        ]
        .iter()
        .any(|needle| descriptor.label.starts_with(needle))
        {
            found.push((descriptor.label.clone(), descriptor.value));
        }
    });

    found
}

/// Give a local subscription back, so that the address it held is free for the
/// other driver — the same step the reference's test makes.
fn local_subscription_close(client: &mut Client, registration_id: i64) {
    client
        .remove_subscription(registration_id, DEFAULT_TIMEOUT)
        .expect("the driver answers a removal it made");
}
