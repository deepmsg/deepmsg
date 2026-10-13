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

use deepmsg_client::client::{
    AsyncDestination, AsyncDestinationPoll, Client, CommandError, DEFAULT_TIMEOUT,
};
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
         reader driver: {reader_counters:?}\n\
         --- publisher errors ---\n{}\n--- reader errors ---\n{}",
        driver_errors(first.aeron_dir()),
        driver_errors(second.aeron_dir()),
    );

    let _ = early;
}

/// Whatever the driver recorded as an error, read out of its CnC file the way
/// the driver's own tool does — the one place a refusal says why.
fn driver_errors(aeron_dir: &std::path::Path) -> String {
    use deepmsg_cnc::error_log::ErrorLogEntry;

    let Ok(cnc) = deepmsg_cnc::CncFile::open(aeron_dir, Duration::from_secs(1)) else {
        return format!("<{aeron_dir:?} could not be opened>");
    };
    let Some(reader) = cnc.error_log() else {
        return String::from("<no error log region>");
    };

    let mut entries: Vec<ErrorLogEntry> = Vec::new();
    let _ = reader.read(0, &mut entries);

    entries
        .iter()
        .map(|entry| format!("{:?}", entry.text))
        .collect::<Vec<_>>()
        .join("\n")
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
            "Errors",
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

/// The ports the asynchronous half uses. Its own, for the reason the others have
/// theirs: `cargo test` runs the tests in a file at once.
const ASYNC_PORT: u16 = 24_916;
/// See [`ASYNC_PORT`], and the port a **subscription** holds so that the
/// destination below cannot have it.
const HELD_PORT: u16 = 24_917;

/// A destination added **without waiting** for the driver: the command goes out,
/// the answer is read later, and a second subscription is what makes a driver
/// refuse one.
///
/// Three claims, which are the whole of what a non-blocking form has to get
/// right:
///
/// * the command is taken by the driver — the far end connects, which is not
///   something the caller could have known from its own return value;
/// * the answer is **consumed** by the poll, so asking twice answers
///   `Unknown` rather than the same thing forever;
/// * a driver that refuses says so in the poll, in its own words, rather than
///   in a return value the caller never looked at.
///
/// The refusal is made with a subscriber of this client's own holding an
/// address: a receive destination binds the address it names rather than
/// joining a channel endpoint, so one that is already held cannot be created.
#[test]
fn a_destination_can_be_added_without_waiting_for_the_driver() {
    let Some(mut own) = OwnDriver::start("mds-async-destination") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let publication = client
        .add_exclusive_publication(&publisher_channel(ASYNC_PORT), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the publication the destination names");
    let mds = client
        .add_subscription(MDS_CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a manual subscription");

    // The add returns a handle and nothing else: no answer has been read, and
    // the driver has not been waited on.
    let added = client
        .async_add_rcv_destination(mds, &subscriber_channel(ASYNC_PORT), DEFAULT_TIMEOUT)
        .expect("the command goes into the ring");

    // And nothing has been waited on: an answer can only have been read by
    // `Client::poll`, which the call above did not make, so the poll below must
    // find the request still outstanding. This is what says the call returned
    // **before** the driver answered rather than after — the one thing about a
    // non-blocking face that its own return value cannot show.
    assert!(
        matches!(
            client.destination_poll(added),
            AsyncDestinationPoll::Awaiting
        ),
        "the command has not been answered yet: nothing has driven the client"
    );

    let ready = await_destination(&mut client, added);
    assert!(
        matches!(ready, AsyncDestinationPoll::Ready),
        "the driver takes the destination: {ready:?}"
    );
    assert!(
        matches!(
            client.destination_poll(added),
            AsyncDestinationPoll::Unknown
        ),
        "and the answer was consumed by the poll that read it"
    );

    // The destination is really on, which is the half a caller cannot see from
    // its own return value: the publisher is connected to a reader it has never
    // heard of before this.
    assert!(
        await_connected(&mut client, publication),
        "the publisher behind the address that was just added is connected"
    );

    // And the remove has the same two faces.
    let removed = client
        .async_remove_rcv_destination(mds, &subscriber_channel(ASYNC_PORT), DEFAULT_TIMEOUT)
        .expect("the command goes into the ring");

    assert!(
        matches!(
            await_destination(&mut client, removed),
            AsyncDestinationPoll::Ready
        ),
        "the driver takes the removal"
    );

    // The refusal: a second subscription holds an address, and a receive
    // destination naming it is one the driver cannot create.
    let held = client
        .add_subscription(&subscriber_channel(HELD_PORT), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the subscription that holds the address");

    let refused = client
        .async_add_rcv_destination(mds, &subscriber_channel(HELD_PORT), DEFAULT_TIMEOUT)
        .expect("the command goes into the ring");

    match await_destination(&mut client, refused) {
        AsyncDestinationPoll::Failed(CommandError::Driver { message, .. }) => assert!(
            message.contains("Address already in use"),
            "the driver's own words for the refusal: {message}"
        ),
        other => panic!("a destination that cannot be created is refused: {other:?}"),
    }

    let _ = held;
    let _ = own.stop();
}

/// Drive the client until a destination command has been answered.
fn await_destination(client: &mut Client, destination: AsyncDestination) -> AsyncDestinationPoll {
    let deadline = Instant::now() + DEADLINE;

    loop {
        assert!(Instant::now() < deadline, "the driver never answered");

        client.poll();

        match client.destination_poll(destination) {
            AsyncDestinationPoll::Awaiting => std::thread::sleep(Duration::from_millis(1)),
            answered => return answered,
        }
    }
}
