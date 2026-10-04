//! Batch two of the Java alignment: what a client can see of its images.
//!
//! Three things the client could not do before, each of them something the
//! reference has had all along:
//!
//! * learn that an image arrived or left without walking every subscription
//!   (`AvailableImageHandler`/`UnavailableImageHandler`, `Aeron.java:417`);
//! * poll a subscription **by fragment** rather than by reassembled message
//!   (`Subscription.poll`, `Subscription.java:188`), which is what makes a
//!   zero-copy reader possible at all;
//! * ask an image where it came from, where the reader joined, and whether the
//!   stream has ended (`Image.sourceIdentity`, `Image.joinPosition`,
//!   `Image.isEndOfStream`).
//!
//! Everything here is our own driver and our own client over `aeron:ipc`, so it
//! needs no reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_client::image_event::ImageEvent;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::logbuffer::descriptor::END_OF_STREAM_OPEN;
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream every test here uses.
const STREAM_ID: i32 = 1001;

/// Shared memory with a term small enough to fill: the default IPC term is
/// orders of magnitude larger, and the fragmentation below wants a window it
/// can actually move.
const CHANNEL: &str = "aeron:ipc?term-length=64k";

/// `AERON_IPC_MTU_LENGTH_DEFAULT` (`aeron_driver_context.c`), which is what a
/// channel that names no `mtu` gets.
const IPC_MTU: i32 = 1408;

/// How long the whole exchange may take before the test is a failure.
const DEADLINE: Duration = Duration::from_secs(30);

/// A driver of ours with a client connected to it.
fn own_driver_and_client(name: &str) -> Option<(OwnDriver, Client)> {
    let mut own = OwnDriver::start(name)?;

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    Some((own, client))
}

/// Poll until the subscription holds an image.
fn await_image(client: &mut Client, subscription: i64) {
    let deadline = Instant::now() + DEADLINE;

    while client
        .subscription(subscription)
        .expect("the subscription")
        .images()
        .is_empty()
    {
        assert!(Instant::now() < deadline, "no image arrived");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Drain image events, polling, until at least one has arrived.
fn await_image_event(client: &mut Client) -> Vec<ImageEvent> {
    let deadline = Instant::now() + DEADLINE;
    let mut events = Vec::new();

    while events.is_empty() {
        assert!(Instant::now() < deadline, "no image event arrived");
        client.poll();
        events.extend(client.image_events());
        std::thread::sleep(Duration::from_millis(1));
    }

    events
}

#[test]
fn an_image_event_announces_the_image_and_its_removal() {
    let Some((_own, mut client)) = own_driver_and_client("image-events") else {
        driver::announce_own_skip();
        return;
    };

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    // The event is the announcement, not the image: it has to arrive without
    // anything walking the subscription's image list.
    let events = await_image_event(&mut client);
    assert_eq!(1, events.len(), "one image, one event: {events:?}");

    await_image(&mut client, subscription);

    // Copied out rather than borrowed, because the removal below needs the
    // client mutably and the image lives inside it.
    let (session_id, join_position) = {
        let image = client
            .subscription(subscription)
            .expect("the subscription")
            .images()
            .first()
            .expect("the image the event named");

        (image.session_id(), image.join_position())
    };

    assert_eq!(
        ImageEvent::Available {
            subscription_registration_id: subscription,
            publication_registration_id: publication,
            session_id,
            stream_id: STREAM_ID,
            position: join_position,
        },
        events[0],
        "the event names the image the subscription actually holds"
    );

    // Removing the publication takes the image with it, and that is announced
    // too.
    client
        .remove_publication(publication, DEFAULT_TIMEOUT)
        .expect("the publication is removed");

    let gone = await_image_event(&mut client);
    assert_eq!(
        ImageEvent::Unavailable {
            subscription_registration_id: subscription,
            publication_registration_id: publication,
            session_id,
            stream_id: STREAM_ID,
            position: join_position,
        },
        gone[0],
        "and that the image went away, from where this reader had got to"
    );

    assert!(
        client
            .subscription(subscription)
            .expect("the subscription")
            .images()
            .is_empty(),
        "the image is gone from the subscription as well"
    );
}

#[test]
fn a_fragmented_message_is_delivered_once_per_frame() {
    let Some((_own, mut client)) = own_driver_and_client("fragment-poll") else {
        driver::announce_own_skip();
        return;
    };

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_image(&mut client, subscription);

    // Larger than one frame can carry, so the append path splits it — which is
    // the whole point of the fragment-level poll.
    let payload = vec![0x5au8; 4000];
    let per_frame = client
        .publication(publication)
        .expect("the publication")
        .max_payload_length()
        .expect("a payload length");
    let expected_frames = payload.len().div_ceil(per_frame);
    assert!(
        expected_frames > 1,
        "the message has to fragment for this test to mean anything: {payload:?} bytes fits \
         {per_frame} per frame"
    );

    let deadline = Instant::now() + DEADLINE;
    loop {
        assert!(
            Instant::now() < deadline,
            "the publication never took the message"
        );
        client.poll();

        match client.offer(publication, &payload) {
            Some(Appended::Ok { .. }) => break,
            Some(Appended::BackPressured) => {}
            other => panic!("offer came back {other:?}"),
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    let mut frames: Vec<Vec<u8>> = Vec::new();
    let deadline = Instant::now() + DEADLINE;
    while frames.len() < expected_frames {
        assert!(
            Instant::now() < deadline,
            "read {} of {expected_frames} frames",
            frames.len()
        );

        client.poll();
        client.poll_subscription_fragments(subscription, 10, |fragment| {
            let mut payload = vec![0u8; fragment.payload_length()];
            fragment.copy_payload(&mut payload);
            frames.push(payload);
        });

        std::thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        expected_frames,
        frames.len(),
        "the frames are delivered one at a time, not reassembled"
    );

    let rebuilt: Vec<u8> = frames.concat();
    assert_eq!(payload, rebuilt, "and they still add up to the message");
}

#[test]
fn an_images_state_is_readable_from_the_client() {
    let Some((_own, mut client)) = own_driver_and_client("image-state") else {
        driver::announce_own_skip();
        return;
    };

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_image(&mut client, subscription);

    let image = client
        .subscription(subscription)
        .expect("the subscription")
        .images()
        .first()
        .expect("the image");

    // The driver's answer to the channel, not the string this client wrote:
    // an IPC image reports the constant (`aeron_driver_conductor.c:3612-3613`).
    assert_eq!("aeron:ipc", image.source_identity());

    assert_eq!(
        image.position(),
        image.join_position(),
        "a reader that has read nothing is where it joined"
    );

    assert_eq!(Some(IPC_MTU), image.mtu_length());

    assert_eq!(
        Some(END_OF_STREAM_OPEN),
        image.end_of_stream_position(),
        "the stream is open, so it has no end position yet"
    );
    assert_eq!(
        Some(false),
        image.is_end_of_stream(),
        "and a reader that has not reached one is not at the end"
    );

    assert_eq!(
        Some(0),
        image.active_transport_count(),
        "an IPC image has no transports to count"
    );
    assert_eq!(Some(false), image.is_publication_revoked());

    // And the publication is still there, so nothing above was the last gasp.
    assert!(client.publication(publication).is_some());
}
