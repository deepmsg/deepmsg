//! The IPC pub/sub roundtrip, against a live reference driver.
//!
//! This is the acceptance test for P0's last item, and it is the first test in
//! the tree that can *falsify* the term-buffer work rather than merely restate
//! it: a log buffer only exists once a publication does, so until now every
//! assertion about frame layout was derived from the reference's source and
//! agreed with by our own reader.
//!
//! Four things have to be true at once, and each has its own failure mode:
//!
//! 1. `ADD_PUBLICATION` is encoded as the reference reads it, and
//!    `ON_PUBLICATION_READY`'s log path is parsed without the four-byte
//!    alignment `ON_AVAILABLE_IMAGE` uses.
//! 2. The log file is mapped at the right offset — the metadata is the **last**
//!    page, so a reader that assumes the CnC file's layout gets a term length of
//!    zero.
//! 3. A frame written by our producer is readable by our consumer, with the
//!    right session, stream and payload.
//! 4. **The subscriber's position counter unblocks the publisher.** This is the
//!    one that no single-sided test can reach: the driver raises the producer's
//!    window limit from the minimum of the subscriber positions, so a
//!    subscriber that reads without reporting eventually stops the producer.
//!    The second message below cannot be sent unless the first was reported.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT, FRAGMENT_LIMIT};
use deepmsg_client::fragment_assembler::Message;
use deepmsg_cnc::CncFile;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::driver::{self, READY_TIMEOUT, ReferenceDriver};

/// One stream, used by every test here so a failure names its own driver.
const STREAM_ID: i32 = 2001;

/// How long to keep offering while the driver has not opened the window yet.
const WINDOW_TIMEOUT: Duration = Duration::from_secs(10);

fn start(test_name: &str) -> Option<(ReferenceDriver, CncFile)> {
    let Some(binary) = driver::locate_verified() else {
        driver::announce_skip();
        return None;
    };

    let mut reference = ReferenceDriver::start_with(&binary, test_name, &[]).expect("start driver");
    let cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the driver must publish a readable CnC file");

    Some((reference, cnc))
}

/// Poll a client until it has an image on `subscription_id`, returning the
/// image's registration id.
fn await_image(client: &mut Client, subscription_id: i64, within: Duration) -> Option<i64> {
    let deadline = Instant::now() + within;
    loop {
        client.poll();

        if let Some(subscription) = client.subscription(subscription_id) {
            if let Some(image) = subscription.images().first() {
                return Some(image.registration_id());
            }
        }

        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Offer until the driver's window permits it, or give up.
///
/// The window is closed until a subscriber links and the driver's duty cycle
/// raises the limit, so the first attempts are expected to come back
/// `NotConnected` — which is a distinct outcome from `BackPressured`, and the
/// assertion below relies on that distinction being real.
fn offer_within(
    client: &Client,
    registration_id: i64,
    payload: &[u8],
    within: Duration,
) -> Result<Appended, Vec<Appended>> {
    let deadline = Instant::now() + within;
    let mut seen = Vec::new();

    loop {
        match client.offer(registration_id, payload) {
            Some(outcome @ Appended::Ok { .. }) => return Ok(outcome),
            Some(other) => seen.push(other),
            None => return Err(seen),
        }

        if Instant::now() >= deadline {
            return Err(seen);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Collect every message a subscription delivers within `within`.
///
/// Messages, not fragments: a subscription's default delivery reassembles the
/// frames a message arrived in, and this test is about the bytes surviving the
/// trip rather than about how many frames they took.
fn drain(client: &mut Client, subscription_id: i64, within: Duration) -> Vec<Vec<u8>> {
    let deadline = Instant::now() + within;
    let mut collected = Vec::new();

    while Instant::now() < deadline {
        client.poll_subscription(subscription_id, FRAGMENT_LIMIT, |message: Message<'_>| {
            collected.push(message.payload.to_vec());
        });

        if !collected.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    collected
}

#[test]
fn an_ipc_payload_round_trips_between_two_clients() {
    let Some((reference, _cnc)) = start("pubsub-roundtrip") else {
        return;
    };

    // The subscriber first. The driver links a *new* publication to the
    // subscriptions that already exist, so this order means the image arrives
    // without anything having to re-scan.
    let mut subscriber = Client::connect(reference.aeron_dir()).expect("connect subscriber");
    let subscription_id = subscriber
        .add_subscription("aeron:ipc", STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the driver must confirm the subscription");

    let mut publisher = Client::connect(reference.aeron_dir()).expect("connect publisher");
    let publication_id = publisher
        .add_publication("aeron:ipc", STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the driver must confirm the publication");

    // Reaching here means `ON_PUBLICATION_READY` decoded and the log buffer
    // mapped: `add_publication` does both, and fails if the path were parsed
    // with the image's alignment or the metadata read from the wrong end.
    let publication = publisher.publication(publication_id).expect("held");
    assert!(publication.session_id() != 0, "the driver allocates one");

    let image_id = await_image(&mut subscriber, subscription_id, WINDOW_TIMEOUT)
        .expect("an image should attach once a matching publication exists");

    let image = subscriber
        .subscription(subscription_id)
        .and_then(|s| s.image(image_id))
        .expect("the image is attached");
    assert_eq!(STREAM_ID, image.stream_id());
    assert_eq!(
        publication.session_id(),
        image.session_id(),
        "the image must be reading the publication it was linked to"
    );

    // Now send. The first attempt will almost certainly find the window shut —
    // the driver has not yet run the duty cycle that raises the limit — so the
    // retry loop is the test, not a workaround.
    let first = b"the first message";
    offer_within(&publisher, publication_id, first, WINDOW_TIMEOUT)
        .expect("the window should open once a subscriber is attached");

    let received = drain(&mut subscriber, subscription_id, WINDOW_TIMEOUT);
    assert_eq!(
        vec![first.to_vec()],
        received,
        "the payload must arrive byte for byte"
    );

    // The second message is the one that proves the position counter is being
    // reported. The producer's window is `min_sub_pos + term_length / 2`, and
    // it advances only as the subscriber reports; a subscriber that read
    // without publishing its position would leave the first message's worth of
    // window consumed and never replenished.
    let second = b"and the second";
    offer_within(&publisher, publication_id, second, WINDOW_TIMEOUT)
        .expect("the window should still be open after the subscriber reported");

    let received = drain(&mut subscriber, subscription_id, WINDOW_TIMEOUT);
    assert_eq!(vec![second.to_vec()], received);
}

#[test]
fn the_subscriber_position_counter_tracks_what_was_read() {
    // The counter is the contract between the two processes: the driver reads
    // it every duty cycle to compute the producer's limit, so "the consumer
    // advanced it" is not an implementation detail but the flow-control signal
    // itself. Asserting it directly means a failure points at the counter
    // rather than at a mysterious stall two messages later.
    let Some((reference, cnc)) = start("pubsub-position") else {
        return;
    };

    let mut subscriber = Client::connect(reference.aeron_dir()).expect("connect subscriber");
    let subscription_id = subscriber
        .add_subscription("aeron:ipc", STREAM_ID, DEFAULT_TIMEOUT)
        .expect("subscribe");

    let mut publisher = Client::connect(reference.aeron_dir()).expect("connect publisher");
    let publication_id = publisher
        .add_publication("aeron:ipc", STREAM_ID, DEFAULT_TIMEOUT)
        .expect("publish");

    let image_id = await_image(&mut subscriber, subscription_id, WINDOW_TIMEOUT)
        .expect("an image should attach");

    let (counter_id, join_position) = {
        let image = subscriber
            .subscription(subscription_id)
            .and_then(|s| s.image(image_id))
            .expect("attached");
        (image.subscriber_position_id(), image.position())
    };

    // The driver writes the join position into the counter as part of linking
    // the subscription, before it sends the image, so the image must have
    // started there rather than at zero.
    let counters = cnc.counters().expect("counters are readable");
    assert_eq!(
        Some(join_position),
        counters.value(counter_id),
        "the image should have joined where the driver said it did"
    );

    let payload = b"counted";
    offer_within(&publisher, publication_id, payload, WINDOW_TIMEOUT).expect("offer");

    let received = drain(&mut subscriber, subscription_id, WINDOW_TIMEOUT);
    assert_eq!(vec![payload.to_vec()], received);

    let after = counters.value(counter_id).expect("readable");
    assert!(
        after > join_position,
        "reading a frame must advance the subscriber position: {join_position} -> {after}"
    );

    // And the image agrees with the counter, which is the part the driver
    // actually consumes.
    let image = subscriber
        .subscription(subscription_id)
        .and_then(|s| s.image(image_id))
        .expect("attached");
    assert_eq!(Some(image.position()), counters.value(counter_id));
}
