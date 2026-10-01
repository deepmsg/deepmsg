//! Rejecting an image, from both clients' side, against a real driver.
//!
//! The whole chain in one test, because no piece of it is worth anything on its
//! own: one client publishes, another subscribes and rejects the image it is
//! reading, and the **publisher's** client is the one that hears about it. That
//! last part is the one that is easy to leave out and impossible to notice —
//! the `ERR` frame goes to the publisher's own driver, and a driver that drops
//! it there leaves the mechanism working with nobody told.
//!
//! `aeron:ipc` rather than UDP, so the test needs no sockets and no second
//! process: for IPC the thing a subscriber is handed *is* the publication, and
//! the rejection is the IPC branch of the driver's handler. The network branch
//! is covered where it can be, in `crates/driver/src/conductor.rs`.
//!
//! The reference's own version of this is
//! `io.aeron.RejectImageTest.shouldOnlyReceivePublicationErrorFrameOnRelevantClient`,
//! which is what the G1-1 slice is accepted by; this is the same shape against
//! this build's own client.

use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cnc::command::ERROR_CODE_IMAGE_REJECTED;
use deepmsg_tests::driver::{self, OwnDriver};

const STREAM_ID: i32 = 1001;
const CHANNEL: &str = "aeron:ipc";
const REASON: &str = "Needs to be closed";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Poll both clients until `ready` says so, or the deadline passes.
fn await_until(within: Duration, mut poll: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + within;

    while Instant::now() < deadline {
        if poll() {
            return true;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    false
}

/// One client's subscription, polled until its first image is attached.
fn subscription_with_an_image(client: &mut Client, registration_id: i64) -> bool {
    await_until(Duration::from_secs(10), || {
        client.poll();
        client
            .subscription(registration_id)
            .is_some_and(|subscription| !subscription.images().is_empty())
    })
}

#[test]
fn a_rejected_image_tells_the_publisher_and_nobody_else() {
    let Some(mut own) = OwnDriver::start("reject-image") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    // Two clients, because the point of the test is *which* one is told: the
    // publication error belongs to the client holding the publication, and a
    // subscriber that rejected the image hears nothing back.
    let mut publisher_client = Client::connect(own.aeron_dir()).expect("the publisher connects");
    let mut subscriber_client = Client::connect(own.aeron_dir()).expect("the subscriber connects");

    let publication = publisher_client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");

    let subscription = subscriber_client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    assert!(
        subscription_with_an_image(&mut subscriber_client, subscription),
        "the subscription is handed the publication's image"
    );

    // One message across, so this reader has a position to reject at rather
    // than the join position it never advanced past.
    //
    // The offer is retried rather than asserted: an IPC publication's limit
    // starts where the stream does, and it is the *driver* that raises it once
    // it has seen the subscriber's position counter — which is a pass of its
    // own and not something the client can hurry.
    let mut messages = 0usize;
    let read = await_until(Duration::from_secs(10), || {
        publisher_client.poll();

        if messages == 0
            && matches!(
                publisher_client.offer(publication, b"hello"),
                Some(deepmsg_core::logbuffer::append::Appended::Ok { .. })
            )
        {
            messages = 1;
        }

        subscriber_client.poll();
        subscriber_client.poll_subscription(subscription, 10, |_| {});

        messages > 0
            && subscriber_client
                .subscription(subscription)
                .and_then(|subscription| subscription.image(publication))
                .is_some_and(|image| image.position() > 0)
    });
    assert!(read, "the subscriber reads a message");

    let image_registration_id = subscriber_client
        .subscription(subscription)
        .and_then(|subscription| subscription.image(publication))
        .expect("the image")
        .registration_id();

    assert_eq!(
        publication, image_registration_id,
        "for `aeron:ipc` the image a subscriber holds *is* the publication, \
         which is why one command has two targets"
    );

    let position = subscriber_client
        .subscription(subscription)
        .and_then(|subscription| subscription.image(publication))
        .expect("the image")
        .position();

    subscriber_client
        .reject_image(image_registration_id, position, REASON, DEFAULT_TIMEOUT)
        .expect("the driver acts on the rejection");

    // The subscriber is told nothing: the error frame is the publisher's.
    assert!(
        subscriber_client.publication_errors().is_empty(),
        "a client that rejected an image is not the one owed the news"
    );

    // Drained into `errors` rather than checked in place: the drain *is* the
    // read, so a wait condition that called it would empty the queue it was
    // waiting on.
    let mut errors = Vec::new();
    let told = await_until(Duration::from_secs(10), || {
        publisher_client.poll();
        errors.extend(publisher_client.publication_errors());
        !errors.is_empty()
    });
    assert!(told, "the publisher's client hears about it");

    assert_eq!(1, errors.len());
    let error = &errors[0];

    assert_eq!(publication, error.registration_id);
    assert_eq!(STREAM_ID, error.stream_id);
    assert_eq!(ERROR_CODE_IMAGE_REJECTED, error.error_code);
    assert_eq!(REASON.as_bytes(), error.message);
    assert!(error.is_rejected() && !error.is_revoked());

    // An IPC publication answers no destination and hears from no receiver, so
    // the three ids the driver had none of are the null value — and the source
    // is a loopback address with a zero port rather than an absence, which is
    // what the reference sends (`aeron_ipc_publication.c:231-243`).
    assert_eq!(-1, error.destination_registration_id);
    assert_eq!(-1, error.receiver_id);
    assert_eq!(-1, error.group_tag);
    assert_eq!(
        Some("1.0.0.127:0".parse().expect("an address")),
        error.source,
        "the reversal is the reference's; its own client reports the same bytes"
    );

    // Drained, so a second read finds nothing rather than the same event twice.
    assert!(publisher_client.publication_errors().is_empty());
}
