//! Our client's `REJECT_IMAGE`, against the **reference** driver.
//!
//! The local round trip in `tests/integration/reject_image.rs` proves this
//! build's encoder and decoder agree with each other, which is exactly what it
//! cannot prove they agree with anyone else. What is left is the one claim a
//! single implementation can never check on its own: that the record this
//! client sends is one the reference's driver reads, and that the
//! `ON_PUBLICATION_ERROR` it answers with is one this client reads.
//!
//! The record is worth testing here rather than anywhere else because the two
//! reference clients disagree about its length — the C one sends `sizeof` plus
//! the reason plus a NUL, the Java one `MINIMUM_SIZE` plus the reason — and the
//! difference is invisible until a driver refuses one of them.
//!
//! `aeron:ipc` over the reference driver, so the reject takes the IPC branch
//! there: the reference synthesizes the publication error directly
//! (`aeron_ipc_publication.c:216-277`) instead of sending an `ERR` frame, and
//! it is the same `ON_PUBLICATION_ERROR` either way.

use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cnc::command::ERROR_CODE_IMAGE_REJECTED;
use deepmsg_tests::driver::{self, READY_TIMEOUT, ReferenceDriver};

const STREAM_ID: i32 = 1001;
const CHANNEL: &str = "aeron:ipc";
const REASON: &str = "Needs to be closed";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

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

#[test]
fn the_reference_driver_reads_our_rejection_and_answers_with_an_error_frame() {
    let Some(binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    let mut reference =
        ReferenceDriver::start_with(&binary, "our-client-rejects", &[]).expect("start driver");
    reference
        .await_cnc(READY_TIMEOUT)
        .expect("the driver publishes its CnC file");

    let mut client = Client::connect(reference.aeron_dir()).expect("connect to the reference");

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    assert!(
        await_until(Duration::from_secs(20), || {
            client.poll();
            client
                .subscription(subscription)
                .is_some_and(|subscription| !subscription.images().is_empty())
        }),
        "the reference hands the subscription an image"
    );

    // One message across, so the reader has a position of its own to reject at
    // — and the offer is retried, because the limit is the *reference's* to
    // raise and it does that on its own pass.
    let mut messages = 0usize;
    assert!(
        await_until(Duration::from_secs(20), || {
            client.poll();

            if messages == 0
                && matches!(
                    client.offer(publication, b"hello"),
                    Some(deepmsg_core::logbuffer::append::Appended::Ok { .. })
                )
            {
                messages = 1;
            }

            client.poll_subscription(subscription, 10, |_| {});

            messages > 0
                && client
                    .subscription(subscription)
                    .and_then(|subscription| subscription.image(publication))
                    .is_some_and(|image| image.position() > 0)
        }),
        "the message crosses the reference driver's IPC path"
    );

    let position = client
        .subscription(subscription)
        .and_then(|subscription| subscription.image(publication))
        .expect("the image")
        .position();

    // The command under test. Its bytes are this build's; whether the reference
    // believes them is what the acknowledgement says — a record it called
    // malformed would be recorded and answered with **silence**, and this would
    // be a timeout instead.
    client
        .reject_image(publication, position, REASON, DEFAULT_TIMEOUT)
        .expect("the reference driver acts on the rejection");

    let mut errors = Vec::new();
    assert!(
        await_until(Duration::from_secs(20), || {
            client.poll();
            errors.extend(client.publication_errors());
            !errors.is_empty()
        }),
        "the reference driver answers with a publication error"
    );

    let error = &errors[0];
    assert_eq!(publication, error.registration_id);
    assert_eq!(STREAM_ID, error.stream_id);
    assert_eq!(ERROR_CODE_IMAGE_REJECTED, error.error_code);
    assert_eq!(REASON.as_bytes(), error.message);
    assert_eq!(-1, error.destination_registration_id);
    assert_eq!(-1, error.receiver_id);
    // The reference synthesizes a loopback `sockaddr_in` for an IPC rejection
    // and assigns `INADDR_LOOPBACK` to `s_addr` without an `htonl`, so the four
    // bytes it memcpy's out are the little-endian image of `0x7f000001`. This
    // is the assertion that keeps this driver writing the same bytes.
    assert_eq!(
        Some("1.0.0.127:0".parse().expect("an address")),
        error.source,
        "the reference's own bytes, read the way its own client reads them"
    );
}
