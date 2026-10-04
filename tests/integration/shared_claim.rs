//! Claiming space in a **shared** publication, and a subscriber reading it.
//!
//! `offer` copies the payload into the term. `try_claim` hands back a window
//! onto the term instead, which is the only way to produce bytes in place —
//! Java's `ConcurrentPublication.tryClaim`, and `aeron_publication_try_claim`
//! behind it. What a unit test cannot say is that the bytes written through the
//! window are the bytes a reader gets, which is what this is for.
//!
//! Everything here is our own client against our own driver over `aeron:ipc`,
//! so it needs no reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH;
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream every test here publishes on.
const STREAM_ID: i32 = 1001;

/// Shared memory with a term small enough that the window moves: the default
/// IPC term is orders of magnitude larger, and a producer that never meets the
/// window would never be told to wait.
const CHANNEL: &str = "aeron:ipc?term-length=64k";

/// How long a test may take before it is a failure rather than a slow machine.
const DEADLINE: Duration = Duration::from_secs(30);

/// The message carrying `index`, so one delivered twice or out of order is a
/// failure rather than a count that happens to add up.
fn numbered(index: i64) -> Vec<u8> {
    let text = format!("claimed message {index}");
    let mut payload = text.into_bytes();
    payload.resize(64, b'.');
    payload
}

#[test]
fn a_claimed_message_is_the_bytes_a_subscriber_reads() {
    let Some(mut own) = OwnDriver::start("shared-claim") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a shared publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

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

    let mut arrived: Vec<Vec<u8>> = Vec::new();
    let mut sent = 0i64;
    let total = 20i64;

    while sent < total || (arrived.len() as i64) < total {
        assert!(
            Instant::now() < deadline,
            "claimed {sent} of {total}, read {}",
            arrived.len()
        );

        client.poll();

        if sent < total {
            let payload = numbered(sent);

            // The window, which the reader below moves — and the reason a
            // claim has to be retried rather than merely failing.
            match client.try_claim(publication, payload.len()) {
                Some(Ok(claim)) => {
                    let frame = claim.frame();

                    // The frame the reader will take, one header longer than
                    // the payload. `frame_length` reads negative until
                    // `publish`, which is what keeps a reader off a frame
                    // being written.
                    let frame_length =
                        i32::try_from(payload.len() + DATA_HEADER_LENGTH).expect("small");

                    assert!(
                        frame.frame_length().expect("in range") < 0,
                        "a claimed frame is not readable until it is published"
                    );

                    frame.write_payload(&payload).expect("in range");
                    frame.publish(frame_length).expect("in range");

                    sent += 1;
                }

                // The window, which the reader below moves; a rotation, which
                // the next pass claims into. Both are "try again".
                Some(Err(
                    Appended::BackPressured
                    | Appended::NotConnected
                    | Appended::EndOfLog
                    | Appended::MidRotation,
                )) => {}

                Some(Err(other)) => panic!("claim {sent} came back {other:?}"),
                None => panic!("the publication is gone"),
            }
        }

        client.poll_subscription(subscription, 10, |message| {
            arrived.push(message.payload.to_vec());
        });
    }

    assert_eq!(total as usize, arrived.len());

    for (index, payload) in arrived.iter().enumerate() {
        assert_eq!(
            numbered(index as i64),
            *payload,
            "message {index} is the bytes that were written into the claim, \
             not one written over by the claim that followed it"
        );
    }
}
