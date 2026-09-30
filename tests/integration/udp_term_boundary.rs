//! A stream that reaches the end of a term, over UDP.
//!
//! A term is filled by frames, and the last one rarely fits: when it does not,
//! the sender covers the remainder with a **padding** frame — a data header
//! whose length is the whole remainder, sent as **its header alone**, because
//! the tail it describes is not data (`aeron_udp_protocol.h:237` never compares
//! a data frame's length against the datagram, and the reference's own send
//! path does the same).
//!
//! A receiver has to insert that frame like any other. When one did not — the
//! dispatch read every DATA-or-PAD datagram through a reader that **refuses PAD
//! by design** — the term's tail stayed zeroed, the image's gap scanner read it
//! as a hole, and the receiver asked for those bytes forever.
//!
//! **What this test pins is the *batched* case**: the sender appends a term's
//! last data frame and the padding behind it to one datagram, and the receiver
//! writes both. That is the shape a stream produces on its own — the padding
//! rides with the frame before it whenever the datagram budget has room — and
//! it is half of the story `tests/interop/udp_padding_frame.rs` tells: the
//! other half is a padding that arrives **alone**, which is what happens when
//! the sender's window ends exactly at the term boundary or when the padding is
//! retransmitted, and which this shape cannot produce.
//!
//! Neither shape was covered before: the other UDP tests use 32-byte messages,
//! and `16 MiB / 64` divides exactly, so no term ever ends with a partial
//! message and no padding frame is ever sent at all. Here the term is 64 KiB
//! and the message is a kilobyte, so every term ends with one and a boundary
//! arrives after 62 messages instead of after 15,887.
//!
//! It runs against **our own driver** and our own client, so it needs no
//! reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::driver::{self, OwnDriver};

/// How many messages to send.
///
/// A 64 KiB term holds 62 of them, so this crosses three term boundaries — each
/// of which ends with a padding frame — while staying a fast test.
const MESSAGES: i64 = 200;

/// One kilobyte of payload, plus a 32-byte header, aligned to 1056 — which does
/// not divide 64 KiB.
const LENGTH: usize = 1024;

/// How long the whole exchange may take before the test is a failure.
const DEADLINE: Duration = Duration::from_secs(30);

#[test]
fn a_subscriber_crosses_the_term_boundary_a_padding_frame_fills() {
    let Some(mut own) = OwnDriver::start("udp-term-boundary") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");
    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{}|term-length=64k",
        free_port()
    );
    let stream_id = 1002;

    let publication = client
        .add_publication(&channel, stream_id, DEFAULT_TIMEOUT)
        .expect("a publication on the channel");
    let subscription = client
        .add_subscription(&channel, stream_id, DEFAULT_TIMEOUT)
        .expect("a subscription on the channel");

    await_image(&mut client, subscription);

    let deadline = Instant::now() + DEADLINE;
    let mut offered = 0_i64;
    let mut received = 0_i64;

    while received < MESSAGES {
        assert!(
            Instant::now() < deadline,
            "received {received} of {MESSAGES} messages after {DEADLINE:?}; {offered} were \
             offered — a reader stopped at a term boundary never catches up"
        );

        // The window is closed until the subscriber's status messages have been
        // through, so an offer that comes back anything but `Ok` is the loop
        // asking again rather than a failure.
        while offered < MESSAGES {
            let payload = numbered(offered);

            if let Some(Appended::Ok { .. }) = client.offer(publication, &payload) {
                offered += 1;
            } else {
                break;
            }
        }

        client.poll();
        client.poll_subscription(subscription, 10, |message| {
            assert_eq!(
                numbered(received),
                message.payload,
                "message {received} arrived out of order or was delivered twice"
            );
            received += 1;
        });
    }

    assert_eq!(MESSAGES, offered, "every message was offered");
    let _ = own.stop();
}

/// The message carrying number `index`, so a message delivered twice or out of
/// order is a failure rather than a count that happens to add up.
fn numbered(index: i64) -> Vec<u8> {
    let mut payload = vec![0xA5_u8; LENGTH];
    payload[..8].copy_from_slice(&index.to_le_bytes());

    payload
}

/// Wait until the subscription has an image, which is what "the publication is
/// connected to this endpoint" looks like from here.
fn await_image(client: &mut Client, subscription: i64) {
    let deadline = Instant::now() + Duration::from_secs(10);

    while Instant::now() < deadline {
        client.poll();

        if client
            .subscription(subscription)
            .is_some_and(|subscription| !subscription.images().is_empty())
        {
            return;
        }

        std::thread::sleep(Duration::from_millis(5));
    }

    panic!("the subscription never saw an image");
}

/// A UDP port nothing else on this machine is likely to hold.
///
/// The same arithmetic `tests/interop/udp_transport.rs` uses: high ports, from
/// a range derived from the process id, so two runs collide only if something
/// else chose exactly this one.
fn free_port() -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 20_000 + (std::process::id() as u16 % 20_000);

    base
}
