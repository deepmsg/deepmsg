//! A stream that said `reliable=false`, read from end to end.
//!
//! `reliable` had been parsed into a subscription's parameters since P1 and
//! read by nobody: the image wrote `true` into its metadata whatever the
//! channel said, and asked its sender for every hole it found. A client that
//! named the parameter got a reliable stream — the same failure G1 exists to
//! remove, in the one place where the difference is not a message but the data
//! itself.
//!
//! What the reference does with it (`aeron_publication_image.c:1024-1066`): an
//! unreliable image does not ask. It covers the hole with a padding frame and
//! reads on, so the frames that were in it are **gone** — never retransmitted,
//! never delivered — and `loss-gap-fills` is the only trace. That is a
//! deliberate trade a trading system can want: latency and bandwidth over
//! completeness, for a stream whose consumer can live without a message.
//!
//! So the assertions are a shape only this behaviour produces, and they are
//! deliberately a *combination*, because each one alone has another explanation:
//!
//! - fewer messages arrive than were published — the loss is not recovered;
//! - but most of them do — the reader was not merely stuck at the first hole;
//! - `loss-gap-fills` moved — the holes were filled, which is what let it past;
//! - no NAK went out and nothing was retransmitted — the recovery that would
//!   have closed the gap never happened.
//!
//! The loss is made by **this driver's own** seam
//! (`deepmsg.debug.send.data.loss.drop.every`, `config.rs`), so the test needs
//! no reference checkout and runs in CI. It is the same seam
//! `tests/interop/udp_transport.rs` drives a withheld frame through, with the
//! opposite expectation: there a reference subscriber gets every message back.
//!
//! The reference's own `GapFillLossTest` is the oracle this mirrors, and it
//! cannot run against this driver yet for two reasons that are not this slice's:
//! it injects its loss through an **incoming interceptor**
//! (`CTestMediaDriver.enableRandomLossOnReceive` sets
//! `AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS`), and it asserts the loss was
//! reported by reading the **loss report file** (`SystemTests.verifyLossOccurredForStream`).

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_driver::system_counters::id;
use deepmsg_tests::driver::{self, OwnDriver};

/// How many messages to publish.
///
/// A 64 KiB term holds 62 of them, so this crosses three terms and, at one
/// datagram withheld in four, leaves several holes in each.
const MESSAGES: i64 = 200;

/// One kilobyte of payload, plus the 32-byte header, for a frame of 1056 —
/// which is one datagram's worth, so the withheld datagrams are whole messages.
const LENGTH: usize = 1024;

/// How long the whole exchange may take before the test is a failure.
const DEADLINE: Duration = Duration::from_secs(30);

/// How long the stream has to be quiet before what has arrived is all there is.
const QUIET: Duration = Duration::from_millis(500);

#[test]
fn an_unreliable_subscription_reads_past_the_holes_it_does_not_ask_for() {
    // One datagram in every four is withheld from every endpoint this driver
    // sends through — the loss is on the way out, so the image never sees the
    // frame and the hole is real (`send_endpoints.rs::attach_data_loss_generator`).
    let Some(mut own) = OwnDriver::start_with(
        "unreliable-stream",
        &["-Ddeepmsg.debug.send.data.loss.drop.every=4"],
    ) else {
        driver::announce_own_skip();
        return;
    };

    let cnc = own
        .await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");
    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{}|term-length=64k",
        free_port()
    );
    let stream_id = 1003;

    let publication = client
        .add_publication(&channel, stream_id, DEFAULT_TIMEOUT)
        .expect("a publication on the channel");

    // The whole point: the same channel, and the one parameter that says this
    // reader would rather have holes than wait for them.
    let subscription = client
        .add_subscription(
            &format!("{channel}|reliable=false"),
            stream_id,
            DEFAULT_TIMEOUT,
        )
        .expect("an unreliable subscription on the channel");

    await_image(&mut client, subscription);

    let deadline = Instant::now() + DEADLINE;
    let mut offered = 0_i64;
    let mut received = 0_i64;

    // Offer everything. Each message runs the loss generator, so the holes are
    // made as the stream is written rather than by a sender that has already
    // stopped.
    while offered < MESSAGES {
        assert!(
            Instant::now() < deadline,
            "{offered} of {MESSAGES} messages were offered in {DEADLINE:?}"
        );

        while offered < MESSAGES {
            if let Some(Appended::Ok { .. }) = client.offer(publication, &numbered(offered)) {
                offered += 1;
            } else {
                break;
            }
        }

        client.poll();
        client.poll_subscription(subscription, 10, |_| received += 1);
    }

    // Then read until the stream goes quiet. A reliable receiver would stop
    // here with every message; this one stops short, and stays short.
    let mut last_change = Instant::now();
    let mut previous = received;

    while Instant::now() < deadline {
        client.poll();
        client.poll_subscription(subscription, 10, |_| received += 1);

        if received != previous {
            previous = received;
            last_change = Instant::now();
        } else if last_change.elapsed() > QUIET {
            break;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    let gap_fills = counter(&cnc, id::LOSS_GAP_FILLS);
    let naks = counter(&cnc, id::NAK_MESSAGES_SENT);
    let retransmits = counter(&cnc, id::RETRANSMITS_SENT);

    assert_eq!(MESSAGES, offered, "every message was offered");

    assert!(
        gap_fills > 0,
        "the stream must have been lossy enough to leave holes to fill, or this test proves \
         nothing: loss-gap-fills={gap_fills}"
    );
    assert!(
        received > MESSAGES / 2,
        "and the reader has to have moved past them: {received} of {MESSAGES} arrived, \
         gap-fills={gap_fills}"
    );
    assert!(
        received < MESSAGES,
        "{received} of {MESSAGES} messages arrived: an unreliable image fills its holes rather \
         than asking for them, so the ones lost on the wire are lost for good \
         (`aeron_publication_image.c:1053-1066`)"
    );
    assert_eq!(
        0, naks,
        "an unreliable image sends no NAK at all (`:1024`), so nothing may have asked"
    );
    assert_eq!(
        0, retransmits,
        "and nothing may have been retransmitted in answer to one"
    );

    let _ = own.stop();
}

/// One counter's value out of a live CnC file.
fn counter(cnc: &deepmsg_cnc::CncFile, counter_id: i32) -> i64 {
    cnc.counters()
        .and_then(|counters| counters.value(counter_id))
        .unwrap_or(0)
}

/// The message carrying number `index`, so a gap is a number that never arrived
/// rather than a count that happens to fall short.
fn numbered(index: i64) -> Vec<u8> {
    let mut payload = vec![0x5A_u8; LENGTH];
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
fn free_port() -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 40_000 + (std::process::id() as u16 % 20_000);

    base
}
