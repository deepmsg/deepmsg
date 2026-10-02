//! G3-1c: `aeron:ipc` as a receive destination.
//!
//! A multi-destination subscription's channel is a *network* one — it is built
//! with `control-mode=manual` and gains sockets as sources are added — and one
//! of the sources it may be given is a publication in this driver's own memory.
//! `aeron:ipc` as a destination is that source: the subscription is handed an
//! IPC publication's log buffer, with the IPC channel constant as its source
//! identity, and reads it without a socket being opened for it.
//!
//! It is the mirror of `aeron-spy:` and shares its handler's shape
//! (`aeron_driver_conductor.c:5617-5700` against `:5808-5870`). What is
//! different is what it reads: a spy names a *network* publication, and this
//! names an IPC one.
//!
//! Both halves are covered here — the link is made when the destination is
//! added, and given up when the destination is removed — because the reference
//! resolves them in two functions and a build with only one of them would
//! deliver messages and still leak a reader.
//!
//! Everything here is our own, so it needs no reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream every test here publishes on.
const STREAM_ID: i32 = 1001;

/// One kilobyte of payload. Small enough that a handful of messages are one
/// datagram each, which is all this file has to say about the wire.
const LENGTH: usize = 1024;

/// How long the whole exchange may take before the test is a failure.
const DEADLINE: Duration = Duration::from_secs(30);

/// A UDP port to name in the channel, derived from the process id so that two
/// tests running at once do not choose the same one.
fn free_port(offset: u16) -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 20_000 + (std::process::id() as u16 % 20_000);

    base.saturating_add(offset)
}

/// The message carrying number `index`, so a message delivered twice or out of
/// order is a failure rather than a count that happens to add up.
fn numbered(index: i64) -> Vec<u8> {
    let mut payload = vec![0x7C_u8; LENGTH];
    payload[..8].copy_from_slice(&index.to_le_bytes());

    payload
}

/// Poll until the subscription holds `expected` images, or fail.
///
/// An image is what a destination gives a subscription, and it arrives as a
/// separate message after the acknowledgement — so "the destination was added"
/// and "the subscription is reading it" are two different facts, and this waits
/// for the second.
fn await_images(client: &mut Client, subscription: i64, expected: usize, what: &str) {
    let deadline = Instant::now() + DEADLINE;

    loop {
        let held = client
            .subscription(subscription)
            .expect("the subscription")
            .images()
            .len();

        if held == expected {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "{what}: the subscription holds {held} images, not {expected}"
        );

        client.poll();
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Read until the subscription has nothing left.
///
/// The reader has to be at the producer's position before its publication can
/// finish draining (`is_drained`), and a reader left behind holds the
/// publication in `DRAINING` — a state whose readers are told nothing, because
/// the announcement belongs to the one after it. The reference's own test polls
/// the subscription to the end before it closes the publication for the same
/// reason (`Tests.awaitConnected`).
fn drain_to_end(client: &mut Client, subscription: i64) {
    let deadline = Instant::now() + DEADLINE;

    loop {
        if 0 == client.poll_subscription(subscription, 100, |_message| {}) {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "the subscription never caught up with the publication"
        );
    }
}

/// An IPC publication added to a manual subscription is a source it reads: the
/// messages go in through `offer`, come out of `poll_subscription`, and the
/// image the client is told about is the publication's own registration id.
///
/// It is the fact `docs/compat.md` used to record as a divergence — "`aeron:ipc`
/// is refused as a receive destination" — and the row is gone with this test.
#[test]
fn an_ipc_publication_can_be_added_to_a_subscription_as_a_source() {
    let Some(mut own) = OwnDriver::start("ipc-as-a-source") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let publication = client
        .add_publication("aeron:ipc", STREAM_ID, DEFAULT_TIMEOUT)
        .expect("an IPC publication");

    // The subscription a source may be added to has to be a
    // `control-mode=manual` one: it is the kind with no socket of its own and
    // therefore the kind a source can be named on.
    let mds_channel = format!(
        "aeron:udp?control=127.0.0.1:{}|control-mode=manual",
        free_port(5)
    );
    let mds = client
        .add_subscription(&mds_channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a multi-destination subscription");

    await_images(&mut client, mds, 0, "it starts with no source");

    // The answer is an acknowledgement of the *destination*: the subscription
    // already exists, and a destination is something it has.
    let destination = client
        .add_rcv_destination(mds, "aeron:ipc", DEFAULT_TIMEOUT)
        .expect("an IPC destination is served");

    assert!(destination > 0, "the destination has a registration id");

    // And the subscription is handed the publication's own buffer, under the
    // subscription's registration id — one thing, not two.
    await_images(
        &mut client,
        mds,
        1,
        "the IPC source gives the subscription an image",
    );

    let images = client.subscription(mds).expect("the subscription").images();
    assert_eq!(
        publication,
        images[0].registration_id(),
        "the image is the publication's log buffer, not something built from datagrams"
    );

    // Read a few messages through it. This is the half that makes the test
    // worth having: a link with a counter and no bytes would pass every
    // assertion above.
    let deadline = Instant::now() + DEADLINE;
    let mut offered = 0_i64;
    let mut received = 0_i64;

    while received < 20 {
        assert!(
            Instant::now() < deadline,
            "{received} of 20 messages after {DEADLINE:?}; {offered} were offered"
        );

        while offered < 40 {
            if let Some(Appended::Ok { .. }) = client.offer(publication, &numbered(offered)) {
                offered += 1;
            } else {
                break;
            }
        }

        client.poll();
        client.poll_subscription(mds, 10, |message| {
            assert_eq!(
                numbered(received),
                message.payload,
                "message {received} arrived out of order or was delivered twice"
            );
            received += 1;
        });
    }

    // Taking the source out announces the image it was holding, and the
    // subscription itself stays: only a source was removed.
    client
        .remove_rcv_destination(mds, "aeron:ipc", DEFAULT_TIMEOUT)
        .expect("the source comes back out");

    await_images(&mut client, mds, 0, "the image goes with the source");
    assert!(client.subscription(mds).is_some());

    // The other way round, which is a different path in the driver: the
    // publication goes first and the destination is still attached. The
    // reference takes the reader with the publication
    // (`aeron_driver_conductor_unlink_ipc_subscriptions`), and the client is
    // told its image is gone rather than left polling a log buffer that is
    // about to be unmapped.
    client
        .add_rcv_destination(mds, "aeron:ipc", DEFAULT_TIMEOUT)
        .expect("the source goes back on");

    await_images(&mut client, mds, 1, "and reads the publication again");

    // And this reader is polled to the end of the stream before the
    // publication goes, because that is what "drained" means: a publication
    // finishes draining when every reader is at the producer's position
    // (`is_drained`), and one left behind holds it in `DRAINING` — whose
    // readers are told nothing, since the announcement belongs to the state
    // after it. The reference's own test polls the subscription first for the
    // same reason (`Tests.awaitConnected`).
    drain_to_end(&mut client, mds);

    client
        .remove_publication(publication, DEFAULT_TIMEOUT)
        .expect("the publication closes");

    await_images(&mut client, mds, 0, "the image goes with the publication");

    // And the destination can still be taken off a subscription with no
    // publication behind it: a link whose source is gone is still a link.
    client
        .remove_rcv_destination(mds, "aeron:ipc", DEFAULT_TIMEOUT)
        .expect("the source comes back out");

    assert!(client.subscription(mds).is_some());

    let _ = own.stop();
}
