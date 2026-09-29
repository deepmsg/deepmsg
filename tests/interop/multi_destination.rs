//! A4: a reference publisher's messages, arriving through a **receive
//! destination** we added to a manual subscription.
//!
//! Every other interop test in this tree gives the subscription an address of
//! its own — `aeron:udp?endpoint=…` — and that address is where the publisher
//! sends. This one gives the subscription **none**: the channel is
//! `aeron:udp?control-mode=manual`, a channel with no socket until something
//! is added to it, and the only address in the whole arrangement is the one in
//! the destination a client adds (`aeron_subscription_async_add_destination`).
//!
//! So the two things this test can falsify that nothing else can:
//!
//! 1. **The destination is what binds.** A manual channel has no socket until a
//!    destination arrives; if the destination binds anywhere but the address
//!    its URI names — `remote_data`, the same field the reference's receive
//!    port manager is handed (`media/aeron_receive_destination.c:55-63`) —
//!    then the publisher's frames land nowhere and not one message arrives.
//! 2. **A session can come into being on a destination's socket.** The `SETUP`
//!    that describes the stream arrives on the destination's transport, not on
//!    an endpoint's, and the image built from it must send its status messages
//!    back the way that packet came.
//!
//! Who speaks first is not a detail here but the whole shape of it, and it is
//! worth stating because the obvious answer is wrong. The destination names no
//! `control=`, so `has_explicit_control` is false and the receiver sends
//! **nothing** when the destination is added
//! (`aeron_receive_channel_endpoint.c:1136-1156`, which is a no-op without it).
//! No `SEND_SETUP` is ever sent — the first frame on the wire is the
//! publisher's own, because a publication with no initial connection sends a
//! `SETUP` to its endpoint address on a timer
//! (`aeron_network_publication.c:586-589` and `:380-430`, whose clock is
//! started one timeout in the past at `:285` so the first one goes out at
//! once). The receiver answers that `SETUP` with its status message, which is
//! what opens the publisher's window — and from there the data flows.
//!
//! Measured, not inferred: on the reference's own
//! `aeron_c_multi_destination_test.cpp` MDS case, the first frame of the whole
//! session is an outbound `SETUP` from the publication and no `SEND_SETUP`
//! appears at any point.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT, FRAGMENT_LIMIT};
use deepmsg_client::fragment_assembler::Message;
use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT, ReferenceDriver};
use deepmsg_tests::samples;

/// One stream, so a failure names its own driver.
const STREAM_ID: i32 = 4104;

/// The channel a destination can be added to at all
/// (`aeron_udp_channel_is_multi_destination`, `media/aeron_udp_channel.h:147-151`).
///
/// It names no `endpoint`, as the reference's own MDS test does not
/// (`aeron-driver/src/test/c/aeron_c_multi_destination_test.cpp:32`): there is
/// no local address here at all, which is exactly what makes the destination
/// the only address in the arrangement.
const MANUAL_CHANNEL: &str = "aeron:udp?control-mode=manual";

/// A port in a range this process is unlikely to collide with, offset so that
/// two tests in one binary do not ask for the same one.
fn free_udp_port(offset: u16) -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 20_000 + (std::process::id() as u16 % 20_000);

    base.saturating_add(offset)
}

/// Collect `expected` messages, or as many as arrive before the deadline.
fn drain_messages(
    client: &mut Client,
    subscription_id: i64,
    within: Duration,
    expected: usize,
) -> Vec<Vec<u8>> {
    let deadline = Instant::now() + within;
    let mut collected = Vec::new();

    while Instant::now() < deadline && collected.len() < expected {
        // `poll` is what reads the driver's events — an `ON_AVAILABLE_IMAGE`
        // does not arrive by itself — and `poll_subscription` is what reads the
        // image it attaches.
        client.poll();

        client.poll_subscription(subscription_id, FRAGMENT_LIMIT, |message: Message<'_>| {
            collected.push(message.payload.to_vec());
        });

        std::thread::sleep(Duration::from_millis(10));
    }

    collected
}

/// What our client currently believes about the subscription, for a failure.
fn client_view(client: &Client, subscription_id: i64) -> String {
    match client.subscription(subscription_id) {
        Some(subscription) => format!(
            "channel={} images={}",
            subscription.channel(),
            subscription.images().len()
        ),
        None => "no such subscription".to_string(),
    }
}

#[test]
fn a_reference_publishers_messages_reach_our_receive_destination() {
    let Some(publisher_binary) = samples::locate("BasicPublisher") else {
        driver::announce_tool_skip("BasicPublisher");
        return;
    };

    let Some(reference_binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    // Two names, because the harness names each driver's log after the test —
    // one name for both would put two drivers' output in one file.
    let Some(mut own) = OwnDriver::start("mds-our-subscriber") else {
        driver::announce_own_skip();
        return;
    };

    let mut reference = ReferenceDriver::start(&reference_binary, "mds-reference-publisher")
        .expect("start the reference driver");

    let port = free_udp_port(11);
    // The publisher's channel, and — verbatim — the destination's URI. The
    // destination is not another address convention: it is the published
    // channel (`aeron_c_multi_destination_test.cpp:96-106`, which passes
    // `PUB_URI_1` to both).
    let published_channel = format!("aeron:udp?endpoint=localhost:{port}");

    let _reference_cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the reference driver must publish a readable CnC file");
    // Readiness only: a client cannot attach before the driver has published
    // its file. Nothing here asserts on the counters it holds — the two facts
    // this test is about are on the wire and in the client's own view.
    let _cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    // The subscriber first, and this time it is not merely the polite order:
    // our destination is what binds `port`, so a publisher started first would
    // be sending into a closed one. Nothing else in this arrangement has that
    // socket — the subscription's own channel names no address.
    let mut subscriber = Client::connect(own.aeron_dir()).expect("connect our client");
    let subscription_id = subscriber
        .add_subscription(MANUAL_CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the manual subscription");

    let _destination_id = subscriber
        .add_rcv_destination(subscription_id, &published_channel, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the destination");

    let messages = 5;
    let mut publisher = samples::Sample::start(
        &publisher_binary,
        "publisher",
        reference.aeron_dir(),
        &[
            "-c",
            &published_channel,
            "-s",
            &STREAM_ID.to_string(),
            "-m",
            &messages.to_string(),
        ],
    );

    publisher.await_output(Duration::from_secs(20), "its publication", |output| {
        output.contains("Publication") || output.contains("published")
    });

    let received = drain_messages(
        &mut subscriber,
        subscription_id,
        Duration::from_secs(30),
        messages,
    );

    let publisher_said = publisher.output();
    let _ = publisher.terminate(Duration::from_secs(5));
    let _ = own.stop();
    let _ = reference.stop();

    assert!(
        !received.is_empty(),
        "not one message arrived through the destination.\nour client sees:\n{}\n\
         the publisher said:\n{publisher_said}\nour driver said:\n{}",
        client_view(&subscriber, subscription_id),
        own.log_tail(60)
    );

    // Both sides of the same fact. Ours: the bytes are the reference's own.
    // Theirs: the reference's publisher believed it was connected to a
    // subscriber — which it can only believe if a status message of ours
    // reached it, and the only way one could is through the `SETUP` it sent
    // into the destination.
    //
    // The count is the publisher's rather than a constant because the sample
    // offers once a second and gives up on the run after `-m`, so an offer that
    // came back "not connected" is a message the reference never published —
    // not ours to deliver. What has to hold is that *everything it did
    // publish* arrived.
    let published: Vec<u64> = publisher_said
        .lines()
        .filter(|line| line.ends_with("yay!"))
        .filter_map(|line| {
            line.strip_prefix("offering ")?
                .split_once('/')?
                .0
                .parse()
                .ok()
        })
        .collect();

    assert!(
        !published.is_empty(),
        "the reference publisher never reported a successful offer, so nothing \
         of ours reached its flow control:\n{publisher_said}"
    );

    let received_text: Vec<String> = received
        .iter()
        .map(|message| String::from_utf8_lossy(message).to_string())
        .collect();
    let received_index: Vec<u64> = received_text
        .iter()
        .filter_map(|text| text.strip_prefix("Hello World! ")?.parse().ok())
        .collect();

    assert_eq!(
        received_text.len(),
        received_index.len(),
        "every message that arrived is one of the sample's own: {received_text:?}"
    );
    assert_eq!(
        published, received_index,
        "every message the reference published arrived, once, in order"
    );

    // And the window did not close again afterwards. A failure to publish
    // *after* the first success is a different defect from a first handshake
    // that was merely slow — it would be our status messages stopping — and
    // the equality above cannot tell the two apart.
    let connected_at = publisher_said
        .find("yay!")
        .expect("a success, checked above");
    assert!(
        !publisher_said[connected_at..].contains("not connected to a subscriber"),
        "the reference publisher lost its subscriber after it had one:\n{publisher_said}"
    );
}
