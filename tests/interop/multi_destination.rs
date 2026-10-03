//! The destination plane at the message level, in both directions.
//!
//! Every other interop test in this tree puts the reference opposite a single
//! address: a channel's own `endpoint`, which is where one side sends and the
//! other listens. A destination is the thing that replaces that — the channel
//! itself may name no address at all, and the addresses are the ones clients
//! add.
//!
//! **A1/A2/A3 (sending):** our publication takes destinations and fans out to
//! two reference subscribers, each behind its own reference driver. This is the
//! only test in the tree where one publication's bytes have to reach two
//! independent receivers.
//!
//! **A4 (receiving):** a reference publisher's messages, arriving through a
//! **receive destination** we added to a manual subscription.
//!
//! The receive side is described first below because its shape is the
//! surprising one; the send side reuses the same two facts.
//!
//! ## A4: a reference publisher's messages, through a receive destination
//!
//! This one gives the subscription **no** address of its own: the channel is
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

use std::path::Path;
use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT, FRAGMENT_LIMIT};
use deepmsg_client::fragment_assembler::Message;
use deepmsg_core::logbuffer::append::Appended;
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

/// Which of the sample's offers it reported as published, from what it wrote
/// to stdout.
///
/// **Not line by line.** The sample prints its publication's status footer —
/// `4104:-1586784077` — with no newline after it, so its first offer lands on
/// the same line as that footer, and the record is split by a newline in the
/// middle of it:
///
/// ```text
/// 4104:-1586784077offering 0/
/// 5 - yay!
/// ```
///
/// A reader that took each line for a record would drop exactly the first
/// message the publisher did publish — which reads as a driver that delivered
/// something nobody sent. The records have no newlines of their own, so the
/// newlines go first and the records are found by their own shape.
///
/// An offer that failed says what it failed with instead of `yay!`, and is
/// left out for the same reason the test's comment gives: a message the
/// publisher never published is not ours to deliver.
fn scan_offers(output: &str) -> Vec<u64> {
    let flattened: String = output
        .chars()
        .filter(|character| *character != '\n')
        .collect();

    flattened
        .split("offering ")
        .skip(1)
        .filter_map(|record| {
            let (index, rest) = record.split_once('/')?;
            let (_, result) = rest.split_once(" - ")?;

            result
                .starts_with("yay!")
                .then(|| index.trim().parse().ok())
                .flatten()
        })
        .collect()
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
    let published: Vec<u64> = scan_offers(&publisher_said);

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

/// A reference subscriber behind its own reference driver.
///
/// Each one binds the port its channel names, so each is a separate receiver
/// with a separate `aeron.dir` — which is what makes this two destinations
/// rather than one driver with two sockets.
struct ReferenceSubscriber {
    driver: ReferenceDriver,
    sample: samples::Sample,
}

impl ReferenceSubscriber {
    fn start(
        driver_binary: &std::path::Path,
        subscriber_binary: &std::path::Path,
        name: &str,
        channel: &str,
    ) -> Self {
        let mut driver = ReferenceDriver::start(driver_binary, name).expect("start the driver");
        driver
            .await_cnc(READY_TIMEOUT)
            .expect("the reference driver must publish a readable CnC file");

        let sample = samples::Sample::start(
            subscriber_binary,
            "subscriber",
            driver.aeron_dir(),
            &["-c", channel, "-s", &STREAM_ID.to_string()],
        );
        sample.await_output(Duration::from_secs(20), "its subscription", |output| {
            output.contains("Subscription channel status")
        });

        Self { driver, sample }
    }

    /// The payloads it has printed that begin with `prefix`, in order.
    ///
    /// The sample prints one line per *message* — its own assembler has
    /// already put fragments back together
    /// (`aeron-samples/src/main/cpp/BasicSubscriber.cpp:72-78`) — so what comes
    /// out is the application's payload and not the wire's frames.
    ///
    /// The prefix is how a test separates what it is asserting on from what it
    /// published to get here: a warm-up message is not a fact about anything,
    /// and this suite's payloads all name their own phase.
    fn received(&self, prefix: &str) -> Vec<String> {
        self.sample
            .output()
            .lines()
            .filter(|line| line.starts_with("Message to stream "))
            .filter_map(|line| {
                let rest = line.split_once("<<")?.1;
                Some(rest.strip_suffix(">>")?.to_string())
            })
            .filter(|payload| payload.starts_with(prefix))
            .collect()
    }

    /// The port the sample reported as the source of the image it has — this
    /// driver's address, as the wire saw it
    /// (`aeron-samples/src/main/cpp/BasicSubscriber.cpp:110-116`).
    fn image_source_port(&self) -> Option<u16> {
        self.sample
            .output()
            .lines()
            .find(|line| line.starts_with("Available image "))
            .and_then(|line| line.rsplit_once("from ").map(|(_head, address)| address))
            .and_then(|address| address.trim().rsplit_once(':'))
            .and_then(|(_ip, port)| port.trim().parse().ok())
    }

    /// Whether this subscriber has an image right now: the last thing it said
    /// about images was that one became available.
    ///
    /// The sample prints both transitions
    /// (`aeron-samples/src/main/cpp/BasicSubscriber.cpp:110-122`), so "it has
    /// one" is a statement about the *order* of those two lines rather than
    /// about either one alone.
    fn has_image(&self) -> bool {
        self.sample
            .output()
            .lines()
            .rfind(|line| {
                line.starts_with("Available image ") || line.starts_with("Unavailable image ")
            })
            .is_some_and(|line| line.starts_with("Available image "))
    }

    /// Wait for `expected` payloads beginning with `prefix`, or give up.
    fn await_received(&self, prefix: &str, expected: usize, within: Duration) -> Vec<String> {
        let deadline = Instant::now() + within;
        let mut payloads = self.received(prefix);

        while payloads.len() < expected && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
            payloads = self.received(prefix);
        }

        payloads
    }

    fn stop(self) {
        let mut sample = self.sample;
        let mut driver = self.driver;
        let _ = sample.terminate(Duration::from_secs(5));
        let _ = driver.stop();
    }
}

/// A1: one publication, two destinations, two reference drivers.
///
/// The send side of the destination plane, and the thing none of the
/// single-address tests can reach: `send()` has to fan out, and a *control*
/// frame — the `SETUP` that brings each image into being — has to go to each
/// destination by the same route the data does. Get that wrong and the
/// failure is not a corrupt byte, it is a subscriber that never hears anything
/// at all, because a publication with no image never opens its window.
///
/// The two subscribers are the reference's *own* sample programs behind the
/// reference's *own* driver, so neither side of the fan-out shares code with
/// this build: whatever they print, they printed from bytes this driver put on
/// the wire.
#[test]
fn our_publications_destinations_reach_two_reference_subscribers() {
    let Some(subscriber_binary) = samples::locate("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };

    let Some(reference_binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    let Some(mut own) = OwnDriver::start("mdc-our-publisher") else {
        driver::announce_own_skip();
        return;
    };

    // Readiness only; nothing here asserts on the counters.
    let _cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let channel_one = format!("aeron:udp?endpoint=localhost:{}", free_udp_port(21));
    let channel_two = format!("aeron:udp?endpoint=localhost:{}", free_udp_port(22));

    // The subscribers first: each binds the port its own destination will
    // name, and a destination is only an address — nothing would retry into a
    // port that was closed when the command was issued.
    let first = ReferenceSubscriber::start(
        &reference_binary,
        &subscriber_binary,
        "mdc-reference-subscriber-1",
        &channel_one,
    );
    let second = ReferenceSubscriber::start(
        &reference_binary,
        &subscriber_binary,
        "mdc-reference-subscriber-2",
        &channel_two,
    );

    let mut publisher = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication_id = publisher
        .add_publication(MANUAL_CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the manual publication");

    publisher
        .add_destination(publication_id, &channel_one, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the first destination");
    publisher
        .add_destination(publication_id, &channel_two, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the second destination");

    // Both destinations have to be *up* before anything is asserted about the
    // messages, and getting the second one up is itself the interesting part.
    //
    // A publication announces itself to every destination on a 100 ms cadence
    // **only while it has no connection at all** (`aeron_network_publication.c:586-589`),
    // and the two `add_destination` commands are serviced a few milliseconds
    // apart — so the first `SETUP` can go out to the first destination alone,
    // the first subscriber answers it, and the announcement stops before the
    // second destination ever sees one. Measured, on this test: the wire shows
    // `SETUP → :28502` on its own, then the first `DATA` to both destinations,
    // and only *then* a `SEND_SETUP` from :28503 — the second subscriber
    // eliciting a `SETUP` for a session it is receiving data for and knows
    // nothing about (`aeron_data_packet_dispatcher.c:616-659`).
    //
    // That is the protocol working, not a defect: a destination added to a
    // publication that is already connected joins **mid-stream**, through the
    // same elicitation path a late subscriber uses. The warm-up below is
    // therefore not a workaround — it is the thing that has to happen, and the
    // assertion after it is that it happened.
    let warm_up = warm_up(
        &publisher,
        publication_id,
        &[&first, &second],
        Duration::from_secs(20),
    );

    let first_has_image = first.has_image();
    let second_has_image = second.has_image();
    let log = own.log_tail(60);

    assert!(
        warm_up && first_has_image && second_has_image,
        "a destination that never comes up cannot receive anything: \
         first={first_has_image} second={second_has_image}\nour driver said:\n{log}"
    );

    // Every payload carries its own number, so a subscriber that received the
    // right *count* of the wrong bytes is a failure rather than a pass.
    let messages = 5;
    let payloads: Vec<Vec<u8>> = (0..messages)
        .map(|index| format!("destination {index}").into_bytes())
        .collect();

    let offered = offer_all(
        &publisher,
        publication_id,
        &payloads,
        Duration::from_secs(20),
    );

    let first_saw = first.await_received("destination ", messages, Duration::from_secs(20));
    let second_saw = second.await_received("destination ", messages, Duration::from_secs(20));

    let log = own.log_tail(60);
    first.stop();
    second.stop();
    let _ = own.stop();

    let expected: Vec<String> = payloads
        .iter()
        .map(|payload| String::from_utf8_lossy(payload).to_string())
        .collect();

    assert_eq!(
        offered, messages,
        "the publication's window never opened for every message — a destination \
         that never answered cannot be offered to.\nour driver said:\n{log}"
    );
    assert_eq!(
        expected, first_saw,
        "the first subscriber did not receive what was published\nour driver said:\n{log}"
    );
    assert_eq!(
        expected, second_saw,
        "the second subscriber did not receive what was published\nour driver said:\n{log}"
    );
}

/// Offer throwaway payloads until every subscriber has an image, or give up.
///
/// The payloads are distinguishable from the ones a test asserts on — they
/// carry `warmup` — because the point is to get each destination to the state
/// where it *can* receive, and which of the warm-up messages a destination
/// caught on the way up is not a fact about anything.
///
/// An offer that comes back `NotConnected` is expected: the window is closed
/// until the first subscriber's status message arrives, and that subscriber
/// only has somewhere to send one after the publication's own `SETUP` reached
/// it.
fn warm_up(
    client: &Client,
    publication_id: i64,
    subscribers: &[&ReferenceSubscriber],
    within: Duration,
) -> bool {
    let deadline = Instant::now() + within;
    let mut index = 0;

    while Instant::now() < deadline {
        if subscribers.iter().all(|subscriber| subscriber.has_image()) {
            return true;
        }

        let payload = format!("warmup {index}");
        let _ = client.offer(publication_id, payload.as_bytes());
        index += 1;

        std::thread::sleep(Duration::from_millis(10));
    }

    subscribers.iter().all(|subscriber| subscriber.has_image())
}

/// Offer every payload, retrying until the window opens, and answer how many
/// went through.
///
/// The window is closed until each subscriber's status message has reached the
/// publication, and a subscriber learns where to send one only after the first
/// `SETUP` reaches it — so the first attempts are expected to come back
/// `NotConnected`, and that is the chain this test is about rather than a
/// reason to fail.
fn offer_all(
    client: &Client,
    publication_id: i64,
    payloads: &[Vec<u8>],
    within: Duration,
) -> usize {
    let deadline = Instant::now() + within;
    let mut offered = 0;

    while offered < payloads.len() && Instant::now() < deadline {
        match client.offer(publication_id, &payloads[offered]) {
            Some(Appended::Ok { .. }) => offered += 1,
            Some(Appended::BackPressured | Appended::NotConnected) | None => {
                std::thread::sleep(Duration::from_millis(10));
            }
            other => panic!("the driver refused the payload: {other:?}"),
        }
    }

    offered
}

/// A2: removing a destination takes exactly one receiver out of the fan-out.
///
/// The claim is narrow and worth stating precisely: after the removal the
/// *removed* subscriber receives none of the messages published **from then
/// on**, and the other one still receives all of them. Nothing is asserted
/// about frames already in flight — a datagram sent a microsecond before the
/// command was serviced is not a stale destination. That is why the payloads
/// carry their phase as well as their number: a test that counted messages
/// would be measuring the race rather than the removal.
///
/// Both commands are exercised, because both are one conversation with the
/// driver and only one of them is by URI: `remove_destination` matches on the
/// channel, `remove_destination_by_id` on the registration id the *add*
/// answered with.
#[test]
fn removing_a_destination_leaves_the_other_one_receiving() {
    let Some(subscriber_binary) = samples::locate("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };

    let Some(reference_binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    let Some(mut own) = OwnDriver::start("mdc-remove-destination") else {
        driver::announce_own_skip();
        return;
    };

    let _cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let channel_one = format!("aeron:udp?endpoint=localhost:{}", free_udp_port(31));
    let channel_two = format!("aeron:udp?endpoint=localhost:{}", free_udp_port(32));

    let first = ReferenceSubscriber::start(
        &reference_binary,
        &subscriber_binary,
        "mdc-remove-subscriber-1",
        &channel_one,
    );
    let second = ReferenceSubscriber::start(
        &reference_binary,
        &subscriber_binary,
        "mdc-remove-subscriber-2",
        &channel_two,
    );

    let mut publisher = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication_id = publisher
        .add_publication(MANUAL_CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the manual publication");

    let _first_destination = publisher
        .add_destination(publication_id, &channel_one, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the first destination");
    let second_destination = publisher
        .add_destination(publication_id, &channel_two, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the second destination");

    // Both destinations have to be up before anything is asserted about the
    // messages: the second one joins mid-stream through the elicitation path,
    // exactly as in A1, and a message it was never able to receive is not a
    // fact about removal.
    let up = warm_up(
        &publisher,
        publication_id,
        &[&first, &second],
        Duration::from_secs(20),
    );
    assert!(
        up,
        "both destinations have to be up before removal means anything"
    );

    let messages = 3;
    let bodies = |phase: &str| -> Vec<String> {
        (0..messages)
            .map(|index| format!("{phase} {index}"))
            .collect()
    };
    let as_bytes = |phase: &str| -> Vec<Vec<u8>> {
        bodies(phase).into_iter().map(String::into_bytes).collect()
    };

    // Both destinations live: both subscribers have to see these.
    let before = as_bytes("before");
    assert_eq!(
        before.len(),
        offer_all(&publisher, publication_id, &before, Duration::from_secs(20)),
        "both destinations are added by now, so every message has to be offered"
    );
    let _ = first.await_received("before ", messages, Duration::from_secs(20));
    let _ = second.await_received("before ", messages, Duration::from_secs(20));

    // One destination leaves, by URI.
    publisher
        .remove_destination(publication_id, &channel_one, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the removal");

    let mid = as_bytes("mid");
    assert_eq!(
        mid.len(),
        offer_all(&publisher, publication_id, &mid, Duration::from_secs(20)),
        "the remaining destination still has to take every message"
    );
    let second_saw_mid = second.await_received("mid ", messages, Duration::from_secs(20));
    let first_saw_mid = first.received("mid ");
    let first_saw_before = first.received("before ");

    // And the last one leaves, by id — nothing is left to send to.
    publisher
        .remove_destination_by_id(publication_id, second_destination, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the removal by id");

    let after = as_bytes("after");
    let _ = offer_all(&publisher, publication_id, &after, Duration::from_secs(5));
    std::thread::sleep(Duration::from_millis(500));
    let second_saw_after = second.received("after ");

    let log = own.log_tail(60);
    first.stop();
    second.stop();
    let _ = own.stop();

    // Phase one: the first subscriber was receiving, which is what makes the
    // silence after its removal a statement about removal rather than about a
    // destination that never worked at all.
    assert_eq!(
        messages,
        first_saw_before.len(),
        "the first subscriber has to have been receiving before it was removed: \
         {first_saw_before:?}"
    );
    assert!(
        first_saw_mid.is_empty(),
        "the removed subscriber received messages published after its removal: {first_saw_mid:?}"
    );
    assert_eq!(
        bodies("mid"),
        second_saw_mid,
        "the destination that stayed has to keep receiving\nour driver said:\n{log}"
    );
    assert!(
        second_saw_after.is_empty(),
        "with every destination removed nothing is left to send to, but the last \
         subscriber received: {second_saw_after:?}"
    );
}

/// Read `mdc-num-dest` out of a driver's own counters, with the reference's own
/// viewer (`AeronStat -d <dir> -w false`).
///
/// The label rather than the number is matched: ids are handed out in
/// allocation order, and only the label says what a counter *is*.
fn mdc_num_dest(stat_binary: &Path, dir: &Path) -> Result<i64, String> {
    let text = aeron_stat_text(stat_binary, dir)?;

    for line in text.lines() {
        let Some((_id, rest)) = line.trim_start().split_once(':') else {
            continue;
        };
        let Some((value, label)) = rest.rsplit_once(" - ") else {
            continue;
        };

        // The label carries the channel after a colon (`mdc-num-dest: <channel>`),
        // as every channel-scoped counter does.
        if label.trim().starts_with("mdc-num-dest") {
            return value
                .trim()
                .parse::<i64>()
                .map_err(|error| format!("parse {value:?}: {error}\n{text}"));
        }
    }

    Err(format!("no mdc-num-dest counter in:\n{text}"))
}

/// What the reference's own viewer says about a driver's counters.
fn aeron_stat_text(stat_binary: &Path, dir: &Path) -> Result<String, String> {
    let output = std::process::Command::new(stat_binary)
        .arg("-d")
        .arg(dir)
        .arg("-w")
        .arg("false")
        .output()
        .map_err(|error| format!("run AeronStat: {error}"))?;

    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// The address in the `snd-local-sockaddr` counter's label: where this driver's
/// send socket was bound, as another program reads it.
///
/// The label is `snd-local-sockaddr: <channel status id> <ip:port>`
/// (`aeron_position.c:295-297`).
fn send_local_sockaddr(stat_binary: &Path, dir: &Path) -> Result<String, String> {
    let text = aeron_stat_text(stat_binary, dir)?;

    for line in text.lines() {
        let Some((_id, rest)) = line.trim_start().split_once(':') else {
            continue;
        };
        let Some((_value, label)) = rest.rsplit_once(" - ") else {
            continue;
        };

        if let Some(rest) = label.trim().strip_prefix("snd-local-sockaddr: ") {
            return rest
                .rsplit_once(' ')
                .map(|(_status_id, address)| address.to_string())
                .ok_or_else(|| format!("an address was expected in {label:?}\n{text}"));
        }
    }

    Err(format!("no snd-local-sockaddr counter in:\n{text}"))
}

/// A3, the manual half: `mdc-num-dest` follows the client's commands and
/// nothing else.
///
/// This is the counter that says how many places a multi-destination channel
/// is sending to, read by the *reference's* own counter viewer rather than by
/// this build's reader — so it is a statement about the CnC file as another
/// program sees it, not about what this driver believes it wrote.
///
/// The last step is the one with a rule in it: a **manual** channel's
/// destinations never time out (`aeron_udp_destination_tracker.c:107-118`
/// removes an entry for inactivity only when the control mode is *dynamic*),
/// so stopping the subscriber behind a destination does not take the
/// destination away. A count that fell there would be this driver expiring
/// something the reference keeps.
///
/// The dynamic half — an entry that a subscriber's status message *creates*,
/// and that five seconds of silence removes — is not here: it needs a
/// subscriber whose status messages reach this publication's control address,
/// which is its own arrangement.
#[test]
fn the_destination_count_follows_the_destinations_of_a_manual_channel() {
    let Some(stat_binary) = driver::locate_aeron_stat() else {
        driver::announce_tool_skip("AeronStat");
        return;
    };

    let Some(subscriber_binary) = samples::locate("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };

    let Some(reference_binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    let Some(mut own) = OwnDriver::start("mdc-destination-count") else {
        driver::announce_own_skip();
        return;
    };

    let _cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");
    let dir = own.aeron_dir().to_path_buf();

    let channel_one = format!("aeron:udp?endpoint=localhost:{}", free_udp_port(41));
    let channel_two = format!("aeron:udp?endpoint=localhost:{}", free_udp_port(42));

    let first = ReferenceSubscriber::start(
        &reference_binary,
        &subscriber_binary,
        "mdc-count-subscriber-1",
        &channel_one,
    );
    let second = ReferenceSubscriber::start(
        &reference_binary,
        &subscriber_binary,
        "mdc-count-subscriber-2",
        &channel_two,
    );

    let mut publisher = Client::connect(own.aeron_dir()).expect("connect our client");

    // One reading per step, each after the driver has answered the command
    // that changed it — the commands are synchronous, so the counter is a
    // statement about a completed operation rather than about a race.
    let mut readings: Vec<(&str, Result<i64, String>)> = Vec::new();

    let publication_id = publisher
        .add_publication(MANUAL_CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the manual publication");
    readings.push(("after the publication", mdc_num_dest(&stat_binary, &dir)));

    publisher
        .add_destination(publication_id, &channel_one, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the first destination");
    readings.push(("after one destination", mdc_num_dest(&stat_binary, &dir)));

    publisher
        .add_destination(publication_id, &channel_two, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the second destination");
    readings.push(("after two destinations", mdc_num_dest(&stat_binary, &dir)));

    publisher
        .remove_destination(publication_id, &channel_one, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the removal");
    readings.push(("after removing one", mdc_num_dest(&stat_binary, &dir)));

    // The subscriber behind the remaining destination goes away. On a manual
    // channel that is not a reason to forget it.
    first.stop();
    let mut second_sample = second;
    let _ = second_sample.sample.terminate(Duration::from_secs(5));
    std::thread::sleep(Duration::from_secs(7));
    readings.push((
        "seven seconds after the subscriber stopped",
        mdc_num_dest(&stat_binary, &dir),
    ));

    publisher
        .remove_destination(publication_id, &channel_two, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the removal");
    readings.push(("after removing the last", mdc_num_dest(&stat_binary, &dir)));

    let _ = second_sample.driver.stop();
    let log = own.log_tail(60);
    let _ = own.stop();

    let values: Vec<i64> = readings
        .iter()
        .map(|(step, reading)| {
            *reading
                .as_ref()
                .unwrap_or_else(|error| panic!("{step}: {error}\nour driver said:\n{log}"))
        })
        .collect();

    assert_eq!(
        vec![0, 1, 2, 1, 1, 0],
        values,
        "the count has to follow the commands and nothing else: {:?}",
        readings.iter().map(|(step, _)| *step).collect::<Vec<_>>()
    );
}

/// Read `mdc-num-dest` until it is `expected`, or give up.
fn await_mdc_num_dest(
    stat_binary: &Path,
    dir: &Path,
    expected: i64,
    within: Duration,
) -> Result<i64, String> {
    let deadline = Instant::now() + within;
    let mut reading = mdc_num_dest(stat_binary, dir);

    while Instant::now() < deadline {
        if matches!(reading, Ok(value) if value == expected) {
            return reading;
        }

        std::thread::sleep(Duration::from_millis(50));
        reading = mdc_num_dest(stat_binary, dir);
    }

    reading
}

/// A3, the dynamic half: on a `control-mode=dynamic` channel the destinations
/// are not the client's — they are made and unmade by the status messages that
/// arrive at the channel's own control address.
///
/// The arrangement is what makes this a different test from the manual half.
/// The publication **binds** the control address (`control=localhost:C`) and
/// has no destination at all until something talks to it; the subscriber binds
/// its own endpoint `P` and sends its control messages *to C*
/// (`aeron_udp_channel.c:465-487`: a channel that names a `control=` sends its
/// control frames there). So the subscriber speaks first — the same shape as a
/// destination with an explicit control address — and the publication learns
/// where to send data from the **source** of what arrived
/// (`aeron_udp_destination_tracker.c:263-300`).
///
/// Both halves of the entry's life are asserted, and they have different
/// causes: one status message **creates** it, and five seconds without one
/// **removes** it (`AERON_UDP_DESTINATION_TRACKER_DESTINATION_TIMEOUT_NS`,
/// `media/aeron_udp_destination_tracker.h:30-53`; this build's value is the
/// same five seconds, `destination_tracker.rs:39`). A manual channel never
/// does the second — that is the half above.
#[test]
fn a_dynamic_channels_destinations_follow_the_status_messages() {
    let Some(stat_binary) = driver::locate_aeron_stat() else {
        driver::announce_tool_skip("AeronStat");
        return;
    };

    let Some(subscriber_binary) = samples::locate("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };

    let Some(reference_binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    let Some(mut own) = OwnDriver::start("mdc-dynamic-destination-count") else {
        driver::announce_own_skip();
        return;
    };

    let _cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");
    let dir = own.aeron_dir().to_path_buf();

    let subscriber_channel = format!(
        "aeron:udp?endpoint=localhost:{}|control=localhost:{}",
        free_udp_port(51),
        free_udp_port(52)
    );
    let publication_channel = format!(
        "aeron:udp?control=localhost:{}|control-mode=dynamic",
        free_udp_port(52)
    );

    // The publication first: it binds the control address the subscriber will
    // talk to, and there is nothing on the other side yet.
    let mut publisher = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication_id = publisher
        .add_publication(&publication_channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the dynamic publication");

    let empty = mdc_num_dest(&stat_binary, &dir);

    let subscriber = ReferenceSubscriber::start(
        &reference_binary,
        &subscriber_binary,
        "mdc-dynamic-subscriber",
        &subscriber_channel,
    );

    // The subscriber's own `SEND_SETUP` is the first status message to arrive
    // at C, and it is what makes the entry.
    let created = await_mdc_num_dest(&stat_binary, &dir, 1, Duration::from_secs(20));

    // Stop it and wait out the timeout: no more status messages arrive, so the
    // entry expires. Nothing about the *client* changes — the publication is
    // still open — which is what makes this a statement about the tracker.
    subscriber.stop();
    let expired = await_mdc_num_dest(&stat_binary, &dir, 0, Duration::from_secs(20));

    let log = own.log_tail(60);
    let _ = own.stop();

    assert_eq!(
        Ok(0),
        empty,
        "a dynamic publication starts with no destinations — nothing has talked \
         to it yet.\nour driver said:\n{log}"
    );
    assert_eq!(
        Ok(1),
        created,
        "the subscriber's status message has to create a destination.\nour driver said:\n{log}"
    );
    assert_eq!(
        Ok(0),
        expired,
        "and five seconds without one has to remove it.\nour driver said:\n{log}"
    );
    let _ = publication_id;
}

/// A15, the sending half: the address a send endpoint is bound to is readable,
/// and it is the address that is really on the wire.
///
/// Two independent readings of one fact, which is the point. One is the
/// reference's own counter viewer printing this driver's
/// `snd-local-sockaddr` label; the other is the reference's own subscriber
/// reporting where the bytes it received came from. The label is a claim this
/// driver makes about itself; the subscriber's line is what the kernel did.
///
/// Only the **port** is compared, and that is not a weakening: this channel
/// names no local address, so the socket is bound to the wildcard and
/// `getsockname` says `0.0.0.0:<port>` while the datagrams that reach a
/// subscriber on loopback come from `127.0.0.1:<port>`. The port is the part
/// that was unknowable before the socket existed — which is why the counter
/// exists at all, and what `ReplayMerge` reads it for.
#[test]
fn the_address_a_send_endpoint_is_bound_to_is_what_a_reader_finds() {
    let Some(stat_binary) = driver::locate_aeron_stat() else {
        driver::announce_tool_skip("AeronStat");
        return;
    };

    let Some(subscriber_binary) = samples::locate("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };

    let Some(reference_binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    let Some(mut own) = OwnDriver::start("mdc-send-local-sockaddr") else {
        driver::announce_own_skip();
        return;
    };

    let _cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");
    let dir = own.aeron_dir().to_path_buf();

    let channel = format!("aeron:udp?endpoint=localhost:{}", free_udp_port(61));
    let subscriber = ReferenceSubscriber::start(
        &reference_binary,
        &subscriber_binary,
        "mdc-address-subscriber",
        &channel,
    );

    let mut publisher = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication_id = publisher
        .add_publication(MANUAL_CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the manual publication");
    publisher
        .add_destination(publication_id, &channel, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the destination");

    // Any traffic will do, and the publication's own periodic `SETUP` is the
    // first of it: what is being read is an address, not a payload.
    let up = warm_up(
        &publisher,
        publication_id,
        &[&subscriber],
        Duration::from_secs(20),
    );

    let label = send_local_sockaddr(&stat_binary, &dir);
    let from_the_wire = subscriber.image_source_port();

    let log = own.log_tail(60);
    subscriber.stop();
    let _ = own.stop();

    assert!(
        up,
        "the subscriber never formed an image\nour driver said:\n{log}"
    );

    let label = label.unwrap_or_else(|error| panic!("{error}\nour driver said:\n{log}"));
    let label_port: u16 = label
        .rsplit_once(':')
        .and_then(|(_ip, port)| port.trim().parse().ok())
        .unwrap_or_else(|| panic!("no port in the counter's address {label:?}"));

    let wire_port = from_the_wire.expect("the subscriber reports where its image came from");

    assert_ne!(0, label_port, "the port has to be the one the kernel chose");
    assert_eq!(
        wire_port, label_port,
        "the counter says {label}, and the bytes that reached the subscriber \
         came from port {wire_port}\nour driver said:\n{log}"
    );
}
