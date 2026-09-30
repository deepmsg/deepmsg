//! A13: a subscription that reads a local publication's log buffer.
//!
//! `aeron-spy:` names a channel a client wants to read **from this driver's own
//! memory** rather than from the wire. What it gets back is an ordinary image
//! message whose log file is the publication's own buffer and whose source
//! identity is the IPC constant — so a client that already reads images reads
//! this one with no change at all — and what it does **not** get is a socket:
//! the reply carries no channel status, no receive endpoint counter is
//! allocated, and nothing is bound.
//!
//! That last part is the whole of what this test is for. A spy that quietly
//! became a subscriber would still deliver every message, and none of the
//! assertions about *what arrived* would notice. So the counters are asserted
//! too: the spy's channel appears in a `sub-pos` label and in no `rcv-channel`
//! one, while the ordinary subscription beside it appears in both.
//!
//! Both orders are covered, because both are real: a spy added to a stream
//! already being published, and a spy waiting for a publication that has not
//! been created yet. They run the two halves of the match — the scan a spy
//! makes and the scan a new publication makes — and a build that had only one
//! of them would pass one of these tests and fail the other.
//!
//! Everything here is our own, so it needs no reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream every test here publishes on.
const STREAM_ID: i32 = 1001;

/// How many messages each test sends. A 64 KiB term holds 62 of these, so this
/// crosses three term boundaries — which is where a reader that is a *position*
/// rather than a socket has to keep up.
const MESSAGES: i64 = 200;

/// One kilobyte of payload, plus a 32-byte header, aligned to 1056.
const LENGTH: usize = 1024;

/// How long the whole exchange may take before the test is a failure.
const DEADLINE: Duration = Duration::from_secs(30);

/// A UDP port to name in the channel.
///
/// Derived from the process id, as the other UDP tests do, so that two tests
/// running at once do not choose the same one.
fn free_port(offset: u16) -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 20_000 + (std::process::id() as u16 % 20_000);

    base.saturating_add(offset)
}

/// The message carrying number `index`, so a message delivered twice or out of
/// order is a failure rather than a count that happens to add up.
fn numbered(index: i64) -> Vec<u8> {
    let mut payload = vec![0xA5_u8; LENGTH];
    payload[..8].copy_from_slice(&index.to_le_bytes());

    payload
}

/// Read everything a subscription has, checking the numbers as they come.
fn drain(client: &mut Client, subscription: i64, received: &mut i64) {
    client.poll_subscription(subscription, 10, |message| {
        assert_eq!(
            numbered(*received),
            message.payload,
            "message {received} arrived out of order or was delivered twice"
        );
        *received += 1;
    });
}

/// Offer until the window permits it, poll both readers, and stop when both
/// have everything.
///
/// Both readers have to be polled, and that is not tidiness: the publication's
/// limit is taken over the positions its readers report, so a reader this test
/// forgot to advance would hold the producer back and the *other* reader would
/// never see the last message.
fn publish_and_read(client: &mut Client, publication: i64, readers: &[i64], what: &str) {
    let deadline = Instant::now() + DEADLINE;
    let mut offered = 0_i64;
    let mut received: Vec<i64> = vec![0; readers.len()];

    while received.iter().any(|count| *count < MESSAGES) {
        assert!(
            Instant::now() < deadline,
            "{what}: {received:?} of {MESSAGES} after {DEADLINE:?}; {offered} were offered"
        );

        while offered < MESSAGES {
            if let Some(Appended::Ok { .. }) = client.offer(publication, &numbered(offered)) {
                offered += 1;
            } else {
                break;
            }
        }

        client.poll();

        for (index, subscription) in readers.iter().enumerate() {
            drain(client, *subscription, &mut received[index]);
        }
    }

    assert_eq!(MESSAGES, offered, "{what}: every message was offered");
}

/// The label of every counter whose name is `name`, for the assertions below.
fn labels(cnc: &deepmsg_cnc::CncFile, name: &str) -> Vec<String> {
    let Some(counters) = cnc.counters() else {
        panic!("the counter regions could not be read");
    };

    let mut found = Vec::new();
    counters.for_each(|descriptor| {
        let label = descriptor.label.clone();

        if label.starts_with(&format!("{name}: ")) {
            found.push(label);
        }
    });

    found
}

/// The value of the first counter whose name is `name`, while the driver runs.
fn counter_value(cnc: &deepmsg_cnc::CncFile, name: &str) -> Option<i64> {
    let counters = cnc.counters()?;
    let mut value = None;

    counters.for_each(|descriptor| {
        if value.is_none() && descriptor.label.starts_with(&format!("{name}: ")) {
            value = Some(descriptor.value);
        }
    });

    value
}

/// Offer until one is taken or the deadline passes, answering whether any was.
///
/// Only the **refused** answer is usable: a caller told `true` has a message
/// in the stream that it did not count, so this is asked only where the answer
/// is expected to be `false`, and only about a publication that has not been
/// read from.
fn an_offer_is_taken(client: &mut Client, publication: i64, within: Duration) -> bool {
    let deadline = Instant::now() + within;

    loop {
        if let Some(Appended::Ok { .. }) = client.offer(publication, &numbered(0)) {
            return true;
        }

        if Instant::now() >= deadline {
            return false;
        }

        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Wait for the publication's two limits to stand in a particular relation.
///
/// `pub-lmt` is where a producer may write to and `snd-pos` is how far the
/// stream has been *sent* — which, for a publication whose readers are all
/// local, is as far as they have read. The relation between them is the whole
/// of what "this publication counts its spies" means to a producer, and it is
/// the one thing the counters can be asked while the driver runs.
fn await_limits(cnc: &deepmsg_cnc::CncFile, expected: impl Fn(i64, i64) -> bool, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        let limit = counter_value(cnc, "pub-lmt").unwrap_or(0);
        let sent = counter_value(cnc, "snd-pos").unwrap_or(0);

        if expected(limit, sent) {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "{what}: `pub-lmt` is {limit} and `snd-pos` is {sent}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// What the spy is, as opposed to what it read.
///
/// Three claims, and each one is a way of being a socket:
///
/// * no **channel status** on the client's own view of the subscription — the
///   reply carried `NOT_ALLOCATED`, which the client reads as "there is no
///   socket to report on";
/// * no `rcv-channel` counter naming the spy channel, when the ordinary
///   subscription beside it has one;
/// * a `sub-pos` counter naming it, because a spy *is* a reader and that
///   counter is what makes it one.
fn assert_the_spy_has_no_socket(
    cnc: &deepmsg_cnc::CncFile,
    client: &Client,
    spy: i64,
    channel: &str,
) {
    let subscription = client.subscription(spy).expect("the subscription");
    assert!(
        subscription.channel_status_indicator_id().is_none(),
        "a spy has no socket, so it has no channel status"
    );

    let spy_uri = format!("aeron-spy:{channel}");

    assert!(
        labels(cnc, "rcv-channel")
            .iter()
            .all(|label| !label.contains(&spy_uri)),
        "a spy must not have a receive endpoint"
    );
    assert!(
        labels(cnc, "sub-pos")
            .iter()
            .any(|label| label.contains(&spy_uri)),
        "the spy is a reader, and its position counter says so"
    );
}

/// Poll until a subscription holds `expected` images, or give up.
fn await_images(client: &mut Client, subscription: i64, expected: usize, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);

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

#[test]
fn a_spy_can_be_added_to_a_subscription_as_a_source() {
    let Some(mut own) = OwnDriver::start("spy-as-a-source") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let published_channel = format!("aeron:udp?endpoint=127.0.0.1:{}", free_port(3));

    let publication = client
        .add_publication(&published_channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication on the channel");

    // The third way a spy link is made: a destination on a subscription. It
    // has to be a `control-mode=manual` channel, which is the only kind a
    // source may be added to.
    let mds_channel = format!(
        "aeron:udp?control=127.0.0.1:{}|control-mode=manual",
        free_port(4)
    );
    let mds = client
        .add_subscription(&mds_channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a multi-destination subscription");

    await_images(&mut client, mds, 0, "it starts with no source");

    // The spy is a *source*, and the answer is an acknowledgement of the
    // destination rather than a subscription — the subscription already exists.
    let destination = client
        .add_rcv_destination(
            mds,
            &format!("aeron-spy:{published_channel}"),
            DEFAULT_TIMEOUT,
        )
        .expect("a spy is served as a receive destination");

    assert!(destination > 0, "the destination has a registration id");

    // And the subscription is given the publication's own buffer, under the
    // *subscription's* registration id — which is what makes the pair one
    // thing rather than two.
    await_images(
        &mut client,
        mds,
        1,
        "the spy gives the subscription an image",
    );

    let images = client.subscription(mds).expect("the subscription").images();
    assert_eq!(
        publication,
        images[0].registration_id(),
        "the image is the publication's log buffer, not something built from datagrams"
    );

    // Taking the source out again announces the image it was holding, which is
    // the one place a spy removal differs from a subscription removal.
    client
        .remove_rcv_destination(
            mds,
            &format!("aeron-spy:{published_channel}"),
            DEFAULT_TIMEOUT,
        )
        .expect("the source comes back out");

    await_images(&mut client, mds, 0, "the image goes with the source");

    // And the subscription itself is still there: only the source was removed.
    assert!(client.subscription(mds).is_some());

    let _ = own.stop();
}

#[test]
fn a_spy_reads_the_publication_it_names() {
    let Some(mut own) = OwnDriver::start("spy-reads-publication") else {
        driver::announce_own_skip();
        return;
    };

    let cnc = own
        .await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{}|term-length=64k",
        free_port(1)
    );

    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication on the channel");

    // An ordinary subscriber, and it is not scenery: a unicast publication with
    // no receiver has nowhere to send and a limit of zero, so without this the
    // producer could not publish at all and the spy would have nothing to read.
    let subscriber = client
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription on the channel");

    assert!(
        client
            .subscription(subscriber)
            .expect("the subscription")
            .channel_status_indicator_id()
            .is_some(),
        "an ordinary subscription has a socket, and a channel status with it"
    );

    // The spy, on the same channel — and it is added *after* the publication,
    // so this is the scan the spy itself makes.
    let spy = client
        .add_subscription(&format!("aeron-spy:{channel}"), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a spy subscription on the channel");

    publish_and_read(
        &mut client,
        publication,
        &[subscriber, spy],
        "a spy beside a subscriber",
    );

    assert_the_spy_has_no_socket(&cnc, &client, spy, &channel);

    let _ = own.stop();
}

#[test]
fn a_spy_that_arrives_first_is_given_the_publication_when_it_appears() {
    let Some(mut own) = OwnDriver::start("spy-waits-for-publication") else {
        driver::announce_own_skip();
        return;
    };

    let cnc = own
        .await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{}|term-length=64k",
        free_port(2)
    );

    // The spy first, on a stream nobody publishes: the link it does not make
    // here is the one the publication's own scan makes below.
    let spy = client
        .add_subscription(&format!("aeron-spy:{channel}"), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a spy subscription on the channel");

    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication on the channel");

    let subscriber = client
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription on the channel");

    publish_and_read(
        &mut client,
        publication,
        &[subscriber, spy],
        "a spy that was already waiting",
    );

    assert_the_spy_has_no_socket(&cnc, &client, spy, &channel);

    let _ = own.stop();
}

/// A13-d: whether a publication **counts** the spy that reads it.
///
/// The two halves are the same arrangement with one setting between them. A
/// unicast publication with no receiver has a limit of `snd-pos`, and `snd-pos`
/// only moves when a receiver's status message opens the window — so a stream
/// whose only reader is a spy is **stalled**, and the producer cannot publish
/// the very message the spy is waiting to read. `aeron.spies.simulate.connection`
/// is what says the spy stands in for the wire.
///
/// What a client sees of that is the log buffer's `is_connected` byte: an
/// `offer` against it comes back `NotConnected` until the publication has a
/// reader, and a spy is one exactly when the setting says so. So the two tests
/// below differ in one flag and in the answer to "does this offer work at all",
/// and both of them read the same 200 messages through the same spy when it
/// does.
#[test]
fn a_spy_counts_as_a_connection_when_the_setting_says_so() {
    let Some(mut own) =
        OwnDriver::start_with("spy-ssc-on", &["-Daeron.spies.simulate.connection=true"])
    else {
        driver::announce_own_skip();
        return;
    };

    let cnc = own
        .await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{}|term-length=64k",
        free_port(5)
    );

    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication on the channel");

    // The spy, and deliberately **no** ordinary subscriber: it is the only
    // reader this stream will ever have.
    let spy = client
        .add_subscription(&format!("aeron-spy:{channel}"), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a spy subscription on the channel");

    // No probe offer before this, and the reason is the message itself: what
    // is being read is a *sequence*, so a probe that put one message in the
    // stream and did not count it would make every number afterwards wrong.
    // That the producer may publish at all is what the 200 messages prove.
    publish_and_read(&mut client, publication, &[spy], "a spy as the only reader");

    // And `snd-pos` follows, which is the other half of the fiction: nothing
    // has left the machine, but the stream has got as far as its reader, and
    // the counter that says what was sent says so.
    let deadline = Instant::now() + Duration::from_secs(10);
    let sent = loop {
        let sent = counter_value(&cnc, "snd-pos").unwrap_or(0);

        if sent > 0 {
            break sent;
        }

        assert!(
            Instant::now() < deadline,
            "`snd-pos` never moved: an `ssc` publication with only spies takes it up to them"
        );
        std::thread::sleep(Duration::from_millis(10));
    };

    assert!(
        sent <= MESSAGES * LENGTH as i64,
        "`snd-pos` is where the readers are, and no further: {sent}"
    );

    let _ = own.stop();
}

#[test]
fn a_spy_is_not_a_connection_without_the_setting() {
    let Some(mut own) = OwnDriver::start("spy-ssc-off") else {
        driver::announce_own_skip();
        return;
    };

    let cnc = own
        .await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{}|term-length=64k",
        free_port(6)
    );

    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication on the channel");

    let spy = client
        .add_subscription(&format!("aeron-spy:{channel}"), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a spy subscription on the channel");

    // The link is made — the spy has an image for the publication's buffer —
    // and the publication does not count it, because nobody asked it to.
    assert!(
        !an_offer_is_taken(&mut client, publication, Duration::from_secs(2)),
        "without `ssc` a spy is not a reader, so there is no receiver, no window, and nothing \
         to publish"
    );

    assert_eq!(
        0,
        counter_value(&cnc, "snd-pos").unwrap_or(0),
        "nothing was sent, because nothing could be"
    );
    assert_eq!(
        0,
        counter_value(&cnc, "pub-pos").unwrap_or(0),
        "and nothing was published"
    );

    // The spy still holds an image: what `ssc` decides is not whether the spy
    // is linked, but whether the publication counts the link.
    await_images(&mut client, spy, 1, "the link is made whatever `ssc` says");

    let _ = own.stop();
}

/// The other half of the reader count: a spy that **leaves**.
///
/// A spy added as a source is the one kind that can be taken out again — a
/// subscription removal goes through the same path, but no client can ask for
/// one yet (`Client` has no `remove_subscription`), so this is where the
/// removal is end to end.
///
/// What it pins is that the publication is *told*. A reader left in a
/// publication's set after its counter has been given back is a limit computed
/// from an id something else now owns — and, with `ssc`, a stream that stays
/// connected for a reader that is gone. So the assertion is that the producer
/// **stops**: the source is removed, the last reader with it, and the next
/// offer is refused.
#[test]
fn a_publication_forgets_a_spy_that_is_taken_away() {
    let Some(mut own) = OwnDriver::start_with(
        "spy-ssc-source-removed",
        &["-Daeron.spies.simulate.connection=true"],
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
        free_port(7)
    );

    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication on the channel");

    let mds = client
        .add_subscription(
            &format!(
                "aeron:udp?control=127.0.0.1:{}|control-mode=manual",
                free_port(8)
            ),
            STREAM_ID,
            DEFAULT_TIMEOUT,
        )
        .expect("a multi-destination subscription");

    let source = format!("aeron-spy:{channel}");

    client
        .add_rcv_destination(mds, &source, DEFAULT_TIMEOUT)
        .expect("a spy is served as a receive destination");

    // The spy is the only reader, so this is the `ssc` path again — the
    // publication counts it, and the messages go into the buffer for it.
    publish_and_read(&mut client, publication, &[mds], "a spy added as a source");

    // While the spy is there the producer's window reaches past what has been
    // sent: the limit is the furthest reader plus a term window, and a reader
    // does not have to be on the wire for that to hold.
    await_limits(
        &cnc,
        |limit, sent| limit > sent,
        "a spy with `ssc` opens the producer's window",
    );

    client
        .remove_rcv_destination(mds, &source, DEFAULT_TIMEOUT)
        .expect("the source comes back out");

    // And when the last reader goes the limit collapses onto `snd-pos`, which
    // is what a publication with no readers has. A publication that still
    // counted the spy it was just told to forget would hold the window open.
    await_limits(
        &cnc,
        |limit, sent| limit == sent,
        "the publication forgets the spy it was told to drop",
    );

    let _ = own.stop();
}

/// A14: three spies on one publication, one reading and two not.
///
/// This is the shape P1-4 §13.4 recorded as unreachable — "three subscriptions
/// on one publication, one reading and two not" needs a reader that can hold
/// the producer back *without* being a socket, and a spy is the only thing that
/// is. A laggard is what the tether cycle exists for: a publication whose limit
/// is computed from its slowest reader cannot move at all while one reader has
/// stopped, so the machine puts it aside, and the two ways it can end are the
/// two this test watches.
///
/// The three are the reference's (`aeron_network_publication.c:1120-1236`):
/// a reader that is behind and quiet is told its image has gone; one that is
/// **not** rejoining is then closed, its counter given back, and it is never
/// heard from again; one that **is** rejoining rests instead, and is woken at
/// `snd-pos` — where the stream has got to, which is the only place an image it
/// is handed can be read from.
///
/// The timeouts are compressed to milliseconds. `resting` is much the longest
/// so that the resting window is wide enough to be observed rather than caught
/// in passing.
#[test]
fn a_publication_puts_its_laggards_aside_and_wakes_the_one_that_rejoins() {
    let Some(mut own) = OwnDriver::start_with(
        "spy-untethered",
        &[
            "-Daeron.spies.simulate.connection=true",
            "-Daeron.untethered.window.limit.timeout=200ms",
            "-Daeron.untethered.linger.timeout=200ms",
            "-Daeron.untethered.resting.timeout=2s",
        ],
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
        free_port(9)
    );

    // `tether=false` on all three, and it is not decoration: a **tethered**
    // reader is one that has asked to keep its place whatever it costs, and the
    // machine never puts one aside (`aeron_network_publication.c:1132-1135`).
    // The whole scenario needs readers that can be put aside.
    let spy = format!("aeron-spy:{channel}|tether=false");

    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication on the channel");

    // Three spies on one channel: the one that reads, the one that stops, and
    // the one that stops but says it will be back.
    let reader = client
        .add_subscription(&spy, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a spy");
    let laggard = client
        .add_subscription(&spy, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a spy");
    let rejoiner = client
        .add_subscription(&format!("{spy}|rejoin=true"), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a spy that is rejoining");

    for subscription in [reader, laggard, rejoiner] {
        await_images(&mut client, subscription, 1, "every spy is linked");
    }

    // Only the first is polled. The other two sit at the position they linked
    // at, which is what makes them laggards — and what holds the producer back
    // until the machine puts them aside, so a run that reads all the messages
    // is itself evidence that the machine ran.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut offered = 0_i64;
    let mut received = 0_i64;

    while received < MESSAGES {
        assert!(
            Instant::now() < deadline,
            "the reader got {received} of {MESSAGES} with {offered} offered — a laggard that is \
             never put aside holds the producer back for ever"
        );

        while offered < MESSAGES {
            if let Some(Appended::Ok { .. }) = client.offer(publication, &numbered(offered)) {
                offered += 1;
            } else {
                break;
            }
        }

        client.poll();
        drain(&mut client, reader, &mut received);
    }

    // The one that was not coming back: told its image has gone, and that is
    // the end of it. It cannot read again, so the image never returns.
    await_images(
        &mut client,
        laggard,
        0,
        "the laggard is told its image has gone",
    );
    await_images(
        &mut client,
        rejoiner,
        0,
        "and so is the one that is rejoining",
    );

    // And the one that was: woken where the stream is — which is the assertion
    // that it went all the way round. Its image was linked at position zero and
    // it has read nothing since, so a position that is not zero can only have
    // come from the machine seeding it.
    await_images(&mut client, rejoiner, 1, "the rejoining spy is woken");

    let sent = counter_value(&cnc, "snd-pos").unwrap_or(0);
    assert!(
        sent > 0,
        "the stream has to have moved for the wake to mean anything"
    );
    assert_eq!(
        sent,
        client
            .subscription(rejoiner)
            .expect("the subscription")
            .images()[0]
            .position(),
        "a woken reader starts where the stream is, not where it stalled"
    );

    // And the reader that kept up was never put aside: it still holds its
    // image, and it read everything.
    assert_eq!(
        1,
        client
            .subscription(reader)
            .expect("the subscription")
            .images()
            .len()
    );

    let _ = own.stop();
}
