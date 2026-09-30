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
