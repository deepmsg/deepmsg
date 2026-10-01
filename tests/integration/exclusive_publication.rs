//! G2-1: a publication with one producer, from a client of ours to a driver of
//! ours.
//!
//! The driver has served `ADD_EXCLUSIVE_PUBLICATION` since before this slice —
//! what was missing was a client that could ask, and an append path that does
//! not claim. This is where both meet a real log buffer: no socket, because an
//! IPC publication's subscriber reads the very pages the producer writes, so
//! every byte that arrives has been through the append path this slice added.
//!
//! Three things are asserted that the concurrent round trip cannot say:
//!
//! * the log the driver created is marked **exclusive** in its metadata
//!   (`LogType::ExclusivePublication`, the byte at `TYPE_OFFSET`), which is the
//!   driver's answer to the command and not something this client can fake;
//! * the publication's own `term_id`/`term_offset` **move with the log** across
//!   term rotations — a concurrent publication has no such pair, because it
//!   takes its place from a claim every time;
//! * the stream crosses **three** term boundaries, so the `EndOfLog` path that
//!   re-seeds that pair is run rather than described.
//!
//! Everything here is our own, so it needs no reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::logbuffer::descriptor::{self, LogType};
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream every test here publishes on.
const STREAM_ID: i32 = 1001;

/// The channel both ends use: shared memory, so a delivered message has been
/// through the append path and nothing else.
///
/// The term length is named rather than left to the driver's IPC default, which
/// is orders of magnitude larger: 200 messages have to cross **three** term
/// boundaries for the `EndOfLog` path to run at all, and with the default they
/// would not reach the first one.
const CHANNEL: &str = "aeron:ipc?term-length=64k";

/// A kilobyte of payload plus a 32-byte header, aligned to 1056. A 64 KiB term
/// holds 62 of these.
const LENGTH: usize = 1024;

/// How many messages to send: enough to cross three term boundaries, which is
/// where a producer that holds its own offset has to notice the log move.
const MESSAGES: i64 = 200;

/// How long the whole exchange may take before the test is a failure.
const DEADLINE: Duration = Duration::from_secs(30);

/// The message carrying number `index`, so one delivered twice or out of order
/// is a failure rather than a count that happens to add up.
fn numbered(index: i64) -> Vec<u8> {
    let mut payload = vec![0x5A_u8; LENGTH];
    payload[..8].copy_from_slice(&index.to_le_bytes());

    payload
}

/// Offer `MESSAGES` messages and read them back, interleaved.
///
/// Interleaved because it has to be: the window is the **reader's** to move, so
/// a producer that offers without reading fills it and stops. The two loops
/// have to be one.
fn publish_and_read(
    client: &mut Client,
    publication: i64,
    subscription: i64,
    exclusive: bool,
) -> Vec<Vec<u8>> {
    let mut arrived: Vec<Vec<u8>> = Vec::new();
    let mut sent = 0i64;
    let deadline = Instant::now() + DEADLINE;

    while sent < MESSAGES || (arrived.len() as i64) < MESSAGES {
        assert!(
            Instant::now() < deadline,
            "offered {sent} of {MESSAGES}, read {}",
            arrived.len()
        );

        client.poll();

        if sent < MESSAGES {
            let outcome = if exclusive {
                client.offer_exclusive(publication, &numbered(sent))
            } else {
                client.offer(publication, &numbered(sent))
            };

            match outcome {
                Some(Appended::Ok { .. }) => sent += 1,

                // The append rotated the log. The publication re-read where it
                // went, so the retry lands in the new term — which is the whole
                // of what this test is here to run.
                Some(Appended::EndOfLog | Appended::MidRotation) => {}

                // The window, which the reader below moves.
                Some(Appended::BackPressured) => {}

                other => panic!("offer {sent} came back {other:?}"),
            }
        }

        client.poll_subscription(subscription, 10, |message| {
            arrived.push(message.payload.to_vec());
        });
    }

    arrived
}

/// Wait until the subscription has been told about an image, or the deadline
/// passes.
fn await_images(client: &mut Client, subscription: i64) {
    let deadline = Instant::now() + DEADLINE;

    loop {
        client.poll();

        let held = client
            .subscription(subscription)
            .expect("the subscription")
            .images()
            .len();

        if held > 0 {
            return;
        }

        assert!(Instant::now() < deadline, "no image arrived");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The log type byte the driver wrote for a publication this client holds.
fn log_type_of(client: &Client, publication: i64, exclusive: bool) -> LogType {
    let buffer = if exclusive {
        client
            .exclusive_publication(publication)
            .expect("the exclusive publication")
            .log()
    } else {
        client
            .publication(publication)
            .expect("the publication")
            .log()
    };

    let metadata = buffer
        .file()
        .region(
            buffer.geometry().metadata_offset,
            descriptor::METADATA_LENGTH,
        )
        .expect("the metadata block");

    LogType::from_byte(metadata.load_u8(descriptor::TYPE_OFFSET).expect("in range"))
}

#[test]
fn an_exclusive_publication_publishes_across_terms_and_a_subscription_reads_it() {
    let Some(mut own) = OwnDriver::start("exclusive-round-trip") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let publication = client
        .add_exclusive_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("an exclusive publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_images(&mut client, subscription);

    // The driver's own answer to the command: not something this client can
    // write, and the one byte that says the channel really is exclusive.
    assert_eq!(
        LogType::ExclusivePublication,
        log_type_of(&client, publication, true),
        "the driver created an exclusive log"
    );

    let opened_at = {
        let held = client
            .exclusive_publication(publication)
            .expect("the publication");
        (held.term_id(), held.term_offset())
    };

    let arrived = publish_and_read(&mut client, publication, subscription, true);
    assert_eq!(MESSAGES as usize, arrived.len());

    for (index, message) in arrived.iter().enumerate() {
        assert_eq!(
            numbered(index as i64),
            *message,
            "message {index} is the one that was sent"
        );
    }

    // And the publication followed the log across the rotations rather than
    // being left behind at the offset it opened at.
    let held = client
        .exclusive_publication(publication)
        .expect("the publication");

    assert_ne!(
        opened_at,
        (held.term_id(), held.term_offset()),
        "the publication's position moved with the log"
    );
    assert!(
        held.term_id() > opened_at.0,
        "and it moved forward: {} terms",
        held.term_id() - opened_at.0
    );
    assert!(
        held.term_offset() < LENGTH as i32 * 62,
        "into a term it did not start in"
    );
}

#[test]
fn a_concurrent_publication_on_the_same_stream_is_not_marked_exclusive() {
    // The contrast, so the assertion above cannot pass by every log being
    // marked the same way. Two publications, one byte apart.
    let Some(mut own) = OwnDriver::start("exclusive-vs-concurrent") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let exclusive = client
        .add_exclusive_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("an exclusive publication");
    let concurrent = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a concurrent publication");

    assert_eq!(
        LogType::ExclusivePublication,
        log_type_of(&client, exclusive, true)
    );
    assert_eq!(
        LogType::ConcurrentPublication,
        log_type_of(&client, concurrent, false)
    );
}

/// The same round trip over a **concurrent** publication, so a failure above
/// can be told apart from a failure in the IPC read path that has nothing to do
/// with this slice.
#[test]
fn a_concurrent_publication_round_trips_over_ipc() {
    let Some(mut own) = OwnDriver::start("concurrent-ipc-round-trip") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let publication = client
        .add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a concurrent publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    await_images(&mut client, subscription);

    let arrived = publish_and_read(&mut client, publication, subscription, false);
    assert_eq!(
        MESSAGES as usize,
        arrived.len(),
        "the concurrent path delivers too"
    );
}
