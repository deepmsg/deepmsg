//! Our archive node, through our own driver **and our own archive**.
//!
//! The criterion the node's own unit tests cannot make: it forwards what it reads
//! into the channel an archive records, and it answers **nothing**. Both halves need
//! a driver — the publication it forwards into and the subscription it reads are the
//! driver's resources — and the second half needs a channel to be silent on, which is
//! what the source channel is here.
//!
//! The assertion that bites is the **negative** one. A node that answered its client
//! the way an echo node does would publish on the source channel; this test reads
//! that channel and requires nothing to arrive, so a change that quietly made the
//! archive node an echo node — the mistake this type exists to avoid — fails here
//! rather than showing up as a benchmark that measures the wrong round trip.
//!
//! It is not a benchmark and reports no numbers: the grid turns this into numbers,
//! and the two arms of that grid are what have to be comparable.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use deepmsg_archive::client::ArchiveContext;
use deepmsg_bench::loadtest::config::IdleStrategy;
use deepmsg_bench::loadtest::transport::archive_node::ArchiveNode;
use deepmsg_bench::loadtest::transport::util::{ChannelSettings, MIN_MESSAGE_LENGTH};
use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::archive::{
    self, CONTROL_CHANNEL, CONTROL_STREAM_ID, RESPONSE_CHANNEL, RESPONSE_STREAM_ID,
};
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// Three channels in shared memory, and the two that carry the measurement are the
/// two the node is between: what a client writes, and what an archive records.
const DESTINATION_CHANNEL: &str = "aeron:ipc?term-length=64k";
const DESTINATION_STREAM: i32 = 3101;
/// Where an echo node would answer. Nothing may arrive here.
const SOURCE_CHANNEL: &str = "aeron:ipc?term-length=64k";
const SOURCE_STREAM: i32 = 3102;
/// What the archive records, and what this test reads to see the forwarding.
const RECORD_CHANNEL: &str = "aeron:ipc?term-length=64k";
const RECORD_STREAM: i32 = 3103;

/// How many messages to forward before the test is done.
const MESSAGES: i64 = 64;

/// A message of the bench's own minimum: a timestamp, a receiver index and a
/// checksum, so that what is forwarded is what a run would send.
const MESSAGE_LENGTH: usize = MIN_MESSAGE_LENGTH;

/// How long anything here may take.
const DEADLINE: Duration = Duration::from_secs(30);

/// The settings a node runs under here.
fn settings(directory: &std::path::Path) -> ChannelSettings {
    ChannelSettings {
        directory: directory.to_path_buf(),
        destination_channel: DESTINATION_CHANNEL.to_owned(),
        destination_stream: DESTINATION_STREAM,
        source_channel: SOURCE_CHANNEL.to_owned(),
        source_stream: SOURCE_STREAM,
        record_channel: RECORD_CHANNEL.to_owned(),
        record_stream: RECORD_STREAM,
        receiver_count: 1,
        receiver_index: 0,
        use_try_claim: false,
        fragment_limit: 10,
        connection_timeout: DEADLINE,
    }
}

/// The archive a local run reaches: the control channel an archive always opens,
/// and a response channel to be answered on (`tests/src/archive.rs`'s own pair).
fn archive_context() -> ArchiveContext {
    let mut context = ArchiveContext::resolve(&[]);

    context.control_request_channel = Some(CONTROL_CHANNEL.to_owned());
    context.control_request_stream_id = CONTROL_STREAM_ID;
    context.control_response_channel = Some(RESPONSE_CHANNEL.to_owned());
    context.control_response_stream_id = RESPONSE_STREAM_ID;
    context.message_timeout_ns = DEADLINE.as_nanos().try_into().unwrap_or(i64::MAX);

    context
}

/// One message, with the two fields a run's reader looks for.
fn message(index: i64) -> Vec<u8> {
    let mut payload = vec![0_u8; MESSAGE_LENGTH];

    payload[0..8].copy_from_slice(&(index * 1000).to_le_bytes());
    payload[MESSAGE_LENGTH - 8..MESSAGE_LENGTH].copy_from_slice(&index.to_le_bytes());

    payload
}

#[test]
fn the_node_forwards_into_the_recording_and_answers_nothing() {
    let archive_dir = TempDir::new("archive-node-round-trip");

    let Some(mut media_driver) = OwnArchivingMediaDriver::start(
        "archive-node-round-trip",
        archive_dir.path(),
        &archive::PROPERTIES,
    ) else {
        archiving_driver::announce_skip();
        return;
    };

    media_driver
        .await_ready(DEADLINE)
        .expect("the archiving media driver comes up");

    let aeron_dir = media_driver.aeron_dir().to_path_buf();
    let mut client = Client::connect(&aeron_dir).expect("the client connects");

    let mut node = ArchiveNode::new(
        settings(&aeron_dir),
        IdleStrategy::BusySpin,
        &archive_context(),
    )
    .expect("the node connects to the driver and the archive");

    // The node is what a real run has in its own process: it runs until it is told
    // to stop or its client goes away, so it is driven from a thread here.
    let running = node.running();
    let node_thread = std::thread::spawn(move || node.run());

    // What the node reads, and the two channels this end reads: one to see the
    // forwarding, and one that must stay silent.
    let publication = client
        .add_exclusive_publication(DESTINATION_CHANNEL, DESTINATION_STREAM, DEFAULT_TIMEOUT)
        .expect("the client publishes where the node reads");
    let recorded = client
        .add_subscription(RECORD_CHANNEL, RECORD_STREAM, DEFAULT_TIMEOUT)
        .expect("a reader of the recorded channel");
    let answered = client
        .add_subscription(SOURCE_CHANNEL, SOURCE_STREAM, DEFAULT_TIMEOUT)
        .expect("a reader of the channel an echo node would answer on");

    let deadline = Instant::now() + DEADLINE;
    let mut sent = 0;
    let mut forwarded = 0;
    let mut answered_back = 0;

    // Stopping early on the first answer is what makes the negative assertion the
    // one that fires: a node that echoed would answer immediately, and a loop that
    // ran to the deadline first would report the forwarding instead — the effect
    // rather than the fault.
    while forwarded < MESSAGES && 0 == answered_back {
        assert!(
            Instant::now() < deadline,
            "the node forwarded {forwarded} of {MESSAGES} messages, and answered {answered_back} times"
        );

        while sent < MESSAGES {
            match client.offer_exclusive(publication, &message(sent)) {
                Some(Appended::Ok { .. }) => sent += 1,
                Some(Appended::NotConnected | Appended::BackPressured) => break,
                other => panic!("the message could not be published ({other:?})"),
            }
        }

        client.poll();

        forwarded += read(&mut client, recorded);
        answered_back += read(&mut client, answered);

        std::thread::sleep(Duration::from_millis(1));
    }

    // ★ Nothing came back. A node that echoed would have answered on the source
    // channel, and this is where that would be seen.
    assert_eq!(
        0, answered_back,
        "the node answered nothing: the reply a client reads is the archive's replay, \
         not the node's"
    );

    assert_eq!(
        MESSAGES, forwarded,
        "and everything the client sent went into the recorded channel"
    );

    running.store(false, Ordering::Relaxed);
    let _ = node_thread.join();

    // And the archive the node asked to record it is now recording: the channel the
    // node named, with the session id it named, is in the catalog. This is what says
    // the messages above went into a **recording** rather than into a channel.
    let mut recordings = 0;
    let found = archive_client(&mut client)
        .list_recordings_for_uri(
            &mut client,
            0,
            5,
            RECORD_CHANNEL,
            RECORD_STREAM,
            &mut |_descriptor| recordings += 1,
        )
        .expect("the archive lists the recording the node asked for");

    assert!(
        found > 0 && recordings > 0,
        "the node's startRecording named a channel the archive made a recording of"
    );

    let _ = media_driver.stop();
}

/// An archive client of this test's own, for the questions the node does not answer.
fn archive_client(client: &mut Client) -> deepmsg_archive::client::archive::Archive {
    deepmsg_archive::client::archive::Archive::connect(
        &archive_context(),
        client,
        &mut deepmsg_archive::client::NoCredentials,
        deepmsg_archive::client::archive::Handlers::default(),
    )
    .expect("the archive is reachable")
}

/// Read every fragment a subscription has, and say how many there were.
///
/// The count comes from the client's own scan, which is the only thing that moves a
/// reader through a term: a subscription asked about its images would answer the same
/// number every turn.
fn read(client: &mut Client, subscription: i64) -> i64 {
    let mut fragments = 0;

    client.poll_subscription_fragments(subscription, 10, |_fragment| fragments += 1);

    fragments
}
