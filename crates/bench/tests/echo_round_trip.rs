//! Our echo transceiver against our node, through our own driver.
//!
//! The two halves of an echo measurement, end to end, with no reference in the
//! picture: a client that publishes and reads replies, a node that answers, and
//! the round trips that come back. What it says is that the two agree about the
//! message — where the timestamp is, where the receiver index is, whose message
//! a message is — because a disagreement about any of those shows up here as
//! nothing arriving at all.
//!
//! It is not a benchmark and reports no numbers: the grid is what turns this
//! into numbers, and it needs a machine nobody else is using.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use deepmsg_bench::loadtest::config::{
    Builder, Configuration, IdleStrategy, TimeUnit, Transceiver,
};
use deepmsg_bench::loadtest::recorder::{self, Recorder};
use deepmsg_bench::loadtest::result;
use deepmsg_bench::loadtest::transceiver::{Clock, MessageTransceiver, SystemClock};
use deepmsg_bench::loadtest::transport::echo::{EchoTransceiver, PollGate};
use deepmsg_bench::loadtest::transport::node::EchoNode;
use deepmsg_bench::loadtest::transport::util::{ChannelSettings, MIN_MESSAGE_LENGTH};
use deepmsg_tests::driver::{self, OwnDriver};

/// Both channels are shared memory: this is a test of the two halves agreeing,
/// not of anything a network does to them.
const CLIENT_TO_NODE: &str = "aeron:ipc?term-length=64k";
const NODE_TO_CLIENT: &str = "aeron:ipc?term-length=64k";

/// Two streams, so that the client does not hear its own publication.
const CLIENT_TO_NODE_STREAM: i32 = 3001;
const NODE_TO_CLIENT_STREAM: i32 = 3002;

/// How many round trips to make before the test is done.
const MESSAGES: i64 = 500;

/// How long a message is: the bench's own minimum, which is a timestamp, a
/// receiver index and a checksum and nothing else.
const MESSAGE_LENGTH: i32 = MIN_MESSAGE_LENGTH as i32;

/// A message longer than one frame can carry, so that it reaches the node as
/// several fragments and the node has to put it back together.
///
/// `AERON_IPC_MTU_LENGTH_DEFAULT` is 1408 (`aeron_driver_context.c`), so this
/// splits into three frames on both hops. The shape the bench itself never
/// sends — every rate it runs is one frame — and the branch of the node's echo
/// loop that only a message like this one reaches.
const FRAGMENTED_MESSAGE_LENGTH: i32 = 4_000;

/// `AERON_IPC_MTU_LENGTH_DEFAULT` (`aeron_driver_context.c`), which is what a
/// channel that names no `mtu` gets — the same constant
/// `tests/integration/image_lifecycle.rs` checks a fragmented message against.
const IPC_MTU: i32 = 1408;

/// The whole point of the message above, as an invariant rather than a comment:
/// a message that fits one frame would make that test pass without reaching the
/// branch it exists for. Checked here so that lowering the constant is a
/// compile error and not a test that quietly stopped testing anything.
const _: () = assert!(FRAGMENTED_MESSAGE_LENGTH > IPC_MTU);

/// How long any one round trip may take before the test is a failure rather
/// than a slow machine.
const DEADLINE: Duration = Duration::from_secs(30);

/// The settings a round trip runs under, with the sender's kind chosen.
///
/// `use_try_claim` is the bench's `aeron.use.try.claim`, and the two kinds of
/// send are not interchangeable for every message: a **claim** is one frame by
/// construction — it answers how long the frame it took is, not where a message
/// of several frames would end — so a message wider than one frame can only be
/// offered. The reference refuses the same way (`tryClaim`'s length is checked
/// against the maximum *payload*, where `offer` fragments).
fn settings(directory: &std::path::Path, use_try_claim: bool) -> ChannelSettings {
    ChannelSettings {
        directory: directory.to_path_buf(),
        destination_channel: CLIENT_TO_NODE.to_owned(),
        destination_stream: CLIENT_TO_NODE_STREAM,
        source_channel: NODE_TO_CLIENT.to_owned(),
        source_stream: NODE_TO_CLIENT_STREAM,
        receiver_count: 1,
        receiver_index: 0,
        // The default a run uses, so this exercises the claim path: the client
        // claims, writes its three fields where the message will lie, publishes.
        use_try_claim,
        fragment_limit: 10,
        connection_timeout: Duration::from_secs(20),
    }
}

fn configuration(directory: &std::path::Path, message_length: i32) -> Configuration {
    Builder::new()
        .warmup_iterations(0)
        .iterations(1)
        .message_rate(1000)
        .message_length(message_length)
        .transceiver(Transceiver::InMemory)
        .output_time_unit(TimeUnit::Nanoseconds)
        .output_directory(directory)
        .output_file_name_prefix("echo")
        .build()
        .expect("the test's own configuration is valid")
}

#[test]
fn a_message_goes_to_the_node_and_comes_back() {
    // Claimed, as every run in the grid claims.
    let scratch = round_trip("echo-round-trip", MESSAGE_LENGTH, true);

    let _ = std::fs::remove_dir_all(&scratch);
}

/// A message the node has to put back together.
///
/// The node reads **fragments** and echoes messages, so a message that arrived
/// in several frames is reassembled on the node's own assembler before it is
/// sent back — the branch of the echo loop that no run in the grid reaches,
/// because every rate the bench runs is one frame. A node that forwarded each
/// fragment as a message of its own would answer a 4,000-byte message with
/// three replies, and this is where that shows.
#[test]
fn a_message_wider_than_one_frame_comes_back_whole() {
    // Offered and not claimed: a claim is one frame and this message is three.
    let scratch = round_trip("echo-round-trip-fragmented", FRAGMENTED_MESSAGE_LENGTH, false);

    let _ = std::fs::remove_dir_all(&scratch);
}

fn round_trip(test_name: &str, message_length: i32, use_try_claim: bool) -> PathBuf {
    let Some(mut own) = OwnDriver::start(test_name) else {
        driver::announce_own_skip();
        return PathBuf::new();
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    // A directory of its own, per test: two tests run at once, and both sharing
    // one is one of them removing the other's directory between the node's first
    // write and its last.
    let scratch = std::env::temp_dir().join(format!(
        "deepmsg-bench-echo-{}-{test_name}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("a scratch directory");
    let logs = scratch.join("logs");
    std::fs::create_dir_all(&logs).expect("the logs directory the node writes into");

    let mut node = EchoNode::new(
        settings(own.aeron_dir(), use_try_claim),
        IdleStrategy::BusySpin,
        0,
    )
    .expect("the node connects to the driver");
    let running = node.running();
    let node_thread = std::thread::spawn(move || node.run());

    let mut transceiver = EchoTransceiver::new(
        settings(own.aeron_dir(), use_try_claim),
        IdleStrategy::BusySpin,
        scratch.clone(),
        // The duty cycle on every `receive()`: this test is about the round trip
        // and not about the poll gate.
        PollGate::Every,
    )
    .expect("the client connects to the driver");
    // The clock is named because the trait's `init` says nothing about it and
    // nothing else has been called yet to infer it from.
    let configuration = configuration(&scratch, message_length);
    MessageTransceiver::<SystemClock>::init(&mut transceiver, &configuration)
        .expect("the client finds the node");

    let checksum = recorder::checksum();
    let mut recorder = Recorder::new(result::histogram(), checksum, SystemClock);
    // Read back off the configuration and not taken from the argument: a test
    // that sent a different number of bytes than it configured would be
    // checking its own arithmetic rather than the round trip.
    let message_length =
        usize::try_from(configuration.message_length()).expect("a positive message length");
    let deadline = Instant::now() + DEADLINE;

    let mut sent = 0;
    while (sent < MESSAGES || recorder.received_messages() < MESSAGES) && Instant::now() < deadline
    {
        if sent < MESSAGES {
            // The timestamp is when the message is *meant* to go out — the
            // reference's meaning, and what the round trip below is measured
            // from. Here it is simply now, read off the same clock the recorder
            // times the reply against, or the two would be an origin apart.
            let timestamp = SystemClock.nano_time();
            sent += MessageTransceiver::<SystemClock>::send(
                &mut transceiver,
                1,
                message_length,
                timestamp,
                checksum,
                &mut recorder,
            ) as i64;
        }

        MessageTransceiver::<SystemClock>::receive(&mut transceiver, &mut recorder);
    }

    assert_eq!(sent, MESSAGES, "every message should have gone out");
    assert_eq!(
        recorder.received_messages(),
        MESSAGES,
        "and every one should have come back"
    );
    assert_eq!(
        recorder.histogram().len(),
        u64::try_from(MESSAGES).expect("positive")
    );

    MessageTransceiver::<SystemClock>::destroy(&mut transceiver).expect("the client closes");
    running.store(false, std::sync::atomic::Ordering::Relaxed);

    let _ = node_thread.join();

    scratch
}
