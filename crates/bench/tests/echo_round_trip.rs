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

/// How long any one round trip may take before the test is a failure rather
/// than a slow machine.
const DEADLINE: Duration = Duration::from_secs(30);

fn settings(directory: &std::path::Path) -> ChannelSettings {
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
        use_try_claim: true,
        fragment_limit: 10,
        connection_timeout: Duration::from_secs(20),
    }
}

fn configuration(directory: &std::path::Path) -> Configuration {
    Builder::new()
        .warmup_iterations(0)
        .iterations(1)
        .message_rate(1000)
        .message_length(i32::try_from(MIN_MESSAGE_LENGTH).expect("a message length"))
        .transceiver(Transceiver::InMemory)
        .output_time_unit(TimeUnit::Nanoseconds)
        .output_directory(directory)
        .output_file_name_prefix("echo")
        .build()
        .expect("the test's own configuration is valid")
}

#[test]
fn a_message_goes_to_the_node_and_comes_back() {
    let Some(mut own) = OwnDriver::start("echo-round-trip") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let scratch = std::env::temp_dir().join(format!("deepmsg-bench-echo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("a scratch directory");

    let mut node = EchoNode::new(settings(own.aeron_dir()), IdleStrategy::BusySpin, 0)
        .expect("the node connects to the driver");
    let running = node.running();
    let node_thread = std::thread::spawn(move || node.run());

    let mut transceiver = EchoTransceiver::new(
        settings(own.aeron_dir()),
        IdleStrategy::BusySpin,
        scratch.clone(),
        // The duty cycle on every `receive()`: this test is about the round trip
        // and not about the poll gate.
        PollGate::Every,
    )
    .expect("the client connects to the driver");
    // The clock is named because the trait's `init` says nothing about it and
    // nothing else has been called yet to infer it from.
    MessageTransceiver::<SystemClock>::init(&mut transceiver, &configuration(&scratch))
        .expect("the client finds the node");

    let checksum = recorder::checksum();
    let mut recorder = Recorder::new(result::histogram(), checksum, SystemClock);
    let message_length = MIN_MESSAGE_LENGTH;
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
    let _ = std::fs::remove_dir_all(&scratch);
}
