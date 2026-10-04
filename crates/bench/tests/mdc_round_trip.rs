//! The multi-destination echo, which is the reference's `EchoTest.multipleDestinations`.
//!
//! One client, two nodes, and three drivers: the client's, and one per node.
//! That is the reference's topology and not a simplification of it — each of its
//! extra `EchoNode`s is given a driver of its own
//! (`EchoTest.java:162-168`) — and it is what makes the client's publication a
//! *multi-destination* one rather than a publication with two subscribers.
//!
//! The channels are the reference's: the client publishes on a control channel
//! whose destinations are discovered as the nodes announce themselves, and the
//! nodes publish back on one endpoint the client is subscribed to. Every message
//! names the receiver it is for, and a node answers only its own, so a batch of
//! nineteen to two receivers earns nineteen replies and not thirty-eight.
//!
//! What this is really asking is whether a publication can fan out at all. If it
//! cannot, the run reports back pressure — every message that gets no answer is
//! one the rig waits for — and the count comes up short.

use std::sync::atomic::Ordering;
use std::time::Duration;

use deepmsg_bench::loadtest::config::{
    Builder, Configuration, IdleStrategy, TimeUnit, Transceiver,
};
use deepmsg_bench::loadtest::result::Status;
use deepmsg_bench::loadtest::rig::LoadTestRig;
use deepmsg_bench::loadtest::transport::echo::EchoTransceiver;
use deepmsg_bench::loadtest::transport::node::EchoNode;
use deepmsg_bench::loadtest::transport::util::ChannelSettings;
use deepmsg_tests::driver::{self, OwnDriver};

/// The reference's two destinations.
const NUM_DESTINATIONS: i32 = 2;

/// Where the nodes publish back, which the client subscribes to.
const NODES_PUBLISH_CHANNEL: &str = "aeron:udp?endpoint=localhost:20202";

/// Where the client publishes, with the destinations discovered rather than
/// named — `control-mode=dynamic`, group semantics, and min flow control tagged
/// with the group. The reference's own string, character for character
/// (`EchoTest.java:130-131`).
const CLIENT_PUBLISH_CHANNEL: &str =
    "aeron:udp?control=localhost:10101|control-mode=dynamic|fc=min,g:/2|group=true|term-length=64k";

/// The ports are the reference's, which is why this is the only test in its
/// binary: two of these running at once would share them.
const DESTINATION_STREAM: i32 = 77777;
const SOURCE_STREAM: i32 = 55555;

fn settings(directory: &std::path::Path, receiver_index: i32) -> ChannelSettings {
    ChannelSettings {
        directory: directory.to_path_buf(),
        destination_channel: CLIENT_PUBLISH_CHANNEL.to_owned(),
        destination_stream: DESTINATION_STREAM,
        source_channel: NODES_PUBLISH_CHANNEL.to_owned(),
        source_stream: SOURCE_STREAM,
        receiver_count: NUM_DESTINATIONS,
        receiver_index,
        use_try_claim: true,
        fragment_limit: 10,
        connection_timeout: Duration::from_secs(20),
    }
}

/// The reference's own configuration for this test, message for message.
fn configuration(directory: &std::path::Path) -> Configuration {
    Builder::new()
        .warmup_iterations(1)
        .warmup_message_rate(7)
        .iterations(1)
        .message_rate(19)
        .message_length(288)
        .batch_size(1)
        .transceiver(Transceiver::Echo)
        .output_time_unit(TimeUnit::Microseconds)
        .output_directory(directory)
        .output_file_name_prefix("mdc")
        .build()
        .expect("the reference's own configuration is valid")
}

#[test]
fn a_client_two_nodes_and_three_drivers_agree() {
    let Some(mut client_driver) = OwnDriver::start("mdc-client") else {
        driver::announce_own_skip();
        return;
    };

    let Some(mut node_drivers): Option<Vec<OwnDriver>> = (0..NUM_DESTINATIONS)
        .map(|index| OwnDriver::start(&format!("mdc-node-{index}")))
        .collect()
    else {
        driver::announce_own_skip();
        return;
    };

    client_driver
        .await_cnc(Duration::from_secs(10))
        .expect("the client's driver is up");
    for node in &mut node_drivers {
        node.await_cnc(Duration::from_secs(10))
            .expect("a node's driver is up");
    }

    let scratch = std::env::temp_dir().join(format!("deepmsg-bench-mdc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("a scratch directory");

    // One node per destination, each on its own driver, each answering only the
    // messages addressed to it.
    let mut stops = Vec::new();
    let mut threads = Vec::new();

    for (index, node_driver) in node_drivers.iter().enumerate() {
        let mut node = EchoNode::new(
            settings(
                node_driver.aeron_dir(),
                i32::try_from(index).expect("small"),
            ),
            IdleStrategy::BusySpin,
            i32::try_from(index).expect("small"),
        )
        .expect("the node connects to its driver");

        stops.push(node.running());
        threads.push(std::thread::spawn(move || node.run()));
    }

    let channels = settings(client_driver.aeron_dir(), 0);
    let transceiver = EchoTransceiver::new(channels, IdleStrategy::BusySpin, scratch.clone())
        .expect("the client connects to its driver");

    let mut rig = LoadTestRig::new(
        configuration(&scratch),
        transceiver,
        deepmsg_bench::loadtest::recorder::Recorder::new(
            deepmsg_bench::loadtest::result::histogram(),
            deepmsg_bench::loadtest::recorder::checksum(),
            deepmsg_bench::loadtest::transceiver::SystemClock,
        ),
        IdleStrategy::BusySpin,
        deepmsg_bench::loadtest::progress::NullProgressReporter,
        Vec::new(),
    );

    let status = rig.run();

    for stop in &stops {
        stop.store(false, Ordering::Relaxed);
    }
    for thread in threads {
        let _ = thread.join();
    }

    let _ = std::fs::remove_dir_all(&scratch);

    // The reference's own assertion is that the run's output carries no
    // `WARNING:`, and a warning is printed exactly when the three counts do not
    // agree — so this is that assertion, without the string.
    let status = status.expect("the run goes through");
    assert_eq!(
        status,
        Status::Ok,
        "seven warmup messages and nineteen measured ones were sent; every one \
         should have been answered by the node it named"
    );
}
