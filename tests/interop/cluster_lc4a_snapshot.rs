//! P3-2b's LC-4a card, re-hosted on P3-2c's fixture: **the reference's
//! consensus module drives our container through a snapshot and a restart**,
//! with the reference's client sending the messages.
//!
//! The two shapes the card's DoD names:
//!
//! * `a_snapshot_survives_a_container_restart` — a Java cluster (driver,
//!   archive and consensus module in one process) and our container running the
//!   auction twin. Bids go through the reference's `AeronCluster`, the SNAPSHOT
//!   toggle turns once, the container is stopped and started again **in the
//!   same directory**, and the recovered state answers the next bid with what
//!   the snapshot held.
//! * `the_twins_snapshot_is_the_java_ones_byte_for_byte` — one consensus
//!   module, **two containers**: the reference's `ClusteredServiceContainer`
//!   running `BasicAuctionClusteredService` (service 0) and ours running the
//!   Rust twin (service 1). One `SNAPSHOT` produces two recordings; their
//!   payload bytes must be equal.
//!
//! Both run in **both modes** (plan §4.5, Q4): the container on a thread of this
//! process and the container as its own binary. A Java container is always the
//! second — a Java object cannot live in this process — so the twin's Java half
//! is the same in both.
//!
//! # Environment
//!
//! `DEEPMSG_REF_AERON_ALL_JAR` and `DEEPMSG_AERON_TESTS` are required; without
//! them `TestCluster::start` answers `None` and each test prints a skip (the
//! workspace's rule for the Java half of the cluster suite).

use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use deepmsg_archive::client::{Archive, ArchiveContext, Handlers, NoCredentials, ReplayParams};
use deepmsg_client::client::Client;
use deepmsg_cluster_common::counters::recovery::RecoveryStateKey;
use deepmsg_cluster_common::counters::{self, CLUSTER_SNAPSHOT_COUNTER_TYPE_ID};
use deepmsg_tests::cluster::{MemberSpec, Options, ProcessMode, TestCluster};

const DEADLINE: Duration = Duration::from_secs(120);
const CLUSTER_ID: i32 = 8;
const REPLAY_CHANNEL: &str = "aeron:ipc";
const REPLAY_STREAM_ID: i32 = 103;
/// A control response stream no container of this cluster uses.
const READER_RESPONSE_STREAM_ID: i32 = 9_000;

static SERIAL: Mutex<()> = Mutex::new(());

/// The probes both scenarios compile.
fn options() -> Options {
    Options {
        cluster_id: CLUSTER_ID,
        probes: vec![
            "ClusterAuctionBidClientProbe.java",
            "ClusterServiceContainerProbe.java",
            "ClusterAuctionServiceProbe.java",
        ],
        ..Options::default()
    }
}

fn start(test_name: &str, members: &[MemberSpec]) -> Option<TestCluster> {
    TestCluster::start(test_name, members, options())
}

/// The recovery state's snapshot recording id for one service.
fn snapshot_recording_id(cluster: &TestCluster, service_id: i32) -> Option<i64> {
    let client = cluster.client();
    let counters = client.counters_reader()?;
    let counter_id = counters::find_recovery_state(&counters, CLUSTER_ID)?;
    let key = counters.key(counter_id)?;
    RecoveryStateKey::decode(&key)
        .ok()?
        .snapshot_recording_id(service_id)
        .ok()
}

/// The consensus module's own snapshot counter (205). It is incremented only
/// after every service ack has arrived **and** the recording log has been
/// forced, so it is the moment after which a restart can recover.
fn snapshot_count(cluster: &TestCluster) -> i64 {
    cluster
        .counter(CLUSTER_SNAPSHOT_COUNTER_TYPE_ID)
        .unwrap_or(0)
}

fn await_snapshot_count(cluster: &TestCluster, expected: i64) {
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        if snapshot_count(cluster) >= expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "the consensus module never counted {expected} snapshot(s)\nnode log:\n{}",
        cluster.node_log(0)
    );
}

fn await_snapshot_recording(cluster: &TestCluster, service_id: i32) -> i64 {
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        if let Some(recording_id) = snapshot_recording_id(cluster, service_id) {
            if recording_id != -1 {
                return recording_id;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "service {service_id} never acked a snapshot recording\nnode log:\n{}",
        cluster.node_log(0)
    );
}

/// `<cluster id> <bids> <customer> <first price> <snapshot?> <hold>`.
fn bid_args(
    bids: usize,
    customer: u64,
    first_price: u64,
    snapshot: bool,
    hold_ms: u64,
) -> Vec<String> {
    vec![
        CLUSTER_ID.to_string(),
        bids.to_string(),
        customer.to_string(),
        first_price.to_string(),
        snapshot.to_string(),
        hold_ms.to_string(),
    ]
}

/// Run the bid probe.
fn bid(
    cluster: &TestCluster,
    bids: usize,
    customer: u64,
    first_price: u64,
    snapshot: bool,
) -> String {
    let mut args = vec![cluster.aeron_dir(0).display().to_string()];
    args.extend(bid_args(bids, customer, first_price, snapshot, 0));
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    cluster.run_probe("ClusterAuctionBidClientProbe", &args)
}

/// The same, on a thread, so its session stays open while another turn takes
/// the snapshot.
fn bid_in_background(
    cluster: &TestCluster,
    bids: usize,
    customer: u64,
    first_price: u64,
    hold_ms: u64,
) -> std::thread::JoinHandle<String> {
    let mut args = vec![cluster.aeron_dir(0).display().to_string()];
    args.extend(bid_args(bids, customer, first_price, false, hold_ms));
    let jar = cluster.jar().to_path_buf();
    let classes = cluster.classes().to_path_buf();

    std::thread::spawn(move || {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        deepmsg_tests::java::run_probe(&jar, &classes, "ClusterAuctionBidClientProbe", &args)
    })
}

/// Replay one recording to its end and answer its payload bytes.
fn read_recording(aeron_dir: &Path, recording_id: i64) -> Vec<u8> {
    let mut client = Client::connect(aeron_dir).expect("a client onto the driver");

    let mut context = ArchiveContext::new();
    context.control_request_channel = Some("aeron:ipc?term-length=64k".to_owned());
    context.control_response_channel = Some("aeron:ipc?term-length=64k".to_owned());
    context.control_request_stream_id = 10;
    context.control_response_stream_id = READER_RESPONSE_STREAM_ID;
    let mut archive = Archive::connect(
        &context,
        &mut client,
        &mut NoCredentials,
        Handlers::default(),
    )
    .expect("the archive connects");

    let subscription_id = archive
        .replay(
            &mut client,
            recording_id,
            REPLAY_CHANNEL,
            REPLAY_STREAM_ID,
            &ReplayParams {
                position: 0,
                // The recording was auto-stopped when the snapshot finished, so
                // a replay to its end carries the end-of-stream marker.
                length: -2,
                ..ReplayParams::default()
            },
        )
        .expect("the recording replays");

    let deadline = Instant::now() + DEADLINE;
    let image_registration_id = loop {
        let image = client
            .subscription(subscription_id)
            .and_then(|subscription| subscription.images().first())
            .map(deepmsg_client::image::Image::registration_id);
        if let Some(registration_id) = image {
            break registration_id;
        }
        assert!(Instant::now() < deadline, "the replay image never arrived");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    };

    let mut bytes = Vec::new();
    loop {
        let image_is_end = client
            .subscription(subscription_id)
            .and_then(|subscription| {
                subscription
                    .images()
                    .iter()
                    .find(|image| image.registration_id() == image_registration_id)
                    .map(deepmsg_client::image::Image::is_end_of_stream)
            })
            .flatten()
            .unwrap_or(false);

        let read = client.poll_image(subscription_id, image_registration_id, 100, |fragment| {
            let length = fragment.payload_length();
            if let Some((buffer, offset, _)) = fragment.payload_region() {
                let mut payload = vec![0_u8; length];
                if buffer.copy_out(offset, &mut payload).is_some() {
                    bytes.extend_from_slice(&payload);
                }
            }
        });

        if image_is_end && read.unwrap_or(0) == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the replay never reached its end"
        );
    }

    bytes
}

/// Ten winning bids, a snapshot, a restart in the same directory, and the
/// recovered auction answers with the state the snapshot held.
fn snapshot_survives_a_restart(test_name: &str, mode: ProcessMode) {
    let Some(mut cluster) = start(
        test_name,
        &[MemberSpec::our_container(0, "basic-auction", mode)],
    ) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };

    cluster.await_active();

    // Ten winning bids from one customer: the state after them is (1, 109).
    let reading = bid(&cluster, 10, 1, 100, true);
    assert!(
        reading.contains("RECEIVED 10"),
        "the bids went through, client said: {reading}\nnode log:\n{}",
        cluster.node_log(0)
    );
    assert!(
        reading.contains("LAST 1 109 true"),
        "the last bid won, client said: {reading}"
    );
    assert!(
        reading.contains("SNAPSHOT_REQUESTED"),
        "the SNAPSHOT toggle turned, client said: {reading}"
    );

    // The module counts the snapshot only after the service's ack and the
    // recording log's force, which is the point a restart can recover from.
    await_snapshot_count(&cluster, 1);

    // Stop the container and the node, and start both again in the same
    // directories. The recovery state counter is allocated during the module's
    // startup from the on-disk `recording.log`; only a restart sees the
    // snapshot taken a moment ago.
    cluster.restart_node(0);

    let recording_id = await_snapshot_recording(&cluster, 0);
    assert_ne!(
        recording_id, -1,
        "the recovery state names the service snapshot"
    );

    // A losing bid is answered with the current state, which must be the state
    // the snapshot held — not the empty auction a fresh container would have.
    let reading = bid(&cluster, 1, 99, 0, false);
    assert!(
        reading.contains("LAST 1 109 false"),
        "the recovered auction answered with the snapshot's state, client said: {reading}\n\
         node log:\n{}",
        cluster.node_log(0)
    );

    cluster.shutdown();
}

/// One consensus module, two containers, one snapshot, equal bytes.
fn twins_agree_byte_for_byte(test_name: &str, mode: ProcessMode) {
    let Some(mut cluster) = start(
        test_name,
        &[
            MemberSpec::java_container(
                0,
                "ClusterAuctionServiceProbe$BasicAuctionClusteredService",
            )
            .with_service_id(0)
            .with_responder(true),
            // Our container does not answer a client in this scenario: service 0
            // is the responder, and two responders would put two copies of the
            // egress on the client's channel.
            MemberSpec::our_container(0, "basic-auction", mode)
                .with_service_id(1)
                .with_responder(false),
        ],
    ) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };

    cluster.await_active();

    // Two clients, one bid each, both **held open**: the snapshot's session
    // table then carries two sessions, which is what makes its order
    // observable (and what a session-order mutation can fail on).
    let first = bid_in_background(&cluster, 1, 1, 100, 8_000);
    let second = bid_in_background(&cluster, 1, 2, 200, 8_000);

    // Let both bids land before the snapshot reads the state.
    std::thread::sleep(Duration::from_secs(3));

    // One SNAPSHOT action reaches both services.
    let reading = bid(&cluster, 0, 0, 0, true);
    assert!(
        reading.contains("SNAPSHOT_REQUESTED"),
        "the SNAPSHOT toggle turned, client said: {reading}"
    );

    // Both acks are in and the module has forced its log.
    await_snapshot_count(&cluster, 1);

    // The two held clients answer their readings before the node goes away.
    for (name, handle) in [("first", first), ("second", second)] {
        let reading = handle.join().expect("the client thread did not panic");
        assert!(
            reading.contains("LAST "),
            "the {name} client answered, client said: {reading}"
        );
    }

    // The recording ids only become readable through a recovery state, which is
    // allocated at module startup — so stop the node (and the containers, which
    // would otherwise hold the services' resources) and start it again over the
    // same archive directory.
    cluster.restart_node(0);

    let java_recording_id = await_snapshot_recording(&cluster, 0);
    let rust_recording_id = await_snapshot_recording(&cluster, 1);

    let java_snapshot = read_recording(cluster.aeron_dir(0), java_recording_id);
    let rust_snapshot = read_recording(cluster.aeron_dir(0), rust_recording_id);

    assert_eq!(
        rust_snapshot.len(),
        java_snapshot.len(),
        "the two snapshots are the same number of bytes"
    );
    assert_eq!(
        rust_snapshot, java_snapshot,
        "the Rust twin's snapshot is the Java one's bytes"
    );

    cluster.shutdown();
}

#[test]
fn a_snapshot_survives_a_container_restart_in_process() {
    let _serial = SERIAL.lock().expect("the serial lock");
    snapshot_survives_a_restart("lc4a-snapshot-in-process", ProcessMode::InProcess);
}

#[test]
fn a_snapshot_survives_a_container_restart_external() {
    let _serial = SERIAL.lock().expect("the serial lock");
    snapshot_survives_a_restart("lc4a-snapshot-external", ProcessMode::External);
}

#[test]
fn the_twins_snapshot_is_the_java_ones_byte_for_byte_in_process() {
    let _serial = SERIAL.lock().expect("the serial lock");
    twins_agree_byte_for_byte("lc4a-twin-in-process", ProcessMode::InProcess);
}

#[test]
fn the_twins_snapshot_is_the_java_ones_byte_for_byte_external() {
    let _serial = SERIAL.lock().expect("the serial lock");
    twins_agree_byte_for_byte("lc4a-twin-external", ProcessMode::External);
}
