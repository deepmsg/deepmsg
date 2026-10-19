//! P3-2b's LC-4a card: **the reference's consensus module drives our container
//! through a snapshot and a restart**, with the reference's client sending the
//! messages.
//!
//! The two shapes the card's DoD names:
//!
//! * `a_snapshot_survives_a_container_restart` — a Java cluster (driver, archive
//!   and consensus module in one process) and our container running the auction
//!   twin. Bids go through the reference's `AeronCluster`, the SNAPSHOT toggle
//!   turns once, the container is stopped and started again **in the same
//!   directory**, and the recovered state answers the next bid with what the
//!   snapshot held.
//! * `the_twins_snapshot_is_the_java_ones_byte_for_byte` — one consensus module,
//!   **two containers**: the reference's `ClusteredServiceContainer` running
//!   `BasicAuctionClusteredService` (service 0) and ours running the Rust twin
//!   (service 1). One `SNAPSHOT` produces two recordings; their payload bytes
//!   must be equal.
//!
//! Both tests own a whole cluster on `/dev/shm` names and Java processes, so
//! [`SERIAL`] keeps them from meeting (the binary runs tests in parallel).
//!
//! # Environment
//!
//! `DEEPMSG_REF_AERON_ALL_JAR` and `DEEPMSG_AERON_TESTS` are required; without
//! them the tests print a skip and return (the workspace's rule for the Java
//! half of the cluster suite).

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use deepmsg_archive::client::{Archive, ArchiveContext, Handlers, NoCredentials, ReplayParams};
use deepmsg_client::client::Client;
use deepmsg_cluster_common::counters::recovery::RecoveryStateKey;
use deepmsg_cluster_common::counters::{
    self, CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID, CLUSTER_SNAPSHOT_COUNTER_TYPE_ID,
};
use deepmsg_cluster_service::{
    BasicAuction, ClusteredServiceAgent, ContainerError, ServiceContainerConfig,
};
use deepmsg_tests::aeron_tests;
use deepmsg_tests::driver::{self, AGRONA_JVM_ARGS, ReferenceDriver};
use deepmsg_tests::java;
use deepmsg_tests::temp::TempDir;

const DEADLINE: Duration = Duration::from_secs(120);
const CLUSTER_ID: i32 = 8;
const CONTROL_CHANNEL: &str = "aeron:ipc?term-length=128k";
const MEMBERS: &str = "0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0";
const REPLAY_CHANNEL: &str = "aeron:ipc";
const REPLAY_STREAM_ID: i32 = 103;
/// A control response stream no container of this cluster uses.
const READER_RESPONSE_STREAM_ID: i32 = 9_000;

static SERIAL: Mutex<()> = Mutex::new(());

/// The Java probes, compiled once.
fn compile_java() -> Option<TempDir> {
    let bid = aeron_tests::java("ClusterAuctionBidClientProbe.java")?;
    let service = aeron_tests::java("ClusterAuctionServiceProbe.java")?;

    let classes = TempDir::new("cluster-lc4a-snapshot-classes");
    java::compile_probe(&bid, classes.path())?;
    java::compile_probe(&service, classes.path())?;
    Some(classes)
}

/// One Java node: driver, archive and consensus module, with `service_count`
/// services expected.
struct SnapshotCluster {
    node: Option<ReferenceDriver>,
    aeron: TempDir,
    cluster: TempDir,
    _archive: TempDir,
    classes: TempDir,
    jar: PathBuf,
}

impl SnapshotCluster {
    fn start(test_name: &str, service_count: i32) -> Option<Self> {
        let jar = driver::locate_aeron_all()?;
        let classes = compile_java()?;
        let aeron = TempDir::new(&format!("{test_name}-aeron"));
        let cluster = TempDir::new(&format!("{test_name}-cluster"));
        let archive = TempDir::new(&format!("{test_name}-archive"));

        let mut this = Self {
            node: None,
            aeron,
            cluster,
            _archive: archive,
            classes,
            jar,
        };
        this.node = Some(this.spawn_node(service_count));
        Some(this)
    }

    /// Start the Java node over the instance's own directories.
    fn spawn_node(&self, service_count: i32) -> ReferenceDriver {
        let jar = &self.jar;
        let aeron = &self.aeron;
        let cluster = &self.cluster;
        let archive = &self._archive;

        let mut args: Vec<String> = AGRONA_JVM_ARGS
            .iter()
            .map(|arg| (*arg).to_string())
            .collect();
        args.push(format!("-Daeron.dir={}", aeron.path().display()));
        args.push(format!("-Daeron.cluster.dir={}", cluster.path().display()));
        args.push(format!("-Daeron.archive.dir={}", archive.path().display()));
        args.push("-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:0".to_owned());
        args.push("-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0".to_owned());
        args.push(format!("-Daeron.cluster.id={CLUSTER_ID}"));
        args.push(format!("-Daeron.cluster.members={MEMBERS}"));
        args.push("-Daeron.cluster.ingress.channel=aeron:ipc".to_owned());
        args.push("-Daeron.cluster.consensus.channel=aeron:ipc".to_owned());
        args.push("-Daeron.cluster.log.channel=aeron:ipc".to_owned());
        args.push(format!("-Daeron.cluster.control.channel={CONTROL_CHANNEL}"));
        args.push("-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0".to_owned());
        args.push(format!("-Daeron.cluster.service.count={service_count}"));
        args.push("-Daeron.threading.mode=SHARED".to_owned());
        args.push("-Daeron.print.configuration=false".to_owned());
        args.push("-cp".to_owned());
        args.push(jar.to_string_lossy().into_owned());
        args.push("io.aeron.cluster.ClusteredMediaDriver".to_owned());

        let process_dir = cluster.path().join("java-node-process");
        let log = cluster.path().join("java-node.log");
        ReferenceDriver::spawn_with_args_and_log(Path::new("java"), &process_dir, &log, &args, &[])
            .unwrap_or_else(|error| panic!("the Java node did not start: {error}"))
    }

    /// Stop the node and start it again over the same directories: what a
    /// recovery plan is built from is the on-disk `recording.log`, and the
    /// recovery state counter is allocated once, at that startup.
    fn restart_node(&mut self, service_count: i32) {
        if let Some(mut node) = self.node.take() {
            let status = node.stop().expect("the Java node stops");
            assert!(
                status.success() || status.code() == Some(143),
                "the Java node exited with {status}\nnode log:\n{}",
                node.log_tail(80)
            );
        }
        self.node = Some(self.spawn_node(service_count));
    }

    fn aeron_dir(&self) -> &Path {
        self.aeron.path()
    }

    fn config(&self, service_id: i32) -> ServiceContainerConfig {
        ServiceContainerConfig {
            cluster_id: CLUSTER_ID,
            service_id,
            service_name: format!("basic-auction-{service_id}"),
            aeron_dir: self.aeron_dir().to_path_buf(),
            cluster_dir: self.cluster.path().to_path_buf(),
            mark_file_dir: None,
            control_channel: CONTROL_CHANNEL.to_owned(),
            service_stream_id: 104,
            consensus_module_stream_id: 105,
            log_fragment_limit: 50,
            // One service answers a client, as `isRespondingService` means in a
            // multi-service deployment: service 0 here, so the twin test's
            // clients get egress from the Java container and both containers
            // still apply the same log.
            responding_service: service_id == 0,
            error_buffer_length: 1024 * 1024,
            idle_strategy: "org.agrona.concurrent.BackoffIdleStrategy".to_owned(),
            app_version: 1,
            replay_channel: REPLAY_CHANNEL.to_owned(),
            replay_stream_id: REPLAY_STREAM_ID,
            snapshot_channel: "aeron:ipc?alias=snapshot".to_owned(),
            snapshot_stream_id: 106,
            archive_local_control_channel: "aeron:ipc?term-length=64k".to_owned(),
            archive_local_control_stream_id: 10,
            archive_control_response_stream_id: CLUSTER_ID * 100 + 100 + 20 + (service_id + 1),
            snapshot_duration_threshold_ns: 1_000_000_000,
        }
    }

    fn service_log(&self, service_id: i32) -> String {
        std::fs::read_to_string(
            self.cluster
                .path()
                .join(format!("java-service-{service_id}.log")),
        )
        .unwrap_or_default()
    }

    fn node_log(&self) -> String {
        self.node
            .as_ref()
            .map_or_else(String::new, |node| node.log_tail(80))
    }

    fn client(&self) -> Client {
        let deadline = Instant::now() + DEADLINE;
        loop {
            match Client::connect(self.aeron_dir()) {
                Ok(client) => return client,
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(error) => panic!(
                    "no client onto the Java driver: {error}\nnode log:\n{}",
                    self.node_log()
                ),
            }
        }
    }

    /// Wait until the consensus module is ACTIVE — which, with services, also
    /// means every service container has joined and acked its log.
    fn await_active(&self) {
        let client = self.client();
        let deadline = Instant::now() + DEADLINE;
        while Instant::now() < deadline {
            if counter_value(&client, CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) == Some(1) {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "the cluster never became ACTIVE\nnode log:\n{}\nservice 0 log:\n{}\nservice 1 log:\n{}",
            self.node_log(),
            self.service_log(0),
            self.service_log(1)
        );
    }

    /// The recovery state's snapshot recording id for one service.
    fn snapshot_recording_id(&self, service_id: i32) -> Option<i64> {
        let client = self.client();
        let counters = client.counters_reader()?;
        let counter_id = counters::find_recovery_state(&counters, CLUSTER_ID)?;
        let key = counters.key(counter_id)?;
        RecoveryStateKey::decode(&key)
            .ok()?
            .snapshot_recording_id(service_id)
            .ok()
    }

    /// The consensus module's own snapshot counter (205). It is incremented
    /// only after every service ack has arrived **and** the recording log has
    /// been forced, so it is the moment after which a restart can recover.
    fn snapshot_count(&self) -> i64 {
        let client = self.client();
        counter_value(&client, CLUSTER_SNAPSHOT_COUNTER_TYPE_ID).unwrap_or(0)
    }

    fn await_snapshot_count(&self, expected: i64) {
        let deadline = Instant::now() + DEADLINE;
        while Instant::now() < deadline {
            if self.snapshot_count() >= expected {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "the consensus module never counted {expected} snapshot(s)\nnode log:\n{}",
            self.node_log()
        );
    }

    fn await_snapshot_recording(&self, service_id: i32) -> i64 {
        let deadline = Instant::now() + DEADLINE;
        while Instant::now() < deadline {
            if let Some(recording_id) = self.snapshot_recording_id(service_id) {
                if recording_id != -1 {
                    return recording_id;
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "service {service_id} never acked a snapshot recording\nnode log:\n{}",
            self.node_log()
        );
    }

    /// Start our container on a thread of this process, running the auction.
    fn start_container(&self, service_id: i32) -> Container {
        let config = self.config(service_id);
        let (started_tx, started_rx) = mpsc::channel();

        let thread = std::thread::spawn(move || -> Result<(), String> {
            let mut agent = match ClusteredServiceAgent::launch(config, BasicAuction::new()) {
                Ok(agent) => agent,
                Err(error) => {
                    let message = error.to_string();
                    let _ = started_tx.send(Err(message.clone()));
                    return Err(message);
                }
            };
            let shutdown = agent.shutdown_handle();
            let _ = started_tx.send(Ok(shutdown));

            let mut outcome = Ok(());
            loop {
                match agent.do_work() {
                    Ok(_) => {}
                    Err(ContainerError::AgentTermination(message)) => {
                        eprintln!("the container stopped: {message}");
                        break;
                    }
                    Err(error) => {
                        eprintln!("the container failed: {error}");
                        outcome = Err(error.to_string());
                        break;
                    }
                }
            }
            if let Err(error) = agent.close() {
                outcome = Err(error.to_string());
            }
            outcome
        });

        let shutdown = started_rx
            .recv_timeout(DEADLINE)
            .expect("the container reported its start")
            .unwrap_or_else(|error| panic!("the container did not start: {error}"));

        Container {
            shutdown: Some(shutdown),
            thread: Some(thread),
        }
    }

    /// The command line the bid probe is run with.
    fn bid_command(
        &self,
        bids: usize,
        customer: u64,
        first_price: u64,
        snapshot: bool,
        hold_ms: u64,
    ) -> Vec<String> {
        vec![
            self.aeron_dir().display().to_string(),
            CLUSTER_ID.to_string(),
            bids.to_string(),
            customer.to_string(),
            first_price.to_string(),
            snapshot.to_string(),
            hold_ms.to_string(),
        ]
    }

    /// Run the bid probe: `<bids> <customer> <first price> <snapshot?> <hold>`.
    fn bid(&self, bids: usize, customer: u64, first_price: u64, snapshot: bool) -> String {
        let args = self.bid_command(bids, customer, first_price, snapshot, 0);
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        java::run_probe(
            &self.jar,
            self.classes.path(),
            "ClusterAuctionBidClientProbe",
            &args,
        )
    }

    /// The same, on a thread, so its session stays open while another turn
    /// takes the snapshot.
    fn bid_in_background(
        &self,
        bids: usize,
        customer: u64,
        first_price: u64,
        hold_ms: u64,
    ) -> std::thread::JoinHandle<String> {
        let args: Vec<String> = self.bid_command(bids, customer, first_price, false, hold_ms);
        let jar = self.jar.clone();
        let classes = self.classes.path().to_path_buf();

        std::thread::spawn(move || {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            java::run_probe(&jar, &classes, "ClusterAuctionBidClientProbe", &args)
        })
    }

    /// Start the reference's container for the Java twin as a process.
    fn start_java_container(&self, service_id: i32) -> ReferenceDriver {
        let aeron_dir = self.aeron_dir().display().to_string();
        let cluster_dir = self.cluster.path().display().to_string();
        let mut args: Vec<String> = AGRONA_JVM_ARGS
            .iter()
            .map(|arg| (*arg).to_string())
            .collect();
        args.push("-cp".to_owned());
        args.extend([
            format!("{}:{}", self.jar.display(), self.classes.path().display()),
            "ClusterAuctionServiceProbe".to_owned(),
            aeron_dir,
            cluster_dir,
            CLUSTER_ID.to_string(),
            service_id.to_string(),
        ]);
        let process_dir = self
            .cluster
            .path()
            .join(format!("java-service-{service_id}-process"));
        let log = self
            .cluster
            .path()
            .join(format!("java-service-{service_id}.log"));
        ReferenceDriver::spawn_with_args_and_log(Path::new("java"), &process_dir, &log, &args, &[])
            .unwrap_or_else(|error| panic!("the Java container did not start: {error}"))
    }

    fn shutdown(&mut self) {
        if let Some(mut node) = self.node.take() {
            let status = node.stop().expect("the Java node stops");
            assert!(
                status.success() || status.code() == Some(143),
                "the Java node exited with {status}\nnode log:\n{}",
                node.log_tail(80)
            );
        }
    }
}

/// The container thread and its stop handle.
struct Container {
    shutdown: Option<deepmsg_cluster_service::ShutdownHandle>,
    thread: Option<std::thread::JoinHandle<Result<(), String>>>,
}

impl Container {
    fn stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.stop();
        }
        if let Some(thread) = self.thread.take() {
            match thread.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => panic!("the container failed: {error}"),
                Err(_) => panic!("the container thread panicked"),
            }
        }
    }
}

fn counter_value(client: &Client, type_id: i32) -> Option<i64> {
    let counters = client.counters_reader()?;
    let counter_id = counters::find_cluster_counter(&counters, type_id, CLUSTER_ID)?;
    counters.value(counter_id)
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

#[test]
fn a_snapshot_survives_a_container_restart() {
    let _serial = SERIAL.lock().expect("the serial lock");
    let Some(mut cluster) = SnapshotCluster::start("lc4a-snapshot-restart", 1) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };

    let mut container = cluster.start_container(0);
    cluster.await_active();

    // Ten winning bids from one customer: the state after them is (1, 109).
    let reading = cluster.bid(10, 1, 100, true);
    assert!(
        reading.contains("RECEIVED 10"),
        "the bids went through, client said: {reading}\nnode log:\n{}",
        cluster.node_log()
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
    cluster.await_snapshot_count(1);

    // Stop everything and start it again in the same directories. The recovery
    // state counter is allocated during the module's startup from the on-disk
    // `recording.log`; only a restart sees the snapshot taken a moment ago.
    container.stop();
    cluster.restart_node(1);

    let recording_id = cluster.await_snapshot_recording(0);
    assert_ne!(
        recording_id, -1,
        "the recovery state names the service snapshot"
    );

    let mut container = cluster.start_container(0);

    // A losing bid is answered with the current state, which must be the state
    // the snapshot held — not the empty auction a fresh container would have.
    let reading = cluster.bid(1, 99, 0, false);
    assert!(
        reading.contains("LAST 1 109 false"),
        "the recovered auction answered with the snapshot's state, client said: {reading}\n\
         node log:\n{}",
        cluster.node_log()
    );

    container.stop();
    cluster.shutdown();
}

#[test]
fn the_twins_snapshot_is_the_java_ones_byte_for_byte() {
    let _serial = SERIAL.lock().expect("the serial lock");
    let Some(mut cluster) = SnapshotCluster::start("lc4a-snapshot-twin", 2) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };

    let mut java_container = cluster.start_java_container(0);
    let mut rust_container = cluster.start_container(1);
    cluster.await_active();

    // Two clients, one bid each, both **held open**: the snapshot's session
    // table then carries two sessions, which is what makes its order
    // observable (and what a session-order mutation can fail on).
    let first = cluster.bid_in_background(1, 1, 100, 8_000);
    let second = cluster.bid_in_background(1, 2, 200, 8_000);

    // Let both bids land before the snapshot reads the state.
    std::thread::sleep(Duration::from_secs(3));

    // One SNAPSHOT action reaches both services.
    let reading = cluster.bid(0, 0, 0, true);
    assert!(
        reading.contains("SNAPSHOT_REQUESTED"),
        "the SNAPSHOT toggle turned, client said: {reading}"
    );

    // Both acks are in and the module has forced its log.
    cluster.await_snapshot_count(1);

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
    rust_container.stop();
    let status = java_container.stop().expect("the Java container stops");
    assert!(
        status.success() || status.code() == Some(143),
        "the Java container exited with {status}"
    );
    cluster.restart_node(2);

    let java_recording_id = cluster.await_snapshot_recording(0);
    let rust_recording_id = cluster.await_snapshot_recording(1);

    let java_snapshot = read_recording(cluster.aeron_dir(), java_recording_id);
    let rust_snapshot = read_recording(cluster.aeron_dir(), rust_recording_id);

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
