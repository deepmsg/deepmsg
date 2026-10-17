//! LC-4b: **our consensus module, the reference's service container, the
//! reference's client** — the S2 seam with Java on its far side.
//!
//! Everything else in this workspace's cluster suite puts our own fake service
//! on stream 104/105, and two halves of one implementation agreeing is not yet
//! the wire contract. Here the far side is the reference's own
//! `ClusteredServiceContainer`, loaded with a Java `ClusteredService` compiled
//! from the fork's `deepmsg-tests/java/`, and the client is the reference's own
//! `AeronCluster`. What has to agree is:
//!
//! * the CM's `JoinLog` and the container's `ServiceAck` (startup),
//! * the log recording the CM makes and the container reads to replay the
//!   client's message (`LogAdapter`),
//! * the container's egress through the CM's `EgressPublisher` back to the
//!   client,
//! * the SNAPSHOT toggle: the CM asks the services for their snapshots and waits
//!   for every ack (`snapshotOnServiceAck`), which only a real container can
//!   exercise,
//! * the same directories again after a stop: the CM recovers from its snapshot
//!   and the container loads the snapshot the reference wrote.
//!
//! The driver and the archive are this build's
//! (`deepmsg-archiving-media-driver`); only the two cluster halves change sides,
//! which is what makes this a test of S2 rather than of the whole stack.
//!
//! # The harness's `ReferenceDriver` is one process per directory
//!
//! [`ReferenceDriver`] removes the aeron directory when it drops, because the
//! process it was built for — a media driver — owns that directory. Two of the
//! three processes here **share** one directory (the CM and the container attach
//! to the driver's), so each is spawned with a path of its own that names
//! nothing: the real directory is in their `-Daeron.dir` argument. Spawning them
//! with the shared path makes the first one to stop delete the driver's CnC file
//! — which is exactly what this test did until the directory was made private.
//!
//! `LC-4b` is single-node here. Three-node LC-4b, and the timer/session-switch
//! scripts, need the consensus publisher and follower paths (P3-6a/6b) and are
//! recorded as this slice's remaining scope.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cluster_common::counters::{
    self, CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID, CLUSTER_CONTROL_TOGGLE_TYPE_ID,
    CLUSTER_SNAPSHOT_COUNTER_TYPE_ID,
};
use deepmsg_tests::aeron_tests;
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::driver::{self, AGRONA_JVM_ARGS, ReferenceDriver};
use deepmsg_tests::java;
use deepmsg_tests::temp::TempDir;

const DEADLINE: Duration = Duration::from_secs(90);
const CLUSTER_ID: i32 = 7;
const CONTROL_CHANNEL: &str = "aeron:ipc?term-length=128k";
const ARENA: &str = "0,localhost:9001,localhost:9002,localhost:9003,localhost:9004,localhost:9005";

/// Where `deepmsg-cluster-node` is expected, relative to the tests crate.
const DEFAULT_CLUSTER_NODE: &str = "../target/debug/deepmsg-cluster-node";

fn locate_cluster_node() -> Option<PathBuf> {
    driver::locate_tool("DEEPMSG_CLUSTER_NODE", DEFAULT_CLUSTER_NODE)
}

fn counter_value(client: &Client, type_id: i32) -> Option<i64> {
    let counters = client.counters_reader()?;
    let counter_id = counters::find_cluster_counter(&counters, type_id, CLUSTER_ID)?;
    counters.value(counter_id)
}

/// The Java classes both halves of LC-4b need, compiled once per test.
fn compile_java() -> Option<TempDir> {
    let service = aeron_tests::java("ClusterEchoService.java")?;
    let client = aeron_tests::java("ClusterEchoClientProbe.java")?;

    let classes = TempDir::new("cluster-lc4b-classes");
    java::compile_probe(&service, classes.path())?;
    java::compile_probe(&client, classes.path())?;
    Some(classes)
}

/// One running cluster: our driver+archive, our CM, the reference's container.
///
/// The cluster directory is **the caller's**, because a restart means the same
/// directory again — the CM's `recording.log`, `cluster-mark.dat` and
/// `node-state.dat`, and the container's service mark file, all stay where they
/// were.
struct Lc4b {
    test_name: String,
    driver: OwnArchivingMediaDriver,
    cluster_dir: PathBuf,
    classes: TempDir,
    jar: PathBuf,
    container: Option<ReferenceDriver>,
    consensus_module: Option<ReferenceDriver>,
}

impl Lc4b {
    fn start(
        test_name: &str,
        driver: OwnArchivingMediaDriver,
        cluster_dir: &Path,
        jar: PathBuf,
        classes: TempDir,
    ) -> Self {
        let mut cluster = Self {
            test_name: test_name.to_owned(),
            driver,
            cluster_dir: cluster_dir.to_path_buf(),
            classes,
            jar,
            container: None,
            consensus_module: None,
        };
        cluster.start_consensus_module();
        cluster.start_container();
        cluster
    }

    fn start_consensus_module(&mut self) {
        let Some(binary) = locate_cluster_node() else {
            archiving_driver::announce_skip();
            panic!("{}: no deepmsg-cluster-node binary", self.test_name);
        };

        let properties: Vec<String> = vec![
            format!("-Daeron.dir={}", self.driver.aeron_dir().display()),
            format!("-Daeron.cluster.dir={}", self.cluster_dir.display()),
            format!("-Daeron.cluster.id={CLUSTER_ID}"),
            "-Daeron.cluster.member.id=0".to_owned(),
            format!("-Daeron.cluster.members={ARENA}"),
            "-Daeron.cluster.ingress.channel=aeron:ipc".to_owned(),
            "-Daeron.cluster.consensus.channel=aeron:ipc".to_owned(),
            "-Daeron.cluster.log.channel=aeron:ipc".to_owned(),
            format!("-Daeron.cluster.control.channel={CONTROL_CHANNEL}"),
            "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0".to_owned(),
            "-Daeron.cluster.service.count=1".to_owned(),
        ];

        // The process handle's directory, not the driver's: see the module docs.
        let log = self.cluster_dir.join("cluster-node.log");
        let node = ReferenceDriver::spawn_with_args_and_log(
            &binary,
            &self.process_dir("consensus-module"),
            &log,
            &properties,
            &[],
        )
        .unwrap_or_else(|error| panic!("{}: the CM did not start: {error}", self.test_name));

        self.consensus_module = Some(node);
    }

    fn start_container(&mut self) {
        let classpath = std::env::join_paths([self.jar.as_path(), self.classes.path()])
            .expect("two paths join");

        let mut args: Vec<String> = AGRONA_JVM_ARGS
            .iter()
            .map(|arg| (*arg).to_string())
            .collect();
        args.push(format!("-Daeron.dir={}", self.driver.aeron_dir().display()));
        args.push(format!(
            "-Daeron.cluster.dir={}",
            self.cluster_dir.display()
        ));
        args.push(format!("-Daeron.cluster.id={CLUSTER_ID}"));
        args.push(format!("-Daeron.cluster.control.channel={CONTROL_CHANNEL}"));
        args.push("-Daeron.cluster.service.class.name=ClusterEchoService".to_owned());
        args.push("-Daeron.threading.mode=SHARED".to_owned());
        args.push("-Daeron.print.configuration=false".to_owned());
        args.push("-cp".to_owned());
        args.push(classpath.to_string_lossy().into_owned());
        args.push("io.aeron.cluster.service.ClusteredServiceContainer".to_owned());

        // The process handle's directory, not the driver's: see the module docs.
        let log = self.cluster_dir.join("container.log");
        let container = ReferenceDriver::spawn_with_args_and_log(
            Path::new("java"),
            &self.process_dir("container"),
            &log,
            &args,
            &[],
        )
        .unwrap_or_else(|error| panic!("{}: the container did not start: {error}", self.test_name));

        self.container = Some(container);
    }

    /// A path for a [`ReferenceDriver`] that shares the driver's aeron
    /// directory, so the handle's drop-time cleanup removes this instead of the
    /// driver's CnC file. It is never created.
    fn process_dir(&self, name: &str) -> PathBuf {
        self.cluster_dir.join(format!("{name}-process"))
    }

    fn container_log(&self) -> String {
        self.container
            .as_ref()
            .map_or_else(String::new, |container| container.log_tail(40))
    }

    fn node_log(&self) -> String {
        self.consensus_module
            .as_ref()
            .map_or_else(String::new, |node| node.log_tail(40))
    }

    /// A client onto the driver, waiting for the CnC file if it has to.
    fn client(&self) -> Client {
        let deadline = Instant::now() + DEADLINE;
        loop {
            match Client::connect(self.driver.aeron_dir()) {
                Ok(client) => return client,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(error) => panic!(
                    "no client onto the cluster: {error}\ndriver log:\n{}",
                    self.driver.log_tail(40)
                ),
            }
        }
    }

    /// Wait until the CM is the ACTIVE leader with the container joined.
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
            "the cluster never became ACTIVE\nnode log:\n{}\ncontainer log:\n{}\ndriver log:\n{}",
            self.node_log(),
            self.container_log(),
            self.driver.log_tail(40)
        );
    }

    /// Run the reference client for `count` echoes and answer what it printed.
    fn echo(&self, count: usize) -> String {
        let aeron_dir = self.driver.aeron_dir().display().to_string();
        java::run_probe(
            &self.jar,
            self.classes.path(),
            "ClusterEchoClientProbe",
            &[aeron_dir.as_str(), &count.to_string()],
        )
    }

    /// Write the operator's control toggle, as `ClusterControl.main` does.
    fn toggle(&self, state: i64) {
        let client = self.client();
        let counter_id = {
            let counters = client.counters_reader().expect("counters");
            counters::find_cluster_counter(&counters, CLUSTER_CONTROL_TOGGLE_TYPE_ID, CLUSTER_ID)
                .expect("the control toggle")
        };
        client
            .counters_writable()
            .expect("writable counters")
            .set_value(counter_id, state)
            .expect("the toggle is written");
    }

    fn await_snapshot(&self) {
        let client = self.client();
        let deadline = Instant::now() + DEADLINE;
        while Instant::now() < deadline {
            if counter_value(&client, CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) == Some(1)
                && counter_value(&client, CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) == Some(1)
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "the snapshot never completed\nnode log:\n{}\ncontainer log:\n{}",
            self.node_log(),
            self.container_log()
        );
    }

    /// Stop the container and the CM, newest first.
    ///
    /// The CM is ours, so its stop is an assertion: `SIGTERM` ends the duty
    /// cycle, the agent closes, and the process exits **0**. The container is a
    /// JVM: its `ShutdownSignalBarrier` hook runs, so the cluster side is clean,
    /// but a JVM killed by a signal still exits **143** — the clean half is the
    /// work its hook did (mark file terminated, sessions closed), not this code.
    fn stop(&mut self) {
        if let Some(mut container) = self.container.take() {
            let status = container.stop().expect("the container stops");
            assert!(
                status.success() || status.code() == Some(143),
                "the container exited with {status}"
            );
        }
        if let Some(mut node) = self.consensus_module.take() {
            let status = node.stop().expect("the CM stops");
            assert!(status.success(), "the CM exited with {status}");
        }
    }

    /// The same directories, new processes.
    fn restart(&mut self) {
        self.start_consensus_module();
        self.start_container();
        self.await_active();
    }

    /// Stop the cluster and the driver, for the end of a test.
    fn shutdown(&mut self) {
        self.stop();
        self.driver.stop().expect("the driver stops");
    }
}

fn start_driver_and_java(
    test_name: &str,
) -> Option<(OwnArchivingMediaDriver, TempDir, PathBuf, TempDir)> {
    let jar = driver::locate_aeron_all()?;
    let classes = compile_java()?;
    // The archive directory is returned so the caller keeps it alive: a dropped
    // `TempDir` deletes the archive under a running driver, and the archive then
    // refuses every recording with `usableSpace=0`.
    let archive_dir = TempDir::new(&format!("{test_name}-archive"));
    let mut driver =
        OwnArchivingMediaDriver::start(test_name, archive_dir.path(), &archive::PROPERTIES)?;
    driver.await_ready(DEADLINE).expect("the driver is ready");
    Some((driver, archive_dir, jar, classes))
}

#[test]
fn the_reference_client_echoes_through_our_cm_and_its_own_container() {
    let Some((driver, _archive_dir, jar, classes)) = start_driver_and_java("cluster-lc4b-echo")
    else {
        eprintln!("SKIPPED: the reference jar, java, or the archiving media driver");
        return;
    };
    if locate_cluster_node().is_none() {
        archiving_driver::announce_skip();
        return;
    }

    let cluster_dir = TempDir::new("cluster-lc4b-echo-cluster");
    let mut cluster = Lc4b::start(
        "cluster-lc4b-echo",
        driver,
        cluster_dir.path(),
        jar,
        classes,
    );
    cluster.await_active();

    for count in [1_usize, 5] {
        let reading = cluster.echo(count);
        assert!(
            reading.contains(&format!("RECEIVED {count}")),
            "expected {count} echoes, client said: {reading}\nnode log:\n{}\ncontainer log:\n{}",
            cluster.node_log(),
            cluster.container_log()
        );
    }

    cluster.shutdown();
}

#[test]
fn the_snapshot_toggle_takes_the_containers_snapshot_and_the_cluster_recovers() {
    let Some((driver, _archive_dir, jar, classes)) = start_driver_and_java("cluster-lc4b-snapshot")
    else {
        eprintln!("SKIPPED: the reference jar, java, or the archiving media driver");
        return;
    };
    if locate_cluster_node().is_none() {
        archiving_driver::announce_skip();
        return;
    }

    let cluster_dir = TempDir::new("cluster-lc4b-snapshot-cluster");
    let mut cluster = Lc4b::start(
        "cluster-lc4b-snapshot",
        driver,
        cluster_dir.path(),
        jar,
        classes,
    );
    cluster.await_active();
    assert!(
        cluster.echo(3).contains("RECEIVED 3"),
        "the cluster echoes before the snapshot\nnode log:\n{}\ncontainer log:\n{}",
        cluster.node_log(),
        cluster.container_log()
    );

    // `ClusterControl.SNAPSHOT` = 4. With one real service this walks the whole
    // ack path: the CM's SNAPSHOT action, the container's own snapshot
    // recording, its `ServiceAck`, and `snapshotOnServiceAck`.
    cluster.toggle(4);
    cluster.await_snapshot();
    let client = cluster.client();
    assert_eq!(
        Some(i64::from(
            deepmsg_cluster::control::cluster_control::ToggleState::Neutral.code()
        )),
        counter_value(&client, CLUSTER_CONTROL_TOGGLE_TYPE_ID),
        "the module resets the toggle once the snapshot set is complete"
    );

    // A clean stop, then the same directories again: the CM replays its log to
    // the snapshot and the container loads the snapshot the reference wrote.
    cluster.stop();
    cluster.restart();

    assert!(
        cluster.echo(2).contains("RECEIVED 2"),
        "the recovered cluster echoes\nnode log:\n{}\ncontainer log:\n{}",
        cluster.node_log(),
        cluster.container_log()
    );

    cluster.shutdown();
}
