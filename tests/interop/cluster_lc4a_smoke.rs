//! P3-2a's LC-4a smoke: **the reference's driver, archive and consensus
//! module, deepmsg's service container, the reference's client**.
//!
//! Everything else in this workspace's cluster suite either puts our own fake
//! service on streams 104/105 or our own consensus module on the other side. A
//! container that only ever met our own module would be two halves of one
//! implementation agreeing; here the far side of S2 is the reference's own
//! `ConsensusModuleAgent` inside `ClusteredMediaDriver`, and the client is the
//! reference's own `AeronCluster`.
//!
//! The container runs on a thread of this process, because P3-2a has no
//! container binary (P3-2c builds it). That is not a shortcut in the seam: the
//! container's only connection to the node is its own `Client` on the node's
//! aeron directory and the S2 IPC streams, which is the same connection a
//! process would have.
//!
//! # What the two tests assert
//!
//! * `a_java_cluster_echoes_through_our_container` — one echo and ten thousand
//!   through a real session (the whole path: ingress, log, the container's
//!   `on_session_message`, egress back through the Java driver).
//! * `timers_schedule_cancel_and_expire_through_the_java_cm` — a timer fires, a
//!   cancelled timer does not, and a second one fires: the container's
//!   `ScheduleTimer`/`CancelTimer` reach the Java module and its `TimerEvent`s
//!   come back through the log.
//!
//! # One at a time
//!
//! Each test owns a whole cluster on `/dev/shm` names and a Java process, and
//! the two tests in this binary share a process; [`SERIAL`] keeps them from
//! meeting.

use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cluster_common::counters::{self, CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID};
use deepmsg_cluster_service::{ClusteredServiceAgent, ContainerError, ServiceContainerConfig};
use deepmsg_tests::aeron_tests;
use deepmsg_tests::cluster_service_echo::EchoServiceWithTermination;
use deepmsg_tests::driver::{self, AGRONA_JVM_ARGS, ReferenceDriver};
use deepmsg_tests::java;
use deepmsg_tests::temp::TempDir;

const DEADLINE: Duration = Duration::from_secs(90);
const CLUSTER_ID: i32 = 7;
const CONTROL_CHANNEL: &str = "aeron:ipc?term-length=128k";
/// One member: id, ingress, consensus, log, catchup, archive. The endpoints are
/// unused because every channel below is IPC, and `:0` keeps two tests from
/// binding the same port.
const MEMBERS: &str = "0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0";

/// The tests in this binary each start a whole cluster.
static SERIAL: Mutex<()> = Mutex::new(());

/// The Java probes both tests need, compiled once each.
fn compile_java() -> Option<TempDir> {
    let echo = aeron_tests::java("ClusterEchoClientProbe.java")?;
    let timers = aeron_tests::java("ClusterTimerClientProbe.java")?;

    let classes = TempDir::new("cluster-lc4a-classes");
    java::compile_probe(&echo, classes.path())?;
    java::compile_probe(&timers, classes.path())?;
    Some(classes)
}

/// The reference node: one Java process with a media driver, an archive and a
/// consensus module in it.
struct Lc4a {
    node: Option<ReferenceDriver>,
    aeron: TempDir,
    cluster: TempDir,
    _archive: TempDir,
    classes: TempDir,
    jar: std::path::PathBuf,
}

impl Lc4a {
    fn start(test_name: &str) -> Option<Self> {
        let jar = driver::locate_aeron_all()?;
        let classes = compile_java()?;
        let aeron = TempDir::new(&format!("{test_name}-aeron"));
        let cluster = TempDir::new(&format!("{test_name}-cluster"));
        let archive = TempDir::new(&format!("{test_name}-archive"));

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
        args.push("-Daeron.cluster.service.count=1".to_owned());
        args.push("-Daeron.threading.mode=SHARED".to_owned());
        args.push("-Daeron.print.configuration=false".to_owned());
        args.push("-cp".to_owned());
        args.push(jar.to_string_lossy().into_owned());
        args.push("io.aeron.cluster.ClusteredMediaDriver".to_owned());

        // The handle's own directory is private: `ReferenceDriver::Drop` removes
        // the aeron directory it was given, and the real one is shared with the
        // container.
        let process_dir = cluster.path().join("java-node-process");
        let log = cluster.path().join("java-node.log");
        let node = ReferenceDriver::spawn_with_args_and_log(
            Path::new("java"),
            &process_dir,
            &log,
            &args,
            &[],
        )
        .unwrap_or_else(|error| panic!("{test_name}: the Java node did not start: {error}"));

        Some(Self {
            node: Some(node),
            aeron,
            cluster,
            _archive: archive,
            classes,
            jar,
        })
    }

    fn aeron_dir(&self) -> &Path {
        self.aeron.path()
    }

    fn config(&self) -> ServiceContainerConfig {
        ServiceContainerConfig {
            cluster_id: CLUSTER_ID,
            service_id: 0,
            service_name: "cluster-echo".to_owned(),
            aeron_dir: self.aeron_dir().to_path_buf(),
            cluster_dir: self.cluster.path().to_path_buf(),
            mark_file_dir: None,
            control_channel: CONTROL_CHANNEL.to_owned(),
            service_stream_id: 104,
            consensus_module_stream_id: 105,
            log_fragment_limit: 50,
            responding_service: true,
            error_buffer_length: 1024 * 1024,
            idle_strategy: "org.agrona.concurrent.BackoffIdleStrategy".to_owned(),
            app_version: 1,
            replay_channel: "aeron:ipc".to_owned(),
            replay_stream_id: 103,
            snapshot_channel: "aeron:ipc?alias=snapshot".to_owned(),
            snapshot_stream_id: 106,
            archive_local_control_channel: "aeron:ipc?term-length=64k".to_owned(),
            archive_local_control_stream_id: 10,
            // `clusterId * 100 + 100 + controlResponseStreamId + (serviceId + 1)`.
            archive_control_response_stream_id: CLUSTER_ID * 100 + 100 + 20 + 1,
            snapshot_duration_threshold_ns: 1_000_000_000,
        }
    }

    fn node_log(&self) -> String {
        self.node
            .as_ref()
            .map_or_else(String::new, |node| node.log_tail(60))
    }

    /// A client onto the Java driver, waiting for its CnC file.
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

    /// Wait until the container has joined and the node is ACTIVE.
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
            "the cluster never became ACTIVE\nnode log:\n{}",
            self.node_log()
        );
    }

    /// Start the container on a thread of this process.
    fn start_container(&self) -> Container {
        let (service, terminated) = EchoServiceWithTermination::new();
        let config = self.config();
        let (started_tx, started_rx) = mpsc::channel();

        let thread = std::thread::spawn(move || -> Result<(), String> {
            let mut agent = match ClusteredServiceAgent::launch(config, service) {
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
            terminated,
        }
    }

    fn echo(&self, count: usize, timeout_seconds: u64) -> String {
        let aeron_dir = self.aeron_dir().display().to_string();
        java::run_probe(
            &self.jar,
            self.classes.path(),
            "ClusterEchoClientProbe",
            &[
                aeron_dir.as_str(),
                &count.to_string(),
                &timeout_seconds.to_string(),
            ],
        )
    }

    fn timers(&self) -> String {
        let aeron_dir = self.aeron_dir().display().to_string();
        java::run_probe(
            &self.jar,
            self.classes.path(),
            "ClusterTimerClientProbe",
            &[aeron_dir.as_str(), "30"],
        )
    }

    fn shutdown(&mut self) {
        if let Some(mut node) = self.node.take() {
            let status = node.stop().expect("the Java node stops");
            // `ClusteredMediaDriver.main` installs a `ShutdownSignalBarrier`, so
            // a SIGTERM is a clean stop; a JVM still reports 143 for the signal.
            assert!(
                status.success() || status.code() == Some(143),
                "the Java node exited with {status}\nnode log:\n{}",
                node.log_tail(60)
            );
        }
    }
}

/// The container thread and its stop handle.
struct Container {
    shutdown: Option<deepmsg_cluster_service::ShutdownHandle>,
    thread: Option<std::thread::JoinHandle<Result<(), String>>>,
    terminated: Arc<std::sync::atomic::AtomicBool>,
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
        assert!(
            self.terminated.load(std::sync::atomic::Ordering::Acquire),
            "on_terminate ran when the container was stopped"
        );
    }
}

fn counter_value(client: &Client, type_id: i32) -> Option<i64> {
    let counters = client.counters_reader()?;
    let counter_id = counters::find_cluster_counter(&counters, type_id, CLUSTER_ID)?;
    counters.value(counter_id)
}

#[test]
fn a_java_cluster_echoes_through_our_container() {
    let _serial = SERIAL.lock().expect("the serial lock");
    let Some(mut cluster) = Lc4a::start("cluster-lc4a-echo") else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };

    let mut container = cluster.start_container();
    cluster.await_active();

    let reading = cluster.echo(1, 20);
    assert!(
        reading.contains("RECEIVED 1"),
        "one echo came back, client said: {reading}\nnode log:\n{}",
        cluster.node_log()
    );

    let reading = cluster.echo(10_000, 60);
    assert!(
        reading.contains("RECEIVED 10000"),
        "ten thousand echoes came back, client said: {reading}\nnode log:\n{}",
        cluster.node_log()
    );

    container.stop();
    cluster.shutdown();
}

#[test]
fn timers_schedule_cancel_and_expire_through_the_java_cm() {
    let _serial = SERIAL.lock().expect("the serial lock");
    let Some(mut cluster) = Lc4a::start("cluster-lc4a-timers") else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };

    let mut container = cluster.start_container();
    cluster.await_active();

    let reading = cluster.timers();
    assert!(
        reading.contains("TIMER 1"),
        "the scheduled timer fired, client said: {reading}\nnode log:\n{}",
        cluster.node_log()
    );
    assert!(
        reading.contains("CANCELLED 2"),
        "the cancelled timer did not fire, client said: {reading}\nnode log:\n{}",
        cluster.node_log()
    );
    assert!(
        reading.contains("TIMER 3"),
        "the third timer fired, client said: {reading}\nnode log:\n{}",
        cluster.node_log()
    );

    container.stop();
    cluster.shutdown();
}
