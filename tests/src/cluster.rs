//! `TestCluster`'s Rust version: a fixture that picks each member's implementation.
//!
//! The reference's cluster tests are a **Java dependency-injection harness**
//! (`TestCluster`/`TestNode`), not a black-box one: a test hands the node a Java
//! `ClusteredService`, a timer supplier, an extension, and reads
//! `node.service().messageCount()` out of the same JVM. That harness cannot be
//! reused for a Rust implementation (plan §2.3), so this is the replacement:
//! the same scenarios expressed over members chosen from a table.
//!
//! # The member table
//!
//! ```text
//! MemberSpec { id, service_id, node, service, driver, backup, mode, responder }
//! ```
//!
//! * `node` — who runs the consensus module. Only [`NodeImpl::Java`] is here
//!   today; ours is P3-6a's `cluster_harness`.
//! * `service` — who runs the container: [`ServiceImpl::Rust`] names a business
//!   from `deepmsg_cluster_service::services`, [`ServiceImpl::Test`] one of the
//!   fixture's own (with the reference's test-business observables, in process
//!   only), [`ServiceImpl::Java`] a Java class the reference's container loads.
//! * `driver` — the media driver. Only [`DriverImpl::Java`] is here today: the
//!   reference's `ClusteredMediaDriver` starts its own.
//! * `backup` — a backup member. **Carried from the first version and refused
//!   now**: `ClusterBackup` is P3-9, and the point of carrying it is that P3-9
//!   must not have to change this table (plan §7.2 revision).
//! * `mode` — [`ProcessMode::InProcess`] runs our container on a thread of this
//!   process (the only way to read a business's own state, which is what the
//!   reference's in-JVM harness does); [`ProcessMode::External`] runs the
//!   `deepmsg-clustered-service-container` binary and waits for two signals
//!   before calling it ready: its **mark file** and its own `STARTED` line.
//!   The mark file alone is not enough — it is signalled before the agent has
//!   recovered, and a container stopped in that window is killed by the signal
//!   instead of handling it. A Java container is always `External`: a Java
//!   object cannot live in this process.
//! * `service_id` — a node may run several containers in a multi-service
//!   deployment; every spec with the same `id` shares that node's directories
//!   and process.
//!
//! **Every scenario runs in both modes.** They are two different startup paths —
//! the second is the one LC-3 and a deployment take — and a mode that is never
//! run is a mode that is never tested (Q4a/Q4b, plan §7.2).
//!
//! # What "ready" means for an external container
//!
//! The mark file, and four things in it: component type `CONTAINER`, the
//! member's service id, the cluster id, and a live activity timestamp. **File
//! existence is not enough**: a restart over the same directory always finds the
//! previous run's file, and a test that waited for existence would read the dead
//! one and call the new process started.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cluster_common::counters::{
    self, CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID, CLUSTER_NODE_ROLE_TYPE_ID,
};

/// `Cluster.Role.LEADER` (`counters::ROLE_LEADER` is the same value).
const ROLE_LEADER: i64 = 2;
use deepmsg_cluster_common::mark::{ClusterMarkFile, SEMANTIC_VERSION};
use deepmsg_cluster_service::service::{LogHeader, SnapshotReader, SnapshotWriter};
use deepmsg_cluster_service::services;
use deepmsg_cluster_service::{
    ClusteredService, ClusteredServiceAgent, ContainerError, Role, ServiceContainerConfig,
    SessionId, ShutdownHandle, TimeUnit,
};
use deepmsg_codec_cluster::close_reason::CloseReason;
use deepmsg_codec_cluster_mark::cluster_component_type::ClusterComponentType;
use deepmsg_core::clock::epoch_millis;

use crate::aeron_tests;
use crate::driver::{self, AGRONA_JVM_ARGS};
use crate::java;
use crate::temp::TempDir;

/// How long a start, a stop or a wait may take before the test is a failure.
pub const DEADLINE: Duration = Duration::from_secs(120);

/// Environment variable naming the container binary to run.
pub const CONTAINER_ENV: &str = "DEEPMSG_CONTAINER_BIN";
/// Where the binary is when the variable is unset and the workspace is built.
pub const DEFAULT_CONTAINER: &str = "../target/debug/deepmsg-clustered-service-container";

/// Who runs a member's consensus module.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeImpl {
    /// Ours — P3-6a's `cluster_harness`, not this fixture.
    Rust,
    /// The reference's `ClusteredMediaDriver`.
    Java,
}

/// Who runs a member's service container.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceImpl {
    /// A business from `deepmsg_cluster_service::services::NAMES`.
    Rust(&'static str),
    /// A **test** business, defined here rather than in the product crate
    /// (see [`TestService`]). Its observables are the reference's
    /// `TestNode.TestService` ones: message count, session counts, a snapshot
    /// that can be told to fail. `InProcess` only — the binary has no such
    /// name, and giving the product a fault-injection business is not a trade
    /// this fixture gets to make.
    Test(&'static str),
    /// A Java class the reference's container loads by name.
    Java(&'static str),
}

/// Which media driver a member uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DriverImpl {
    /// Ours — P3-7's mixed matrix, not this fixture.
    Deepmsg,
    /// The reference's, started inside `ClusteredMediaDriver`.
    Java,
}

/// How a member's container is started.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessMode {
    /// On a thread of this process.
    InProcess,
    /// As the `deepmsg-clustered-service-container` binary.
    External,
}

/// One container of the cluster: a node, a service id, and who runs it.
#[derive(Clone, Copy, Debug)]
pub struct MemberSpec {
    /// `aeron.cluster.member.id` of the node it runs on.
    pub id: i32,
    /// `aeron.cluster.service.id`.
    pub service_id: i32,
    /// Who runs the consensus module.
    pub node: NodeImpl,
    /// Who runs the container.
    pub service: ServiceImpl,
    /// Which driver.
    pub driver: DriverImpl,
    /// A backup member. Refused today: `ClusterBackup` is P3-9.
    pub backup: bool,
    /// How the container is started.
    pub mode: ProcessMode,
    /// `aeron.cluster.service.responder` — whether this container sends egress
    /// while its node is the leader (the reference's default is `true`).
    pub responder: bool,
}

impl MemberSpec {
    /// Our container running `service` on a Java node.
    #[must_use]
    pub const fn our_container(id: i32, service: &'static str, mode: ProcessMode) -> Self {
        Self {
            id,
            service_id: 0,
            node: NodeImpl::Java,
            service: ServiceImpl::Rust(service),
            driver: DriverImpl::Java,
            backup: false,
            mode,
            responder: true,
        }
    }

    /// The reference's container running the Java class `class` on a Java node.
    #[must_use]
    pub const fn java_container(id: i32, class: &'static str) -> Self {
        Self {
            id,
            service_id: 0,
            node: NodeImpl::Java,
            service: ServiceImpl::Java(class),
            driver: DriverImpl::Java,
            backup: false,
            mode: ProcessMode::External,
            responder: true,
        }
    }

    /// Our test business on a Java node, in process.
    ///
    /// `service` is `test-service` (echo, plus the reference's test-business
    /// observables) or `sequence-service` (the same echo, and every session
    /// message's first eight bytes checked against that session's next
    /// sequence number).
    #[must_use]
    pub const fn test_container(id: i32, service: &'static str) -> Self {
        Self {
            id,
            service_id: 0,
            node: NodeImpl::Java,
            service: ServiceImpl::Test(service),
            driver: DriverImpl::Java,
            backup: false,
            mode: ProcessMode::InProcess,
            responder: true,
        }
    }

    /// A container with its own service id on the same node.
    #[must_use]
    pub const fn with_service_id(mut self, service_id: i32) -> Self {
        self.service_id = service_id;
        self
    }

    /// Whether this container sends egress (`aeron.cluster.service.responder`).
    #[must_use]
    pub const fn with_responder(mut self, responder: bool) -> Self {
        self.responder = responder;
        self
    }
}

/// The cluster-wide settings a scenario does not vary.
#[derive(Clone, Debug)]
pub struct Options {
    /// `aeron.cluster.id`.
    pub cluster_id: i32,
    /// `aeron.cluster.control.channel`: both S2 streams ride it.
    pub control_channel: String,
    /// `aeron.cluster.service.stream.id`.
    pub service_stream_id: i32,
    /// `aeron.cluster.consensus.module.stream.id`.
    pub consensus_module_stream_id: i32,
    /// The client-facing ingress stream.
    pub ingress_stream_id: i32,
    /// The client-facing egress stream.
    pub egress_stream_id: i32,
    /// `aeron.cluster.replay.channel`.
    pub replay_channel: String,
    /// `aeron.cluster.replay.stream.id`.
    pub replay_stream_id: i32,
    /// `aeron.cluster.snapshot.channel`.
    pub snapshot_channel: String,
    /// `aeron.cluster.snapshot.stream.id`.
    pub snapshot_stream_id: i32,
    /// `aeron.cluster.ingress.channel`.
    pub ingress_channel: String,
    /// `aeron.cluster.consensus.channel`.
    pub consensus_channel: String,
    /// `aeron.cluster.log.channel`.
    pub log_channel: String,
    /// `aeron.cluster.members`; `None` is the single-member string.
    pub members: Option<String>,
    /// The Java probe sources to compile (`java/<name>.java`).
    pub probes: Vec<&'static str>,
    /// Extra `-D` properties for the Java node — a scenario that needs a
    /// different session timeout, say. Applied last, so a file's value is
    /// overridden the way a JVM's `-D` is.
    pub node_properties: Vec<(String, String)>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            cluster_id: 7,
            control_channel: "aeron:ipc?term-length=128k".to_owned(),
            service_stream_id: 104,
            consensus_module_stream_id: 105,
            ingress_stream_id: 101,
            egress_stream_id: 102,
            replay_channel: "aeron:ipc".to_owned(),
            replay_stream_id: 103,
            snapshot_channel: "aeron:ipc?alias=snapshot".to_owned(),
            snapshot_stream_id: 106,
            ingress_channel: "aeron:ipc".to_owned(),
            consensus_channel: "aeron:ipc".to_owned(),
            log_channel: "aeron:ipc".to_owned(),
            members: None,
            probes: Vec::new(),
            node_properties: Vec::new(),
        }
    }
}

impl Options {
    /// The members string, defaulting to one member with unused endpoints.
    #[must_use]
    pub fn members_string(&self) -> String {
        self.members.clone().unwrap_or_else(|| {
            "0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0".to_owned()
        })
    }
}

/// The reference's port scheme for a multi-node cluster: every member owns a
/// block of [`PORTS_PER_NODE`], with the archive control channel, the client
/// facing ingress, the member facing consensus and the log at fixed offsets
/// inside it (`ClusterConfig.java:42-67`).
///
/// A cluster and its client must agree on these numbers twice over: the members
/// are told `id,ingress,consensus,log,catchup,archive` (which is how a member
/// binds its endpoints), and a client that names `ingressEndpoints` is told
/// `id=endpoint` instead. Both spellings are produced here so a scenario
/// cannot get one of them from a different base than the other.
#[derive(Clone, Copy, Debug)]
pub struct ClusterPorts {
    base: u16,
}

/// `ClusterConfig.PORTS_PER_NODE`.
pub const PORTS_PER_NODE: u16 = 100;
/// The offset of a member's archive control port (`ClusterConfig.java:47`).
pub const ARCHIVE_CONTROL_PORT_OFFSET: u16 = 1;
/// The offset of a member's ingress port (`:52`).
pub const CLIENT_FACING_PORT_OFFSET: u16 = 2;
/// The offset of a member's consensus port (`:57`).
pub const MEMBER_FACING_PORT_OFFSET: u16 = 3;
/// The offset of a member's log port (`:62`).
pub const LOG_PORT_OFFSET: u16 = 4;

impl ClusterPorts {
    /// A cluster whose first member's block starts at `base`.
    #[must_use]
    pub const fn new(base: u16) -> Self {
        Self { base }
    }

    /// One port in a member's block.
    #[must_use]
    pub fn endpoint(&self, member: i32, offset: u16) -> String {
        format!(
            "localhost:{}",
            self.base + u16::try_from(member).expect("a member id") * PORTS_PER_NODE + offset
        )
    }

    /// Where a member's clients connect (`:52`).
    #[must_use]
    pub fn ingress_endpoint(&self, member: i32) -> String {
        self.endpoint(member, CLIENT_FACING_PORT_OFFSET)
    }

    /// The reference's `aeron.cluster.members`:
    /// `id,ingress,consensus,log,catchup,archive` per member, `|` between them.
    ///
    /// The catch-up endpoint is left at port zero: the reference's own scheme
    /// assigns it from the same block, and nothing in these scenarios uses it.
    #[must_use]
    pub fn members_string(&self, members: i32) -> String {
        (0..members)
            .map(|member| {
                format!(
                    "{member},{},{},{},localhost:0,{}",
                    self.endpoint(member, CLIENT_FACING_PORT_OFFSET),
                    self.endpoint(member, MEMBER_FACING_PORT_OFFSET),
                    self.endpoint(member, LOG_PORT_OFFSET),
                    self.endpoint(member, ARCHIVE_CONTROL_PORT_OFFSET),
                )
            })
            .collect::<Vec<_>>()
            .join("|")
    }

    /// What a client's `ingressEndpoints` names: `id=endpoint`, comma
    /// separated (`AeronCluster.parseIngressEndpoints`, `AC:735-756`).
    #[must_use]
    pub fn ingress_endpoints(&self, members: i32) -> String {
        (0..members)
            .map(|member| format!("{member}={}", self.ingress_endpoint(member)))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// A running cluster of members whose containers this fixture owns.
pub struct TestCluster {
    nodes: Vec<Node>,
    containers: Vec<ContainerSlot>,
    options: Options,
    jar: PathBuf,
    classes: TempDir,
}

/// One node: its directories, its process, and how many containers it expects.
struct Node {
    id: i32,
    aeron: TempDir,
    cluster: TempDir,
    archive: TempDir,
    service_count: i32,
    process: Option<Process>,
}

/// One container of the table, attached to a node.
struct ContainerSlot {
    spec: MemberSpec,
    node: usize,
    container: Option<Container>,
    /// The observables of a [`ServiceImpl::Test`] business, for the test that
    /// asked for it.
    test_state: Option<Arc<TestServiceState>>,
}

/// How a container ended.
#[derive(Debug)]
pub enum ContainerExit {
    /// It ended the way the reference calls orderly (a termination, or a
    /// clean `SIGTERM`).
    Ok,
    /// Its duty cycle failed; the message is what the agent printed.
    Failed(String),
    /// A process's exit status, for an external container.
    Exited(ExitStatus),
}

/// A container the fixture started, in whichever shape its mode asked for.
enum Container {
    InProcess {
        shutdown: ShutdownHandle,
        thread: std::thread::JoinHandle<Result<(), String>>,
        terminated: Arc<std::sync::atomic::AtomicBool>,
    },
    External(Process),
    Java(Process),
}

impl TestCluster {
    /// Start every member of `members`, or `None` when the reference checkout,
    /// its jar, `javac` or a probe is missing (the suite's way of skipping).
    ///
    /// # Panics
    ///
    /// When a member's configuration is one this fixture refuses (`backup`,
    /// [`NodeImpl::Rust`], [`DriverImpl::Deepmsg`]) or when a node or container
    /// does not start.
    pub fn start(test_name: &str, members: &[MemberSpec], options: Options) -> Option<Self> {
        assert!(!members.is_empty(), "a cluster needs at least one member");
        for member in members {
            assert!(
                !member.backup,
                "member {} asks for a backup: ClusterBackup is P3-9, and this fixture carries \
                 the field so that card does not have to change the table",
                member.id
            );
            assert_eq!(
                member.node,
                NodeImpl::Java,
                "member {} asks for our consensus module: that is P3-6a's `cluster_harness`",
                member.id
            );
            assert_eq!(
                member.driver,
                DriverImpl::Java,
                "member {} asks for our driver: that is P3-7's mixed matrix",
                member.id
            );
            assert!(
                !(matches!(member.service, ServiceImpl::Java(_))
                    && member.mode != ProcessMode::External),
                "member {} asks for a Java container in process: a Java object cannot live in \
                 this process",
                member.id
            );
            assert!(
                !(matches!(member.service, ServiceImpl::Test(_))
                    && member.mode != ProcessMode::InProcess),
                "member {} asks for a test business as a process: the binary has no such name, \
                 and this scenario records that loss (`n/a:external-injection`) rather than \
                 teaching the product to fail on demand",
                member.id
            );
        }

        let jar = driver::locate_aeron_all()?;
        let classes = compile_probes(&options.probes)?;

        let mut nodes: Vec<Node> = Vec::new();
        for member in members {
            if nodes.iter().any(|node| node.id == member.id) {
                continue;
            }
            nodes.push(Node {
                id: member.id,
                aeron: TempDir::new(&format!("{test_name}-{}-aeron", member.id)),
                cluster: TempDir::new(&format!("{test_name}-{}-cluster", member.id)),
                archive: TempDir::new(&format!("{test_name}-{}-archive", member.id)),
                service_count: i32::try_from(
                    members.iter().filter(|other| other.id == member.id).count(),
                )
                .expect("a node's service count fits an i32"),
                process: None,
            });
        }

        let containers = members
            .iter()
            .map(|spec| {
                let node = nodes
                    .iter()
                    .position(|candidate| candidate.id == spec.id)
                    .expect("every spec's node was created");
                ContainerSlot {
                    spec: *spec,
                    node,
                    container: None,
                    test_state: None,
                }
            })
            .collect();

        let mut cluster = Self {
            nodes,
            containers,
            options,
            jar,
            classes,
        };

        cluster.start_all();
        Some(cluster)
    }

    /// The reference's `aeron-all` jar the probes run from.
    #[must_use]
    pub fn jar(&self) -> &Path {
        &self.jar
    }

    /// The directory the compiled probes live in.
    #[must_use]
    pub fn classes(&self) -> &Path {
        self.classes.path()
    }

    /// A node's driver directory (`aeron.dir`).
    #[must_use]
    pub fn aeron_dir(&self, member: i32) -> &Path {
        self.node(member).aeron.path()
    }

    /// A node's cluster directory (`aeron.cluster.dir`).
    #[must_use]
    pub fn cluster_dir(&self, member: i32) -> &Path {
        self.node(member).cluster.path()
    }

    /// A client onto the first node's driver, waiting for its CnC file.
    pub fn client(&self) -> Client {
        let aeron_dir = self.nodes[0].aeron.path();
        let deadline = Instant::now() + DEADLINE;
        loop {
            match Client::connect(aeron_dir) {
                Ok(client) => return client,
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(error) => panic!(
                    "no client onto the Java driver: {error}\nnode log:\n{}",
                    self.node_log(self.nodes[0].id)
                ),
            }
        }
    }

    /// Wait until the cluster is ACTIVE — which, with services, also means every
    /// container has joined and acked its log.
    pub fn await_active(&self) {
        let client = self.client();
        let deadline = Instant::now() + DEADLINE;
        while Instant::now() < deadline {
            if counter_value(
                &client,
                CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
                self.options.cluster_id,
            ) == Some(1)
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "the cluster never became ACTIVE\nnode log:\n{}\n{}",
            self.node_log(self.nodes[0].id),
            self.container_logs()
        );
    }

    /// One of the cluster's counters, by type id, on the first node.
    #[must_use]
    pub fn counter(&self, type_id: i32) -> Option<i64> {
        counter_value(&self.client(), type_id, self.options.cluster_id)
    }

    /// The same counter read on one member's driver — the only way to ask a
    /// multi-node cluster which member leads (`Cluster.Role` is counter 201,
    /// per node).
    #[must_use]
    pub fn counter_on(&self, member: i32, type_id: i32) -> Option<i64> {
        let client = Client::connect(self.aeron_dir(member)).ok()?;
        counter_value(&client, type_id, self.options.cluster_id)
    }

    /// The member whose role counter says LEADER, other than `excluded`.
    ///
    /// The connect timeout is short on purpose: a member that has been stopped
    /// leaves a directory whose heartbeat ages out, and `Client::connect` would
    /// spend its whole timeout discovering that — long enough that a caller
    /// waiting here stops polling its own client, which is that client's
    /// liveness window on another driver.
    #[must_use]
    pub fn leader_other_than(&self, excluded: i32) -> Option<i32> {
        self.nodes
            .iter()
            .map(|node| node.id)
            .filter(|member| *member != excluded)
            .find(|member| {
                Client::connect_with_timeout(self.aeron_dir(*member), Duration::from_millis(100))
                    .ok()
                    .and_then(|client| {
                        let counters = client.counters_reader()?;
                        let id = counters::find_cluster_counter(
                            &counters,
                            CLUSTER_NODE_ROLE_TYPE_ID,
                            self.options.cluster_id,
                        )?;
                        counters.value(id)
                    })
                    == Some(ROLE_LEADER)
            })
    }

    /// The member whose role counter says LEADER, once one does.
    ///
    /// A member whose node was stopped is skipped: its driver's CnC file can
    /// still carry a heartbeat inside the client's ten-second liveness window,
    /// so a connect would succeed and read the role it had before it died —
    /// which is how a test that stopped the leader saw it "still leading".
    pub fn await_leader(&self) -> i32 {
        let deadline = Instant::now() + DEADLINE;
        while Instant::now() < deadline {
            for node in &self.nodes {
                if node.process.is_none() {
                    continue;
                }
                if self.counter_on(node.id, CLUSTER_NODE_ROLE_TYPE_ID) == Some(ROLE_LEADER) {
                    return node.id;
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "no member ever became the leader\n{}",
            self.container_logs()
        );
    }

    /// Run one of the compiled probes and hand back what it printed.
    pub fn run_probe(&self, class: &str, args: &[&str]) -> String {
        java::run_probe(&self.jar, self.classes.path(), class, args)
    }

    /// Stop one container, asserting the way it was asked to stop.
    pub fn stop_container(&mut self, member: i32, service_id: i32) {
        let index = self.slot_index(member, service_id);
        if let Some(container) = self.containers[index].container.take() {
            match container {
                Container::InProcess {
                    shutdown,
                    thread,
                    terminated,
                } => {
                    shutdown.stop();
                    match thread.join() {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            panic!(
                                "member {member} service {service_id}'s container failed: {error}"
                            )
                        }
                        Err(_) => panic!("member {member} service {service_id}'s thread panicked"),
                    }
                    assert!(
                        terminated.load(std::sync::atomic::Ordering::Acquire),
                        "member {member} service {service_id}'s `on_terminate` ran when the \
                         container was stopped"
                    );
                }
                Container::External(mut process) => {
                    let status = process.stop().expect("the container stops");
                    assert!(
                        status.success(),
                        "member {member} service {service_id}'s container exited with \
                         {status}\ncontainer log:\n{}",
                        process.log_tail(80)
                    );
                }
                Container::Java(mut process) => {
                    let status = process.stop().expect("the Java container stops");
                    assert!(
                        status.success() || status.code() == Some(143),
                        "member {member} service {service_id}'s Java container exited with \
                         {status}\ncontainer log:\n{}",
                        process.log_tail(80)
                    );
                }
            }
        }
    }

    /// Whether a container is still running.
    pub fn container_is_running(&mut self, member: i32, service_id: i32) -> bool {
        let index = self.slot_index(member, service_id);
        match &mut self.containers[index].container {
            Some(Container::InProcess { thread, .. }) => !thread.is_finished(),
            Some(Container::External(process)) | Some(Container::Java(process)) => process
                .try_wait()
                .expect("the container is waitable")
                .is_none(),
            None => false,
        }
    }

    /// Wait until a container has ended, and answer how.
    ///
    /// Ends the fixture's ownership of it: a later [`Self::stop_container`] is a
    /// no-op, which is what a scenario about a container that dies on its own
    /// needs.
    pub fn await_container_exit(&mut self, member: i32, service_id: i32) -> ContainerExit {
        let index = self.slot_index(member, service_id);
        let deadline = Instant::now() + DEADLINE;

        loop {
            let exit = match self.containers[index].container.as_mut() {
                Some(Container::InProcess { thread, .. }) => thread.is_finished().then_some(()),
                Some(Container::External(process)) | Some(Container::Java(process)) => process
                    .try_wait()
                    .expect("the container is waitable")
                    .map(|_| ()),
                None => panic!("member {member} service {service_id}'s container was not started"),
            };

            if exit.is_some() {
                let container = self.containers[index].container.take();
                return match container {
                    Some(Container::InProcess { thread, .. }) => match thread.join() {
                        Ok(Ok(())) => ContainerExit::Ok,
                        Ok(Err(error)) => ContainerExit::Failed(error),
                        Err(_) => ContainerExit::Failed("the container thread panicked".to_owned()),
                    },
                    Some(Container::External(mut process)) | Some(Container::Java(mut process)) => {
                        let status = process.stop().expect("the process answers");
                        ContainerExit::Exited(status)
                    }
                    None => unreachable!("the container was there a moment ago"),
                };
            }

            assert!(
                Instant::now() < deadline,
                "member {member} service {service_id}'s container never ended\n{}",
                self.container_logs()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Start one container again over the same directories.
    pub fn start_container(&mut self, member: i32, service_id: i32) {
        let index = self.slot_index(member, service_id);
        let (container, test_state) = self.spawn_container(index);
        self.containers[index].container = Some(container);
        self.containers[index].test_state = test_state;
    }

    /// Stop every container of a node and the node, then start both again over
    /// the same directories — what a recovery plan is built from is the on-disk
    /// `recording.log`, and the recovery-state counter is allocated once, at
    /// that startup.
    ///
    /// A test that needs to read that counter should use
    /// [`Self::restart_node_without_containers`]: the module holds the counter
    /// only while it waits for the services to join and ack, and a restarted
    /// container acks the moment it is up.
    pub fn restart_node(&mut self, member: i32) {
        self.restart_node_without_containers(member);
        let slots: Vec<usize> = self
            .containers
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.spec.id == member)
            .map(|(index, _)| index)
            .collect();
        for index in slots {
            let (container, test_state) = self.spawn_container(index);
            self.containers[index].container = Some(container);
            self.containers[index].test_state = test_state;
        }
    }

    /// Stop one member's node. Its containers are the caller's to stop first:
    /// a container outliving its node ends by itself, and a test that wants to
    /// see the end should watch it.
    pub fn stop_node(&mut self, member: i32) {
        let index = self.node_index(member);
        if let Some(mut process) = self.nodes[index].process.take() {
            let status = process.stop().expect("the Java node stops");
            assert!(
                status.success() || status.code() == Some(143),
                "member {member}'s Java node exited with {status}\nnode log:\n{}",
                process.log_tail(80)
            );
        }
    }

    /// The half of [`Self::restart_node`] that stops the containers and the
    /// node, then starts the node **alone**.
    ///
    /// The recovery-state counter lives only from the module's startup until
    /// every service has acked (`ConsensusModuleAgent.java:3505-3520`), so a
    /// test that reads a snapshot's recording id out of it has to do that while
    /// the services are still down.
    pub fn restart_node_without_containers(&mut self, member: i32) {
        let slots: Vec<usize> = self
            .containers
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.spec.id == member)
            .map(|(index, _)| index)
            .collect();
        for index in &slots {
            let slot = &self.containers[*index];
            let (member, service_id) = (slot.spec.id, slot.spec.service_id);
            self.stop_container(member, service_id);
        }

        let node_index = self.node_index(member);
        if let Some(mut process) = self.nodes[node_index].process.take() {
            let status = process.stop().expect("the Java node stops");
            assert!(
                status.success() || status.code() == Some(143),
                "member {member}'s Java node exited with {status}\nnode log:\n{}",
                process.log_tail(80)
            );
        }
        self.start_node(node_index);
    }

    /// Stop every container, then every node.
    pub fn shutdown(&mut self) {
        let slots: Vec<(i32, i32)> = self
            .containers
            .iter()
            .map(|slot| (slot.spec.id, slot.spec.service_id))
            .collect();
        for (member, service_id) in slots {
            self.stop_container(member, service_id);
        }

        for node in &mut self.nodes {
            if let Some(mut process) = node.process.take() {
                let status = process.stop().expect("the Java node stops");
                assert!(
                    status.success() || status.code() == Some(143),
                    "member {}'s Java node exited with {status}\nnode log:\n{}",
                    node.id,
                    process.log_tail(80)
                );
            }
        }
    }

    /// The last lines of a node's output.
    #[must_use]
    pub fn node_log(&self, member: i32) -> String {
        self.node(member)
            .process
            .as_ref()
            .map_or_else(String::new, |process| process.log_tail(80))
    }

    /// The last lines of every container's output, for a failure message.
    #[must_use]
    pub fn container_logs(&self) -> String {
        self.containers
            .iter()
            .filter_map(|slot| match &slot.container {
                Some(Container::External(process)) | Some(Container::Java(process)) => {
                    Some(format!(
                        "member {} service {} container log:\n{}",
                        slot.spec.id,
                        slot.spec.service_id,
                        process.log_tail(60)
                    ))
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The config a container runs with.
    #[must_use]
    pub fn container_config(&self, member: i32, service_id: i32) -> ServiceContainerConfig {
        let index = self.slot_index(member, service_id);
        let slot = &self.containers[index];
        let node = &self.nodes[slot.node];

        ServiceContainerConfig {
            cluster_id: self.options.cluster_id,
            service_id: slot.spec.service_id,
            service_name: deepmsg_cluster_service::config::container_thread_name(
                self.options.cluster_id,
                slot.spec.service_id,
                deepmsg_archive::server::config::ThreadNaming::Classic,
            ),
            service: match slot.spec.service {
                ServiceImpl::Rust(name) | ServiceImpl::Test(name) => Some((*name).to_owned()),
                ServiceImpl::Java(class) => Some((*class).to_owned()),
            },
            thread_naming: deepmsg_archive::server::config::ThreadNaming::Classic,
            aeron_dir: node.aeron.path().to_path_buf(),
            cluster_dir: node.cluster.path().to_path_buf(),
            mark_file_dir: None,
            control_channel: self.options.control_channel.clone(),
            service_stream_id: self.options.service_stream_id,
            consensus_module_stream_id: self.options.consensus_module_stream_id,
            log_fragment_limit: 50,
            responding_service: slot.spec.responder,
            error_buffer_length: 1024 * 1024,
            idle_strategy: "org.agrona.concurrent.BackoffIdleStrategy".to_owned(),
            app_version: 1,
            replay_channel: self.options.replay_channel.clone(),
            replay_stream_id: self.options.replay_stream_id,
            snapshot_channel: self.options.snapshot_channel.clone(),
            snapshot_stream_id: self.options.snapshot_stream_id,
            archive_local_control_channel: "aeron:ipc?term-length=64k".to_owned(),
            archive_local_control_stream_id: 10,
            archive_control_response_stream_id: self.options.cluster_id * 100
                + 100
                + 20
                + (slot.spec.service_id + 1),
            snapshot_duration_threshold_ns: 1_000_000_000,
            cycle_threshold_ns: 100_000_000,
        }
    }

    /// Every member's spec.
    pub fn members(&self) -> impl Iterator<Item = &MemberSpec> {
        self.containers.iter().map(|slot| &slot.spec)
    }

    /// A [`ServiceImpl::Test`] business's observables, or `None` when that
    /// container is not a test business.
    #[must_use]
    pub fn test_service(&self, member: i32, service_id: i32) -> Option<Arc<TestServiceState>> {
        let index = self.slot_index(member, service_id);
        self.containers[index].test_state.clone()
    }

    fn node_index(&self, member: i32) -> usize {
        self.nodes
            .iter()
            .position(|node| node.id == member)
            .unwrap_or_else(|| panic!("no member {member}"))
    }

    fn node(&self, member: i32) -> &Node {
        &self.nodes[self.node_index(member)]
    }

    fn slot_index(&self, member: i32, service_id: i32) -> usize {
        self.containers
            .iter()
            .position(|slot| slot.spec.id == member && slot.spec.service_id == service_id)
            .unwrap_or_else(|| panic!("no member {member} service {service_id}"))
    }

    fn start_all(&mut self) {
        for index in 0..self.nodes.len() {
            self.start_node(index);
        }
        for index in 0..self.containers.len() {
            let (container, test_state) = self.spawn_container(index);
            self.containers[index].container = Some(container);
            self.containers[index].test_state = test_state;
        }
    }

    /// Start one node (and wait for its driver's CnC). Its containers are the
    /// caller's to start.
    pub fn start_node_by_id(&mut self, member: i32) {
        let index = self.node_index(member);
        self.start_node(index);
    }

    fn start_node(&mut self, index: usize) {
        // A restart over the same `aeron.dir` would leave the previous driver's
        // CnC file in place, and a container started into that window attaches
        // to the dead one and never joins. The cluster's own state is in
        // `cluster.dir` and `archive.dir`; the driver's directory is the one
        // thing a restart is free to throw away.
        let _ = std::fs::remove_dir_all(self.nodes[index].aeron.path());

        let process = self.spawn_java_node(index);
        self.nodes[index].process = Some(process);
        self.await_driver_ready(index);
    }

    /// Wait until the restarted node's driver has published a CnC file a client
    /// can attach to, watching the process as we go.
    fn await_driver_ready(&mut self, index: usize) {
        let aeron_dir = self.nodes[index].aeron.path().to_path_buf();
        let member = self.nodes[index].id;
        let deadline = Instant::now() + DEADLINE;

        loop {
            if Client::connect(&aeron_dir).is_ok() {
                return;
            }
            if let Some(status) = self.nodes[index]
                .process
                .as_mut()
                .expect("the node is running")
                .try_wait()
                .expect("the node is waitable")
            {
                panic!(
                    "member {member}'s Java node exited with {status} before its CnC was ready:\n{}",
                    self.node_log(member)
                );
            }
            assert!(
                Instant::now() < deadline,
                "member {member}'s driver never published a CnC file\n{}",
                self.node_log(member)
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn spawn_java_node(&self, index: usize) -> Process {
        let node = &self.nodes[index];
        let mut args: Vec<String> = AGRONA_JVM_ARGS
            .iter()
            .map(|arg| (*arg).to_string())
            .collect();
        args.push(format!("-Daeron.dir={}", node.aeron.path().display()));
        args.push(format!(
            "-Daeron.cluster.dir={}",
            node.cluster.path().display()
        ));
        args.push(format!(
            "-Daeron.archive.dir={}",
            node.archive.path().display()
        ));
        args.push("-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:0".to_owned());
        args.push("-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0".to_owned());
        args.push(format!("-Daeron.cluster.id={}", self.options.cluster_id));
        args.push(format!("-Daeron.cluster.member.id={}", node.id));
        args.push(format!(
            "-Daeron.cluster.members={}",
            self.options.members_string()
        ));
        args.push(format!(
            "-Daeron.cluster.ingress.channel={}",
            self.options.ingress_channel
        ));
        args.push(format!(
            "-Daeron.cluster.consensus.channel={}",
            self.options.consensus_channel
        ));
        args.push(format!(
            "-Daeron.cluster.log.channel={}",
            self.options.log_channel
        ));
        args.push(format!(
            "-Daeron.cluster.control.channel={}",
            self.options.control_channel
        ));
        args.push("-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0".to_owned());
        args.push(format!(
            "-Daeron.cluster.service.count={}",
            node.service_count
        ));
        args.push("-Daeron.threading.mode=SHARED".to_owned());
        args.push("-Daeron.print.configuration=false".to_owned());
        for (name, value) in &self.options.node_properties {
            args.push(format!("-D{name}={value}"));
        }
        args.push("-cp".to_owned());
        // The probes are on the node's classpath too: a Java node loads its
        // authenticator supplier **by class name** from a property
        // (`ConsensusModule.java:807`), which is how a scenario gives a
        // three-node cluster a challenge/response authenticator without a
        // custom node main.
        args.push(format!(
            "{}:{}",
            self.jar.display(),
            self.classes.path().display()
        ));
        args.push("io.aeron.cluster.ClusteredMediaDriver".to_owned());

        let log = node.cluster.path().join("java-node.log");
        Process::spawn(Path::new("java"), &log, &args, &[])
            .unwrap_or_else(|error| panic!("member {}'s Java node did not start: {error}", node.id))
    }

    fn spawn_container(&self, index: usize) -> (Container, Option<Arc<TestServiceState>>) {
        let slot = &self.containers[index];
        match slot.spec.service {
            ServiceImpl::Rust(name) => {
                let service = services::by_name(name).unwrap_or_else(|| {
                    panic!(
                        "member {} service {} asks for a business this build does not have: {name}",
                        slot.spec.id, slot.spec.service_id
                    )
                });
                let container = match slot.spec.mode {
                    ProcessMode::InProcess => self.spawn_in_process(index, service),
                    ProcessMode::External => {
                        Container::External(self.spawn_external_container(index))
                    }
                };
                (container, None)
            }
            ServiceImpl::Test(name) => {
                assert!(
                    matches!(
                        name,
                        "test-service" | "sequence-service" | "sequenced-egress"
                    ),
                    "no test business called `{name}`; the fixture has `test-service`, \
                     `sequence-service` and `sequenced-egress`"
                );
                let state = Arc::new(TestServiceState::default());
                let service: Box<dyn ClusteredService + Send> = if name == "sequenced-egress" {
                    Box::new(SequencedEgressService::new(Arc::clone(&state)))
                } else {
                    Box::new(TestService::new(
                        Arc::clone(&state),
                        name == "sequence-service",
                    ))
                };
                let container = self.spawn_in_process(index, service);
                (container, Some(state))
            }
            ServiceImpl::Java(class) => (
                Container::Java(self.spawn_java_container(index, class)),
                None,
            ),
        }
    }

    fn spawn_in_process(
        &self,
        index: usize,
        service: Box<dyn ClusteredService + Send>,
    ) -> Container {
        let slot = &self.containers[index];
        let member_id = slot.spec.id;
        let service_id = slot.spec.service_id;
        let config = self.container_config(member_id, service_id);
        let terminated = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&terminated);
        let observed = Observed {
            inner: service,
            terminated: flag,
        };
        let (started_tx, started_rx) = mpsc::channel();

        let thread = std::thread::spawn(move || -> Result<(), String> {
            // The reference's in-process container is given an Aeron client it
            // does not own (`ownsAeronClient=false`): when this agent ends, the
            // client's counters stay in the driver and the consensus module
            // does not see a service leave. Closing it here would take the
            // node's driver side down with the failed service, which is the
            // bug `cluster_lc4a_snapshot_failure` caught.
            let mut agent = match ClusteredServiceAgent::launch_with_client_ownership(
                config, observed, false,
            ) {
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
                        eprintln!("member {member_id} service {service_id} stopped: {message}");
                        break;
                    }
                    Err(error) => {
                        eprintln!("member {member_id} service {service_id} failed: {error}");
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
            .unwrap_or_else(|error| {
                panic!("member {member_id} service {service_id}'s container did not start: {error}")
            });

        Container::InProcess {
            shutdown,
            thread,
            terminated,
        }
    }

    fn spawn_external_container(&self, index: usize) -> Process {
        let slot = &self.containers[index];
        let (member_id, service_id) = (slot.spec.id, slot.spec.service_id);
        let binary = driver::locate_tool(CONTAINER_ENV, DEFAULT_CONTAINER).unwrap_or_else(|| {
            panic!(
                "no {DEFAULT_CONTAINER}; build it with `cargo build --workspace --bins` or set \
                 {CONTAINER_ENV}"
            )
        });
        let config = self.container_config(member_id, service_id);
        let mut args = vec![
            format!("-Daeron.dir={}", config.aeron_dir.display()),
            format!("-Daeron.cluster.dir={}", config.cluster_dir.display()),
            format!("-Daeron.cluster.id={}", config.cluster_id),
            format!("-Daeron.cluster.service.id={}", config.service_id),
            format!(
                "-Ddeepmsg.cluster.service={}",
                config.service.clone().unwrap_or_default()
            ),
            format!("-Daeron.cluster.control.channel={}", config.control_channel),
            format!(
                "-Daeron.cluster.service.stream.id={}",
                config.service_stream_id
            ),
            format!(
                "-Daeron.cluster.consensus.module.stream.id={}",
                config.consensus_module_stream_id
            ),
            format!("-Daeron.cluster.replay.channel={}", config.replay_channel),
            format!(
                "-Daeron.cluster.replay.stream.id={}",
                config.replay_stream_id
            ),
            format!(
                "-Daeron.cluster.snapshot.channel={}",
                config.snapshot_channel
            ),
            format!(
                "-Daeron.cluster.snapshot.stream.id={}",
                config.snapshot_stream_id
            ),
            format!(
                "-Daeron.archive.local.control.channel={}",
                config.archive_local_control_channel
            ),
            format!(
                "-Daeron.archive.local.control.stream.id={}",
                config.archive_local_control_stream_id
            ),
            format!(
                "-Daeron.cluster.service.responder={}",
                config.responding_service
            ),
            format!(
                "-Daeron.cluster.log.fragment.limit={}",
                config.log_fragment_limit
            ),
        ];
        args.push(format!(
            "-Daeron.archive.control.response.stream.id={}",
            config.archive_control_response_stream_id
                - (config.cluster_id * 100 + 100 + (config.service_id + 1))
        ));

        let log = self.nodes[slot.node]
            .cluster
            .path()
            .join(format!("our-service-{service_id}.log"));
        let process = Process::spawn(&binary, &log, &args, &[]).unwrap_or_else(|error| {
            panic!("member {member_id} service {service_id}'s container did not start: {error}")
        });
        self.await_container_ready(index, process)
    }

    fn spawn_java_container(&self, index: usize, class: &'static str) -> Process {
        let slot = &self.containers[index];
        let (member_id, service_id) = (slot.spec.id, slot.spec.service_id);
        let node = &self.nodes[slot.node];
        let mut args: Vec<String> = AGRONA_JVM_ARGS
            .iter()
            .map(|arg| (*arg).to_string())
            .collect();
        args.push("-cp".to_owned());
        args.extend([
            format!("{}:{}", self.jar.display(), self.classes.path().display()),
            "ClusterServiceContainerProbe".to_owned(),
            node.aeron.path().display().to_string(),
            node.cluster.path().display().to_string(),
            self.options.cluster_id.to_string(),
            service_id.to_string(),
            class.to_owned(),
            slot.spec.responder.to_string(),
            self.options.control_channel.clone(),
        ]);

        let log = node
            .cluster
            .path()
            .join(format!("java-service-{service_id}.log"));
        let process = Process::spawn(Path::new("java"), &log, &args, &[]).unwrap_or_else(|error| {
            panic!(
                "member {member_id} service {service_id}'s Java container did not start: {error}"
            )
        });
        self.await_container_ready(index, process)
    }

    /// Wait for an external (our binary or Java) container's mark file to say
    /// it is up, watching the process as we go.
    fn await_container_ready(&self, index: usize, mut process: Process) -> Process {
        let slot = &self.containers[index];
        let (member_id, service_id) = (slot.spec.id, slot.spec.service_id);
        let path = self
            .container_config(member_id, service_id)
            .mark_file_path();
        let deadline = Instant::now() + DEADLINE;

        loop {
            if let Ok(mark) = ClusterMarkFile::open(&path) {
                let now = epoch_millis();
                let ready = mark.component_type() == Some(ClusterComponentType::CONTAINER)
                    && mark.service_id() == Some(service_id)
                    && mark.cluster_id() == Some(self.options.cluster_id)
                    && mark.version() == Some(SEMANTIC_VERSION)
                    && ClusterMarkFile::is_active(&path, now, 10_000);
                // Both, not either: the mark file is signalled by `launch`
                // before the agent has recovered, and a container stopped in
                // that window is killed by the signal instead of handling it —
                // the process's own `STARTED` line is the point after which a
                // stop is orderly.
                if ready && process.log_contains("STARTED") {
                    return process;
                }
            }

            if let Some(status) = process.try_wait().expect("the container is waitable") {
                panic!(
                    "member {member_id} service {service_id}'s container exited with {status} \
                     before its mark file was ready\ncontainer log:\n{}",
                    process.log_tail(80)
                );
            }
            assert!(
                Instant::now() < deadline,
                "member {member_id} service {service_id}'s container never signalled ready in its \
                 mark file ({})\ncontainer log:\n{}",
                path.display(),
                process.log_tail(80)
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// What a [`TestService`] did, as a test can read it from its own process.
///
/// These are the reference's `TestNode.TestService` observables
/// (`messageCount`, the session counters, `failNextSnapshot`,
/// `isTerminated`) — the *stub-service* assertion surface the plan's Q4c
/// counted. They exist only in the in-process arm, which is the point: an
/// external container's business state is not readable, and a scenario that
/// needs it records that loss rather than pretending.
#[derive(Default)]
pub struct TestServiceState {
    messages: AtomicUsize,
    session_opens: AtomicUsize,
    session_closes: AtomicUsize,
    close_reasons: Mutex<Vec<CloseReason>>,
    fail_next_snapshot: AtomicBool,
    /// `TestNode.TestService.wasSnapshotTaken` (`TestNode.java`): a snapshot of
    /// this business was written. The reference's backup cases read it after a
    /// cluster shutdown, whose snapshots are what the backup has to carry.
    snapshot_taken: AtomicBool,
    /// The same business was handed a snapshot on `on_start` — which is the
    /// assertion behind "a node started from a backup directory came up from
    /// the snapshot", not merely "the backup counted one".
    snapshot_loaded: AtomicBool,
    terminated: AtomicBool,
    /// The messages whose first eight bytes were not the session's next
    /// sequence number — a gap, a repeat, or a payload too short to hold one.
    ///
    /// The reference's `SequenceCheckingService` counts the same thing
    /// (`ClusterSessionReliabilityTest.java:267-309`), and it is the assertion
    /// two kinds of case need: "the session was closed before messages could be
    /// lost silently" and "a failover neither lost nor repeated one".
    out_of_sequence: AtomicUsize,
    /// The next sequence number the business expects from each session.
    next_sequence: Mutex<HashMap<SessionId, u64>>,
    /// Messages a timer-driven business offered, which no client action drove.
    offered: AtomicUsize,
}

impl TestServiceState {
    /// Every session message and service message the business saw.
    #[must_use]
    pub fn messages(&self) -> usize {
        self.messages.load(Ordering::Acquire)
    }

    /// Messages that were not the session's next sequence number.
    #[must_use]
    pub fn out_of_sequence(&self) -> usize {
        self.out_of_sequence.load(Ordering::Acquire)
    }

    /// Messages a timer-driven business offered (`sequenced-egress`).
    #[must_use]
    pub fn offered(&self) -> usize {
        self.offered.load(Ordering::Acquire)
    }

    /// `on_session_open` calls.
    #[must_use]
    pub fn session_opens(&self) -> usize {
        self.session_opens.load(Ordering::Acquire)
    }

    /// `on_session_close` calls.
    #[must_use]
    pub fn session_closes(&self) -> usize {
        self.session_closes.load(Ordering::Acquire)
    }

    /// The reasons of those closes, in order (`CloseReason::CLOSE` for a
    /// client that closed, `TIMEOUT`/`SESSION_TIMEOUT` for one that vanished).
    #[must_use]
    pub fn close_reasons(&self) -> Vec<CloseReason> {
        self.close_reasons.lock().expect("the reasons lock").clone()
    }

    /// Whether a snapshot of this business was taken
    /// (`TestService.wasSnapshotTaken`).
    #[must_use]
    pub fn was_snapshot_taken(&self) -> bool {
        self.snapshot_taken.load(Ordering::Acquire)
    }

    /// Whether `on_start` was given a snapshot to recover from
    /// (`TestService.wasSnapshotLoaded`).
    #[must_use]
    pub fn was_snapshot_loaded(&self) -> bool {
        self.snapshot_loaded.load(Ordering::Acquire)
    }

    /// Make the next snapshot fail (`TestService.failNextSnapshot`).
    pub fn fail_next_snapshot(&self) {
        self.fail_next_snapshot.store(true, Ordering::Release);
    }

    /// Whether `on_terminate` ran.
    #[must_use]
    pub fn was_terminated(&self) -> bool {
        self.terminated.load(Ordering::Acquire)
    }
}

/// The reference's `TestNode.TestService`, as far as LC-4a needs it: the echo
/// semantics of the product business, plus the counters and the snapshot
/// failure the reference's test business carries.
struct TestService {
    inner: deepmsg_cluster_service::EchoService,
    state: Arc<TestServiceState>,
    /// Whether the first eight bytes of every session message are that
    /// session's sequence number, little endian.
    ///
    /// On only for the fixture's `sequence-service`: the other cases send
    /// whatever they like, and reading "hello cluster" as a sequence would
    /// count a gap on every message.
    check_sequence: bool,
}

impl TestService {
    fn new(state: Arc<TestServiceState>, check_sequence: bool) -> Self {
        Self {
            inner: deepmsg_cluster_service::EchoService::new(),
            state,
            check_sequence,
        }
    }

    /// `SequenceCheckingService.onSessionMessage` (`CSR:278-296`): the message's
    /// first eight bytes against what this session owes next.
    fn check_one_sequence(&self, session: SessionId, payload: &[u8]) {
        let sequence = payload
            .get(..8)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_le_bytes);

        let mut next = self.state.next_sequence.lock().expect("the sequence lock");
        let expected = next.entry(session).or_insert(0);

        if sequence != Some(*expected) {
            self.state.out_of_sequence.fetch_add(1, Ordering::AcqRel);
        }

        // A message with no sequence in it still moves nothing: the next
        // message is judged against the same expectation, so one malformed
        // payload counts as one failure rather than poisoning the session.
        if let Some(sequence) = sequence {
            *expected = sequence.wrapping_add(1);
        }
    }
}

/// Build the fixture's in-process test business by name, with its observables.
///
/// The names are the fixture's own — `test-service` (the reference's echo plus
/// the observables) and `sequence-service` (the same, checking each session's
/// sequence numbers). A product business comes from
/// `deepmsg_cluster_service::services`, not from here; these two exist only in
/// the test process, which is what makes their state readable by a test.
pub fn in_process_test_service(
    name: &str,
) -> (Box<dyn ClusteredService + Send>, Arc<TestServiceState>) {
    let state = Arc::new(TestServiceState::default());
    let service = match name {
        "test-service" => TestService::new(Arc::clone(&state), false),
        "sequence-service" => TestService::new(Arc::clone(&state), true),
        other => panic!("no test business named {other}"),
    };
    (Box::new(service), state)
}

impl ClusteredService for TestService {
    fn on_start(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        mut snapshot: Option<&mut SnapshotReader<'_>>,
    ) -> Result<(), ContainerError> {
        if let Some(reader) = snapshot.as_deref_mut() {
            // `TestNode.TestService.onStart` (`TestNode.java:546-576`): the
            // business restores its message count from its own snapshot image
            // — one record, the count at the moment the snapshot was taken.
            // Without it a business started from a backup directory counts only
            // what the log replay delivers, which is how a snapshot-carrying
            // case would read a plausible but wrong number.
            self.state.snapshot_loaded.store(true, Ordering::Release);
            let mut count = None;
            loop {
                reader.poll(10, |fragment| {
                    if let Some(value) = fragment.payload_i64_at(0) {
                        count = Some(value);
                    }
                });
                if count.is_some() || reader.is_end_of_stream() {
                    break;
                }
                std::thread::yield_now();
            }
            if let Some(value) = count {
                self.state
                    .messages
                    .store(usize::try_from(value).unwrap_or(0), Ordering::Release);
            }
        }
        self.inner.on_start(cluster, snapshot)
    }

    fn on_session_open(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        session: SessionId,
        timestamp: i64,
    ) -> Result<(), ContainerError> {
        self.state.session_opens.fetch_add(1, Ordering::AcqRel);

        if self.check_sequence {
            self.state
                .next_sequence
                .lock()
                .expect("the sequence lock")
                .insert(session, 0);
        }

        self.inner.on_session_open(cluster, session, timestamp)
    }

    fn on_session_close(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        session: SessionId,
        timestamp: i64,
        reason: CloseReason,
    ) -> Result<(), ContainerError> {
        self.state.session_closes.fetch_add(1, Ordering::AcqRel);
        self.state
            .close_reasons
            .lock()
            .expect("the reasons lock")
            .push(reason);

        if self.check_sequence {
            self.state
                .next_sequence
                .lock()
                .expect("the sequence lock")
                .remove(&session);
        }

        self.inner
            .on_session_close(cluster, session, timestamp, reason)
    }

    fn on_session_message(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        session: Option<SessionId>,
        timestamp: i64,
        payload: &[u8],
        header: &LogHeader,
    ) -> Result<(), ContainerError> {
        self.state.messages.fetch_add(1, Ordering::AcqRel);

        if self.check_sequence {
            if let Some(session) = session {
                self.check_one_sequence(session, payload);
            }
        }

        self.inner
            .on_session_message(cluster, session, timestamp, payload, header)
    }

    fn on_timer_event(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        correlation_id: i64,
        timestamp: i64,
    ) -> Result<(), ContainerError> {
        self.inner
            .on_timer_event(cluster, correlation_id, timestamp)
    }

    fn on_take_snapshot(&mut self, writer: &mut SnapshotWriter<'_>) -> Result<(), ContainerError> {
        if self.state.fail_next_snapshot.swap(false, Ordering::AcqRel) {
            return Err(ContainerError::Cluster(
                "This is a simulated failure for this snapshot".to_owned(),
            ));
        }
        self.inner.on_take_snapshot(writer)?;

        // The reference's `TestService.onTakeSnapshot` writes the business's
        // own message count into the snapshot (`TestNode.java:697-712`), then
        // `wasSnapshotTaken`. The count is what a case reads back through
        // `wasSnapshotLoaded` + `messageCount()` after a restart.
        let record = i64::try_from(self.state.messages())
            .unwrap_or(i64::MAX)
            .to_le_bytes();
        loop {
            if writer.offer(&record) > 0 {
                break;
            }
            writer.idle();
        }
        self.state.snapshot_taken.store(true, Ordering::Release);
        Ok(())
    }

    fn on_role_change(&mut self, new_role: Role) -> Result<(), ContainerError> {
        self.inner.on_role_change(new_role)
    }

    fn on_terminate(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
    ) -> Result<(), ContainerError> {
        self.state.terminated.store(true, Ordering::Release);
        self.inner.on_terminate(cluster)
    }
}

/// The reference's `SequencedEgressService`
/// (`ClusterSessionReliabilityTest.java:311-380`): a ten-millisecond timer
/// offers every open session's next sequence number.
///
/// What it is for is egress traffic that **no client action drives**. The case
/// that drops the client's egress and then lets it through again needs the
/// stream to have moved on while the client was not reading: an echo service
/// stops producing the moment the client stops offering, so a client that
/// rejoined the image would be shown nothing and the case could not tell a
/// rejoin from a correct absence.
struct SequencedEgressService {
    state: Arc<TestServiceState>,
    next_sequence: HashMap<SessionId, u64>,
}

/// The reference's `TIMER_CORRELATION_ID`.
const SEQUENCED_TIMER_ID: i64 = 1;

impl SequencedEgressService {
    fn new(state: Arc<TestServiceState>) -> Self {
        Self {
            state,
            next_sequence: HashMap::new(),
        }
    }

    /// Schedule the next tick, retrying while the cluster refuses.
    ///
    /// A timer's deadline is in the **cluster clock's unit**, not nanoseconds:
    /// the reference converts ten milliseconds into it
    /// (`cluster.timeUnit().convert(10, MILLISECONDS)`,
    /// `ClusterSessionReliabilityTest.java:345`), and a deadline a million
    /// times too large is a timer that never fires.
    fn schedule(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
    ) -> Result<(), ContainerError> {
        let interval = match cluster.time_unit() {
            TimeUnit::Milliseconds => 10,
            TimeUnit::Microseconds => 10_000,
            TimeUnit::Nanoseconds => 10_000_000,
        };
        let deadline = cluster.time().saturating_add(interval);

        while !cluster.schedule_timer(SEQUENCED_TIMER_ID, deadline)? {
            cluster.idle(0)?;
        }

        Ok(())
    }

    fn cancel(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
    ) -> Result<(), ContainerError> {
        while !cluster.cancel_timer(SEQUENCED_TIMER_ID)? {
            cluster.idle(0)?;
        }

        Ok(())
    }
}

impl ClusteredService for SequencedEgressService {
    fn on_start(
        &mut self,
        _cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        _snapshot: Option<&mut SnapshotReader<'_>>,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_session_open(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        session: SessionId,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
        self.state.session_opens.fetch_add(1, Ordering::AcqRel);
        self.next_sequence.insert(session, 0);
        if self.next_sequence.len() == 1 {
            self.schedule(cluster)?;
        }

        Ok(())
    }

    fn on_session_close(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        session: SessionId,
        _timestamp: i64,
        reason: CloseReason,
    ) -> Result<(), ContainerError> {
        self.state.session_closes.fetch_add(1, Ordering::AcqRel);
        self.state
            .close_reasons
            .lock()
            .expect("the reasons lock")
            .push(reason);
        self.next_sequence.remove(&session);

        if self.next_sequence.is_empty() {
            self.cancel(cluster)?;
        }

        Ok(())
    }

    fn on_session_message(
        &mut self,
        _cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        _session: Option<SessionId>,
        _timestamp: i64,
        _payload: &[u8],
        _header: &LogHeader,
    ) -> Result<(), ContainerError> {
        self.state.messages.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn on_timer_event(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        correlation_id: i64,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
        if correlation_id != SEQUENCED_TIMER_ID {
            return Ok(());
        }

        self.schedule(cluster)?;

        let sessions: Vec<SessionId> = self.next_sequence.keys().copied().collect();

        for session in sessions {
            let Some(sequence) = self.next_sequence.get(&session).copied() else {
                continue;
            };
            let Some(mut client_session) = cluster.session(session) else {
                continue;
            };

            if client_session.offer(&sequence.to_le_bytes())? > 0 {
                self.next_sequence.insert(session, sequence.wrapping_add(1));
                self.state.offered.fetch_add(1, Ordering::AcqRel);
            }
        }

        Ok(())
    }

    fn on_take_snapshot(&mut self, _writer: &mut SnapshotWriter<'_>) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_role_change(&mut self, _new_role: Role) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_terminate(
        &mut self,
        _cluster: &mut deepmsg_cluster_service::Cluster<'_>,
    ) -> Result<(), ContainerError> {
        self.state.terminated.store(true, Ordering::Release);
        Ok(())
    }
}

/// A business the fixture can observe: `on_terminate` has no read-back in the
/// reference's API, so the test that asserts it ran wraps the business.
struct Observed<S> {
    inner: S,
    terminated: Arc<std::sync::atomic::AtomicBool>,
}

impl<S: ClusteredService + Send> ClusteredService for Observed<S> {
    fn on_start(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        snapshot: Option<&mut SnapshotReader<'_>>,
    ) -> Result<(), ContainerError> {
        self.inner.on_start(cluster, snapshot)
    }

    fn on_session_open(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        session: SessionId,
        timestamp: i64,
    ) -> Result<(), ContainerError> {
        self.inner.on_session_open(cluster, session, timestamp)
    }

    fn on_session_close(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        session: SessionId,
        timestamp: i64,
        reason: CloseReason,
    ) -> Result<(), ContainerError> {
        self.inner
            .on_session_close(cluster, session, timestamp, reason)
    }

    fn on_session_message(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        session: Option<SessionId>,
        timestamp: i64,
        payload: &[u8],
        header: &LogHeader,
    ) -> Result<(), ContainerError> {
        self.inner
            .on_session_message(cluster, session, timestamp, payload, header)
    }

    fn on_timer_event(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        correlation_id: i64,
        timestamp: i64,
    ) -> Result<(), ContainerError> {
        self.inner
            .on_timer_event(cluster, correlation_id, timestamp)
    }

    fn on_take_snapshot(&mut self, writer: &mut SnapshotWriter<'_>) -> Result<(), ContainerError> {
        self.inner.on_take_snapshot(writer)
    }

    fn on_role_change(&mut self, new_role: Role) -> Result<(), ContainerError> {
        self.inner.on_role_change(new_role)
    }

    fn on_terminate(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
    ) -> Result<(), ContainerError> {
        self.terminated
            .store(true, std::sync::atomic::Ordering::Release);
        self.inner.on_terminate(cluster)
    }
}

/// A child process the fixture owns: spawned with the inherited `AERON_*`
/// removed, stopped with `SIGTERM`, and with its log kept for the failure
/// message (the directories belong to [`TempDir`], so nothing here deletes).
struct Process {
    child: Child,
    log_path: PathBuf,
    stopped: bool,
}

impl Process {
    fn spawn(
        program: &Path,
        log_path: &Path,
        args: &[String],
        extra_env: &[(&str, &str)],
    ) -> Result<Self, std::io::Error> {
        let log = std::fs::File::create(log_path)?;
        let log_err = log.try_clone()?;

        let mut command = Command::new(program);
        command
            .args(args)
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err));
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("AERON_") {
                command.env_remove(key);
            }
        }
        for (key, value) in extra_env {
            command.env(key, value);
        }

        Ok(Self {
            child: command.spawn()?,
            log_path: log_path.to_path_buf(),
            stopped: false,
        })
    }

    fn try_wait(&mut self) -> Result<Option<ExitStatus>, std::io::Error> {
        let status = self.child.try_wait()?;
        if status.is_some() {
            self.stopped = true;
        }
        Ok(status)
    }

    fn stop(&mut self) -> Result<ExitStatus, std::io::Error> {
        if self.stopped {
            return self.child.wait();
        }
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status();

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait()? {
                self.stopped = true;
                return Ok(status);
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                self.stopped = true;
                return self.child.wait();
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Whether the process has printed `needle` (readiness, not liveness: the
    /// mark file says the component concluded, this says its duty cycle has
    /// begun).
    fn log_contains(&self, needle: &str) -> bool {
        std::fs::read_to_string(&self.log_path)
            .map(|text| text.contains(needle))
            .unwrap_or(false)
    }

    fn log_tail(&self, lines: usize) -> String {
        let text = std::fs::read_to_string(&self.log_path).unwrap_or_default();
        let all: Vec<&str> = text.lines().collect();
        all[all.len().saturating_sub(lines)..].join("\n")
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Compile the probe sources the scenario needs, or `None` when one is missing.
fn compile_probes(probes: &[&'static str]) -> Option<TempDir> {
    let classes = TempDir::new("cluster-fixture-classes");
    for name in probes {
        let source = aeron_tests::java(name)?;
        java::compile_probe(&source, classes.path())?;
    }
    Some(classes)
}

fn counter_value(client: &Client, type_id: i32, cluster_id: i32) -> Option<i64> {
    let counters = client.counters_reader()?;
    let counter_id = counters::find_cluster_counter(&counters, type_id, cluster_id)?;
    counters.value(counter_id)
}
