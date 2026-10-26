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
//! * `node` — who runs the consensus module: [`NodeImpl::Java`] is the
//!   reference's `ClusteredMediaDriver` (driver, archive and module in one
//!   JVM); [`NodeImpl::Rust`] is this build's — a
//!   `deepmsg-archiving-media-driver` process with the module either on a
//!   thread of this process (`ProcessMode::InProcess`) or as the
//!   `deepmsg-cluster-node` binary (`External`). A mixed cluster is a member
//!   table that names both, which is LC-3 (P3-7).
//! * `service` — who runs the container: [`ServiceImpl::Rust`] names a business
//!   from `deepmsg_cluster_service::services`, [`ServiceImpl::Test`] one of the
//!   fixture's own (with the reference's test-business observables, in process
//!   only), [`ServiceImpl::Java`] a Java class the reference's container loads.
//! * `driver` — the media driver. [`DriverImpl::Java`] is the reference's,
//!   started inside `ClusteredMediaDriver` and therefore only with
//!   [`NodeImpl::Java`]; [`DriverImpl::Deepmsg`] is this build's and only with
//!   [`NodeImpl::Rust`]. The other two crosses are refused on purpose: a
//!   reference module on this driver is LC-0's question, and this build's
//!   module on the reference's driver is not a deployment this build targets.
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
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cluster::{ConsensusModuleAgent, ConsensusModuleConfig, LaunchComponents};
use deepmsg_cluster_common::config::Properties;
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
use deepmsg_driver::idle::Strategy;

use crate::aeron_tests;
use crate::archiving_driver::OwnArchivingMediaDriver;
use crate::driver::{self, AGRONA_JVM_ARGS};
use crate::java;
use crate::temp::TempDir;

/// How long a start, a stop or a wait may take before the test is a failure.
pub const DEADLINE: Duration = Duration::from_secs(120);

/// Environment variable naming the container binary to run.
pub const CONTAINER_ENV: &str = "DEEPMSG_CONTAINER_BIN";
/// Where the binary is when the variable is unset and the workspace is built.
pub const DEFAULT_CONTAINER: &str = "../target/debug/deepmsg-clustered-service-container";
/// Environment variable naming our consensus module binary to run.
pub const CLUSTER_NODE_ENV: &str = "DEEPMSG_CLUSTER_NODE_BIN";
/// Where our consensus module binary is when the variable is unset and the
/// workspace is built.
pub const DEFAULT_CLUSTER_NODE: &str = "../target/debug/deepmsg-cluster-node";

/// Who runs a member's consensus module.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeImpl {
    /// Ours: a `deepmsg-archiving-media-driver` process and this build's
    /// consensus module.
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
    /// Ours. Only with [`NodeImpl::Rust`]: this build's consensus module
    /// attaches to a driver somebody else runs.
    Deepmsg,
    /// The reference's, started inside `ClusteredMediaDriver`.
    Java,
}

/// How a member's container — and, when the node is ours, its consensus module
/// — is started.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessMode {
    /// On a thread of this process (the module as well, for [`NodeImpl::Rust`]).
    InProcess,
    /// As the `deepmsg-clustered-service-container` binary — and the
    /// `deepmsg-cluster-node` binary for a [`NodeImpl::Rust`] node.
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

    /// Our consensus module on our driver, running our container in `mode`.
    ///
    /// The mixed matrix's other half (LC-3, P3-7): the module and the driver are
    /// this build's, and the container is this build's too — a Java container
    /// beside a Rust module is LC-4b's shape, and it stays expressible by
    /// flipping this spec's `service`.
    #[must_use]
    pub const fn rust_node(id: i32, service: &'static str, mode: ProcessMode) -> Self {
        Self {
            id,
            service_id: 0,
            node: NodeImpl::Rust,
            service: ServiceImpl::Rust(service),
            driver: DriverImpl::Deepmsg,
            backup: false,
            mode,
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
    /// Extra `-D` properties for every node — a scenario that needs a
    /// different session timeout, say. Applied last, so a file's value is
    /// overridden the way a JVM's `-D` is.
    pub node_properties: Vec<(String, String)>,
    /// Extra `-D` properties for **one** member, applied after
    /// [`Self::node_properties`].
    ///
    /// A cluster's members are told the same things almost always, and the
    /// exception is the point: a case that wants one particular member to be
    /// the leader gives **that member** the appointment and nobody else, so
    /// that its death leaves an election rather than the reference's
    /// appointed-leader stall (`Election.java:715-718`: a member appointed to
    /// somebody else never nominates, so with every member told the same dead
    /// id the survivors canvass forever).
    pub member_properties: Vec<(i32, String, String)>,
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
            member_properties: Vec::new(),
        }
    }
}

impl Options {
    /// Every `-D` property this member is started with: the cluster-wide ones
    /// first, then the ones that name it.
    #[must_use]
    pub fn properties_for(&self, member: i32) -> Vec<(String, String)> {
        let mut properties = self.node_properties.clone();
        properties.extend(
            self.member_properties
                .iter()
                .filter(|(id, _, _)| *id == member)
                .map(|(_, name, value)| (name.clone(), value.clone())),
        );
        properties
    }

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
///
/// **A base belongs below `net.ipv4.ip_local_port_range`** (32768 on this
/// machine): inside it any process may take a port mid-run, and a cluster that
/// loses one of its own endpoints to, say, `avahi-daemon` fails in a way that
/// reads as a member dying for no reason (the member's follower log handover
/// binds its own log endpoint, `Election.java:1362`).
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

/// One node: its directories, its process(es), and how many containers it
/// expects.
struct Node {
    id: i32,
    aeron: TempDir,
    cluster: TempDir,
    archive: TempDir,
    service_count: i32,
    /// A Java node: one JVM that is driver, archive and consensus module.
    process: Option<Process>,
    /// Our node's driver and archive — a process of its own, because our
    /// consensus module attaches to a driver somebody runs.
    driver: Option<OwnArchivingMediaDriver>,
    /// Our node's consensus module.
    module: Option<ModuleHandle>,
    /// Where our external module writes — kept after the process is stopped,
    /// because a case that watches a member stop needs to read what it said on
    /// the way out.
    module_log: Option<PathBuf>,
    /// What our consensus module reported. Its thread has no log file, so a
    /// failure message reads this instead.
    faults: Arc<Mutex<Vec<String>>>,
}

/// Our consensus module, in whichever shape its mode asked for.
enum ModuleHandle {
    /// A thread of this process, driven the way `cluster_harness` drives one.
    InProcess {
        stop: Arc<AtomicBool>,
        thread: std::thread::JoinHandle<()>,
    },
    /// The `deepmsg-cluster-node` binary.
    External(Process),
}

impl Node {
    /// What this node's module reported, one line each.
    fn fault_lines(&self) -> Vec<String> {
        self.faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Whether anything that runs this node is still there.
    ///
    /// Two things can be: a Java node's one JVM, and our node's module (its
    /// driver outlives nothing — a stopped member has neither).
    fn is_running(&self) -> bool {
        self.process.is_some() || self.module.is_some()
    }
}

/// Record a line from a module thread, which has no log file of its own.
fn record_fault(faults: &Arc<Mutex<Vec<String>>>, line: String) {
    faults
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(line);
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
        Self::start_inner(test_name, members, options, Vec::new())
    }

    /// [`Self::start`], with some members' `cluster.dir` and `archive.dir`
    /// supplied by the caller — `(member id, archive dir, cluster dir)`, the
    /// order [`BackupNode::into_directories`] hands them over in.
    ///
    /// The reference's `startStaticNodeFromBackup` (`TestCluster.java:623-687`)
    /// starts a member over the two directories a backup wrote: they are not
    /// this fixture's to create, and `deleteDirOnStart` is `false` because what
    /// is in them *is* the thing under test. The driver's own directory is
    /// still new — the reference deletes and recreates that one.
    ///
    /// [`BackupNode::into_directories`]: crate::cluster_harness::BackupNode::into_directories
    pub fn start_over_directories(
        test_name: &str,
        members: &[MemberSpec],
        options: Options,
        directories: Vec<(i32, TempDir, TempDir)>,
    ) -> Option<Self> {
        Self::start_inner(test_name, members, options, directories)
    }

    fn start_inner(
        test_name: &str,
        members: &[MemberSpec],
        options: Options,
        mut directories: Vec<(i32, TempDir, TempDir)>,
    ) -> Option<Self> {
        assert!(!members.is_empty(), "a cluster needs at least one member");
        for member in members {
            assert!(
                !member.backup,
                "member {} asks for a backup: ClusterBackup is P3-9, and this fixture carries \
                 the field so that card does not have to change the table",
                member.id
            );
            assert_eq!(
                member.driver,
                match member.node {
                    NodeImpl::Java => DriverImpl::Java,
                    NodeImpl::Rust => DriverImpl::Deepmsg,
                },
                "member {} asks for {}'s driver beside {}'s consensus module: a reference module \
                 starts its own driver (`ClusteredMediaDriver`) and ours attaches to one somebody \
                 runs, so the two crosses are two other cards — the reference module on our driver \
                 is LC-0's, ours on the reference's is not a deployment this build targets",
                member.id,
                match member.driver {
                    DriverImpl::Java => "the reference",
                    DriverImpl::Deepmsg => "our",
                },
                match member.node {
                    NodeImpl::Java => "the reference",
                    NodeImpl::Rust => "our",
                }
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

        // A node is one set of directories and one implementation of the
        // module: a second service on the same member id may vary its container,
        // not who runs the node under it.
        for (index, member) in members.iter().enumerate() {
            for other in &members[index + 1..] {
                assert!(
                    other.id != member.id
                        || (other.node == member.node && other.driver == member.driver),
                    "member {} carries two services that disagree about who runs its node \
                     ({:?}/{:?} and {:?}/{:?})",
                    member.id,
                    member.node,
                    member.driver,
                    other.node,
                    other.driver
                );
            }
        }

        // A member whose node is ours needs two binaries of this build, and the
        // suite's way of saying "not built" is a skip, not a panic halfway into
        // a start: the first member would already have a driver running.
        if members.iter().any(|member| member.node == NodeImpl::Rust) {
            let needs_binaries = crate::archiving_driver::locate_own_archiving_media_driver()
                .is_some()
                && members.iter().all(|member| {
                    member.node != NodeImpl::Rust
                        || member.mode != ProcessMode::External
                        || driver::locate_tool(CLUSTER_NODE_ENV, DEFAULT_CLUSTER_NODE).is_some()
                });
            if !needs_binaries {
                crate::archiving_driver::announce_skip();
                return None;
            }
        }

        let jar = driver::locate_aeron_all()?;
        let classes = compile_probes(&options.probes)?;

        let mut nodes: Vec<Node> = Vec::new();
        for member in members {
            if nodes.iter().any(|node| node.id == member.id) {
                continue;
            }
            let supplied = directories
                .iter()
                .position(|(id, _, _)| *id == member.id)
                .map(|index| directories.remove(index));
            let (archive, cluster) = match supplied {
                Some((_, archive, cluster)) => (archive, cluster),
                None => (
                    TempDir::new(&format!("{test_name}-{}-archive", member.id)),
                    TempDir::new(&format!("{test_name}-{}-cluster", member.id)),
                ),
            };
            nodes.push(Node {
                id: member.id,
                aeron: TempDir::new(&format!("{test_name}-{}-aeron", member.id)),
                cluster,
                archive,
                service_count: i32::try_from(
                    members.iter().filter(|other| other.id == member.id).count(),
                )
                .expect("a node's service count fits an i32"),
                process: None,
                driver: None,
                module: None,
                module_log: None,
                faults: Arc::new(Mutex::new(Vec::new())),
            });
        }
        assert!(
            directories.is_empty(),
            "directories were supplied for members that are not in the table: {:?}",
            directories.iter().map(|(id, _, _)| *id).collect::<Vec<_>>()
        );

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

    /// A node's archive directory (`aeron.archive.dir`) — where the recordings
    /// this member made live, as segments of the term buffers.
    #[must_use]
    pub fn archive_dir(&self, member: i32) -> &Path {
        self.node(member).archive.path()
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
                if !node.is_running() {
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

    /// Replace a **stopped** member's implementation and start it again over the
    /// directories it already had — LC-5's rolling replacement (`3J → 3R → 3J`
    /// in a live cluster, plan §3.6).
    ///
    /// What `spec` may change is who runs the node and the container; what it
    /// may not change is which member it is ([`Self::start_inner`]'s one
    /// implementation per node id still holds). The **state** directories —
    /// `aeron.cluster.dir` (`recording.log`, `node-state.dat`, the mark file)
    /// and `aeron.archive.dir` (the recordings) — are the fixture's own and are
    /// not recreated here: they are the thing under test. Only the driver's
    /// directory is thrown away, and [`Self::start_node`] does that on every
    /// restart already.
    ///
    /// # Panics
    ///
    /// When the member is still running, still has a container, is not in the
    /// table, or when the table's other containers on this member disagree with
    /// `spec` about who runs their node.
    pub fn replace_member(&mut self, spec: MemberSpec) {
        let slots: Vec<usize> = self
            .containers
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.spec.id == spec.id)
            .map(|(index, _)| index)
            .collect();
        assert!(
            !slots.is_empty(),
            "member {} is not in the table, so there is nothing to replace",
            spec.id
        );
        assert!(
            !self.node(spec.id).is_running(),
            "member {} is still running: a replacement starts over the directories the old \
             implementation left, and a live one is still writing them",
            spec.id
        );
        for index in &slots {
            assert!(
                self.containers[*index].container.is_none(),
                "member {} service {}'s container is still running: stop it before replacing \
                 the member",
                spec.id,
                self.containers[*index].spec.service_id
            );
        }

        let replacement: Vec<usize> = slots
            .into_iter()
            .filter(|index| self.containers[*index].spec.service_id == spec.service_id)
            .collect();
        assert!(
            !replacement.is_empty(),
            "member {} has no service {} to replace",
            spec.id,
            spec.service_id
        );
        // The services that are *not* being replaced stay on the member, and a
        // node has one module: they have to agree with the one that arrives.
        for (index, slot) in self.containers.iter().enumerate() {
            if slot.spec.id == spec.id && !replacement.contains(&index) {
                assert!(
                    slot.spec.node == spec.node && slot.spec.driver == spec.driver,
                    "member {} asked to be replaced by {:?}/{:?}, but its service {} stays on \
                     {:?}/{:?}: a node has one module and one driver",
                    spec.id,
                    spec.node,
                    spec.driver,
                    slot.spec.service_id,
                    slot.spec.node,
                    slot.spec.driver
                );
            }
        }
        for index in replacement {
            self.containers[index].spec = spec;
        }

        self.start_node_by_id(spec.id);
        let service_ids: Vec<i32> = self
            .containers
            .iter()
            .filter(|slot| slot.spec.id == spec.id)
            .map(|slot| slot.spec.service_id)
            .collect();
        for service_id in service_ids {
            self.start_container(spec.id, service_id);
        }
    }

    /// Stop one member's node. Its containers are the caller's to stop first:
    /// a container outliving its node ends by itself, and a test that wants to
    /// see the end should watch it.
    pub fn stop_node(&mut self, member: i32) {
        let index = self.node_index(member);
        self.stop_node_processes(index);
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
        self.stop_node_processes(node_index);
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

        for index in 0..self.nodes.len() {
            self.stop_node_processes(index);
        }
    }

    /// The last lines of a node's output.
    #[must_use]
    pub fn node_log(&self, member: i32) -> String {
        let node = self.node(member);
        let mut text = node
            .process
            .as_ref()
            .map_or_else(String::new, |process| process.log_tail(80));
        if let Some(driver) = &node.driver {
            text.push_str(&driver.log_tail(40));
        }
        if let Some(path) = &node.module_log {
            let module = std::fs::read_to_string(path).unwrap_or_default();
            let lines: Vec<&str> = module.lines().collect();
            if !lines.is_empty() {
                text.push_str("\nconsensus module log:\n");
                text.push_str(&lines[lines.len().saturating_sub(40)..].join("\n"));
            }
        }
        let faults = node.fault_lines();
        if !faults.is_empty() {
            text.push_str("\nconsensus module:\n");
            text.push_str(&faults.join("\n"));
        }
        text
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

        match self.node_impl(index) {
            NodeImpl::Java => {
                let process = self.spawn_java_node(index);
                self.nodes[index].process = Some(process);
                self.await_driver_ready(index);
            }
            NodeImpl::Rust => self.start_rust_node(index),
        }
    }

    /// Who runs this node. Every spec that names it agrees — [`Self::start_inner`]
    /// refuses a table where they do not.
    fn node_impl(&self, index: usize) -> NodeImpl {
        let id = self.nodes[index].id;
        self.containers
            .iter()
            .find(|slot| slot.spec.id == id)
            .map_or(NodeImpl::Java, |slot| slot.spec.node)
    }

    /// How this member's containers — and, when the node is ours, its module —
    /// are started.
    fn member_mode(&self, index: usize) -> ProcessMode {
        let id = self.nodes[index].id;
        self.containers
            .iter()
            .find(|slot| slot.spec.id == id)
            .map_or(ProcessMode::External, |slot| slot.spec.mode)
    }

    /// Start our node: a driver-and-archive process, then the module.
    fn start_rust_node(&mut self, index: usize) {
        let id = self.nodes[index].id;
        let archive_channel = self.archive_control_channel(id);
        let properties = [
            // The archiving media driver runs the driver and the archive on
            // one thread and refuses anything but SHARED or INVOKER.
            "-Daeron.threading.mode=SHARED".to_owned(),
            "-Daeron.archive.threading.mode=SHARED".to_owned(),
            format!("-Daeron.archive.control.channel={archive_channel}"),
            "-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0".to_owned(),
        ];
        let property_refs: Vec<&str> = properties.iter().map(String::as_str).collect();

        let mut driver = {
            let node = &self.nodes[index];
            OwnArchivingMediaDriver::start_in(
                node.aeron.path(),
                node.archive.path(),
                &property_refs,
            )
        }
        .unwrap_or_else(|| {
            panic!(
                "member {id} asks for our node, and there is no \
                 `deepmsg-archiving-media-driver` binary: `start` skips a table that asks for one \
                 before it gets here, so this build changed under the fixture"
            )
        });
        driver.await_ready(DEADLINE).unwrap_or_else(|error| {
            panic!("member {id}'s driver and archive never became ready: {error}")
        });

        let arguments = self.rust_module_arguments(index);
        let module = match self.member_mode(index) {
            ProcessMode::InProcess => {
                self.nodes[index].module_log = None;
                self.launch_rust_module(index, arguments)
            }
            ProcessMode::External => {
                self.nodes[index].module_log =
                    Some(self.nodes[index].cluster.path().join("cluster-node.log"));
                ModuleHandle::External(self.spawn_cluster_node(index, &arguments))
            }
        };

        self.nodes[index].driver = Some(driver);
        self.nodes[index].module = Some(module);
    }

    /// The module properties a [`NodeImpl::Rust`] member is given — the same
    /// names and the same values its Java sibling gets from
    /// `ClusteredMediaDriver`, so that a difference between the two is a
    /// difference in the implementations and not in what they were told.
    fn rust_module_arguments(&self, index: usize) -> Vec<String> {
        let node = &self.nodes[index];
        let mut arguments = vec![
            format!("-Daeron.dir={}", node.aeron.path().display()),
            format!("-Daeron.cluster.dir={}", node.cluster.path().display()),
            format!("-Daeron.cluster.id={}", self.options.cluster_id),
            format!("-Daeron.cluster.member.id={}", node.id),
            format!("-Daeron.cluster.members={}", self.options.members_string()),
            format!(
                "-Daeron.cluster.ingress.channel={}",
                self.options.ingress_channel
            ),
            format!(
                "-Daeron.cluster.consensus.channel={}",
                self.options.consensus_channel
            ),
            format!("-Daeron.cluster.log.channel={}", self.options.log_channel),
            format!(
                "-Daeron.cluster.control.channel={}",
                self.options.control_channel
            ),
            "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0".to_owned(),
            format!("-Daeron.cluster.service.count={}", node.service_count),
        ];
        for (name, value) in &self.options.properties_for(node.id) {
            arguments.push(format!("-D{name}={value}"));
        }
        arguments
    }

    /// Our consensus module on a thread of this process, as `cluster_harness`
    /// runs one.
    fn launch_rust_module(&self, index: usize, arguments: Vec<String>) -> ModuleHandle {
        let node = &self.nodes[index];
        let id = node.id;
        let mut properties = Properties::new();
        properties
            .apply_args(arguments)
            .unwrap_or_else(|error| panic!("member {id}'s module properties: {error}"));
        let config = ConsensusModuleConfig::from_properties(&properties)
            .unwrap_or_else(|error| panic!("member {id}'s module is refused: {error}"));
        let idle_strategy = config.idle_strategy.clone();

        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let faults = Arc::clone(&node.faults);

        let thread = std::thread::spawn(move || {
            let Some(mut idle) = Strategy::by_agrona_name(&idle_strategy) else {
                record_fault(
                    &faults,
                    format!("member {id}: unknown idle strategy {idle_strategy}"),
                );
                return;
            };
            let handler = Arc::clone(&faults);
            let mut agent = match ConsensusModuleAgent::launch_with_components(
                config,
                LaunchComponents {
                    // The reference's `ClusterTests.errorHandler`
                    // (`ClusterTest.java:565`): a WARN is printed by the agent
                    // and goes no further, an ERROR is a failure — and here a
                    // failure is what a case's waits panic with.
                    error_handler: Some(Box::new(move |error: &deepmsg_cluster::ClusterError| {
                        if !error.is_warning() {
                            record_fault(&handler, format!("member {id}: {error}"));
                        }
                    })),
                    ..LaunchComponents::default()
                },
            ) {
                Ok(agent) => agent,
                Err(error) => {
                    record_fault(&faults, format!("member {id}: launch: {error}"));
                    return;
                }
            };

            while !flag.load(Ordering::Relaxed) {
                match agent.do_work() {
                    Ok(work) => idle.idle(work),
                    Err(error) if error.is_termination() => break,
                    Err(error) => {
                        record_fault(&faults, format!("member {id}: the duty cycle: {error}"));
                        break;
                    }
                }
            }

            if let Err(error) = agent.close() {
                record_fault(&faults, format!("member {id}: close: {error}"));
            }
        });

        ModuleHandle::InProcess { stop, thread }
    }

    /// Our consensus module as the `deepmsg-cluster-node` binary.
    fn spawn_cluster_node(&self, index: usize, arguments: &[String]) -> Process {
        let node = &self.nodes[index];
        let binary =
            driver::locate_tool(CLUSTER_NODE_ENV, DEFAULT_CLUSTER_NODE).unwrap_or_else(|| {
                panic!(
                    "no {DEFAULT_CLUSTER_NODE}; build it with `cargo build --workspace --bins` \
                     or set {CLUSTER_NODE_ENV}"
                )
            });
        let log = node.cluster.path().join("cluster-node.log");
        Process::spawn(&binary, &log, arguments, &[]).unwrap_or_else(|error| {
            panic!(
                "member {}'s consensus module did not start: {error}",
                node.id
            )
        })
    }

    /// Stop whatever runs this node: the reference's one JVM, or our module and
    /// its driver.
    fn stop_node_processes(&mut self, index: usize) {
        let id = self.nodes[index].id;
        if let Some(mut process) = self.nodes[index].process.take() {
            let status = process.stop().expect("the Java node stops");
            assert!(
                status.success() || status.code() == Some(143),
                "member {id}'s Java node exited with {status}\nnode log:\n{}",
                process.log_tail(80)
            );
        }
        if let Some(module) = self.nodes[index].module.take() {
            match module {
                ModuleHandle::InProcess { stop, thread } => {
                    stop.store(true, Ordering::Release);
                    // The duty cycle polls the flag, so this returns once it
                    // has taken one more turn — **but a thread inside
                    // `do_work` is not in that turn**: the election's
                    // `await_services_ready` polls its acks in a loop with no
                    // deadline (the reference's does the same,
                    // `ConsensusModuleAgent.java:1820-1836`), and a member
                    // that is joining a term while this fixture takes its
                    // service containers away never leaves it.
                    //
                    // An unbounded join here cost the interop suite **100
                    // minutes** once: every other binary had finished, this
                    // thread sat in `await_services_ready`, and the harness
                    // reported nothing because a hung test never fails. Bound
                    // the wait so the next one is a failure that names the
                    // state.
                    let (finished_tx, finished_rx) = mpsc::channel();
                    let joiner = std::thread::spawn(move || {
                        let _ = thread.join();
                        let _ = finished_tx.send(());
                    });
                    if finished_rx.recv_timeout(DEADLINE).is_err() {
                        // The module thread cannot be killed, and it holds a
                        // client onto this node's driver: stop the driver so
                        // the rest of the fixture does not wait on it too, then
                        // fail with what is known.
                        if let Some(mut driver) = self.nodes[index].driver.take() {
                            let _ = driver.stop();
                        }
                        panic!(
                            "member {id}'s consensus module thread is still inside its duty \
                             cycle {DEADLINE:?} after being told to stop. The stop flag is read \
                             between duty cycles, so this is a module blocked inside one — the \
                             election's `await_services_ready` is the unbounded wait that does \
                             it, and a shutdown that stops the containers first leaves it \
                             waiting for acks that can never come. This is a hang made into a \
                             failure; the case still needs a shorter scenario or a different \
                             stop order."
                        );
                    }
                    drop(joiner);
                }
                ModuleHandle::External(mut process) => {
                    let status = process.stop().expect("our node stops");
                    assert!(
                        status.success() || status.code() == Some(143),
                        "member {id}'s node exited with {status}\nnode log:\n{}",
                        process.log_tail(80)
                    );
                }
            }
        }
        if let Some(mut driver) = self.nodes[index].driver.take() {
            let _ = driver.stop();
        }
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

    /// Where a member's archive listens: the endpoint the member string
    /// advertises for it, or a kernel-chosen port when the string names zero
    /// (which is what [`Options::default`] builds).
    fn archive_control_channel(&self, member: i32) -> String {
        use deepmsg_cluster_common::member::{ClusterMember, find_member};

        let endpoint = ClusterMember::parse_list(&self.options.members_string())
            .ok()
            .and_then(|members| {
                find_member(&members, member).map(|found| found.archive_endpoint.clone())
            })
            .filter(|endpoint| !endpoint.ends_with(":0"));

        endpoint.map_or_else(
            || "aeron:udp?endpoint=localhost:0".to_owned(),
            |endpoint| format!("aeron:udp?endpoint={endpoint}"),
        )
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
        // The member string says where a member's archive is reachable, and a
        // backup dials that endpoint — it reads it out of the cluster's
        // response and connects there (`ClusterBackupAgent.java:578-590`).
        // The reference's fixture gives every archive the same fixed endpoint
        // it puts in the member string (`TestCluster.java:275`,
        // `:2189-2192`); a member string that names port zero keeps the
        // kernel-chosen port.
        args.push(format!(
            "-Daeron.archive.control.channel={}",
            self.archive_control_channel(node.id)
        ));
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
        for (name, value) in &self.options.properties_for(node.id) {
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

/// The stream the API business's own market publication uses — a stream no
/// client of the cluster asks for, so the case can tell its messages from the
/// cluster's.
pub const API_MARKET_STREAM_ID: i32 = 4001;

/// What the business offers on its **own** publication from inside
/// `on_take_snapshot` — the frame the case reads on its subscriber.
///
/// A business's own stream is a different publication from the snapshot
/// recording, which is why this is a marker and not the state record: it must
/// arrive on the wire and *not* end up in the snapshot's bytes.
pub const SNAPSHOT_MARKET_MARKER: &[u8] = b"api-market-in-snapshot";

/// What the API-facing test business observed, for the case that reads it.
///
/// A business cannot panic its way out of a failure — a panic takes the
/// container down — so every unexpected answer is recorded here in words.
#[derive(Default)]
pub struct ApiState {
    /// `Cluster.context()`, as the business read it.
    pub service_id: AtomicI64,
    pub cluster_id: AtomicI64,
    pub aeron_dir: Mutex<String>,
    /// The registration id `aeron().add_publication(..)` answered with, and how
    /// many of its offers landed.
    pub market_publication: AtomicI64,
    pub market_offers: AtomicUsize,
    /// The positions `try_claim` answered with (service and egress), and how
    /// many claimed frames were committed.
    pub claimed: AtomicUsize,
    /// `for_each_client_session`: views visited, and offers accepted.
    pub visits: AtomicUsize,
    pub fanned_out: AtomicUsize,
    /// The marker offers the snapshot callback made on the business's own
    /// publication, and whether `on_start` was handed a snapshot at all.
    pub snapshot_market_offers: AtomicUsize,
    pub started_with_snapshot: AtomicBool,
    /// How many snapshots this business has taken, and the value `on_start`
    /// read back out of the business's own record (`0` = nothing was loaded).
    pub snapshots_written: AtomicI64,
    pub loaded_state: AtomicI64,
    /// How many records the business's snapshot image carried on load. One is
    /// correct: the frame the callback published on its **own** publication
    /// goes to that stream, not into the recording, and a case that read two
    /// would be looking at a mix-up of the two publications.
    pub snapshot_records: AtomicUsize,
    /// `ClientSession::close` answers.
    pub closes: AtomicUsize,
    /// The service messages the business offered and later read back, in the
    /// order they came: `(what it offered, what the log delivered)`.
    pub service_messages: Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
    pub fault: Mutex<Option<String>>,
}

impl ApiState {
    fn fault(&self, message: String) {
        let mut slot = self.fault.lock().expect("the fault lock");
        if slot.is_none() {
            *slot = Some(message);
        }
    }

    /// Every failure recorded, so a case can name them all at once.
    #[must_use]
    pub fn faults(&self) -> Vec<String> {
        self.fault
            .lock()
            .expect("the fault lock")
            .clone()
            .into_iter()
            .collect()
    }
}

/// The business the API cases drive: one message per door of the container's
/// public surface, chosen by the first payload byte.
struct ApiService {
    state: Arc<ApiState>,
}

impl ApiService {
    fn new(state: Arc<ApiState>) -> Self {
        Self { state }
    }

    /// What the business offered as a **service message**, held until the log
    /// hands it back, so the read-back can be compared with the offer.
    fn record_offered(&self, offered: &[u8]) {
        self.state
            .service_messages
            .lock()
            .expect("the service-message lock")
            .push((offered.to_vec(), Vec::new()));
    }

    /// A service message came back: pair it with the oldest offer.
    fn record_delivered(&self, delivered: &[u8]) {
        let mut pending = self
            .state
            .service_messages
            .lock()
            .expect("the service-message lock");

        // The **first** offer still unanswered, not the newest: the log may
        // hand two service messages back after both were offered, and pairing
        // them with the latest would leave the first unanswered for ever.
        let Some(open) = pending
            .iter_mut()
            .find(|(_, delivered)| delivered.is_empty())
        else {
            self.state.fault(format!(
                "a service message arrived with nothing offered: {delivered:?}"
            ));
            return;
        };
        open.1 = delivered.to_vec();
    }
}

impl ClusteredService for ApiService {
    fn on_start(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        snapshot: Option<&mut SnapshotReader<'_>>,
    ) -> Result<(), ContainerError> {
        // A restart is a **new client**: the registration id the previous
        // process used is not in this one's publication table, so a business
        // that keeps publications across a restart has to forget them here.
        self.state
            .market_publication
            .store(0, std::sync::atomic::Ordering::Release);

        if let Some(reader) = snapshot {
            // The business's own record, written by `on_take_snapshot` through
            // the writer: reading it back is how a case tells "loaded a
            // snapshot" from "started".
            self.state
                .started_with_snapshot
                .store(true, std::sync::atomic::Ordering::Release);
            let mut loaded = None;
            let mut records = 0;
            while !reader.is_end_of_stream() {
                reader.poll(10, |fragment| {
                    records += 1;
                    if let Some(value) = fragment.payload_i64_at(0) {
                        loaded = Some(value);
                    }
                });
            }
            self.state
                .snapshot_records
                .store(records, std::sync::atomic::Ordering::Release);
            if let Some(value) = loaded {
                self.state
                    .loaded_state
                    .store(value, std::sync::atomic::Ordering::Release);
            }
        }

        let context = cluster.context();
        self.state
            .service_id
            .store(i64::from(context.service_id), Ordering::Release);
        self.state
            .cluster_id
            .store(i64::from(context.cluster_id), Ordering::Release);
        *self.state.aeron_dir.lock().expect("the directory lock") =
            context.aeron_dir.display().to_string();
        Ok(())
    }

    fn on_session_open(
        &mut self,
        _cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        _session: SessionId,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_session_close(
        &mut self,
        _cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        _session: SessionId,
        _timestamp: i64,
        _reason: CloseReason,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_session_message(
        &mut self,
        cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        session: Option<SessionId>,
        _timestamp: i64,
        payload: &[u8],
        _header: &LogHeader,
    ) -> Result<(), ContainerError> {
        let Some(session_id) = session else {
            self.record_delivered(payload);
            return Ok(());
        };

        match payload.first().copied() {
            Some(b'v') => self.vectored(cluster, session_id),
            Some(b'k') => self.claim(cluster, session_id),
            Some(b'f') => self.fan_out(cluster),
            Some(b'x') => self.close_one(cluster, session_id),
            Some(b'm') => self.market(cluster, session_id),
            other => self.state.fault(format!(
                "unknown command {other:?} from session {session_id:?}"
            )),
        }

        Ok(())
    }

    fn on_timer_event(
        &mut self,
        _cluster: &mut deepmsg_cluster_service::Cluster<'_>,
        _correlation_id: i64,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_take_snapshot(&mut self, writer: &mut SnapshotWriter<'_>) -> Result<(), ContainerError> {
        // (1) The business's own publication, through the client the writer
        //     holds — the door this case exists for. `Cluster::aeron()`'s borrow
        //     ended with `on_start`, and this callback is handed no `Cluster`.
        let registration_id = self
            .state
            .market_publication
            .load(std::sync::atomic::Ordering::Acquire);
        if registration_id > 0 {
            match writer
                .aeron()
                .offer(registration_id, SNAPSHOT_MARKET_MARKER)
            {
                Some(deepmsg_core::logbuffer::append::Appended::Ok { .. }) => {
                    self.state
                        .snapshot_market_offers
                        .fetch_add(1, Ordering::AcqRel);
                }
                other => self.state.fault(format!(
                    "the market offer in the snapshot answered {other:?}"
                )),
            }
        }

        // (2) And the writer's own door in the same callback: `aeron()` takes
        //     `&self` and so does `offer`, so the two borrows do not stand in
        //     each other's way. The record is what `on_start` reads back.
        let taken = self.state.snapshots_written.fetch_add(1, Ordering::AcqRel) + 1;
        if writer.offer(&taken.to_le_bytes()) < 0 {
            self.state
                .fault("the snapshot writer refused its own record".to_owned());
        }
        Ok(())
    }

    fn on_role_change(&mut self, _new_role: Role) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_terminate(
        &mut self,
        _cluster: &mut deepmsg_cluster_service::Cluster<'_>,
    ) -> Result<(), ContainerError> {
        Ok(())
    }
}

impl ApiService {
    /// `v`: an egress message in parts, and a **service** message in parts.
    ///
    /// The egress one the client compares against a single-buffer offer of the
    /// same bytes; the service message the business reads back out of the log.
    fn vectored(&self, cluster: &mut deepmsg_cluster_service::Cluster<'_>, session: SessionId) {
        let parts: [&[u8]; 2] = [b"vec-one", b"vec-two"];
        let joined = b"vec-onevec-two";

        if let Some(mut view) = cluster.session(session) {
            if let Err(error) = view.offer_vectored(&parts) {
                self.state.fault(format!("egress offer_vectored: {error}"));
            }
            if let Err(error) = view.offer(joined) {
                self.state.fault(format!("egress offer: {error}"));
            }
        } else {
            self.state
                .fault(format!("no session {session:?} to offer on"));
        }

        match cluster.offer_vectored(&parts) {
            Ok(_) => self.record_offered(joined),
            Err(error) => self.state.fault(format!("service offer_vectored: {error}")),
        }
    }

    /// `k`: a claimed egress message, and a claimed service message.
    fn claim(&self, cluster: &mut deepmsg_cluster_service::Cluster<'_>, session: SessionId) {
        let payload = b"claim";

        if let Some(mut view) = cluster.session(session) {
            match view.try_claim(payload.len()) {
                Ok(deepmsg_cluster_service::TryClaim::Claimed(mut claimed)) => {
                    let Some(window) = claimed.payload_mut() else {
                        self.state
                            .fault("an egress claim with no window".to_owned());
                        return;
                    };
                    window.copy_from_slice(payload);
                    match claimed.commit() {
                        Ok(_) => {
                            self.state.claimed.fetch_add(1, Ordering::AcqRel);
                        }
                        Err(error) => self.state.fault(format!("egress commit: {error}")),
                    }
                }
                Ok(deepmsg_cluster_service::TryClaim::NotClaimed(code)) => self
                    .state
                    .fault(format!("the egress claim was refused with {code}")),
                Err(error) => self.state.fault(format!("egress try_claim: {error}")),
            }
        }

        match cluster.try_claim(payload.len()) {
            Ok(deepmsg_cluster_service::TryClaim::Claimed(mut claimed)) => {
                let Some(window) = claimed.payload_mut() else {
                    self.state
                        .fault("a service claim with no window".to_owned());
                    return;
                };
                window.copy_from_slice(payload);
                if let Err(error) = claimed.commit() {
                    self.state.fault(format!("service commit: {error}"));
                    return;
                }
                self.state.claimed.fetch_add(1, Ordering::AcqRel);
                self.record_offered(payload);
            }
            Ok(deepmsg_cluster_service::cluster::TryClaim::NotClaimed(code)) => self
                .state
                .fault(format!("the service claim was refused with {code}")),
            Err(error) => self.state.fault(format!("service try_claim: {error}")),
        }
    }

    /// `f`: every session, visited once and offered once.
    fn fan_out(&self, cluster: &mut deepmsg_cluster_service::Cluster<'_>) {
        let state = Arc::clone(&self.state);
        let mut visits = 0;
        let mut offered = 0;

        cluster.for_each_client_session(|view| {
            visits += 1;
            if view.offer(b"fan").is_ok() {
                offered += 1;
            }
        });

        state.visits.fetch_add(visits, Ordering::AcqRel);
        state.fanned_out.fetch_add(offered, Ordering::AcqRel);
    }

    /// `x`: close the session through the session's own door.
    fn close_one(&self, cluster: &mut deepmsg_cluster_service::Cluster<'_>, session: SessionId) {
        match cluster.session(session) {
            Some(mut view) => match view.close() {
                Ok(closed) => {
                    if closed {
                        self.state.closes.fetch_add(1, Ordering::AcqRel);
                    }
                }
                Err(error) => self.state.fault(format!("session close: {error}")),
            },
            None => self.state.fault(format!("no session {session:?} to close")),
        }
    }

    /// `m`: a market-data publication made through `Cluster.aeron()`.
    ///
    /// The channel is the one the client is already listening on — its own
    /// egress channel, which is what `responseChannel()` names — so the
    /// publication is in the container's driver and the subscriber in the
    /// client's, which is the only arrangement that proves the client is real.
    fn market(&self, cluster: &mut deepmsg_cluster_service::Cluster<'_>, session: SessionId) {
        let channel = cluster
            .sessions()
            .map(|info| info.response_channel.to_owned())
            .next();

        let Some(channel) = channel else {
            self.state.fault("no session to name a channel".to_owned());
            return;
        };

        let client = cluster.aeron();
        let existing = self
            .state
            .market_publication
            .load(std::sync::atomic::Ordering::Acquire);
        let registration_id = if existing > 0 {
            existing
        } else {
            match client.add_publication(&channel, API_MARKET_STREAM_ID, Duration::from_secs(1)) {
                Ok(id) => {
                    self.state.market_publication.store(id, Ordering::Release);
                    id
                }
                Err(error) => {
                    self.state
                        .fault(format!("add_publication on {channel}: {error}"));
                    return;
                }
            }
        };

        let Some(publication) = client.publication(registration_id) else {
            self.state
                .fault("the market publication is not in the client".to_owned());
            return;
        };
        let limit = deepmsg_cluster_service::proxy::position_limit(client, publication);
        if matches!(
            publication.offer(limit, b"market"),
            deepmsg_core::logbuffer::append::Appended::Ok { .. }
        ) {
            self.state.market_offers.fetch_add(1, Ordering::AcqRel);
        }

        if let Some(mut view) = cluster.session(session) {
            if let Err(error) = view.offer(b"market-ack") {
                self.state.fault(format!("market ack: {error}"));
            }
        }
    }
}

/// The API business, with its observables.
pub fn in_process_api_service() -> (Box<dyn ClusteredService + Send>, Arc<ApiState>) {
    let state = Arc::new(ApiState::default());
    let service = ApiService::new(Arc::clone(&state));
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
