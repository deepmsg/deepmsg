//! Three consensus modules, each with its own driver and archive, forming one
//! cluster over UDP.
//!
//! This is the instrument P3-6a built and P3-6b grew: **three pure-Rust nodes
//! elect a leader, commit, and keep committing when a follower dies**, and a
//! stopped node can be brought back on the same directory and driver so the
//! follower paths — catchup, log replication, truncation — have something to
//! come back to.
//!
//! # The topology
//!
//! Each member gets its own aeron directory, archive directory and cluster
//! directory, and its own `deepmsg-archiving-media-driver` process — a media
//! driver *and* an archive, because a cluster member records the log through
//! its archive and `Archive::connect` is a local IPC connection
//! (`crates/cluster/consensus/src/config.rs`). The consensus module itself runs
//! in the test process, one thread each: `launch` blocks until the node has
//! joined the log and the election has closed, so a thread is the only place
//! it can live.
//!
//! The client gets a fourth driver of its own, so that stopping a member cannot
//! take the client with it.
//!
//! # Ports
//!
//! The reference's scheme (`TestCluster.java:1877-1905`) with a process-id
//! base, because these tests share a machine with the rest of the workspace's
//! integration suite. See [`Ports`].
//!
//! # What is deliberately absent
//!
//! `aeron.cluster.service.count=0`: the service container is P3-2c's, and the
//! cards' DoD does not need it — a follower's append position, which is what
//! commit is computed from, is the log recording, not the service.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deepmsg_archive::client::Credentials;
use deepmsg_client::client::Client;
use deepmsg_cluster::session::authenticator::{Authenticator, SessionProxy};
use deepmsg_cluster::{
    ClusterBackup, ClusterBackupComponents, ClusterBackupConfig, ClusterBackupContext,
    ConsensusModuleAgent, ConsensusModuleConfig, LaunchComponents, ReplayStart, SourceType,
};
use deepmsg_cluster_common::config::Properties;
use deepmsg_cluster_common::counters::{
    CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID, CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID,
    CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID, CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID,
    CLUSTER_BACKUP_STATE_TYPE_ID, CLUSTER_COMMIT_POSITION_TYPE_ID, CLUSTER_ELECTION_STATE_TYPE_ID,
    CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID, CLUSTER_NODE_ROLE_TYPE_ID,
    CLUSTER_SNAPSHOT_COUNTER_TYPE_ID, find_cluster_counter,
};
use deepmsg_cluster_service::agent::ClusteredServiceAgent;
use deepmsg_cluster_service::config::ServiceContainerConfig;
use deepmsg_cnc::file::CncFile;
use deepmsg_codec_cluster::challenge_codec::ChallengeDecoder;
use deepmsg_codec_cluster::challenge_response_codec::ChallengeResponseEncoder;
use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::session_connect_request_codec::SessionConnectRequestEncoder;
use deepmsg_codec_cluster::session_event_codec::SessionEventDecoder;
use deepmsg_codec_cluster::session_message_header_codec::SessionMessageHeaderEncoder;
use deepmsg_codec_cluster::{ReadBuf, WriteBuf};
use deepmsg_driver::idle::Strategy;

use crate::archive;
use crate::archiving_driver::{self, OwnArchivingMediaDriver};
use crate::cluster::{TestServiceState, in_process_test_service};
use crate::driver::OwnDriver;
use crate::temp::TempDir;

/// The cluster id every counter key carries.
pub const CLUSTER_ID: i32 = 7;
/// How many members stand up.
pub const MEMBER_COUNT: i32 = 3;
/// The ingress stream every member listens on.
pub const INGRESS_STREAM_ID: i32 = 101;
/// The egress stream the client listens on.
pub const EGRESS_STREAM_ID: i32 = 102;
/// `SemanticVersion.compose(0, 3, 0)`.
pub const PROTOCOL_VERSION: i32 = 3 << 8;
/// How long an exchange may take before the test is a failure.
pub const DEADLINE: Duration = Duration::from_secs(90);

/// The budget for a wait whose work is a **catch-up**: a follower joining a
/// log it is behind on.
///
/// The election's own attempt budget is `aeron.cluster.leader.heartbeat.timeout`
/// (ten seconds, the reference's default, set to thirty in [`Node::start`]
/// because this harness runs three consensus modules, three archiving media
/// drivers and a client driver on one machine), and an attempt that misses its
/// budget is retried from `INIT` — which restarts the replay, the log
/// subscription and the recording. Several such attempts are ordinary on a
/// loaded runner, and the reference's own cluster tests give their scenarios
/// minutes rather than ninety seconds.
pub const CATCHUP_DEADLINE: Duration = Duration::from_secs(180);

/// Member roles, as counter 201 carries them (`Cluster.Role`).
pub const ROLE_FOLLOWER: i64 = 0;
/// The leader's role code.
pub const ROLE_LEADER: i64 = 2;
/// `ElectionState.CLOSED` (`crates/cluster/consensus/src/election/state.rs`).
pub const ELECTION_CLOSED: i64 = 17;

/// The reference's port scheme (`TestCluster.java:1877-1905`) with a base that
/// moves with the process id, so two concurrent `cargo test` runs do not meet
/// on a fixed port.
#[derive(Clone, Copy)]
pub struct Ports {
    base: u16,
    /// Added to [`Self::client_egress`], so a second client can answer on a
    /// port of its own.
    egress_offset: u16,
    /// The shape of the three channels the reference lets a test change
    /// (`TestCluster.java:145-149`): the log, the ingress and the egress.
    ///
    /// The defaults are the reference's own. They are fields rather than
    /// literals so H5's second arm can rest the *same ports* on a different
    /// term buffer length and MTU (`ClusterTest.java:1892-1895`).
    shape: ChannelShape,
}

/// Term buffer length and MTU of the log, ingress and egress channels.
///
/// The ingress and the log share a term length in the second arm because an
/// ingress message is appended to the log unchanged, so the largest legal
/// message is `term-length / 8` on both.
#[derive(Clone, Copy)]
pub struct ChannelShape {
    /// `aeron.cluster.log.channel`'s `term-length` (`TestCluster.java:145`).
    pub log_term_length: i32,
    /// `aeron.cluster.ingress.channel`'s (`:148`).
    pub ingress_term_length: i32,
    /// `aeron.cluster.egress.channel`'s (`:147`).
    pub egress_term_length: i32,
    /// `mtu`, when the test names one; the driver's default otherwise.
    pub mtu: Option<i32>,
}

impl Default for ChannelShape {
    fn default() -> Self {
        Self {
            log_term_length: 512 * 1024,
            ingress_term_length: 128 * 1024,
            egress_term_length: 128 * 1024,
            mtu: None,
        }
    }
}

/// The variable that names the port block a run takes, for a machine with more
/// than one of these test binaries running at once.
pub const PORT_BASE_ENV: &str = "CLUSTER_HARNESS_PORT_BASE";

impl Ports {
    pub fn new() -> Self {
        // A block of fifty per run, because the highest offset used is +35 (a
        // second client's egress port is +30 plus its offset). Eighty blocks,
        // 21000..25200 — below the kernel's ephemeral range, which is where a
        // random `pid %` would otherwise land.
        //
        // ★ **The block is per *process*, and two processes can land on the
        // same one**: `pid %` is not a reservation, and this repository is
        // developed in several worktrees at once. A run that has to coexist
        // names its block ([`PORT_BASE_ENV`]); the earlier scheme used ten-port
        // spacing against forty ports of use, so *any* two concurrent processes
        // overlapped and the second one met `Address already in use` from a
        // socket the first was still holding.
        let base = std::env::var(PORT_BASE_ENV)
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or_else(|| 21_000 + (u16::try_from(std::process::id() % 80).unwrap()) * 50);

        Self {
            base,
            egress_offset: 0,
            shape: ChannelShape::default(),
        }
    }

    /// A block whose first port is `base`.
    ///
    /// For a binary whose cases run on threads of one process: each case takes
    /// a block of its own past the one [`Self::new`] would pick, because the
    /// blocks are per process and the cases are not.
    #[must_use]
    pub const fn with_base(base: u16) -> Self {
        Self {
            base,
            egress_offset: 0,
            shape: ChannelShape {
                log_term_length: 512 * 1024,
                ingress_term_length: 128 * 1024,
                egress_term_length: 128 * 1024,
                mtu: None,
            },
        }
    }

    /// The first port of this run's block, for a test that needs a port of its
    /// own past every offset the cluster uses (the highest is `+35`; a backup
    /// takes `+43` and `+44`).
    #[must_use]
    pub const fn base(&self) -> u16 {
        self.base
    }

    /// Sit the same ports on a new channel shape — H5's second arm, which
    /// restarts the cluster with the new one (`ClusterTest.java:1892-1895`).
    pub fn set_channel_shape(&mut self, shape: ChannelShape) {
        self.shape = shape;
    }

    fn mtu_parameter(&self) -> String {
        self.shape
            .mtu
            .map(|mtu| format!("|mtu={mtu}"))
            .unwrap_or_default()
    }

    /// `aeron:udp?term-length=…|alias=raft` (`TestCluster.java:145`).
    pub fn log_channel(&self) -> String {
        format!(
            "aeron:udp?term-length={}|alias=raft{}",
            self.shape.log_term_length,
            self.mtu_parameter()
        )
    }

    /// The egress channel's **parameters**, as a template for the module
    /// (`TestCluster.java:147`). No `endpoint`: the reference's client names
    /// `endpoint=localhost:0` and its response channel is rewritten by the
    /// driver through `response-correlation-id`; this harness's client names a
    /// fixed endpoint of its own, and `refine_response_channel` would overwrite
    /// it.
    pub fn egress_channel_template(&self) -> String {
        format!(
            "aeron:udp?term-length={}{}",
            self.shape.egress_term_length,
            self.mtu_parameter()
        )
    }

    /// `hostname(memberId):2<clusterId>11<memberId>` (`:1924-1927`).
    pub fn ingress(&self, member_id: i32) -> u16 {
        self.base + u16::try_from(member_id).unwrap()
    }

    /// `:2<clusterId>22<memberId>` (`:1890`).
    pub fn consensus(&self, member_id: i32) -> u16 {
        self.base + 10 + u16::try_from(member_id).unwrap()
    }

    /// `:2<clusterId>33<memberId>` (`:1892`) — the log destination.
    pub fn log(&self, member_id: i32) -> u16 {
        self.base + 20 + u16::try_from(member_id).unwrap()
    }

    /// Where the client's egress subscription listens.
    pub fn client_egress(&self) -> u16 {
        self.base + 30 + self.egress_offset
    }

    /// Where this member's archive listens for control requests
    /// (`TestCluster`'s archive scheme). Log replication dials it, so unlike
    /// the catchup endpoint it cannot be a port the kernel picks.
    pub fn archive(&self, member_id: i32) -> u16 {
        self.base + 40 + u16::try_from(member_id).unwrap()
    }

    /// Where a backup's own consensus **subscription** listens
    /// (`TestCluster.java:591-592`: the consensus channel with the backup's
    /// endpoint on it).
    pub fn backup_consensus(&self) -> u16 {
        self.base + 43
    }

    /// Where a backup's archive receives the cluster's log and snapshots —
    /// `aeron.cluster.backup.catchup.endpoint` (`TestCluster.java:599`).
    pub fn backup_catchup(&self) -> u16 {
        self.base + 44
    }

    pub fn ingress_channel(&self, member_id: i32) -> String {
        format!(
            "aeron:udp?term-length={}|alias=ingress|endpoint=localhost:{}{}",
            self.shape.ingress_term_length,
            self.ingress(member_id),
            self.mtu_parameter()
        )
    }

    /// The client's request publication, which is the ingress channel's shape
    /// with the leader's endpoint (`TestCluster.java:856-858`: the reference
    /// client is configured with the ingress channel itself).
    pub fn ingress_publication_channel(&self, member_id: i32) -> String {
        format!(
            "aeron:udp?term-length={}|endpoint=localhost:{}{}",
            self.shape.ingress_term_length,
            self.ingress(member_id),
            self.mtu_parameter()
        )
    }

    pub fn consensus_channel(&self, member_id: i32) -> String {
        format!(
            "aeron:udp?alias=consensus|endpoint=localhost:{}",
            self.consensus(member_id)
        )
    }

    /// A copy of these ports whose egress port is moved by `offset`.
    #[must_use]
    pub fn with_egress_offset(&self, offset: u16) -> Self {
        Self {
            base: self.base,
            egress_offset: offset,
            shape: self.shape,
        }
    }

    pub fn client_egress_channel(&self) -> String {
        format!(
            "aeron:udp?term-length={}|endpoint=localhost:{}{}",
            self.shape.egress_term_length,
            self.client_egress(),
            self.mtu_parameter()
        )
    }
}

impl Default for Ports {
    fn default() -> Self {
        Self::new()
    }
}

/// `id,ingress,consensus,log,catchup,archive` per member
/// (`ClusterMember.java:561-564`; `common/member.rs`).
///
/// The archive endpoint is where **log replication** finds the leader's
/// archive, so it is a real port (`Ports::archive`); a follower's catchup
/// endpoint is resolved at run time and stays `:0`.
pub fn members(ports: &Ports) -> String {
    (0..MEMBER_COUNT)
        .map(|id| {
            format!(
                "{id},localhost:{},localhost:{},localhost:{},localhost:0,localhost:{}",
                ports.ingress(id),
                ports.consensus(id),
                ports.log(id),
                ports.archive(id),
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// Failures a node thread reports after it has launched, so the driving test
/// fails with the node's own words rather than a timeout.
#[derive(Clone, Default)]
pub struct Faults {
    recorded: Arc<Mutex<Vec<String>>>,
    /// Failures a case has said it **expects** — an error path it is testing.
    /// A recorded message containing one of these is kept in
    /// [`Self::snapshot`] but does not fail [`Self::check`].
    expected: Arc<Mutex<Vec<String>>>,
}

impl Faults {
    /// Declare that `pattern` (a substring) is an expected failure.
    ///
    /// The reference's own refusal cases are about a member (or a backup)
    /// failing on purpose — invalid credentials, say — and a harness that
    /// panics on the very failure the case is asserting would make the case
    /// unportable.
    pub fn expect(&self, pattern: &str) {
        self.expected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(pattern.to_owned());
    }

    pub fn record(&self, message: String) {
        self.recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(message);
    }

    fn is_expected(&self, message: &str) -> bool {
        self.expected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|pattern| message.contains(pattern))
    }

    pub fn check(&self) {
        let first = self
            .recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|message| !self.is_expected(message))
            .cloned();
        if let Some(first) = first {
            panic!("a consensus module failed: {first}");
        }
    }

    /// The recorded failures, for an assertion's message.
    pub fn snapshot(&self) -> Vec<String> {
        self.recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// One member: its driver+archive process, its directories, and its module
/// thread.
///
/// A stopped node keeps its directories and its driver, so [`Self::restart`]
/// brings the module back on the same log and the same archive — which is what
/// the follower paths need (risk 8 in the P3-6b plan: the module's `close` must
/// have run, or the driver refuses the same cluster id).
pub struct Node {
    id: i32,
    /// The driver's name, which with the process id is its aeron directory —
    /// kept so [`Self::restart_whole`] recreates the same path.
    name: String,
    cluster_dir: TempDir,
    archive_dir: TempDir,
    /// The in-process test container, when this node runs one. `None` is the
    /// zero-service arrangement `zero_alloc` and the three-node harness use.
    service: Option<InProcessService>,
    /// What the test business did, kept beside the container rather than in
    /// it: a case reads it **after** the node has been stopped, which is when
    /// a clean shutdown's snapshots are the thing being asserted.
    service_state: Option<Arc<TestServiceState>>,
    /// The test business's name, kept so a restart starts the same one.
    service_name: Option<&'static str>,
    /// The member's authenticator, when a case supplies one — the equivalent
    /// of the reference's `authenticatorSupplier` (`ConsensusModule.java:807`).
    authenticator: Option<TestAuthenticator>,
    /// `None` only while [`Self::restart_whole`] is between two drivers: the
    /// old one has to be **dropped** before the new one starts, because its
    /// `Drop` removes the aeron directory and the log — the same path the new
    /// one has just created.
    driver: Option<OwnArchivingMediaDriver>,
    /// The driver's own property list, kept so [`Self::restart_whole`] starts
    /// the same archive again over the same directory.
    driver_properties: Vec<String>,
    /// The properties the module is launched with, kept so a restart settles
    /// the same configuration from the same values.
    args: Vec<String>,
    faults: Faults,
    stop: Arc<AtomicBool>,
    /// Set by the module's own thread when it ends — a clean `SHUTDOWN`/`ABORT`
    /// ends the duty cycle from the inside, which [`Self::stop`] cannot tell
    /// from a module still running.
    finished: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// One in-process test container: its stop handle, its thread, and what its
/// business did.
struct InProcessService {
    shutdown: deepmsg_cluster_service::ShutdownHandle,
    thread: std::thread::JoinHandle<Result<(), String>>,
}

impl Node {
    /// Start the driver and launch the module. `None` when the driver cannot
    /// start, which is how a test announces a skip.
    pub fn start(
        id: i32,
        ports: &Ports,
        member_string: &str,
        faults: &Faults,
        launched: &mpsc::Sender<(i32, Result<(), String>)>,
    ) -> Option<Self> {
        Self::start_inner(id, ports, member_string, faults, launched, None, None, None)
    }

    /// The same, with one in-process test container (`test-service` or
    /// `sequence-service`) on this member.
    ///
    /// A member with a container is a different cluster configuration, not a
    /// different parameter: `aeron.cluster.service.count` decides whether
    /// `launch` waits for a `JoinLog` ack, so the two arrangements are started
    /// by the two named constructors rather than by a boolean a reader would
    /// have to chase.
    pub fn start_with_service(
        id: i32,
        ports: &Ports,
        member_string: &str,
        faults: &Faults,
        launched: &mpsc::Sender<(i32, Result<(), String>)>,
        service: &'static str,
    ) -> Option<Self> {
        Self::start_inner(
            id,
            ports,
            member_string,
            faults,
            launched,
            Some(service),
            None,
            None,
        )
    }

    /// The same, with an authenticator — a cluster that refuses a session whose
    /// credentials it does not accept.
    pub fn start_with_service_and_authenticator(
        id: i32,
        ports: &Ports,
        member_string: &str,
        faults: &Faults,
        launched: &mpsc::Sender<(i32, Result<(), String>)>,
        service: &'static str,
        authenticator: TestAuthenticator,
    ) -> Option<Self> {
        Self::start_inner(
            id,
            ports,
            member_string,
            faults,
            launched,
            Some(service),
            Some(authenticator),
            None,
        )
    }

    /// Start a member over directories that already exist — the reference's
    /// `startStaticNodeFromBackup` (`TestCluster.java:623-687`): a cluster of
    /// one over the backup's archive and cluster directories.
    ///
    /// The directories are passed in so ownership moves to this node: the
    /// backup they came from must have been stopped first, and its `TempDir`s
    /// are what is left when it is told to hand them over.
    #[allow(clippy::too_many_arguments)] // one per thing a case varies
    pub fn start_over_directories(
        id: i32,
        ports: &Ports,
        member_string: &str,
        faults: &Faults,
        launched: &mpsc::Sender<(i32, Result<(), String>)>,
        service: Option<&'static str>,
        archive_dir: TempDir,
        cluster_dir: TempDir,
    ) -> Option<Self> {
        Self::start_inner(
            id,
            ports,
            member_string,
            faults,
            launched,
            service,
            None,
            Some((archive_dir, cluster_dir)),
        )
    }

    #[allow(clippy::too_many_arguments)] // one per thing a case varies
    fn start_inner(
        id: i32,
        ports: &Ports,
        member_string: &str,
        faults: &Faults,
        launched: &mpsc::Sender<(i32, Result<(), String>)>,
        service: Option<&'static str>,
        authenticator: Option<TestAuthenticator>,
        directories: Option<(TempDir, TempDir)>,
    ) -> Option<Self> {
        // The aeron directory is the process id **and** this name
        // (`temp_aeron_dir`), and `cargo test` runs a binary's cases on
        // threads of one process: a name without the port block gives two
        // cases' member 0 one directory, and the second driver's
        // `dir.delete.on.start` meets `ENOTEMPTY`. The block is unique per
        // case, so it is the part of the name that keeps them apart.
        let name = format!("cluster-node-{id}-{}", ports.base());
        let (archive_dir, cluster_dir) = directories.unwrap_or_else(|| {
            (
                TempDir::new(&format!("{name}-archive")),
                TempDir::new(&format!("{name}-cluster")),
            )
        });

        // The archive's control channel is given a fixed endpoint: a follower
        // replicating from this member's archive dials the endpoint in the
        // member string, and `localhost:0` would name a port the kernel picked
        // in another process.
        let mut properties: Vec<String> = archive::PROPERTIES
            .iter()
            .map(|property| (*property).to_owned())
            .collect();
        properties.push(format!(
            "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:{}",
            ports.archive(id)
        ));
        let property_refs: Vec<&str> = properties.iter().map(String::as_str).collect();

        let mut driver = OwnArchivingMediaDriver::start(&name, archive_dir.path(), &property_refs)?;
        driver.await_ready(DEADLINE).expect("the driver comes up");

        let mut args = vec![
            format!("-Daeron.dir={}", driver.aeron_dir().display()),
            format!("-Daeron.cluster.dir={}", cluster_dir.path().display()),
            format!("-Daeron.cluster.id={CLUSTER_ID}"),
            format!("-Daeron.cluster.member.id={id}"),
            format!("-Daeron.cluster.members={member_string}"),
            format!(
                "-Daeron.cluster.ingress.channel={}",
                ports.ingress_channel(id)
            ),
            format!(
                "-Daeron.cluster.consensus.channel={}",
                ports.consensus_channel(id)
            ),
            // No endpoint: the reference calls that shape MDC
            // (`ConsensusModule.java:2145`), and the module puts
            // `control-mode=manual` on the publication itself.
            format!("-Daeron.cluster.log.channel={}", ports.log_channel()),
            format!(
                "-Daeron.cluster.egress.channel={}",
                ports.egress_channel_template()
            ),
            "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k".to_owned(),
            "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0".to_owned(),
            // The election's attempt budget, raised from the reference's
            // ten-second default: this harness runs three consensus modules,
            // three archiving media drivers (each a process) and a client
            // driver over one machine, and a CI runner has four virtual cores.
            // A catch-up attempt that misses the budget is **retried from
            // `INIT`**, which throws away the replay, the log subscription and
            // the recording start — so a budget that is too short turns one
            // slow step into a loop. The reference's own `TestCluster` exposes
            // this as `withLeaderHeartbeatTimeoutNs`.
            "-Daeron.cluster.leader.heartbeat.timeout=30s".to_owned(),
        ];
        match service {
            None => args.push("-Daeron.cluster.service.count=0".to_owned()),
            Some(name) => {
                // One container, on the control channel this harness already
                // gives the module. The stream ids and channels are the
                // reference's defaults spelled out, so the container and the
                // module cannot settle them from different tables.
                args.extend([
                    "-Daeron.cluster.service.count=1".to_owned(),
                    "-Daeron.cluster.service.id=0".to_owned(),
                    format!("-Ddeepmsg.cluster.service={name}"),
                    "-Daeron.cluster.service.responder=true".to_owned(),
                    "-Daeron.cluster.service.stream.id=104".to_owned(),
                    "-Daeron.cluster.consensus.module.stream.id=105".to_owned(),
                    "-Daeron.cluster.replay.channel=aeron:ipc".to_owned(),
                    "-Daeron.cluster.replay.stream.id=103".to_owned(),
                    "-Daeron.cluster.snapshot.channel=aeron:ipc?alias=snapshot".to_owned(),
                    "-Daeron.cluster.snapshot.stream.id=106".to_owned(),
                    "-Daeron.archive.local.control.channel=aeron:ipc?term-length=64k".to_owned(),
                    "-Daeron.archive.local.control.stream.id=10".to_owned(),
                    "-Daeron.archive.control.response.stream.id=20".to_owned(),
                ]);
            }
        }

        let mut node = Self {
            id,
            name: name.clone(),
            cluster_dir,
            archive_dir,
            service: None,
            service_state: None,
            service_name: service,
            authenticator,
            driver: Some(driver),
            driver_properties: properties,
            args,
            faults: faults.clone(),
            stop: Arc::new(AtomicBool::new(false)),
            finished: Arc::new(AtomicBool::new(false)),
            thread: None,
        };
        node.launch_thread(launched);
        Some(node)
    }

    /// Launch the module on this node's directories and driver, in a thread.
    fn launch_thread(&mut self, launched: &mpsc::Sender<(i32, Result<(), String>)>) {
        let mut properties = Properties::new();
        properties
            .apply_args(self.args.clone())
            .expect("the properties parse");
        let mut config = ConsensusModuleConfig::from_properties(&properties)
            .expect("the module's configuration settles");
        // This build has no class loader; the flag is the programmatic
        // extension seam a zero-service cluster needs (`ConsensusModule.java:2158`).
        // A member with a container has no such gap.
        config.consensus_module_extension = self.service_name.is_none();
        let idle_strategy = config.idle_strategy.clone();

        let id = self.id;
        let stop = Arc::new(AtomicBool::new(false));
        self.stop = Arc::clone(&stop);
        let finished = Arc::new(AtomicBool::new(false));
        self.finished = Arc::clone(&finished);
        let faults = self.faults.clone();
        let sender = launched.clone();
        let authenticator = self.authenticator.clone();

        self.thread = Some(std::thread::spawn(move || {
            let Some(mut idle) = Strategy::by_agrona_name(&idle_strategy) else {
                faults.record(format!("node {id}: unknown idle strategy {idle_strategy}"));
                return;
            };

            // Blocks until the election has closed for this member: the
            // leader's log recording, or the follower's join of the log.
            let mut agent = match ConsensusModuleAgent::launch_with_components(
                config,
                LaunchComponents {
                    authenticator: authenticator
                        .map(|authenticator| Box::new(authenticator) as Box<dyn Authenticator>),
                    ..LaunchComponents::default()
                },
            ) {
                Ok(agent) => agent,
                Err(error) => {
                    let _ = sender.send((id, Err(error.to_string())));
                    return;
                }
            };
            let _ = sender.send((id, Ok(())));

            // An `AgentTermination` is how an orderly `SHUTDOWN`/`ABORT` ends a
            // module (`CMA:403-478`): the duty cycle returns it and the node is
            // meant to stop. Anything else is the fault the harness records.
            while !stop.load(Ordering::Relaxed) {
                match agent.do_work() {
                    Ok(work) => idle.idle(work),
                    Err(error) if error.is_termination() => break,
                    Err(error) => {
                        faults.record(format!("node {id}: the duty cycle: {error}"));
                        break;
                    }
                }
            }

            if let Err(error) = agent.close() {
                faults.record(format!("node {id}: close: {error}"));
            }

            finished.store(true, Ordering::Relaxed);
        }));

        self.launch_service();
    }

    /// Start the in-process container, when this node has one.
    ///
    /// After the module's thread, not before: the module's `launch` is what
    /// waits for the container's `JoinLog` ack, and it blocks in the thread
    /// above while this one starts the container that answers it — the same
    /// arrangement `crate::cluster`'s in-process arm uses, with the roles of
    /// the two threads swapped.
    fn launch_service(&mut self) {
        let Some(name) = self.service_name else {
            return;
        };

        let mut properties = Properties::new();
        properties
            .apply_args(self.args.clone())
            .expect("the container's properties parse");
        let config = ServiceContainerConfig::from_properties(&properties)
            .expect("the container's configuration settles");
        let (service, state) = in_process_test_service(name);
        let (started_tx, started_rx) =
            mpsc::channel::<Result<deepmsg_cluster_service::ShutdownHandle, String>>();
        let id = self.id;
        let faults = self.faults.clone();
        let running_faults = self.faults.clone();

        let thread = std::thread::spawn(move || -> Result<(), String> {
            // The reference's in-process container is given a client it does
            // not own: ending the container must not take the driver's
            // counters with it (`Client::detach`).
            let mut agent =
                match ClusteredServiceAgent::launch_with_client_ownership(config, service, false) {
                    Ok(agent) => agent,
                    Err(error) => {
                        let message = error.to_string();
                        let _ = started_tx.send(Err(message.clone()));
                        return Err(message);
                    }
                };
            let shutdown = agent.shutdown_handle();
            let _ = started_tx.send(Ok(shutdown));

            let mut idle = Strategy::by_agrona_name("backoff").expect("backoff");
            loop {
                match agent.do_work() {
                    Ok(work) => idle.idle(work),
                    Err(deepmsg_cluster_service::ContainerError::AgentTermination(_)) => break,
                    Err(error) => {
                        let _ = agent.close();
                        running_faults.record(format!("node {id}: the container: {error}"));
                        return Err(error.to_string());
                    }
                }
            }
            agent.close().map_err(|error| error.to_string())
        });

        // A container that never comes up leaves the module waiting for its
        // ack; the wait has a budget of its own, but a fault here is the
        // clearer report.
        match started_rx.recv_timeout(DEADLINE) {
            Ok(Ok(shutdown)) => {
                self.service_state = Some(Arc::clone(&state));
                self.service = Some(InProcessService { shutdown, thread });
            }
            Ok(Err(error)) => {
                faults.record(format!("node {id}: the container did not start: {error}"));
            }
            Err(error) => {
                faults.record(format!(
                    "node {id}: the container never reported its start: {error}"
                ));
            }
        }
    }

    /// The test business's observables, when this node runs one in process.
    ///
    /// Kept after [`Self::stop`] has ended the container: a case that asserts
    /// on what a shutdown's snapshot left behind reads it then.
    #[must_use]
    pub fn service_state(&self) -> Option<Arc<TestServiceState>> {
        self.service_state.clone()
    }

    /// The archive directory this node owns.
    pub fn archive_dir(&self) -> &std::path::Path {
        self.archive_dir.path()
    }

    /// Stop the module and the **driver**, keeping every directory.
    ///
    /// H5's construction needs the media down before the mark files go: an
    /// archive still running would rewrite its mark and catalog after the
    /// test had deleted them.
    pub fn stop_media(&mut self) {
        self.stop();
        if let Some(mut driver) = self.driver.take() {
            driver.stop().expect("the driver stops");
        }
    }

    /// Re-settle the module's three channel arguments from `ports`.
    ///
    /// The channels are the one part of a launch this harness lets a test
    /// change between restarts (`ClusterTest.java:1892-1895`), so they are
    /// re-read rather than frozen at [`Self::start`]. Everything else — the
    /// directories, the member list, the stream ids — settles from the same
    /// values.
    pub fn set_channel_shape(&mut self, ports: &Ports) {
        let id = self.id;
        let replacements = [
            (
                "-Daeron.cluster.ingress.channel=",
                ports.ingress_channel(id),
            ),
            ("-Daeron.cluster.log.channel=", ports.log_channel()),
            (
                "-Daeron.cluster.egress.channel=",
                ports.egress_channel_template(),
            ),
        ];
        for (prefix, value) in replacements {
            if let Some(arg) = self.args.iter_mut().find(|arg| arg.starts_with(prefix)) {
                *arg = format!("{prefix}{value}");
            }
        }
    }

    /// Bring the **driver and the module** back over the same directories.
    ///
    /// The reference's `restartAllNodes` (`StartFromTruncatedRecordingLogTest`)
    /// restarts the whole cluster because the archive and cluster mark files
    /// are deleted while nothing holds them; a module-only restart leaves the
    /// running archive's view of its own mark file in place. The driver's
    /// aeron directory is derived from the process id and the node's name, so
    /// it is the same path and the module's `-Daeron.dir` stays valid.
    pub fn restart_whole(&mut self, launched: &mpsc::Sender<(i32, Result<(), String>)>) {
        self.stop();
        // Stop and **drop** the old driver first: its `Drop` removes the aeron
        // directory and the log file, which are exactly the paths the new
        // driver creates — an assignment would drop it after the new one had
        // started and take the new driver's CnC file with it.
        if let Some(mut driver) = self.driver.take() {
            driver.stop().expect("the driver stops");
        }
        let refs: Vec<&str> = self.driver_properties.iter().map(String::as_str).collect();
        let name = self.name.clone();
        let mut driver = OwnArchivingMediaDriver::start(&name, self.archive_dir.path(), &refs)
            .expect("the driver comes back up");
        driver.await_ready(DEADLINE).expect("the driver is ready");
        self.driver = Some(driver);
        self.launch_thread(launched);
    }

    /// Bring a stopped module back on the same directory, archive and driver.
    ///
    /// The previous agent's `close` has run (that is what [`Self::stop`] waits
    /// for), so the driver's counters for the old module are gone and the same
    /// cluster id can be taken again.
    pub fn restart(&mut self, launched: &mpsc::Sender<(i32, Result<(), String>)>) {
        assert!(self.is_stopped(), "a running node cannot be restarted");
        self.launch_thread(launched);
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn aeron_dir(&self) -> &std::path::Path {
        self.driver
            .as_ref()
            .expect("the driver is running")
            .aeron_dir()
    }

    /// The last `lines` lines the node's driver+archive process wrote.
    #[must_use]
    pub fn driver_log_tail(&self, lines: usize) -> String {
        self.driver
            .as_ref()
            .map_or_else(|| "<no driver>".to_owned(), |driver| driver.log_tail(lines))
    }

    pub fn cluster_dir(&self) -> &std::path::Path {
        self.cluster_dir.path()
    }

    /// Stop the module — "the follower dies" — with the media left running so
    /// the restart path has something to come back to.
    ///
    /// The module first, with its container still running: a module stopped in
    /// the middle of `awaitServicesReady` is waiting for an ack only a live
    /// container can send, and joining it before the container goes is a
    /// fixture that hangs on the last line of a passing case. The container is
    /// stopped (not abandoned) right after: a client of the module that is
    /// still polling when the module's counters go is the failure
    /// `cluster_lc4a_snapshot_failure` caught.
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);

        // A module that does not come back within the budget is a fault, not a
        // join that never returns: a case that hangs teaches nothing, and the
        // thread it would wait for is one this build's duty cycle is stuck
        // inside (a blocking archive call, or an `awaitServicesReady` whose
        // ack cannot arrive).
        let deadline = Instant::now() + DEADLINE;
        while !self.finished.load(Ordering::Relaxed) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        if !self.finished.load(Ordering::Relaxed) {
            self.faults
                .record(format!("node {}: the module did not stop", self.id));
            let _ = self.thread.take();
            return;
        }

        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }

        if let Some(service) = self.service.take() {
            service.shutdown.stop();
            match service.thread.join() {
                Ok(Ok(())) | Err(_) => {}
                Ok(Err(error)) => self
                    .faults
                    .record(format!("node {}: the container: {error}", self.id)),
            }
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.thread.is_none()
    }

    /// Whether the module's thread has ended on its own — a `SHUTDOWN`/`ABORT`
    /// rather than [`Self::stop`].
    #[must_use]
    pub fn has_finished(&self) -> bool {
        self.finished.load(Ordering::Relaxed)
    }

    /// The reference's construction for starting from a truncated log
    /// (`StartFromTruncatedRecordingLogTest.truncateRecordingLogAndDeleteMarkFiles`):
    /// rewrite `recording.log` down to the last term entry plus the latest
    /// snapshots, delete the two mark files, and drop the `.rec` segments the
    /// rewritten log no longer names.
    ///
    /// The module and the driver must both be stopped: the archive holds its
    /// own mark file and catalog open, and the whole point is that the next
    /// start rebuilds them.
    pub fn truncate_recording_log(&self) -> Result<(), String> {
        use deepmsg_cluster::persist::recording_log::{self, RecordingLog};

        let log_path = self.cluster_dir.path().join(recording_log::FILENAME);
        let old = RecordingLog::open(&log_path).map_err(|error| error.to_string())?;
        let last_term = old
            .find_last_term()
            .cloned()
            .ok_or_else(|| "recording.log has no term entry".to_owned())?;
        let mut snapshots: Vec<_> = old
            .entries()
            .iter()
            .filter(|entry| entry.is_snapshot())
            .cloned()
            .collect();
        snapshots.sort_by_key(|entry| entry.position);
        let snapshots = &snapshots[snapshots.len().saturating_sub(2)..];
        drop(old);

        let new_path = self.cluster_dir.path().join("recording.log.truncated");
        let _ = std::fs::remove_file(&new_path);
        let mut truncated = RecordingLog::open(&new_path).map_err(|error| error.to_string())?;
        truncated
            .append_term(
                last_term.leadership_term_id,
                last_term.term_base_log_position,
                last_term.log_position,
                last_term.timestamp,
                last_term.recording_id,
                last_term.service_id,
            )
            .map_err(|error| error.to_string())?;
        for snapshot in snapshots {
            truncated
                .append_snapshot(
                    snapshot.leadership_term_id,
                    snapshot.term_base_log_position,
                    snapshot.log_position,
                    snapshot.timestamp,
                    snapshot.recording_id,
                    snapshot.service_id,
                )
                .map_err(|error| error.to_string())?;
        }
        truncated.force(1).map_err(|error| error.to_string())?;
        drop(truncated);
        std::fs::rename(&new_path, &log_path).map_err(|error| error.to_string())?;

        // The mark files are what the restarted archive and module write afresh.
        for path in [
            self.cluster_dir.path().join("cluster-mark.dat"),
            self.archive_dir.path().join("archive-mark.dat"),
        ] {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(format!("{}: {error}", path.display())),
            }
        }

        // Segments no entry names any more are the recordings the truncation
        // just dropped; the reference deletes them for the same reason.
        let rewritten = RecordingLog::open(&log_path).map_err(|error| error.to_string())?;
        let referenced: Vec<i64> = rewritten.entries().iter().map(|e| e.recording_id).collect();
        drop(rewritten);
        let entries =
            std::fs::read_dir(self.archive_dir.path()).map_err(|error| error.to_string())?;
        for entry in entries {
            let entry = entry.map_err(|error| error.to_string())?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(recording_id) = name
                .strip_suffix(".rec")
                .and_then(|prefix| prefix.split('-').next())
                .and_then(|id| id.parse::<i64>().ok())
            else {
                continue;
            };
            if !referenced.contains(&recording_id) {
                let _ = std::fs::remove_file(entry.path());
            }
        }

        Ok(())
    }

    /// The reference's `ClusterTool.seedRecordingLogFromSnapshot`
    /// (`ClusterToolOperator.java:201-270`): rewrite `recording.log` down to
    /// the latest snapshot **and its services'**, with every entry's term base
    /// and log position set to zero, keeping the original as
    /// `recording.log.bak`.
    ///
    /// This is H5's second arm's `seedRecordingsFromLatestSnapshot`
    /// (`TestCluster.java:2449-2456`). Unlike [`Self::truncate_recording_log`]
    /// it neither keeps the last term entry nor deletes the mark files: the
    /// cluster is meant to come back with **no log at all**, only the snapshot
    /// state, which is what lets the next term be recorded with a different
    /// term buffer length and MTU (`ClusterTest.java:1823-1933`).
    ///
    /// The module and the driver must both be stopped, as for the truncation:
    /// the next start is what reads the rewritten log.
    pub fn seed_recording_log_from_snapshot(&self) -> Result<(), String> {
        use deepmsg_cluster::persist::recording_log::{
            self, CONSENSUS_MODULE_SERVICE_ID, ENTRY_TYPE_SNAPSHOT, RecordingLog,
        };

        let log_path = self.cluster_dir.path().join(recording_log::FILENAME);
        let original = RecordingLog::open(&log_path).map_err(|error| error.to_string())?;
        let entries = original.entries().to_vec();
        drop(original);

        // The latest snapshot of the module itself, then the ones services
        // wrote immediately before it, by service id. `isValidSnapshot` is
        // `isValid && type == SNAPSHOT` (`RecordingLog.java:1354-1357`), which
        // is *not* `Entry::is_snapshot` — that also admits standby snapshots.
        let module_snapshot = entries.iter().rposition(|entry| {
            entry.is_valid
                && entry.entry_type == ENTRY_TYPE_SNAPSHOT
                && entry.service_id == CONSENSUS_MODULE_SERVICE_ID
        });

        let seeded: Vec<_> = match module_snapshot {
            None => Vec::new(),
            Some(index) => {
                let mut seeded = Vec::new();
                let mut service_id = CONSENSUS_MODULE_SERVICE_ID;
                let mut at = Some(index);
                while let Some(i) = at {
                    let entry = &entries[i];
                    if !(entry.is_valid
                        && entry.entry_type == ENTRY_TYPE_SNAPSHOT
                        && entry.service_id == service_id)
                    {
                        break;
                    }
                    let mut zeroed = entry.clone();
                    zeroed.term_base_log_position = 0;
                    zeroed.log_position = 0;
                    zeroed.archive_endpoint = None;
                    seeded.push(zeroed);
                    service_id += 1;
                    at = i.checked_sub(1);
                }
                seeded.reverse();
                seeded
            }
        };

        let backup = self.cluster_dir.path().join("recording.log.bak");
        std::fs::copy(&log_path, &backup).map_err(|error| error.to_string())?;

        let new_path = self.cluster_dir.path().join("recording.log.seeded");
        let _ = std::fs::remove_file(&new_path);
        let mut log = RecordingLog::open(&new_path).map_err(|error| error.to_string())?;
        for entry in &seeded {
            log.append_snapshot(
                entry.leadership_term_id,
                entry.term_base_log_position,
                entry.log_position,
                entry.timestamp,
                entry.recording_id,
                entry.service_id,
            )
            .map_err(|error| error.to_string())?;
        }
        log.force(1).map_err(|error| error.to_string())?;
        drop(log);
        std::fs::rename(&new_path, &log_path).map_err(|error| error.to_string())?;

        Ok(())
    }

    /// `recording.log` as `type:term:recording:base:position` per entry — a
    /// failure's way to say what the recovery plan was built from.
    #[must_use]
    pub fn recording_log_entries(&self) -> String {
        use deepmsg_cluster::persist::recording_log::{self, RecordingLog};

        let path = self.cluster_dir.path().join(recording_log::FILENAME);
        match RecordingLog::open(&path) {
            Ok(log) => log
                .entries()
                .iter()
                .map(|entry| {
                    format!(
                        "{}:{}:{}:{}:{}",
                        entry.entry_type,
                        entry.leadership_term_id,
                        entry.recording_id,
                        entry.term_base_log_position,
                        entry.log_position
                    )
                })
                .collect::<Vec<_>>()
                .join(","),
            Err(error) => format!("<{error}>"),
        }
    }

    /// The last term in this member's `recording.log` — the entry a
    /// `BackupResponse` reports (`ConsensusPublisher.java:495-500` fills its
    /// five log fields from `RecordingLog.findLastTerm`).
    ///
    /// `None` when the log has no term, or cannot be opened.
    pub fn last_term(&self) -> Option<LastTerm> {
        use deepmsg_cluster::persist::recording_log::{self, RecordingLog};

        let path = self.cluster_dir.path().join(recording_log::FILENAME);
        let log = RecordingLog::open(&path).ok()?;
        log.find_last_term().map(|entry| LastTerm {
            recording_id: entry.recording_id,
            leadership_term_id: entry.leadership_term_id,
            term_base_log_position: entry.term_base_log_position,
        })
    }

    /// The term buffer length and MTU the archive recorded the **current log
    /// recording** with.
    ///
    /// The descriptor is where H5's second arm reads "the channels changed":
    /// the log recording made before the restart is 256 KiB / 1408, the one
    /// made after it is 2 MiB / 8992 (`Archive::list_recording`, the same query
    /// `crates/cluster/consensus/src/agent/mod.rs` builds its recovery plan
    /// from).
    ///
    /// A second client on the node's own aeron directory and an archive
    /// control session over IPC — the shape the module itself uses
    /// (`crates/cluster/consensus/src/config.rs:481-487`), with the response
    /// stream left at its default (the module offsets its own by
    /// `clusterId * 100 + 100`).
    pub fn log_recording_shape(&self) -> Result<RecordingShape, String> {
        use deepmsg_archive::client::{
            Archive as ArchiveClient, ArchiveContext, Handlers, NoCredentials,
        };
        use deepmsg_cluster::persist::recording_log::{self, RecordingLog};

        let log_path = self.cluster_dir.path().join(recording_log::FILENAME);
        let log = RecordingLog::open(&log_path).map_err(|error| error.to_string())?;
        let term = log
            .find_last_term()
            .ok_or_else(|| "recording.log has no term entry".to_owned())?;
        let (leadership_term_id, recording_id) = (term.leadership_term_id, term.recording_id);
        drop(log);

        let mut client = Client::connect(self.aeron_dir()).map_err(|error| error.to_string())?;
        let mut context = ArchiveContext::new();
        context.aeron_dir = self.aeron_dir().to_string_lossy().into_owned();
        context.control_request_channel = Some("aeron:ipc".to_owned());
        context.control_response_channel = Some("aeron:ipc".to_owned());
        let mut archive = ArchiveClient::connect(
            &context,
            &mut client,
            &mut NoCredentials,
            Handlers::default(),
        )
        .map_err(|error| format!("{error:?}"))?;

        let mut shape = None;
        archive
            .list_recording(&mut client, recording_id, &mut |descriptor| {
                shape = Some(RecordingShape {
                    recording_id,
                    leadership_term_id,
                    term_buffer_length: descriptor.term_buffer_length,
                    mtu_length: descriptor.mtu_length,
                    stop_position: descriptor.stop_position,
                });
            })
            .map_err(|error| format!("{error:?}"))?;

        shape.ok_or_else(|| format!("the archive does not know recording {recording_id}"))
    }
}

/// What [`Node::last_term`] read: the member's own last recording-log entry.
#[derive(Clone, Copy, Debug)]
pub struct LastTerm {
    /// The recording the term lives in.
    pub recording_id: i64,
    /// The term itself.
    pub leadership_term_id: i64,
    /// Where the term starts in the log.
    pub term_base_log_position: i64,
}

/// What [`Node::log_recording_shape`] read: the term the recording holds, and
/// the shape it was made with.
#[derive(Clone, Copy, Debug)]
pub struct RecordingShape {
    /// The log recording.
    pub recording_id: i64,
    /// The leadership term whose recording this is.
    pub leadership_term_id: i64,
    /// `term_buffer_length` from the recording's descriptor.
    pub term_buffer_length: i32,
    /// `mtu_length` from the recording's descriptor.
    pub mtu_length: i32,
    /// How far the recording has been written; for a live log this is the
    /// member's applied position.
    pub stop_position: i64,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The cluster counters this harness reads from a member's driver.
pub struct NodeCounters {
    cnc: CncFile,
    aeron_dir: std::path::PathBuf,
}

impl NodeCounters {
    pub fn open(aeron_dir: &std::path::Path) -> Self {
        Self {
            cnc: CncFile::open(aeron_dir, DEADLINE).expect("the node's CnC file"),
            aeron_dir: aeron_dir.to_path_buf(),
        }
    }

    pub fn value(&self, type_id: i32) -> Option<i64> {
        let counters = self.cnc.counters()?;
        let id = find_cluster_counter(&counters, type_id, CLUSTER_ID)?;
        counters.value(id)
    }

    /// The value at a counter **id** the cluster named — a `BackupResponse`
    /// carries the commit-position counter's id, not its value, and this is how
    /// a test reads what the id points at.
    pub fn counter_value(&self, counter_id: i32) -> Option<i64> {
        self.cnc.counters()?.value(counter_id)
    }

    /// Write one of the module's counters — how an operator drives the cluster
    /// without an operator tool: the control toggle is a counter, and a
    /// `SHUTDOWN` is that counter set to its code (`ClusterControl`).
    pub fn set(&self, type_id: i32, value: i64) {
        let id = {
            let counters = self.cnc.counters().expect("counters");
            find_cluster_counter(&counters, type_id, CLUSTER_ID).expect("the counter")
        };
        CncFile::open_writable(&self.aeron_dir, DEADLINE)
            .expect("the node's CnC file, writable")
            .counters_writable()
            .expect("writable counters")
            .set_value(id, value)
            .expect("the counter is written");
    }

    pub fn role(&self) -> Option<i64> {
        self.value(CLUSTER_NODE_ROLE_TYPE_ID)
    }

    pub fn commit_position(&self) -> Option<i64> {
        self.value(CLUSTER_COMMIT_POSITION_TYPE_ID)
    }

    /// The archive's position counter for `recording_id`: how far this
    /// member's log recording has been written, which is the position the
    /// member has applied the log up to.
    #[must_use]
    pub fn recording_position(&self, recording_id: i64) -> Option<i64> {
        use deepmsg_archive::server::counters::ARCHIVE_RECORDING_POSITION_TYPE_ID;
        use deepmsg_archive::server::recording_pos::parse_key;

        let counters = self.cnc.counters()?;
        let mut position = None;
        counters.for_each(|descriptor| {
            if position.is_some() || descriptor.type_id != ARCHIVE_RECORDING_POSITION_TYPE_ID {
                return;
            }
            let Some(key) = counters.key(descriptor.counter_id) else {
                return;
            };
            if parse_key(&key).is_some_and(|identity| identity.recording_id == recording_id) {
                position = counters.value(descriptor.counter_id);
            }
        });
        position
    }

    pub fn election_state(&self) -> Option<i64> {
        self.value(CLUSTER_ELECTION_STATE_TYPE_ID)
    }

    pub fn snapshot_count(&self) -> Option<i64> {
        self.value(CLUSTER_SNAPSHOT_COUNTER_TYPE_ID)
    }

    pub fn leadership_term_id(&self) -> Option<i64> {
        self.value(CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID)
    }

    /// The log recording's position — the follower's append position, which is
    /// what the leader's quorum arithmetic reads. Counter type 100 is the
    /// archive's `RecordingPos`; it carries the session and stream in its label
    /// rather than a cluster key, so the largest of them is taken.
    pub fn append_position(&self) -> Option<i64> {
        let counters = self.cnc.counters()?;
        let mut position = None;
        counters.for_each(|descriptor| {
            if descriptor.type_id == 100 {
                position = Some(
                    position.map_or(descriptor.value, |value: i64| value.max(descriptor.value)),
                );
            }
        });
        position
    }
}

/// What a cluster member's authenticator does, for the authentication cases.
///
/// The reference supplies these as Java classes (`SampleAuthenticator` and its
/// two test suppliers); this is the same decision as data. The shape is the
/// sample's: a per-session **state** decided from the connect request, moved by
/// a challenge response, and read by the two callbacks the session manager runs
/// every pass while a session is pending (`SampleAuthenticator.java`). The
/// callbacks are idempotent — `on_challenged_session` runs again and again
/// while the session waits for its response, and must do nothing until one has
/// arrived.
#[derive(Clone)]
pub struct TestAuthenticator {
    kind: TestAuthenticatorKind,
    sessions: std::collections::HashMap<i64, TestAuthenticationState>,
}

#[derive(Clone)]
enum TestAuthenticatorKind {
    /// The reference's `SimpleCredentialsSupplier`: the credentials are the
    /// principal, and the cluster accepts exactly `expected_credentials`.
    Simple {
        expected_credentials: Vec<u8>,
        principal: Vec<u8>,
    },
    /// The reference's `ChallengeResponseCredentialsSupplier`: a challenge is
    /// sent at the connect request, and the answer must be `expected_response`.
    ChallengeResponse {
        challenge: Vec<u8>,
        expected_response: Vec<u8>,
        principal: Vec<u8>,
    },
}

/// Where one session stands, as `SampleAuthenticator.SessionState` has it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TestAuthenticationState {
    /// A challenge has been sent and its response is awaited.
    AwaitingResponse,
    /// The credentials were accepted.
    Authenticated,
    /// The credentials were refused.
    Rejected,
}

impl TestAuthenticator {
    /// A cluster that accepts exactly `expected_credentials`.
    #[must_use]
    pub fn simple(expected_credentials: &[u8], principal: &[u8]) -> Self {
        Self {
            kind: TestAuthenticatorKind::Simple {
                expected_credentials: expected_credentials.to_vec(),
                principal: principal.to_vec(),
            },
            sessions: std::collections::HashMap::new(),
        }
    }

    /// A cluster that challenges, and accepts exactly `expected_response`.
    #[must_use]
    pub fn challenge_response(
        challenge: &[u8],
        expected_response: &[u8],
        principal: &[u8],
    ) -> Self {
        Self {
            kind: TestAuthenticatorKind::ChallengeResponse {
                challenge: challenge.to_vec(),
                expected_response: expected_response.to_vec(),
                principal: principal.to_vec(),
            },
            sessions: std::collections::HashMap::new(),
        }
    }

    fn state(&self, session_id: i64) -> Option<TestAuthenticationState> {
        self.sessions.get(&session_id).copied()
    }
}

impl Authenticator for TestAuthenticator {
    fn on_connect_request(&mut self, session_id: i64, encoded_credentials: &[u8], _now_ms: i64) {
        let state = match &self.kind {
            TestAuthenticatorKind::Simple {
                expected_credentials,
                ..
            } => {
                if encoded_credentials == expected_credentials.as_slice() {
                    TestAuthenticationState::Authenticated
                } else {
                    TestAuthenticationState::Rejected
                }
            }
            // The connect request names no acceptable principal in this shape:
            // it asks for the challenge, and the answer is judged below.
            TestAuthenticatorKind::ChallengeResponse { .. } => {
                TestAuthenticationState::AwaitingResponse
            }
        };
        self.sessions.insert(session_id, state);
    }

    fn on_challenge_response(&mut self, session_id: i64, encoded_credentials: &[u8], _now_ms: i64) {
        // `SampleAuthenticator.onChallengeResponse`: a response moves
        // `CHALLENGE` to `AUTHENTICATED` only for the expected answer, and
        // anything else is a refusal — including a second answer.
        let state = match &self.kind {
            TestAuthenticatorKind::ChallengeResponse {
                expected_response, ..
            } => {
                if self.state(session_id) == Some(TestAuthenticationState::AwaitingResponse)
                    && encoded_credentials == expected_response.as_slice()
                {
                    TestAuthenticationState::Authenticated
                } else {
                    TestAuthenticationState::Rejected
                }
            }
            TestAuthenticatorKind::Simple { .. } => TestAuthenticationState::Rejected,
        };
        self.sessions.insert(session_id, state);
    }

    fn on_connected_session(
        &mut self,
        session: &mut dyn SessionProxy,
        _now_ms: i64,
    ) -> Result<(), deepmsg_cluster::ClusterError> {
        let session_id = session.session_id();
        match (self.state(session_id), &self.kind) {
            (Some(TestAuthenticationState::Authenticated), kind) => {
                session.authenticate(principal(kind))?;
                self.sessions.remove(&session_id);
            }
            (
                Some(TestAuthenticationState::AwaitingResponse),
                TestAuthenticatorKind::ChallengeResponse { challenge, .. },
            ) => {
                session.challenge(challenge)?;
            }
            _ => {
                session.reject();
                self.sessions.remove(&session_id);
            }
        }
        Ok(())
    }

    fn on_challenged_session(
        &mut self,
        session: &mut dyn SessionProxy,
        _now_ms: i64,
    ) -> Result<(), deepmsg_cluster::ClusterError> {
        let session_id = session.session_id();
        match self.state(session_id) {
            Some(TestAuthenticationState::AwaitingResponse) => {}
            Some(TestAuthenticationState::Authenticated) => {
                let principal = principal(&self.kind).to_vec();
                session.authenticate(&principal)?;
                self.sessions.remove(&session_id);
            }
            _ => {
                session.reject();
                self.sessions.remove(&session_id);
            }
        }
        Ok(())
    }
}

fn principal(kind: &TestAuthenticatorKind) -> &[u8] {
    match kind {
        TestAuthenticatorKind::Simple { principal, .. }
        | TestAuthenticatorKind::ChallengeResponse { principal, .. } => principal,
    }
}

/// A backup node: its own driver **and archive**, a `ClusterBackup` agent on a
/// thread of this process, and the five probes the reference's `TestBackupNode`
/// reads (`TestBackupNode.java:91-142`).
///
/// The backup is a fourth `OwnArchivingMediaDriver` rather than a member: it
/// never joins the cluster, and the log it records is the local archive's copy
/// of the cluster's replay (`ClusterBackup.java:887-893`).
pub struct BackupNode {
    aeron_dir: std::path::PathBuf,
    /// `None` only after [`Self::into_directories`] has handed them to the
    /// member started from this backup.
    archive_dir: Option<TempDir>,
    cluster_dir: Option<TempDir>,
    driver: Option<OwnArchivingMediaDriver>,
    counters: NodeCounters,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    faults: Faults,
    role_name: String,
}

/// What a test varies about a backup node. The defaults are the reference
/// fixture's (`TestCluster.java:594-616`): `FOLLOWER`, a kernel-chosen catchup
/// port, replication from the beginning, and no credentials.
pub struct BackupOptions {
    /// `aeron.cluster.backup.source.type`.
    pub source_type: SourceType,
    /// `aeron.cluster.backup.catchup.endpoint`'s port; `0` asks the kernel.
    pub catchup_port: u16,
    /// `cluster.backup.initial.replay.start`.
    pub initial_replay_start: ReplayStart,
    /// What the backup offers the cluster and its archive. `None` is
    /// [`deepmsg_archive::client::NoCredentials`].
    pub credentials: Option<TestCredentials>,
}

/// What a case's backup offers the cluster and its archive.
///
/// The reference's three suppliers (`Null`, `Simple`, `ChallengeResponse`)
/// differ in two answers — the encoded credentials, and the answer to a
/// challenge — so this is those two byte strings. A case that must be refused
/// sends the wrong ones; nothing here looks at their content.
#[derive(Clone, Debug, Default)]
pub struct TestCredentials {
    /// What `encoded()` answers.
    pub encoded: Vec<u8>,
    /// What `on_challenge()` answers.
    pub challenge_answer: Vec<u8>,
}

impl Credentials for TestCredentials {
    fn encoded(&mut self) -> Vec<u8> {
        self.encoded.clone()
    }

    fn on_challenge(&mut self, _challenge: &[u8]) -> Result<Vec<u8>, String> {
        Ok(self.challenge_answer.clone())
    }
}

impl Default for BackupOptions {
    fn default() -> Self {
        Self {
            source_type: SourceType::Follower,
            catchup_port: 0,
            initial_replay_start: ReplayStart::Beginning,
            credentials: None,
        }
    }
}

impl BackupNode {
    /// Start a backup for `member_string` with the fixture's defaults.
    pub fn start(ports: &Ports, member_string: &str, faults: &Faults) -> Option<Self> {
        Self::start_with(ports, member_string, faults, BackupOptions::default())
    }

    /// The same, with what a case varies.
    pub fn start_with(
        ports: &Ports,
        member_string: &str,
        faults: &Faults,
        options: BackupOptions,
    ) -> Option<Self> {
        let archive_dir = TempDir::new("cluster-backup-archive");
        let cluster_dir = TempDir::new("cluster-backup-cluster");
        Self::start_over_directories(
            ports,
            member_string,
            faults,
            options,
            archive_dir,
            cluster_dir,
        )
    }

    /// Start over directories that already exist — a backup resumed after a
    /// stop (`TestCluster.java:652-665`: `cleanStart=false` keeps the
    /// recording and the log).
    pub fn start_over_directories(
        ports: &Ports,
        member_string: &str,
        faults: &Faults,
        options: BackupOptions,
        archive_dir: TempDir,
        cluster_dir: TempDir,
    ) -> Option<Self> {
        let properties: Vec<String> = archive::PROPERTIES
            .iter()
            .map(|property| (*property).to_owned())
            .collect();
        let property_refs: Vec<&str> = properties.iter().map(String::as_str).collect();

        let mut driver =
            OwnArchivingMediaDriver::start(
                &format!("cluster-backup-{}", ports.base()),
                archive_dir.path(),
                &property_refs,
            )?;
        driver
            .await_ready(DEADLINE)
            .expect("the backup's archive comes up");

        let consensus_endpoints = (0..MEMBER_COUNT)
            .map(|id| format!("localhost:{}", ports.consensus(id)))
            .collect::<Vec<_>>()
            .join(",");

        let mut properties = Properties::new();
        properties
            .apply_args(vec![
                format!("-Daeron.dir={}", driver.aeron_dir().display()),
                format!("-Daeron.cluster.dir={}", cluster_dir.path().display()),
                format!("-Daeron.cluster.id={CLUSTER_ID}"),
                format!("-Daeron.cluster.members={member_string}"),
                format!(
                    "-Daeron.cluster.consensus.channel=aeron:udp?endpoint=localhost:{}",
                    ports.backup_consensus()
                ),
                format!("-Daeron.cluster.consensus.endpoints={consensus_endpoints}"),
                format!(
                    "-Daeron.cluster.backup.catchup.endpoint=localhost:{}",
                    options.catchup_port
                ),
                format!(
                    "-Daeron.cluster.backup.source.type={}",
                    options.source_type.name()
                ),
                format!(
                    "-Dcluster.backup.initial.replay.start={}",
                    options.initial_replay_start.name()
                ),
                // The reference's own fixture shortens this to a second so a
                // failed attempt retries while the case is still watching
                // (`TestCluster.java:598`).
                "-Daeron.cluster.backup.cool.down.interval=1s".to_owned(),
            ])
            .expect("the backup's properties parse");
        let config = ClusterBackupConfig::from_properties(&properties)
            .expect("the backup's configuration settles");

        let counters = NodeCounters::open(driver.aeron_dir());
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let recorded_faults = faults.clone();
        let faults = faults.clone();
        let (started_tx, started_rx) = mpsc::channel::<Result<String, String>>();

        let credentials = options.credentials;
        let thread = std::thread::spawn(move || {
            // Built here rather than handed in: the components hold trait
            // objects with no `Send` bound (the reference's context holds the
            // same live objects), and the agent is what lives on this thread.
            let components = ClusterBackupComponents {
                credentials: match credentials {
                    None => Box::new(deepmsg_archive::client::NoCredentials),
                    Some(credentials) => Box::new(credentials),
                },
                ..ClusterBackupComponents::default()
            };
            let context = ClusterBackupContext::new(config);
            let mut agent = match ClusterBackup::launch_with_components(context, components) {
                Ok(agent) => agent,
                Err(error) => {
                    let _ = started_tx.send(Err(error.to_string()));
                    return;
                }
            };
            let _ = started_tx.send(Ok(agent.role_name().to_owned()));

            let mut idle = Strategy::by_agrona_name("backoff").expect("backoff");
            while !flag.load(Ordering::Relaxed) {
                match agent.do_work() {
                    Ok(work) => idle.idle(work),
                    Err(error) if error.is_termination() => break,
                    Err(error) => {
                        // `AgentRunner` counts the error and keeps running: the
                        // agent has put itself in `RESET_BACKUP` and the next
                        // turn cools down and re-queries. A fixture that broke
                        // here would never observe a reset — and a case whose
                        // point is the reset would wait for a state the thread
                        // has already left.
                        faults.record(format!("the backup: {error}"));
                        idle.idle(0);
                    }
                }
            }

            if let Err(error) = agent.close() {
                faults.record(format!("the backup's close: {error}"));
            }
        });

        let role_name = match started_rx.recv_timeout(DEADLINE) {
            Ok(Ok(role_name)) => role_name,
            Ok(Err(error)) => panic!("the backup did not launch: {error}"),
            Err(error) => panic!("the backup never reported its launch: {error}"),
        };

        Some(Self {
            aeron_dir: driver.aeron_dir().to_path_buf(),
            archive_dir: Some(archive_dir),
            cluster_dir: Some(cluster_dir),
            driver: Some(driver),
            counters,
            stop,
            thread: Some(thread),
            faults: recorded_faults,
            role_name,
        })
    }

    /// `ClusterBackup.State` (counter 208) as its code.
    pub fn state(&self) -> Option<i64> {
        self.counters.value(CLUSTER_BACKUP_STATE_TYPE_ID)
    }

    /// Counter 209, or `None` when the counter is gone.
    pub fn live_log_position(&self) -> Option<i64> {
        self.counters
            .value(CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID)
    }

    /// Counter 210 — the epoch-millisecond deadline of the next query.
    pub fn next_query_deadline_ms(&self) -> Option<i64> {
        self.counters.value(CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID)
    }

    /// Counter 240.
    pub fn snapshot_retrieve_count(&self) -> Option<i64> {
        self.counters
            .value(CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID)
    }

    /// Counter 211: how many errors have been counted.
    pub fn error_count(&self) -> Option<i64> {
        self.counters.value(CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID)
    }

    /// The backup's mark file error log, as text
    /// (`TestBackupNode.clusterBackupErrorLog`).
    pub fn error_log(&self) -> Vec<String> {
        use deepmsg_cnc::error_log::ErrorLogReader;

        let path = self
            .cluster_dir()
            .join(deepmsg_cluster_common::mark::FILENAME);
        let Ok(mark) = deepmsg_cluster_common::mark::ClusterMarkFile::open(&path) else {
            return Vec::new();
        };
        let Some(buffer) = mark.error_buffer_read_only() else {
            return Vec::new();
        };
        let mut entries = Vec::new();
        let _ = ErrorLogReader::new(buffer).read(i64::MIN, &mut entries);
        entries.into_iter().map(|entry| entry.text).collect()
    }

    /// The thread name the agent runs under (`ClusterBackupAgent.roleName`),
    /// read from the agent itself rather than from the configuration.
    #[must_use]
    pub fn role_name(&self) -> &str {
        &self.role_name
    }

    /// Where the backup's own recording of the cluster's log starts — the
    /// reference's `TestBackupNode.recordingLogStartPosition`
    /// (`TestBackupNode.java:149-166`): the last term's recording, then the
    /// backup archive's start position for it.
    pub fn recording_log_start_position(&self) -> Option<i64> {
        use deepmsg_archive::client::{Archive, ArchiveContext, Handlers, NoCredentials};
        use deepmsg_cluster::persist::recording_log::{self, RecordingLog};

        let log_path = self.cluster_dir().join(recording_log::FILENAME);
        let log = RecordingLog::open(&log_path).ok()?;
        let recording_id = log.find_last_term()?.recording_id;
        drop(log);

        let mut client = Client::connect(&self.aeron_dir).ok()?;
        let mut context = ArchiveContext::new();
        context.aeron_dir = self.aeron_dir.to_string_lossy().into_owned();
        context.control_request_channel = Some("aeron:ipc".to_owned());
        context.control_response_channel = Some("aeron:ipc".to_owned());
        let mut archive = Archive::connect(
            &context,
            &mut client,
            &mut NoCredentials,
            Handlers::default(),
        )
        .ok()?;

        archive.get_start_position(&mut client, recording_id).ok()
    }

    /// The backup's own aeron directory.
    pub fn aeron_dir(&self) -> &std::path::Path {
        &self.aeron_dir
    }

    pub fn cluster_dir(&self) -> &std::path::Path {
        self.cluster_dir
            .as_ref()
            .expect("the directories are still this node's")
            .path()
    }

    /// Stop the agent and the driver, and hand the directories over — the
    /// reference's `startStaticNodeFromBackup` needs both (`TestCluster
    /// .java:623-687`).
    pub fn into_directories(mut self) -> (TempDir, TempDir) {
        self.stop_agent();
        if let Some(mut driver) = self.driver.take() {
            driver.stop().expect("the backup's driver stops");
        }
        (
            self.archive_dir
                .take()
                .expect("the directories are still here"),
            self.cluster_dir
                .take()
                .expect("the directories are still here"),
        )
    }

    fn stop_agent(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for BackupNode {
    fn drop(&mut self) {
        self.stop_agent();
        if let Some(mut driver) = self.driver.take() {
            let _ = driver.stop();
        }
        // A recorded fault that the case never looked at is a case that ended
        // without knowing: the harness's own rule for every node.
        let _ = &self.faults;
    }
}

/// The client side: one ingress publication and one egress subscription on a
/// driver that is not any member's.
pub struct ClientSide {
    _driver: OwnDriver,
    client: Client,
    ingress_publication_id: i64,
    egress_subscription_id: i64,
    pub egress_channel: String,
}

impl ClientSide {
    pub fn start(ports: &Ports, leader_id: i32) -> Option<Self> {
        Self::start_at(ports, leader_id, 0)
    }

    /// [`Self::start`], with the egress port moved by `egress_offset` — a
    /// second client whose response channel does not collide with a previous
    /// one the cluster may still hold open.
    pub fn start_at(ports: &Ports, leader_id: i32, egress_offset: u16) -> Option<Self> {
        let ports = &ports.with_egress_offset(egress_offset);
        let driver = OwnDriver::start(&format!("cluster-harness-client-{}", ports.base()))?;
        let mut client = Client::connect(driver.aeron_dir()).expect("the test client connects");

        let egress_channel = ports.client_egress_channel();
        let ingress_publication_id = client
            .add_publication(
                &ports.ingress_publication_channel(leader_id),
                INGRESS_STREAM_ID,
                DEADLINE,
            )
            .expect("the ingress publication");
        let egress_subscription_id = client
            .add_subscription(&egress_channel, EGRESS_STREAM_ID, DEADLINE)
            .expect("the egress subscription");

        Some(Self {
            _driver: driver,
            client,
            ingress_publication_id,
            egress_subscription_id,
            egress_channel,
        })
    }

    /// The ingress publication's shape and window, for a failure's message.
    #[must_use]
    pub fn ingress_publication_state(&self) -> String {
        let Some(publication) = self.client.publication(self.ingress_publication_id) else {
            return "<no publication>".to_owned();
        };
        let limit = self
            .client
            .counters_reader()
            .and_then(|counters| counters.value(publication.position_limit_counter_id()));
        format!(
            "term={} position={:?} limit={limit:?} connected={:?}",
            publication.term_buffer_length(),
            publication.position(),
            publication.is_connected(),
        )
    }

    pub fn offer(&self, bytes: &[u8]) -> bool {
        matches!(
            self.client.offer(self.ingress_publication_id, bytes),
            Some(deepmsg_core::logbuffer::append::Appended::Ok { .. })
        )
    }

    /// One client poll, and every `SessionEvent` it delivered.
    pub fn poll_events(&mut self) -> Vec<SessionEvent> {
        self.client.poll();

        let mut events = Vec::new();
        self.client
            .poll_subscription(self.egress_subscription_id, 10, |message| {
                if let Some(event) = decode_session_event(message.payload) {
                    events.push(event);
                }
            });
        events
    }

    /// One client poll and no event collection.
    pub fn poll(&mut self) {
        self.client.poll();
    }

    /// One poll, and everything the egress subscription delivered: the
    /// `SessionEvent`s, the `Challenge`s, and how many session messages (the
    /// service's replies) arrived.
    ///
    /// They share the stream, so a case that wants the replies must consume
    /// the events in the same pass — polling for one and then the other would
    /// read the same fragment twice or miss it.
    pub fn poll_egress(&mut self) -> (Vec<SessionEvent>, usize) {
        let (events, _, messages) = self.poll_egress_all();
        (events, messages)
    }

    /// [`Self::poll_egress`], keeping the challenges a cluster sends a client
    /// that must answer one.
    pub fn poll_egress_all(&mut self) -> (Vec<SessionEvent>, Vec<Challenge>, usize) {
        self.client.poll();

        let mut events = Vec::new();
        let mut challenges = Vec::new();
        let mut messages = 0;
        self.client
            .poll_subscription(self.egress_subscription_id, 10, |message| {
                if let Some(event) = decode_session_event(message.payload) {
                    events.push(event);
                } else if let Some(challenge) = decode_challenge(message.payload) {
                    challenges.push(challenge);
                } else {
                    messages += 1;
                }
            });

        (events, challenges, messages)
    }

    /// Open a session and wait for the `OK` event — the reference's
    /// `connectClient` followed by its `awaitReady` (`AeronCluster`).
    pub fn connect_session(&mut self, faults: &Faults) -> SessionEvent {
        self.connect_session_with(&[], &[], faults)
    }

    /// The same, with the credentials a cluster's authenticator will judge
    /// (`SessionConnectRequest.encodedCredentials`) and, for a cluster that
    /// challenges, the answer to send back.
    pub fn connect_session_with(
        &mut self,
        credentials: &[u8],
        challenge_answer: &[u8],
        faults: &Faults,
    ) -> SessionEvent {
        const CORRELATION_ID: i64 = 1;
        let request =
            connect_request_with_credentials(CORRELATION_ID, &self.egress_channel, credentials);
        offer_message(self, faults, &request);

        let deadline = Instant::now() + DEADLINE;
        loop {
            faults.check();
            let (events, challenges, _) = self.poll_egress_all();

            for challenge in challenges {
                self.answer_challenge(&challenge, challenge_answer, faults);
            }

            if let Some(event) = events
                .into_iter()
                .find(|event| event.correlation_id == CORRELATION_ID)
            {
                assert_eq!(
                    EventCode::OK,
                    event.code,
                    "the session was refused: {:?} {}",
                    event.code,
                    event.detail
                );
                return event;
            }
            assert!(
                Instant::now() < deadline,
                "the session open event never arrived"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Answer a `Challenge` on the ingress publication — what the reference's
    /// `AeronCluster` does with its credentials supplier
    /// (`AeronCluster.java:1235-1290`).
    pub fn answer_challenge(&self, challenge: &Challenge, answer: &[u8], faults: &Faults) {
        let message = challenge_response_message(
            challenge.correlation_id,
            challenge.cluster_session_id,
            answer,
        );
        offer_message(self, faults, &message);
    }

    /// Offer one session message on the open session.
    pub fn send(&self, session: &SessionEvent, payload: &[u8], faults: &Faults) {
        let message = ingress_message(
            session.leadership_term_id,
            session.cluster_session_id,
            payload,
        );
        offer_message(self, faults, &message);
    }

    /// Send `count` messages and wait until as many replies have arrived — the
    /// reference's `sendAndAwaitMessages`.
    pub fn send_and_await_messages(
        &mut self,
        session: &SessionEvent,
        count: usize,
        faults: &Faults,
    ) {
        for index in 0..count {
            self.send(session, &[b'0' + u8::try_from(index % 10).unwrap()], faults);
        }
        self.await_responses(count, faults);
    }

    /// Poll until `count` replies have arrived on the current session.
    pub fn await_responses(&mut self, count: usize, faults: &Faults) {
        let deadline = Instant::now() + DEADLINE;
        let mut seen = 0;
        while seen < count {
            faults.check();
            let (_, messages) = self.poll_egress();
            seen += messages;
            assert!(
                Instant::now() < deadline,
                "only {seen} of {count} replies arrived"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// The last `lines` lines the client's own driver wrote.
    #[must_use]
    pub fn driver_log_tail(&self, lines: usize) -> String {
        self._driver.log_tail(lines)
    }

    /// How many images the egress subscription has — a test's way to tell "the
    /// cluster never published" from "the event was not read".
    #[must_use]
    pub fn egress_image_count(&self) -> usize {
        self.client
            .subscription(self.egress_subscription_id)
            .map_or(0, |subscription| subscription.images().len())
    }
}

/// A `Challenge` (`Challenge.java`): what a cluster sends a session it wants
/// credentials from before it may open.
#[derive(Clone, Debug)]
pub struct Challenge {
    /// The correlation id of the connect request that is being challenged.
    pub correlation_id: i64,
    /// The session the cluster assigned.
    pub cluster_session_id: i64,
    /// The challenge to answer.
    pub encoded_challenge: Vec<u8>,
}

/// Decode a `Challenge`, or `None` for a payload that is something else.
pub fn decode_challenge(payload: &[u8]) -> Option<Challenge> {
    if payload.len() < MESSAGE_HEADER_LENGTH {
        return None;
    }
    let header = deepmsg_codec_cluster::message_header_codec::MessageHeaderDecoder::default()
        .wrap(ReadBuf::new(payload), 0);
    if header.schema_id() != deepmsg_codec_cluster::SBE_SCHEMA_ID
        || header.template_id() != deepmsg_codec_cluster::challenge_codec::SBE_TEMPLATE_ID
    {
        return None;
    }

    let mut decoder = ChallengeDecoder::default().header(header, 0);
    let (offset, length) = decoder.encoded_challenge_decoder();
    Some(Challenge {
        correlation_id: decoder.correlation_id(),
        cluster_session_id: decoder.cluster_session_id(),
        encoded_challenge: decoder.encoded_challenge_slice((offset, length)).to_vec(),
    })
}

/// A `ChallengeResponse` on the ingress publication (`ChallengeResponse.java`).
pub fn challenge_response_message(
    correlation_id: i64,
    cluster_session_id: i64,
    encoded_credentials: &[u8],
) -> Vec<u8> {
    let mut buffer = vec![0_u8; MESSAGE_HEADER_LENGTH + 16 + 4 + encoded_credentials.len()];
    let encoder =
        ChallengeResponseEncoder::default().wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .correlation_id(correlation_id)
        .cluster_session_id(cluster_session_id)
        .encoded_credentials(encoded_credentials);
    let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
    buffer.truncate(length);
    buffer
}

#[derive(Clone, Debug)]
pub struct SessionEvent {
    pub cluster_session_id: i64,
    pub correlation_id: i64,
    pub leadership_term_id: i64,
    pub code: EventCode,
    /// The event's `detail` — a close reason, or an error's message.
    pub detail: String,
}

pub fn decode_session_event(payload: &[u8]) -> Option<SessionEvent> {
    if payload.len() < MESSAGE_HEADER_LENGTH {
        return None;
    }

    let header = deepmsg_codec_cluster::message_header_codec::MessageHeaderDecoder::default()
        .wrap(ReadBuf::new(payload), 0);
    if header.schema_id() != deepmsg_codec_cluster::SBE_SCHEMA_ID
        || header.template_id() != deepmsg_codec_cluster::session_event_codec::SBE_TEMPLATE_ID
    {
        return None;
    }

    let mut decoder = SessionEventDecoder::default().header(header, 0);
    let (detail_offset, detail_length) = decoder.detail_decoder();
    Some(SessionEvent {
        cluster_session_id: decoder.cluster_session_id(),
        correlation_id: decoder.correlation_id(),
        leadership_term_id: decoder.leadership_term_id(),
        code: decoder.code(),
        detail: String::from_utf8_lossy(decoder.detail_slice((detail_offset, detail_length)))
            .into_owned(),
    })
}

/// `SessionConnectRequestEncoder`'s encoding, written by hand as the reference
/// client writes it (`AeronCluster`): the response channel is the egress
/// subscription's own endpoint.
pub fn connect_request(correlation_id: i64, response_channel: &str) -> Vec<u8> {
    connect_request_with_credentials(correlation_id, response_channel, &[])
}

/// [`connect_request`] with credentials a cluster's authenticator will see.
pub fn connect_request_with_credentials(
    correlation_id: i64,
    response_channel: &str,
    credentials: &[u8],
) -> Vec<u8> {
    let mut buffer = vec![0_u8; 256 + response_channel.len() + credentials.len()];
    let encoder = SessionConnectRequestEncoder::default()
        .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .correlation_id(correlation_id)
        .response_stream_id(EGRESS_STREAM_ID)
        .version(PROTOCOL_VERSION)
        .response_channel(response_channel.as_bytes())
        .encoded_credentials(credentials)
        .client_info(b"cluster-harness");
    let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
    buffer.truncate(length);
    buffer
}

/// The longest payload an ingress message may carry on a channel of this term
/// length: `computeMaxMessageLength(term) - SESSION_HEADER_LENGTH`
/// (`ClusterTest.java:1850`).
///
/// The message *is* the session header plus the payload —
/// `AeronCluster.SESSION_HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH +
/// SessionMessageHeaderEncoder.BLOCK_LENGTH` (`AeronCluster.java:99-100`) — and
/// [`ingress_message`] writes exactly that. One such message fills one eighth
/// of a term, which is what makes the log position advance per message equal to
/// `term-length / 8`.
///
/// # Panics
///
/// A term length too small for a session header.
#[must_use]
pub fn max_ingress_payload(term_length: i32) -> usize {
    let message_length = usize::try_from(deepmsg_core::logbuffer::position::max_message_length(
        term_length,
    ))
    .expect("a positive term length");
    message_length
        .checked_sub(MESSAGE_HEADER_LENGTH + usize::from(SBE_SESSION_HEADER_BLOCK_LENGTH))
        .expect("the term length is too small for a session header")
}

/// `SessionMessageHeaderEncoder.BLOCK_LENGTH` (`AeronCluster.java:99-100`).
const SBE_SESSION_HEADER_BLOCK_LENGTH: u16 = 24;

/// A `SessionMessageHeader` and its payload (`SessionMessageHeaderEncoder`).
pub fn ingress_message(
    leadership_term_id: i64,
    cluster_session_id: i64,
    payload: &[u8],
) -> Vec<u8> {
    let mut buffer = vec![0_u8; 64 + payload.len()];
    let encoder = SessionMessageHeaderEncoder::default()
        .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .leadership_term_id(leadership_term_id)
        .cluster_session_id(cluster_session_id)
        .timestamp(1);
    let header_length = MESSAGE_HEADER_LENGTH + message.encoded_length();
    buffer[header_length..header_length + payload.len()].copy_from_slice(payload);
    buffer.truncate(header_length + payload.len());
    buffer
}

/// Wait until `condition` holds, failing on a recorded fault or the deadline.
pub fn wait_until<F>(what: &str, faults: &Faults, mut condition: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        faults.check();
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("timed out waiting for {what}");
}

/// Wait for every member's `launch` to come back.
pub fn await_launched(
    faults: &Faults,
    launched: &mpsc::Receiver<(i32, Result<(), String>)>,
    count: i32,
) {
    let deadline = Instant::now() + DEADLINE;
    let mut done = 0;
    while done < count {
        assert!(
            Instant::now() < deadline,
            "only {done} of {count} modules launched; faults: {:?}",
            faults.snapshot()
        );
        match launched.recv_timeout(Duration::from_millis(100)) {
            Ok((_, Ok(()))) => done += 1,
            Ok((id, Err(error))) => panic!(
                "node {id} did not launch: {error}; faults: {:?}",
                faults.snapshot()
            ),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("every node died launching"),
        }
    }
}

/// Whether every node's election has closed.
pub fn elections_closed(counters: &[NodeCounters]) -> bool {
    counters
        .iter()
        .all(|node| node.election_state() == Some(ELECTION_CLOSED))
}

/// The single leader's id among `counters`, or `None` while the roles do not
/// say.
pub fn leader_of(counters: &[NodeCounters]) -> Option<i32> {
    let roles: Vec<Option<i64>> = counters.iter().map(NodeCounters::role).collect();
    let leaders: Vec<usize> = roles
        .iter()
        .enumerate()
        .filter(|(_, role)| **role == Some(ROLE_LEADER))
        .map(|(index, _)| index)
        .collect();

    (leaders.len() == 1).then(|| leaders[0] as i32)
}

/// Offer a message, waiting out back pressure, checking faults.
pub fn offer_message(client: &ClientSide, faults: &Faults, message: &[u8]) {
    let deadline = Instant::now() + DEADLINE;
    while !client.offer(message) {
        assert!(
            Instant::now() < deadline,
            "the ingress publication is stuck: {} len={}",
            client.ingress_publication_state(),
            message.len()
        );
        faults.check();
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Announce a skip when the archiving driver is not available.
pub fn announce_skip() {
    archiving_driver::announce_skip();
}
