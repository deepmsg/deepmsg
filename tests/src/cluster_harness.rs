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

use deepmsg_client::client::Client;
use deepmsg_cluster::{ConsensusModuleAgent, ConsensusModuleConfig};
use deepmsg_cluster_common::config::Properties;
use deepmsg_cluster_common::counters::{
    CLUSTER_COMMIT_POSITION_TYPE_ID, CLUSTER_ELECTION_STATE_TYPE_ID,
    CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID, CLUSTER_NODE_ROLE_TYPE_ID, find_cluster_counter,
};
use deepmsg_cnc::file::CncFile;
use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::session_connect_request_codec::SessionConnectRequestEncoder;
use deepmsg_codec_cluster::session_event_codec::SessionEventDecoder;
use deepmsg_codec_cluster::session_message_header_codec::SessionMessageHeaderEncoder;
use deepmsg_codec_cluster::{ReadBuf, WriteBuf};
use deepmsg_driver::idle::Strategy;

use crate::archive;
use crate::archiving_driver::{self, OwnArchivingMediaDriver};
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

/// Member roles, as counter 201 carries them (`Cluster.Role`).
pub const ROLE_FOLLOWER: i64 = 0;
/// The leader's role code.
pub const ROLE_LEADER: i64 = 2;
/// `ElectionState.CLOSED` (`crates/cluster/consensus/src/election/state.rs`).
pub const ELECTION_CLOSED: i64 = 17;

/// The reference's port scheme (`TestCluster.java:1877-1905`) with a base that
/// moves with the process id, so two concurrent `cargo test` runs do not meet
/// on a fixed port.
pub struct Ports {
    base: u16,
}

impl Ports {
    pub fn new() -> Self {
        // 21000..24990, in tens; the highest offset used is +30.
        Self {
            base: 21_000 + (u16::try_from(std::process::id() % 400).unwrap()) * 10,
        }
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
        self.base + 30
    }

    pub fn ingress_channel(&self, member_id: i32) -> String {
        format!(
            "aeron:udp?term-length=128k|alias=ingress|endpoint=localhost:{}",
            self.ingress(member_id)
        )
    }

    pub fn consensus_channel(&self, member_id: i32) -> String {
        format!(
            "aeron:udp?alias=consensus|endpoint=localhost:{}",
            self.consensus(member_id)
        )
    }

    pub fn client_egress_channel(&self) -> String {
        format!("aeron:udp?endpoint=localhost:{}", self.client_egress())
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
/// The archive endpoint is never dialled: the module's archive connection is
/// IPC to the archive beside it, and a follower's catchup replay reaches it
/// through a dynamically resolved catchup endpoint (`:0`).
pub fn members(ports: &Ports) -> String {
    (0..MEMBER_COUNT)
        .map(|id| {
            format!(
                "{id},localhost:{},localhost:{},localhost:{},localhost:0,localhost:0",
                ports.ingress(id),
                ports.consensus(id),
                ports.log(id),
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// Failures a node thread reports after it has launched, so the driving test
/// fails with the node's own words rather than a timeout.
#[derive(Clone, Default)]
pub struct Faults(Arc<Mutex<Vec<String>>>);

impl Faults {
    pub fn record(&self, message: String) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(message);
    }

    pub fn check(&self) {
        let first = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .first()
            .cloned();
        if let Some(first) = first {
            panic!("a consensus module failed: {first}");
        }
    }

    /// The recorded failures, for an assertion's message.
    pub fn snapshot(&self) -> Vec<String> {
        self.0
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
    cluster_dir: TempDir,
    _archive_dir: TempDir,
    driver: OwnArchivingMediaDriver,
    /// The properties the module is launched with, kept so a restart settles
    /// the same configuration from the same values.
    args: Vec<String>,
    faults: Faults,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
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
        let name = format!("cluster-node-{id}");
        let archive_dir = TempDir::new(&format!("{name}-archive"));
        let cluster_dir = TempDir::new(&format!("{name}-cluster"));

        let mut driver =
            OwnArchivingMediaDriver::start(&name, archive_dir.path(), &archive::PROPERTIES)?;
        driver.await_ready(DEADLINE).expect("the driver comes up");

        let args = vec![
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
            "-Daeron.cluster.log.channel=aeron:udp?term-length=512k|alias=raft".to_owned(),
            "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k".to_owned(),
            "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0".to_owned(),
            "-Daeron.cluster.service.count=0".to_owned(),
        ];

        let mut node = Self {
            id,
            cluster_dir,
            _archive_dir: archive_dir,
            driver,
            args,
            faults: faults.clone(),
            stop: Arc::new(AtomicBool::new(false)),
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
        let config = ConsensusModuleConfig::from_properties(&properties)
            .expect("the module's configuration settles");
        let idle_strategy = config.idle_strategy.clone();

        let id = self.id;
        let stop = Arc::new(AtomicBool::new(false));
        self.stop = Arc::clone(&stop);
        let faults = self.faults.clone();
        let sender = launched.clone();

        self.thread = Some(std::thread::spawn(move || {
            let Some(mut idle) = Strategy::by_agrona_name(&idle_strategy) else {
                faults.record(format!("node {id}: unknown idle strategy {idle_strategy}"));
                return;
            };

            // Blocks until the election has closed for this member: the
            // leader's log recording, or the follower's join of the log.
            let mut agent = match ConsensusModuleAgent::launch(config) {
                Ok(agent) => agent,
                Err(error) => {
                    let _ = sender.send((id, Err(error.to_string())));
                    return;
                }
            };
            let _ = sender.send((id, Ok(())));

            while !stop.load(Ordering::Relaxed) {
                match agent.do_work() {
                    Ok(work) => idle.idle(work),
                    Err(error) => {
                        faults.record(format!("node {id}: the duty cycle: {error}"));
                        break;
                    }
                }
            }

            if let Err(error) = agent.close() {
                faults.record(format!("node {id}: close: {error}"));
            }
        }));
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
        self.driver.aeron_dir()
    }

    pub fn cluster_dir(&self) -> &std::path::Path {
        self.cluster_dir.path()
    }

    /// Stop the module — "the follower dies" — with the media left running so
    /// the restart path has something to come back to.
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.thread.is_none()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The cluster counters this harness reads from a member's driver.
pub struct NodeCounters {
    cnc: CncFile,
}

impl NodeCounters {
    pub fn open(aeron_dir: &std::path::Path) -> Self {
        Self {
            cnc: CncFile::open(aeron_dir, DEADLINE).expect("the node's CnC file"),
        }
    }

    pub fn value(&self, type_id: i32) -> Option<i64> {
        let counters = self.cnc.counters()?;
        let id = find_cluster_counter(&counters, type_id, CLUSTER_ID)?;
        counters.value(id)
    }

    pub fn role(&self) -> Option<i64> {
        self.value(CLUSTER_NODE_ROLE_TYPE_ID)
    }

    pub fn commit_position(&self) -> Option<i64> {
        self.value(CLUSTER_COMMIT_POSITION_TYPE_ID)
    }

    pub fn election_state(&self) -> Option<i64> {
        self.value(CLUSTER_ELECTION_STATE_TYPE_ID)
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
        let driver = OwnDriver::start("cluster-harness-client")?;
        let mut client = Client::connect(driver.aeron_dir()).expect("the test client connects");

        let egress_channel = ports.client_egress_channel();
        let ingress_publication_id = client
            .add_publication(
                &format!("aeron:udp?endpoint=localhost:{}", ports.ingress(leader_id)),
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
}

#[derive(Clone, Copy, Debug)]
pub struct SessionEvent {
    pub cluster_session_id: i64,
    pub correlation_id: i64,
    pub leadership_term_id: i64,
    pub code: EventCode,
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

    let decoder = SessionEventDecoder::default().header(header, 0);
    Some(SessionEvent {
        cluster_session_id: decoder.cluster_session_id(),
        correlation_id: decoder.correlation_id(),
        leadership_term_id: decoder.leadership_term_id(),
        code: decoder.code(),
    })
}

/// `SessionConnectRequestEncoder`'s encoding, written by hand as the reference
/// client writes it (`AeronCluster`): the response channel is the egress
/// subscription's own endpoint.
pub fn connect_request(correlation_id: i64, response_channel: &str) -> Vec<u8> {
    let mut buffer = vec![0_u8; 256 + response_channel.len()];
    let encoder = SessionConnectRequestEncoder::default()
        .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .correlation_id(correlation_id)
        .response_stream_id(EGRESS_STREAM_ID)
        .version(PROTOCOL_VERSION)
        .response_channel(response_channel.as_bytes())
        .encoded_credentials(b"")
        .client_info(b"cluster-harness");
    let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
    buffer.truncate(length);
    buffer
}

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
            Ok((id, Err(error))) => panic!("node {id} did not launch: {error}"),
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
            "the ingress publication is stuck"
        );
        faults.check();
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Announce a skip when the archiving driver is not available.
pub fn announce_skip() {
    archiving_driver::announce_skip();
}
