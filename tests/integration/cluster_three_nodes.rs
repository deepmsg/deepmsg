//! P3-6a's three-node harness: three consensus modules, each with its own
//! driver and archive, forming one cluster over UDP.
//!
//! This is the instrument the card's DoD names — **three pure-Rust nodes elect
//! a leader, commit, and keep committing when a follower dies**. It is not
//! LC-2 (that runs the reference's own cluster tests against this build); it is
//! the smallest cluster this build can stand up by itself, so that every piece
//! of wiring P3-6a added (`ConsensusPublisher`, the multi-member log
//! destinations, the follower's append-position heartbeat) is exercised by
//! something other than a unit test.
//!
//! # The topology
//!
//! Each member gets its own aeron directory, archive directory and cluster
//! directory, and its own `deepmsg-archiving-media-driver` process — a media
//! driver *and* an archive, because a cluster member records the log through
//! its archive and `Archive::connect` is a local IPC connection
//! (`crates/cluster/consensus/src/config.rs:452-458`). The consensus module
//! itself runs in this process, one thread each: `launch` blocks until the
//! node has joined the log and the election has closed, so a thread is the
//! only place it can live.
//!
//! The client gets a fourth driver of its own, so that stopping a member cannot
//! take the client with it.
//!
//! # Ports
//!
//! The reference's scheme (`TestCluster.java:1877-1905`) with a process-id
//! base, because this test shares a machine with the rest of the workspace's
//! integration suite. See [`Ports`].
//!
//! # What is deliberately absent
//!
//! `aeron.cluster.service.count=0`: the service container is P3-2c's, and the
//! DoD does not need it — a follower's append position, which is what commit is
//! computed from, is the log recording, not the service. No snapshots, no
//! restarts, no catch-up: those are P3-6b.

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
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::driver::OwnDriver;
use deepmsg_tests::temp::TempDir;

const CLUSTER_ID: i32 = 7;
const MEMBER_COUNT: i32 = 3;
const INGRESS_STREAM_ID: i32 = 101;
const EGRESS_STREAM_ID: i32 = 102;
/// `SemanticVersion.compose(0, 3, 0)`.
const PROTOCOL_VERSION: i32 = 3 << 8;
const DEADLINE: Duration = Duration::from_secs(90);

/// Member roles, as counter 201 carries them (`Cluster.Role`).
const ROLE_FOLLOWER: i64 = 0;
const ROLE_LEADER: i64 = 2;
/// `ElectionState.CLOSED` (`crates/cluster/consensus/src/election/state.rs`).
const ELECTION_CLOSED: i64 = 17;

/// The reference's port scheme (`TestCluster.java:1877-1905`) with a base that
/// moves with the process id, so two concurrent `cargo test` runs do not meet
/// on a fixed port.
struct Ports {
    base: u16,
}

impl Ports {
    fn new() -> Self {
        // 21000..24990, in tens; the highest offset used is +30.
        Self {
            base: 21_000 + (u16::try_from(std::process::id() % 400).unwrap()) * 10,
        }
    }

    /// `hostname(memberId):2<clusterId>11<memberId>` (`:1924-1927`).
    fn ingress(&self, member_id: i32) -> u16 {
        self.base + u16::try_from(member_id).unwrap()
    }

    /// `:2<clusterId>22<memberId>` (`:1890`).
    fn consensus(&self, member_id: i32) -> u16 {
        self.base + 10 + u16::try_from(member_id).unwrap()
    }

    /// `:2<clusterId>33<memberId>` (`:1892`) — the log destination.
    fn log(&self, member_id: i32) -> u16 {
        self.base + 20 + u16::try_from(member_id).unwrap()
    }

    /// Where the client's egress subscription listens.
    fn client_egress(&self) -> u16 {
        self.base + 30
    }

    fn ingress_channel(&self, member_id: i32) -> String {
        format!(
            "aeron:udp?term-length=128k|alias=ingress|endpoint=localhost:{}",
            self.ingress(member_id)
        )
    }

    fn consensus_channel(&self, member_id: i32) -> String {
        format!(
            "aeron:udp?alias=consensus|endpoint=localhost:{}",
            self.consensus(member_id)
        )
    }

    fn client_egress_channel(&self) -> String {
        format!("aeron:udp?endpoint=localhost:{}", self.client_egress())
    }
}

/// `id,ingress,consensus,log,catchup,archive` per member
/// (`ClusterMember.java:561-564`; `common/member.rs`).
///
/// The archive endpoint is never dialled in this card: the module's archive
/// connection is IPC to the archive beside it. Catch-up is dynamic (`:0`) until
/// P3-6b gives it a destination.
fn members(ports: &Ports) -> String {
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
struct Faults(Arc<Mutex<Vec<String>>>);

impl Faults {
    fn record(&self, message: String) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(message);
    }

    fn check(&self) {
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
}

/// One member: its driver+archive process, its directories, and its module
/// thread.
struct Node {
    /// Held so the directories outlive the module thread and the driver.
    _cluster_dir: TempDir,
    _archive_dir: TempDir,
    driver: OwnArchivingMediaDriver,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Node {
    fn start(
        id: i32,
        ports: &Ports,
        member_string: &str,
        faults: &Faults,
        launched: &mpsc::Sender<(i32, Result<(), String>)>,
    ) -> Option<Self> {
        let name = format!("cluster-three-nodes-{id}");
        let archive_dir = TempDir::new(&format!("{name}-archive"));
        let cluster_dir = TempDir::new(&format!("{name}-cluster"));

        let mut driver =
            OwnArchivingMediaDriver::start(&name, archive_dir.path(), &archive::PROPERTIES)?;
        driver.await_ready(DEADLINE).expect("the driver comes up");

        let mut properties = Properties::new();
        properties
            .apply_args([
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
            ])
            .expect("the properties parse");

        let config = ConsensusModuleConfig::from_properties(&properties)
            .expect("the module's configuration settles");
        let idle_strategy = config.idle_strategy.clone();

        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread_faults = faults.clone();
        let sender = launched.clone();

        let thread = std::thread::spawn(move || {
            let Some(mut idle) = Strategy::by_agrona_name(&idle_strategy) else {
                thread_faults.record(format!("node {id}: unknown idle strategy {idle_strategy}"));
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

            while !thread_stop.load(Ordering::Relaxed) {
                match agent.do_work() {
                    Ok(work) => idle.idle(work),
                    Err(error) => {
                        thread_faults.record(format!("node {id}: the duty cycle: {error}"));
                        break;
                    }
                }
            }

            if let Err(error) = agent.close() {
                thread_faults.record(format!("node {id}: close: {error}"));
            }
        });

        Some(Self {
            _cluster_dir: cluster_dir,
            _archive_dir: archive_dir,
            driver,
            stop,
            thread: Some(thread),
        })
    }

    fn aeron_dir(&self) -> &std::path::Path {
        self.driver.aeron_dir()
    }

    /// Stop the module — the "the follower dies" of the card, with the media
    /// left running so the restart path has something to come back to (P3-6b).
    fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    fn is_stopped(&self) -> bool {
        self.thread.is_none()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The three counters this harness reads from a member's driver.
struct NodeCounters {
    cnc: CncFile,
}

impl NodeCounters {
    fn open(aeron_dir: &std::path::Path) -> Self {
        Self {
            cnc: CncFile::open(aeron_dir, DEADLINE).expect("the node's CnC file"),
        }
    }

    fn value(&self, type_id: i32) -> Option<i64> {
        let counters = self.cnc.counters()?;
        let id = find_cluster_counter(&counters, type_id, CLUSTER_ID)?;
        counters.value(id)
    }

    fn role(&self) -> Option<i64> {
        self.value(CLUSTER_NODE_ROLE_TYPE_ID)
    }

    fn commit_position(&self) -> Option<i64> {
        self.value(CLUSTER_COMMIT_POSITION_TYPE_ID)
    }

    fn election_state(&self) -> Option<i64> {
        self.value(CLUSTER_ELECTION_STATE_TYPE_ID)
    }

    fn leadership_term_id(&self) -> Option<i64> {
        self.value(CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID)
    }

    /// The log recording's position — the follower's append position, which is
    /// what the leader's quorum arithmetic reads. Counter type 100 is the
    /// archive's `RecordingPos`; it carries the session and stream in its label
    /// rather than a cluster key, so the largest of them is taken.
    fn append_position(&self) -> Option<i64> {
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
struct ClientSide {
    _driver: OwnDriver,
    client: Client,
    ingress_publication_id: i64,
    egress_subscription_id: i64,
    egress_channel: String,
}

impl ClientSide {
    fn start(ports: &Ports, leader_id: i32) -> Option<Self> {
        let driver = OwnDriver::start("cluster-three-nodes-client")?;
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

    fn offer(&self, bytes: &[u8]) -> bool {
        matches!(
            self.client.offer(self.ingress_publication_id, bytes),
            Some(deepmsg_core::logbuffer::append::Appended::Ok { .. })
        )
    }

    /// One client poll, and every `SessionEvent` it delivered.
    fn poll_events(&mut self) -> Vec<SessionEvent> {
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
}

#[derive(Clone, Copy, Debug)]
struct SessionEvent {
    cluster_session_id: i64,
    correlation_id: i64,
    leadership_term_id: i64,
    code: EventCode,
}

fn decode_session_event(payload: &[u8]) -> Option<SessionEvent> {
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

/// `SessionConnectRequestDecoder`'s encoding, written by hand as the reference
/// client writes it (`AeronCluster`): the response channel is the egress
/// subscription's own endpoint.
fn connect_request(correlation_id: i64, response_channel: &str) -> Vec<u8> {
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
        .client_info(b"three-node-harness");
    let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
    buffer.truncate(length);
    buffer
}

/// A `SessionMessageHeader` and its payload (`SessionMessageHeaderEncoder`).
fn ingress_message(leadership_term_id: i64, cluster_session_id: i64, payload: &[u8]) -> Vec<u8> {
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

fn wait_until<F>(what: &str, faults: &Faults, mut condition: F)
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

/// P3-6a's DoD, end to end.
///
/// One test rather than several: the three nodes share a port allocation, and
/// this binary's tests run in parallel threads of one process. The phases are
/// the card's sentences — elect, commit, and commit while a follower is dead.
#[test]
fn three_nodes_elect_commit_and_keep_committing_when_a_follower_dies() {
    let ports = Ports::new();
    let member_string = members(&ports);
    let faults = Faults::default();
    let (launched_tx, launched_rx) = mpsc::channel();

    let mut nodes = Vec::new();
    for id in 0..MEMBER_COUNT {
        let Some(node) = Node::start(id, &ports, &member_string, &faults, &launched_tx) else {
            archiving_driver::announce_skip();
            return;
        };
        nodes.push(node);
    }

    // ---- launched: every `launch` came back -------------------------------
    let deadline = Instant::now() + DEADLINE;
    let mut launched = 0;
    while launched < MEMBER_COUNT {
        assert!(
            Instant::now() < deadline,
            "only {launched} of {MEMBER_COUNT} modules launched; faults: {:?}",
            faults.0.lock().expect("the fault list")
        );
        match launched_rx.recv_timeout(Duration::from_millis(100)) {
            Ok((_, Ok(()))) => launched += 1,
            Ok((id, Err(error))) => panic!("node {id} did not launch: {error}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("every node died launching"),
        }
    }

    let counters: Vec<NodeCounters> = nodes
        .iter()
        .map(|node| NodeCounters::open(node.aeron_dir()))
        .collect();

    // ---- C1/C2/C3: one closed election, one term, one leader --------------
    wait_until("every member's election to close", &faults, || {
        counters
            .iter()
            .all(|node| node.election_state() == Some(ELECTION_CLOSED))
    });

    let terms: Vec<Option<i64>> = counters
        .iter()
        .map(NodeCounters::leadership_term_id)
        .collect();
    let term = terms[0].expect("the first member's leadership term");
    assert!(term >= 0, "a leadership term was established: {terms:?}");
    assert!(
        terms.iter().all(|value| *value == Some(term)),
        "the members agree on the leadership term: {terms:?}"
    );

    let roles: Vec<Option<i64>> = counters.iter().map(NodeCounters::role).collect();
    assert_eq!(
        1,
        roles
            .iter()
            .filter(|role| **role == Some(ROLE_LEADER))
            .count(),
        "exactly one leader: {roles:?}"
    );
    assert_eq!(
        MEMBER_COUNT as usize - 1,
        roles
            .iter()
            .filter(|role| **role == Some(ROLE_FOLLOWER))
            .count(),
        "everyone else is a follower: {roles:?}"
    );
    let leader_id = roles
        .iter()
        .position(|role| *role == Some(ROLE_LEADER))
        .expect("a leader") as i32;

    // ---- C4: a client opens a session through the leader's ingress --------
    let Some(mut client) = ClientSide::start(&ports, leader_id) else {
        archiving_driver::announce_skip();
        return;
    };

    let connect = connect_request(1, &client.egress_channel);
    let deadline = Instant::now() + DEADLINE;
    while !client.offer(&connect) {
        assert!(
            Instant::now() < deadline,
            "the ingress publication never accepted a connect request"
        );
        faults.check();
        std::thread::sleep(Duration::from_millis(5));
    }

    let mut open_event = None;
    wait_until("the session open event", &faults, || {
        for event in client.poll_events() {
            if event.correlation_id == 1 && event.code == EventCode::OK {
                open_event = Some(event);
            }
        }
        open_event.is_some()
    });
    let open_event = open_event.expect("the session opened");

    // ---- C5: three messages, committed on every member --------------------
    let before: Vec<Option<i64>> = counters.iter().map(NodeCounters::commit_position).collect();

    for index in 0..3_u8 {
        let message = ingress_message(
            open_event.leadership_term_id,
            open_event.cluster_session_id,
            &[b'0' + index],
        );
        let deadline = Instant::now() + DEADLINE;
        while !client.offer(&message) {
            assert!(
                Instant::now() < deadline,
                "the ingress publication is stuck"
            );
            faults.check();
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    let target = before.iter().flatten().copied().max().unwrap_or(0);
    wait_until("the leader to commit the three messages", &faults, || {
        client.poll_events();
        counters[leader_id as usize]
            .commit_position()
            .is_some_and(|position| position > target)
    });

    let leader_commit = counters[leader_id as usize]
        .commit_position()
        .expect("the leader's commit position");
    assert!(leader_commit > target);

    // A follower's *commit* counter is moved by the log adapter as it applies
    // the stream (`P3-6b`); what P3-6a owes is that its recording reached what
    // the leader committed — the append position that put it in the quorum.
    wait_until(
        "the followers' log recordings to reach the commit",
        &faults,
        || {
            client.poll_events();
            counters.iter().enumerate().all(|(index, node)| {
                index == leader_id as usize
                    || node
                        .append_position()
                        .is_some_and(|position| position >= leader_commit)
            })
        },
    );

    // ---- C6: kill a follower; the leader keeps committing -----------------
    let victim = (0..MEMBER_COUNT)
        .find(|id| *id != leader_id)
        .expect("a follower to stop");
    nodes[victim as usize].stop();
    assert!(nodes[victim as usize].is_stopped());
    let victim_append = counters[victim as usize].append_position();

    let before_kill = leader_commit;
    for index in 0..3_u8 {
        let message = ingress_message(
            open_event.leadership_term_id,
            open_event.cluster_session_id,
            &[b'a' + index],
        );
        let deadline = Instant::now() + DEADLINE;
        while !client.offer(&message) {
            assert!(
                Instant::now() < deadline,
                "the ingress publication is stuck"
            );
            faults.check();
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    wait_until(
        "the commit position to move with a follower stopped",
        &faults,
        || {
            client.poll_events();
            counters[leader_id as usize]
                .commit_position()
                .is_some_and(|position| position > before_kill)
        },
    );

    // The surviving follower keeps recording what the leader commits, which
    // is the append position the quorum was recomputed from; the dead one's
    // recording stays where it was, which is what makes this a kill rather
    // than a clean stop on all three.
    let survivor = (0..MEMBER_COUNT)
        .find(|id| *id != leader_id && *id != victim)
        .expect("the surviving follower");
    let leader_after_kill = counters[leader_id as usize]
        .commit_position()
        .expect("the leader's commit position");
    wait_until(
        "the surviving follower's recording to keep up",
        &faults,
        || {
            counters[survivor as usize]
                .append_position()
                .is_some_and(|position| position >= leader_after_kill)
        },
    );
    assert_eq!(
        victim_append,
        counters[victim as usize].append_position(),
        "the killed follower's recording did not move"
    );

    // ---- teardown ---------------------------------------------------------
    for node in &mut nodes {
        node.stop();
    }
    let _ = &nodes;
}
