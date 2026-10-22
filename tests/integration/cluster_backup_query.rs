//! The cluster's half of the backup protocol, end to end: a `BackupQuery` is
//! sent to a member's consensus endpoint and the member answers on the response
//! channel the query named.
//!
//! This pins the commit that made a member answer at all — until then the
//! message was decoded, counted and dropped. It is deliberately **not** the
//! backup (`ClusterBackup`, P3-9's other half): this test plays the backup by
//! hand, one query at a time, so a wrong answer's shape is found here rather
//! than after the agent, the binary and twenty system cases are written on top
//! of it (`analysis/cluster/records/P3-9.md` §3.4).
//!
//! Both call sites are exercised: the **leader** answers through
//! `processAllPendingSessions`, which it already walks for client sessions, and
//! a **follower** through its own `processPendingBackupSessions`
//! (`SessionManager.java:569-580`, `ConsensusModuleAgent.java:2417,2440`).
//!
//! # What the answer must say
//!
//! The reference's `backupResponse` (`ConsensusPublisher.java:482-523`) carries
//! this member's last recording-log term, the **id** of its commit-position
//! counter, who leads, who answered, the snapshot set it would give a backup
//! and the active member list. The id, not the value, is what makes the test
//! read the answering member's own CnC: an answer naming a counter that is not
//! the member's commit position fails even if the number returned happened to
//! be right.

use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cluster::control::cluster_control::ToggleState;
use deepmsg_cluster_common::counters::CLUSTER_CONTROL_TOGGLE_TYPE_ID;
use deepmsg_codec_cluster::backup_query_codec::BackupQueryEncoder;
use deepmsg_codec_cluster::backup_response_codec::{BackupResponseDecoder, SBE_TEMPLATE_ID};
use deepmsg_codec_cluster::message_header_codec::{
    ENCODED_LENGTH as MESSAGE_HEADER_LENGTH, MessageHeaderDecoder,
};
use deepmsg_codec_cluster::{ReadBuf, WriteBuf};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::cluster_harness::{
    ClientSide, DEADLINE, ELECTION_CLOSED, Faults, MEMBER_COUNT, Node, NodeCounters,
    PROTOCOL_VERSION, Ports, ROLE_LEADER, announce_skip, await_launched, members, wait_until,
};
use deepmsg_tests::driver::OwnDriver;

/// `aeron.cluster.consensus.stream.id`'s default (`ConsensusModule.java:614`).
/// The backup both publishes its query on it and listens for the answer on it
/// (`ClusterBackupAgent.java:170-172`: one stream id, `ctx.consensusStreamId()`).
const CONSENSUS_STREAM_ID: i32 = 108;

/// Where the response subscription listens: `Ports`' block uses `+30 + offset`
/// for a client's egress, and offsets 0..2 land on the members' archive ports.
const RESPONSE_EGRESS_OFFSET: u16 = 13;

/// One query's correlation id. The backup sends one at a time and matches the
/// answer by it (`ClusterBackupAgent.java:688-702`); so does this test.
const CORRELATION_ID: i64 = 42;

#[test]
fn a_member_answers_a_backup_query_from_its_own_recording_log() {
    let ports = Ports::new();
    let member_string = members(&ports);
    let faults = Faults::default();
    let (launched_tx, launched_rx) = std::sync::mpsc::channel();

    let mut nodes = Vec::new();
    for id in 0..MEMBER_COUNT {
        let Some(node) = Node::start(id, &ports, &member_string, &faults, &launched_tx) else {
            announce_skip();
            return;
        };
        nodes.push(node);
    }
    await_launched(&faults, &launched_rx, MEMBER_COUNT);

    let counters: Vec<NodeCounters> = nodes
        .iter()
        .map(|node| NodeCounters::open(node.aeron_dir()))
        .collect();

    wait_until("every member's election to close", &faults, || {
        counters
            .iter()
            .all(|node| node.election_state() == Some(ELECTION_CLOSED))
    });

    let leader_id = counters
        .iter()
        .position(|node| node.role() == Some(ROLE_LEADER))
        .expect("a leader") as i32;
    let follower_id = (0..MEMBER_COUNT)
        .find(|id| *id != leader_id)
        .expect("a follower");

    let Some(mut backup) = BackupSide::start(&ports) else {
        announce_skip();
        return;
    };

    // Both members answer the same query shape; the leader's answer and the
    // follower's are asserted identically, because the point of the two call
    // sites is that they agree.
    for (index, member_id) in [leader_id, follower_id].into_iter().enumerate() {
        let correlation_id = CORRELATION_ID + index as i64;
        let publication = backup.add_query_publication(&ports.consensus_channel(member_id));

        backup.send_query(publication, correlation_id);
        let response = backup.await_response(correlation_id);

        assert_eq!(
            correlation_id, response.correlation_id,
            "the answer echoes the query's correlation id"
        );
        assert_eq!(
            member_id, response.member_id,
            "member {member_id} answered for itself"
        );
        assert_eq!(
            leader_id, response.leader_member_id,
            "member {member_id} named the member the election said leads"
        );

        let term = counters[member_id as usize]
            .leadership_term_id()
            .expect("the member's leadership term");
        let last_term = nodes[member_id as usize]
            .last_term()
            .expect("the member's last recording-log term");

        // The five log fields are the member's **own** `recording.log`'s last
        // term, not the leader's advertisement of it: `findLastTerm` runs on
        // the member that answers (`SessionManager.java:672-690`).
        assert_eq!(
            last_term.recording_id, response.log_recording_id,
            "the answer names the recording of member {member_id}'s last term"
        );
        assert_eq!(
            last_term.leadership_term_id, response.log_leadership_term_id,
            "the answer's log term is the last term member {member_id} recorded"
        );
        assert_eq!(
            last_term.term_base_log_position, response.log_term_base_log_position,
            "the answer's log term base is the member's own"
        );
        assert_eq!(
            last_term.leadership_term_id, response.last_leadership_term_id,
            "the answer's last term is this member's own"
        );
        assert_eq!(
            last_term.term_base_log_position, response.last_term_base_log_position,
            "the answer's last term base is this member's own"
        );
        assert_eq!(
            term, response.last_leadership_term_id,
            "the member's recorded last term is the term its counter carries"
        );

        // The counter *id* is the member's own commit-position counter, and the
        // member's own CnC is where it has to be readable.
        let commit_by_type = counters[member_id as usize]
            .commit_position()
            .expect("the member's commit position");
        let commit_by_id = counters[member_id as usize]
            .counter_value(response.commit_position_counter_id)
            .unwrap_or_else(|| {
                panic!(
                    "the answer names counter {}, which member {member_id}'s CnC does not hold",
                    response.commit_position_counter_id
                )
            });
        assert_eq!(
            commit_by_type, commit_by_id,
            "counter {} is member {member_id}'s commit position",
            response.commit_position_counter_id
        );

        // A fresh cluster has taken no snapshot, so the answer's group is empty
        // — the reference's `findSnapshotAtOrBeforeOrLowest` with nothing to
        // find (`RecordingLog.java`).
        assert!(
            response.snapshots.is_empty(),
            "a fresh cluster answers no snapshots: {:?}",
            response.snapshots
        );

        let answered: Vec<&str> = response.cluster_members.split('|').collect();
        assert_eq!(
            MEMBER_COUNT as usize,
            answered.len(),
            "the answer names every member: {}",
            response.cluster_members
        );
        for (expected, entry) in (0..MEMBER_COUNT).zip(answered) {
            let id: i32 = entry
                .split(',')
                .next()
                .expect("an id")
                .parse()
                .expect("a member id");
            assert_eq!(expected, id, "the member list is in id order");
        }
    }

    for node in &mut nodes {
        node.stop();
    }
}

/// A backup played by hand: its own driver, one response subscription and one
/// query publication per member.
struct BackupSide {
    _driver: OwnDriver,
    client: Client,
    response_subscription: i64,
    response_channel: String,
}

impl BackupSide {
    fn start(ports: &Ports) -> Option<Self> {
        // The name carries the case's port block, as every other driver this
        // harness starts does (`cluster-node-{id}-{base}`,
        // `cluster-harness-client-{base}`, `cluster-backup-{base}`):
        // `ReferenceDriver::start_with_env` **removes** the directory it is
        // about to use (`tests/src/driver.rs`), so two cases of this binary
        // sharing a name delete each other's CnC file while they run on
        // threads of one process. That is what CI showed: both cases failing
        // at the connect below with `Cnc(NotFound)`.
        let driver = OwnDriver::start(&format!("cluster-backup-query-{}", ports.base()))?;
        let mut client = Client::connect(driver.aeron_dir()).expect("the backup's client connects");

        // The response channel is the backup's own consensus channel: the
        // member opens a publication to it and to `CONSENSUS_STREAM_ID`
        // (`ClusterBackupAgent.java:170-172`).
        //
        // It is the harness's egress channel shape. `refineResponseChannel`
        // lays the member's egress template over the channel the query carried;
        // a term-length mismatch is **not** observable here — this build's
        // receiver maps an image at the sender's term length
        // (`publication_images.rs`), as the reference does — so refinement's
        // egress-template branch rests on its unit tests (`agent/mod.rs`, the
        // `refine_response_channel` cases), not on this one.
        let response_channel = ports
            .with_egress_offset(RESPONSE_EGRESS_OFFSET)
            .client_egress_channel();
        let response_subscription = client
            .add_subscription(&response_channel, CONSENSUS_STREAM_ID, DEADLINE)
            .expect("the response subscription");

        Some(Self {
            _driver: driver,
            client,
            response_subscription,
            response_channel,
        })
    }

    fn add_query_publication(&mut self, consensus_channel: &str) -> i64 {
        self.client
            .add_publication(consensus_channel, CONSENSUS_STREAM_ID, DEADLINE)
            .expect("the query publication")
    }

    /// Offer the query until the publication has connected and accepted it —
    /// the reference retries a backup query on its interval, this retries until
    /// the test's deadline.
    fn send_query(&mut self, publication: i64, correlation_id: i64) {
        self.send_query_at(publication, correlation_id, i64::MAX);
    }

    /// The same, naming the log position the answer's snapshot set is chosen
    /// against (`ClusterBackupAgent.backupQuery`'s `logPosition` argument,
    /// `:690-700`).
    fn send_query_at(&mut self, publication: i64, correlation_id: i64, log_position: i64) {
        let message = backup_query_at(correlation_id, &self.response_channel, log_position);
        let deadline = Instant::now() + DEADLINE;
        loop {
            let outcome = self.client.offer(publication, &message);
            if matches!(outcome, Some(Appended::Ok { .. })) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the query publication never accepted the query: {outcome:?}"
            );
            self.client.poll();
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn await_response(&mut self, correlation_id: i64) -> BackupResponse {
        let deadline = Instant::now() + DEADLINE;
        loop {
            self.client.poll();

            let mut found = None;
            self.client
                .poll_subscription(self.response_subscription, 10, |message| {
                    if let Some(response) = decode_backup_response(message.payload) {
                        if response.correlation_id == correlation_id {
                            found = Some(response);
                        }
                    }
                });
            if let Some(response) = found {
                return response;
            }

            assert!(
                Instant::now() < deadline,
                "no answer to correlation id {correlation_id}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// `ConsensusPublisher.backupQuery`'s message, with the log position the
/// snapshot set is chosen against.
fn backup_query_at(correlation_id: i64, response_channel: &str, log_position: i64) -> Vec<u8> {
    let mut buffer = vec![0_u8; 256 + response_channel.len()];
    let encoder =
        BackupQueryEncoder::default().wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .correlation_id(correlation_id)
        .response_stream_id(CONSENSUS_STREAM_ID)
        .version(PROTOCOL_VERSION)
        .log_position(log_position)
        .response_channel(response_channel.as_bytes())
        .encoded_credentials(b"");
    let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
    buffer.truncate(length);
    buffer
}

#[derive(Debug)]
struct BackupResponse {
    correlation_id: i64,
    log_recording_id: i64,
    log_leadership_term_id: i64,
    log_term_base_log_position: i64,
    last_leadership_term_id: i64,
    last_term_base_log_position: i64,
    commit_position_counter_id: i32,
    leader_member_id: i32,
    member_id: i32,
    /// `(recording id, log position, service id)` per entry, in the group's
    /// order.
    snapshots: Vec<(i64, i64, i32)>,
    cluster_members: String,
}

fn decode_backup_response(payload: &[u8]) -> Option<BackupResponse> {
    if payload.len() < MESSAGE_HEADER_LENGTH {
        return None;
    }

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
    if header.schema_id() != deepmsg_codec_cluster::SBE_SCHEMA_ID
        || header.template_id() != SBE_TEMPLATE_ID
    {
        return None;
    }

    let decoder = BackupResponseDecoder::default().header(header, 0);
    let correlation_id = decoder.correlation_id();
    let log_recording_id = decoder.log_recording_id();
    let log_leadership_term_id = decoder.log_leadership_term_id();
    let log_term_base_log_position = decoder.log_term_base_log_position();
    let last_leadership_term_id = decoder.last_leadership_term_id();
    let last_term_base_log_position = decoder.last_term_base_log_position();
    let commit_position_counter_id = decoder.commit_position_counter_id();
    let leader_member_id = decoder.leader_member_id();
    let member_id = decoder.member_id();

    let mut snapshots = Vec::new();
    let mut group = decoder.snapshots_decoder();
    while group.advance().expect("the group reads").is_some() {
        snapshots.push((
            group.recording_id(),
            group.log_position(),
            group.service_id(),
        ));
    }
    let mut decoder = group.parent().expect("the group has a parent");

    let coordinates = decoder.cluster_members_decoder();
    let cluster_members =
        String::from_utf8_lossy(decoder.cluster_members_slice(coordinates)).into_owned();

    Some(BackupResponse {
        correlation_id,
        log_recording_id,
        log_leadership_term_id,
        log_term_base_log_position,
        last_leadership_term_id,
        last_term_base_log_position,
        commit_position_counter_id,
        leader_member_id,
        member_id,
        snapshots,
        cluster_members,
    })
}

/// `ClusterBackupTest.shouldQueryForSnapshotsWithLogPosition`
/// (`ClusterBackupTest.java:667-716`): a query that names a log position is
/// answered with the snapshot **at or before** it, or the lowest one when the
/// position precedes them all (`RecordingLog.findSnapshotAtOrBeforeOrLowest`).
///
/// Four snapshots, twenty messages apart, and a query per boundary. The answer
/// is the member's own recording log's snapshot — the group in
/// `BackupResponse`, not a counter's value.
#[test]
fn a_backup_query_finds_the_snapshot_at_or_before_its_log_position() {
    // The binary's other case takes `Ports::new()`'s block; this one takes the
    // next, because `cargo test` runs the two on threads of one process.
    static BASE: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
    let base = *BASE.get_or_init(|| Ports::new().base());
    let ports = Ports::with_base(base + 50);
    let member_string = members(&ports);
    let faults = Faults::default();
    let (launched_tx, launched_rx) = std::sync::mpsc::channel();

    let mut nodes = Vec::new();
    for id in 0..MEMBER_COUNT {
        let Some(node) = Node::start(id, &ports, &member_string, &faults, &launched_tx) else {
            announce_skip();
            return;
        };
        nodes.push(node);
    }
    await_launched(&faults, &launched_rx, MEMBER_COUNT);

    let counters: Vec<NodeCounters> = nodes
        .iter()
        .map(|node| NodeCounters::open(node.aeron_dir()))
        .collect();
    wait_until("every member's election to close", &faults, || {
        counters
            .iter()
            .all(|node| node.election_state() == Some(ELECTION_CLOSED))
    });

    let leader_id = counters
        .iter()
        .position(|node| node.role() == Some(ROLE_LEADER))
        .expect("a leader") as i32;
    let leader = &nodes[leader_id as usize];

    // Four rounds of twenty messages and one snapshot each
    // (`ClusterBackupTest.java:676-692`).
    let Some(mut client) = ClientSide::start(&ports, leader_id) else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&faults);
    for round in 1..=4 {
        // Twenty messages per round, and no replies to wait for: this cluster
        // has no service (the query path does not need one), so what the round
        // needs is the **log** to move before the snapshot is taken.
        let before = counters[leader_id as usize].commit_position().unwrap_or(0);
        for _ in 0..20 {
            client.send(&session, b"x", &faults);
        }
        wait_until("the round's messages on the log", &faults, || {
            counters[leader_id as usize]
                .commit_position()
                .is_some_and(|position| position > before)
        });

        counters[leader_id as usize].set(
            CLUSTER_CONTROL_TOGGLE_TYPE_ID,
            i64::from(ToggleState::Snapshot.code()),
        );
        wait_until("the leader's snapshot count", &faults, || {
            counters[leader_id as usize].snapshot_count() == Some(round)
        });
    }

    // The snapshots the member holds, oldest first — each round's entry in its
    // own `recording.log`.
    let snapshots = {
        use deepmsg_cluster::persist::recording_log::{
            self, CONSENSUS_MODULE_SERVICE_ID, ENTRY_TYPE_SNAPSHOT, RecordingLog,
        };
        let log = RecordingLog::open(&leader.cluster_dir().join(recording_log::FILENAME))
            .expect("the leader's recording log");
        log.entries()
            .iter()
            .filter(|entry| {
                entry.is_valid
                    && entry.entry_type == ENTRY_TYPE_SNAPSHOT
                    && entry.service_id == CONSENSUS_MODULE_SERVICE_ID
            })
            .map(|entry| (entry.recording_id, entry.log_position))
            .collect::<Vec<_>>()
    };
    assert_eq!(4, snapshots.len(), "four consensus-module snapshots");

    let Some(mut backup) = BackupSide::start(&ports) else {
        announce_skip();
        return;
    };
    let publication = backup.add_query_publication(&ports.consensus_channel(leader_id));

    // position → the snapshot the answer must name.
    let mut cases: Vec<(i64, usize)> = vec![(0, 0)];
    for (index, (_, position)) in snapshots.iter().enumerate() {
        cases.push((position - 1, index.saturating_sub(1)));
        cases.push((*position, index));
        cases.push((position + 1, index));
    }
    cases.push((snapshots[3].1 + 1_000_000, 3));
    cases.push((i64::MAX, 3));

    for (offset, (position, expected)) in cases.into_iter().enumerate() {
        let correlation_id = CORRELATION_ID + 100 + offset as i64;
        backup.send_query_at(publication, correlation_id, position);
        let response = backup.await_response(correlation_id);

        assert_eq!(
            1,
            response.snapshots.len(),
            "position {position} is answered with one snapshot: {:?}",
            response.snapshots
        );
        let (recording_id, log_position, service_id) = response.snapshots[0];
        assert_eq!(
            snapshots[expected].0, recording_id,
            "position {position} names snapshot {expected}'s recording"
        );
        assert_eq!(
            snapshots[expected].1, log_position,
            "position {position} names snapshot {expected}'s log position"
        );
        assert_eq!(
            deepmsg_cluster::persist::recording_log::CONSENSUS_MODULE_SERVICE_ID,
            service_id,
            "the snapshot is the consensus module's own"
        );
    }

    for node in &mut nodes {
        node.stop();
    }
}

/// **Two backups in one process have directories of their own** — the
/// regression guard for the shared driver name that made both cases of this
/// binary delete each other's CnC file (CI, the post-merge run of #135: both
/// panicked at the connect in [`BackupSide::start`] with `Cnc(NotFound)`).
///
/// `ReferenceDriver::start_with_env` removes the directory it is about to use,
/// so sharing a name across cases is sharing a directory — and the second case
/// to start deletes the first one's. Every other driver this harness starts
/// carries the case's port block in its name; this asserts the backup's does.
#[test]
fn two_backups_in_one_process_have_directories_of_their_own() {
    static BASE: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
    let base = *BASE.get_or_init(|| Ports::new().base());
    let first = Ports::with_base(base + 200);
    let second = Ports::with_base(base + 264);

    let Some(first_backup) = BackupSide::start(&first) else {
        announce_skip();
        return;
    };
    let Some(second_backup) = BackupSide::start(&second) else {
        announce_skip();
        return;
    };

    assert_ne!(
        first_backup._driver.aeron_dir(),
        second_backup._driver.aeron_dir(),
        "two cases' backups must not share an aeron directory"
    );
    // The property CI lost: the first backup's CnC is still connectable after
    // the second one started.
    let _client = Client::connect(first_backup._driver.aeron_dir())
        .expect("the first backup's CnC survives the second backup's start");
}
