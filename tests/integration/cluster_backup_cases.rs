//! `ClusterBackupTest` (`ClusterBackupTest.java`, P3-9), on this build's own
//! three-node harness.
//!
//! The reference's twenty cases are scenarios over `TestCluster`: a cluster of
//! three, a backup node that copies its log, then a **single** member started
//! from the backup's directories. Each case asserts two things — the backup
//! got there (its state, live-log and snapshot counters), and the member
//! started from what it copied has the cluster's messages and snapshot.
//!
//! # What stands in for the reference's fixture
//!
//! * `TestNode.TestService` → the harness's in-process `test-service`; its
//!   `messageCount` and `wasSnapshotLoaded` are [`TestServiceState`]'s.
//! * `TestBackupNode` → [`BackupNode`], the same five counters plus the mark
//!   file's error log.
//! * `TestCluster.startStaticNodeFromBackup` → [`BackupNode::into_directories`]
//!   followed by [`Node::start_over_directories`]: one member, on the backup's
//!   archive and cluster directories.
//!
//! # One case per binary?
//!
//! No: each case owns a `Ports` block of its own ([`CasePorts`]), because the
//! blocks are per *process* and `cargo test` runs the cases of one binary on
//! threads of one process.

use std::sync::mpsc;

use deepmsg_cluster::control::cluster_control::ToggleState;
use deepmsg_cluster::{BackupState, ReplayStart, SourceType};
use deepmsg_cluster_common::counters::CLUSTER_CONTROL_TOGGLE_TYPE_ID;
use deepmsg_tests::cluster_harness::{
    BackupNode, BackupOptions, ClientSide, ELECTION_CLOSED, Faults, MEMBER_COUNT, Node,
    NodeCounters, Ports, ROLE_LEADER, TestAuthenticator, TestCredentials, announce_skip,
    await_launched, members, wait_until,
};

/// One case at a time.
///
/// Each case starts three consensus modules, their drivers and a backup — four
/// or five busy loops apiece — and `cargo test` runs a binary's cases on as
/// many threads as the machine has cores. Two at once on a small runner starve
/// the elections the deadlines are measured against; the reference's own
/// cluster cases are serial for the same reason. The lock is a `Mutex`: a case
/// that fails poisons it, and the next case takes it anyway
/// (`PoisonError::into_inner`) because a failed case is not a reason to fail
/// the rest.
fn serial_guard() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A `Ports` block for one case.
struct CasePorts(Ports);

impl CasePorts {
    fn new(index: u16) -> Self {
        static BASE: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
        let base = *BASE.get_or_init(|| Ports::new().base());
        Self(Ports::with_base(base + (index + 1) * 50))
    }

    fn ports(&self) -> Ports {
        self.0
    }
}

/// A cluster of three, each member running the in-process test business.
struct Cluster {
    nodes: Vec<Node>,
    counters: Vec<NodeCounters>,
    ports: Ports,
    member_string: String,
    faults: Faults,
    launched_tx: mpsc::Sender<(i32, Result<(), String>)>,
    launched_rx: mpsc::Receiver<(i32, Result<(), String>)>,
}

impl Cluster {
    fn start(ports: Ports) -> Option<Self> {
        Self::start_inner(ports, None)
    }

    /// A cluster whose members authenticate their sessions — the reference's
    /// `withAuthenticationSupplier`.
    fn start_authenticated(
        ports: Ports,
        authenticator: &dyn Fn() -> TestAuthenticator,
    ) -> Option<Self> {
        Self::start_inner(ports, Some(authenticator))
    }

    fn start_inner(
        ports: Ports,
        authenticator: Option<&dyn Fn() -> TestAuthenticator>,
    ) -> Option<Self> {
        let member_string = members(&ports);
        let faults = Faults::default();
        let (launched_tx, launched_rx) = mpsc::channel();

        let mut nodes = Vec::new();
        for id in 0..MEMBER_COUNT {
            let node = match authenticator {
                None => Node::start_with_service(
                    id,
                    &ports,
                    &member_string,
                    &faults,
                    &launched_tx,
                    "test-service",
                ),
                Some(authenticator) => Node::start_with_service_and_authenticator(
                    id,
                    &ports,
                    &member_string,
                    &faults,
                    &launched_tx,
                    "test-service",
                    authenticator(),
                ),
            };
            let Some(node) = node else {
                announce_skip();
                return None;
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

        Some(Self {
            nodes,
            counters,
            ports,
            member_string,
            faults,
            launched_tx,
            launched_rx,
        })
    }

    fn leader(&self) -> i32 {
        self.counters
            .iter()
            .position(|node| node.role() == Some(ROLE_LEADER))
            .expect("a leader") as i32
    }

    fn leader_counters(&self) -> &NodeCounters {
        &self.counters[usize::try_from(self.leader()).unwrap()]
    }

    /// The position the cluster's log has reached — the reference's
    /// `leader.service().cluster().logPosition()`, read through the commit
    /// counter the service applies up to.
    fn log_position(&self) -> i64 {
        self.leader_counters()
            .commit_position()
            .expect("the leader's commit position")
    }

    fn client(&self) -> Option<ClientSide> {
        ClientSide::start(&self.ports, self.leader())
    }

    /// The reference's `sendAndAwaitMessages`: open a session, send `count`
    /// messages and wait for as many replies.
    fn send_and_await_messages(&self, client: &mut ClientSide, count: usize) {
        let session = client.connect_session(&self.faults);
        client.send_and_await_messages(&session, count, &self.faults);
    }

    /// The reference's `takeSnapshot` (`ClusterControl.ToggleState.SNAPSHOT`).
    fn take_snapshot(&self) {
        self.leader_counters().set(
            CLUSTER_CONTROL_TOGGLE_TYPE_ID,
            i64::from(ToggleState::Snapshot.code()),
        );
    }

    /// The reference's `awaitSnapshotCount`: every member has taken `count`.
    fn await_snapshot_count(&self, count: i64) {
        wait_until("every member's snapshot count", &self.faults, || {
            self.counters
                .iter()
                .all(|node| node.snapshot_count() == Some(count))
        });
    }

    /// The reference's `shutdownCluster`: an orderly `SHUTDOWN`, which
    /// snapshots and then terminates every member.
    fn shutdown(&mut self) {
        self.leader_counters().set(
            CLUSTER_CONTROL_TOGGLE_TYPE_ID,
            i64::from(ToggleState::Shutdown.code()),
        );
        wait_until("every member's shutdown", &self.faults, || {
            self.nodes.iter().all(Node::has_finished)
        });
        self.stop_all();
    }

    fn stop_all(&mut self) {
        for node in &mut self.nodes {
            node.stop_media();
        }
    }

    /// Bring the whole cluster back on its own directories, as the reference's
    /// `restartAllNodes` does after a shutdown.
    fn restart_all(&mut self) {
        let launched = self.launched_tx.clone();
        for node in &mut self.nodes {
            node.restart_whole(&launched);
        }
        await_launched(&self.faults, &self.launched_rx, MEMBER_COUNT);

        self.counters = self
            .nodes
            .iter()
            .map(|node| NodeCounters::open(node.aeron_dir()))
            .collect();
        wait_until("every member's election to close", &self.faults, || {
            self.counters
                .iter()
                .all(|node| node.election_state() == Some(ELECTION_CLOSED))
        });
    }

    /// Stop one member — the reference's `stopNode`, which closes the module,
    /// its container and its media.
    fn stop_node(&mut self, id: i32) {
        self.nodes[usize::try_from(id).unwrap()].stop_media();
    }

    /// The reference's `startClusterBackupNode` with the fixture's defaults.
    fn start_backup(&self) -> Option<BackupNode> {
        BackupNode::start(&self.ports, &self.member_string, &self.faults)
    }

    fn start_backup_with(&self, options: BackupOptions) -> Option<BackupNode> {
        BackupNode::start_with(&self.ports, &self.member_string, &self.faults, options)
    }

    /// Wait for the backup to reach `BACKING_UP` and its live log position to
    /// reach `position` — the reference's `awaitBackupLiveLogPosition`.
    fn await_backup_live(&self, backup: &BackupNode, position: i64) {
        wait_until("the backup to reach BACKING_UP", &self.faults, || {
            backup.state() == Some(BackupState::BackingUp.code())
        });
        wait_until("the backup's live log position", &self.faults, || {
            backup
                .live_log_position()
                .is_some_and(|live| live >= position)
        });
    }

    /// The reference's `startStaticNodeFromBackup`: one member over the
    /// backup's archive and cluster directories.
    fn start_from_backup(&mut self, backup: BackupNode) -> Node {
        let (archive_dir, cluster_dir) = backup.into_directories();
        let id = MEMBER_COUNT;
        let member_string = format!(
            "{id},localhost:{},localhost:{},localhost:{},localhost:0,localhost:{}",
            self.ports.ingress(id),
            self.ports.consensus(id),
            self.ports.log(id),
            self.ports.archive(id),
        );

        let node = Node::start_over_directories(
            id,
            &self.ports,
            &member_string,
            &self.faults,
            &self.launched_tx,
            Some("test-service"),
            archive_dir,
            cluster_dir,
        )
        .expect("the member started from the backup comes up");

        await_launched(&self.faults, &self.launched_rx, 1);
        node
    }
}

/// The reference's `awaitServiceMessageCount`: the business has applied
/// `count` messages.
fn await_messages(node: &Node, faults: &Faults, count: usize) {
    let state = node.service_state().expect("the test business");
    wait_until("the service's message count", faults, || {
        state.messages() == count
    });
}

/// `ClusterBackupTest.shouldBackupClusterNoSnapshotsAndEmptyLog`
/// (`ClusterBackupTest.java:60-79`).
#[test]
fn should_backup_cluster_no_snapshots_and_empty_log() {
    let _serial = serial_guard();
    let ports = CasePorts::new(0);
    let Some(mut cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let log_position = cluster.log_position();

    let Some(backup) = cluster.start_backup() else {
        announce_skip();
        return;
    };
    cluster.await_backup_live(&backup, log_position);

    cluster.stop_all();
    let mut node = cluster.start_from_backup(backup);

    let state = node.service_state().expect("the test business");
    assert_eq!(0, state.messages(), "the empty log carries no messages");
    assert!(
        !state.was_snapshot_loaded(),
        "an empty log needs no snapshot"
    );

    node.stop_media();
}

/// `shouldBackupClusterNoSnapshotsAndNonEmptyLog`, for one `SourceType`
/// (`ClusterBackupTest.java:81-108`).
fn backup_cluster_no_snapshots_and_non_empty_log(source_type: SourceType, index: u16) {
    let _serial = serial_guard();
    let ports = CasePorts::new(index);
    let Some(mut cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    cluster.send_and_await_messages(&mut client, 10);
    let log_position = cluster.log_position();

    let options = BackupOptions {
        source_type,
        ..Default::default()
    };
    let Some(backup) = cluster.start_backup_with(options) else {
        announce_skip();
        return;
    };
    cluster.await_backup_live(&backup, log_position);

    cluster.stop_all();
    let mut node = cluster.start_from_backup(backup);
    await_messages(&node, &cluster.faults, 10);
    assert!(!node.service_state().expect("state").was_snapshot_loaded());

    node.stop_media();
}

/// `ClusterBackupTest.shouldBackupClusterNoSnapshotsAndNonEmptyLog` with
/// `@EnumSource(SourceType)`: `ANY`, `LEADER` and `FOLLOWER`.
#[test]
fn should_backup_cluster_no_snapshots_and_non_empty_log_from_any() {
    backup_cluster_no_snapshots_and_non_empty_log(SourceType::Any, 1);
}

#[test]
fn should_backup_cluster_no_snapshots_and_non_empty_log_from_leader() {
    backup_cluster_no_snapshots_and_non_empty_log(SourceType::Leader, 2);
}

#[test]
fn should_backup_cluster_no_snapshots_and_non_empty_log_from_follower() {
    backup_cluster_no_snapshots_and_non_empty_log(SourceType::Follower, 3);
}

/// `shouldBackupClusterNoSnapshotsAndThenSendMessages`
/// (`ClusterBackupTest.java:110-136`).
#[test]
fn should_backup_cluster_no_snapshots_and_then_send_messages() {
    let _serial = serial_guard();
    let ports = CasePorts::new(4);
    let Some(mut cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let Some(backup) = cluster.start_backup() else {
        announce_skip();
        return;
    };
    wait_until("the backup to reach BACKING_UP", &cluster.faults, || {
        backup.state() == Some(BackupState::BackingUp.code())
    });

    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    cluster.send_and_await_messages(&mut client, 10);
    let log_position = cluster.log_position();
    cluster.await_backup_live(&backup, log_position);

    cluster.stop_all();
    let mut node = cluster.start_from_backup(backup);
    await_messages(&node, &cluster.faults, 10);
    assert!(!node.service_state().expect("state").was_snapshot_loaded());

    node.stop_media();
}

/// `shouldBackupClusterWithSnapshot` (`ClusterBackupTest.java:138-168`).
#[test]
fn should_backup_cluster_with_snapshot() {
    let _serial = serial_guard();
    let ports = CasePorts::new(5);
    let Some(mut cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    cluster.send_and_await_messages(&mut client, 10);
    cluster.take_snapshot();
    cluster.await_snapshot_count(1);
    let log_position = cluster.log_position();

    let Some(backup) = cluster.start_backup() else {
        announce_skip();
        return;
    };
    cluster.await_backup_live(&backup, log_position);
    wait_until("the backup's snapshot count", &cluster.faults, || {
        backup.snapshot_retrieve_count() == Some(1)
    });

    cluster.stop_all();
    let mut node = cluster.start_from_backup(backup);
    await_messages(&node, &cluster.faults, 10);
    assert!(
        node.service_state().expect("state").was_snapshot_loaded(),
        "the member came up from the snapshot the backup carried"
    );

    node.stop_media();
}

/// `shouldBackupClusterAfterCleanShutdown` (`ClusterBackupTest.java:170-203`).
#[test]
fn should_backup_cluster_after_clean_shutdown() {
    let _serial = serial_guard();
    let ports = CasePorts::new(6);
    let Some(mut cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    cluster.send_and_await_messages(&mut client, 10);

    cluster.shutdown();
    for node in &cluster.nodes {
        assert!(
            node.service_state().expect("state").was_snapshot_taken(),
            "a clean shutdown snapshots every member's service"
        );
    }

    cluster.restart_all();
    let log_position = cluster.log_position();

    let Some(backup) = cluster.start_backup() else {
        announce_skip();
        return;
    };
    cluster.await_backup_live(&backup, log_position);
}

/// `shouldBackupClusterWithSnapshotAndNonEmptyLog`
/// (`ClusterBackupTest.java:205-241`).
#[test]
fn should_backup_cluster_with_snapshot_and_non_empty_log() {
    let _serial = serial_guard();
    let ports = CasePorts::new(7);
    let Some(mut cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);
    client.send_and_await_messages(&session, 10, &cluster.faults);
    cluster.take_snapshot();
    cluster.await_snapshot_count(1);

    client.send_and_await_messages(&session, 7, &cluster.faults);
    let log_position = cluster.log_position();

    let Some(backup) = cluster.start_backup() else {
        announce_skip();
        return;
    };
    cluster.await_backup_live(&backup, log_position);

    cluster.stop_all();
    let mut node = cluster.start_from_backup(backup);
    await_messages(&node, &cluster.faults, 17);
    assert!(node.service_state().expect("state").was_snapshot_loaded());

    node.stop_media();
}

/// `shouldBackupClusterWithSnapshotThenSend`
/// (`ClusterBackupTest.java:375-411`).
#[test]
fn should_backup_cluster_with_snapshot_then_send() {
    let _serial = serial_guard();
    let ports = CasePorts::new(8);
    let Some(mut cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);
    client.send_and_await_messages(&session, 10, &cluster.faults);
    cluster.take_snapshot();
    cluster.await_snapshot_count(1);

    let Some(backup) = cluster.start_backup() else {
        announce_skip();
        return;
    };

    client.send_and_await_messages(&session, 7, &cluster.faults);
    let log_position = cluster.log_position();
    cluster.await_backup_live(&backup, log_position);

    cluster.stop_all();
    let mut node = cluster.start_from_backup(backup);
    await_messages(&node, &cluster.faults, 17);
    assert!(node.service_state().expect("state").was_snapshot_loaded());

    node.stop_media();
}

/// `shouldBackupClusterWithFromLatestSnapshotLogPosition`
/// (`ClusterBackupTest.java:413-455`).
#[test]
fn should_backup_cluster_with_from_latest_snapshot_log_position() {
    let _serial = serial_guard();
    let ports = CasePorts::new(9);
    let Some(mut cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);
    client.send_and_await_messages(&session, 10, &cluster.faults);
    cluster.take_snapshot();
    cluster.await_snapshot_count(1);
    let snapshot_position = cluster.log_position();

    client.send_and_await_messages(&session, 7, &cluster.faults);
    let log_position = cluster.log_position();

    let options = BackupOptions {
        initial_replay_start: ReplayStart::LatestSnapshot,
        ..Default::default()
    };
    let Some(backup) = cluster.start_backup_with(options) else {
        announce_skip();
        return;
    };
    cluster.await_backup_live(&backup, log_position);

    assert_eq!(
        Some(snapshot_position),
        backup.recording_log_start_position(),
        "the backup's recording starts where the snapshot does"
    );

    cluster.stop_all();
    let mut node = cluster.start_from_backup(backup);
    await_messages(&node, &cluster.faults, 17);
    assert!(node.service_state().expect("state").was_snapshot_loaded());

    node.stop_media();
}

/// `shouldBeAbleToGetTimeOfNextBackupQuery`
/// (`ClusterBackupTest.java:457-471`).
#[test]
fn should_be_able_to_get_time_of_next_backup_query() {
    let _serial = serial_guard();
    let ports = CasePorts::new(10);
    let Some(cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let Some(backup) = cluster.start_backup() else {
        announce_skip();
        return;
    };
    wait_until("the backup to reach BACKING_UP", &cluster.faults, || {
        backup.state() == Some(BackupState::BackingUp.code())
    });

    let now_ms = deepmsg_core::clock::epoch_millis();
    let deadline = backup.next_query_deadline_ms().expect("counter 210");
    assert!(
        deadline > now_ms,
        "the next query is in the future: deadline={deadline} now={now_ms}"
    );

    drop(backup);
}

/// `shouldBackupClusterNoSnapshotsAndNonEmptyLogWithReQuery`
/// (`ClusterBackupTest.java:473-507`).
#[test]
fn should_backup_cluster_no_snapshots_and_non_empty_log_with_re_query() {
    let _serial = serial_guard();
    let ports = CasePorts::new(11);
    let Some(mut cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);
    client.send_and_await_messages(&session, 10, &cluster.faults);
    let log_position = cluster.log_position();

    let Some(backup) = cluster.start_backup() else {
        announce_skip();
        return;
    };
    cluster.await_backup_live(&backup, log_position);
    let now_ms = deepmsg_core::clock::epoch_millis();
    assert!(
        backup
            .next_query_deadline_ms()
            .is_some_and(|deadline| deadline > now_ms),
        "a live deadline, not the null the agent starts with"
    );

    client.send_and_await_messages(&session, 5, &cluster.faults);
    let next_log_position = cluster.log_position();
    cluster.await_backup_live(&backup, next_log_position);

    cluster.stop_all();
    let mut node = cluster.start_from_backup(backup);
    await_messages(&node, &cluster.faults, 15);

    node.stop_media();
}

/// `shouldBackupClusterNoSnapshotsAndNonEmptyLogAfterFailure`
/// (`ClusterBackupTest.java:509-537`).
#[test]
fn should_backup_cluster_no_snapshots_and_non_empty_log_after_failure() {
    let _serial = serial_guard();
    let ports = CasePorts::new(12);
    let Some(mut cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    cluster.send_and_await_messages(&mut client, 10);

    let leader = cluster.leader();
    cluster.stop_node(leader);
    wait_until("a new leader", &cluster.faults, || {
        cluster
            .counters
            .iter()
            .any(|n| n.role() == Some(ROLE_LEADER))
    });
    let log_position = cluster.log_position();

    let Some(backup) = cluster.start_backup() else {
        announce_skip();
        return;
    };
    cluster.await_backup_live(&backup, log_position);

    cluster.stop_all();
    let mut node = cluster.start_from_backup(backup);
    await_messages(&node, &cluster.faults, 10);
    assert!(!node.service_state().expect("state").was_snapshot_loaded());

    node.stop_media();
}

/// `shouldBackupClusterNoSnapshotsAndNonEmptyLogWithFailure`
/// (`ClusterBackupTest.java:539-579`): the leader dies **after** the backup
/// has caught up, and the backup keeps up with the member that replaces it.
#[test]
fn should_backup_cluster_no_snapshots_and_non_empty_log_with_failure() {
    let _serial = serial_guard();
    let ports = CasePorts::new(13);
    let Some(mut cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    cluster.send_and_await_messages(&mut client, 10);
    let log_position = cluster.log_position();

    let Some(backup) = cluster.start_backup() else {
        announce_skip();
        return;
    };
    cluster.await_backup_live(&backup, log_position);

    let leader = cluster.leader();
    cluster.stop_node(leader);
    wait_until("a new leader", &cluster.faults, || {
        cluster
            .counters
            .iter()
            .any(|n| n.role() == Some(ROLE_LEADER))
    });

    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    cluster.send_and_await_messages(&mut client, 5);
    let next_log_position = cluster.log_position();
    cluster.await_backup_live(&backup, next_log_position);

    cluster.stop_all();
    let mut node = cluster.start_from_backup(backup);
    await_messages(&node, &cluster.faults, 15);
    assert!(!node.service_state().expect("state").was_snapshot_loaded());

    node.stop_media();
}

/// `shouldBackupClusterAndJoinLive` (`ClusterBackupTest.java:612-635`).
#[test]
fn should_backup_cluster_and_join_live() {
    let _serial = serial_guard();
    let ports = CasePorts::new(14);
    let Some(mut cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);
    client.send_and_await_messages(&session, 100, &cluster.faults);

    let Some(backup) = cluster.start_backup() else {
        announce_skip();
        return;
    };

    client.send_and_await_messages(&session, 100, &cluster.faults);
    await_messages(
        &cluster.nodes[usize::try_from(cluster.leader()).unwrap()],
        &cluster.faults,
        200,
    );
    let log_position = cluster.log_position();
    cluster.await_backup_live(&backup, log_position);

    cluster.stop_all();
    let mut node = cluster.start_from_backup(backup);
    await_messages(&node, &cluster.faults, 200);

    node.stop_media();
}

/// `shouldBackupClusterWithSnapshotAndNonEmptyLogWithSimpleAuthentication`
/// (`ClusterBackupTest.java:243-288`): the same scenario with a cluster that
/// authenticates and a backup that offers the right simple credentials.
#[test]
fn should_backup_cluster_with_snapshot_and_non_empty_log_with_simple_authentication() {
    let _serial = serial_guard();
    let ports = CasePorts::new(15);
    let authenticator = || TestAuthenticator::simple(b"admin", b"admin");
    let Some(mut cluster) = Cluster::start_authenticated(ports.ports(), &authenticator) else {
        return;
    };
    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    let session = client.connect_session_with(b"admin", &[], &cluster.faults);
    client.send_and_await_messages(&session, 10, &cluster.faults);
    cluster.take_snapshot();
    cluster.await_snapshot_count(1);

    client.send_and_await_messages(&session, 7, &cluster.faults);
    let log_position = cluster.log_position();

    let options = BackupOptions {
        credentials: Some(TestCredentials {
            encoded: b"admin".to_vec(),
            challenge_answer: Vec::new(),
        }),
        ..Default::default()
    };
    let Some(backup) = cluster.start_backup_with(options) else {
        announce_skip();
        return;
    };
    cluster.await_backup_live(&backup, log_position);

    cluster.stop_all();
    let mut node = cluster.start_from_backup(backup);
    await_messages(&node, &cluster.faults, 17);
    assert!(node.service_state().expect("state").was_snapshot_loaded());

    node.stop_media();
}

/// `shouldBackupClusterWithSnapshotAndNonEmptyLogWithChallengeResponseAuthentication`
/// (`ClusterBackupTest.java:290-332`): the cluster challenges, and both the
/// client and the backup answer it.
#[test]
fn should_backup_cluster_with_snapshot_and_non_empty_log_with_challenge_response_authentication() {
    let _serial = serial_guard();
    let ports = CasePorts::new(16);
    let authenticator = || TestAuthenticator::challenge_response(b"challenge", b"answer", b"admin");
    let Some(mut cluster) = Cluster::start_authenticated(ports.ports(), &authenticator) else {
        return;
    };
    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    let session = client.connect_session_with(b"principal", b"answer", &cluster.faults);
    client.send_and_await_messages(&session, 10, &cluster.faults);
    cluster.take_snapshot();
    cluster.await_snapshot_count(1);

    client.send_and_await_messages(&session, 7, &cluster.faults);
    let log_position = cluster.log_position();

    let options = BackupOptions {
        credentials: Some(TestCredentials {
            encoded: b"principal".to_vec(),
            challenge_answer: b"answer".to_vec(),
        }),
        ..Default::default()
    };
    let Some(backup) = cluster.start_backup_with(options) else {
        announce_skip();
        return;
    };
    cluster.await_backup_live(&backup, log_position);

    cluster.stop_all();
    let mut node = cluster.start_from_backup(backup);
    await_messages(&node, &cluster.faults, 17);
    assert!(node.service_state().expect("state").was_snapshot_loaded());

    node.stop_media();
}

/// The shared shape of the two refusal cases: the backup is refused, resets,
/// and the refusal is in its mark file's error log — not silently retried.
fn a_refused_backup_logs_the_error(
    ports: Ports,
    authenticator: TestAuthenticator,
    credentials: TestCredentials,
) {
    let authenticator = move || authenticator.clone();
    let Some(cluster) = Cluster::start_authenticated(ports, &authenticator) else {
        return;
    };
    // The refusal is the case: the backup's duty cycle fails on purpose, and
    // the harness must not read that as a cluster failure.
    cluster.faults.expect("AUTHENTICATION_REJECTED");

    let options = BackupOptions {
        credentials: Some(credentials),
        ..Default::default()
    };
    let Some(backup) = cluster.start_backup_with(options) else {
        announce_skip();
        return;
    };

    wait_until("the backup to reset", &cluster.faults, || {
        backup.state() == Some(BackupState::ResetBackup.code())
    });
    wait_until(
        "the refusal in the backup's error log",
        &cluster.faults,
        || {
            backup
                .error_log()
                .iter()
                .any(|entry| entry.contains("AUTHENTICATION_REJECTED"))
        },
    );
    assert!(
        backup.error_count().is_some_and(|count| count > 0),
        "the refusal is counted in counter 211"
    );
}

/// `shouldLogErrorWithInvalidCredentials`
/// (`ClusterBackupTest.java:334-352`).
#[test]
fn should_log_error_with_invalid_credentials() {
    let _serial = serial_guard();
    let ports = CasePorts::new(17);
    a_refused_backup_logs_the_error(
        ports.ports(),
        TestAuthenticator::simple(b"admin", b"admin"),
        TestCredentials {
            encoded: b"not-admin".to_vec(),
            challenge_answer: Vec::new(),
        },
    );
}

/// `shouldLogErrorWithInvalidChallengeResponse`
/// (`ClusterBackupTest.java:354-373`).
#[test]
fn should_log_error_with_invalid_challenge_response() {
    let _serial = serial_guard();
    let ports = CasePorts::new(18);
    a_refused_backup_logs_the_error(
        ports.ports(),
        TestAuthenticator::challenge_response(b"challenge", b"answer", b"admin"),
        TestCredentials {
            encoded: b"principal".to_vec(),
            challenge_answer: b"wrong-answer".to_vec(),
        },
    );
}

/// `shouldResumeBackupIfStopped` (`ClusterBackupTest.java:637-665`), for one
/// catchup port: a backup stopped part way through resumes where it left off
/// — it **extends** its recording rather than starting a new one — and catches
/// up with what the cluster committed while it was down.
///
/// `weakened: 100_000 messages → 2_000`. The reference's count exists to cross
/// a log-segment boundary before the backup is stopped; this build's segments
/// are the same size (the archive's default), and the resume path is what the
/// case is about, so the count is smaller to keep the suite's wall clock sane.
fn resume_backup_if_stopped(catchup_port: u16, index: u16) {
    let _serial = serial_guard();
    let ports = CasePorts::new(index);
    let Some(cluster) = Cluster::start(ports.ports()) else {
        return;
    };
    let Some(mut client) = cluster.client() else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);
    client.send_and_await_messages(&session, 2_000, &cluster.faults);

    let options = || BackupOptions {
        catchup_port,
        ..Default::default()
    };
    let Some(backup) = cluster.start_backup_with(options()) else {
        announce_skip();
        return;
    };
    cluster.await_backup_live(&backup, 1);

    // The backup stops, keeping its directories — the reference's
    // `backupNode.close()` followed by `startClusterBackupNode(cleanStart=
    // false)`.
    let (archive_dir, cluster_dir) = backup.into_directories();

    client.send_and_await_messages(&session, 1_000, &cluster.faults);
    let log_position = cluster.log_position();

    let Some(backup) = BackupNode::start_over_directories(
        &cluster.ports,
        &cluster.member_string,
        &cluster.faults,
        options(),
        archive_dir,
        cluster_dir,
    ) else {
        announce_skip();
        return;
    };
    cluster.await_backup_live(&backup, log_position);
}

/// `shouldResumeBackupIfStopped` with the catchup endpoint's port chosen by the
/// kernel (`@ValueSource(ints = { 0, 8833 })`, first arm).
#[test]
fn should_resume_backup_if_stopped_with_a_wildcard_catchup_port() {
    resume_backup_if_stopped(0, 19);
}

/// The same, with a fixed catchup port.
#[test]
fn should_resume_backup_if_stopped_with_a_fixed_catchup_port() {
    let ports = CasePorts::new(20);
    resume_backup_if_stopped(ports.ports().backup_catchup(), 20);
}
