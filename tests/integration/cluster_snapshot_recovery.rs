//! P3-6c's F3-a: **the snapshot a cluster comes back on**.
//!
//! The three rows of `records/P3-6c.md`'s F3 family that need no new product
//! behaviour — every member snapshots, the members stop, they restart on their
//! own directories, and the state they come back with is the one the snapshot
//! carries:
//!
//! * `ClusterTest.shouldStopLeaderAndFollowersThenRestartAllWithSnapshot`
//!   (`ClusterTest.java:254`) — a `SNAPSHOT` toggle, a stop, a restart, and
//!   every business handed its snapshot;
//! * `ClusterTest.shouldShutdownClusterAndRestartWithSnapshots` (`:338`) — the
//!   orderly `SHUTDOWN`, whose snapshot is the one the restart loads (the
//!   restart half of
//!   [`deepmsg_tests::cluster_harness`]'s `shutdown_snapshots_and_then_ends_the_duty_cycle`);
//! * `MultiNodeTest.shouldReplayWithAppointedLeaderWithThreeNodesWithNoSnapshot`
//!   (`MultiNodeTest.java:63`) — an appointed leader serves, every member
//!   restarts, and the log alone carries the ten messages back.
//!
//! # What a case reads
//!
//! The reference asserts through the business: `wasSnapshotTaken`,
//! `wasSnapshotLoaded`, and its message count (`TestNode.java:440-451`,
//! `:516-529`). These cases read the same three things through the harness's
//! in-process `test-service`; the snapshot counter is read as the reference's
//! `awaitSnapshotCount` does. Where the reference does **not** assert
//! something — the appointed leader after the restart — the case says so.

use std::sync::mpsc;

use deepmsg_cluster::control::cluster_control::ToggleState;
use deepmsg_cluster_common::counters::CLUSTER_CONTROL_TOGGLE_TYPE_ID;
use deepmsg_tests::cluster_harness::{
    ClientSide, ELECTION_CLOSED, Faults, MEMBER_COUNT, Node, NodeCounters, Ports, ROLE_LEADER,
    announce_skip, await_every_snapshot_count, await_launched, await_snapshots_loaded, leader_of,
    members, take_snapshot, wait_until,
};

/// One case at a time: three members plus their drivers per case, and the
/// deadlines below are wall-clock.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A `Ports` block of this case's own — the blocks are per *process*, and the
/// spacing is the block width (sixty) with room to spare.
struct CasePorts(Ports);

impl CasePorts {
    fn new(index: u16) -> Self {
        static BASE: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
        let base = *BASE.get_or_init(|| Ports::new().base());
        Self(Ports::with_base(base + (index + 1) * 64))
    }

    fn ports(&self) -> &Ports {
        &self.0
    }
}

/// Three members, each running the in-process test business.
struct Cluster {
    nodes: Vec<Node>,
    counters: Vec<NodeCounters>,
    faults: Faults,
    launched_tx: mpsc::Sender<(i32, Result<(), String>)>,
    launched_rx: mpsc::Receiver<(i32, Result<(), String>)>,
}

impl Cluster {
    /// `module_arguments` are the per-member settings the case varies
    /// (`-Daeron.cluster.appointed.leader.id`, in row 13).
    fn start(ports: Ports, module_arguments: &[&str]) -> Option<Self> {
        let member_string = members(&ports);
        let faults = Faults::default();
        let (launched_tx, launched_rx) = mpsc::channel();

        let mut nodes = Vec::new();
        for id in 0..MEMBER_COUNT {
            let Some(node) = Node::start_with_properties(
                id,
                &ports,
                &member_string,
                &faults,
                &launched_tx,
                Some("test-service"),
                &[],
                module_arguments,
            ) else {
                announce_skip();
                return None;
            };
            nodes.push(node);
        }
        await_launched(&faults, &launched_rx, MEMBER_COUNT);

        let mut cluster = Self {
            nodes,
            counters: Vec::new(),
            faults,
            launched_tx,
            launched_rx,
        };
        cluster.open_counters();
        cluster.await_closed();
        Some(cluster)
    }

    /// The counters belong to the driver and are allocated per module run, so
    /// a restart leaves the old mapping stale — reopen them, as the restart
    /// cases in `cluster_restart_path` do.
    fn open_counters(&mut self) {
        self.counters = self
            .nodes
            .iter()
            .map(|node| NodeCounters::open(node.aeron_dir()))
            .collect();
    }

    fn await_closed(&self) {
        wait_until("every member's election to close", &self.faults, || {
            self.counters
                .iter()
                .all(|node| node.election_state() == Some(ELECTION_CLOSED))
        });
    }

    fn leader(&self) -> i32 {
        leader_of(&self.counters).expect("a leader")
    }

    fn leader_counters(&self) -> &NodeCounters {
        &self.counters[usize::try_from(self.leader()).unwrap()]
    }

    fn followers(&self) -> usize {
        self.counters
            .iter()
            .filter(|node| node.role() != Some(ROLE_LEADER))
            .count()
    }

    fn stop_all(&mut self) {
        for node in &mut self.nodes {
            node.stop();
        }
    }

    /// `TestCluster.restartAllNodes(false)`: every member back on its own
    /// directories — the same archive, the same cluster log, the same driver.
    fn restart_all(&mut self) {
        for node in &mut self.nodes {
            node.restart(&self.launched_tx);
        }
        await_launched(&self.faults, &self.launched_rx, MEMBER_COUNT);
        self.open_counters();
        self.await_closed();
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

/// **Every member restarts on the snapshot it took** —
/// `ClusterTest.shouldStopLeaderAndFollowersThenRestartAllWithSnapshot`
/// (`ClusterTest.java:254`).
///
/// The reference takes a snapshot, stops all three, restarts all three on their
/// own directories, and waits for `awaitSnapshotsLoaded`. The three messages
/// before the snapshot are this build's addition: without them the loaded
/// business would have nothing to restore and "loaded" would be the only
/// reading, not "loaded **the right state**".
#[test]
fn every_member_restarts_on_the_snapshot_it_took() {
    let _serial = serial();
    let ports = CasePorts::new(0);
    let Some(mut cluster) = Cluster::start(*ports.ports(), &[]) else {
        return;
    };

    let leader = cluster.leader();
    let Some(mut client) = ClientSide::start(ports.ports(), leader) else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);
    client.send_and_await_messages(&session, 3, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 3);
    }

    take_snapshot(cluster.leader_counters());
    await_every_snapshot_count(&cluster.counters, &cluster.faults, 1);
    for node in &cluster.nodes {
        assert!(
            node.service_state()
                .expect("the business")
                .was_snapshot_taken(),
            "every member's business took the snapshot"
        );
    }

    drop(client);
    cluster.stop_all();
    cluster.restart_all();

    assert_eq!(2, cluster.followers(), "the two followers came back");
    assert_eq!(
        Some(ROLE_LEADER),
        cluster.counters[usize::try_from(cluster.leader()).unwrap()].role()
    );
    await_snapshots_loaded(&cluster.nodes, &cluster.faults);
    for id in 0..MEMBER_COUNT {
        // The business restores its count from its own snapshot
        // (`TestNode.java:546-576`); the log delivers nothing after the
        // snapshot position.
        await_messages(&cluster.nodes[id as usize], &cluster.faults, 3);
    }

    cluster.stop_all();
}

/// **The control for the case above**: the same restart with **no** snapshot —
/// nothing was taken, so nothing is loaded — and the log is what carries the
/// three messages back.
///
/// This is what makes `was_snapshot_loaded` a discriminating instrument: the
/// message count is three either way, so a case that only counted messages
/// would pass whether or not a snapshot existed.
#[test]
fn a_restart_without_a_snapshot_loads_nothing() {
    let _serial = serial();
    let ports = CasePorts::new(1);
    let Some(mut cluster) = Cluster::start(*ports.ports(), &[]) else {
        return;
    };

    let leader = cluster.leader();
    let Some(mut client) = ClientSide::start(ports.ports(), leader) else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);
    client.send_and_await_messages(&session, 3, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 3);
    }
    // No `take_snapshot`.
    assert_eq!(
        Some(0),
        cluster.leader_counters().snapshot_count(),
        "no snapshot was taken"
    );

    drop(client);
    cluster.stop_all();
    cluster.restart_all();

    for node in &cluster.nodes {
        assert!(
            !node
                .service_state()
                .expect("the business")
                .was_snapshot_loaded(),
            "a member with no snapshot to load must not claim one"
        );
    }
    for id in 0..MEMBER_COUNT {
        await_messages(&cluster.nodes[id as usize], &cluster.faults, 3);
    }

    cluster.stop_all();
}

/// **An orderly shutdown snapshots every member, and the restart comes back on
/// those snapshots** — `ClusterTest.shouldShutdownClusterAndRestartWithSnapshots`
/// (`ClusterTest.java:338`).
///
/// The first half (a `SHUTDOWN` snapshots and then ends the duty cycle) is
/// `cluster_control::shutdown_snapshots_and_then_ends_the_duty_cycle` for one
/// in-process member; this is the cluster half the row asks for: three members,
/// the restart, and the businesses loaded from what the shutdown wrote.
///
/// The negative control is the reference's other shutdown: an `ABORT` takes no
/// snapshot, which our own `cluster_control::abort_ends_the_duty_cycle_without_a_snapshot`
/// asserts.
#[test]
fn a_graceful_shutdown_snapshots_every_member_and_the_cluster_comes_back() {
    let _serial = serial();
    let ports = CasePorts::new(2);
    let Some(mut cluster) = Cluster::start(*ports.ports(), &[]) else {
        return;
    };

    let leader = cluster.leader();
    let Some(mut client) = ClientSide::start(ports.ports(), leader) else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);
    client.send_and_await_messages(&session, 3, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 3);
    }
    drop(client);

    // `TestCluster.shutdownCluster` (`TestCluster.java:1514`): the orderly toggle, which
    // snapshots first (`ConsensusModuleAgent.java:2585-2607`).
    cluster.leader_counters().set(
        CLUSTER_CONTROL_TOGGLE_TYPE_ID,
        i64::from(ToggleState::Shutdown.code()),
    );
    wait_until(
        "every member to end its duty cycle",
        &cluster.faults,
        || cluster.nodes.iter().all(Node::has_finished),
    );
    for node in &cluster.nodes {
        assert!(
            node.service_state()
                .expect("the business")
                .was_snapshot_taken(),
            "a clean shutdown snapshots every member's business"
        );
    }
    // No counter read here, deliberately: the module's counters are released
    // when the duty cycle closes (`Node::stop` rejoins it), so a snapshot
    // count read after the termination says nothing. The reference asserts the
    // business's flag after the shutdown and reads the *loaded* flag after the
    // restart, which is what this case does.

    cluster.stop_all();
    cluster.restart_all();

    assert_eq!(2, cluster.followers(), "the two followers came back");
    // The reference compares the followers to the leader
    // (`ClusterTest.java:358-364`), not to the term before the shutdown: a
    // whole-cluster restart is free to close a new term, and this one does
    // (0 -> 1).
    let new_leader = cluster.leader();
    let term_after = cluster.counters[usize::try_from(new_leader).unwrap()]
        .leadership_term_id()
        .expect("the leader's term");
    for node in &cluster.counters {
        assert_eq!(
            Some(term_after),
            node.leadership_term_id(),
            "every member closes in the leader's term"
        );
    }
    await_snapshots_loaded(&cluster.nodes, &cluster.faults);
    for id in 0..MEMBER_COUNT {
        await_messages(&cluster.nodes[id as usize], &cluster.faults, 3);
    }

    cluster.stop_all();
}

/// **The appointed leader serves again after every member restarts** —
/// `MultiNodeTest.shouldReplayWithAppointedLeaderWithThreeNodesWithNoSnapshot`
/// (`MultiNodeTest.java:63`).
///
/// The reference asserts the appointment before the outage and only "a leader"
/// after it; this case keeps asserting member 1, because otherwise the row
/// would pass for a cluster that had simply forgotten the appointment.
#[test]
fn an_appointed_leader_serves_again_after_every_member_restarts() {
    let _serial = serial();
    let ports = CasePorts::new(3);
    let appointed = 1;
    let argument = format!("-Daeron.cluster.appointed.leader.id={appointed}");
    let Some(mut cluster) = Cluster::start(*ports.ports(), &[argument.as_str()]) else {
        return;
    };

    assert_eq!(appointed, cluster.leader(), "the appointed member leads");
    assert_eq!(
        Some(ROLE_LEADER),
        cluster.counters[usize::try_from(appointed).unwrap()].role()
    );

    let Some(mut client) = ClientSide::start(ports.ports(), appointed) else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);
    client.send_and_await_messages(&session, 10, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 10);
    }
    drop(client);

    cluster.stop_all();
    cluster.restart_all();

    assert_eq!(
        appointed,
        cluster.leader(),
        "the appointed member leads again"
    );
    for id in 0..MEMBER_COUNT {
        await_messages(&cluster.nodes[id as usize], &cluster.faults, 10);
    }

    cluster.stop_all();
}
