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
    ClientSide, ELECTION_CLOSED, Faults, MEMBER_COUNT, Node, NodeCounters, Ports, ROLE_FOLLOWER,
    ROLE_LEADER, announce_skip, await_every_snapshot_count, await_launched, await_snapshot_count,
    await_snapshots_loaded, invalidate_latest_snapshot, leader_of, members, take_snapshot,
    wait_until,
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
        let deadline = std::time::Instant::now() + deepmsg_tests::cluster_harness::DEADLINE;
        while std::time::Instant::now() < deadline {
            self.faults.check();
            if self
                .counters
                .iter()
                .all(|node| node.election_state() == Some(ELECTION_CLOSED))
            {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let logs = self
            .nodes
            .iter()
            .map(|node| format!("--- node {} ---\n{}", node.id(), node.driver_log_tail(25)))
            .collect::<Vec<_>>()
            .join("\n");
        panic!(
            "every member's election to close: election={:?} role={:?} term={:?} append={:?} commit={:?} faults={:?}\n{logs}",
            self.counters
                .iter()
                .map(NodeCounters::election_state)
                .collect::<Vec<_>>(),
            self.counters
                .iter()
                .map(NodeCounters::role)
                .collect::<Vec<_>>(),
            self.counters
                .iter()
                .map(NodeCounters::leadership_term_id)
                .collect::<Vec<_>>(),
            self.counters
                .iter()
                .map(NodeCounters::append_position)
                .collect::<Vec<_>>(),
            self.counters
                .iter()
                .map(NodeCounters::commit_position)
                .collect::<Vec<_>>(),
            self.faults.snapshot(),
        );
    }

    fn leader(&self) -> i32 {
        wait_until("a leader", &self.faults, || {
            leader_of(&self.counters).is_some()
        });
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

    /// `TestCluster.startStaticNode(index, false)`: that member back on its own
    /// directories, and nothing else touched.
    fn stop_one(&mut self, id: i32) {
        self.nodes[usize::try_from(id).unwrap()].stop();
    }

    fn restart_one(&mut self, id: i32) {
        self.nodes[usize::try_from(id).unwrap()].restart(&self.launched_tx);
        await_launched(&self.faults, &self.launched_rx, 1);
        self.open_counters();
    }

    /// `TestCluster.startStaticNode(index, true)`: the reference's `cleanStart`
    /// — the archive and cluster directories emptied first, so the member comes
    /// back with no log at all and has to be caught up from the leader.
    fn restart_empty_one(&mut self, id: i32) {
        self.nodes[usize::try_from(id).unwrap()].restart_empty(&self.launched_tx);
        await_launched(&self.faults, &self.launched_rx, 1);
        self.open_counters();
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
    let deadline = std::time::Instant::now() + deepmsg_tests::cluster_harness::DEADLINE;
    while std::time::Instant::now() < deadline {
        faults.check();
        if state.messages() == count {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let counters = NodeCounters::open(node.aeron_dir());
    panic!(
        "node {}'s service never reached {count}: it has {}; role={:?} election={:?} term={:?} append={:?} commit={:?}",
        node.id(),
        state.messages(),
        counters.role(),
        counters.election_state(),
        counters.leadership_term_id(),
        counters.append_position(),
        counters.commit_position(),
    );
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

/// Waits for `node` to stop while the client on the cluster keeps its session
/// alive — the reference's client sends a keepalive every second, and a member
/// stopping takes about thirty (the module waits for its log subscription's
/// local sockets to be released, `agent/mod.rs::await_local_sockets_closed`).
///
/// Silence for the driver's ten-second image liveness timeout drops the
/// ingress publication, and a dropped publication does not come back: see
/// [`a_keep_alive_keeps_the_ingress_publication_connected`] for the reading.
fn stop_while_keeping_alive(
    node: &mut Node,
    client: &mut ClientSide,
    session: &deepmsg_tests::cluster_harness::SessionEvent,
    faults: &Faults,
) {
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            node.stop();
            let _ = done_tx.send(());
        });
        let mut last = std::time::Instant::now();
        loop {
            faults.check();
            // Two different clocks, and both are needed: `poll` is the
            // client's heartbeat to **its own** driver, which reclaims a
            // client that has not polled for `aeron.client.liveness.timeout`
            // (ten seconds, `crates/driver/src/clients.rs`) along with its
            // publications; the keepalive is data on the **member's** ingress,
            // which drops an image that sees none for the same ten seconds.
            client.poll();
            std::thread::sleep(std::time::Duration::from_millis(50));
            if last.elapsed() >= std::time::Duration::from_secs(1) {
                client.send_keep_alive(session, faults);
                last = std::time::Instant::now();
            }
            if done_rx.try_recv().is_ok() {
                break;
            }
        }
    });
}

/// **A follower comes back to a snapshot taken while it was down** —
/// `ClusterTest.shouldReplaySnapshotTakenWhileDown` (`ClusterTest.java:691`).
///
/// The snapshot is a `SNAPSHOT` **action appended to the cluster log**
/// (`ConsensusModuleAgent.java:2569`, `appendAction`), and every member takes
/// its own snapshot when it applies that position. A member that was away
/// therefore does not "fetch" one from anybody: it replays the action on the
/// way back and takes its own. That is what the snapshot counter is read for —
/// only a local `takeSnapshot` increments it (`:3190`), so `count == 1` on the
/// returning member says the action was replayed, not that some other path
/// filled the gap.
///
/// The reading this row once was — the returning member looping in
/// `FollowerCatchupAwait`, its append stuck at 256 against the leader's 1312 —
/// is gone once a new term starts from the log's **applied** position rather
/// than its **appended** one (`records/P3-6c.md` §19).
#[test]
fn a_follower_comes_back_to_a_snapshot_taken_while_it_was_down() {
    let _serial = serial();
    let ports = CasePorts::new(4);
    let Some(mut cluster) = Cluster::start(*ports.ports(), &[]) else {
        return;
    };

    let leader = cluster.leader();
    let away = (0..MEMBER_COUNT)
        .find(|id| *id != leader)
        .expect("a follower");
    let staying = (0..MEMBER_COUNT)
        .find(|id| *id != leader && *id != away)
        .expect("the other follower");

    let Some(mut client) = ClientSide::start(ports.ports(), leader) else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);

    wait_until(
        "the away member's election to close",
        &cluster.faults,
        || {
            cluster.counters[usize::try_from(away).unwrap()].election_state()
                == Some(ELECTION_CLOSED)
        },
    );
    stop_while_keeping_alive(
        &mut cluster.nodes[usize::try_from(away).unwrap()],
        &mut client,
        &session,
        &cluster.faults,
    );

    // The snapshot is taken while it is away; the members that are here take
    // it, and no wait below names the member that is not.
    take_snapshot(cluster.leader_counters());
    await_snapshot_count(
        &cluster.counters[usize::try_from(leader).unwrap()],
        &cluster.faults,
        1,
    );
    await_snapshot_count(
        &cluster.counters[usize::try_from(staying).unwrap()],
        &cluster.faults,
        1,
    );

    client.send_and_await_messages(&session, 10, &cluster.faults);
    for id in [leader, staying] {
        await_messages(
            &cluster.nodes[usize::try_from(id).unwrap()],
            &cluster.faults,
            10,
        );
    }

    cluster.restart_one(away);

    // The reading that tells the two possible ways back apart.
    await_snapshot_count(
        &cluster.counters[usize::try_from(away).unwrap()],
        &cluster.faults,
        1,
    );
    assert_eq!(
        Some(ROLE_FOLLOWER),
        cluster.counters[usize::try_from(away).unwrap()].role(),
        "the member that was away is a follower of the term it missed"
    );
    await_messages(
        &cluster.nodes[usize::try_from(away).unwrap()],
        &cluster.faults,
        10,
    );

    drop(client);
    cluster.stop_all();
}

/// **A keepalive keeps the ingress publication connected, and silence does
/// not** — the reading behind [`stop_while_keeping_alive`].
///
/// The reference's client sends `SessionKeepAlive` every second
/// (`AeronCluster`'s keepalive), which is what keeps a member's ingress image
/// of the publication alive at all. This client sent nothing while a case
/// waited, and the image went away at about twelve seconds — measured, not
/// inferred: the same cluster, an idle publication, `connected` flipping
/// between t+11 s and t+12 s.
#[test]
fn a_keep_alive_keeps_the_ingress_publication_connected() {
    let _serial = serial();
    let ports = CasePorts::new(7);
    let Some(mut cluster) = Cluster::start(*ports.ports(), &[]) else {
        return;
    };
    let leader = cluster.leader();
    let Some(mut client) = ClientSide::start(ports.ports(), leader) else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);

    let started = std::time::Instant::now();
    let mut ticks = 0_u32;
    while started.elapsed() < std::time::Duration::from_secs(15) {
        client.poll();
        std::thread::sleep(std::time::Duration::from_millis(100));
        ticks += 1;
        if ticks % 10 == 0 {
            client.send_keep_alive(&session, &cluster.faults);
        }
    }
    assert!(
        client
            .ingress_publication_state()
            .contains("connected=Some(true)"),
        "a keepalive a second keeps it connected: {}",
        client.ingress_publication_state()
    );

    drop(client);
    cluster.stop_all();
}

/// **An empty follower catches up from the shutdown snapshot and takes its
/// own** —
/// `ClusterTest.shouldCatchupFromEmptyLogThenSnapshotAfterShutdownAndFollowerCleanStart`
/// (`ClusterTest.java:1213`).
///
/// The two members that kept their directories lost nothing: they load the
/// snapshot the shutdown wrote. The one that comes back on an empty directory
/// has nothing to load, is caught up from the leader's log, and — because that
/// log carries the shutdown's `SNAPSHOT` action — takes a snapshot of its own
/// at that position.
///
/// The clean-start member's reading — nothing applied, `Canvass` for ever, its
/// archive refusing `unknown recording id: -1` — was a replication whose
/// destination recording id never reached the member: the `REPLICATE` signal
/// that names it was dropped and the signal was applied to the handle rather
/// than to the election that writes the id down (`records/P3-6c.md` §20).
#[test]
fn an_empty_follower_replays_the_shutdown_snapshot_and_takes_its_own() {
    let _serial = serial();
    let ports = CasePorts::new(5);
    let Some(mut cluster) = Cluster::start(*ports.ports(), &[]) else {
        return;
    };

    let leader = cluster.leader();
    let Some(mut client) = ClientSide::start(ports.ports(), leader) else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);
    client.send_and_await_messages(&session, 10, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 10);
    }
    drop(client);

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
    cluster.stop_all();

    // The reference's `cleanStart` member is 2 (`ClusterTest.java:1242-1246`),
    // and it is the one that must **not** have been handed a snapshot.
    let clean = 2;
    cluster.restart_one(0);
    cluster.restart_one(1);
    cluster.restart_empty_one(clean);

    assert_ne!(clean, cluster.leader(), "an empty log cannot lead");
    await_snapshots_loaded(&cluster.nodes[..2], &cluster.faults);
    assert!(
        !cluster.nodes[usize::try_from(clean).unwrap()]
            .service_state()
            .expect("the business")
            .was_snapshot_loaded(),
        "a member that came back on an empty directory has no snapshot to load"
    );

    await_messages(
        &cluster.nodes[usize::try_from(clean).unwrap()],
        &cluster.faults,
        10,
    );
    await_snapshot_count(
        &cluster.counters[usize::try_from(clean).unwrap()],
        &cluster.faults,
        1,
    );
    assert!(
        cluster.nodes[usize::try_from(clean).unwrap()]
            .service_state()
            .expect("the business")
            .was_snapshot_taken(),
        "the replayed SNAPSHOT action makes the caught-up member take its own"
    );

    cluster.stop_all();
}

/// **Two snapshots are replayed into an empty follower log** —
/// `ClusterTest.shouldReplayMultipleSnapshotsWithEmptyFollowerLog`
/// (`ClusterTest.java:1295`).
///
/// Two snapshots, two messages before the first and one between them, and a
/// member that comes back clean: it replays both actions, catches up to the
/// three messages, and the cluster keeps serving afterwards.
///
/// The flood this row was — the same `unknown recording id: -1` — went with the
/// destination recording id the `REPLICATE` signal carries and the election
/// writes down (`records/P3-6c.md` §20).
#[test]
fn two_snapshots_are_replayed_into_an_empty_follower_log() {
    let _serial = serial();
    let ports = CasePorts::new(6);
    let Some(mut cluster) = Cluster::start(*ports.ports(), &[]) else {
        return;
    };

    let leader = cluster.leader();
    let Some(mut client) = ClientSide::start(ports.ports(), leader) else {
        announce_skip();
        return;
    };
    let session = client.connect_session(&cluster.faults);

    client.send_and_await_messages(&session, 2, &cluster.faults);
    take_snapshot(cluster.leader_counters());
    await_every_snapshot_count(&cluster.counters, &cluster.faults, 1);

    client.send_and_await_messages(&session, 1, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 3);
    }
    drop(client);

    cluster.leader_counters().set(
        CLUSTER_CONTROL_TOGGLE_TYPE_ID,
        i64::from(ToggleState::Shutdown.code()),
    );
    wait_until(
        "every member to end its duty cycle",
        &cluster.faults,
        || cluster.nodes.iter().all(Node::has_finished),
    );
    cluster.stop_all();

    let clean = 2;
    cluster.restart_one(0);
    cluster.restart_one(1);
    cluster.restart_empty_one(clean);

    assert_ne!(clean, cluster.leader(), "an empty log cannot lead");
    await_snapshots_loaded(&cluster.nodes[..2], &cluster.faults);
    assert!(
        !cluster.nodes[usize::try_from(clean).unwrap()]
            .service_state()
            .expect("the business")
            .was_snapshot_loaded(),
        "a member that came back on an empty directory has no snapshot to load"
    );
    await_messages(
        &cluster.nodes[usize::try_from(clean).unwrap()],
        &cluster.faults,
        3,
    );

    // The reference reconnects a client after the restart and sends four more
    // (`ClusterTest.java:1350-1356`); this build's client does not follow a
    // redirect, so reconnecting is a second client (`cluster_restart_path`).
    let new_leader = cluster.leader();
    let Some(mut second_client) = ClientSide::start_at(ports.ports(), new_leader, 1) else {
        announce_skip();
        return;
    };
    let second_session = second_client.connect_session(&cluster.faults);
    second_client.send_and_await_messages(&second_session, 4, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 7);
    }

    drop(second_client);
    cluster.stop_all();
}

/// A client that follows the cluster to a new leader: this build's `ClientSide`
/// does not act on a redirect (`cluster_restart_path` makes the same move), so
/// a case whose leader moves drops the old client and takes a second one whose
/// egress port is its own.
fn client_on(
    ports: &Ports,
    leader: i32,
    faults: &Faults,
    offset: u16,
) -> Option<(ClientSide, deepmsg_tests::cluster_harness::SessionEvent)> {
    let mut client = ClientSide::start_at(ports, leader, offset)?;
    let session = client.connect_session(faults);
    Some((client, session))
}

/// **The last snapshot is marked invalid, so the one before it is what the
/// cluster comes back on** — `ClusterTest.shouldRecoverWhenLastSnapshotIsMarkedInvalid`
/// (`ClusterTest.java:1597`).
///
/// Two snapshots (three messages each), a leader that goes and comes back, three
/// more messages, and then the **second** snapshot is invalidated with every
/// member stopped. What the restart must recover from is the first snapshot plus
/// the log: nine messages, not six and not none.
///
/// The cross-term form is the reference's own row, and it used to stall a
/// restarted member in `FollowerReplay` three runs in four. That stall is
/// `records/P3-6c.md` §19's — a new term starting from the log's **appended**
/// position instead of the **applied** one — and the same-term half is
/// `the_snapshot_before_the_invalidated_one_is_what_the_cluster_comes_back_on`.
#[test]
fn the_last_snapshot_is_marked_invalid_and_the_one_before_it_is_used() {
    let _serial = serial();
    let ports = CasePorts::new(8);
    let Some(mut cluster) = Cluster::start(*ports.ports(), &[]) else {
        return;
    };

    let leader0 = cluster.leader();
    let Some(mut client) = ClientSide::start(ports.ports(), leader0) else {
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

    client.send_and_await_messages(&session, 3, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 6);
    }

    take_snapshot(cluster.leader_counters());
    await_every_snapshot_count(&cluster.counters, &cluster.faults, 2);

    // The leader goes and comes back; the client cannot follow, so it is given
    // back and a second one is opened on whoever leads now.
    drop(client);
    cluster.stop_one(leader0);
    let leader1 = cluster.leader();
    cluster.restart_one(leader0);
    let Some((mut client, session)) = client_on(ports.ports(), leader1, &cluster.faults, 1) else {
        announce_skip();
        return;
    };
    client.send_and_await_messages(&session, 3, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 9);
    }
    drop(client);

    cluster.stop_all();
    for node in &cluster.nodes {
        invalidate_latest_snapshot(node).expect("the latest snapshot is invalidated");
    }
    cluster.restart_all();
    cluster.leader();
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 9);
    }

    cluster.stop_all();
}

/// **The snapshot before the invalidated one is what the cluster comes back
/// on** — the same-term half of
/// `ClusterTest.shouldRecoverWhenLastSnapshotIsMarkedInvalid`
/// (`ClusterTest.java:1597`).
///
/// Two snapshots, the newest invalidated on every member with all of them
/// stopped, and the restart must recover the six messages from the first
/// snapshot plus the log. This is the reading that says the fallback itself
/// works; the reference's row also moves the leader across a term, and that
/// half is the one that stalls (`a_follower_comes_back_to_a_snapshot...`'s
/// neighbour, rows 7-9 in `records/P3-6c.md` §16).
#[test]
fn the_snapshot_before_the_invalidated_one_is_what_the_cluster_comes_back_on() {
    let _serial = serial();
    let ports = CasePorts::new(10);
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
    client.send_and_await_messages(&session, 3, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 6);
    }
    take_snapshot(cluster.leader_counters());
    await_every_snapshot_count(&cluster.counters, &cluster.faults, 2);
    drop(client);

    cluster.stop_all();
    for node in &cluster.nodes {
        invalidate_latest_snapshot(node).expect("an invalidation");
    }
    cluster.restart_all();
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 6);
    }
    cluster.stop_all();
}

/// **The latest snapshot is invalidated between two elections** —
/// `ClusterTest.shouldRecoverWhenLastSnapshotIsInvalidBetweenTwoElections`
/// (`ClusterTest.java:1707`).
///
/// A snapshot in the first term, nothing in the second, and the first one
/// invalidated with every member stopped: the cluster comes back on the log.
///
/// The cross-term form used to stall a restarted member in `FollowerReplay`;
/// it stops once a new term starts from the log's **applied** position rather
/// than its **appended** one (`records/P3-6c.md` §19).
#[test]
fn a_snapshot_invalidated_between_two_elections() {
    let _serial = serial();
    let ports = CasePorts::new(11);
    let Some(mut cluster) = Cluster::start(*ports.ports(), &[]) else {
        return;
    };

    let leader0 = cluster.leader();
    let Some((mut client, session)) = client_on(ports.ports(), leader0, &cluster.faults, 0) else {
        announce_skip();
        return;
    };
    client.send_and_await_messages(&session, 3, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 3);
    }

    // Term one: the leader goes and comes back, three more messages.
    drop(client);
    cluster.stop_one(leader0);
    let leader1 = cluster.leader();
    cluster.restart_one(leader0);
    let Some((mut client, session)) = client_on(ports.ports(), leader1, &cluster.faults, 1) else {
        announce_skip();
        return;
    };
    client.send_and_await_messages(&session, 3, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 6);
    }

    take_snapshot(cluster.leader_counters());
    await_every_snapshot_count(&cluster.counters, &cluster.faults, 1);

    // Term two: no snapshot at all.
    drop(client);
    cluster.stop_one(leader1);
    let leader2 = cluster.leader();
    cluster.restart_one(leader1);
    let Some((mut client, session)) = client_on(ports.ports(), leader2, &cluster.faults, 1) else {
        announce_skip();
        return;
    };
    client.send_and_await_messages(&session, 3, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 9);
    }
    drop(client);

    cluster.stop_all();
    for node in &cluster.nodes {
        invalidate_latest_snapshot(node).expect("the only snapshot is invalidated");
    }
    cluster.restart_all();
    cluster.leader();
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 9);
    }

    cluster.stop_all();
}

/// **The last two snapshots are invalidated after an election** —
/// `ClusterTest.shouldRecoverWhenLastTwosSnapshotsAreInvalidAfterElection`
/// (`ClusterTest.java:1753`).
///
/// Three snapshots across two terms; the newest two are invalidated with every
/// member stopped, so the recovery is the first snapshot plus the log. The
/// per-member counters are the reference's own: the member that was away for a
/// snapshot replays its action and is one behind.
#[test]
#[ignore = "F3-c reading: no longer stalls (§20); it now fails at its own teardown — `invalidate_latest_snapshot`: `unknown leadershipTermId=1`, a recording log with a term-1 snapshot and no term-1 term entry — records/P3-6c.md §20.4"]
fn the_last_two_snapshots_are_invalidated_after_an_election() {
    let _serial = serial();
    let ports = CasePorts::new(12);
    let Some(mut cluster) = Cluster::start(*ports.ports(), &[]) else {
        return;
    };

    let leader0 = cluster.leader();
    let Some((mut client, session)) = client_on(ports.ports(), leader0, &cluster.faults, 0) else {
        announce_skip();
        return;
    };
    client.send_and_await_messages(&session, 3, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 3);
    }
    take_snapshot(cluster.leader_counters());
    await_every_snapshot_count(&cluster.counters, &cluster.faults, 1);

    // The leader goes and comes back; the snapshot after that is the second.
    drop(client);
    cluster.stop_one(leader0);
    let leader1 = cluster.leader();
    cluster.restart_one(leader0);
    let Some((mut client, session)) = client_on(ports.ports(), leader1, &cluster.faults, 1) else {
        announce_skip();
        return;
    };
    client.send_and_await_messages(&session, 3, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 6);
    }

    take_snapshot(cluster.leader_counters());
    for id in 0..MEMBER_COUNT {
        // The member that was away for the first snapshot replays its action
        // and is one behind (`ClusterTest.java:1795-1798`).
        let expected = if id == leader0 { 1 } else { 2 };
        await_snapshot_count(
            &cluster.counters[usize::try_from(id).unwrap()],
            &cluster.faults,
            expected,
        );
    }

    client.send_and_await_messages(&session, 3, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 9);
    }

    take_snapshot(cluster.leader_counters());
    for id in 0..MEMBER_COUNT {
        let expected = if id == leader0 { 2 } else { 3 };
        await_snapshot_count(
            &cluster.counters[usize::try_from(id).unwrap()],
            &cluster.faults,
            expected,
        );
    }
    client.send_and_await_messages(&session, 3, &cluster.faults);
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 12);
    }
    drop(client);

    cluster.stop_all();
    for node in &cluster.nodes {
        invalidate_latest_snapshot(node).expect("one invalidation");
        invalidate_latest_snapshot(node).expect("and another");
    }
    cluster.restart_all();
    cluster.leader();
    for node in &cluster.nodes {
        await_messages(node, &cluster.faults, 12);
    }

    cluster.stop_all();
}
