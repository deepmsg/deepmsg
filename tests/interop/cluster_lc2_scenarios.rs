//! **LC-2**: the reference's own client against **our** three-node cluster.
//!
//! Every criterion written before this one has a JVM on one side of it. LC-4a
//! is the reference's consensus module driving our container; LC-1 is our client
//! on a Java cluster; LC-4b our module on a Java container. LC-2 is the pairing
//! the card's DoD is written against: **all members ours**, and the client the
//! reference's own `AeronCluster` — so what the case asserts is a reference
//! implementation's view of our cluster, which no amount of our client agreeing
//! with our module can stand in for.
//!
//! # Why a process
//!
//! The Java client is a `java` invocation of the fork's
//! `ClusterEchoClientProbe.java` (the probe LC-4b already uses), extended with
//! the three arguments this direction needs: the cluster's ingress channel (no
//! endpoint), the `id=endpoint` list its members advertise, and an egress
//! channel to be answered on. With none of them it is the LC-4b probe, IPC
//! against a node in the same driver.
//!
//! It gets a driver of its own (`OwnDriver`), because a client of a cluster in
//! another process cannot reach it any other way — which is also what makes the
//! case a statement about the wire rather than about a shared directory.
//!
//! # This file
//!
//! The election scenarios of P3-6c's F2 (`AppointedLeaderTest` and the
//! appointed-leader arm of `MultiNodeTest`), on top of the direction's smoke.
//! See `analysis/cluster/deepmsg-p3-6c-plan-lc2-scenarios.md`.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use deepmsg_cluster::config::CONSENSUS_STREAM_ID_DEFAULT as CONSENSUS_STREAM;
use deepmsg_tests::aeron_tests;
use deepmsg_tests::cluster_harness::{
    ELECTION_CLOSED, Faults, MEMBER_COUNT, Node, NodeCounters, Ports, ROLE_LEADER, announce_skip,
    await_launched, members, wait_until,
};
use deepmsg_tests::driver::OwnDriver;
use deepmsg_tests::java;
use deepmsg_tests::temp::TempDir;

/// The scenarios here bind ports derived from the **process** id
/// (`cluster_harness::Ports`), so `cargo test`'s parallel threads would race
/// for them. They take turns.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A three-node cluster of this build, each member running the echo business,
/// and its counters — or `None` when the fixture cannot stand one up on this
/// machine.
fn start_cluster(
    ports: &Ports,
    module_arguments: &[&str],
) -> Option<(Vec<Node>, Vec<NodeCounters>, Faults)> {
    start_cluster_with(ports, module_arguments, &|_| Vec::new())
}

/// [`start_cluster`], with this member's **driver** settings — where the
/// network knobs live (`debug.*.data.loss.*`, `crates/driver/src/config.rs`).
fn start_cluster_with(
    ports: &Ports,
    module_arguments: &[&str],
    driver_properties: &dyn Fn(i32) -> Vec<String>,
) -> Option<(Vec<Node>, Vec<NodeCounters>, Faults)> {
    let faults = Faults::default();
    let (launched_tx, launched_rx) = mpsc::channel();
    let member_string = members(ports);
    let mut nodes = Vec::new();

    for id in 0..MEMBER_COUNT {
        let properties = driver_properties(id);
        let property_refs: Vec<&str> = properties.iter().map(String::as_str).collect();
        let Some(node) = Node::start_with_properties(
            id,
            ports,
            &member_string,
            &faults,
            &launched_tx,
            Some("test-service"),
            &property_refs,
            module_arguments,
        ) else {
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

    Some((nodes, counters, faults))
}

/// The leader the counters name, once there is exactly one.
fn the_leader(counters: &[NodeCounters]) -> Option<i32> {
    let leaders: Vec<i32> = (0..MEMBER_COUNT)
        .filter(|id| counters[*id as usize].role() == Some(ROLE_LEADER))
        .collect();

    match leaders.as_slice() {
        [leader] => Some(*leader),
        _ => None,
    }
}

/// What the reference's client reads back after offering `count` messages.
///
/// One `java` invocation on a driver of its own: the probe is the fork's, the
/// three channel arguments are this direction's (see the module note).
fn java_client_echoes(ports: &Ports, count: u32) -> String {
    java_client_echoes_at(ports, count, 0)
}

/// The same, with `pause_millis` between messages — how a case keeps the
/// client running while the cluster is made to lose a member.
fn java_client_echoes_at(ports: &Ports, count: u32, pause_millis: u32) -> String {
    let Some(source) = aeron_tests::java("ClusterEchoClientProbe.java") else {
        announce_skip();
        return String::new();
    };
    let classes = TempDir::new("cluster-lc2-probe-classes");
    let Some(()) = java::compile_probe(&source, classes.path()) else {
        announce_skip();
        return String::new();
    };
    let Some(jar) = deepmsg_tests::driver::locate_aeron_all() else {
        announce_skip();
        return String::new();
    };
    let Some(client_driver) = OwnDriver::start(&format!("cluster-lc2-client-{}", ports.base()))
    else {
        announce_skip();
        return String::new();
    };

    let aeron_dir = client_driver.aeron_dir().display().to_string();
    let ingress = ports.client_ingress_channel();
    let endpoints = ports.client_ingress_endpoints();
    let egress = ports.client_egress_wildcard_channel();
    let count = count.to_string();
    let timeout = (30 + count.parse::<u32>().unwrap_or(0) * pause_millis / 1000).to_string();
    let pause = pause_millis.to_string();

    java::run_probe(
        &jar,
        classes.path(),
        "ClusterEchoClientProbe",
        &[
            aeron_dir.as_str(),
            &count,
            &timeout,
            &count,
            &ingress,
            &endpoints,
            &egress,
            &pause,
        ],
    )
}

/// Wait for exactly one leader and answer it, or panic with what the counters
/// said.
fn await_leader(counters: &[NodeCounters], faults: &Faults) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(60);

    while Instant::now() < deadline {
        faults.check();
        if let Some(leader) = the_leader(counters) {
            return leader;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    panic!(
        "no single leader; roles {:?}",
        (0..MEMBER_COUNT)
            .map(|id| counters[id as usize].role())
            .collect::<Vec<_>>()
    );
}

/// **A Java client echoes five messages through a cluster of our members.**
#[test]
fn a_java_client_echoes_through_our_three_node_cluster() {
    let _serial = serial();
    let ports = Ports::new();
    let Some((mut nodes, counters, faults)) = start_cluster(&ports, &[]) else {
        return;
    };

    await_leader(&counters, &faults);

    let reading = java_client_echoes(&ports, 5);
    faults.check();
    assert!(
        reading.contains("RECEIVED 5"),
        "the reference's client read five echoes back through our cluster: {reading}"
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **The cluster elects the member it was told to** (`MultiNodeTest
/// .shouldElectAppointedLeaderWithThreeNodesWithNoReplayNoSnapshot`,
/// `AppointedLeaderTest.shouldEchoMessagesViaService`): every member is started
/// with `aeron.cluster.appointed.leader.id`, and the cluster must go to that
/// member rather than to whichever ballot wins, with the reference's client
/// echoing through it.
#[test]
fn an_appointed_leader_is_the_one_that_serves() {
    let _serial = serial();
    let ports = Ports::new();
    let appointed = 1;
    let argument = format!("-Daeron.cluster.appointed.leader.id={appointed}");
    let Some((mut nodes, counters, faults)) = start_cluster(&ports, &[argument.as_str()]) else {
        return;
    };

    let leader = await_leader(&counters, &faults);
    assert_eq!(
        appointed, leader,
        "the appointed member leads, not whichever election won"
    );

    let reading = java_client_echoes(&ports, 3);
    faults.check();
    assert!(
        reading.contains("RECEIVED 3"),
        "the reference's client echoes through the appointed leader: {reading}"
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **A consensus partition heals, the cluster elects again, and the
/// reference's client offers on the far side of it**
/// (`ClusterTest.shouldFormClusterAfterFullPartition`, `ClusterTest.java:3502-3559`).
///
/// Every member stops hearing every other member's **consensus** traffic for a
/// window (`debug.receive.data.loss.*` on stream 108, heartbeats included,
/// `crates/driver/src/config.rs`), so no member can append an opinion and the
/// leader steps down on its heartbeat timeout. When the window closes the
/// cluster has to elect again, and the reference's client — one client, one
/// session, connected before the window — has to have its next message
/// answered after it.
///
/// # Why the consensus stream only
///
/// That is the reference's own shape: its case enables a
/// `StreamIdLossGenerator` on `consensusStreamId` and nothing else
/// (`ClusterTest.java:3526-3530`). The log is what makes the difference. A
/// blackout that also stops the log leaves the leader's log ahead of its
/// commit position — uncommitted entries it appended while it could not reach
/// a quorum — and the container's log reader is bounded by the commit position
/// (`BoundedLogAdapter.poll(commitPosition)`, `BoundedLogAdapter.java:100-108`).
/// Its old image therefore never drains, is never announced unavailable, and
/// the new term's `JoinLog` is never taken: the consensus module waits in
/// `awaitServicesReady` for an ack that cannot come. That is what the case
/// below (`a_full_receive_blackout_...`, ignored) shows, and it is why this
/// one is the consensus stream's.
///
/// # Why no traffic during the window
///
/// For the same reason, from the other end: messages offered *during* the
/// blackout are appended by the leader and never committed, which is the same
/// uncommitted tail. The reference's case sends one message before the outage
/// and its next after `reconnectClient()`; this one keeps the **same** session
/// and offers its second message once the window has closed, which is the
/// stronger of the two readings.
#[test]
fn a_consensus_partition_heals_and_the_cluster_elects_again() {
    let _serial = serial();
    let ports = Ports::new();
    // The harness starts members with a 30 s heartbeat timeout, and a leader
    // that cannot hear its quorum steps down only after missing it
    // (`agent/mod.rs:955-962`): three seconds here, so the partition outlasts
    // the timeout and every member really does step down.
    let heartbeat = "-Daeron.cluster.leader.heartbeat.timeout=3s";
    // Longer than the partition, so the client's session is not closed for age
    // while the cluster is deaf.
    let session_timeout = "-Daeron.cluster.session.timeout=60s";
    // The window is measured from the consensus stream's **first** frame,
    // which is the cluster starting: 15 s is past every member's election and
    // past the probe's compile-and-connect, and 21 s is well inside the 40 s
    // the client waits before its second message.
    let properties = &|_id: i32| -> Vec<String> {
        vec![
            "-Daeron.image.liveness.timeout=1000ms".to_owned(),
            format!("-Ddeepmsg.debug.receive.data.loss.stream.id={CONSENSUS_STREAM}"),
            "-Ddeepmsg.debug.receive.data.loss.from.millis=15000".to_owned(),
            "-Ddeepmsg.debug.receive.data.loss.until.millis=21000".to_owned(),
            "-Ddeepmsg.debug.receive.data.loss.drop.heartbeats=true".to_owned(),
        ]
    };

    let Some((mut nodes, counters, faults)) =
        start_cluster_with(&ports, &[heartbeat, session_timeout], properties)
    else {
        return;
    };

    await_leader(&counters, &faults);

    // Two messages 40 s apart: the first is answered before the window, the
    // pause spans it, and the second is offered after it — same client, same
    // session.
    let reading = java_client_echoes_at(&ports, 2, 40_000);
    faults.check();
    assert!(
        reading.contains("RECEIVED 2"),
        "the reference's client was answered on both sides of the partition: {reading}; \
         roles {:?}, terms {:?}, election {:?}",
        (0..MEMBER_COUNT)
            .map(|id| counters[id as usize].role())
            .collect::<Vec<_>>(),
        (0..MEMBER_COUNT)
            .map(|id| counters[id as usize].leadership_term_id())
            .collect::<Vec<_>>(),
        (0..MEMBER_COUNT)
            .map(|id| counters[id as usize].election_state())
            .collect::<Vec<_>>(),
    );

    // And the cluster really did elect again rather than merely survive: one
    // leader, every member closed, and the leader's term past its first.
    let leader = await_leader(&counters, &faults);
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline
        && !counters
            .iter()
            .all(|node| node.election_state() == Some(ELECTION_CLOSED))
    {
        faults.check();
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        counters
            .iter()
            .all(|node| node.election_state() == Some(ELECTION_CLOSED)),
        "every member's election closes again; states {:?}",
        (0..MEMBER_COUNT)
            .map(|id| counters[id as usize].election_state())
            .collect::<Vec<_>>()
    );
    assert!(
        counters[leader as usize].leadership_term_id().unwrap_or(0) >= 1,
        "the leader that serves after the partition is in a later term: {:?}",
        (0..MEMBER_COUNT)
            .map(|id| counters[id as usize].leadership_term_id())
            .collect::<Vec<_>>()
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **A full receive blackout leaves every container holding its old log
/// image.** Ignored: this is the measurement that says why the case above is
/// the consensus stream's, not a criterion anyone passes.
///
/// Every member stops hearing **everything** — log, consensus, ingress — for
/// the same window, and the client offers throughout it (one message every
/// 100 ms). Measured state after the window, with the defects the diagnosis
/// found fixed: the cluster elects, the followers form log images, and then
/// every container sits on an image that cannot drain —
/// `position` 7008, its only reader at 6816 because the commit position is
/// 6816 — so `JoinLog` is never taken and the consensus module spins in
/// `awaitServicesReady`. See `records/P3-6c.md` §4.
#[test]
#[ignore = "P3-6c: a full receive blackout leaves the leader's log ahead of \
            its commit position, and the container's old image then never \
            drains (from 6912/7008 to 6816, measured) — the new term's \
            JoinLog is never taken and the module waits in \
            awaitServicesReady. See records/P3-6c.md."]
fn a_full_receive_blackout_leaves_the_container_holding_its_log_image() {
    let _serial = serial();
    let ports = Ports::new();
    let heartbeat = "-Daeron.cluster.leader.heartbeat.timeout=3s";
    let session_timeout = "-Daeron.cluster.session.timeout=60s";
    let properties = &|_id: i32| -> Vec<String> {
        vec![
            "-Daeron.image.liveness.timeout=1000ms".to_owned(),
            "-Ddeepmsg.debug.receive.data.loss.from.millis=8000".to_owned(),
            "-Ddeepmsg.debug.receive.data.loss.until.millis=14000".to_owned(),
            "-Ddeepmsg.debug.receive.data.loss.drop.heartbeats=true".to_owned(),
        ]
    };

    let Some((mut nodes, counters, faults)) =
        start_cluster_with(&ports, &[heartbeat, session_timeout], properties)
    else {
        return;
    };

    await_leader(&counters, &faults);

    let reading = java_client_echoes_at(&ports, 300, 100);
    faults.check();
    assert!(
        reading.contains("RECEIVED 300"),
        "the reference's client read every echo across the partition: {reading}"
    );

    for node in &mut nodes {
        node.stop();
    }
}
