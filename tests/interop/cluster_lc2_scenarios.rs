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
) -> Option<(Vec<Node>, Vec<NodeCounters>)> {
    start_cluster_with(ports, module_arguments, &|_| Vec::new())
}

/// [`start_cluster`], with this member's **driver** settings — where the
/// network knobs live (`debug.*.data.loss.*`, `crates/driver/src/config.rs`).
fn start_cluster_with(
    ports: &Ports,
    module_arguments: &[&str],
    driver_properties: &dyn Fn(i32) -> Vec<String>,
) -> Option<(Vec<Node>, Vec<NodeCounters>)> {
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

    Some((nodes, counters))
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
    let Some((mut nodes, counters)) = start_cluster(&ports, &[]) else {
        return;
    };

    await_leader(&counters, &Faults::default());

    let reading = java_client_echoes(&ports, 5);
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
    let Some((mut nodes, counters)) = start_cluster(&ports, &[argument.as_str()]) else {
        return;
    };

    let leader = await_leader(&counters, &Faults::default());
    assert_eq!(
        appointed, leader,
        "the appointed member leads, not whichever election won"
    );

    let reading = java_client_echoes(&ports, 3);
    assert!(
        reading.contains("RECEIVED 3"),
        "the reference's client echoes through the appointed leader: {reading}"
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// **A full partition heals and the cluster elects again**
/// (`ClusterTest.shouldFormClusterAfterFullPartition`).
///
/// Every member's **receive** side is blacked out for a window
/// (`debug.receive.data.loss.from/until.millis`, heartbeats included,
/// `crates/driver/src/config.rs`), so nobody hears anybody: the leader steps
/// down on its heartbeat timeout and every member canvasses. When the window
/// closes the cluster has to elect and take the reference's client's messages
/// again — the client is still offering, one every 100 ms, across the window.
///
/// Why the whole cluster and not the leader alone: the window is a driver
/// setting, decided before the cluster starts, and which member leads is not
/// known until it has elected. A *selective* partition needs an instrument that
/// can be switched while the driver runs, which this build's `debug.*.data.loss`
/// family deliberately does not have (`media/receive_loss.rs`, module note); the
/// leader-only shape is recorded as the gap that instrument would close.
#[test]
#[ignore = "P3-6c: the cluster re-elects after the partition heals, but the \
            reference's client cannot offer again (OFFER_FAILED -1) — the \
            new-leader event, the session, or its ingress publication. \
            See records/P3-6c.md."]
fn a_full_partition_heals_and_the_cluster_elects_again() {
    let _serial = serial();
    let ports = Ports::new();
    // The harness starts members with a 30 s heartbeat timeout, and a leader
    // that cannot hear its quorum steps down only after missing it
    // (`agent/mod.rs:955-962`): three seconds here, so the partition outlasts
    // the timeout and every member really does step down.
    let heartbeat = "-Daeron.cluster.leader.heartbeat.timeout=3s";
    // Longer than the partition, so no session is closed for age while the
    // cluster is deaf.
    let session_timeout = "-Daeron.cluster.session.timeout=60s";
    let properties = &|_id: i32| -> Vec<String> {
        vec![
            "-Ddeepmsg.debug.receive.data.loss.from.millis=8000".to_owned(),
            "-Ddeepmsg.debug.receive.data.loss.until.millis=14000".to_owned(),
            "-Ddeepmsg.debug.receive.data.loss.drop.heartbeats=true".to_owned(),
        ]
    };

    let Some((mut nodes, counters)) =
        start_cluster_with(&ports, &[heartbeat, session_timeout], properties)
    else {
        return;
    };

    await_leader(&counters, &Faults::default());

    // 300 messages at 100 ms: the run spans the 8-14 s window with room on both
    // sides, and the reference's client has to keep its session across it.
    let reading = java_client_echoes_at(&ports, 300, 100);
    assert!(
        reading.contains("RECEIVED 300"),
        "the reference's client read every echo across the partition: {reading}"
    );

    for node in &mut nodes {
        node.stop();
    }
}
