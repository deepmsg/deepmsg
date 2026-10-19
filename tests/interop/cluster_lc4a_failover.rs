//! LC-4a's three-node scenarios: **a leader is elected, and the cluster keeps
//! serving when that leader goes away**.
//!
//! The single-node scenarios are in the other `cluster_lc4a_*` files; this one
//! is the part that needs three consensus modules, three drivers, three
//! archives and three containers — the shape `TestCluster.withStaticNodes(3)`
//! gives the reference's own suite.
//!
//! # Ports
//!
//! The reference's scheme (`ClusterConfig.java:42-67`): every member owns a
//! block of `PORTS_PER_NODE = 100`, with archive control at `+1`, ingress at
//! `+2`, consensus at `+3`, log at `+4`. Each test takes its own base so two
//! clusters in one process cannot meet.
//!
//! # The client
//!
//! The probes speak IPC to **one member's driver**, so a test finds the leader
//! first (`await_leader`) and points the probe there. That is also how a
//! failover is exercised: after the leader is stopped, a new probe is pointed
//! at the new leader — the same "reconnect" the reference's `reconnectClient`
//! performs, and the one a real client performs when it is told where the new
//! leader is.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use deepmsg_cluster_common::counters::CLUSTER_NODE_ROLE_TYPE_ID;
use deepmsg_tests::cluster::{MemberSpec, Options, ProcessMode, TestCluster};

const CLUSTER_ID: i32 = 13;
const MEMBERS: i32 = 3;
/// `ClusterConfig.PORTS_PER_NODE`.
const PORTS_PER_NODE: u16 = 100;
/// The offset of a member's archive control port (`:47`).
const ARCHIVE_CONTROL_PORT_OFFSET: u16 = 1;
/// The offset of a member's ingress port (`:52`).
const CLIENT_FACING_PORT_OFFSET: u16 = 2;
/// The offset of a member's consensus port (`:57`).
const MEMBER_FACING_PORT_OFFSET: u16 = 3;
/// The offset of a member's log port (`:62`).
const LOG_PORT_OFFSET: u16 = 4;

static SERIAL: Mutex<()> = Mutex::new(());

/// The next cluster's port base; each test takes a block it will not share.
fn next_port_base() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(31_000);
    NEXT.fetch_add(1_000, Ordering::Relaxed)
}

fn member_endpoint(base: u16, member: i32, offset: u16) -> String {
    format!(
        "localhost:{}",
        base + u16::try_from(member).expect("a member id") * PORTS_PER_NODE + offset
    )
}

/// `id,ingress,consensus,log,catchup,archive` per member, `|` between members.
fn members_string(base: u16) -> String {
    (0..MEMBERS)
        .map(|member| {
            format!(
                "{member},{},{},{},localhost:0,{}",
                member_endpoint(base, member, CLIENT_FACING_PORT_OFFSET),
                member_endpoint(base, member, MEMBER_FACING_PORT_OFFSET),
                member_endpoint(base, member, LOG_PORT_OFFSET),
                member_endpoint(base, member, ARCHIVE_CONTROL_PORT_OFFSET),
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn options(base: u16) -> Options {
    Options {
        cluster_id: CLUSTER_ID,
        members: Some(members_string(base)),
        ingress_channel: "aeron:udp?term-length=64k".to_owned(),
        consensus_channel: "aeron:udp?term-length=64k".to_owned(),
        log_channel: "aeron:udp?term-length=64k".to_owned(),
        node_properties: vec![
            // The probes speak IPC to the leader's driver; a cluster only
            // subscribes to IPC ingress when it is told to
            // (`ConsensusModuleAgent.java:3387-3415`).
            (
                "aeron.cluster.ingress.ipc.allowed".to_owned(),
                "true".to_owned(),
            ),
        ],
        probes: vec![
            "ClusterEchoClientProbe.java",
            "ClusterServiceMessageClientProbe.java",
            "ClusterAuctionBidClientProbe.java",
            "ClusterServiceContainerProbe.java",
            "ClusterEchoTimerService.java",
        ],
        ..Options::default()
    }
}

fn members(mode: ProcessMode) -> [MemberSpec; MEMBERS as usize] {
    [
        MemberSpec::our_container(0, "echo", mode).with_service_id(0),
        MemberSpec::our_container(1, "echo", mode).with_service_id(0),
        MemberSpec::our_container(2, "echo", mode).with_service_id(0),
    ]
}

fn start(test_name: &str, members: [MemberSpec; MEMBERS as usize]) -> Option<TestCluster> {
    TestCluster::start(test_name, &members, options(next_port_base()))
}

/// Wait until every member has a role, and answer the leader's id.
fn await_all_joined(cluster: &TestCluster) -> i32 {
    let leader = cluster.await_leader();
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        let roles: Vec<Option<i64>> = (0..MEMBERS)
            .map(|member| cluster.counter_on(member, CLUSTER_NODE_ROLE_TYPE_ID))
            .collect();
        if roles.iter().all(Option::is_some)
            && roles.iter().filter(|role| **role == Some(2)).count() == 1
        {
            return leader;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "the members never agreed on one leader\n{}",
        cluster.container_logs()
    );
}

fn elect_one_leader_and_serve(test_name: &str, mode: ProcessMode) {
    let Some(mut cluster) = start(test_name, members(mode)) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };

    cluster.await_active();
    let leader = await_all_joined(&cluster);
    let aeron_dir = cluster.aeron_dir(leader).display().to_string();

    let reading = cluster.run_probe("ClusterEchoClientProbe", &[aeron_dir.as_str(), "1", "30"]);
    assert!(
        reading.contains("RECEIVED 1"),
        "the leader answered through its own container, client said: {reading}\n{}",
        cluster.container_logs()
    );

    cluster.shutdown();
}

fn new_leader_serves(test_name: &str, mode: ProcessMode) {
    let Some(mut cluster) = start(test_name, members(mode)) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };

    cluster.await_active();
    let leader = await_all_joined(&cluster);
    let aeron_dir = cluster.aeron_dir(leader).display().to_string();

    let reading = cluster.run_probe("ClusterEchoClientProbe", &[aeron_dir.as_str(), "1", "30"]);
    assert!(
        reading.contains("RECEIVED 1"),
        "the first leader answered, client said: {reading}\n{}",
        cluster.container_logs()
    );

    // Stop the leader's container and node: the remaining two elect one of
    // themselves.
    cluster.stop_container(leader, 0);
    cluster.stop_node(leader);

    let new_leader = cluster.await_leader();
    assert_ne!(new_leader, leader, "a different member leads");
    let aeron_dir = cluster.aeron_dir(new_leader).display().to_string();

    // A new client — the reference's `reconnectClient()` — reaches the new
    // leader and is served.
    let reading = cluster.run_probe("ClusterEchoClientProbe", &[aeron_dir.as_str(), "1", "60"]);
    assert!(
        reading.contains("RECEIVED 1"),
        "the new leader answered, client said: {reading}\n{}",
        cluster.container_logs()
    );

    cluster.shutdown();
}

/// Send `MESSAGE_COUNT` service messages to the current leader and assert each
/// came back exactly once.
fn service_messages_through(cluster: &TestCluster) -> String {
    let leader = cluster.await_leader();
    let aeron_dir = cluster.aeron_dir(leader).display().to_string();
    cluster.run_probe(
        "ClusterServiceMessageClientProbe",
        &[aeron_dir.as_str(), "10", "60"],
    )
}

fn service_messages_survive_failover(test_name: &str, mode: ProcessMode) {
    let Some(mut cluster) = start(test_name, members(mode)) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };

    cluster.await_active();
    await_all_joined(&cluster);

    // Ten messages through the first leader.
    let reading = service_messages_through(&cluster);
    assert!(
        reading.contains("RECEIVED 10"),
        "ten service messages came back once each, client said: {reading}\n{}",
        cluster.container_logs()
    );

    // The leader's container and node go away; the other two elect one of
    // themselves.
    let old_leader = cluster.await_leader();
    cluster.stop_container(old_leader, 0);
    cluster.stop_node(old_leader);
    let new_leader = cluster.await_leader();
    assert_ne!(new_leader, old_leader, "a different member leads");

    // Ten more through the new leader, with no duplicate and no loss.
    let reading = service_messages_through(&cluster);
    assert!(
        reading.contains("RECEIVED 10"),
        "ten service messages after the failover came back once each, client said: {reading}\n{}",
        cluster.container_logs()
    );

    // The old leader comes back and catches up rather than replaying into a
    // duplicate.
    cluster.start_node_by_id(old_leader);
    cluster.start_container(old_leader, 0);
    let reading = service_messages_through(&cluster);
    assert!(
        reading.contains("RECEIVED 10"),
        "the cluster still answers once each after the old leader returns, client said: {reading}\n{}",
        cluster.container_logs()
    );

    cluster.shutdown();
}

fn service_messages_survive_restart(test_name: &str, mode: ProcessMode) {
    let Some(mut cluster) = start(test_name, members(mode)) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };

    cluster.await_active();
    await_all_joined(&cluster);

    let reading = service_messages_through(&cluster);
    assert!(
        reading.contains("RECEIVED 10"),
        "ten service messages came back once each, client said: {reading}\n{}",
        cluster.container_logs()
    );

    // Every container and every node stops, then everything starts again over
    // the same directories: the reference's `stopAllNodes`/`restartAllNodes`.
    for member in 0..MEMBERS {
        cluster.stop_container(member, 0);
    }
    for member in 0..MEMBERS {
        cluster.stop_node(member);
    }
    for member in 0..MEMBERS {
        cluster.start_node_by_id(member);
    }
    for member in 0..MEMBERS {
        cluster.start_container(member, 0);
    }

    cluster.await_active();
    await_all_joined(&cluster);

    let reading = service_messages_through(&cluster);
    assert!(
        reading.contains("RECEIVED 10"),
        "ten service messages after a full restart came back once each, client said: {reading}\n{}",
        cluster.container_logs()
    );

    cluster.shutdown();
}

#[test]
fn three_nodes_elect_one_leader_and_serve_in_process() {
    let _serial = SERIAL.lock().expect("the serial lock");
    elect_one_leader_and_serve("lc4a-three-nodes-in-process", ProcessMode::InProcess);
}

#[test]
fn three_nodes_elect_one_leader_and_serve_external() {
    let _serial = SERIAL.lock().expect("the serial lock");
    elect_one_leader_and_serve("lc4a-three-nodes-external", ProcessMode::External);
}

#[test]
fn a_new_leader_serves_after_the_old_one_is_stopped_in_process() {
    let _serial = SERIAL.lock().expect("the serial lock");
    new_leader_serves("lc4a-failover-in-process", ProcessMode::InProcess);
}

#[test]
fn a_new_leader_serves_after_the_old_one_is_stopped_external() {
    let _serial = SERIAL.lock().expect("the serial lock");
    new_leader_serves("lc4a-failover-external", ProcessMode::External);
}

#[test]
fn service_messages_survive_a_failover_in_process() {
    let _serial = SERIAL.lock().expect("the serial lock");
    service_messages_survive_failover(
        "lc4a-service-messages-failover-in-process",
        ProcessMode::InProcess,
    );
}

#[test]
fn service_messages_survive_a_failover_external() {
    let _serial = SERIAL.lock().expect("the serial lock");
    service_messages_survive_failover(
        "lc4a-service-messages-failover-external",
        ProcessMode::External,
    );
}

#[test]
fn service_messages_survive_a_full_cluster_restart_in_process() {
    let _serial = SERIAL.lock().expect("the serial lock");
    service_messages_survive_restart(
        "lc4a-service-messages-restart-in-process",
        ProcessMode::InProcess,
    );
}

#[test]
fn service_messages_survive_a_full_cluster_restart_external() {
    let _serial = SERIAL.lock().expect("the serial lock");
    service_messages_survive_restart(
        "lc4a-service-messages-restart-external",
        ProcessMode::External,
    );
}

fn service_messages_survive_snapshot_and_restart(test_name: &str, mode: ProcessMode) {
    let Some(mut cluster) = start(test_name, members(mode)) else {
        eprintln!("SKIPPED: the reference jar, java, or the fork's Java probes");
        return;
    };

    cluster.await_active();
    await_all_joined(&cluster);

    let reading = service_messages_through(&cluster);
    assert!(
        reading.contains("RECEIVED 10"),
        "ten service messages came back once each, client said: {reading}\n{}",
        cluster.container_logs()
    );

    // One SNAPSHOT, from the leader's driver. The container script of the
    // snapshot is not this scenario's subject — it is that the restart that
    // follows starts from it.
    let leader = cluster.await_leader();
    let aeron_dir = cluster.aeron_dir(leader).display().to_string();
    let reading = cluster.run_probe(
        "ClusterAuctionBidClientProbe",
        &[
            aeron_dir.as_str(),
            &CLUSTER_ID.to_string(),
            "0",
            "0",
            "0",
            "true",
            "0",
        ],
    );
    assert!(
        reading.contains("SNAPSHOT_REQUESTED"),
        "the SNAPSHOT toggle turned, client said: {reading}\n{}",
        cluster.container_logs()
    );

    // The module counts it only after every service acked and the log was
    // forced, which is the point a restart can recover from.
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if cluster.counter_on(
            leader,
            deepmsg_cluster_common::counters::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID,
        ) == Some(1)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        cluster.counter_on(
            leader,
            deepmsg_cluster_common::counters::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID
        ),
        Some(1),
        "the module counted the snapshot\n{}",
        cluster.container_logs()
    );

    for member in 0..MEMBERS {
        cluster.stop_container(member, 0);
    }
    for member in 0..MEMBERS {
        cluster.stop_node(member);
    }
    for member in 0..MEMBERS {
        cluster.start_node_by_id(member);
    }
    for member in 0..MEMBERS {
        cluster.start_container(member, 0);
    }

    cluster.await_active();
    await_all_joined(&cluster);

    let reading = service_messages_through(&cluster);
    assert!(
        reading.contains("RECEIVED 10"),
        "ten service messages after a snapshot and a restart came back once each, client said: \
         {reading}\n{}",
        cluster.container_logs()
    );

    cluster.shutdown();
}

#[test]
fn service_messages_survive_a_snapshot_and_a_restart_in_process() {
    let _serial = SERIAL.lock().expect("the serial lock");
    service_messages_survive_snapshot_and_restart(
        "lc4a-service-messages-snapshot-restart-in-process",
        ProcessMode::InProcess,
    );
}

#[test]
fn service_messages_survive_a_snapshot_and_a_restart_external() {
    let _serial = SERIAL.lock().expect("the serial lock");
    service_messages_survive_snapshot_and_restart(
        "lc4a-service-messages-snapshot-restart-external",
        ProcessMode::External,
    );
}
