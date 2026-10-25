//! **P3-7's first rung**: a cluster whose members do not all run the same
//! implementation — LC-3's mixed membership.
//!
//! What is new is the **node**, not the container. One member is this build's:
//! a `deepmsg-archiving-media-driver` process with this build's consensus
//! module, on a thread of this process (`ProcessMode::InProcess`) or as the
//! `deepmsg-cluster-node` binary (`External`). The other two are the
//! reference's `ClusteredMediaDriver` processes. Every member's container is
//! this build's, which LC-1 and LC-4a already proved on both kinds of node —
//! so a red here is about the consensus module meeting its peer, and not about
//! a container meeting a node.
//!
//! # What this binary is for
//!
//! It is the reconnaissance the rest of the card is planned on
//! (`analysis/cluster/deepmsg-p3-7-plan-mixed-membership.md` §2): one cell
//! whose failure names the layer, before a matrix of twenty-four.
//!
//! * **D1 — do the two implementations elect each other?** Three members, one
//!   leader, one term; then the reference's client-visible contract — an echo
//!   through the leader — and the two leadership directions, appointed so that
//!   neither is left to whoever happens to win a race.
//! * **D2 — can our member catch up from a Java leader?** Stop it, commit, start
//!   it again over the same directories, and read its commit position.
//! * **D3 — can a Java member catch up from ours?** The same the other way
//!   round, which is the direction in which *our* log replay has to satisfy the
//!   reference's follower.
//!
//! Each of them runs beside a **control**: the same table with every node the
//! reference's. A green control beside a red mixed case is what makes the mixed
//! case's red about the consensus modules rather than about the fixture.
//!
//! # The ports
//!
//! [`ClusterPorts`] is the reference's own scheme (`ClusterConfig.java:425-445`)
//! and the base is this binary's: below `net.ipv4.ip_local_port_range`, because
//! inside it any process may hold a port mid-run (memory: the ephemeral-range
//! lesson, `records/P3-9.md` §9.2).
//!
//! # One at a time
//!
//! Each case starts three JVMs or their Rust equivalent and a driver, so
//! [`SERIAL`] holds them apart and the binary is meant to run with
//! `--test-threads=1`.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use deepmsg_cluster_common::counters::{
    CLUSTER_COMMIT_POSITION_TYPE_ID as COMMIT_POSITION, CLUSTER_NODE_ROLE_TYPE_ID,
};
use deepmsg_tests::cluster::{ClusterPorts, MemberSpec, Options, ProcessMode, TestCluster};
use deepmsg_tests::cluster_client::{Session, base_context, send_and_await_echo};

/// The tests in this binary each start a whole cluster.
static SERIAL: Mutex<()> = Mutex::new(());

const MEMBERS: i32 = 3;
const CLUSTER_ID: i32 = 21;
const DEADLINE: Duration = Duration::from_secs(60);

/// `Cluster.Role.LEADER` (`Cluster.java:51-68`; the fixture's `ROLE_LEADER`).
const ROLE_LEADER: i64 = 2;
/// `Cluster.Role.FOLLOWER` (`Cluster.java:51-68`). `CANDIDATE` is 1 and is
/// never a settled state.
const ROLE_FOLLOWER: i64 = 0;

/// The member this build runs, in a 1R2J table. Members `1` and `2` are the
/// reference's nodes with this build's containers.
const OURS: i32 = 0;

/// The next cluster's port base; this binary owns the 15xxx block.
fn next_port_base() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(13_000);
    NEXT.fetch_add(1_000, Ordering::Relaxed)
}

/// The cluster-wide settings every case here uses.
///
/// `appointed_leader` is `None` for an election and `Some(member)` to make the
/// direction of leadership a fact of the case rather than of who won a race
/// (`aeron.cluster.appointed.leader.id`, `ConsensusModule.java:465-480`).
///
/// The timer supplier is written down rather than left to the default: it is a
/// **byte contract**, not a performance knob (plan §7.4 point 3), and the
/// snapshot comparisons this card exists for are only meaningful when both
/// sides iterate the same container.
fn options(base: u16, appointed_leader: Option<i32>) -> Options {
    let mut node_properties = vec![(
        "aeron.cluster.timer.service.supplier".to_owned(),
        "io.aeron.cluster.WheelTimerServiceSupplier".to_owned(),
    )];
    if let Some(leader) = appointed_leader {
        node_properties.push((
            "aeron.cluster.appointed.leader.id".to_owned(),
            leader.to_string(),
        ));
    }

    Options {
        cluster_id: CLUSTER_ID,
        members: Some(ClusterPorts::new(base).members_string(MEMBERS)),
        ingress_channel: "aeron:udp?term-length=128k".to_owned(),
        consensus_channel: "aeron:udp?term-length=64k".to_owned(),
        log_channel: "aeron:udp?term-length=64k".to_owned(),
        node_properties,
        ..Options::default()
    }
}

/// The member table: our node, and two of the reference's.
fn members(mode: ProcessMode) -> Vec<MemberSpec> {
    vec![
        MemberSpec::rust_node(OURS, "echo", mode),
        MemberSpec::our_container(1, "echo", mode),
        MemberSpec::our_container(2, "echo", mode),
    ]
}

fn skip() {
    eprintln!(
        "SKIPPED: the reference jar, java, the fork's Java probes, our driver or our \
         consensus module binary is missing"
    );
}

fn node_logs(cluster: &TestCluster) -> String {
    (0..MEMBERS)
        .map(|member| format!("member {member}:\n{}", cluster.node_log(member)))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Start the cluster over `base` and wait until every member has a role and
/// exactly one is the leader.
///
/// The base is the caller's: the client must be pointed at the **same** block
/// the members were, and a helper that picks its own would hand the client a
/// second, empty one.
fn start(
    test_name: &str,
    base: u16,
    members: &[MemberSpec],
    appointed_leader: Option<i32>,
) -> (TestCluster, i32) {
    let Some(cluster) = TestCluster::start(test_name, members, options(base, appointed_leader))
    else {
        skip();
        panic!("{test_name}: the fixture cannot stand a cluster up on this machine");
    };

    let leader = await_all_joined(&cluster);
    (cluster, leader)
}

/// Wait until every member has a role and exactly one is the leader.
///
/// [`TestCluster::await_leader`] answers as soon as *one* member says LEADER,
/// which on a cluster that is still joining is a member that has not been
/// canvassed yet — the case would then read a follower's INIT as a follower's
/// FOLLOW, or worse, miss a second leader.
fn await_all_joined(cluster: &TestCluster) -> i32 {
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        let roles = roles(cluster);
        // `None` is a member whose module has not published a role at all;
        // `Some(0)` is a **follower**, which is a role of its own
        // (`Cluster.java:51-68`: FOLLOWER=0, CANDIDATE=1, LEADER=2).
        if roles.iter().all(Option::is_some)
            && roles
                .iter()
                .filter(|role| **role == Some(ROLE_LEADER))
                .count()
                == 1
        {
            return i32::try_from(
                roles
                    .iter()
                    .position(|role| *role == Some(ROLE_LEADER))
                    .expect("one member says LEADER"),
            )
            .expect("a member id");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "the members never agreed on one leader\n{}",
        node_logs(cluster)
    );
}

/// Every member's role counter, or `None` for a member that has not set one.
fn roles(cluster: &TestCluster) -> Vec<Option<i64>> {
    (0..MEMBERS)
        .map(|member| cluster.counter_on(member, CLUSTER_NODE_ROLE_TYPE_ID))
        .collect()
}

/// One leader, and two followers — read on every member's own driver.
fn assert_one_leader(cluster: &TestCluster, leader: i32) {
    let roles = roles(cluster);
    let leaders = roles
        .iter()
        .filter(|role| **role == Some(ROLE_LEADER))
        .count();
    assert_eq!(
        leaders,
        1,
        "member {leader} is the leader and {leaders} members say LEADER: {roles:?}\n{}",
        node_logs(cluster)
    );
    for member in 0..MEMBERS {
        if member != leader {
            assert_eq!(
                roles[usize::try_from(member).expect("a member id")],
                Some(ROLE_FOLLOWER),
                "member {member} is not the leader, so it is a follower\n{}",
                node_logs(cluster)
            );
        }
    }
}

/// A client connects through the members' ingress endpoints, echoes `count`
/// messages, and answers how many came back.
fn echo(cluster: &TestCluster, label: &str, base: u16, count: u64) {
    let ports = ClusterPorts::new(base);
    let mut context = base_context(label, DEADLINE);
    context.ingress_channel = Some("aeron:udp?term-length=128k".to_owned());
    context.ingress_endpoints = Some(ports.ingress_endpoints(MEMBERS));

    let Some(mut session) = Session::connect(label, context, &[], || node_logs(cluster)) else {
        skip();
        panic!("{label}: our client's driver is missing");
    };

    let deadline = Instant::now() + DEADLINE;
    for sequence in 0..count {
        send_and_await_echo(&mut session, sequence, deadline);
    }

    assert_eq!(
        session.messages().len(),
        usize::try_from(count).expect("a count"),
        "every message came back\n{}",
        node_logs(cluster)
    );
}

/// D1, in process: one member is ours, two are the reference's, and the
/// cluster elects, commits and echoes.
#[test]
fn a_mixed_cluster_elects_and_echoes_with_our_module_in_process() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let (mut cluster, leader) = start(
        "lc3-mixed-in-process",
        base,
        &members(ProcessMode::InProcess),
        None,
    );
    assert_one_leader(&cluster, leader);
    echo(&cluster, "lc3-mixed-in-process", base, 5);
    cluster.shutdown();
}

/// D1, the other startup path: our module as the `deepmsg-cluster-node`
/// binary. It is a different program with a different signal path, and a mode
/// that is never run is a mode that is never tested (plan §7.2, Q4b).
#[test]
fn a_mixed_cluster_elects_and_echoes_with_our_module_as_a_binary() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let (mut cluster, leader) = start(
        "lc3-mixed-external",
        base,
        &members(ProcessMode::External),
        None,
    );
    assert_one_leader(&cluster, leader);
    echo(&cluster, "lc3-mixed-external", base, 5);
    cluster.shutdown();
}

/// The direction the 1R2J table would otherwise leave to chance: **ours leads**.
#[test]
fn our_member_leads_when_the_cluster_appoints_it() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let (mut cluster, leader) = start(
        "lc3-our-leader",
        base,
        &members(ProcessMode::InProcess),
        Some(OURS),
    );
    assert_eq!(
        leader,
        OURS,
        "the appointed leader must be the leader\n{}",
        node_logs(&cluster)
    );
    assert_one_leader(&cluster, leader);
    echo(&cluster, "lc3-our-leader", base, 5);
    cluster.shutdown();
}

/// And the other: **the reference leads**, with our member a follower.
#[test]
fn a_reference_member_leads_when_the_cluster_appoints_it() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let (mut cluster, leader) = start(
        "lc3-java-leader",
        base,
        &members(ProcessMode::InProcess),
        Some(1),
    );
    assert_eq!(
        leader,
        1,
        "the appointed leader must be the leader\n{}",
        node_logs(&cluster)
    );
    assert_one_leader(&cluster, leader);
    echo(&cluster, "lc3-java-leader", base, 5);
    cluster.shutdown();
}

/// The fixture's self-check: the same table with **every** node the reference's.
///
/// If this is red, the scenario reads the fixture wrong — the ports, the
/// member string, the container, the client — and the mixed cases would be
/// testing something other than the mixed membership (plan §7.2, Q4d).
/// A green control beside a red mixed case is what makes the mixed case's red
/// about the consensus modules.
#[test]
fn a_control_cluster_of_three_reference_nodes_elects_and_echoes() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let members = vec![
        MemberSpec::test_container(0, "test-service"),
        MemberSpec::test_container(1, "test-service"),
        MemberSpec::test_container(2, "test-service"),
    ];
    let (mut cluster, leader) = start("lc3-control", base, &members, None);
    assert_one_leader(&cluster, leader);
    echo(&cluster, "lc3-control", base, 5);
    cluster.shutdown();
}

/// The follower that was away comes back, and its commit position is the
/// leader's.
///
/// `stop` is the member taken out; the cluster keeps a quorum, so the messages
/// offered while it is gone are exactly what it has to fetch on the way back.
fn a_follower_catches_up(appointed_leader: i32, stop: i32, label: &str) {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let (mut cluster, leader) = start(
        label,
        base,
        &members(ProcessMode::InProcess),
        Some(appointed_leader),
    );
    assert_eq!(leader, appointed_leader, "the appointed leader leads");

    let ports = ClusterPorts::new(base);
    let mut context = base_context(label, DEADLINE);
    context.ingress_channel = Some("aeron:udp?term-length=128k".to_owned());
    context.ingress_endpoints = Some(ports.ingress_endpoints(MEMBERS));
    let Some(mut session) = Session::connect(label, context, &[], || node_logs(&cluster)) else {
        skip();
        panic!("{label}: our client's driver is missing");
    };
    let deadline = Instant::now() + DEADLINE;
    for sequence in 0..5 {
        send_and_await_echo(&mut session, sequence, deadline);
    }

    // Out for a while: containers first, then the node — a container outliving
    // its node ends by itself, and a test that wants to see the end watches it.
    cluster.stop_container(stop, 0);
    cluster.stop_node(stop);

    for sequence in 5..10 {
        send_and_await_echo(&mut session, sequence, deadline);
    }

    cluster.start_node_by_id(stop);
    cluster.start_container(stop, 0);

    // The catch-up is a position, not a feeling: the restarted member's commit
    // position has to reach the leader's, and it has to do that as a follower.
    let leader_position = || cluster.counter_on(leader, COMMIT_POSITION);
    let stop_deadline = Instant::now() + DEADLINE;
    loop {
        let follower = cluster.counter_on(stop, COMMIT_POSITION);
        let leader_position = leader_position();
        if follower.is_some() && follower == leader_position {
            break;
        }
        assert!(
            Instant::now() < stop_deadline,
            "member {stop} never caught up: its commit position is {follower:?}, the leader's is \
             {leader_position:?}\n{}",
            node_logs(&cluster)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        cluster.counter_on(stop, CLUSTER_NODE_ROLE_TYPE_ID),
        Some(ROLE_FOLLOWER),
        "the member that caught up is a follower\n{}",
        node_logs(&cluster)
    );

    // And the cluster is whole again: the next message goes through with every
    // member back.
    send_and_await_echo(&mut session, 10, deadline);
    cluster.shutdown();
}

/// **D2**: our member, away, comes back to a **reference** leader.
#[test]
fn our_member_catches_up_from_a_reference_leader() {
    a_follower_catches_up(1, OURS, "lc3-catchup-ours");
}

/// **D3**: a reference member, away, comes back to **our** leader.
#[test]
fn a_reference_member_catches_up_from_our_leader() {
    a_follower_catches_up(OURS, 1, "lc3-catchup-theirs");
}
