//! **P3-7's first rung**: a cluster whose members do not all run the same
//! implementation — LC-3's mixed membership — in one cell, both ways round,
//! fast enough to be the subset a CI job runs (plan §9 Q6).
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
//! # What these cases are
//!
//! The reconnaissance the card was planned on
//! (`analysis/cluster/deepmsg-p3-7-plan-mixed-membership.md` §2): one cell
//! whose failure names the layer, before a matrix. The full matrix — six
//! scenarios × both topologies × both leadership directions × both modes — is
//! `cluster_lc3_matrix`; this binary is the same two implementations meeting
//! in the smallest number of ways that still covers both startup paths, both
//! leadership directions and both catch-up directions, beside an all-reference
//! control.
//!
//! # The ports
//!
//! `ClusterPorts` is the reference's own scheme (`ClusterConfig.java:425-445`)
//! and each case takes a block of its own from this binary's band
//! ([`bands::LC3_SMOKE`]): below `net.ipv4.ip_local_port_range`,
//! because inside it any process may hold a port mid-run (`records/P3-9.md`
//! §9.2).
//!
//! # One at a time
//!
//! Each case starts three JVMs or their Rust equivalent and a driver, so
//! [`SERIAL`] holds them apart and the binary is meant to run with
//! `--test-threads=1`.

use std::sync::Mutex;

use deepmsg_tests::cluster::{MemberSpec, Options, ProcessMode, TestCluster, bands};
use deepmsg_tests::cluster_mixed::{
    self, Leader, LeaderImpl, Topology, assert_one_leader, await_caught_up,
    await_everyone_at_the_leaders_position, commit_position, connect, echo, node_logs, options,
    start_member, stop_member,
};

/// The tests in this binary each start a whole cluster.
static SERIAL: Mutex<()> = Mutex::new(());

/// The member this build runs in a 1R2J table.
const OURS: i32 = 0;
/// A reference member, for the directions the 1R2J table gives us.
const THEIRS: i32 = 1;

/// The next cluster's port base, from this binary's band ([`bands::LC3_SMOKE`]).
fn next_port_base() -> u16 {
    bands::LC3_SMOKE.next_base()
}

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Start a cluster over a fresh port block and wait until every member has a
/// role and exactly one is the leader.
fn start_with(
    test_name: &str,
    members: &[MemberSpec],
    settings: Options,
) -> Option<(TestCluster, i32)> {
    cluster_mixed::start(test_name, members, settings)
}

/// The 1R2J table's cluster, with `leader` appointed by every member.
fn start_mixed(
    test_name: &str,
    mode: ProcessMode,
    leader: LeaderImpl,
) -> Option<(TestCluster, i32, u16)> {
    let base = next_port_base();
    let appointed = Topology::OneRustTwoJava.member_of(leader);
    let settings = cluster_mixed::options_with(options(base), Leader::Everyone(appointed));
    let (cluster, found) =
        start_with(test_name, &Topology::OneRustTwoJava.members(mode), settings)?;
    Some((cluster, found, base))
}

/// D1, in process: one member is ours, two are the reference's, and the
/// cluster elects, commits and echoes.
#[test]
fn a_mixed_cluster_elects_and_echoes_with_our_module_in_process() {
    let _serial = serial();
    let name = "lc3-mixed-in-process";
    let Some((mut cluster, leader, base)) =
        start_mixed(name, ProcessMode::InProcess, LeaderImpl::Rust)
    else {
        cluster_mixed::skip(name);
        return;
    };
    assert_one_leader(&cluster, leader);
    let Some(mut session) = connect(name, base, &cluster) else {
        cluster_mixed::skip("our client's driver");
        return;
    };
    echo(&mut session, 0, 5);
    await_everyone_at_the_leaders_position(&cluster, leader, &mut session);
    cluster.shutdown();
}

/// D1, the other startup path: our module as the `deepmsg-cluster-node`
/// binary. It is a different program with a different signal path, and a mode
/// that is never run is a mode that is never tested (plan §7.2, Q4b).
#[test]
fn a_mixed_cluster_elects_and_echoes_with_our_module_as_a_binary() {
    let _serial = serial();
    let name = "lc3-mixed-external";
    let Some((mut cluster, leader, base)) =
        start_mixed(name, ProcessMode::External, LeaderImpl::Rust)
    else {
        cluster_mixed::skip(name);
        return;
    };
    assert_one_leader(&cluster, leader);
    let Some(mut session) = connect(name, base, &cluster) else {
        cluster_mixed::skip("our client's driver");
        return;
    };
    echo(&mut session, 0, 5);
    await_everyone_at_the_leaders_position(&cluster, leader, &mut session);
    cluster.shutdown();
}

/// The direction the 1R2J table would otherwise leave to chance: **ours leads**.
#[test]
fn our_member_leads_when_the_cluster_appoints_it() {
    let _serial = serial();
    let name = "lc3-our-leader";
    let Some((mut cluster, leader, base)) =
        start_mixed(name, ProcessMode::InProcess, LeaderImpl::Rust)
    else {
        cluster_mixed::skip(name);
        return;
    };
    assert_eq!(
        leader,
        OURS,
        "the appointed leader must be the leader\n{}",
        node_logs(&cluster)
    );
    assert_one_leader(&cluster, leader);
    let Some(mut session) = connect(name, base, &cluster) else {
        cluster_mixed::skip("our client's driver");
        return;
    };
    echo(&mut session, 0, 5);
    cluster.shutdown();
}

/// And the other: **the reference leads**, with our member a follower.
#[test]
fn a_reference_member_leads_when_the_cluster_appoints_it() {
    let _serial = serial();
    let name = "lc3-java-leader";
    let Some((mut cluster, leader, base)) =
        start_mixed(name, ProcessMode::InProcess, LeaderImpl::Java)
    else {
        cluster_mixed::skip(name);
        return;
    };
    assert_eq!(
        leader,
        THEIRS,
        "the appointed leader must be the leader\n{}",
        node_logs(&cluster)
    );
    assert_one_leader(&cluster, leader);
    let Some(mut session) = connect(name, base, &cluster) else {
        cluster_mixed::skip("our client's driver");
        return;
    };
    echo(&mut session, 0, 5);
    cluster.shutdown();
}

/// A follower that was away comes back, and its commit position is the
/// leader's.
fn a_follower_catches_up(appointed: LeaderImpl, stop: i32, name: &str) {
    let Some((mut cluster, leader, base)) = start_mixed(name, ProcessMode::InProcess, appointed)
    else {
        cluster_mixed::skip(name);
        return;
    };
    assert_ne!(leader, stop, "{name}: the member that leaves is a follower");

    let Some(mut session) = connect(name, base, &cluster) else {
        cluster_mixed::skip("our client's driver");
        return;
    };
    echo(&mut session, 0, 5);

    stop_member(&mut cluster, stop);
    echo(&mut session, 5, 10);

    let position =
        commit_position(&cluster, leader).expect("the leader publishes a commit position");
    start_member(&mut cluster, stop);
    await_caught_up(&cluster, stop, position, &mut session);

    // And the cluster is whole again: the next message goes through with every
    // member back.
    echo(&mut session, 10, 12);
    cluster.shutdown();
}

/// **D2**: our member, away, comes back to a **reference** leader.
#[test]
fn our_member_catches_up_from_a_reference_leader() {
    let _serial = serial();
    a_follower_catches_up(LeaderImpl::Java, OURS, "lc3-catchup-ours");
}

/// **D3**: a reference member, away, comes back to **our** leader.
#[test]
fn a_reference_member_catches_up_from_our_leader() {
    let _serial = serial();
    a_follower_catches_up(LeaderImpl::Rust, THEIRS, "lc3-catchup-theirs");
}

/// The fixture's self-check: the same table with **every** node the
/// reference's.
///
/// If this is red, the scenario reads the fixture wrong — the ports, the
/// member string, the container, the client — and the mixed cases would be
/// testing something other than the mixed membership (plan §7.2, Q4d).
#[test]
fn a_control_cluster_of_three_reference_nodes_elects_and_echoes() {
    let _serial = serial();
    let name = "lc3-control";
    let base = next_port_base();
    let members = vec![
        MemberSpec::our_container(0, "echo", ProcessMode::InProcess),
        MemberSpec::our_container(1, "echo", ProcessMode::InProcess),
        MemberSpec::our_container(2, "echo", ProcessMode::InProcess),
    ];
    let Some((mut cluster, leader)) = start_with(name, &members, cluster_mixed::options(base))
    else {
        cluster_mixed::skip(name);
        return;
    };
    assert_one_leader(&cluster, leader);
    let Some(mut session) = connect(name, base, &cluster) else {
        cluster_mixed::skip("our client's driver");
        return;
    };
    echo(&mut session, 0, 5);
    cluster.shutdown();
}
