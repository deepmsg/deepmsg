//! **P3-7b**: LC-3's matrix — six scenarios, both topologies, both leadership
//! directions, both startup paths.
//!
//! One case is a [`Cell`]: a [`Topology`] (`1R2J` or `2R1J`), a [`LeaderImpl`]
//! appointed to lead, and a [`ProcessMode`] for our members. Each scenario runs
//! all eight cells, and each cell is a fresh cluster on a port block of its own.
//!
//! # The scenarios
//!
//! | scenario | what it does |
//! |---|---|
//! | steady echo | ten messages through the leader, then every member at the leader's commit position |
//! | kill leader | the leader dies, the survivors elect, the messages go on, the dead one comes back |
//! | kill follower | a follower is away while the cluster commits, then comes back |
//! | snapshot | the SNAPSHOT admin request, every member takes its own snapshot, a member reloads its own |
//! | catchup | a follower is away **across a snapshot**, so what it is missing is not in its log |
//! | replication | a follower is away for a few messages and the leader's log recording never restarted |
//!
//! The last two differ in what the returning member has to fetch; **which path
//! it took is P3-7c's judgement** (the recordings' bytes), and here both are
//! judged by the position the member reaches (see `cluster_mixed`'s module
//! note). What this binary establishes is that both paths *run* in every cell.
//!
//! # Leadership is appointed, twice over
//!
//! Scenarios that never kill a leader tell **every** member who leads
//! (`Leader::Everyone`), which is the deployment shape. The kill-leader
//! scenario tells **only** the member that is meant to lead
//! (`Leader::OnlyThatMember`), because a member appointed to somebody else
//! never nominates (`Election.java:715-718`) — with every member told the same
//! dead id the survivors canvass forever, and the case would be asserting the
//! reference's stall rather than a failover.
//!
//! With that, the two transitions this card exists for come out deterministic:
//! in `1R2J` the Rust leader dies and both survivors are the reference's; in
//! `2R1J` the reference's leader dies and both survivors are ours.
//!
//! # One cluster at a time
//!
//! Every cell starts three members and their drivers, so [`SERIAL`] holds the
//! whole matrix apart and the binary is meant to run with `--test-threads=1`.
//! The port base is this binary's own block (17xxx), below
//! `net.ipv4.ip_local_port_range`.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use deepmsg_codec_cluster::admin_request_type::AdminRequestType;
use deepmsg_codec_cluster::admin_response_code::AdminResponseCode;
use deepmsg_tests::cluster::{Options, ProcessMode, TestCluster};
use deepmsg_tests::cluster_client::Session;
use deepmsg_tests::cluster_mixed::{
    self, DEADLINE, Leader, LeaderImpl, MEMBERS, Topology, assert_one_leader, await_caught_up,
    await_everyone_at_the_leaders_position, commit_position, connect, echo, leadership_term,
    node_logs, options, snapshots_taken, start_member, stop_member,
};

/// The whole matrix runs one cell at a time.
static SERIAL: Mutex<()> = Mutex::new(());

/// The next cluster's port base; this binary owns the 17xxx block.
fn next_port_base() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(17_000);
    NEXT.fetch_add(1_000, Ordering::Relaxed)
}

/// One cell of the matrix.
#[derive(Clone, Copy, Debug)]
struct Cell {
    topology: Topology,
    leader: LeaderImpl,
    mode: ProcessMode,
}

impl Cell {
    fn name(&self, scenario: &str) -> String {
        format!(
            "{scenario}-{:?}-{:?}-{:?}",
            self.topology, self.leader, self.mode
        )
    }

    /// The member this cell appoints.
    fn appointed(&self) -> i32 {
        self.topology.member_of(self.leader)
    }
}

/// Run `body` on all eight cells, one cluster at a time.
fn on_every_cell(body: impl Fn(&Cell)) {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // `LC3_CELL=n` runs one cell, for a failure that has to be narrowed down;
    // a full run leaves it unset.
    let only: Option<usize> = std::env::var("LC3_CELL")
        .ok()
        .and_then(|cell| cell.parse().ok());
    for (index, topology) in Topology::ALL.into_iter().enumerate() {
        for (leader_index, leader) in LeaderImpl::ALL.into_iter().enumerate() {
            for (mode_index, mode) in [ProcessMode::InProcess, ProcessMode::External]
                .into_iter()
                .enumerate()
            {
                let cell_index = index * 4 + leader_index * 2 + mode_index;
                if only.is_some_and(|only| only != cell_index) {
                    continue;
                }
                body(&Cell {
                    topology,
                    leader,
                    mode,
                });
            }
        }
    }
}

/// Run `body` on every topology in both modes — the cells whose case has no
/// leadership dimension of its own.
fn on_every_topology_and_mode(body: impl Fn(&Cell)) {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let only: Option<usize> = std::env::var("LC3_CELL")
        .ok()
        .and_then(|cell| cell.parse().ok());
    for (index, topology) in Topology::ALL.into_iter().enumerate() {
        for (mode_index, mode) in [ProcessMode::InProcess, ProcessMode::External]
            .into_iter()
            .enumerate()
        {
            let cell_index = index * 2 + mode_index;
            if only.is_some_and(|only| only != cell_index) {
                continue;
            }
            // The leader field is unused by the cases that call this; the
            // appointment it would name is not made.
            body(&Cell {
                topology,
                leader: LeaderImpl::Rust,
                mode,
            });
        }
    }
}

/// Start one cell's cluster, check the appointment took, run `body`, stop it.
fn with_cluster(
    cell: &Cell,
    scenario: &str,
    appointment: fn(&Cell) -> Leader,
    extra: impl FnOnce(Options, i32) -> Options,
    body: impl FnOnce(&mut TestCluster, i32, u16),
) {
    let base = next_port_base();
    let members = cell.topology.members(cell.mode);
    let appointed = cell.appointed();
    let settings = cluster_mixed::options_with(extra(options(base), appointed), appointment(cell));

    let Some((mut cluster, leader)) =
        cluster_mixed::start(&cell.name(scenario), &members, settings)
    else {
        cluster_mixed::skip(&cell.name(scenario));
        return;
    };

    if let Leader::Everyone(appointed) = appointment(cell) {
        assert_eq!(
            leader,
            appointed,
            "{}: the appointed member must lead\n{}",
            cell.name(scenario),
            node_logs(&cluster)
        );
    }
    assert_one_leader(&cluster, leader);

    body(&mut cluster, leader, base);
    cluster.shutdown();
}

/// A member that is not the leader. When `other_implementation` is set, one
/// whose implementation differs from the leader's is preferred, because a
/// member of the other kind is the interesting one to take away.
fn a_follower(topology: Topology, leader: i32, other_implementation: bool) -> i32 {
    let leader_is_ours = topology.rust_members().contains(&leader);
    (0..MEMBERS)
        .filter(|member| *member != leader)
        .find(|member| {
            !other_implementation || topology.rust_members().contains(member) != leader_is_ours
        })
        .unwrap_or_else(|| if leader == 0 { 1 } else { 0 })
}

/// Wait until the session follows a leader that is not `dead`, and answer it.
///
/// **The wait drives the session**, the way LC-1's failover case does: a client
/// that is not polled through a whole election is a client its own driver
/// reaps, and the failure that produces looks like a cluster that never
/// elected.
fn follow_a_new_leader(cluster: &TestCluster, session: &mut Session, dead: i32) -> i32 {
    let deadline = Instant::now() + DEADLINE;
    let mut new_leader = None;
    let followed = session.poll_until(deadline, |session| {
        if new_leader.is_none() {
            new_leader = cluster.leader_other_than(dead);
        }
        new_leader.is_some_and(|leader| leader == session.session.leader_member_id())
    });
    assert!(
        followed,
        "the session never followed a new leader {new_leader:?}: it says member {} leads\n{}",
        session.session.leader_member_id(),
        node_logs(cluster)
    );
    new_leader.expect("a leader the session followed")
}

/// Ask the cluster for a snapshot and wait until every member in `expected`
/// has taken one.
///
/// `expected` is not always every member: the catch-up case takes a snapshot
/// **while a member is away**, and that member's counter can never move.
fn take_a_snapshot(cluster: &TestCluster, session: &mut Session, expected: &[i32]) {
    assert!(
        session
            .session
            .send_admin_request_to_take_a_snapshot(&mut session.client, 42),
        "the session has an ingress publication to send the request on"
    );

    let deadline = Instant::now() + DEADLINE;
    assert!(
        session.poll_until(deadline, |session| {
            !session.received.borrow().admin_responses.is_empty()
        }),
        "the cluster never answered the admin request\n{}",
        node_logs(cluster)
    );
    let responses = session.received.borrow().admin_responses.clone();
    assert!(
        responses.iter().any(
            |response| response.request_type == AdminRequestType::SNAPSHOT
                && response.response_code == AdminResponseCode::OK
                && response.message.is_empty()
        ),
        "the cluster accepted the snapshot request: {responses:?}"
    );

    // The toggle is in the log, so every member takes its own snapshot when it
    // reaches that position — the leader first, the followers on their way
    // through.
    loop {
        let taken: Vec<Option<i64>> = expected
            .iter()
            .map(|member| snapshots_taken(cluster, *member))
            .collect();
        if taken
            .iter()
            .all(|count| count.is_some_and(|count| count >= 1))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "not every member took the snapshot: {taken:?}\n{}",
            node_logs(cluster)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Ten messages through the leader, and every member at the leader's position.
#[test]
fn the_matrix_echoes() {
    on_every_cell(|cell| {
        with_cluster(
            cell,
            "steady-echo",
            |cell| Leader::Everyone(cell.appointed()),
            |options, _| options,
            |cluster, leader, base| {
                let Some(mut session) = connect(&cell.name("steady-echo"), base, cluster) else {
                    cluster_mixed::skip("our client's driver");
                    return;
                };
                echo(&mut session, 0, 10);
                await_everyone_at_the_leaders_position(cluster, leader, &mut session);
            },
        );
    });
}

/// The leader dies; the survivors elect one of their own, the client follows,
/// and the dead member comes back to the new leader's position.
///
/// **No appointment here, and no leadership dimension in the cell**: a member
/// appointed to somebody else never nominates (`Election.java:715-718`), so a
/// cluster that is told who leads cannot fail over — telling only the intended
/// leader does not work either, because the members that were told nobody
/// nominate at the same instant and the race is whoever canvassed first. What
/// this scenario therefore asserts is the failover itself, on both topologies
/// and in both modes; **which implementation the leader was is whatever the
/// election produced**, and it is in the failure message when one runs.
#[test]
fn the_matrix_survives_a_leader_that_dies() {
    on_every_topology_and_mode(|cell| {
        with_cluster(
            cell,
            "kill-leader",
            |_| Leader::Nobody,
            |options, _| options,
            |cluster, leader, base| {
                let name = cell.name("kill-leader");
                let Some(mut session) = connect(&name, base, cluster) else {
                    cluster_mixed::skip("our client's driver");
                    return;
                };
                echo(&mut session, 0, 5);
                let term = leadership_term(cluster, leader).expect("the leader publishes its term");

                stop_member(cluster, leader);
                eprintln!(
                    "{name}: member {leader} ({}) died and the survivors elect",
                    if cell.topology.rust_members().contains(&leader) {
                        "ours"
                    } else {
                        "the reference's"
                    }
                );

                let new_leader = follow_a_new_leader(cluster, &mut session, leader);
                assert_ne!(
                    new_leader, leader,
                    "{name}: the new leader is a different member"
                );
                assert!(
                    session.session.leadership_term_id() > term,
                    "{name}: the new leader took a new term (the session says {}), above {term}\n{}",
                    session.session.leadership_term_id(),
                    node_logs(cluster)
                );

                echo(&mut session, 5, 10);
                let position = commit_position(cluster, new_leader)
                    .expect("the new leader publishes its commit position");

                start_member(cluster, leader);
                await_caught_up(cluster, leader, position, &mut session);
                echo(&mut session, 10, 12);
            },
        );
    });
}

/// A follower is away while the cluster commits, and comes back to the
/// leader's position.
#[test]
fn the_matrix_survives_a_follower_that_dies() {
    on_every_cell(|cell| {
        with_cluster(
            cell,
            "kill-follower",
            |cell| Leader::Everyone(cell.appointed()),
            |options, _| options,
            |cluster, leader, base| {
                let name = cell.name("kill-follower");
                let Some(mut session) = connect(&name, base, cluster) else {
                    cluster_mixed::skip("our client's driver");
                    return;
                };
                echo(&mut session, 0, 5);

                let follower = a_follower(cell.topology, leader, false);
                stop_member(cluster, follower);
                echo(&mut session, 5, 10);

                let position = commit_position(cluster, leader)
                    .expect("the leader publishes its commit position");
                start_member(cluster, follower);
                await_caught_up(cluster, follower, position, &mut session);

                echo(&mut session, 10, 12);
                await_everyone_at_the_leaders_position(cluster, leader, &mut session);
            },
        );
    });
}

/// The SNAPSHOT admin request: every member takes its own snapshot, and a
/// member restarted over its own directory comes back on it.
#[test]
fn the_matrix_takes_a_snapshot() {
    on_every_cell(|cell| {
        with_cluster(
            cell,
            "snapshot",
            |cell| Leader::Everyone(cell.appointed()),
            |options, _| cluster_mixed::allowing_every_command(options),
            |cluster, leader, base| {
                let name = cell.name("snapshot");
                let Some(mut session) = connect(&name, base, cluster) else {
                    cluster_mixed::skip("our client's driver");
                    return;
                };
                echo(&mut session, 0, 5);
                take_a_snapshot(cluster, &mut session, &[0, 1, 2]);

                let follower = a_follower(cell.topology, leader, false);
                stop_member(cluster, follower);
                let position = commit_position(cluster, leader)
                    .expect("the leader publishes its commit position");
                start_member(cluster, follower);
                await_caught_up(cluster, follower, position, &mut session);
                echo(&mut session, 5, 7);
            },
        );
    });
}

/// A follower is away **across a snapshot**: the position it is missing is one
/// its own log no longer covers, so coming back is the catch-up path.
#[test]
fn the_matrix_catches_a_follower_up_across_a_snapshot() {
    on_every_cell(|cell| {
        with_cluster(
            cell,
            "catchup",
            |cell| Leader::Everyone(cell.appointed()),
            |options, _| cluster_mixed::allowing_every_command(options),
            |cluster, leader, base| {
                let name = cell.name("catchup");
                let Some(mut session) = connect(&name, base, cluster) else {
                    cluster_mixed::skip("our client's driver");
                    return;
                };
                echo(&mut session, 0, 5);

                // The member whose implementation differs from the leader's,
                // where the topology has one: the crossing is the point.
                let follower = a_follower(cell.topology, leader, true);
                stop_member(cluster, follower);

                let still_here: Vec<i32> = (0..MEMBERS).filter(|id| *id != follower).collect();
                take_a_snapshot(cluster, &mut session, &still_here);
                echo(&mut session, 5, 10);

                let position = commit_position(cluster, leader)
                    .expect("the leader publishes its commit position");
                start_member(cluster, follower);
                await_caught_up(cluster, follower, position, &mut session);

                echo(&mut session, 10, 12);
                await_everyone_at_the_leaders_position(cluster, leader, &mut session);
            },
        );
    });
}

/// A follower is away for a few messages and the leader's log recording never
/// restarts: the returning member's own recording is what the leader's log
/// publication extends.
#[test]
fn the_matrix_replicates_to_a_follower_that_returns() {
    on_every_cell(|cell| {
        with_cluster(
            cell,
            "replication",
            |cell| Leader::Everyone(cell.appointed()),
            |options, _| options,
            |cluster, leader, base| {
                let name = cell.name("replication");
                let Some(mut session) = connect(&name, base, cluster) else {
                    cluster_mixed::skip("our client's driver");
                    return;
                };
                echo(&mut session, 0, 5);

                let follower = a_follower(cell.topology, leader, true);
                stop_member(cluster, follower);
                echo(&mut session, 5, 7);

                let position = commit_position(cluster, leader)
                    .expect("the leader publishes its commit position");
                start_member(cluster, follower);
                await_caught_up(cluster, follower, position, &mut session);

                echo(&mut session, 7, 10);
                await_everyone_at_the_leaders_position(cluster, leader, &mut session);
            },
        );
    });
}
