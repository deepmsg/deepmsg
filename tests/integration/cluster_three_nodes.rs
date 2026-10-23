//! P3-6a's three-node harness: three consensus modules, each with its own
//! driver and archive, forming one cluster over UDP.
//!
//! This is the instrument the card's DoD names — **three pure-Rust nodes elect
//! a leader, commit, and keep committing when a follower dies**. It is not
//! LC-2 (that runs the reference's own cluster tests against this build); it is
//! the smallest cluster this build can stand up by itself, so that every piece
//! of wiring P3-6a added (`ConsensusPublisher`, the multi-member log
//! destinations, the follower's append-position heartbeat) is exercised by
//! something other than a unit test.
//!
//! The topology, the port scheme and the client side live in
//! [`deepmsg_tests::cluster_harness`], which P3-6b's follower-path criteria
//! share.

use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_tests::cluster_harness::{
    ClientSide, ELECTION_CLOSED, Faults, MEMBER_COUNT, Node, NodeCounters, Ports, ROLE_FOLLOWER,
    ROLE_LEADER, announce_skip, await_launched, connect_request, ingress_message, members,
    offer_message, wait_until,
};
use std::sync::mpsc;

/// P3-6a's DoD, end to end.
///
/// One test rather than several: the three nodes share a port allocation, and
/// this binary's tests run in parallel threads of one process. The phases are
/// the card's sentences — elect, commit, and commit while a follower is dead.
#[test]
fn three_nodes_elect_commit_and_keep_committing_when_a_follower_dies() {
    let ports = Ports::new();
    let member_string = members(&ports);
    let faults = Faults::default();
    let (launched_tx, launched_rx) = mpsc::channel();

    let mut nodes = Vec::new();
    for id in 0..MEMBER_COUNT {
        let Some(node) = Node::start(id, &ports, &member_string, &faults, &launched_tx) else {
            announce_skip();
            return;
        };
        nodes.push(node);
    }

    // ---- launched: every `launch` came back -------------------------------
    await_launched(&faults, &launched_rx, MEMBER_COUNT);

    let counters: Vec<NodeCounters> = nodes
        .iter()
        .map(|node| NodeCounters::open(node.aeron_dir()))
        .collect();

    // ---- C1/C2/C3: one closed election, one term, one leader --------------
    wait_until("every member's election to close", &faults, || {
        counters
            .iter()
            .all(|node| node.election_state() == Some(ELECTION_CLOSED))
    });

    let terms: Vec<Option<i64>> = counters
        .iter()
        .map(NodeCounters::leadership_term_id)
        .collect();
    let term = terms[0].expect("the first member's leadership term");
    assert!(term >= 0, "a leadership term was established: {terms:?}");
    assert!(
        terms.iter().all(|value| *value == Some(term)),
        "the members agree on the leadership term: {terms:?}"
    );

    let roles: Vec<Option<i64>> = counters.iter().map(NodeCounters::role).collect();
    assert_eq!(
        1,
        roles
            .iter()
            .filter(|role| **role == Some(ROLE_LEADER))
            .count(),
        "exactly one leader: {roles:?}"
    );
    assert_eq!(
        MEMBER_COUNT as usize - 1,
        roles
            .iter()
            .filter(|role| **role == Some(ROLE_FOLLOWER))
            .count(),
        "everyone else is a follower: {roles:?}"
    );
    let leader_id = roles
        .iter()
        .position(|role| *role == Some(ROLE_LEADER))
        .expect("a leader") as i32;

    // ---- C4: a client opens a session through the leader's ingress --------
    let Some(mut client) = ClientSide::start(&ports, leader_id) else {
        announce_skip();
        return;
    };

    let connect = connect_request(1, &client.egress_channel);
    offer_message(&client, &faults, &connect);

    let mut open_event = None;
    wait_until("the session open event", &faults, || {
        for event in client.poll_events() {
            if event.correlation_id == 1 && event.code == EventCode::OK {
                open_event = Some(event);
            }
        }
        open_event.is_some()
    });
    let open_event = open_event.expect("the session opened");

    // ---- C5: three messages, committed on every member --------------------
    let before: Vec<Option<i64>> = counters.iter().map(NodeCounters::commit_position).collect();

    for index in 0..3_u8 {
        let message = ingress_message(
            open_event.leadership_term_id,
            open_event.cluster_session_id,
            &[b'0' + index],
        );
        offer_message(&client, &faults, &message);
    }

    let target = before.iter().flatten().copied().max().unwrap_or(0);
    wait_until("the leader to commit the three messages", &faults, || {
        client.poll_events();
        counters[leader_id as usize]
            .commit_position()
            .is_some_and(|position| position > target)
    });

    // `P3-6b` gives each follower a log adapter, so its **commit** counter —
    // not just its recording — reaches what the leader committed: the same
    // position on all three (`ConsensusModuleAgent.java:2526-2532`).
    //
    // Two things this wait does that the first version of it did not:
    //
    // * the leader's value is read **inside** the loop. A value taken once and
    //   then compared with `==` is a snapshot the leader can move past, and a
    //   position nobody is at any more never becomes true again — a hang, not a
    //   slow follower;
    // * the failure carries every member's counters. This wait went red in CI
    //   on 2026-10-23 with the bare `timed out waiting for …`, and there was no
    //   way to tell a follower that never applied from one that was a record
    //   behind.
    let deadline = std::time::Instant::now() + deepmsg_tests::cluster_harness::DEADLINE;
    let mut agreed = false;
    while std::time::Instant::now() < deadline {
        faults.check();
        client.poll_events();
        let leader_now = counters[leader_id as usize].commit_position();
        if leader_now.is_some_and(|position| {
            position > target
                && counters
                    .iter()
                    .all(|node| node.commit_position() == Some(position))
        }) {
            agreed = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(
        agreed,
        "every member's commit counter to reach the leader's: roles={:?} terms={:?} \
         append={:?} commit={:?} (the leader was {leader_id}, target {target})",
        counters.iter().map(NodeCounters::role).collect::<Vec<_>>(),
        counters
            .iter()
            .map(NodeCounters::leadership_term_id)
            .collect::<Vec<_>>(),
        counters
            .iter()
            .map(NodeCounters::append_position)
            .collect::<Vec<_>>(),
        counters
            .iter()
            .map(NodeCounters::commit_position)
            .collect::<Vec<_>>(),
    );
    let leader_commit = counters[leader_id as usize]
        .commit_position()
        .expect("the leader's commit position");
    assert!(leader_commit > target);

    // ---- C6: kill a follower; the leader keeps committing -----------------
    let victim = (0..MEMBER_COUNT)
        .find(|id| *id != leader_id)
        .expect("a follower to stop");
    nodes[victim as usize].stop();
    assert!(nodes[victim as usize].is_stopped());
    let victim_append = counters[victim as usize].append_position();

    let before_kill = leader_commit;
    for index in 0..3_u8 {
        let message = ingress_message(
            open_event.leadership_term_id,
            open_event.cluster_session_id,
            &[b'a' + index],
        );
        offer_message(&client, &faults, &message);
    }

    wait_until(
        "the commit position to move with a follower stopped",
        &faults,
        || {
            client.poll_events();
            counters[leader_id as usize]
                .commit_position()
                .is_some_and(|position| position > before_kill)
        },
    );

    // The surviving follower keeps recording what the leader commits, which
    // is the append position the quorum was recomputed from; the dead one's
    // recording stays where it was, which is what makes this a kill rather
    // than a clean stop on all three.
    let survivor = (0..MEMBER_COUNT)
        .find(|id| *id != leader_id && *id != victim)
        .expect("the surviving follower");
    let leader_after_kill = counters[leader_id as usize]
        .commit_position()
        .expect("the leader's commit position");
    wait_until(
        "the surviving follower's recording to keep up",
        &faults,
        || {
            counters[survivor as usize]
                .append_position()
                .is_some_and(|position| position >= leader_after_kill)
        },
    );
    assert_eq!(
        victim_append,
        counters[victim as usize].append_position(),
        "the killed follower's recording did not move"
    );

    // ---- teardown ---------------------------------------------------------
    for node in &mut nodes {
        node.stop();
    }
    let _ = &nodes;
}
