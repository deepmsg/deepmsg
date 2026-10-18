//! P3-6b's follower-path criteria: catchup, log replication, truncation and
//! re-election, on the three-node harness.
//!
//! The harness itself — three modules, three drivers, three archives, ports,
//! the client side — is [`deepmsg_tests::cluster_harness`]; P3-6a's criteria
//! live in `cluster_three_nodes.rs` and stay there as the regression.
//!
//! What separates these from the steady state is a **restart**: a module that
//! stopped keeps its cluster directory, its archive and its driver, so it comes
//! back to a log that is behind and has to be brought forward by the follower
//! paths (`Election.java:1020-1164`):
//!
//! * catchup — the leader replays its archive into the follower's catchup
//!   endpoint, which is the only way frames already published can arrive;
//! * log replication — the follower's log is older than the leader's term, so
//!   the gap is filled by an archive-to-archive replication;
//! * truncation — a term that was never committed is cut off.

use std::sync::mpsc;

use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_tests::cluster_harness::{
    ClientSide, Faults, MEMBER_COUNT, Node, NodeCounters, Ports, ROLE_FOLLOWER, announce_skip,
    await_launched, connect_request, ingress_message, members, offer_message, wait_until,
};

#[test]
fn a_stopped_follower_catches_up_through_the_leaders_archive() {
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
    await_launched(&faults, &launched_rx, MEMBER_COUNT);

    let counters: Vec<NodeCounters> = nodes
        .iter()
        .map(|node| NodeCounters::open(node.aeron_dir()))
        .collect();

    wait_until("every member's election to close", &faults, || {
        counters.iter().all(|node| {
            node.election_state() == Some(deepmsg_tests::cluster_harness::ELECTION_CLOSED)
        })
    });

    let leader_id = (0..MEMBER_COUNT)
        .find(|id| {
            counters[*id as usize].role() == Some(deepmsg_tests::cluster_harness::ROLE_LEADER)
        })
        .expect("a leader");
    let victim = (0..MEMBER_COUNT)
        .find(|id| *id != leader_id)
        .expect("a follower");

    // ---- a client session and three committed messages ---------------------
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

    let commit_before: Vec<Option<i64>> =
        counters.iter().map(NodeCounters::commit_position).collect();
    let target = commit_before.iter().flatten().copied().max().unwrap_or(0);

    for index in 0..3_u8 {
        let message = ingress_message(
            open_event.leadership_term_id,
            open_event.cluster_session_id,
            &[b'a' + index],
        );
        offer_message(&client, &faults, &message);
    }

    wait_until("the first three messages to commit", &faults, || {
        client.poll_events();
        counters[leader_id as usize]
            .commit_position()
            .is_some_and(|position| position > target)
    });

    // ---- the follower dies, the leader commits three more -----------------
    // Read the recording counter **before** the stop: closing the module stops
    // its recording, and the archive gives the recording-position counter back.
    wait_until("the follower's recording to start", &faults, || {
        client.poll_events();
        counters[victim as usize]
            .append_position()
            .is_some_and(|position| position > 0)
    });
    let victim_append_before = counters[victim as usize]
        .append_position()
        .expect("the follower recorded the first three messages");

    nodes[victim as usize].stop();

    let commit_at_stop = counters[leader_id as usize]
        .commit_position()
        .expect("the leader's commit");

    for index in 0..3_u8 {
        let message = ingress_message(
            open_event.leadership_term_id,
            open_event.cluster_session_id,
            &[b'p' + index],
        );
        offer_message(&client, &faults, &message);
    }

    wait_until(
        "the leader to commit while the follower is dead",
        &faults,
        || {
            client.poll_events();
            counters[leader_id as usize]
                .commit_position()
                .is_some_and(|position| position > commit_at_stop)
        },
    );
    let leader_commit = counters[leader_id as usize]
        .commit_position()
        .expect("the leader's commit after the kill");

    // ---- the follower comes back on the same directory and archive --------
    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    nodes[victim as usize].restart(&relaunch_tx);
    await_launched(&faults, &relaunch_rx, 1);

    // The only way the frames published while it was dead can reach it is the
    // leader's archive replay: a live subscription joins at the publisher's
    // current position, so it would leave the hole unfilled, and the election
    // rejects an image whose join position is not the log position it asked to
    // be replayed from (`Election.verifyLogJoinPosition`, `Election.java:1420`).
    // The target is what the leader had committed when the follower came
    // back; the leader may commit more meanwhile (an idle client session times
    // out and its close is logged), so the follower is asked to **reach** it,
    // not to stop on it.
    wait_until(
        "the restarted follower to commit what the leader committed",
        &faults,
        || {
            client.poll_events();
            counters[victim as usize]
                .commit_position()
                .is_some_and(|position| position >= leader_commit)
        },
    );

    let victim_append_after = counters[victim as usize]
        .append_position()
        .expect("the follower's recording");
    assert!(
        victim_append_after >= leader_commit,
        "the follower's log reached the leader's commit: {victim_append_after} < {leader_commit}"
    );
    assert!(
        victim_append_after > victim_append_before,
        "the recording moved forward from where it was stopped: \
         {victim_append_after} <= {victim_append_before}"
    );
    assert_eq!(
        Some(ROLE_FOLLOWER),
        counters[victim as usize].role(),
        "the restarted module is a follower again"
    );

    // And it is a live follower, not a caught-up corpse: a new commit reaches
    // it the ordinary way.
    let message = ingress_message(
        open_event.leadership_term_id,
        open_event.cluster_session_id,
        b"after catchup",
    );
    offer_message(&client, &faults, &message);

    wait_until(
        "a new message to reach the restarted follower",
        &faults,
        || {
            client.poll_events();
            counters[victim as usize]
                .commit_position()
                .is_some_and(|position| position > leader_commit)
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}
