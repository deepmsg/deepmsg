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
use std::time::{Duration, Instant};

use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_tests::cluster_harness::{
    ClientSide, DEADLINE, Faults, MEMBER_COUNT, Node, NodeCounters, Ports, ROLE_FOLLOWER,
    announce_skip, await_launched, connect_request, ingress_message, members, offer_message,
    wait_until,
};

/// [`wait_until`] for a condition a timeout has to explain: the panic carries
/// what the counters were saying when the deadline passed.
fn wait_with_state(
    what: &str,
    faults: &Faults,
    condition: impl Fn() -> bool,
    state: impl Fn() -> String,
) {
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        faults.check();
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("timed out waiting for {what}; last seen: {}", state());
}

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

/// H4 — a member that missed a term is caught up by **log replication**.
///
/// The shape is `ClusterTest.shouldRecoverWhenFollowerIsMultipleTermsBehind`
/// (`:1419`), with three members instead of four: the follower dies, the
/// cluster moves to a new term without it, and it comes back to a log whose
/// gap starts **before** the new term — `appendPosition < termBaseLogPosition`
/// — so a replay cannot fill it and the election takes the archive-to-archive
/// replication (`Election.java:897-958`).
///
/// The reference's `useResponseChannels=true` arm is `n/a:0e` here:
/// response-mode replication is the archive client's, and it is not in this
/// build.
#[test]
fn a_member_that_missed_a_term_is_caught_up_by_replication() {
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

    wait_until("the first election to close", &faults, || {
        counters.iter().all(|node| {
            node.election_state() == Some(deepmsg_tests::cluster_harness::ELECTION_CLOSED)
        })
    });

    let first_leader = (0..MEMBER_COUNT)
        .find(|id| {
            counters[*id as usize].role() == Some(deepmsg_tests::cluster_harness::ROLE_LEADER)
        })
        .expect("a leader");
    let first_term = counters[first_leader as usize]
        .leadership_term_id()
        .expect("the first term");
    let victim = (0..MEMBER_COUNT)
        .find(|id| *id != first_leader)
        .expect("a follower");
    let survivor = (0..MEMBER_COUNT)
        .find(|id| *id != first_leader && *id != victim)
        .expect("the other follower");

    // ---- a client session and three committed messages ---------------------
    let Some(mut client) = ClientSide::start(&ports, first_leader) else {
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

    let target = counters[first_leader as usize]
        .commit_position()
        .expect("the leader's commit");
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
        counters[first_leader as usize]
            .commit_position()
            .is_some_and(|position| position > target)
    });

    // ---- the follower dies; the leader commits three more -----------------
    wait_until("the follower's recording to start", &faults, || {
        client.poll_events();
        counters[victim as usize]
            .append_position()
            .is_some_and(|position| position > 0)
    });
    let victim_append_before = counters[victim as usize]
        .append_position()
        .expect("the follower's recording");
    nodes[victim as usize].stop();

    let commit_at_stop = counters[first_leader as usize]
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
    wait_until("the leader to commit without the follower", &faults, || {
        client.poll_events();
        counters[first_leader as usize]
            .commit_position()
            .is_some_and(|position| position > commit_at_stop)
    });

    // ---- the leader dies and comes back, so a new term can be elected -----
    // With three members, the two that remain are a quorum; the stopped one
    // has to come back for the cluster to move on, and the term it elects
    // begins above the dead follower's append position.
    nodes[first_leader as usize].stop();
    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    nodes[first_leader as usize].restart(&relaunch_tx);
    await_launched(&faults, &relaunch_rx, 1);

    let survivors = [first_leader, survivor];
    wait_until("the two survivors to elect a new term", &faults, || {
        survivors.iter().all(|id| {
            counters[*id as usize].election_state()
                == Some(deepmsg_tests::cluster_harness::ELECTION_CLOSED)
                && counters[*id as usize]
                    .leadership_term_id()
                    .is_some_and(|term| term > first_term)
        })
    });
    let new_leader = survivors
        .iter()
        .copied()
        .find(|id| {
            counters[*id as usize].role() == Some(deepmsg_tests::cluster_harness::ROLE_LEADER)
        })
        .expect("a new leader among the survivors");
    let new_term = counters[new_leader as usize]
        .leadership_term_id()
        .expect("the new term");

    // ---- the member that missed the term comes back -----------------------
    let (later_tx, later_rx) = mpsc::channel();
    nodes[victim as usize].restart(&later_tx);
    await_launched(&faults, &later_rx, 1);

    wait_with_state(
        "the returning member to be caught up by replication",
        &faults,
        || {
            counters[victim as usize].election_state()
                == Some(deepmsg_tests::cluster_harness::ELECTION_CLOSED)
                && counters[victim as usize].leadership_term_id() == Some(new_term)
                && counters[victim as usize].commit_position().is_some()
        },
        || {
            format!(
                "victim election={:?} term={:?} commit={:?} append={:?}, expected term={new_term}",
                counters[victim as usize].election_state(),
                counters[victim as usize].leadership_term_id(),
                counters[victim as usize].commit_position(),
                counters[victim as usize].append_position(),
            )
        },
    );

    let leader_commit = counters[new_leader as usize]
        .commit_position()
        .expect("the new leader's commit");
    assert!(
        counters[victim as usize]
            .commit_position()
            .is_some_and(|position| position >= leader_commit)
            || counters[victim as usize]
                .append_position()
                .is_some_and(|position| position >= leader_commit),
        "the returning member reached the new term's position"
    );
    assert!(
        counters[victim as usize]
            .append_position()
            .is_some_and(|position| position > victim_append_before),
        "and its recording moved past where it stopped"
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// H2 — the leader dies and the survivors elect a new term.
///
/// The two followers must form a quorum of their own (`Election`), open a new
/// term, and take a client's messages on the new leader's ingress. The dead
/// leader's counters go with its module: `close` gives them back, which is what
/// makes them the evidence that the old term really ended there.
#[test]
fn the_surviving_members_elect_a_new_leader_and_keep_committing() {
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

    wait_until("the first election to close", &faults, || {
        counters.iter().all(|node| {
            node.election_state() == Some(deepmsg_tests::cluster_harness::ELECTION_CLOSED)
        })
    });

    let first_leader = (0..MEMBER_COUNT)
        .find(|id| {
            counters[*id as usize].role() == Some(deepmsg_tests::cluster_harness::ROLE_LEADER)
        })
        .expect("a leader");
    let first_term = counters[first_leader as usize]
        .leadership_term_id()
        .expect("the first term");

    // A session through the first leader, so the old term has committed
    // something a client asked for.
    {
        let Some(mut client) = ClientSide::start(&ports, first_leader) else {
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

        let before = counters[first_leader as usize]
            .commit_position()
            .expect("the leader's commit");
        let message = ingress_message(
            open_event.leadership_term_id,
            open_event.cluster_session_id,
            b"before the leader died",
        );
        offer_message(&client, &faults, &message);

        wait_until("the first leader to commit a message", &faults, || {
            client.poll_events();
            counters[first_leader as usize]
                .commit_position()
                .is_some_and(|position| position > before)
        });
    }

    // ---- the leader dies ---------------------------------------------------
    nodes[first_leader as usize].stop();
    let survivors: Vec<i32> = (0..MEMBER_COUNT).filter(|id| *id != first_leader).collect();

    wait_until("the survivors to elect a new term", &faults, || {
        survivors.iter().all(|id| {
            counters[*id as usize].election_state()
                == Some(deepmsg_tests::cluster_harness::ELECTION_CLOSED)
                && counters[*id as usize]
                    .leadership_term_id()
                    .is_some_and(|term| term > first_term)
        })
    });

    let new_leader = survivors
        .iter()
        .copied()
        .find(|id| {
            counters[*id as usize].role() == Some(deepmsg_tests::cluster_harness::ROLE_LEADER)
        })
        .expect("a new leader among the survivors");
    assert_eq!(
        1,
        survivors
            .iter()
            .filter(|id| {
                counters[**id as usize].role() == Some(deepmsg_tests::cluster_harness::ROLE_LEADER)
            })
            .count(),
        "exactly one new leader"
    );
    let new_term = counters[new_leader as usize]
        .leadership_term_id()
        .expect("the new term");
    assert!(
        new_term > first_term,
        "a new term: {new_term} <= {first_term}"
    );

    // The old module's counters are gone, which is what `close` does; the
    // commit position it had published is no longer anywhere in that driver.
    assert!(
        counters[first_leader as usize].commit_position().is_none(),
        "the dead leader's commit counter went with its module"
    );

    // ---- a client commits on the new leader -------------------------------
    let Some(mut client) = ClientSide::start(&ports, new_leader) else {
        announce_skip();
        return;
    };
    let connect = connect_request(2, &client.egress_channel);
    offer_message(&client, &faults, &connect);

    let mut open_event = None;
    wait_until("the session on the new leader", &faults, || {
        for event in client.poll_events() {
            if event.correlation_id == 2 && event.code == EventCode::OK {
                open_event = Some(event);
            }
        }
        open_event.is_some()
    });
    let open_event = open_event.expect("the session opened on the new leader");
    assert_eq!(
        new_term, open_event.leadership_term_id,
        "the session was opened in the new term"
    );

    let before = counters[new_leader as usize]
        .commit_position()
        .expect("the new leader's commit");
    let message = ingress_message(
        open_event.leadership_term_id,
        open_event.cluster_session_id,
        b"after the election",
    );
    offer_message(&client, &faults, &message);

    wait_until("the new leader to commit", &faults, || {
        client.poll_events();
        counters[new_leader as usize]
            .commit_position()
            .is_some_and(|position| position > before)
    });
    wait_until("the surviving follower to commit", &faults, || {
        client.poll_events();
        survivors.iter().filter(|id| **id != new_leader).all(|id| {
            counters[*id as usize]
                .commit_position()
                .is_some_and(|position| position > before)
        })
    });

    for node in &mut nodes {
        node.stop();
    }
}

/// H6 — an orderly `SHUTDOWN` ends the followers too.
///
/// The operator's shutdown is a counter: the leader's cluster control toggle
/// set to `SHUTDOWN(5)` (`ClusterControl.ToggleState`, `:43-180`). The leader
/// snapshots, publishes the termination position, and every member that
/// reaches it acknowledges and ends its duty cycle
/// (`CMA:2502-2512`, `:3319-3336`) — P3-4e's leader half and this slice's
/// follower half.
#[test]
fn an_orderly_shutdown_ends_every_members_duty_cycle() {
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

    wait_until("the first election to close", &faults, || {
        counters.iter().all(|node| {
            node.election_state() == Some(deepmsg_tests::cluster_harness::ELECTION_CLOSED)
        })
    });

    let leader = (0..MEMBER_COUNT)
        .find(|id| {
            counters[*id as usize].role() == Some(deepmsg_tests::cluster_harness::ROLE_LEADER)
        })
        .expect("a leader");
    counters[leader as usize].set(
        deepmsg_cluster_common::counters::CLUSTER_CONTROL_TOGGLE_TYPE_ID,
        i64::from(deepmsg_cluster::control::cluster_control::ToggleState::Shutdown.code()),
    );

    // Every member's duty cycle ends by itself — a follower that does not
    // reach the termination position keeps running, and the wait then reports
    // which ones did not.
    wait_with_state(
        "every member's duty cycle to end",
        &faults,
        || nodes.iter().all(Node::has_finished),
        || {
            format!(
                "finished={:?} states={:?}",
                nodes.iter().map(Node::has_finished).collect::<Vec<_>>(),
                counters
                    .iter()
                    .map(NodeCounters::election_state)
                    .collect::<Vec<_>>(),
            )
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}
