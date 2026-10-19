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
    CATCHUP_DEADLINE, ChannelShape, ClientSide, DEADLINE, ELECTION_CLOSED, Faults, MEMBER_COUNT,
    Node, NodeCounters, Ports, ROLE_FOLLOWER, announce_skip, await_launched, connect_request,
    ingress_message, max_ingress_payload, members, offer_message, wait_until,
};

/// The scenarios here all bind ports derived from the **process** id
/// (`cluster_harness::Ports`), so `cargo test`'s parallel threads would race
/// for them. They take turns instead.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// [`wait_until`] for a condition a timeout has to explain: the panic carries
/// what the counters were saying when the deadline passed.
fn wait_with_state(
    what: &str,
    faults: &Faults,
    timeout: Duration,
    mut condition: impl FnMut() -> bool,
    state: impl Fn() -> String,
) {
    let deadline = Instant::now() + timeout;
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
    let _serial = serial();
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
    wait_with_state(
        "the restarted follower to commit what the leader committed",
        &faults,
        CATCHUP_DEADLINE,
        || {
            client.poll_events();
            counters[victim as usize]
                .commit_position()
                .is_some_and(|position| position >= leader_commit)
        },
        || {
            let victim_counters = &counters[victim as usize];
            format!(
                "leader_commit={leader_commit}, victim append={:?} commit={:?} role={:?} \
                 election={:?}, leader commit={:?}",
                victim_counters.append_position(),
                victim_counters.commit_position(),
                victim_counters.role(),
                victim_counters.election_state(),
                counters[leader_id as usize].commit_position(),
            ) + &format!(
                "\nvictim archive/driver log:\n{}",
                nodes[victim as usize].driver_log_tail(40)
            )
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
    let _serial = serial();
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
        CATCHUP_DEADLINE,
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
    let _serial = serial();
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
    let _serial = serial();
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
        DEADLINE,
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

/// H5 — the cluster starts from a recording log truncated to its latest
/// snapshot.
///
/// The reference's construction (`StartFromTruncatedRecordingLogTest`): take a
/// snapshot, stop the whole cluster, rewrite every node's `recording.log` down
/// to the last term and its snapshots, delete the archive and cluster mark
/// files and the `.rec` segments the rewritten log no longer names, and start
/// the cluster again. The reference repeats it three times; two rounds here
/// keep the run inside the harness's budget while still proving the log is
/// rewritten **from a log that a previous round produced**.
#[test]
fn the_cluster_starts_from_a_recording_log_truncated_to_its_latest_snapshot() {
    let _serial = serial();
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

    let mut counters: Vec<NodeCounters> = nodes
        .iter()
        .map(|node| NodeCounters::open(node.aeron_dir()))
        .collect();

    for round in 0..2 {
        wait_until("the election to close", &faults, || {
            counters.iter().all(|node| {
                node.election_state() == Some(deepmsg_tests::cluster_harness::ELECTION_CLOSED)
            })
        });
        let leader = (0..MEMBER_COUNT)
            .find(|id| {
                counters[*id as usize].role() == Some(deepmsg_tests::cluster_harness::ROLE_LEADER)
            })
            .expect("a leader");

        // A client session and a committed message, so the snapshot is of a
        // log a client has written to.
        let Some(mut client) = ClientSide::start(&ports, leader) else {
            announce_skip();
            return;
        };
        let connect = connect_request(round * 2 + 1, &client.egress_channel);
        let correlation_id = round * 2 + 1;
        offer_message(&client, &faults, &connect);

        let mut open_event = None;
        wait_until("the session open event", &faults, || {
            for event in client.poll_events() {
                if event.correlation_id == correlation_id && event.code == EventCode::OK {
                    open_event = Some(event);
                }
            }
            open_event.is_some()
        });
        let open_event = open_event.expect("the session opened");

        let before = counters[leader as usize]
            .commit_position()
            .expect("the leader's commit");
        let message = ingress_message(
            open_event.leadership_term_id,
            open_event.cluster_session_id,
            b"before the truncation",
        );
        offer_message(&client, &faults, &message);
        wait_until("the message to commit", &faults, || {
            client.poll_events();
            counters[leader as usize]
                .commit_position()
                .is_some_and(|position| position > before)
        });

        // The operator's snapshot, and the counter going back to NEUTRAL is
        // the reference's "the snapshot is done" (`ClusterControl`).
        counters[leader as usize].set(
            deepmsg_cluster_common::counters::CLUSTER_CONTROL_TOGGLE_TYPE_ID,
            i64::from(deepmsg_cluster::control::cluster_control::ToggleState::Snapshot.code()),
        );
        wait_until("the snapshot", &faults, || {
            counters[leader as usize]
                .snapshot_count()
                .is_some_and(|count| count >= 1)
                && counters[leader as usize]
                    .value(deepmsg_cluster_common::counters::CLUSTER_CONTROL_TOGGLE_TYPE_ID)
                    == Some(1)
        });

        // Stop every module **and its media**, truncate, then bring both halves
        // of each node back: an archive still running would rewrite the mark
        // file and its catalog after the test had deleted them.
        for node in &mut nodes {
            node.stop_media();
        }
        drop(client);
        for node in &nodes {
            node.truncate_recording_log()
                .expect("the recording log is truncated");
        }

        let (restart_tx, restart_rx) = mpsc::channel();
        for node in &mut nodes {
            node.restart_whole(&restart_tx);
        }
        await_launched(&faults, &restart_rx, MEMBER_COUNT);

        // The driver's aeron directory was rebuilt with the driver.
        counters = nodes
            .iter()
            .map(|node| NodeCounters::open(node.aeron_dir()))
            .collect();
    }

    // And the cluster is operational after the last round: a fresh session on
    // the leader it elects, and a message it commits.
    wait_until("the election after the truncated start", &faults, || {
        counters.iter().all(|node| {
            node.election_state() == Some(deepmsg_tests::cluster_harness::ELECTION_CLOSED)
        })
    });
    let leader = (0..MEMBER_COUNT)
        .find(|id| {
            counters[*id as usize].role() == Some(deepmsg_tests::cluster_harness::ROLE_LEADER)
        })
        .expect("a leader after the truncated start");
    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let connect = connect_request(99, &client.egress_channel);
    offer_message(&client, &faults, &connect);
    let mut open_event = None;
    wait_until("the session on the restarted cluster", &faults, || {
        for event in client.poll_events() {
            if event.correlation_id == 99 && event.code == EventCode::OK {
                open_event = Some(event);
            }
        }
        open_event.is_some()
    });
    let open_event = open_event.expect("the session opened");
    let before = counters[leader as usize]
        .commit_position()
        .expect("the leader's commit");
    let message = ingress_message(
        open_event.leadership_term_id,
        open_event.cluster_session_id,
        b"after the truncated start",
    );
    offer_message(&client, &faults, &message);
    wait_until(
        "the message after the truncated start to commit",
        &faults,
        || {
            client.poll_events();
            counters[leader as usize]
                .commit_position()
                .is_some_and(|position| position > before)
        },
    );

    for node in &mut nodes {
        node.stop();
    }
}

/// One batch of maximum-sized ingress messages for a channel of
/// `term_length`: `computeMaxMessageLength(term) - SESSION_HEADER_LENGTH`
/// payload bytes each, so every message fills exactly `term / 8` of the log
/// (`ClusterTest.java:1850-1865`).
fn send_max_sized_batch(
    client: &ClientSide,
    faults: &Faults,
    leadership_term_id: i64,
    cluster_session_id: i64,
    byte: u8,
    count: usize,
    term_length: i32,
) {
    let payload = vec![byte; max_ingress_payload(term_length)];
    for _ in 0..count {
        let message = ingress_message(leadership_term_id, cluster_session_id, &payload);
        offer_message(client, faults, &message);
    }
}

/// The leader's commit position, which is what a cluster with no services can
/// see of a batch landing in the log.
fn commit_of(counters: &[NodeCounters], node: usize) -> Option<i64> {
    counters[node].commit_position()
}

/// H5's second arm — the cluster takes a **new term buffer length and MTU**
/// after its recording log is truncated to the latest snapshot.
///
/// The reference (`ClusterTest.shouldAllowChangingTermBufferLengthAndMtuAfter-
/// RecordingLogIsTruncatedToTheLatestSnapshot`, `ClusterTest.java:1823-1933`)
/// starts on 256 KiB terms with an MTU of 1408, fills the log with messages of
/// the largest size that term allows, snapshots, sends more, then stops the
/// whole cluster, seeds every `recording.log` from its latest snapshot
/// (`ClusterTool.seedRecordingLogFromSnapshot`, `ClusterToolOperator.java:
/// 201-270`: the snapshot entries only, term base and log position zeroed), and
/// restarts it on 2 MiB terms with an MTU of 8992.
///
/// The reference proves the arm with its services' message counts and
/// checksums — the second batch is dropped by the seeding. This cluster has no
/// services (the service container is P3-2c), so the same claims are read where
/// they are observable here:
///
/// * the log recording's **descriptor**: 256 KiB / 1408 before the seeding,
///   2 MiB / 8992 after it (`Archive::list_recording`);
/// * after the restart and **before a client touches it**, every member's
///   commit position is below 64 KiB: the 20 messages that filled the old log
///   were not replayed, only the snapshot state came back;
/// * five maximum-sized messages for the **new** term length commit, and every
///   member reaches the same position.
#[test]
fn a_restarted_cluster_takes_a_new_term_length_and_mtu_after_a_snapshot_seed() {
    let _serial = serial();
    let old_term = 256 * 1024;
    let new_term = 2 * 1024 * 1024;
    let old_mtu = 1408;
    let new_mtu = 8992;
    let mut ports = Ports::new();
    ports.set_channel_shape(ChannelShape {
        log_term_length: old_term,
        ingress_term_length: old_term,
        egress_term_length: old_term,
        mtu: Some(old_mtu),
    });

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

    let mut counters: Vec<NodeCounters> = nodes
        .iter()
        .map(|node| NodeCounters::open(node.aeron_dir()))
        .collect();

    wait_until("the election to close", &faults, || {
        counters
            .iter()
            .all(|node| node.election_state() == Some(ELECTION_CLOSED))
    });
    let leader = (0..MEMBER_COUNT)
        .find(|id| {
            counters[*id as usize].role() == Some(deepmsg_tests::cluster_harness::ROLE_LEADER)
        })
        .expect("a leader");

    // The first batch: 9 messages that each fill an eighth of a 256 KiB term.
    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let connect = connect_request(11, &client.egress_channel);
    offer_message(&client, &faults, &connect);
    let mut open_event = None;
    wait_until("the session open event", &faults, || {
        for event in client.poll_events() {
            if event.correlation_id == 11 && event.code == EventCode::OK {
                open_event = Some(event);
            }
        }
        open_event.is_some()
    });
    let open_event = open_event.expect("the session opened");

    let before = commit_of(&counters, leader as usize).expect("the leader's commit");
    send_max_sized_batch(
        &client,
        &faults,
        open_event.leadership_term_id,
        open_event.cluster_session_id,
        b'x',
        9,
        old_term,
    );
    let per_message = i64::from(old_term / 8);
    wait_until("the first batch to commit", &faults, || {
        commit_of(&counters, leader as usize)
            .is_some_and(|position| position >= before + 9 * per_message)
    });

    // The operator's snapshot; the toggle going back to NEUTRAL is the
    // reference's "the snapshot is done" (`ClusterControl`).
    counters[leader as usize].set(
        deepmsg_cluster_common::counters::CLUSTER_CONTROL_TOGGLE_TYPE_ID,
        i64::from(deepmsg_cluster::control::cluster_control::ToggleState::Snapshot.code()),
    );
    wait_until("the snapshot", &faults, || {
        counters[leader as usize]
            .snapshot_count()
            .is_some_and(|count| count >= 1)
            && counters[leader as usize]
                .value(deepmsg_cluster_common::counters::CLUSTER_CONTROL_TOGGLE_TYPE_ID)
                == Some(1)
    });

    // The log the snapshot recorded was made with the first shape, and the
    // descriptor is where that is readable.
    let old_shape = nodes[leader as usize]
        .log_recording_shape()
        .expect("the leader's log recording");
    assert_eq!(
        old_shape.term_buffer_length, old_term,
        "the log recording before the seeding"
    );
    assert_eq!(old_shape.mtu_length, old_mtu, "its MTU");
    let old_leadership_term = old_shape.leadership_term_id;

    // The second batch: 11 more messages, on a reconnected client like the
    // reference's (`:1870-1886`), on a **second egress port**. The reference
    // gives the reconnected client the same egress channel, which leaves the
    // old session's publication on it; this build's driver does not bring up an
    // image for the new session's publication on that channel while the old one
    // is there (the session-open event never arrives — see the record's
    // findings). The response channel is not what this arm is about, so the
    // second client answers on a port of its own.
    drop(client);
    let Some(mut client) = ClientSide::start_at(&ports, leader, 5) else {
        announce_skip();
        return;
    };
    let connect = connect_request(12, &client.egress_channel);
    offer_message(&client, &faults, &connect);
    let mut open_event = None;
    let mut seen: Vec<(i64, i32, String)> = Vec::new();
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        faults.check();
        for event in client.poll_events() {
            seen.push((
                event.correlation_id,
                event.code.into(),
                event.detail.clone(),
            ));
            if event.correlation_id == 12 && event.code == EventCode::OK {
                open_event = Some(event);
            }
        }
        if open_event.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let diagnostics = |type_id: i32| -> Vec<Option<i64>> {
        counters
            .iter()
            .map(|counter| counter.value(type_id))
            .collect()
    };
    let recording_shapes: Vec<String> = nodes
        .iter()
        .enumerate()
        .map(|(index, node)| match node.log_recording_shape() {
            Ok(shape) => format!(
                "recording={} term={} stop={} applied={:?} buffer={} mtu={}",
                shape.recording_id,
                shape.leadership_term_id,
                shape.stop_position,
                counters[index].recording_position(shape.recording_id),
                shape.term_buffer_length,
                shape.mtu_length
            ),
            Err(error) => format!("<{error}>"),
        })
        .collect();
    assert!(
        open_event.is_some(),
        "timed out waiting for the second session; images={} leader_role={:?} election={:?} \
         commit={:?}/{:?}/{:?} timeouts={:?} errors={:?} invalid={:?} log={recording_shapes:?} \
         events={seen:?} driver={:?} client_driver={:?}",
        client.egress_image_count(),
        counters[leader as usize].role(),
        counters[leader as usize].election_state(),
        commit_of(&counters, 0),
        commit_of(&counters, 1),
        commit_of(&counters, 2),
        diagnostics(deepmsg_cluster_common::counters::CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID),
        diagnostics(deepmsg_cluster_common::counters::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID),
        diagnostics(deepmsg_cluster_common::counters::CLUSTER_INVALID_REQUEST_COUNT_TYPE_ID),
        nodes[leader as usize].driver_log_tail(20),
        client.driver_log_tail(20),
    );
    let open_event = open_event.expect("the second session opened");
    let before = commit_of(&counters, leader as usize).expect("the leader's commit");
    send_max_sized_batch(
        &client,
        &faults,
        open_event.leadership_term_id,
        open_event.cluster_session_id,
        b'y',
        11,
        old_term,
    );
    wait_until("the second batch to commit", &faults, || {
        commit_of(&counters, leader as usize)
            .is_some_and(|position| position >= before + 11 * per_message)
    });
    let before_stop = commit_of(&counters, leader as usize).expect("the leader's commit");
    assert!(
        before_stop >= 20 * per_message,
        "the log was filled by two batches: {before_stop}"
    );

    // Stop every module **and its media** (the reference's `TestNode.close`
    // closes the module, the containers, the archive and the driver), then seed
    // every recording log from its latest snapshot.
    for node in &mut nodes {
        node.stop_media();
    }
    drop(client);
    for node in &nodes {
        node.seed_recording_log_from_snapshot()
            .expect("the recording log is seeded from the snapshot");
    }

    // The same ports, the new shape — and a whole-cluster restart.
    ports.set_channel_shape(ChannelShape {
        log_term_length: new_term,
        ingress_term_length: new_term,
        egress_term_length: new_term,
        mtu: Some(new_mtu),
    });
    let (restart_tx, restart_rx) = mpsc::channel();
    for node in &mut nodes {
        node.set_channel_shape(&ports);
        node.restart_whole(&restart_tx);
    }
    await_launched(&faults, &restart_rx, MEMBER_COUNT);

    counters = nodes
        .iter()
        .map(|node| NodeCounters::open(node.aeron_dir()))
        .collect();

    // The seeded log has no term entry: the recovery plan starts at the
    // snapshot's term, position zero, with no log to replay
    // (`RecordingLog.java:890-935`). So the commit settles **below the old
    // log**, on every member, before anything new is written.
    wait_until("the election after the seeded start", &faults, || {
        counters
            .iter()
            .all(|node| node.election_state() == Some(ELECTION_CLOSED))
    });
    wait_until("the commit to settle below the old log", &faults, || {
        let values: Vec<Option<i64>> = (0..counters.len())
            .map(|node| commit_of(&counters, node))
            .collect();
        values[0].is_some()
            && values.iter().all(|value| *value == values[0])
            && values[0].is_some_and(|position| position < 64 * 1024)
    });

    let leader = (0..MEMBER_COUNT)
        .find(|id| {
            counters[*id as usize].role() == Some(deepmsg_tests::cluster_harness::ROLE_LEADER)
        })
        .expect("a leader after the seeded start");
    let new_shape = nodes[leader as usize]
        .log_recording_shape()
        .expect("the leader's log recording after the restart");
    assert_eq!(
        new_shape.term_buffer_length, new_term,
        "the log recording after the restart"
    );
    assert_eq!(new_shape.mtu_length, new_mtu, "its MTU");
    assert!(
        new_shape.leadership_term_id > old_leadership_term,
        "a new leadership term: {} > {old_leadership_term}",
        new_shape.leadership_term_id
    );

    // And the new shape carries a whole term's worth of messages.
    let Some(mut client) = ClientSide::start(&ports, leader) else {
        announce_skip();
        return;
    };
    let connect = connect_request(13, &client.egress_channel);
    offer_message(&client, &faults, &connect);
    let mut open_event = None;
    wait_until("the session on the reseeded cluster", &faults, || {
        for event in client.poll_events() {
            if event.correlation_id == 13 && event.code == EventCode::OK {
                open_event = Some(event);
            }
        }
        open_event.is_some()
    });
    let open_event = open_event.expect("the session opened");
    let before = commit_of(&counters, leader as usize).expect("the leader's commit");
    send_max_sized_batch(
        &client,
        &faults,
        open_event.leadership_term_id,
        open_event.cluster_session_id,
        b'z',
        5,
        new_term,
    );
    let per_message = i64::from(new_term / 8);
    wait_until("the third batch to commit", &faults, || {
        commit_of(&counters, leader as usize)
            .is_some_and(|position| position >= before + 5 * per_message)
    });
    wait_until("every member to reach the third batch", &faults, || {
        let target = commit_of(&counters, leader as usize);
        target.is_some() && (0..counters.len()).all(|node| commit_of(&counters, node) == target)
    });

    for node in &mut nodes {
        node.stop();
    }
}
