//! **A cluster whose member strings name an archive response endpoint**, so the
//! follower's log replication takes the response-channel route.
//!
//! This is P3-0e's second judge, and the one the card actually asked for: a
//! member string with the seventh attribute (`archiveResponseEndpoint`,
//! `ClusterMember.java:576-604`) makes `newLogReplication` rewrite the copy's
//! channel to `control=<the leader's response endpoint>|control-mode=response`
//! (`CMA:2964-2972`), which is the route that needs the replay token, the
//! second publication and the proxy in the destination archive's
//! `ReplicationSession`.
//!
//! The scenario is `cluster_follower_path`'s H4: a follower stops, the cluster
//! moves to a new term without it, and the returning member comes back to a
//! hole that a replay cannot fill — so the election takes the
//! archive-to-archive copy (`Election.java:897-958`). What changes is the
//! member string; what proves the route is a **mutant** — with the token branch
//! off, the follower never catches up.
//!
//! No Java is involved: the cluster, both archives and the client are this
//! build. The reference's side of the protocol is
//! `replication_response_channel.rs`'s Java-source case.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_tests::cluster_harness::{
    CATCHUP_DEADLINE, ClientSide, ELECTION_CLOSED, Faults, MEMBER_COUNT, Node, NodeCounters, Ports,
    ROLE_LEADER, announce_skip, await_launched, connect_request, ingress_message,
    members_with_response_endpoints, offer_message, wait_until,
};

/// The scenarios here bind ports derived from the **process** id
/// (`cluster_harness::Ports`), so `cargo test`'s parallel threads would race
/// for them. They take turns.
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

/// **A member that missed a term is caught up by a replication over the
/// leader archive's response channel.**
///
/// The shape is `cluster_follower_path`'s H4 (`ClusterTest.shouldRecoverWhen
/// FollowerIsMultipleTermsBehind`, `:1419`): the follower dies, the cluster
/// moves to a new term without it, and it comes back to a log whose gap starts
/// **before** the new term — `appendPosition < termBaseLogPosition` — so a
/// replay cannot fill it and the election takes the archive-to-archive
/// replication (`Election.java:897-958`). The only change is the member string:
/// with the seventh attribute, the replication's channel is a response channel,
/// which is P3-0e's route.
#[test]
fn a_member_that_missed_a_term_is_caught_up_over_a_response_channel() {
    let _serial = serial();
    let ports = Ports::new();
    let member_string = members_with_response_endpoints(&ports);
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
        counters
            .iter()
            .all(|node| node.election_state() == Some(ELECTION_CLOSED))
    });

    let first_leader = (0..MEMBER_COUNT)
        .find(|id| counters[*id as usize].role() == Some(ROLE_LEADER))
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
    nodes[first_leader as usize].stop();
    let (relaunch_tx, relaunch_rx) = mpsc::channel();
    nodes[first_leader as usize].restart(&relaunch_tx);
    await_launched(&faults, &relaunch_rx, 1);

    let survivors = [first_leader, survivor];
    wait_until("the two survivors to elect a new term", &faults, || {
        survivors.iter().all(|id| {
            counters[*id as usize].election_state() == Some(ELECTION_CLOSED)
                && counters[*id as usize]
                    .leadership_term_id()
                    .is_some_and(|term| term > first_term)
        })
    });
    let new_leader = survivors
        .iter()
        .copied()
        .find(|id| counters[*id as usize].role() == Some(ROLE_LEADER))
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
            counters[victim as usize].election_state() == Some(ELECTION_CLOSED)
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
