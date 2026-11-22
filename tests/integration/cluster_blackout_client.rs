//! A full receive blackout, survived by **our** client against **our**
//! cluster.
//!
//! LC-2 (`tests/interop/cluster_lc2_scenarios.rs`) puts the reference's
//! `AeronCluster` on the near end. That is the right oracle for the wire, but
//! it leaves one shape untested: three of our nodes and *our* cluster client
//! across a blackout that takes every member's **reception** — log, consensus
//! and ingress — for six seconds while the client keeps offering. This is that
//! shape, and it is a product regression rather than an interop criterion: no
//! JVM, no reference checkout.
//!
//! # Why the client has a driver of its own
//!
//! Same reason as LC-1's scenarios: a client of a cluster reachable only over
//! UDP is a deployment, and a client sharing a member's driver would not be.
//! [`Session`] starts the driver and connects our `ClusterClient` through it.
//!
//! # The window
//!
//! `deepmsg.debug.receive.data.loss.{from,until}.millis` is a span measured
//! from **each stream's first frame** (`crates/driver/src/config.rs`), so the
//! blackout opens a few seconds after the nodes come up. The client is already
//! offering by then, and keeps offering through it — the case is worthless if
//! the window lands before or after the traffic.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use deepmsg_tests::cluster_client::{Session, base_context};
use deepmsg_tests::cluster_harness::{
    ELECTION_CLOSED, Faults, MEMBER_COUNT, Node, NodeCounters, Ports, announce_skip,
    await_launched, members, wait_until,
};

/// This binary's cases each start a whole cluster on a port block of their
/// own; only one at a time.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// How many messages the client must complete across the window.
const MESSAGES: u64 = 150;

/// The pause between messages: long enough that the count spans the blackout
/// instead of finishing before it opens.
const PAUSE: Duration = Duration::from_millis(100);

/// Everything the failure message can say about the members.
fn members_state(nodes: &[Node], counters: &[NodeCounters]) -> String {
    use deepmsg_cluster_common::counters::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID as MODULE_STATE;

    let mut out = String::new();
    for (id, (node, counters)) in nodes.iter().zip(counters).enumerate() {
        let service = node.service_state().map_or_else(
            || "none".to_owned(),
            |state| {
                format!(
                    "messages={} opens={} closes={} out-of-sequence={}",
                    state.messages(),
                    state.session_opens(),
                    state.session_closes(),
                    state.out_of_sequence()
                )
            },
        );
        out.push_str(&format!(
            "\n--- node {id} ---\n\
             role={:?} module_state={:?} election={:?} term={:?} commit={:?} append={:?}\n\
             service: {service}\n\
             --- system counters ---\n{}\n\
             --- driver log (tail) ---\n{}\n",
            counters.role(),
            counters.value(MODULE_STATE),
            counters.election_state(),
            counters.leadership_term_id(),
            counters.commit_position(),
            counters.append_position(),
            counters.system_counters(),
            node.driver_log_tail(40),
        ));
    }
    out
}

/// How many distinct sequences the echo listener has seen.
fn distinct_messages(session: &Session) -> usize {
    let messages = session.received.borrow().messages.clone();
    let mut seen = std::collections::BTreeSet::new();
    for message in messages {
        if let Ok(bytes) = <[u8; 8]>::try_from(message.as_slice()) {
            seen.insert(u64::from_le_bytes(bytes));
        }
    }
    seen.len()
}

/// Three members of ours lose every received frame for six seconds while our
/// client offers throughout; every echo arrives, and the cluster keeps one
/// leader.
#[test]
fn a_full_receive_blackout_is_survived_by_our_client_and_our_cluster() {
    let _serial = serial();
    let ports = Ports::new();
    let faults = Faults::default();
    let (launched_tx, launched_rx) = std::sync::mpsc::channel();
    let member_string = members(&ports);

    let driver_properties = [
        "-Ddeepmsg.debug.receive.data.loss.from.millis=8000",
        "-Ddeepmsg.debug.receive.data.loss.until.millis=14000",
        "-Ddeepmsg.debug.receive.data.loss.drop.heartbeats=true",
    ];
    let module_arguments = [
        "-Daeron.cluster.leader.heartbeat.timeout=3s",
        "-Daeron.cluster.session.timeout=60s",
    ];

    let mut nodes = Vec::new();
    for id in 0..MEMBER_COUNT {
        let Some(node) = Node::start_with_properties(
            id,
            &ports,
            &member_string,
            &faults,
            &launched_tx,
            Some("test-service"),
            &driver_properties,
            &module_arguments,
        ) else {
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
        counters
            .iter()
            .all(|node| node.election_state() == Some(ELECTION_CLOSED))
    });

    let state = || members_state(&nodes, &counters);
    let mut context = base_context("deepmsg-blackout", Duration::from_secs(120));
    context.ingress_channel = Some("aeron:udp?term-length=128k".to_owned());
    context.ingress_endpoints = Some(ports.client_ingress_endpoints());

    let mut session = Session::connect("cluster-blackout", context, &[], state)
        .unwrap_or_else(|| panic!("the client's own driver did not start"));

    let deadline = Instant::now() + Duration::from_secs(120);
    for sequence in 0..MESSAGES {
        let payload = sequence.to_le_bytes();
        assert!(
            session.offer(&payload, deadline),
            "the session did not take message {sequence}{}\nour client's driver:\n{}",
            state(),
            session.driver_log()
        );

        // An application owns the message until its echo comes back. A
        // message whose offer was accepted just as the blackout opened is in
        // the *sender's* term buffer and no receiver ever saw it: the client
        // cannot know that, and the reference client's contract is that a
        // session message may be lost across a leadership change. So: wait a
        // few seconds, and offer it again while the echo is missing. The
        // sequence travels in the payload, so a duplicate is recognisable.
        let per_attempt = Duration::from_secs(5);
        let mut attempts = 0;
        loop {
            attempts += 1;
            let echo_deadline = deadline.min(Instant::now() + per_attempt);
            if session.poll_until(echo_deadline, |session| {
                session
                    .received
                    .borrow()
                    .messages
                    .iter()
                    .any(|message| message.as_slice() == payload)
            }) {
                break;
            }

            assert!(
                attempts < 8,
                "the echo of message {sequence} never arrived after {attempts} offers; \
                 received {} distinct; new leaders {:?}; session events {:?}{}\n\
                 our client's driver:\n{}",
                distinct_messages(&session),
                session.received.borrow().new_leaders,
                session.received.borrow().events,
                state(),
                session.driver_log()
            );

            assert!(
                session.offer(&payload, deadline),
                "the session did not take message {sequence} again{}",
                state()
            );
        }

        std::thread::sleep(PAUSE);
    }

    faults.check();

    // Every sequence's echo, in order. A re-offered message may echo twice
    // (the first copy can be applied after the retry), so the reading is the
    // **distinct** sequences in arrival order: the application is
    // at-least-once, and the cluster must have applied every message.
    let mut seen: Vec<u64> = Vec::new();
    let mut set = std::collections::BTreeSet::new();
    for message in session.messages() {
        let Ok(bytes) = <[u8; 8]>::try_from(message.as_slice()) else {
            continue;
        };
        let sequence = u64::from_le_bytes(bytes);
        if set.insert(sequence) {
            seen.push(sequence);
        }
    }
    let expected: Vec<u64> = (0..MESSAGES).collect();
    assert_eq!(
        expected,
        seen,
        "the client read every message's echo, in order, and none is missing{}",
        state()
    );

    let terms: Vec<Option<i64>> = counters
        .iter()
        .map(NodeCounters::leadership_term_id)
        .collect();
    assert!(
        terms.iter().all(|term| term.is_some()),
        "every member settled on a leadership term: {terms:?}{}",
        state()
    );
    let leaders = counters
        .iter()
        .filter(|node| node.role() == Some(deepmsg_tests::cluster_harness::ROLE_LEADER))
        .count();
    assert_eq!(1, leaders, "exactly one leader at the end{}", state());

    for node in &mut nodes {
        node.stop();
    }
}
