//! LC-1: our cluster client against a Java cluster, over UDP, on its own driver.
//!
//! The three-node scenarios of the client spec's §5, each of which puts our
//! `AeronCluster` where a deployed client would be: a driver of its own (not a
//! member's), an ingress channel that names one endpoint or all of them, and an
//! egress subscription the cluster answers on.
//!
//! # Why the client has its own driver
//!
//! The reference's own harness gives the client one (`TestCluster.java:960-990`
//! builds a second `MediaDriver.Context` for it), and two of the scenarios here
//! cannot be expressed any other way: connecting to a **follower** is a UDP
//! name, and following a **new leader** needs a medium where the new leader's
//! egress publication can reach the client — an egress publication created on
//! another node's driver cannot.
//!
//! # One at a time
//!
//! Every test starts a whole cluster (three JVMs) and a driver, so [`SERIAL`]
//! holds them apart and each takes a block of ports nobody else uses
//! ([`ClusterPorts`]).

use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use deepmsg_cluster_common::counters::{
    CLUSTER_NODE_ROLE_TYPE_ID, CLUSTER_SNAPSHOT_COUNTER_TYPE_ID,
};
use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_tests::cluster::{ClusterPorts, MemberSpec, Options, TestCluster};
use deepmsg_tests::cluster_client::{Session, base_context, send_and_await_echo};

/// The tests in this binary each start a whole cluster.
static SERIAL: Mutex<()> = Mutex::new(());

const MEMBERS: i32 = 3;
const CLUSTER_ID: i32 = 21;
const ROLE_LEADER: i64 = 2;
const DEADLINE: Duration = Duration::from_secs(30);

/// The next cluster's port base; this binary owns the 13xxx block.
///
/// Below `net.ipv4.ip_local_port_range` (32768 here) on purpose: inside it any
/// process may hold a port mid-run, and a member that cannot bind its own log
/// endpoint fails its follower log handover, re-canvasses and terminates — a
/// failure that reads as a cluster member dying for no reason
/// (`Election.java:1362`; `records/P3-9.md` §9.2 has the case, where
/// `avahi-daemon` held 46004). The 41xxx block this used to own was inside the
/// range and overlapped `cluster_lc1_reliability`'s 43xxx, and `cargo test` runs
/// the two binaries together.
fn next_port_base() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(13_000);
    NEXT.fetch_add(1_000, Ordering::Relaxed)
}

fn options(base: u16, members: i32, properties: &[(&str, &str)]) -> Options {
    Options {
        cluster_id: CLUSTER_ID,
        members: Some(ClusterPorts::new(base).members_string(members)),
        ingress_channel: "aeron:udp?term-length=128k".to_owned(),
        consensus_channel: "aeron:udp?term-length=64k".to_owned(),
        log_channel: "aeron:udp?term-length=64k".to_owned(),
        node_properties: properties
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
        probes: Vec::new(),
        ..Options::default()
    }
}

fn start(
    test_name: &str,
    base: u16,
    members: i32,
    service: &'static str,
    properties: &[(&str, &str)],
) -> Option<TestCluster> {
    TestCluster::start(
        test_name,
        &members_array(members, service),
        options(base, members, properties),
    )
}

/// The member table for a cluster of `members` nodes.
fn members_array(members: i32, service: &'static str) -> Vec<MemberSpec> {
    (0..members)
        .map(|member| MemberSpec::test_container(member, service))
        .collect()
}

/// Wait until every member has a role and exactly one is the leader.
fn await_all_joined(cluster: &TestCluster, members: i32) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(60);

    while Instant::now() < deadline {
        let roles: Vec<Option<i64>> = (0..members)
            .map(|member| cluster.counter_on(member, CLUSTER_NODE_ROLE_TYPE_ID))
            .collect();

        if roles.iter().all(Option::is_some)
            && roles
                .iter()
                .filter(|role| **role == Some(ROLE_LEADER))
                .count()
                == 1
        {
            return cluster.await_leader();
        }

        std::thread::sleep(Duration::from_millis(20));
    }

    panic!(
        "the members never agreed on one leader\n{}",
        cluster.container_logs()
    );
}

fn skip() {
    eprintln!("SKIPPED: the reference jar, java, the fork's Java probes, or our driver is missing");
}

/// Every member's Java node log, for a failure message.
fn node_logs(cluster: &TestCluster, members: i32) -> String {
    (0..members)
        .map(|member| format!("member {member}:\n{}", cluster.node_log(member)))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The client connects on one ingress channel, names the leader, and an echo
/// comes back.
#[test]
fn a_client_connects_on_one_ingress_channel_and_echoes() {
    // A failed test must not turn every later one into a lock failure: the
    // lock protects the machine, not the assertions.
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let Some(cluster) = start("lc1-one-channel", base, MEMBERS, "test-service", &[]) else {
        skip();
        return;
    };

    let leader = await_all_joined(&cluster, MEMBERS);
    let ports = ClusterPorts::new(base);

    let mut context = base_context("deepmsg-lc1-scenarios", DEADLINE);
    context.ingress_channel = Some(format!(
        "aeron:udp?term-length=128k|endpoint={}",
        ports.ingress_endpoint(leader)
    ));

    let Some(mut session) = Session::connect("lc1-one-channel", context, &[], || {
        node_logs(&cluster, MEMBERS)
    }) else {
        skip();
        return;
    };

    assert_eq!(
        leader,
        session.session.leader_member_id(),
        "the session says which member leads, and it is the one that answered"
    );

    send_and_await_echo(&mut session, 1, Instant::now() + DEADLINE);
    session.session.close(&mut session.client);
}

/// The client connects with `ingressEndpoints`, which is the member table
/// rather than one channel.
#[test]
fn a_client_connects_with_an_ingress_endpoint_list() {
    // A failed test must not turn every later one into a lock failure: the
    // lock protects the machine, not the assertions.
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let Some(cluster) = start("lc1-endpoints", base, MEMBERS, "test-service", &[]) else {
        skip();
        return;
    };

    let leader = await_all_joined(&cluster, MEMBERS);
    let ports = ClusterPorts::new(base);

    let mut context = base_context("deepmsg-lc1-scenarios", DEADLINE);
    context.ingress_channel = Some("aeron:udp?term-length=128k".to_owned());
    context.ingress_endpoints = Some(ports.ingress_endpoints(MEMBERS));

    let Some(mut session) = Session::connect("lc1-endpoints", context, &[], || {
        node_logs(&cluster, MEMBERS)
    }) else {
        skip();
        return;
    };

    assert_eq!(leader, session.session.leader_member_id());
    send_and_await_echo(&mut session, 2, Instant::now() + DEADLINE);
    session.session.close(&mut session.client);
}

/// A client pointed at a **follower** is redirected to the leader, and the
/// session that comes back is the leader's.
#[test]
fn a_client_pointed_at_a_follower_is_redirected_to_the_leader() {
    // A failed test must not turn every later one into a lock failure: the
    // lock protects the machine, not the assertions.
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let Some(cluster) = start("lc1-redirect", base, MEMBERS, "test-service", &[]) else {
        skip();
        return;
    };

    let leader = await_all_joined(&cluster, MEMBERS);
    let follower = (0..MEMBERS)
        .find(|member| *member != leader)
        .expect("a three-node cluster has a follower");
    let ports = ClusterPorts::new(base);

    let mut context = base_context("deepmsg-lc1-scenarios", DEADLINE);
    context.ingress_channel = Some(format!(
        "aeron:udp?term-length=128k|endpoint={}",
        ports.ingress_endpoint(follower)
    ));

    let Some(mut session) = Session::connect("lc1-redirect", context, &[], || {
        node_logs(&cluster, MEMBERS)
    }) else {
        skip();
        return;
    };

    assert_eq!(
        leader,
        session.session.leader_member_id(),
        "the connect only concludes after the redirect, so the leader is the answer"
    );

    send_and_await_echo(&mut session, 3, Instant::now() + DEADLINE);
    session.session.close(&mut session.client);
}

/// Messages sent before the leader dies and after the new one is up all arrive,
/// in order, exactly once — asserted at the far end.
#[test]
fn a_client_follows_the_new_leader_without_losing_or_repeating_a_message() {
    // A failed test must not turn every later one into a lock failure: the
    // lock protects the machine, not the assertions.
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let Some(mut cluster) = start("lc1-failover", base, MEMBERS, "sequence-service", &[]) else {
        skip();
        return;
    };

    let leader = await_all_joined(&cluster, MEMBERS);
    let ports = ClusterPorts::new(base);

    let mut context = base_context("deepmsg-lc1-scenarios", DEADLINE);
    context.ingress_channel = Some("aeron:udp?term-length=128k".to_owned());
    context.ingress_endpoints = Some(ports.ingress_endpoints(MEMBERS));
    // An election on this machine takes well under a second, but the client's
    // own deadline is not what a scenario should be measuring.
    context.new_leader_timeout_ns = Some(i64::try_from(DEADLINE.as_nanos()).expect("a timeout"));

    let Some(mut session) = Session::connect("lc1-failover", context, &[], || {
        node_logs(&cluster, MEMBERS)
    }) else {
        skip();
        return;
    };

    const BEFORE: u64 = 10;
    const AFTER: u64 = 7;

    for sequence in 0..BEFORE {
        send_and_await_echo(&mut session, sequence, Instant::now() + DEADLINE);
    }

    cluster.stop_node(leader);

    // The wait **drives the session**: the client is a live client of its own
    // driver, and a test that blocks through a whole election without polling
    // it is a test that lets its driver reap it. That is not hypothetical — a
    // blocking `await_leader` spends the full connect timeout on the dead
    // member's directory, which is the client liveness window itself.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut new_leader = None;
    let followed = loop {
        session.drive();

        if new_leader.is_none() {
            new_leader = cluster.leader_other_than(leader);
        }

        if new_leader == Some(session.session.leader_member_id()) {
            break true;
        }

        if Instant::now() >= deadline {
            break false;
        }

        std::thread::sleep(Duration::from_millis(1));
    };

    assert!(
        followed,
        "the session never followed a new leader {new_leader:?}: state {:?}, leader {}, term {}, \
         new-leader events {:?}, session events {:?}, client terminated {} ({:?})\n{}\ndriver log:\n{}",
        session.session.state(),
        session.session.leader_member_id(),
        session.session.leadership_term_id(),
        session.received.borrow().new_leaders,
        session.received.borrow().events,
        session.client.is_terminated(),
        session.client.error(),
        cluster.container_logs(),
        session.driver_log()
    );

    let new_leader = new_leader.expect("the session followed a new leader that was found");

    for sequence in BEFORE..BEFORE + AFTER {
        send_and_await_echo(&mut session, sequence, deadline);
    }

    let echoes: Vec<u64> = session
        .messages()
        .iter()
        .map(|payload| {
            u64::from_le_bytes(
                payload
                    .get(..8)
                    .and_then(|bytes| bytes.try_into().ok())
                    .expect("every echo is eight bytes"),
            )
        })
        .collect();

    assert_eq!(
        (0..BEFORE + AFTER).collect::<Vec<u64>>(),
        echoes,
        "the listener saw every sequence once, in order"
    );

    let service = cluster
        .test_service(new_leader, 0)
        .expect("an in-process business for the new leader");

    assert_eq!(
        usize::try_from(BEFORE + AFTER).expect("a small count"),
        service.messages(),
        "the new leader's business saw every message exactly once"
    );
    assert_eq!(
        0,
        service.out_of_sequence(),
        "no message was a gap or a repeat of one already sequenced"
    );
}

/// A session that sends nothing times out, and the client is told so.
#[test]
fn a_client_whose_session_times_out_is_closed() {
    // A failed test must not turn every later one into a lock failure: the
    // lock protects the machine, not the assertions.
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let Some(cluster) = start(
        "lc1-session-timeout",
        base,
        MEMBERS,
        "test-service",
        &[("aeron.cluster.session.timeout", "2s")],
    ) else {
        skip();
        return;
    };

    let leader = await_all_joined(&cluster, MEMBERS);
    let ports = ClusterPorts::new(base);

    let mut context = base_context("deepmsg-lc1-scenarios", DEADLINE);
    context.ingress_channel = Some("aeron:udp?term-length=128k".to_owned());
    context.ingress_endpoints = Some(ports.ingress_endpoints(MEMBERS));

    let Some(mut session) = Session::connect("lc1-session-timeout", context, &[], || {
        node_logs(&cluster, MEMBERS)
    }) else {
        skip();
        return;
    };

    let deadline = Instant::now() + DEADLINE;

    // No keep-alive and no message: the only thing that keeps a session open
    // is traffic, and there is none.
    assert!(
        session.poll_until(deadline, |session| session.session.is_closed()),
        "the session never closed after its timeout\n{}",
        cluster.node_log(leader)
    );

    let events = session.received.borrow().events.clone();
    assert!(
        events
            .iter()
            .any(|(code, detail)| *code == EventCode::CLOSED && detail.contains("TIMEOUT")),
        "the client is told CLOSED with the cluster's reason, and the reason is TIMEOUT: {events:?}"
    );

    // The client hears the close on the egress; the business hears it through
    // the log, at its own pace, so it is waited for rather than read once.
    let service = cluster
        .test_service(leader, 0)
        .expect("an in-process business for the leader");
    let closed_for_timeout = || {
        service
            .close_reasons()
            .contains(&deepmsg_codec_cluster::close_reason::CloseReason::TIMEOUT)
    };

    assert!(
        wait_until(deadline, closed_for_timeout),
        "the business is told the session ended for TIMEOUT: {:?}",
        service.close_reasons()
    );
}

/// Wait for `condition`, polling it, until `deadline`.
fn wait_until(deadline: Instant, mut condition: impl FnMut() -> bool) -> bool {
    while Instant::now() < deadline {
        if condition() {
            return true;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    condition()
}

/// An admin request for a snapshot is answered, and the cluster takes one.
#[test]
fn a_client_asks_for_a_snapshot() {
    // A failed test must not turn every later one into a lock failure: the
    // lock protects the machine, not the assertions.
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let Some(cluster) = start(
        "lc1-snapshot",
        base,
        MEMBERS,
        "test-service",
        // The reference's default authorisation service allows the backup and
        // standby commands and nothing else, so a SNAPSHOT request is refused
        // with UNAUTHORISED_ACCESS (`ConsensusModule.java:813`, `:1352`, and
        // `ClusterTest.shouldRejectTakeSnapshotRequestWithAnAuthorisationError`);
        // the case that takes one says `ALLOW_ALL`
        // (`ClusterTest.shouldTakeASnapshotAfterReceivingAdminRequestOfTypeSnapshot`).
        &[("aeron.cluster.authorisation.service.supplier", "ALLOW_ALL")],
    ) else {
        skip();
        return;
    };

    let leader = await_all_joined(&cluster, MEMBERS);
    let ports = ClusterPorts::new(base);

    let mut context = base_context("deepmsg-lc1-scenarios", DEADLINE);
    context.ingress_channel = Some("aeron:udp?term-length=128k".to_owned());
    context.ingress_endpoints = Some(ports.ingress_endpoints(MEMBERS));

    let Some(mut session) = Session::connect("lc1-snapshot", context, &[], || {
        node_logs(&cluster, MEMBERS)
    }) else {
        skip();
        return;
    };

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
        cluster.node_log(leader)
    );

    let responses = session.received.borrow().admin_responses.clone();
    // The reference's own listener asserts the same three things: the type, the
    // code, and that an accepted request carries no message
    // (`ClusterTest.injectAdminResponseEgressListener`, `:2103`).
    assert!(
        responses.iter().any(|response| response.request_type
            == deepmsg_codec_cluster::admin_request_type::AdminRequestType::SNAPSHOT
            && response.response_code
                == deepmsg_codec_cluster::admin_response_code::AdminResponseCode::OK
            && response.message.is_empty()),
        "the cluster accepted the snapshot request: {responses:?}"
    );

    let taken = |node: i32| cluster.counter_on(node, CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);

    assert!(
        session.poll_until(deadline, |_| taken(leader).is_some_and(|count| count >= 1)),
        "the leader's snapshot counter never moved (it is {:?})\n{}",
        taken(leader),
        cluster.node_log(leader)
    );
}

/// The cluster asks the client to prove itself, and the session opens only
/// after it answers.
///
/// The Java side is a probe class loaded by name
/// (`aeron.cluster.authenticator.supplier`, `ConsensusModule.java:807`); when
/// the probe is not in the fork the fixture skips, which is the suite's
/// convention for a missing reference piece.
#[test]
fn a_client_answers_a_challenge_before_the_session_opens() {
    // A failed test must not turn every later one into a lock failure: the
    // lock protects the machine, not the assertions.
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();

    let member = MemberSpec::test_container(0, "test-service");
    let Some(cluster) = TestCluster::start(
        "lc1-challenge",
        &[member],
        Options {
            cluster_id: CLUSTER_ID,
            members: Some(ClusterPorts::new(base).members_string(1)),
            ingress_channel: "aeron:udp?term-length=128k".to_owned(),
            consensus_channel: "aeron:udp?term-length=64k".to_owned(),
            log_channel: "aeron:udp?term-length=64k".to_owned(),
            node_properties: vec![(
                "aeron.cluster.authenticator.supplier".to_owned(),
                "ChallengeAuthenticatorSupplier".to_owned(),
            )],
            probes: vec!["ChallengeAuthenticatorSupplier.java"],
            ..Options::default()
        },
    ) else {
        skip();
        return;
    };

    let leader = await_all_joined(&cluster, 1);
    let ports = ClusterPorts::new(base);

    let mut context = base_context("deepmsg-lc1-scenarios", DEADLINE);
    context.ingress_channel = Some(format!(
        "aeron:udp?term-length=128k|endpoint={}",
        ports.ingress_endpoint(leader)
    ));
    context.credentials = Box::new(ChallengeAnswer);

    let Some(mut session) =
        Session::connect("lc1-challenge", context, &[], || node_logs(&cluster, 1))
    else {
        skip();
        return;
    };

    send_and_await_echo(&mut session, 4, Instant::now() + DEADLINE);

    assert!(
        cluster.node_log(leader).contains("CHALLENGE_OK"),
        "the cluster challenged and accepted the answer\n{}",
        cluster.node_log(leader)
    );
}

/// What the client sends when the cluster asks it to prove itself.
///
/// The connect request carries no credentials, which is what makes the cluster
/// issue a challenge; the answer is the one the probe expects.
struct ChallengeAnswer;

impl deepmsg_cluster_client::CredentialsSupplier for ChallengeAnswer {
    fn encoded(&mut self) -> Vec<u8> {
        Vec::new()
    }

    fn on_challenge(&mut self, challenge: &[u8]) -> Result<Vec<u8>, String> {
        if challenge != b"lc1-challenge" {
            return Err(format!(
                "the cluster asked for {:?}, which is not the challenge",
                String::from_utf8_lossy(challenge)
            ));
        }

        Ok(b"lc1-answer".to_vec())
    }
}
