//! LC-1's reliability cases: a session must be **closed**, not silently
//! starved, when the medium under it goes away.
//!
//! The reference's `ClusterSessionReliabilityTest` injects the loss from Java —
//! a `PortLossGenerator`/`StreamIdLossGenerator` hung on the **client's** media
//! driver, switched on from the test's own thread once the service has seen five
//! messages (`ClusterSessionReliabilityTest.java:92-245`). A driver here is a
//! process, so the same faults are its own settings, decided before it starts:
//!
//! * the client's **sends** stop arriving after the session is up
//!   (`debug.send.data.loss.pass.first` + `hold.millis`): the cluster's ingress
//!   image goes unavailable, and the cluster closes the session — which is the
//!   only way a client learns its messages were lost rather than sequenced;
//! * the client's **receives** stop arriving (`debug.receive.data.loss.*`): the
//!   egress image times out, and the client must not rejoin the same node's
//!   image afterwards, which would show it a gap.
//!
//! # The assertions are both ends'
//!
//! The client's own word — `is_closed` — is not enough: an implementation that
//! gave up on its own would satisfy it. Each case also says the business across
//! the wire saw real traffic, counted no message out of sequence, and was told
//! the session ended for `TIMEOUT`; and that the client offered **more** than
//! the business received, which is the injected loss itself.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use deepmsg_cluster_client::SessionState;
use deepmsg_cluster_common::counters::CLUSTER_NODE_ROLE_TYPE_ID;
use deepmsg_codec_cluster::close_reason::CloseReason;
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::cluster::{ClusterPorts, MemberSpec, Options, TestCluster};
use deepmsg_tests::cluster_client::{Session, base_context};

/// The tests in this binary each start a whole cluster.
static SERIAL: Mutex<()> = Mutex::new(());

const MEMBERS: i32 = 3;
const CLUSTER_ID: i32 = 22;
const ROLE_LEADER: i64 = 2;
const DEADLINE: Duration = Duration::from_secs(30);
/// How often the client offers a message while the fault is injected
/// (`ClusterSessionReliabilityTest.java:167`).
const OFFER_INTERVAL: Duration = Duration::from_millis(10);
/// The cluster's image liveness and session timeout, as the reference's case
/// sets them (`ClusterSessionReliabilityTest.java:109-117`).
const CLUSTER_PROPERTIES: &[(&str, &str)] = &[
    ("aeron.image.liveness.timeout", "1000ms"),
    ("aeron.cluster.session.timeout", "4s"),
];

/// The next cluster's port base; this binary owns the 43xxx block.
fn next_port_base() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(43_000);
    NEXT.fetch_add(1_000, Ordering::Relaxed)
}

fn options(base: u16, members: i32, ingress_channel: &str, properties: &[(&str, &str)]) -> Options {
    Options {
        cluster_id: CLUSTER_ID,
        members: Some(ClusterPorts::new(base).members_string(members)),
        ingress_channel: ingress_channel.to_owned(),
        consensus_channel: "aeron:udp?term-length=64k".to_owned(),
        log_channel: "aeron:udp?term-length=64k".to_owned(),
        node_properties: properties
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
        ..Options::default()
    }
}

fn start(
    test_name: &str,
    base: u16,
    members: i32,
    service: &'static str,
    ingress_channel: &str,
) -> Option<TestCluster> {
    let specs: Vec<MemberSpec> = (0..members)
        .map(|member| MemberSpec::test_container(member, service))
        .collect();

    TestCluster::start(
        test_name,
        &specs,
        options(base, members, ingress_channel, CLUSTER_PROPERTIES),
    )
}

/// The member table's ingress endpoints, which is what a client connects by.
fn ingress_endpoints(base: u16, members: i32) -> String {
    ClusterPorts::new(base).ingress_endpoints(members)
}

fn skip() {
    eprintln!("SKIPPED: the reference jar, java, the fork's Java probes, or our driver is missing");
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

/// The egress blackout that the client's receive side applies: the egress
/// stream's first ten frames go through, then every frame — heartbeats too, or
/// the image cannot notice — is withheld for three seconds.
///
/// The reference's window is its image liveness plus 200 ms, measured from the
/// test's own thread; here the image's liveness check runs on the driver's
/// timer, whose default is one **second** (`TIMER_INTERVAL_NS_DEFAULT`, the
/// reference's own), so a window only 200 ms past the liveness is a race the
/// recovery can win: a retransmission delivered on the tick that would have
/// found the image quiet keeps it alive. Three seconds is the reference's own
/// deadline (`imageLivenessTimeoutNs + 2s`) and leaves two ticks to spare.
const EGRESS_LOSS: &[&str] = &[
    "-Daeron.image.liveness.timeout=1000ms",
    "-Ddeepmsg.debug.receive.data.loss.stream.id=102",
    "-Ddeepmsg.debug.receive.data.loss.pass.first=10",
    "-Ddeepmsg.debug.receive.data.loss.until.millis=3000",
    "-Ddeepmsg.debug.receive.data.loss.drop.heartbeats=true",
];

/// The ingress blackout the client's send side applies: six data-carrying
/// datagrams go through (the connect request and five messages), then
/// everything is withheld for two seconds — the cluster's one-second image
/// liveness and a margin, so the image is gone before the sends come back
/// (`ClusterSessionReliabilityTest.java:178-183`).
const INGRESS_LOSS: &[&str] = &[
    "-Ddeepmsg.debug.send.data.loss.pass.first=6",
    "-Ddeepmsg.debug.send.data.loss.hold.millis=2000",
];

/// Offer a sequence every ten milliseconds until the session closes or the
/// deadline passes, and answer how many the session took.
///
/// The offers that fail are not retried: the point is that the client keeps
/// talking into a session whose messages are not arriving, which is what the
/// reference's loop does (`ClusterSessionReliabilityTest.java:152-187`).
fn offer_until_closed(session: &mut Session, deadline: Instant) -> u64 {
    let mut offered: u64 = 0;

    while !session.session.is_closed() && Instant::now() < deadline {
        let payload = offered.to_le_bytes();

        if matches!(
            session.session.offer(&mut session.client, &payload),
            Some(Appended::Ok { .. })
        ) {
            offered += 1;
        }

        session.drive();
        std::thread::sleep(OFFER_INTERVAL);
    }

    offered
}

/// The assertions both ends must satisfy once the session has been cut off.
fn assert_the_session_was_closed_for_timeout(
    cluster: &TestCluster,
    leader: i32,
    session: &Session,
    offered: u64,
) {
    assert!(
        session.session.is_closed(),
        "the client's session closed, and it offered {offered} messages"
    );

    let service = cluster
        .test_service(leader, 0)
        .expect("an in-process business for the leader");

    let deadline = Instant::now() + DEADLINE;
    let closed_for_timeout = || service.close_reasons().contains(&CloseReason::TIMEOUT);

    assert!(
        wait_until(deadline, closed_for_timeout),
        "the business was told the session ended for TIMEOUT: {:?}",
        service.close_reasons()
    );

    assert!(
        service.messages() >= 5,
        "the business saw the messages that came before the loss: {}",
        service.messages()
    );
    assert_eq!(
        0,
        service.out_of_sequence(),
        "every message the business saw was the session's next sequence number"
    );
    assert!(
        offered > u64::try_from(service.messages()).expect("a message count"),
        "the client offered more than arrived — the loss was injected: {offered} against {}",
        service.messages()
    );
}

fn wait_until(deadline: Instant, mut condition: impl FnMut() -> bool) -> bool {
    while Instant::now() < deadline {
        if condition() {
            return true;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    condition()
}

/// The client's sends stop arriving, and the cluster closes the session instead
/// of losing messages silently.
fn session_gets_closed_when_the_ingress_image_goes_unavailable(multicast: bool) {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let test_name = if multicast {
        "lc1-reliability-multicast"
    } else {
        "lc1-reliability-unicast"
    };
    let ingress_channel = if multicast {
        "aeron:udp?term-length=128k|endpoint=239.192.11.87:20123|interface=127.0.0.1"
    } else {
        "aeron:udp?term-length=128k"
    };

    let Some(cluster) = start(
        test_name,
        base,
        MEMBERS,
        "sequence-service",
        ingress_channel,
    ) else {
        skip();
        return;
    };

    let leader = await_all_joined(&cluster, MEMBERS);

    let mut context = base_context(test_name, DEADLINE);
    context.ingress_channel = Some(ingress_channel.to_owned());
    if !multicast {
        context.ingress_endpoints = Some(ingress_endpoints(base, MEMBERS));
    }

    let Some(mut session) = Session::connect(test_name, context, INGRESS_LOSS, || {
        cluster.node_log(leader)
    }) else {
        skip();
        return;
    };

    let offered = offer_until_closed(&mut session, Instant::now() + DEADLINE);

    assert_the_session_was_closed_for_timeout(&cluster, leader, &session, offered);
}

/// The client's receives stop arriving, and it must not rejoin the same node's
/// egress image afterwards.
fn client_does_not_rejoin_the_egress_image_from_the_same_node() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let test_name = "lc1-reliability-egress";

    let Some(cluster) = start(
        test_name,
        base,
        1,
        "sequenced-egress",
        "aeron:udp?term-length=128k",
    ) else {
        skip();
        return;
    };

    let leader = await_all_joined(&cluster, 1);

    let mut context = base_context(test_name, DEADLINE);
    context.ingress_channel = Some(format!(
        "aeron:udp?term-length=128k|endpoint={}",
        ClusterPorts::new(base).ingress_endpoint(leader)
    ));
    let Some(mut session) =
        Session::connect(test_name, context, EGRESS_LOSS, || cluster.node_log(leader))
    else {
        skip();
        return;
    };

    // The cluster's timer is the only traffic: the client reads, and never
    // offers, so the egress stream keeps moving whether or not the client can
    // read it — which is what makes a rejoin visible as a jump.
    let deadline = Instant::now() + Duration::from_secs(10);

    assert!(
        session.poll_until(deadline, |session| session.messages().len() >= 5),
        "the timer's egress never arrived: {} messages\n{}",
        session.messages().len(),
        cluster.node_log(leader)
    );

    // The blackout outlasts the client driver's image liveness, so the image
    // must be gone: the session leaves the connected state when its egress
    // image closes (`AC:603-607`).
    assert!(
        session.poll_until(deadline, |session| {
            session.session.state() != SessionState::Connected
        }),
        "the egress image never went unavailable: state {:?}, messages {}, offered {}\n{}",
        session.session.state(),
        session.messages().len(),
        cluster
            .test_service(leader, 0)
            .map_or(0, |service| service.offered()),
        session.driver_log()
    );

    // Keep reading for a while after the blackout ends. A client that rejoined
    // the same node's image would now be shown the messages the timer produced
    // in between — a jump in the sequence.
    session.poll_until(Instant::now() + Duration::from_secs(2), |_| false);

    let echoes: Vec<u64> = session
        .messages()
        .iter()
        .map(|payload| {
            u64::from_le_bytes(
                payload
                    .get(..8)
                    .and_then(|bytes| bytes.try_into().ok())
                    .expect("every message is eight bytes"),
            )
        })
        .collect();

    assert!(
        echoes.len() >= 5,
        "the messages that got through before the image went unavailable: {echoes:?}"
    );
    assert_eq!(
        (0..u64::try_from(echoes.len()).expect("a small count")).collect::<Vec<u64>>(),
        echoes,
        "the listener saw a prefix of the sequences and no jump: a rejoin would show one"
    );

    let service = cluster
        .test_service(leader, 0)
        .expect("an in-process business for the leader");

    assert!(
        service.offered() > echoes.len(),
        "the timer offered more than the client received — the loss was injected: {} against {}\n{}",
        service.offered(),
        echoes.len(),
        session.driver_log()
    );
}

#[test]
fn session_should_get_closed_when_ingress_image_goes_unavailable_to_prevent_silent_message_loss() {
    session_gets_closed_when_the_ingress_image_goes_unavailable(false);
}

#[test]
fn session_should_get_closed_when_multicast_ingress_image_goes_unavailable_to_prevent_silent_message_loss()
 {
    session_gets_closed_when_the_ingress_image_goes_unavailable(true);
}

#[test]
fn session_should_get_closed_when_ingress_image_goes_unavailable_after_failover_to_prevent_silent_message_loss()
 {
    session_gets_closed_when_the_ingress_image_goes_unavailable_after_a_failover(false);
}

#[test]
fn session_should_get_closed_when_multicast_ingress_image_goes_unavailable_after_failover_to_prevent_silent_message_loss()
 {
    session_gets_closed_when_the_ingress_image_goes_unavailable_after_a_failover(true);
}

/// The same fault, but the client first has to follow a new leader.
fn session_gets_closed_when_the_ingress_image_goes_unavailable_after_a_failover(multicast: bool) {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = next_port_base();
    let test_name = if multicast {
        "lc1-reliability-multicast-failover"
    } else {
        "lc1-reliability-unicast-failover"
    };
    let ingress_channel = if multicast {
        "aeron:udp?term-length=128k|endpoint=239.192.11.87:20123|interface=127.0.0.1"
    } else {
        "aeron:udp?term-length=128k"
    };

    let Some(mut cluster) = start(
        test_name,
        base,
        MEMBERS,
        "sequence-service",
        ingress_channel,
    ) else {
        skip();
        return;
    };

    let leader = await_all_joined(&cluster, MEMBERS);

    let mut context = base_context(test_name, DEADLINE);
    context.ingress_channel = Some(ingress_channel.to_owned());
    if !multicast {
        context.ingress_endpoints = Some(ingress_endpoints(base, MEMBERS));
    }

    let Some(mut session) = Session::connect(test_name, context, INGRESS_LOSS, || {
        cluster.node_log(leader)
    }) else {
        skip();
        return;
    };

    // No traffic before the failover, as the reference's case does it: what the
    // blackout is anchored on is the **new** endpoint's first data datagram.
    cluster.stop_node(leader);

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
        "the session never followed a new leader {new_leader:?}: state {:?}, leader {}, events {:?}\n{}",
        session.session.state(),
        session.session.leader_member_id(),
        session.received.borrow().events,
        cluster.container_logs()
    );

    let new_leader = new_leader.expect("the session followed a new leader that was found");
    let offered = offer_until_closed(&mut session, Instant::now() + DEADLINE);

    assert_the_session_was_closed_for_timeout(&cluster, new_leader, &session, offered);
}

#[test]
fn client_should_not_rejoin_egress_image_from_the_same_node_to_prevent_silent_message_loss() {
    client_does_not_rejoin_the_egress_image_from_the_same_node();
}
