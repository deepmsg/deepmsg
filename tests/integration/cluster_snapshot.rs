//! P3-4d's acceptance: a single node takes a snapshot, stops, and recovers
//! from it on the same directory — including the log tail written after the
//! snapshot.
//!
//! The node has no services, so the snapshot's service-ack step is the empty
//! set and the trigger is the module's own `take_snapshot` (the ClusterControl
//! and admin triggers are P3-4e). The client is a raw one, as in
//! `cluster_session.rs`.

use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cluster::{ConsensusModuleAgent, ConsensusModuleConfig, ModuleState, Role};
use deepmsg_cluster_common::config::Properties;
use deepmsg_codec_cluster::event_code::EventCode;
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::session_connect_request_codec::SessionConnectRequestEncoder;
use deepmsg_codec_cluster::session_event_codec::SessionEventDecoder;
use deepmsg_codec_cluster::session_message_header_codec::SessionMessageHeaderEncoder;
use deepmsg_codec_cluster::{ReadBuf, WriteBuf};
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

const DEADLINE: Duration = Duration::from_secs(30);
const INGRESS_STREAM_ID: i32 = 101;
const EGRESS_STREAM_ID: i32 = 102;

fn properties(aeron_dir: &std::path::Path, cluster_dir: &std::path::Path) -> Properties {
    let mut properties = Properties::new();
    properties
        .apply_args([
            format!("-Daeron.dir={}", aeron_dir.display()),
            format!("-Daeron.cluster.dir={}", cluster_dir.display()),
            "-Daeron.cluster.id=7".to_owned(),
            "-Daeron.cluster.member.id=0".to_owned(),
            "-Daeron.cluster.members=0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0"
                .to_owned(),
            "-Daeron.cluster.ingress.channel=aeron:ipc".to_owned(),
            "-Daeron.cluster.consensus.channel=aeron:ipc".to_owned(),
            "-Daeron.cluster.log.channel=aeron:ipc".to_owned(),
            "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k".to_owned(),
            "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0".to_owned(),
            "-Daeron.cluster.service.count=0".to_owned(),
        ])
        .expect("the properties parse");
    properties
}

fn drive_until<F: FnMut(&ConsensusModuleAgent) -> bool>(
    agent: &mut ConsensusModuleAgent,
    what: &str,
    mut condition: F,
) {
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        if condition(agent) {
            return;
        }
        agent.do_work().expect("the duty cycle is healthy");
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("timed out waiting for {what}");
}

fn at_steady_state(agent: &ConsensusModuleAgent) -> bool {
    agent.role() == Role::Leader
        && agent.state() == ModuleState::Active
        && agent.election_state().is_none()
}

fn epoch_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock")
            .as_millis(),
    )
    .expect("an epoch in an i64")
}

fn counter_value(agent: &ConsensusModuleAgent, type_id: i32) -> i64 {
    let counter_id = agent.find_counter(type_id).expect("an allocated counter");
    agent
        .client()
        .counters_reader()
        .and_then(|counters| counters.value(counter_id))
        .unwrap_or(0)
}

fn offer(client: &Client, registration_id: i64, bytes: &[u8]) -> bool {
    matches!(
        client.offer(registration_id, bytes),
        Some(deepmsg_core::logbuffer::append::Appended::Ok { .. })
    )
}

fn connect_request() -> Vec<u8> {
    let mut buffer = vec![0_u8; 256];
    let encoder = SessionConnectRequestEncoder::default()
        .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .correlation_id(1)
        .response_stream_id(EGRESS_STREAM_ID)
        .version(3 << 8)
        .response_channel(b"aeron:ipc")
        .encoded_credentials(b"")
        .client_info(b"snapshot-test");
    let length = MESSAGE_HEADER_LENGTH + message.encoded_length();
    buffer.truncate(length);
    buffer
}

fn ingress_message(leadership_term_id: i64, cluster_session_id: i64, payload: &[u8]) -> Vec<u8> {
    let mut buffer = vec![0_u8; 64 + payload.len()];
    let encoder = SessionMessageHeaderEncoder::default()
        .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut message = header.parent().expect("the encoder just wrapped");
    message
        .leadership_term_id(leadership_term_id)
        .cluster_session_id(cluster_session_id)
        .timestamp(1);
    let header_length = MESSAGE_HEADER_LENGTH + message.encoded_length();
    buffer[header_length..header_length + payload.len()].copy_from_slice(payload);
    buffer.truncate(header_length + payload.len());
    buffer
}

/// Open a session with a raw client and answer its id from the OK event.
fn open_session(
    agent: &mut ConsensusModuleAgent,
    client: &mut Client,
    ingress_publication: i64,
    egress_subscription: i64,
) -> i64 {
    let connect = connect_request();
    let deadline = Instant::now() + DEADLINE;
    let mut session_id = 0;
    let mut sent = false;
    while Instant::now() < deadline && session_id == 0 {
        agent.do_work().expect("the duty cycle is healthy");
        client.poll();
        // Offer the connect request **once**: a repeated offer opens another
        // session per copy the module reads.
        if !sent && offer(client, ingress_publication, &connect) {
            sent = true;
        }
        if sent {
            client.poll_subscription(egress_subscription, 10, |message| {
                let payload = message.payload;
                if payload.len() < MESSAGE_HEADER_LENGTH {
                    return;
                }
                let header =
                    deepmsg_codec_cluster::message_header_codec::MessageHeaderDecoder::default()
                        .wrap(ReadBuf::new(payload), 0);
                if header.schema_id() != deepmsg_codec_cluster::SBE_SCHEMA_ID
                    || header.template_id()
                        != deepmsg_codec_cluster::session_event_codec::SBE_TEMPLATE_ID
                {
                    return;
                }
                let decoder = SessionEventDecoder::default().header(header, 0);
                if decoder.code() == EventCode::OK {
                    session_id = decoder.cluster_session_id();
                }
            });
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_ne!(0, session_id, "the session did not open");
    session_id
}

/// Open a session and send `payloads` through it.
fn open_session_and_send(
    agent: &mut ConsensusModuleAgent,
    client: &mut Client,
    ingress_publication: i64,
    egress_subscription: i64,
    payloads: &[&[u8]],
) {
    let session_id = open_session(agent, client, ingress_publication, egress_subscription);
    drive_until(agent, "the open session", |agent| {
        agent.session_count() == 1
    });
    let term = agent.leadership_term_id();
    for payload in payloads {
        let deadline = Instant::now() + DEADLINE;
        while !offer(
            client,
            ingress_publication,
            &ingress_message(term, session_id, payload),
        ) {
            assert!(
                Instant::now() < deadline,
                "the ingress publication stayed closed"
            );
            agent.do_work().expect("the duty cycle is healthy");
            client.poll();
            std::thread::sleep(Duration::from_millis(1));
        }
        agent.do_work().expect("the duty cycle is healthy");
    }
}

fn launch(driver: &OwnArchivingMediaDriver, cluster_dir: &TempDir) -> ConsensusModuleAgent {
    let config =
        ConsensusModuleConfig::from_properties(&properties(driver.aeron_dir(), cluster_dir.path()))
            .expect("the configuration parses");
    ConsensusModuleAgent::launch(config).expect("the module launches")
}

#[test]
fn a_node_recovers_from_its_snapshot_after_a_restart() {
    let archive_dir = TempDir::new("cluster-snapshot-archive");
    let cluster_dir = TempDir::new("cluster-snapshot-cluster");
    let Some(mut driver) = OwnArchivingMediaDriver::start(
        "cluster-snapshot",
        archive_dir.path(),
        &archive::PROPERTIES,
    ) else {
        archiving_driver::announce_skip();
        return;
    };
    driver.await_ready(DEADLINE).expect("the driver is ready");

    let mut agent = launch(&driver, &cluster_dir);
    drive_until(&mut agent, "the first steady state", at_steady_state);

    let mut client = Client::connect(driver.aeron_dir()).expect("the client connects");
    let ingress_publication = client
        .add_publication("aeron:ipc", INGRESS_STREAM_ID, DEADLINE)
        .expect("ingress publication");
    let egress_subscription = client
        .add_subscription("aeron:ipc", EGRESS_STREAM_ID, DEADLINE)
        .expect("egress subscription");
    open_session_and_send(
        &mut agent,
        &mut client,
        ingress_publication,
        egress_subscription,
        &[b"one", b"two", b"three"],
    );

    // The snapshot: its recording, the plan and counter 205.
    agent
        .take_snapshot(epoch_ms())
        .expect("the snapshot is taken");
    assert_eq!(1, agent.recovery_plan().snapshots.len());
    let snapshot = agent.recovery_plan().snapshots[0];
    assert_eq!(1, counter_value(&agent, 205));
    assert_eq!(snapshot.log_position, counter_value(&agent, 203));
    assert_ne!(
        snapshot.recording_id,
        agent.log_recording_id(),
        "the snapshot has its own recording"
    );

    agent.close().expect("the module closes");
    client.close();

    // Same directory, new process.
    let mut agent = launch(&driver, &cluster_dir);
    drive_until(&mut agent, "the recovered steady state", at_steady_state);

    // The reference **closes** sessions recovered from a snapshot at the end
    // of the startup election (`prepareSessionsForNewTerm(isLeaderStartup)`);
    // what a snapshot restores is the next session id, not a live session.
    drive_until(&mut agent, "the recovered session to close", |agent| {
        agent.session_count() == 0
    });
    // Counter 205 is a live counter: a restarted module allocates a fresh one,
    // so it is 0. What says the snapshot was found is the plan and the commit
    // position it restored.
    assert_eq!(1, agent.recovery_plan().snapshots.len());
    assert_eq!(0, counter_value(&agent, 205));
    assert!(
        counter_value(&agent, 203) >= snapshot.log_position,
        "the commit position reaches at least the snapshot's (the tail is replayed)"
    );
    // The election bumps the term on restart, so the counter is at least the
    // snapshot's (`candidateTermId += 1`, `Election.java:684-690`).
    assert!(counter_value(&agent, 239) > snapshot.leadership_term_id);

    // A new session gets the id the snapshot's counter restored.
    let mut client = Client::connect(driver.aeron_dir()).expect("the client connects again");
    let ingress_publication = client
        .add_publication("aeron:ipc", INGRESS_STREAM_ID, DEADLINE)
        .expect("ingress publication");
    let egress_subscription = client
        .add_subscription("aeron:ipc", EGRESS_STREAM_ID, DEADLINE)
        .expect("egress subscription");
    let session_id = open_session(
        &mut agent,
        &mut client,
        ingress_publication,
        egress_subscription,
    );
    assert_eq!(2, session_id, "the next session id comes from the snapshot");

    let before = agent.log_position();
    let term = agent.leadership_term_id();
    let deadline = Instant::now() + DEADLINE;
    while !offer(
        &client,
        ingress_publication,
        &ingress_message(term, 2, b"after"),
    ) {
        assert!(
            Instant::now() < deadline,
            "the ingress publication stayed closed"
        );
        agent.do_work().expect("the duty cycle is healthy");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
    drive_until(&mut agent, "the post-recovery message", |agent| {
        agent.log_position() > before
    });

    agent.close().expect("the module closes");
    client.close();
    driver.stop().expect("the driver stops");
}

#[test]
fn a_node_replays_the_log_tail_written_after_its_snapshot() {
    let archive_dir = TempDir::new("cluster-snapshot-tail-archive");
    let cluster_dir = TempDir::new("cluster-snapshot-tail-cluster");
    let Some(mut driver) = OwnArchivingMediaDriver::start(
        "cluster-snapshot-tail",
        archive_dir.path(),
        &archive::PROPERTIES,
    ) else {
        archiving_driver::announce_skip();
        return;
    };
    driver.await_ready(DEADLINE).expect("the driver is ready");

    let mut agent = launch(&driver, &cluster_dir);
    drive_until(&mut agent, "the first steady state", at_steady_state);

    let mut client = Client::connect(driver.aeron_dir()).expect("the client connects");
    let ingress_publication = client
        .add_publication("aeron:ipc", INGRESS_STREAM_ID, DEADLINE)
        .expect("ingress publication");
    let egress_subscription = client
        .add_subscription("aeron:ipc", EGRESS_STREAM_ID, DEADLINE)
        .expect("egress subscription");
    open_session_and_send(
        &mut agent,
        &mut client,
        ingress_publication,
        egress_subscription,
        &[b"one", b"two"],
    );

    agent
        .take_snapshot(epoch_ms())
        .expect("the snapshot is taken");
    let snapshot = agent.recovery_plan().snapshots[0];

    // One more message after the snapshot, and a **second session** whose open
    // event only exists in the tail: the replay is what restores the next
    // session id from it.
    let term = agent.leadership_term_id();
    let deadline = Instant::now() + DEADLINE;
    while !offer(
        &client,
        ingress_publication,
        &ingress_message(term, 1, b"tail"),
    ) {
        assert!(
            Instant::now() < deadline,
            "the ingress publication stayed closed"
        );
        agent.do_work().expect("the duty cycle is healthy");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
    drive_until(&mut agent, "the tail message", |agent| {
        agent.log_position() > snapshot.log_position
    });

    let second_session_id = open_session(
        &mut agent,
        &mut client,
        ingress_publication,
        egress_subscription,
    );
    assert_eq!(2, second_session_id, "the second session is the tail's");

    agent.close().expect("the module closes");
    client.close();

    let mut agent = launch(&driver, &cluster_dir);
    drive_until(&mut agent, "the recovered steady state", at_steady_state);

    drive_until(&mut agent, "the replay to finish", |agent| {
        !agent.is_replaying()
    });
    drive_until(&mut agent, "the recovered sessions to close", |agent| {
        agent.session_count() == 0
    });
    assert_eq!(
        0,
        agent.session_count(),
        "every session the replay opened was closed by the startup election"
    );
    assert!(
        counter_value(&agent, 203) > snapshot.log_position,
        "the replay moved the commit position past the snapshot"
    );
    assert!(counter_value(&agent, 239) > snapshot.leadership_term_id);

    // A session opened after the restart gets id 3: the replay applied the
    // tail's `SessionOpenEvent` for session 2 and moved the counter.
    let mut client = Client::connect(driver.aeron_dir()).expect("the client connects");
    let ingress_publication = client
        .add_publication("aeron:ipc", INGRESS_STREAM_ID, DEADLINE)
        .expect("ingress publication");
    let egress_subscription = client
        .add_subscription("aeron:ipc", EGRESS_STREAM_ID, DEADLINE)
        .expect("egress subscription");
    assert_eq!(
        3,
        open_session(
            &mut agent,
            &mut client,
            ingress_publication,
            egress_subscription
        ),
        "the tail's session open moved the next session id"
    );

    agent.close().expect("the module closes");
    client.close();
    driver.stop().expect("the driver stops");
}
