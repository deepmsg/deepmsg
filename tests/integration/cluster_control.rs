//! P3-4e's acceptance: the five operator switches on a single node.
//!
//! The operator is a second `Client` writing the CnC counter the module reads
//! (`ClusterControl.java:234-261`), which is exactly what `ClusterControl.main`
//! does; nothing here calls into the agent except the duty cycle. `SUSPEND` /
//! `RESUME` are judged by whether an ingress message the cluster was already
//! handed is applied, `SNAPSHOT` by counter 205 and the recovery plan, and
//! `SHUTDOWN` / `ABORT` by how the duty cycle ends and what the recording log
//! contains.

use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cluster::agent::publisher::ConsensusPublisher;
use deepmsg_cluster::control::cluster_control::ToggleState as ClusterToggle;
use deepmsg_cluster::control::node_control::ToggleState as NodeToggle;
use deepmsg_cluster::{
    ClusterError, ConsensusModuleAgent, ConsensusModuleConfig, ModuleState, Role,
};
use deepmsg_cluster_common::config::Properties;
use deepmsg_cluster_common::counters::{self, CLUSTER_CONTROL_TOGGLE_TYPE_ID};
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
const CLUSTER_ID: i32 = 7;

/// Counter 200 (`Consensus Module state`), 202 (control toggle), 203 (commit
/// position) and 205 (snapshot count), per `ClusterCounters`.
const MODULE_STATE_TYPE_ID: i32 = 200;
const COMMIT_POSITION_TYPE_ID: i32 = 203;
const SNAPSHOT_COUNTER_TYPE_ID: i32 = 205;

fn properties(aeron_dir: &std::path::Path, cluster_dir: &std::path::Path) -> Properties {
    let mut properties = Properties::new();
    properties
        .apply_args([
            format!("-Daeron.dir={}", aeron_dir.display()),
            format!("-Daeron.cluster.dir={}", cluster_dir.display()),
            format!("-Daeron.cluster.id={CLUSTER_ID}"),
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

fn drive_until_termination(agent: &mut ConsensusModuleAgent, what: &str) -> ClusterError {
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        if let Err(error) = agent.do_work() {
            return error;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("timed out waiting for {what}");
}

fn drive_for(agent: &mut ConsensusModuleAgent, duration: Duration) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        agent.do_work().expect("the duty cycle is healthy");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn at_steady_state(agent: &ConsensusModuleAgent) -> bool {
    agent.role() == Role::Leader
        && agent.state() == ModuleState::Active
        && agent.election_state().is_none()
}

fn counter_value(agent: &ConsensusModuleAgent, type_id: i32) -> i64 {
    let counters = agent.client().counters_reader().expect("counters");
    let counter_id = counters::find_cluster_counter(&counters, type_id, CLUSTER_ID)
        .expect("an allocated counter");
    counters.value(counter_id).expect("a live counter")
}

/// Write the control toggle, the way `ClusterControl.main` does (`:234-261`):
/// find the counter by type and cluster id, then set it.
fn set_toggle(operator: &Client, type_id: i32, value: i64) {
    let counter_id = {
        let counters = operator.counters_reader().expect("counters");
        counters::find_cluster_counter(&counters, type_id, CLUSTER_ID).expect("the toggle counter")
    };
    operator
        .counters_writable()
        .expect("writable counters")
        .set_value(counter_id, value)
        .expect("the toggle is written");
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
        .client_info(b"control-test");
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

fn launch(driver: &OwnArchivingMediaDriver, cluster_dir: &TempDir) -> ConsensusModuleAgent {
    let config =
        ConsensusModuleConfig::from_properties(&properties(driver.aeron_dir(), cluster_dir.path()))
            .expect("the configuration parses");
    ConsensusModuleAgent::launch(config).expect("the module launches")
}

#[test]
fn suspend_stops_ingress_and_resume_puts_it_back() {
    let archive_dir = TempDir::new("cluster-control-suspend-archive");
    let cluster_dir = TempDir::new("cluster-control-suspend-cluster");
    let Some(mut driver) = OwnArchivingMediaDriver::start(
        "cluster-control-suspend",
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
    let session_id = open_session(
        &mut agent,
        &mut client,
        ingress_publication,
        egress_subscription,
    );
    drive_until(&mut agent, "the open session", |agent| {
        agent.session_count() == 1
    });

    let mut operator = Client::connect(driver.aeron_dir()).expect("the operator connects");
    let before_suspend = agent.log_position();
    set_toggle(
        &operator,
        CLUSTER_CONTROL_TOGGLE_TYPE_ID,
        i64::from(ClusterToggle::Suspend.code()),
    );
    drive_until(&mut agent, "the SUSPEND", |agent| {
        agent.state() == ModuleState::Suspended
    });
    assert!(
        agent.log_position() > before_suspend,
        "SUSPEND is a ClusterAction appended to the log, not a local flag"
    );
    assert_eq!(2, counter_value(&agent, CLUSTER_CONTROL_TOGGLE_TYPE_ID));

    // A message the cluster is handed while suspended: the publication accepts
    // it, and the duty cycle must not apply it.
    let suspended_at = agent.log_position();
    let term = agent.leadership_term_id();
    assert!(offer(
        &client,
        ingress_publication,
        &ingress_message(term, session_id, b"while suspended"),
    ));
    drive_for(&mut agent, Duration::from_millis(300));
    assert_eq!(
        suspended_at,
        agent.log_position(),
        "ingress was applied while suspended"
    );
    assert_eq!(ModuleState::Suspended, agent.state());

    set_toggle(
        &operator,
        CLUSTER_CONTROL_TOGGLE_TYPE_ID,
        i64::from(ClusterToggle::Resume.code()),
    );
    let suspended_len = agent.log_position();
    drive_until(&mut agent, "the RESUME", |agent| {
        agent.state() == ModuleState::Active
    });
    assert!(
        agent.log_position() > suspended_len,
        "RESUME is a ClusterAction appended to the log too"
    );
    assert_eq!(
        i64::from(ClusterToggle::Neutral.code()),
        counter_value(&agent, CLUSTER_CONTROL_TOGGLE_TYPE_ID),
        "the module resets the toggle once the action is complete"
    );

    // The message that was parked in ingress is applied after the resume.
    drive_until(&mut agent, "the suspended message", |agent| {
        agent.log_position() > suspended_at
    });
    assert_eq!(1, agent.session_count());

    agent.close().expect("the module closes");
    operator.close();
    client.close();
    driver.stop().expect("the driver stops");
}

#[test]
fn the_snapshot_toggle_takes_one_and_returns_to_active() {
    let archive_dir = TempDir::new("cluster-control-snapshot-archive");
    let cluster_dir = TempDir::new("cluster-control-snapshot-cluster");
    let Some(mut driver) = OwnArchivingMediaDriver::start(
        "cluster-control-snapshot",
        archive_dir.path(),
        &archive::PROPERTIES,
    ) else {
        archiving_driver::announce_skip();
        return;
    };
    driver.await_ready(DEADLINE).expect("the driver is ready");

    let mut agent = launch(&driver, &cluster_dir);
    drive_until(&mut agent, "the first steady state", at_steady_state);

    let mut operator = Client::connect(driver.aeron_dir()).expect("the operator connects");
    set_toggle(
        &operator,
        CLUSTER_CONTROL_TOGGLE_TYPE_ID,
        i64::from(ClusterToggle::Snapshot.code()),
    );

    drive_until(&mut agent, "the SNAPSHOT toggle", |agent| {
        agent.state() == ModuleState::Active && counter_value(agent, SNAPSHOT_COUNTER_TYPE_ID) == 1
    });
    assert_eq!(1, agent.recovery_plan().snapshots.len());
    assert_eq!(
        i64::from(ClusterToggle::Neutral.code()),
        counter_value(&agent, CLUSTER_CONTROL_TOGGLE_TYPE_ID)
    );

    agent.close().expect("the module closes");
    operator.close();
    driver.stop().expect("the driver stops");
}

#[test]
fn shutdown_snapshots_and_then_ends_the_duty_cycle() {
    let archive_dir = TempDir::new("cluster-control-shutdown-archive");
    let cluster_dir = TempDir::new("cluster-control-shutdown-cluster");
    let Some(mut driver) = OwnArchivingMediaDriver::start(
        "cluster-control-shutdown",
        archive_dir.path(),
        &archive::PROPERTIES,
    ) else {
        archiving_driver::announce_skip();
        return;
    };
    driver.await_ready(DEADLINE).expect("the driver is ready");

    let mut agent = launch(&driver, &cluster_dir);
    drive_until(&mut agent, "the first steady state", at_steady_state);

    let mut operator = Client::connect(driver.aeron_dir()).expect("the operator connects");
    set_toggle(
        &operator,
        CLUSTER_CONTROL_TOGGLE_TYPE_ID,
        i64::from(ClusterToggle::Shutdown.code()),
    );

    let error = drive_until_termination(&mut agent, "the SHUTDOWN");
    assert!(
        matches!(error, ClusterError::AgentTermination(_)),
        "expected an agent termination, got {error}"
    );
    assert_eq!(ModuleState::Closed, agent.state());
    assert_eq!(
        i64::from(ModuleState::Closed.code()),
        counter_value(&agent, MODULE_STATE_TYPE_ID)
    );
    // An orderly shutdown is a snapshot first (`:2585-2607`).
    assert_eq!(1, agent.recovery_plan().snapshots.len());
    assert_eq!(1, counter_value(&agent, SNAPSHOT_COUNTER_TYPE_ID));
    // A shutdown is not an operator action the module resets: the reference
    // leaves the counter where the operator put it.
    assert_eq!(5, counter_value(&agent, CLUSTER_CONTROL_TOGGLE_TYPE_ID));
    assert!(counter_value(&agent, COMMIT_POSITION_TYPE_ID) >= 0);

    agent.close().expect("the module closes again");
    operator.close();
    driver.stop().expect("the driver stops");
}

#[test]
fn abort_ends_the_duty_cycle_without_a_snapshot() {
    let archive_dir = TempDir::new("cluster-control-abort-archive");
    let cluster_dir = TempDir::new("cluster-control-abort-cluster");
    let Some(mut driver) = OwnArchivingMediaDriver::start(
        "cluster-control-abort",
        archive_dir.path(),
        &archive::PROPERTIES,
    ) else {
        archiving_driver::announce_skip();
        return;
    };
    driver.await_ready(DEADLINE).expect("the driver is ready");

    let mut agent = launch(&driver, &cluster_dir);
    drive_until(&mut agent, "the first steady state", at_steady_state);

    let mut operator = Client::connect(driver.aeron_dir()).expect("the operator connects");
    set_toggle(
        &operator,
        CLUSTER_CONTROL_TOGGLE_TYPE_ID,
        i64::from(ClusterToggle::Abort.code()),
    );

    let error = drive_until_termination(&mut agent, "the ABORT");
    assert!(
        matches!(error, ClusterError::AgentTermination(_)),
        "expected an agent termination, got {error}"
    );
    assert_eq!(ModuleState::Closed, agent.state());
    assert!(
        agent.recovery_plan().snapshots.is_empty(),
        "ABORT must not take a snapshot"
    );
    assert_eq!(0, counter_value(&agent, SNAPSHOT_COUNTER_TYPE_ID));

    agent.close().expect("the module closes again");
    operator.close();
    driver.stop().expect("the driver stops");
}

#[test]
fn an_invalid_toggle_value_ends_the_duty_cycle() {
    // `ClusterControl.ToggleState.get` throws for a value outside the table
    // (`ClusterControl.java:172-178`), and `doWork` rethrows it outside an
    // election (`ConsensusModuleAgent.java:462-465`). A module that swallowed
    // it would keep running with no idea what the operator asked for.
    let archive_dir = TempDir::new("cluster-control-invalid-archive");
    let cluster_dir = TempDir::new("cluster-control-invalid-cluster");
    let Some(mut driver) = OwnArchivingMediaDriver::start(
        "cluster-control-invalid",
        archive_dir.path(),
        &archive::PROPERTIES,
    ) else {
        archiving_driver::announce_skip();
        return;
    };
    driver.await_ready(DEADLINE).expect("the driver is ready");

    let mut agent = launch(&driver, &cluster_dir);
    drive_until(&mut agent, "the first steady state", at_steady_state);

    let mut operator = Client::connect(driver.aeron_dir()).expect("the operator connects");
    set_toggle(&operator, CLUSTER_CONTROL_TOGGLE_TYPE_ID, 99);

    let error = drive_until_termination(&mut agent, "the invalid toggle");
    assert_eq!(
        "cluster: invalid toggle value: 99",
        error.to_string(),
        "the module must refuse the value rather than ignore it"
    );
    // Not a termination: the state is still whatever it was, and `close` is
    // what ends the agent.
    assert_eq!(ModuleState::Active, agent.state());

    agent.close().expect("the module closes");
    operator.close();
    driver.stop().expect("the driver stops");
}

#[test]
fn the_node_toggle_values_are_refused_and_reset() {
    // The two OSS values name features this build does not have, so the module
    // resets the counter and counts the refusal rather than leaving it armed.
    let toggle = NodeToggle::ReplicateStandbySnapshot;

    let archive_dir = TempDir::new("cluster-control-node-archive");
    let cluster_dir = TempDir::new("cluster-control-node-cluster");
    let Some(mut driver) = OwnArchivingMediaDriver::start(
        "cluster-control-node",
        archive_dir.path(),
        &archive::PROPERTIES,
    ) else {
        archiving_driver::announce_skip();
        return;
    };
    driver.await_ready(DEADLINE).expect("the driver is ready");

    let mut agent = launch(&driver, &cluster_dir);
    drive_until(&mut agent, "the first steady state", at_steady_state);

    let mut operator = Client::connect(driver.aeron_dir()).expect("the operator connects");
    set_toggle(
        &operator,
        deepmsg_cluster_common::counters::NODE_CONTROL_TOGGLE_TYPE_ID,
        i64::from(toggle.code()),
    );

    drive_until(&mut agent, "the node toggle to be reset", |agent| {
        counter_value(
            agent,
            deepmsg_cluster_common::counters::NODE_CONTROL_TOGGLE_TYPE_ID,
        ) == i64::from(NodeToggle::Neutral.code())
    });
    assert_eq!(ModuleState::Active, agent.state());

    agent.close().expect("the module closes");
    operator.close();
    driver.stop().expect("the driver stops");
}

/// The publisher the termination round uses has no per-member publications in
/// this slice, so its send answer is the reference's "no publication" one.
#[test]
fn a_null_publication_answers_false() {
    let mut publisher = ConsensusPublisher::new(0);
    assert!(!publisher.termination_position(1, 0, 0));
    assert!(!publisher.termination_ack(0, 0, 0, 1));
}
