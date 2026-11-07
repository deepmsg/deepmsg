//! The cluster's lifecycle lines, with a one-member cluster in this process.
//!
//! This is the level the global logger needs: the consensus module installs it
//! in the process that runs the module, not in a library test. The module is
//! launched the way `cluster_control` launches one — a driver in its own
//! directory, a zero-service extension, a duty cycle this test drives — so the
//! events come from the same code a production member runs.
//!
//! Two criteria:
//!
//! - **role.change is a change, not a notice.** A one-member cluster still goes
//!   follower → candidate → leader, and the lines are checked for consecutive
//!   duplicates: `set_role` writes a line only when the value differs.
//! - **snapshot.begin and snapshot.end are a pair.** The operator's SNAPSHOT
//!   toggle drives one, and the counts and the duration field are asserted.
//!
//! The test binary is its own because it installs the process-global logger;
//! tests that install one cannot share a binary with tests that expect none.

use std::fs;
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_cluster::control::cluster_control::ToggleState as ClusterToggle;
use deepmsg_cluster::{ConsensusModuleAgent, ConsensusModuleConfig, ModuleState, Role};
use deepmsg_cluster_common::config::Properties;
use deepmsg_cluster_common::counters;
use deepmsg_cluster_common::counters::CLUSTER_CONTROL_TOGGLE_TYPE_ID;
use deepmsg_log::{Component, Config, Guard, Level, Roll};
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

const DEADLINE: Duration = Duration::from_secs(30);
const CLUSTER_ID: i32 = 11;

/// Counter 205 (`Cluster snapshot counter`).
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

fn launch(driver: &OwnArchivingMediaDriver, cluster_dir: &TempDir) -> ConsensusModuleAgent {
    let mut config =
        ConsensusModuleConfig::from_properties(&properties(driver.aeron_dir(), cluster_dir.path()))
            .expect("the configuration parses");
    config.consensus_module_extension = true;
    ConsensusModuleAgent::launch(config).expect("the module launches")
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
    panic!("{what} did not happen in {DEADLINE:?}");
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

fn role_of(line: &str) -> Option<&str> {
    line.split("\"role\":\"").nth(1)?.split('"').next()
}

#[test]
fn the_cluster_reports_its_roles_its_election_and_a_snapshot() {
    let logs = TempDir::new("cluster-log-events-log");
    let log_dir = logs.path().join("log");
    let config = Config {
        dir: log_dir.clone(),
        node_id: "cluster-log".to_owned(),
        component: Component::Cluster.as_str(),
        level: Level::Info,
        component_levels: Vec::new(),
        roll: Roll::default(),
        queue_capacity: 8192,
        cpu: None,
        pinner: None,
        clock: deepmsg_core::clock::epoch_nano_time,
    };
    let logging = Guard::new(deepmsg_log::init(config).expect("the logger installs"));

    let archive_dir = TempDir::new("cluster-log-events-archive");
    let cluster_dir = TempDir::new("cluster-log-events-cluster");
    let Some(mut driver) = OwnArchivingMediaDriver::start(
        "cluster-log-events",
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

    agent.close().expect("the module closes");
    operator.close();
    driver.stop().expect("the driver stops");

    // Drain and join the writer before reading: the last line is the one that
    // matters.
    drop(logging);

    let text = fs::read_to_string(log_dir.join("cluster-cluster-log.json"))
        .expect("the cluster's log file");

    let roles: Vec<&str> = text
        .lines()
        .filter(|line| line.contains("\"event\":\"role.change\""))
        .filter_map(role_of)
        .collect();
    assert!(
        roles.len() >= 2,
        "a one-member election changes the role at least twice: {roles:?}; log: {text}"
    );
    for pair in roles.windows(2) {
        assert_ne!(
            pair[0], pair[1],
            "two consecutive role.change lines carry the same role: {roles:?}; log: {text}"
        );
    }
    assert!(
        text.contains("\"event\":\"election.complete\""),
        "the election completed; log: {text}"
    );

    let begins = text.matches("\"event\":\"snapshot.begin\"").count();
    let ends = text.matches("\"event\":\"snapshot.end\"").count();
    assert!(
        begins >= 1,
        "the snapshot toggle produced a begin; log: {text}"
    );
    assert_eq!(begins, ends, "every snapshot.begin has an end; log: {text}");
    assert!(
        text.contains("\"duration_ns\":"),
        "the end carries the duration; log: {text}"
    );
}
