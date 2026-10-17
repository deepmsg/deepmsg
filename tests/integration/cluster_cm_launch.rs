//! P3-4a's acceptance: a consensus module comes up, locks its counters, starts
//! its log recording, and reaches steady state — then leaves its directory in a
//! state the next process can take.
//!
//! The node is run against **our own archiving media driver**, a separate
//! process from the workspace's own build output, exactly as the reference's
//! `ArchivingMediaDriver` shape is: a driver and an archive in one process, and
//! the consensus module a third. That is the topology the card's DoD names
//! ("起得来、连上 archive、计数器全分配、主循环空转；mark 文件活性"), and it is
//! the only one in which `Archive::connect` and `start_recording` are real.
//!
//! The test skips when the binary has not been built, the same way every other
//! harness test in this repository does. `cargo build --bins` before running it.

use std::time::{Duration, Instant};

use deepmsg_cluster::{ConsensusModuleAgent, ConsensusModuleConfig, ModuleState, Role};
use deepmsg_cluster_common::config::Properties;
use deepmsg_cluster_common::counters::{self, CounterSpec};
use deepmsg_cluster_common::mark::SEMANTIC_VERSION;
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

const DEADLINE: Duration = Duration::from_secs(30);

fn properties(aeron_dir: &std::path::Path, cluster_dir: &std::path::Path) -> Properties {
    let mut properties = Properties::new();
    properties
        .apply_args([
            format!("-Daeron.dir={}", aeron_dir.display()),
            format!("-Daeron.cluster.dir={}", cluster_dir.display()),
            "-Daeron.cluster.id=7".to_owned(),
            "-Daeron.cluster.member.id=0".to_owned(),
            // Six endpoints; only the consensus one is subscribed to in this
            // slice, and the channel itself is IPC below, so they are
            // placeholders. The member string still has to parse.
            "-Daeron.cluster.members=0,\
             localhost:0,\
             localhost:0,\
             localhost:0,\
             localhost:0,\
             localhost:0"
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

#[test]
fn a_single_member_module_starts_allocates_its_counters_and_reaches_steady_state() {
    let archive_dir = TempDir::new("cluster-cm-launch-archive");
    let cluster_dir = TempDir::new("cluster-cm-launch-cluster");

    // `aeron.archive.control.channel` is the channel the archive *serves* for
    // remote clients and must be UDP (`Archive.java`); the local control
    // channel the consensus module connects to is the IPC default the archive
    // always opens beside it.
    let Some(mut driver) = OwnArchivingMediaDriver::start(
        "cluster-cm-launch",
        archive_dir.path(),
        &archive::PROPERTIES,
    ) else {
        archiving_driver::announce_skip();
        return;
    };
    driver
        .await_ready(DEADLINE)
        .expect("the archiving media driver comes up");

    let config =
        ConsensusModuleConfig::from_properties(&properties(driver.aeron_dir(), cluster_dir.path()))
            .expect("the configuration settles");
    let mut agent = ConsensusModuleAgent::launch(config).expect("the module launches");

    // ---- the counters, by type id, key and label --------------------------
    let expected = expected_counters();
    for (type_id, spec) in &expected {
        assert_eq!(
            Some(true),
            agent.find_counter(*type_id).map(|_| true),
            "type {type_id} was not allocated with cluster id 7"
        );

        let counters = agent.client().counters_reader().expect("the CnC file");
        let id = counters::find_cluster_counter(&counters, *type_id, 7).unwrap();
        let key = counters.key(id).unwrap();
        assert_eq!(
            spec.key(),
            &key[..spec.key().len()],
            "counter {type_id} key"
        );
        let descriptor = counters.get(id).unwrap();
        assert_eq!(spec.label(), descriptor.label, "counter {type_id} label");
    }

    // ---- the mark file is alive ------------------------------------------
    let mark = agent.mark_file();
    assert_eq!(Some(SEMANTIC_VERSION), mark.version());
    assert_eq!(Some(7), mark.cluster_id());
    assert_eq!(Some(0), mark.member_id());
    assert_eq!(
        Some((10, 104, 105, 101)),
        mark.stream_ids(),
        "archive control, service, consensus-module and ingress streams"
    );
    let first_activity = mark.activity_timestamp().expect("an activity timestamp");
    assert!((deepmsg_core::clock::epoch_millis() - first_activity).abs() < 10_000);

    // ---- the duty cycle reaches steady state ------------------------------
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        agent.do_work().expect("a duty cycle");
        if agent.election_state().is_none() && agent.state() == ModuleState::Active {
            break;
        }
    }

    assert_eq!(
        None,
        agent.election_state(),
        "the election did not finish; last state {:?}",
        agent.election_state()
    );
    assert_eq!(Role::Leader, agent.role());
    assert_eq!(ModuleState::Active, agent.state());
    assert_eq!(
        Some(2),
        counter_value(&agent, counters::CLUSTER_NODE_ROLE_TYPE_ID)
    );
    assert_eq!(
        Some(1),
        counter_value(&agent, counters::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID)
    );

    // The log is recorded: the archive gave the publication a recording id and
    // the publication is the one the election made.
    assert_ne!(-1, agent.log_recording_id(), "no log recording");
    assert_ne!(-1, agent.log_publication_session_id());

    // The term entry is on disk, in the format the persistence slice pinned.
    let recording_log = deepmsg_cluster::persist::recording_log::RecordingLog::open(
        &cluster_dir.path().join("recording.log"),
    )
    .expect("recording.log");
    let last_term = recording_log.find_last_term().expect("a term entry");
    assert_eq!(0, last_term.leadership_term_id);
    assert_eq!(agent.log_recording_id(), last_term.recording_id);

    // ---- the mark file's activity timestamp advances ----------------------
    std::thread::sleep(Duration::from_millis(1_100));
    agent
        .do_work()
        .expect("a duty cycle past the update interval");
    let second_activity = agent.mark_file().activity_timestamp().unwrap();
    assert!(
        second_activity > first_activity,
        "the mark file's activity timestamp did not advance: {first_activity} -> {second_activity}"
    );

    // ---- a clean shutdown leaves an adoptable directory ------------------
    //
    // `signalTerminated` writes the activity timestamp as `-1`
    // (`ClusterMarkFile.java:455-462`, file offset 16) and leaves the version
    // alone. Read it back from the file the process left behind: that field is
    // what tells the next start the directory was left on purpose, and it is
    // what `ClusterMarkFile::adopt_existing` tests.
    agent.close().expect("a clean close");
    let mark_bytes = std::fs::read(cluster_dir.path().join("cluster-mark.dat")).unwrap();
    let timestamp = i64::from_le_bytes(mark_bytes[16..24].try_into().unwrap());
    assert_eq!(
        -1, timestamp,
        "the mark file did not record a deliberately stopped process"
    );

    // ---- the directory comes back up --------------------------------------
    //
    // The archive is still running and the cluster directory is the one the
    // first process left. What this proves is the recovery half: the term and
    // the log recording are read back, the mark file is adopted rather than
    // refused, and the election starts from the recovered positions. It does
    // **not** reach steady state, and that is the honest answer for this slice:
    // the recovered commit position is behind the appended position, so
    // LEADER_REPLAY asks for a replay, which is `P3-4d`.
    let config =
        ConsensusModuleConfig::from_properties(&properties(driver.aeron_dir(), cluster_dir.path()))
            .expect("the configuration settles again");
    let mut agent = ConsensusModuleAgent::launch(config).expect("the module restarts");

    let plan = agent.recovery_plan();
    assert_eq!(0, plan.last_leadership_term_id);
    assert!(plan.log.is_some(), "the log recording was not found");
    assert!(
        plan.appended_log_position > 0,
        "the recording's stop position was not read"
    );
    assert_eq!(
        agent.log_recording_id(),
        plan.log.as_ref().expect("a log").recording_id,
        "the recovered plan's log recording is adopted (`recoverFromSnapshotAndLog`, :3500-3503)"
    );
    assert_eq!(Some(SEMANTIC_VERSION), agent.mark_file().version());

    // The recovered commit position is behind the appended position, so the
    // election replays the tail (`P3-4d`) and must reach steady state.
    let deadline = Instant::now() + DEADLINE;
    while agent.election_state().is_some() || agent.state() != ModuleState::Active {
        assert!(
            Instant::now() < deadline,
            "the restart did not reach steady state (state={:?}, election={:?})",
            agent.state(),
            agent.election_state()
        );
        agent
            .do_work()
            .expect("the restart's duty cycle is healthy");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(Role::Leader, agent.role());

    agent.close().expect("a clean close");
}

/// The reference's first question every slow tick is whether its Aeron client
/// is still there (`ConsensusModuleAgent.java:2386-2390`). A swallowed error
/// here is a duty cycle that keeps working against a driver that has gone.
#[test]
fn a_closed_client_ends_the_duty_cycle() {
    let archive_dir = TempDir::new("cluster-cm-closed-archive");
    let cluster_dir = TempDir::new("cluster-cm-closed-cluster");

    let Some(mut driver) = OwnArchivingMediaDriver::start(
        "cluster-cm-closed",
        archive_dir.path(),
        &archive::PROPERTIES,
    ) else {
        archiving_driver::announce_skip();
        return;
    };
    driver
        .await_ready(DEADLINE)
        .expect("the archiving media driver comes up");

    let config =
        ConsensusModuleConfig::from_properties(&properties(driver.aeron_dir(), cluster_dir.path()))
            .expect("the configuration settles");
    let mut agent = ConsensusModuleAgent::launch(config).expect("the module launches");

    agent.client_mut().close();

    let error = agent
        .do_work()
        .expect_err("the duty cycle must notice the closed client");
    assert!(
        matches!(error, deepmsg_cluster::ClusterError::AgentTermination(_)),
        "expected an agent termination, got {error}"
    );
}

fn counter_value(agent: &ConsensusModuleAgent, type_id: i32) -> Option<i64> {
    let counters = agent.client().counters_reader()?;
    let id = counters::find_cluster_counter(&counters, type_id, 7)?;
    counters.value(id)
}

/// The fifteen counters a consensus module allocates, with the spec P3-1c's
/// encoders build for them.
fn expected_counters() -> Vec<(i32, CounterSpec)> {
    let version = "1.53.2";
    let commit = "deepmsg";
    vec![
        (
            counters::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID,
            counters::versioned_cluster_counter("Cluster Errors", 7, version, commit).unwrap(),
        ),
        (
            counters::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
            counters::cluster_counter("Consensus Module state", 7).unwrap(),
        ),
        (
            counters::CLUSTER_ELECTION_STATE_TYPE_ID,
            counters::cluster_counter("Cluster election state", 7).unwrap(),
        ),
        (
            counters::CLUSTER_ELECTION_COUNT_TYPE_ID,
            counters::cluster_counter("Cluster election count", 7).unwrap(),
        ),
        (
            counters::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID,
            counters::cluster_counter("Cluster leadership term id", 7).unwrap(),
        ),
        (
            counters::CLUSTER_NODE_ROLE_TYPE_ID,
            counters::cluster_counter("Cluster node role", 7).unwrap(),
        ),
        (
            counters::CLUSTER_COMMIT_POSITION_TYPE_ID,
            counters::cluster_counter("Cluster commit-pos:", 7).unwrap(),
        ),
        (
            counters::CLUSTER_CONTROL_TOGGLE_TYPE_ID,
            counters::cluster_counter("Cluster control toggle", 7).unwrap(),
        ),
        (
            counters::NODE_CONTROL_TOGGLE_TYPE_ID,
            counters::cluster_counter("Node control toggle", 7).unwrap(),
        ),
        (
            counters::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID,
            counters::cluster_counter("Cluster snapshot count", 7).unwrap(),
        ),
        (
            counters::CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID,
            counters::cluster_counter("Cluster timed out client count", 7).unwrap(),
        ),
        (
            counters::CLUSTER_MAX_CYCLE_TIME_TYPE_ID,
            counters::cluster_counter("Cluster max cycle time in ns", 7).unwrap(),
        ),
        (
            counters::CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
            counters::cluster_counter("Cluster work cycle time exceeded count: threshold=100ms", 7)
                .unwrap(),
        ),
        (
            counters::CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID,
            counters::cluster_counter("Total max snapshot duration in ns", 7).unwrap(),
        ),
        (
            counters::CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID,
            counters::cluster_counter(
                "Total max snapshot duration exceeded count: threshold=10s",
                7,
            )
            .unwrap(),
        ),
    ]
}
