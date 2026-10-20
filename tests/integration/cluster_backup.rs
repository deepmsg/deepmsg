//! The backup agent (`P3-9`) end to end: one cluster member records a log, a
//! backup asks it where the cluster is, and copies the log into a directory of
//! its own.
//!
//! The instrument is the reference's `TestBackupNode` cut down to what this
//! slice can assert: the backup's **state counter** (208) reaching
//! `BACKING_UP`, its **live log position** counter (209) moving, and a
//! `recording.log` in its cluster directory that names the cluster's log
//! recording. Everything else the reference's system tests read — the snapshot
//! counter, the next-query deadline, the error log — comes with the fixture
//! slice that ports their twenty cases.
//!
//! One member, not three: the backup protocol is between a backup and **one**
//! answering member, and a single-member cluster is the smallest one this
//! build can elect (the same arrangement `zero_alloc.rs` uses). The backup has
//! its own driver *and archive*, which is what makes it a backup rather than a
//! client: the log is recorded by the local archive, not by the backup's own
//! publication.

use std::time::Instant;

use deepmsg_cluster::persist::recording_log::{FILENAME, RecordingLog};
use deepmsg_cluster::{
    BackupState, ClusterBackup, ClusterBackupComponents, ClusterBackupConfig, ClusterBackupContext,
};
use deepmsg_cluster_common::config::Properties;
use deepmsg_cluster_common::counters::{
    CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID, CLUSTER_BACKUP_STATE_TYPE_ID,
};
use deepmsg_driver::idle::Strategy;
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::OwnArchivingMediaDriver;
use deepmsg_tests::cluster_harness::{
    DEADLINE, ELECTION_CLOSED, Faults, Node, NodeCounters, Ports, ROLE_LEADER, announce_skip,
    await_launched, wait_until,
};
use deepmsg_tests::temp::TempDir;

/// `CLUSTER_ID` in the harness. Kept here so the two agree by name.
const CLUSTER_ID: i32 = 7;

/// Where the backup's own consensus subscription listens — inside `Ports`'
/// fifty-port block, past every offset the cluster itself uses.
const BACKUP_CONSENSUS_OFFSET: u16 = 43;
/// Where the backup's local archive receives the cluster's log replay.
const BACKUP_CATCHUP_OFFSET: u16 = 44;

#[test]
fn a_backup_copies_the_cluster_log_into_its_own_directory() {
    let ports = Ports::new();

    // A one-member cluster: the member string names member 0 only, and the
    // member id is that member's.
    let member_string = format!(
        "0,localhost:{},localhost:{},localhost:{},localhost:0,localhost:{}",
        ports.ingress(0),
        ports.consensus(0),
        ports.log(0),
        ports.archive(0)
    );

    let faults = Faults::default();
    let (launched_tx, launched_rx) = std::sync::mpsc::channel();
    let Some(mut node) = Node::start(0, &ports, &member_string, &faults, &launched_tx) else {
        announce_skip();
        return;
    };
    await_launched(&faults, &launched_rx, 1);

    let node_counters = NodeCounters::open(node.aeron_dir());
    wait_until("the member's election to close", &faults, || {
        node_counters.election_state() == Some(ELECTION_CLOSED)
    });
    assert_eq!(Some(ROLE_LEADER), node_counters.role());

    // The backup's own driver and archive. The archive listens on IPC for its
    // local control channel, which is what the backup's local context names
    // (`ClusterBackup.java:887-893`: the local archive must be IPC).
    let backup_archive_dir = TempDir::new("cluster-backup-archive");
    let properties: Vec<&str> = archive::PROPERTIES.to_vec();
    let Some(mut backup_driver) =
        OwnArchivingMediaDriver::start("cluster-backup", backup_archive_dir.path(), &properties)
    else {
        announce_skip();
        return;
    };
    backup_driver
        .await_ready(DEADLINE)
        .expect("the backup's archive comes up");

    let backup_cluster_dir = TempDir::new("cluster-backup-cluster");

    let config = {
        let mut properties = Properties::new();
        properties
            .apply_args(vec![
                format!("-Daeron.dir={}", backup_driver.aeron_dir().display()),
                format!(
                    "-Daeron.cluster.dir={}",
                    backup_cluster_dir.path().display()
                ),
                format!("-Daeron.cluster.id={CLUSTER_ID}"),
                format!("-Daeron.cluster.members={member_string}"),
                format!(
                    "-Daeron.cluster.consensus.channel=aeron:udp?endpoint=localhost:{}",
                    ports.base() + BACKUP_CONSENSUS_OFFSET
                ),
                format!(
                    "-Daeron.cluster.consensus.endpoints=localhost:{}",
                    ports.consensus(0)
                ),
                format!(
                    "-Daeron.cluster.backup.catchup.endpoint=localhost:{}",
                    ports.base() + BACKUP_CATCHUP_OFFSET
                ),
                // The reference's own fixture shortens this to a second, so a
                // failed attempt retries while the test is still watching
                // (`TestCluster.java:600`).
                "-Daeron.cluster.backup.cool.down.interval=1s".to_owned(),
            ])
            .expect("the backup's properties parse");
        ClusterBackupConfig::from_properties(&properties)
            .expect("the backup's configuration settles")
    };

    let context = ClusterBackupContext::new(config);
    let agent = ClusterBackup::launch_with_components(context, ClusterBackupComponents::default())
        .expect("the backup launches");
    assert_eq!(
        "cluster-backup",
        agent.role_name(),
        "the classic thread name"
    );

    // The agent is not `Send` (its credentials, listener and hooks are trait
    // objects), so it is driven from this thread — the same arrangement
    // `zero_alloc.rs` uses for the consensus module: the *other* processes are
    // what wait.
    let mut agent = agent;
    let mut idle = Strategy::by_agrona_name("backoff").expect("an idle strategy");
    let backup_counters = NodeCounters::open(backup_driver.aeron_dir());

    let mut backing_up = false;
    let mut live_position = None;
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline && !(backing_up && live_position.is_some_and(|p| p > 0)) {
        match agent.do_work() {
            Ok(work) => idle.idle(work),
            Err(error) => panic!("the backup: {error}"),
        }

        backing_up = backup_counters.value(CLUSTER_BACKUP_STATE_TYPE_ID)
            == Some(BackupState::BackingUp.code());
        live_position = backup_counters.value(CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID);
    }

    assert!(backing_up, "the backup reached BACKING_UP");
    // The live log position moves once the local archive is recording the
    // cluster's replay: it is the recording-position counter's value, proposed
    // up (`ClusterBackupAgent.java:984-1006`).
    assert!(
        live_position.is_some_and(|position| position > 0),
        "the backup's live log position moved: {live_position:?}"
    );

    // And the directory a consensus module would be started from says so: a
    // TERM entry naming the backup's own recording of the cluster's log.
    let recording_log =
        RecordingLog::open(&backup_cluster_dir.path().join(FILENAME)).expect("the recording log");
    let term = recording_log.find_last_term().expect("a TERM entry");
    assert!(
        term.recording_id >= 0,
        "the term names the backup's recording: {term:?}"
    );
    assert_eq!(
        node_counters.leadership_term_id(),
        Some(term.leadership_term_id),
        "the term is the one the cluster reported, not one of the backup's own"
    );

    // A cluster that has never snapshotted answers an empty set, and a backup
    // that retrieved none leaves counter 240 at zero.
    assert_eq!(
        Some(0),
        backup_counters.value(
            deepmsg_cluster_common::counters::CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID
        )
    );

    agent.close().expect("the backup closes");
    node.stop();
}
