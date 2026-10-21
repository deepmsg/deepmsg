//! `deepmsg-cluster-backup`, the backup node as a process: against a
//! one-member cluster it reaches `BACKING_UP` and its live log position moves,
//! and `SIGTERM` stops it cleanly.
//!
//! This is the deployment shape `ClusterBackupMediaDriver` is — a driver, an
//! archive and a backup in one process — which no in-process test can cover.
//! The cluster side is the harness's one member; the backup is the binary.

use std::time::{Duration, Instant};

use deepmsg_cluster::BackupState;
use deepmsg_cluster_common::counters::{
    CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID, CLUSTER_BACKUP_STATE_TYPE_ID,
};
use deepmsg_tests::cluster_harness::{
    ELECTION_CLOSED, Faults, Node, NodeCounters, Ports, ROLE_LEADER, announce_skip, await_launched,
    wait_until,
};
use deepmsg_tests::driver::{self, ReferenceDriver};
use deepmsg_tests::temp::TempDir;

/// Where `deepmsg-cluster-backup` is expected, relative to the tests crate.
const DEFAULT_CLUSTER_BACKUP: &str = "../target/debug/deepmsg-cluster-backup";

#[test]
fn the_backup_binary_reaches_backing_up_and_stops_cleanly() {
    let ports = Ports::new();
    // One member, as in `cluster_backup.rs`: this is a backup against a
    // cluster it can reach, not a three-node election.
    let member_string = format!(
        "0,localhost:{},localhost:{},localhost:{},localhost:0,localhost:{}",
        ports.ingress(0),
        ports.consensus(0),
        ports.log(0),
        ports.archive(0),
    );
    let faults = Faults::default();
    let (launched_tx, launched_rx) = std::sync::mpsc::channel();

    let Some(mut node) = Node::start(0, &ports, &member_string, &faults, &launched_tx) else {
        announce_skip();
        return;
    };
    await_launched(&faults, &launched_rx, 1);

    let counters = NodeCounters::open(node.aeron_dir());
    wait_until("the member's election to close", &faults, || {
        counters.election_state() == Some(ELECTION_CLOSED)
    });
    assert_eq!(Some(ROLE_LEADER), counters.role());

    let Some(binary) = driver::locate_tool("DEEPMSG_CLUSTER_BACKUP", DEFAULT_CLUSTER_BACKUP) else {
        eprintln!(
            "SKIPPED: no deepmsg-cluster-backup binary; build it \
             (`cargo build -p deepmsg-cluster --bin deepmsg-cluster-backup`) or set \
             DEEPMSG_CLUSTER_BACKUP."
        );
        announce_skip();
        return;
    };

    let aeron_dir = TempDir::new("cluster-backup-binary-aeron");
    let archive_dir = TempDir::new("cluster-backup-binary-archive");
    let cluster_dir = TempDir::new("cluster-backup-binary-cluster");
    let aeron_path = aeron_dir.path().join("aeron");

    let args: Vec<String> = vec![
        format!("-Daeron.dir={}", aeron_path.display()),
        format!("-Daeron.archive.dir={}", archive_dir.path().display()),
        // The reference's own aggregate names the archive's control channel
        // too: `Archive.Context.controlChannel` has no default there either,
        // and the launch refuses without one.
        format!(
            "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:{}",
            ports.base() + 45
        ),
        format!("-Daeron.cluster.dir={}", cluster_dir.path().display()),
        "-Daeron.cluster.id=7".to_owned(),
        format!("-Daeron.cluster.members={member_string}"),
        format!(
            "-Daeron.cluster.consensus.channel=aeron:udp?endpoint=localhost:{}",
            ports.backup_consensus()
        ),
        format!(
            "-Daeron.cluster.consensus.endpoints=localhost:{}",
            ports.consensus(0)
        ),
        format!(
            "-Daeron.cluster.backup.catchup.endpoint=localhost:{}",
            ports.backup_catchup()
        ),
        "-Daeron.cluster.backup.cool.down.interval=1s".to_owned(),
    ];

    let mut backup = ReferenceDriver::spawn_with_args(&binary, &aeron_path, &args, &[])
        .expect("the backup binary starts");

    // The binary allocates its five counters once its driver and archive are
    // up; 208 says which state the agent is in.
    let deadline = Instant::now() + Duration::from_secs(90);
    let backup_counters = loop {
        if let Ok(counters) = deepmsg_cnc::CncFile::try_open(&aeron_path) {
            drop(counters);
            break NodeCounters::open(&aeron_path);
        }
        assert!(
            Instant::now() < deadline,
            "the backup never published its CnC file:\n{}",
            backup.log_tail(60)
        );
        std::thread::sleep(Duration::from_millis(20));
    };

    let mut backing_up = false;
    let mut live_position = None;
    while Instant::now() < deadline && !(backing_up && live_position.is_some_and(|p| p > 0)) {
        backing_up = backup_counters.value(CLUSTER_BACKUP_STATE_TYPE_ID)
            == Some(BackupState::BackingUp.code());
        live_position = backup_counters.value(CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        backing_up,
        "the backup reached BACKING_UP:\n{}",
        backup.log_tail(60)
    );
    assert!(
        live_position.is_some_and(|position| position > 0),
        "its live log position moved: {live_position:?}\n{}",
        backup.log_tail(60)
    );

    // `SIGTERM`: all three loops stop and the process answers 0
    // (`ClusterBackupMediaDriver`'s own signal path).
    let status = backup.stop().expect("the backup stops");
    assert!(
        status.success(),
        "a clean stop exits 0: {status:?}\n{}",
        backup.log_tail(60)
    );

    node.stop_media();
}
