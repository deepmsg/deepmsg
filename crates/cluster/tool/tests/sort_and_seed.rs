//! `sort-recording-log` and `seed-recording-log-from-snapshot`, the seven
//! scenarios `ClusterToolTest` devotes to them (`ClusterToolTest.java:223-368`,
//! `:427-486`).

use std::path::{Path, PathBuf};

use deepmsg_cluster::persist::recording_log::{
    CONSENSUS_MODULE_SERVICE_ID, ENTRY_TYPE_SNAPSHOT, Entry, RecordingLog,
};
use deepmsg_cluster_tool::command;
use deepmsg_cluster_tool::operator::{ClusterTool, FAILURE, SUCCESS};

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "deepmsg-cluster-tool-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn log_path(dir: &Path) -> PathBuf {
    dir.join(deepmsg_cluster::persist::recording_log::FILENAME)
}

fn backup_path(dir: &Path) -> PathBuf {
    dir.join("recording.log.bak")
}

#[test]
fn sort_recording_log_is_a_no_op_if_the_log_is_empty() {
    let dir = scratch("sort-empty");
    // `new RecordingLog(clusterDir, true)` creates the file.
    drop(RecordingLog::open(&log_path(&dir)).unwrap());

    let mut out = Vec::new();
    assert_eq!(
        ClusterTool::default().sort_recording_log(&dir, &mut out),
        FAILURE
    );
    assert_eq!(std::fs::metadata(log_path(&dir)).unwrap().len(), 0);
}

#[test]
fn sort_recording_log_is_a_no_op_if_the_log_does_not_exist() {
    let dir = scratch("sort-missing");
    let mut out = Vec::new();
    assert_eq!(
        ClusterTool::default().sort_recording_log(&dir, &mut out),
        FAILURE
    );
    assert!(!log_path(&dir).exists());
}

#[test]
fn sort_recording_log_is_a_no_op_if_the_log_is_already_sorted() {
    let dir = scratch("sort-sorted");
    {
        let mut log = RecordingLog::open(&log_path(&dir)).unwrap();
        log.append_term(1, 0, 100, 1, 10, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
        log.append_snapshot(1, 0, 100, 2, 11, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
        log.append_term(2, 100, 200, 3, 12, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
    }
    let before = std::fs::read(log_path(&dir)).unwrap();

    let mut out = Vec::new();
    assert_eq!(
        ClusterTool::default().sort_recording_log(&dir, &mut out),
        FAILURE
    );
    assert_eq!(std::fs::read(log_path(&dir)).unwrap(), before);
}

#[test]
fn sort_recording_log_rearranges_data_on_disc() {
    let dir = scratch("sort-unsorted");
    {
        let mut log = RecordingLog::open(&log_path(&dir)).unwrap();
        // Written term 2 first, then term 1: the view sorts them, the file does
        // not.
        log.append_term(2, 100, 200, 3, 12, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
        log.append_term(1, 0, 100, 1, 10, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
    }
    let before = std::fs::read(log_path(&dir)).unwrap();

    let mut out = Vec::new();
    assert_eq!(
        ClusterTool::default().sort_recording_log(&dir, &mut out),
        SUCCESS
    );
    let after = std::fs::read(log_path(&dir)).unwrap();
    assert_ne!(after, before, "the file was rewritten");
    assert!(!dir.join("recording.log.tmp").exists());

    let reread = RecordingLog::open(&log_path(&dir)).unwrap();
    assert!(reread.is_sorted());
    assert_eq!(
        reread
            .entries()
            .iter()
            .map(|entry| entry.leadership_term_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn seed_recording_log_deletes_the_log_when_no_valid_snapshot_is_left() {
    let dir = scratch("seed-none");
    {
        let mut log = RecordingLog::open(&log_path(&dir)).unwrap();
        log.append_term(1, 0, 100, 1, 10, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
        log.append_snapshot(1, 0, 100, 2, 11, 0).unwrap();
        log.append_snapshot(1, 0, 100, 3, 12, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
        log.append_term(2, 100, 200, 4, 13, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
        log.append_snapshot(2, 100, 200, 5, 14, 5).unwrap();
        assert!(log.invalidate_latest_snapshot().unwrap().is_some());
    }
    let before = std::fs::read(log_path(&dir)).unwrap();

    let mut out = Vec::new();
    assert_eq!(
        ClusterTool::default().seed_recording_log_from_snapshot(&dir, &mut out),
        SUCCESS
    );
    assert!(!log_path(&dir).exists());
    assert!(backup_path(&dir).exists());
    assert_eq!(std::fs::read(backup_path(&dir)).unwrap(), before);
}

#[test]
fn seed_recording_log_creates_a_new_log_from_the_latest_valid_snapshot() {
    let dir = scratch("seed");
    let preexisting_backup = [0x1_u8, 0x80, 0, 1, 0xff, 0x7f];
    std::fs::write(backup_path(&dir), preexisting_backup).unwrap();

    let mut log = RecordingLog::open(&log_path(&dir)).unwrap();
    log.append_term(0, 0, -1, 0, 1, CONSENSUS_MODULE_SERVICE_ID)
        .unwrap();
    log.append_snapshot(3, 4000, 4000, 600, 1, 2).unwrap();
    log.append_term(3, 3000, -1, 500, 1, CONSENSUS_MODULE_SERVICE_ID)
        .unwrap();
    log.append_snapshot(2, 2900, 2200, 400, 1, 2).unwrap();
    log.append_snapshot(2, 2900, 2200, 400, 1, 1).unwrap();
    log.append_snapshot(2, 2900, 2200, 400, 1, 0).unwrap();
    log.append_snapshot(2, 2900, 2200, 400, 1, CONSENSUS_MODULE_SERVICE_ID)
        .unwrap();
    log.append_term(2, 2000, -1, 300, 1, CONSENSUS_MODULE_SERVICE_ID)
        .unwrap();
    log.append_snapshot(1, 1800, 1000, 200, 1, CONSENSUS_MODULE_SERVICE_ID)
        .unwrap();
    log.append_snapshot(1, 1800, 1000, 200, 1, 0).unwrap();
    log.append_snapshot(1, 1800, 1000, 200, 1, 1).unwrap();
    log.append_term(1, 1000, -1, 100, 1, CONSENSUS_MODULE_SERVICE_ID)
        .unwrap();
    assert!(log.invalidate_latest_snapshot().unwrap().is_some());

    // The three snapshots of term 1 at 1800, sorted view indices 2..4.
    let expected: Vec<Entry> = log.entries()[2..5]
        .iter()
        .map(|entry| Entry {
            position: 0,
            recording_id: entry.recording_id,
            leadership_term_id: entry.leadership_term_id,
            term_base_log_position: 0,
            log_position: 0,
            timestamp: entry.timestamp,
            service_id: entry.service_id,
            entry_type: entry.entry_type,
            is_valid: entry.is_valid,
            archive_endpoint: None,
            entry_index: 0,
        })
        .collect();
    assert_eq!(expected.len(), 3);
    assert!(
        expected
            .iter()
            .all(|entry| entry.entry_type == ENTRY_TYPE_SNAPSHOT)
    );

    let log_contents = std::fs::read(log_path(&dir)).unwrap();
    let modified = std::fs::metadata(log_path(&dir))
        .unwrap()
        .modified()
        .unwrap();
    drop(log);

    let mut out = Vec::new();
    assert_eq!(
        ClusterTool::default().seed_recording_log_from_snapshot(&dir, &mut out),
        SUCCESS
    );

    // The new log is exactly the truncated run, read back through the sorted
    // view.
    let reread = RecordingLog::open(&log_path(&dir)).unwrap();
    let actual: Vec<(i64, i64, i64, i64, i32, i32, bool)> = reread
        .entries()
        .iter()
        .map(|entry| {
            (
                entry.recording_id,
                entry.term_base_log_position,
                entry.log_position,
                entry.timestamp,
                entry.service_id,
                entry.entry_type,
                entry.is_valid,
            )
        })
        .collect();
    let expected_shape: Vec<(i64, i64, i64, i64, i32, i32, bool)> = expected
        .iter()
        .map(|entry| {
            (
                entry.recording_id,
                entry.term_base_log_position,
                entry.log_position,
                entry.timestamp,
                entry.service_id,
                entry.entry_type,
                entry.is_valid,
            )
        })
        .collect();
    assert_eq!(actual, expected_shape);

    // The backup is the original log, with its modification time.
    assert_eq!(std::fs::read(backup_path(&dir)).unwrap(), log_contents);
    let backed_up = std::fs::metadata(backup_path(&dir))
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(
        backed_up
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        modified
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        "the backup keeps the log's modification time"
    );
}

#[test]
fn seed_recording_log_from_the_command_line_writes_the_same_log() {
    let dir = scratch("seed-cli");
    {
        let mut log = RecordingLog::open(&log_path(&dir)).unwrap();
        log.append_term(1, 0, 100, 1, 10, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
        log.append_snapshot(1, 0, 100, 2, 11, CONSENSUS_MODULE_SERVICE_ID)
            .unwrap();
        log.append_snapshot(1, 0, 100, 3, 12, 0).unwrap();
    }

    let mut out = Vec::new();
    let mut err = Vec::new();
    let status = command::run(
        &[
            dir.to_string_lossy().into_owned(),
            "seed-recording-log-from-snapshot".to_owned(),
        ],
        &mut out,
        &mut err,
    );
    assert_eq!(status, SUCCESS);
    assert!(backup_path(&dir).exists());

    let reread = RecordingLog::open(&log_path(&dir)).unwrap();
    assert_eq!(reread.entries().len(), 2);
    // Descending service id is the view's order at the same position.
    assert_eq!(reread.entries()[0].service_id, 0);
    assert_eq!(reread.entries()[1].service_id, CONSENSUS_MODULE_SERVICE_ID);
    assert!(
        reread
            .entries()
            .iter()
            .all(|entry| entry.term_base_log_position == 0 && entry.log_position == 0)
    );
}
