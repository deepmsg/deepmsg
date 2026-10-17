//! `recording.log` behaviour, against files this build writes and reads back.
//!
//! The two that matter: disk order is not logical order (appends can back-fill
//! an older term, so the view is sorted), and the three in-place edits rewrite
//! a field at the entry's recorded offset rather than appending a correction.

use std::path::{Path, PathBuf};

use deepmsg_cluster::persist::recording_log::{
    CONSENSUS_MODULE_SERVICE_ID, ENTRY_TYPE_SNAPSHOT, ENTRY_TYPE_TERM, RecordingLog,
    RecordingLogError,
};

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "deepmsg-recording-log-{name}-{}-{}",
        std::process::id(),
        1_700_000_000_000_i64
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("recording.log")
}

fn open(path: &Path) -> RecordingLog {
    RecordingLog::open(path).unwrap()
}

#[test]
fn a_new_log_is_empty_and_a_term_entry_is_seventy_two_bytes_on_disk() {
    let path = scratch("empty");
    let mut log = open(&path);
    assert!(log.entries().is_empty());
    assert_eq!(log.length(), 0);

    log.append_term(
        1,
        0,
        100,
        1_700_000_000_000,
        42,
        CONSENSUS_MODULE_SERVICE_ID,
    )
    .unwrap();
    // 48 bytes of fields, aligned to 64.
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 64);
}

#[test]
fn a_term_entry_round_trips_through_the_file() {
    let path = scratch("roundtrip");
    let mut log = open(&path);
    log.append_term(
        3,
        1_000,
        1_500,
        1_700_000_000_000,
        42,
        CONSENSUS_MODULE_SERVICE_ID,
    )
    .unwrap();
    drop(log);

    let reopened = open(&path);
    let entry = &reopened.entries()[0];
    assert_eq!(entry.leadership_term_id, 3);
    assert_eq!(entry.term_base_log_position, 1_000);
    assert_eq!(entry.log_position, 1_500);
    assert_eq!(entry.recording_id, 42);
    assert_eq!(entry.service_id, CONSENSUS_MODULE_SERVICE_ID);
    assert_eq!(entry.entry_type, ENTRY_TYPE_TERM);
    assert!(entry.is_valid);
}

#[test]
fn entries_are_appended_on_sixty_four_byte_boundaries() {
    let path = scratch("align");
    let mut log = open(&path);
    log.append_term(1, 0, 1, 0, 1, -1).unwrap();
    log.append_term(2, 1, 2, 0, 2, -1).unwrap();
    let positions: Vec<u64> = log.entries().iter().map(|e| e.position).collect();
    assert_eq!(positions, vec![0, 64]);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 128);
}

#[test]
fn disk_order_is_not_logical_order() {
    // Back-fill an older term after a newer one: the file keeps append order,
    // the view must not.
    let path = scratch("sort");
    let mut log = open(&path);
    log.append_term(5, 500, 600, 0, 5, -1).unwrap();
    log.append_term(4, 400, 450, 0, 4, -1).unwrap();
    let terms: Vec<i64> = log.entries().iter().map(|e| e.leadership_term_id).collect();
    assert_eq!(terms, vec![4, 5]);

    // And it is the view that is sorted, not the file.
    let raw = std::fs::read(&path).unwrap();
    assert_eq!(i64::from_le_bytes(raw[8..16].try_into().unwrap()), 5);
}

#[test]
fn a_snapshot_sorts_after_the_term_at_the_same_base() {
    let path = scratch("snapshot-order");
    let mut log = open(&path);
    log.append_snapshot(2, 100, 150, 0, 20, -1).unwrap();
    log.append_term(2, 100, 200, 0, 10, -1).unwrap();
    let types: Vec<i32> = log.entries().iter().map(|e| e.entry_type).collect();
    assert_eq!(types, vec![ENTRY_TYPE_TERM, ENTRY_TYPE_SNAPSHOT]);
}

#[test]
fn commit_rewrites_eight_bytes_at_the_entries_own_offset() {
    let path = scratch("commit");
    let mut log = open(&path);
    log.append_term(1, 0, 10, 0, 1, -1).unwrap();
    log.append_term(2, 10, 20, 0, 2, -1).unwrap();
    let length_before = std::fs::metadata(&path).unwrap().len();

    log.commit_log_position(0, 42).unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        length_before,
        "an in-place edit does not grow the file"
    );
    let raw = std::fs::read(&path).unwrap();
    assert_eq!(i64::from_le_bytes(raw[24..32].try_into().unwrap()), 42);

    drop(log);
    assert_eq!(open(&path).entries()[0].log_position, 42);
}

#[test]
fn invalidating_a_snapshot_sets_the_top_bit_and_restoring_clears_it() {
    let path = scratch("invalidate");
    let mut log = open(&path);
    log.append_term(1, 0, 0, 0, 1, -1).unwrap();
    log.append_snapshot(1, 0, 5, 0, 2, -1).unwrap();
    let index = 1;
    let position = log.entries()[index].position;

    assert_eq!(log.invalidate_latest_snapshot().unwrap(), Some(index));
    // The same offset, bit 31 of the type field.
    let raw = std::fs::read(&path).unwrap();
    let type_at = position as usize + 44;
    let value = i32::from_le_bytes(raw[type_at..type_at + 4].try_into().unwrap());
    assert_eq!(value, ENTRY_TYPE_SNAPSHOT | (1 << 31));

    drop(log);
    assert!(!open(&path).entries()[index].is_valid);

    let mut log = open(&path);
    log.restore_invalid_snapshot(index).unwrap();
    drop(log);
    let reopened = open(&path);
    assert!(reopened.entries()[index].is_valid);
    assert_eq!(reopened.entries()[index].position, position, "same slot");
}

#[test]
fn invalidating_with_no_snapshot_answers_none() {
    let path = scratch("no-snapshot");
    let mut log = open(&path);
    log.append_term(1, 0, 0, 0, 1, -1).unwrap();
    assert_eq!(log.invalidate_latest_snapshot().unwrap(), None);
}

#[test]
fn force_levels_follow_the_reference() {
    let path = scratch("force");
    let mut log = open(&path);
    log.append_term(1, 0, 0, 0, 1, -1).unwrap();
    assert!(log.force(0).is_ok());
    assert!(log.force(1).is_ok());
    assert!(log.force(2).is_ok());
}

#[test]
fn a_truncated_tail_is_ignored_rather_than_read_as_an_entry() {
    let path = scratch("truncated");
    let mut log = open(&path);
    log.append_term(1, 0, 0, 0, 1, -1).unwrap();
    drop(log);
    // Cut the file in the middle of the first entry.
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, &bytes[..30]).unwrap();

    let reopened = open(&path);
    assert!(reopened.entries().is_empty());
}

#[test]
fn an_oversized_endpoint_is_refused() {
    let path = scratch("endpoint");
    let mut log = open(&path);
    let result = log.append(deepmsg_cluster::persist::recording_log::Entry {
        position: 0,
        recording_id: 1,
        leadership_term_id: 1,
        term_base_log_position: 0,
        log_position: 0,
        timestamp: 0,
        service_id: -1,
        entry_type: deepmsg_cluster::persist::recording_log::ENTRY_TYPE_STANDBY_SNAPSHOT,
        is_valid: true,
        archive_endpoint: Some("x".repeat(5000)),
    });
    assert!(matches!(
        result,
        Err(RecordingLogError::EndpointTooLong { .. })
    ));
}
