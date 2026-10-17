//! `node-state.dat`, following `NodeStateFileTest`'s seven cases.
//!
//! The one this file exists for is the write order: position and timestamp
//! plainly, then the term id with release ordering, then force. A reader that
//! sees the new term id must also see the position and timestamp that go with
//! it, which is why the last write is the released one.

use std::path::{Path, PathBuf};

use deepmsg_cluster::persist::node_state::{
    self, MINIMUM_FILE_LENGTH, NULL_VALUE, NodeStateError, NodeStateFile,
};

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "deepmsg-node-state-{name}-{}-{}",
        std::process::id(),
        1_700_000_000_000_i64
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn path(dir: &Path) -> PathBuf {
    dir.join(node_state::FILENAME)
}

#[test]
fn a_created_file_is_one_mebibyte_with_no_candidate_term() {
    let dir = scratch("create");
    let file = NodeStateFile::open_or_create(&path(&dir)).unwrap();
    assert_eq!(
        std::fs::metadata(path(&dir)).unwrap().len(),
        MINIMUM_FILE_LENGTH as u64
    );
    assert_eq!(file.candidate_term_id(), NULL_VALUE);
    assert_eq!(file.candidate_term().log_position, 0);
}

#[test]
fn the_header_and_footer_sit_where_the_layout_says() {
    let dir = scratch("layout");
    NodeStateFile::open_or_create(&path(&dir)).unwrap();
    let bytes = std::fs::read(path(&dir)).unwrap();
    // Header: a bare body, version 0.3.0 as the mark file's semantic version.
    assert_eq!(&bytes[0..4], &(3u32 << 8).to_le_bytes());
    // CandidateTerm frame at 8: blockLength 24, templateId 301, schemaId 112,
    // version 10, frameLength 40.
    assert_eq!(&bytes[8..10], &24u16.to_le_bytes());
    assert_eq!(&bytes[10..12], &301u16.to_le_bytes());
    assert_eq!(&bytes[12..14], &112u16.to_le_bytes());
    assert_eq!(&bytes[14..16], &10u16.to_le_bytes());
    assert_eq!(&bytes[16..20], &40i32.to_le_bytes());
    // Footer at 48: templateId 304, frameLength 16.
    assert_eq!(&bytes[50..52], &304u16.to_le_bytes());
    assert_eq!(&bytes[56..60], &16i32.to_le_bytes());
}

#[test]
fn an_update_writes_all_three_fields() {
    let dir = scratch("update");
    let file = NodeStateFile::open_or_create(&path(&dir)).unwrap();
    file.update_candidate_term_id(7, 1_234, 1_700_000_000_000)
        .unwrap();
    let term = file.candidate_term();
    assert_eq!(term.candidate_term_id, 7);
    assert_eq!(term.log_position, 1_234);
    assert_eq!(term.timestamp_ms, 1_700_000_000_000);

    // And it is on disk, not just in the mapping.
    let reopened = NodeStateFile::open(&path(&dir)).unwrap();
    assert_eq!(reopened.candidate_term(), term);
}

#[test]
fn propose_max_only_moves_upwards() {
    let dir = scratch("propose");
    let file = NodeStateFile::open_or_create(&path(&dir)).unwrap();
    file.propose_max_candidate_term_id(5, 10, 100).unwrap();
    assert_eq!(file.candidate_term_id(), 5);

    file.propose_max_candidate_term_id(4, 20, 200).unwrap();
    assert_eq!(file.candidate_term_id(), 5, "a smaller term is ignored");
    assert_eq!(
        file.candidate_term().log_position,
        10,
        "and so is its position"
    );

    file.propose_max_candidate_term_id(6, 30, 300).unwrap();
    assert_eq!(file.candidate_term_id(), 6);
    assert_eq!(file.candidate_term().log_position, 30);
}

#[test]
fn a_file_from_another_major_is_refused() {
    let dir = scratch("version");
    let file = NodeStateFile::open_or_create(&path(&dir)).unwrap();
    drop(file);
    let mut bytes = std::fs::read(path(&dir)).unwrap();
    // Major 1: semantic version 1.0.0.
    bytes[0..4].copy_from_slice(&(1u32 << 16).to_le_bytes());
    std::fs::write(path(&dir), &bytes).unwrap();

    let result = NodeStateFile::open(&path(&dir));
    assert!(matches!(
        result,
        Err(NodeStateError::Version { found: 1, .. })
    ));
}

#[test]
fn a_missing_footer_is_corruption() {
    let dir = scratch("corrupt");
    let file = NodeStateFile::open_or_create(&path(&dir)).unwrap();
    drop(file);
    let mut bytes = std::fs::read(path(&dir)).unwrap();
    // Wipe the footer's template id.
    bytes[50..52].copy_from_slice(&0u16.to_le_bytes());
    std::fs::write(path(&dir), &bytes).unwrap();

    assert!(matches!(
        NodeStateFile::open(&path(&dir)),
        Err(NodeStateError::Corrupt)
    ));
}

#[test]
fn reopening_an_absent_file_creates_it_and_an_existing_one_is_kept() {
    let dir = scratch("reopen");
    let first = NodeStateFile::open_or_create(&path(&dir)).unwrap();
    first.update_candidate_term_id(9, 99, 999).unwrap();
    drop(first);

    let second = NodeStateFile::open_or_create(&path(&dir)).unwrap();
    assert_eq!(second.candidate_term_id(), 9);
}
