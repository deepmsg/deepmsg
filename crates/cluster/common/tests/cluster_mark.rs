//! The cluster mark file, ported from `ClusterMarkFileTest` (18 cases) plus the
//! two the reference cannot have.
//!
//! Ported by name where the name still describes the same thing. Two of the
//! reference's cases are `@DisabledForJreRange(min = JRE.JAVA_21)` — the
//! `force()` spies — so they are counted as disabled in the P3-0b table and not
//! ported here.
//!
//! The last test reads a file *the reference wrote*: that is the LC-5 direction
//! that matters (Java writes, Rust reads), and it is the only test here whose
//! expected values did not come from this code.

use std::path::{Path, PathBuf};

use deepmsg_cluster_common::mark::{
    self, ClusterMarkError, ClusterMarkFile, ERROR_BUFFER_MAX_LENGTH, ERROR_BUFFER_MIN_LENGTH,
    HEADER_LENGTH, LINK_FILENAME, SEMANTIC_VERSION, VERSION_FAILED, major_of,
};
use deepmsg_codec_cluster_mark::cluster_component_type::ClusterComponentType;

const PAGE: usize = 4096;
const ERROR_BUFFER: usize = ERROR_BUFFER_MIN_LENGTH;
const NOW: i64 = 1_700_000_000_000;

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "deepmsg-cluster-mark-{name}-{}-{}",
        std::process::id(),
        NOW
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn create(dir: &Path, kind: ClusterComponentType) -> ClusterMarkFile {
    ClusterMarkFile::create(
        &dir.join(mark::FILENAME),
        kind,
        ERROR_BUFFER,
        PAGE,
        NOW,
        4321,
    )
    .unwrap()
}

#[test]
fn throws_exception_if_error_buffer_length_is_invalid() {
    let dir = scratch("buffer-length");
    for length in [0, ERROR_BUFFER_MIN_LENGTH - 1, ERROR_BUFFER_MAX_LENGTH + 1] {
        let result = ClusterMarkFile::create(
            &dir.join(mark::FILENAME),
            ClusterComponentType::CONSENSUS_MODULE,
            length,
            PAGE,
            NOW,
            1,
        );
        assert!(
            matches!(result, Err(ClusterMarkError::ErrorBufferLength(_))),
            "length {length} should be refused"
        );
    }
}

#[test]
fn should_create_new_mark_file() {
    for kind in [
        ClusterComponentType::CONSENSUS_MODULE,
        ClusterComponentType::CONTAINER,
        ClusterComponentType::BACKUP,
    ] {
        let dir = scratch(&format!("create-{kind:?}"));
        let file = create(&dir, kind);
        assert_eq!(file.component_type(), Some(kind));
        assert_eq!(file.pid(), Some(4321));
        assert_eq!(file.start_timestamp(), Some(NOW));
        assert_eq!(file.candidate_term_id(), Some(-1));
        assert_eq!(file.header_length(), Some(HEADER_LENGTH as i32));
        assert_eq!(
            file.recorded_error_buffer_length(),
            Some(ERROR_BUFFER as i32)
        );
    }
}

#[test]
fn should_update_existing_mark_file() {
    let dir = scratch("update");
    let first = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    first.signal_terminated().unwrap();
    drop(first);

    let second = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    assert_eq!(second.pid(), Some(4321));
    assert_eq!(
        second.component_type(),
        Some(ClusterComponentType::CONSENSUS_MODULE)
    );
}

#[test]
fn cluster_id_accessors() {
    let dir = scratch("cluster-id");
    let file = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    assert_eq!(file.cluster_id(), Some(0));
    file.set_cluster_id(42);
    assert_eq!(file.cluster_id(), Some(42));
}

#[test]
fn member_id_accessors() {
    let dir = scratch("member-id");
    let file = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    assert_eq!(file.member_id(), Some(0));
    file.set_member_id(5);
    assert_eq!(file.member_id(), Some(5));
}

#[test]
fn candidate_term_id_accessors() {
    let dir = scratch("candidate-term");
    let first = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    assert_eq!(first.candidate_term_id(), Some(-1));
    first.signal_terminated().unwrap();
    drop(first);

    let second = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    assert_eq!(
        second.candidate_term_id(),
        Some(-1),
        "inherited from the file"
    );
}

#[test]
fn activity_timestamp_accessors() {
    let dir = scratch("activity");
    let file = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    assert_eq!(file.activity_timestamp(), Some(NOW));
    file.update_activity_timestamp(NOW + 1_000);
    assert_eq!(file.activity_timestamp(), Some(NOW + 1_000));
}

#[test]
fn load_control_properties_accessors() {
    let dir = scratch("var-data");
    let file = create(&dir, ClusterComponentType::CONTAINER);
    file.write_var_data(&[
        "aeron-dir",
        "aeron:ipc?term-length=128k",
        "",
        "echo",
        "",
        "",
    ])
    .unwrap();
    assert_eq!(file.var_data(0).as_deref(), Some("aeron-dir"));
    assert_eq!(
        file.var_data(1).as_deref(),
        Some("aeron:ipc?term-length=128k")
    );
    assert_eq!(file.var_data(2).as_deref(), Some(""));
}

#[test]
fn signal_ready() {
    let dir = scratch("ready");
    let file = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    file.signal_ready(NOW + 500).unwrap();
    assert_eq!(file.version(), Some(SEMANTIC_VERSION));
    assert_eq!(file.activity_timestamp(), Some(NOW + 500));
}

#[test]
fn signal_failed_start() {
    let dir = scratch("failed");
    let file = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    file.signal_failed_start().unwrap();
    assert_eq!(file.version(), Some(VERSION_FAILED));
    assert_eq!(file.activity_timestamp(), Some(-1));
    assert_eq!(
        major_of(VERSION_FAILED),
        255,
        "a failed start is not major 0"
    );
}

#[test]
fn should_align_mark_file_length_based_on_the_file_page_size_from_aeron_client() {
    for page in [4096_usize, 8192, 16384] {
        let dir = scratch(&format!("align-{page}"));
        let file = ClusterMarkFile::create(
            &dir.join(mark::FILENAME),
            ClusterComponentType::CONSENSUS_MODULE,
            ERROR_BUFFER,
            page,
            NOW,
            1,
        )
        .unwrap();
        assert_eq!(file.length() % page, 0);
        assert!(file.length() >= HEADER_LENGTH + ERROR_BUFFER);
    }
}

#[test]
fn should_reject_file_page_size_if_not_power_of_2() {
    let dir = scratch("page-not-pow2");
    let result = ClusterMarkFile::create(
        &dir.join(mark::FILENAME),
        ClusterComponentType::CONSENSUS_MODULE,
        ERROR_BUFFER,
        4097,
        NOW,
        1,
    );
    assert!(matches!(result, Err(ClusterMarkError::PageSize(4097))));
}

#[test]
fn should_reject_file_page_size_if_too_small() {
    let dir = scratch("page-small");
    let result = ClusterMarkFile::create(
        &dir.join(mark::FILENAME),
        ClusterComponentType::CONSENSUS_MODULE,
        ERROR_BUFFER,
        2048,
        NOW,
        1,
    );
    assert!(matches!(result, Err(ClusterMarkError::PageSize(2048))));
}

#[test]
fn should_reject_file_page_size_if_too_big() {
    let dir = scratch("page-big");
    let result = ClusterMarkFile::create(
        &dir.join(mark::FILENAME),
        ClusterComponentType::CONSENSUS_MODULE,
        ERROR_BUFFER,
        mark::PAGE_MAX_SIZE * 2,
        NOW,
        1,
    );
    assert!(matches!(result, Err(ClusterMarkError::PageSize(_))));
}

#[test]
fn should_unmap_buffer_upon_close() {
    let dir = scratch("unmap");
    let path = dir.join(mark::FILENAME);
    {
        let file = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
        file.signal_terminated().unwrap();
    }
    // Dropping released the mapping, so the file can be removed — which a live
    // mapping on Linux would not prevent, but a second create over it must
    // still succeed and see the old candidate term id.
    let again = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    assert_eq!(again.path(), path);
}

#[test]
fn should_handle_existing_mark_file_v0() {
    // The reference *reads* a pre-SBE-header file and rewrites a new header.
    // This build refuses it, and that is a deliberate departure: P3 has no
    // producer of the old format among its peers (1.53.2 writes the new one),
    // and silently rewriting somebody else's file is worse than saying so.
    let dir = scratch("v0");
    let path = dir.join(mark::FILENAME);
    std::fs::write(&path, vec![0_u8; HEADER_LENGTH + ERROR_BUFFER]).unwrap();
    let result = ClusterMarkFile::create(
        &path,
        ClusterComponentType::CONSENSUS_MODULE,
        ERROR_BUFFER,
        PAGE,
        NOW,
        1,
    );
    assert!(matches!(
        result,
        Err(ClusterMarkError::Uninitialised | ClusterMarkError::Version { .. })
    ));
}

#[test]
fn a_backup_may_be_taken_over_by_a_consensus_module_and_nothing_else_may() {
    let dir = scratch("takeover");
    let backup = create(&dir, ClusterComponentType::BACKUP);
    backup.signal_terminated().unwrap();
    drop(backup);

    // BACKUP -> CONSENSUS_MODULE is the one tolerated migration.
    let cm = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    assert_eq!(
        cm.component_type(),
        Some(ClusterComponentType::CONSENSUS_MODULE)
    );
    cm.signal_terminated().unwrap();
    drop(cm);

    // CONSENSUS_MODULE -> BACKUP is not.
    let result = ClusterMarkFile::create(
        &dir.join(mark::FILENAME),
        ClusterComponentType::BACKUP,
        ERROR_BUFFER,
        PAGE,
        NOW,
        1,
    );
    assert!(matches!(
        result,
        Err(ClusterMarkError::ComponentType { .. })
    ));
}

#[test]
fn an_active_mark_file_is_refused_and_a_terminated_one_is_not() {
    let dir = scratch("active");
    let path = dir.join(mark::FILENAME);
    let file = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    assert!(ClusterMarkFile::is_active(&path, NOW, 10_000));
    assert!(!ClusterMarkFile::is_active(&path, NOW + 60_000, 10_000));
    file.signal_terminated().unwrap();
    assert!(!ClusterMarkFile::is_active(&path, NOW, 10_000));
}

#[test]
fn an_existing_error_log_is_rescued_before_being_cleared() {
    let dir = scratch("errors");
    let file = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    {
        let buffer = file.error_buffer().unwrap();
        buffer.copy_in(0, b"previous death").unwrap();
    }
    let saved = file.save_existing_errors("consensus-module", NOW).unwrap();
    let saved = saved.expect("an error log with content is rescued");
    assert_eq!(std::fs::read(&saved).unwrap().len(), ERROR_BUFFER);
    assert_eq!(&std::fs::read(&saved).unwrap()[..14], b"previous death");
    file.clear_error_buffer();
    assert!(
        file.save_existing_errors("consensus-module", NOW)
            .unwrap()
            .is_none()
    );
}

#[test]
fn the_link_points_at_the_mark_files_directory_or_is_absent() {
    let dir = scratch("link");
    let outside = scratch("link-outside");
    let path = outside.join(mark::FILENAME);
    let file = ClusterMarkFile::create(
        &path,
        ClusterComponentType::CONSENSUS_MODULE,
        ERROR_BUFFER,
        PAGE,
        NOW,
        1,
    )
    .unwrap();
    mark::ensure_mark_file_link(&dir, file.path()).unwrap();
    let link = dir.join(LINK_FILENAME);
    assert!(link.exists());
    let target = std::fs::read_to_string(&link).unwrap();
    assert!(target.contains("link-outside"));

    // A mark file inside the directory it describes removes the link.
    let inside = create(&dir, ClusterComponentType::CONSENSUS_MODULE);
    mark::ensure_mark_file_link(&dir, inside.path()).unwrap();
    assert!(!link.exists());
}

#[test]
fn the_reference_written_header_reads_back() {
    // The golden is the reference's own bytes (see fixtures/cluster-mark-header.txt).
    const GOLDEN: &[u8] = include_bytes!("fixtures/cluster-mark-header.bin");
    let dir = scratch("golden");
    let path = dir.join(mark::FILENAME);

    // A file the reference wrote is a full file: header plus a 1 MiB error
    // buffer. Rebuild the tail so the mapping is the length a reader expects.
    let mut bytes = GOLDEN.to_vec();
    bytes.resize(HEADER_LENGTH + ERROR_BUFFER, 0);
    std::fs::write(&path, &bytes).unwrap();

    let mapping = deepmsg_core::pal::MappedFile::open_readonly(&path).unwrap();
    let header = mapping.region(0, HEADER_LENGTH).unwrap();

    assert_eq!(header.load_u8(0), Some(128), "blockLength low byte");
    assert_eq!(header.load_u8(2), Some(200), "templateId low byte");
    assert_eq!(header.load_u8(4), Some(110), "schemaId low byte");
    assert_eq!(header.load_i32(8), Some(SEMANTIC_VERSION), "version");
    assert_eq!(
        header.load_i32(12),
        Some(ClusterComponentType::CONSENSUS_MODULE as i32),
        "componentType"
    );
    assert_eq!(
        header.load_i64(16),
        Some(1_700_000_000_123),
        "activityTimestamp"
    );
    assert_eq!(
        header.load_i64(24),
        Some(1_700_000_000_000),
        "startTimestamp"
    );
    assert_eq!(header.load_i64(40), Some(-1), "candidateTermId");
    // Body offsets, schema order: memberId +56, serviceId +60, headerLength
    // +64, errorBufferLength +68, clusterId +72 — plus the 8-byte SBE header.
    assert_eq!(header.load_i32(64), Some(3), "memberId");
    assert_eq!(header.load_i32(68), Some(0), "serviceId");
    assert_eq!(
        header.load_i32(72),
        Some(HEADER_LENGTH as i32),
        "headerLength"
    );
    assert_eq!(
        header.load_i32(76),
        Some(ERROR_BUFFER as i32),
        "errorBufferLength"
    );
    assert_eq!(header.load_i32(80), Some(7), "clusterId");
}
