//! The negative cases for `request_driver_termination`, against synthetic CnC
//! files.
//!
//! These mirror `aeron-driver/src/test/c/aeron_c_terminate_test.cpp`, which is
//! the reference's own suite for the same call. They need no driver, so unlike
//! the interop suite they run in CI — which matters, because they are the only
//! coverage most of these precondition branches will ever get.
//!
//! One of the reference's cases is green for the wrong reason and is not
//! copied: `shouldFailIfCncFileLengthIsInsufficient` builds a 192-byte file
//! with every region length zero, which makes
//! `aeron_cnc_is_file_length_sufficient` *pass* (`128 + 0 >= 192` is false, but
//! `192 >= 128` is true) and the rejection actually come from the ring
//! initialiser two steps later. Ours patches a region length upward so the
//! length check itself is what refuses the file.

use std::io::{Seek, SeekFrom, Write as _};
use std::path::{Path, PathBuf};

use deepmsg_client::terminate::{TerminationError, TerminationOutcome, request_driver_termination};
use deepmsg_cnc::layout;

// Region sizes chosen so the whole file is small but every structure inside it
// is real: the ring has a 1024-byte capacity (a power of two, so
// `max_message_length` is 128), and the counter regions keep the reference's
// 4:1 ratio.
const TO_DRIVER: usize = 1024 + layout::MPSC_RB_TRAILER_LENGTH;
const TO_CLIENTS: usize = 1024 + layout::BROADCAST_TRAILER_LENGTH;
const COUNTERS_VALUES: usize = 2 * layout::COUNTER_VALUE_LENGTH;
const COUNTERS_METADATA: usize = 4 * COUNTERS_VALUES;
const ERROR_LOG: usize = 8 * layout::ERROR_LOG_HEADER_LENGTH;
const SUM: usize = layout::VERSION_AND_METADATA_LENGTH
    + TO_DRIVER
    + TO_CLIENTS
    + COUNTERS_METADATA
    + COUNTERS_VALUES
    + ERROR_LOG;
const FILE_LENGTH: usize = 8192; // SUM (4544) rounded up to the page size below

const RING_CAPACITY: usize = 1024;

/// A synthetic `cnc.dat`, removed when dropped.
struct Synthetic {
    dir: PathBuf,
}

impl Synthetic {
    fn new(version: i32) -> Self {
        static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("deepmsg-terminate-{}-{n}.dir", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the aeron directory");

        let mut bytes = vec![0u8; FILE_LENGTH];

        write_i32(
            &mut bytes,
            layout::TO_DRIVER_BUFFER_LENGTH_OFFSET,
            TO_DRIVER as i32,
        );
        write_i32(
            &mut bytes,
            layout::TO_CLIENTS_BUFFER_LENGTH_OFFSET,
            TO_CLIENTS as i32,
        );
        write_i32(
            &mut bytes,
            layout::COUNTER_METADATA_BUFFER_LENGTH_OFFSET,
            COUNTERS_METADATA as i32,
        );
        write_i32(
            &mut bytes,
            layout::COUNTER_VALUES_BUFFER_LENGTH_OFFSET,
            COUNTERS_VALUES as i32,
        );
        write_i32(
            &mut bytes,
            layout::ERROR_LOG_BUFFER_LENGTH_OFFSET,
            ERROR_LOG as i32,
        );
        write_i64(
            &mut bytes,
            layout::CLIENT_LIVENESS_TIMEOUT_OFFSET,
            10_000_000_000,
        );
        write_i64(
            &mut bytes,
            layout::START_TIMESTAMP_OFFSET,
            1_700_000_000_000,
        );
        write_i64(&mut bytes, layout::PID_OFFSET, 4242);
        write_i32(&mut bytes, layout::FILE_PAGE_SIZE_OFFSET, 4096);
        // Published last, as a driver does.
        write_i32(&mut bytes, layout::CNC_VERSION_OFFSET, version);

        write_file(&dir, &bytes);
        Self { dir }
    }

    fn path(&self) -> &Path {
        &self.dir
    }

    /// Rewrite one 4-byte field on disk.
    fn patch_i32(self, offset: usize, value: i32) -> Self {
        self.patch(offset, &value.to_le_bytes())
    }

    /// Rewrite one 8-byte field on disk.
    fn patch_i64(self, offset: usize, value: i64) -> Self {
        self.patch(offset, &value.to_le_bytes())
    }

    fn patch(self, offset: usize, bytes: &[u8]) -> Self {
        let path = self.dir.join(deepmsg_cnc::CNC_FILE_NAME);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("reopen the CnC file");
        file.seek(SeekFrom::Start(offset as u64)).expect("seek");
        file.write_all(bytes).expect("patch");
        file.sync_all().expect("sync");
        self
    }

    /// Where the to-driver ring's trailer starts in the file.
    fn ring_trailer(&self) -> usize {
        layout::VERSION_AND_METADATA_LENGTH + RING_CAPACITY
    }
}

impl Drop for Synthetic {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn write_i32(bytes: &mut [u8], offset: usize, value: i32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_i64(bytes: &mut [u8], offset: usize, value: i64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn write_file(dir: &Path, bytes: &[u8]) {
    let mut file =
        std::fs::File::create(dir.join(deepmsg_cnc::CNC_FILE_NAME)).expect("create cnc.dat");
    file.write_all(bytes).expect("write cnc.dat");
    file.sync_all().expect("sync cnc.dat");
}

#[test]
fn the_baseline_file_shape_is_what_the_tests_assume() {
    // If the constants at the top drift, every case below starts testing
    // something else. This says so once instead of in a dozen failures.
    assert_eq!(
        FILE_LENGTH,
        layout::align_up(SUM, 4096),
        "SUM is {SUM}, which should round up to {FILE_LENGTH}"
    );
    assert!(RING_CAPACITY.is_power_of_two());
    assert_eq!(128, RING_CAPACITY / 8, "max_message_length for this ring");
}

#[test]
fn commits_a_command_into_a_well_formed_file() {
    let cnc = Synthetic::new(deepmsg_core::version::CNC_VERSION);

    let outcome = request_driver_termination(cnc.path(), b"please stop").expect("should send");

    assert_eq!(TerminationOutcome::Committed, outcome);
}

#[test]
fn refuses_a_token_longer_than_the_reference_allows() {
    let cnc = Synthetic::new(deepmsg_core::version::CNC_VERSION);
    let token = vec![0u8; deepmsg_cnc::MAX_TOKEN_LENGTH + 1];

    // The reference passes a length longer than its buffer here and relies on
    // the length being checked first. So does this test.
    assert!(matches!(
        request_driver_termination(cnc.path(), &token),
        Err(TerminationError::TokenTooLong {
            limit: deepmsg_cnc::MAX_TOKEN_LENGTH,
            ..
        })
    ));
}

#[test]
fn a_missing_file_is_an_error_not_an_outcome() {
    let missing = std::env::temp_dir().join("deepmsg-terminate-absent.dir");

    assert!(matches!(
        request_driver_termination(&missing, b"x"),
        Err(TerminationError::Io(_))
    ));
}

#[test]
fn a_file_too_short_to_be_a_cnc_file_is_the_silent_no_op() {
    // The reference's `noOpIfCncFileIsEmpty`: 64 bytes, no error, return 0.
    let dir = std::env::temp_dir().join(format!(
        "deepmsg-terminate-short-{}.dir",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create");
    write_file(&dir, &[0u8; 64]);

    let outcome = request_driver_termination(&dir, b"please").expect("a no-op is not an error");

    assert_eq!(
        TerminationOutcome::NoCncFile,
        outcome,
        "a short file is a fact about the directory, not a failure"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn refuses_a_file_whose_major_version_differs() {
    let cnc = Synthetic::new(deepmsg_core::version::semantic_version_compose(1, 0, 0));

    assert!(matches!(
        request_driver_termination(cnc.path(), b"x"),
        Err(TerminationError::Incompatible(
            deepmsg_core::version::CncVersionCompatibility::MajorMismatch
        ))
    ));
}

#[test]
fn refuses_a_file_whose_minor_version_is_older() {
    let cnc = Synthetic::new(deepmsg_core::version::semantic_version_compose(0, 1, 0));

    assert!(matches!(
        request_driver_termination(cnc.path(), b"x"),
        Err(TerminationError::Incompatible(
            deepmsg_core::version::CncVersionCompatibility::InsufficientMinor
        ))
    ));
}

#[test]
fn refuses_a_file_whose_version_is_not_published() {
    // The reference's suite does not cover this at all — the case it calls
    // "empty cnc" is a length problem, not a version one.
    let cnc = Synthetic::new(0);

    assert!(matches!(
        request_driver_termination(cnc.path(), b"x"),
        Err(TerminationError::NotReady)
    ));
}

#[test]
fn classifies_a_negative_version_as_a_mismatch_where_the_reference_would_not() {
    // A deliberate departure, recorded rather than glossed.
    //
    // The reference's *writer* path folds anything `<= 0` into "not
    // initialised" (`aeron-client/src/main/c/aeron_context.c:601`), and its
    // *reader* path treats a negative as a version mismatch. The two disagree
    // with each other, and this follows the reader: `NotReady` is retryable, so
    // classifying corruption as such would make a caller spin out its whole
    // timeout on a file that can never become ready.
    let cnc = Synthetic::new(-1);

    assert!(matches!(
        request_driver_termination(cnc.path(), b"x"),
        Err(TerminationError::Incompatible(
            deepmsg_core::version::CncVersionCompatibility::MajorMismatch
        ))
    ));
}

#[test]
fn refuses_a_ring_whose_capacity_is_not_a_power_of_two() {
    // A capacity that divides evenly into the layout but is not a power of two:
    // 792 = 24 + 768. The reference rejects this at ring init
    // (`aeron_c_terminate_test.cpp:168-190`); so does ours, via the ring rather
    // than the layout, which is why the error is `UnreadableRing`.
    let cnc = Synthetic::new(deepmsg_core::version::CNC_VERSION)
        .patch_i32(layout::TO_DRIVER_BUFFER_LENGTH_OFFSET, 792);

    assert!(matches!(
        request_driver_termination(cnc.path(), b"x"),
        Err(TerminationError::UnreadableRing)
    ));
}

#[test]
fn refuses_a_file_the_regions_do_not_fit_inside() {
    // Unlike the reference's same-named case, this one genuinely trips the
    // length check: the regions claim a megabyte and the file is 8 KiB, so
    // `RegionLayout::compute` refuses before any ring is built.
    let cnc = Synthetic::new(deepmsg_core::version::CNC_VERSION)
        .patch_i32(layout::TO_DRIVER_BUFFER_LENGTH_OFFSET, 1024 * 1024);

    assert!(matches!(
        request_driver_termination(cnc.path(), b"x"),
        Err(TerminationError::Malformed(
            deepmsg_cnc::CncError::RegionsExceedFile { .. }
        ))
    ));
}

#[test]
fn refuses_a_token_longer_than_the_ring_can_carry() {
    // Past the ring's 128-byte message limit but inside AERON_MAX_PATH, so this
    // is the ring's bound doing the refusing rather than the reference's.
    let cnc = Synthetic::new(deepmsg_core::version::CNC_VERSION);
    let token = vec![0u8; 129];

    assert!(matches!(
        request_driver_termination(cnc.path(), &token),
        Err(TerminationError::TokenTooLong { limit: 128, .. })
    ));
}

#[test]
fn reports_a_full_ring_rather_than_pretending_to_have_sent_it() {
    let cnc = Synthetic::new(deepmsg_core::version::CNC_VERSION);
    // The producer position one byte below the capacity, with nothing consumed,
    // is the shape the reference pins at `aeron_c_terminate_test.cpp:217-244`.
    let tail = cnc.ring_trailer() + layout::MPSC_TAIL_POSITION_OFFSET;
    let cnc = cnc.patch_i64(tail, (RING_CAPACITY - 1) as i64);

    assert!(matches!(
        request_driver_termination(cnc.path(), b"x"),
        Err(TerminationError::RingFull)
    ));
}

#[test]
fn an_empty_token_is_legal() {
    // `driver_tool` calls with a null token and zero length, so this path has
    // to work rather than being an edge case.
    let cnc = Synthetic::new(deepmsg_core::version::CNC_VERSION);

    assert_eq!(
        TerminationOutcome::Committed,
        request_driver_termination(cnc.path(), b"").expect("should send")
    );
}
