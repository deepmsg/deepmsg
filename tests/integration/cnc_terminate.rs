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

use deepmsg_client::terminate::{TerminationError, TerminationOutcome, request_driver_termination};
use deepmsg_cnc::layout;
use deepmsg_tests::synthetic::{self, COMMAND_CAPACITY, SyntheticCnc};
use std::io::Write;

/// Where the to-driver ring's trailer starts in the file.
fn ring_trailer() -> usize {
    synthetic::offsets().to_driver + COMMAND_CAPACITY
}

#[test]
fn the_baseline_file_shape_is_what_the_tests_assume() {
    // If the shared builder's sizes drift, every case below starts testing
    // something else. This says so once instead of in a dozen failures.
    assert_eq!(
        synthetic::FILE_LENGTH,
        layout::align_up(synthetic::SUM, 4096),
        "the regions must round up to the file length the builder writes"
    );
    assert!(synthetic::COMMAND_CAPACITY.is_power_of_two());
    assert_eq!(
        128,
        synthetic::COMMAND_CAPACITY / 8,
        "max_message_length for this ring"
    );
}

#[test]
fn commits_a_command_into_a_well_formed_file() {
    let cnc = SyntheticCnc::new(deepmsg_core::version::CNC_VERSION);

    let outcome = request_driver_termination(cnc.path(), b"please stop").expect("should send");

    assert_eq!(TerminationOutcome::Committed, outcome);
}

#[test]
fn refuses_a_token_longer_than_the_reference_allows() {
    let cnc = SyntheticCnc::new(deepmsg_core::version::CNC_VERSION);
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
    //
    // Hand-built rather than via `SyntheticCnc`, because the whole point is a
    // file the builder would never write.
    let dir = std::env::temp_dir().join(format!(
        "deepmsg-terminate-short-{}.dir",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create");

    let mut file = std::fs::File::create(dir.join(deepmsg_cnc::CNC_FILE_NAME)).expect("create");
    Write::write_all(&mut file, &[0u8; 64]).expect("write");
    file.sync_all().expect("sync");

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
    let cnc = SyntheticCnc::new(deepmsg_core::version::semantic_version_compose(1, 0, 0));

    assert!(matches!(
        request_driver_termination(cnc.path(), b"x"),
        Err(TerminationError::Incompatible(
            deepmsg_core::version::CncVersionCompatibility::MajorMismatch
        ))
    ));
}

#[test]
fn refuses_a_file_whose_minor_version_is_older() {
    let cnc = SyntheticCnc::new(deepmsg_core::version::semantic_version_compose(0, 1, 0));

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
    let cnc = SyntheticCnc::new(0);

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
    let cnc = SyntheticCnc::new(-1);

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
    let cnc = SyntheticCnc::new(deepmsg_core::version::CNC_VERSION)
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
    let cnc = SyntheticCnc::new(deepmsg_core::version::CNC_VERSION)
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
    let cnc = SyntheticCnc::new(deepmsg_core::version::CNC_VERSION);
    let token = vec![0u8; 129];

    assert!(matches!(
        request_driver_termination(cnc.path(), &token),
        Err(TerminationError::TokenTooLong { limit: 128, .. })
    ));
}

#[test]
fn reports_a_full_ring_rather_than_pretending_to_have_sent_it() {
    let cnc = SyntheticCnc::new(deepmsg_core::version::CNC_VERSION);
    // The producer position one byte below the capacity, with nothing consumed,
    // is the shape the reference pins at `aeron_c_terminate_test.cpp:217-244`.
    let tail = ring_trailer() + layout::MPSC_TAIL_POSITION_OFFSET;
    let cnc = cnc.patch_i64(tail, (synthetic::COMMAND_CAPACITY - 1) as i64);

    assert!(matches!(
        request_driver_termination(cnc.path(), b"x"),
        Err(TerminationError::RingFull)
    ));
}

#[test]
fn an_empty_token_is_legal() {
    // `driver_tool` calls with a null token and zero length, so this path has
    // to work rather than being an edge case.
    let cnc = SyntheticCnc::new(deepmsg_core::version::CNC_VERSION);

    assert_eq!(
        TerminationOutcome::Committed,
        request_driver_termination(cnc.path(), b"").expect("should send")
    );
}
