//! What a driver decides about a directory an *incompatible* CnC file is in.
//!
//! The directory discipline is one question — is somebody else living here? —
//! and this is the part of it that cannot be answered by looking at a heartbeat.
//! A file whose CnC version this build may not open might belong to a live
//! driver of another version, and the two honest answers are "refuse" and
//! "it is not ours to judge". Deleting it is not one of them.
//!
//! The files here are synthetic (`deepmsg_tests::synthetic`), because a real
//! unreadable-version file is not something a driver of this build can write.

use deepmsg_core::version::{CNC_VERSION, semantic_version_compose};
use deepmsg_driver::config::DriverConfig;
use deepmsg_driver::dir::{self, DirError};
use deepmsg_tests::synthetic::SyntheticCnc;

const NOW_MS: i64 = 1_700_000_000_000;

fn config(dir: &std::path::Path) -> DriverConfig {
    DriverConfig {
        aeron_dir: dir.to_owned(),
        driver_timeout_ms: 50,
        ..DriverConfig::default()
    }
}

#[test]
fn a_same_major_older_minor_file_is_left_alone() {
    // 0.1.0 where this build is 0.2.0: the version rule refuses to *read* it,
    // and refusing to read is not evidence that nobody is home. The reference
    // would refuse this directory too — it judges liveness by major alone, so
    // an older live driver is simply busy.
    let synthetic = SyntheticCnc::new(semantic_version_compose(0, 1, 0));
    let config = config(synthetic.path());

    let error = dir::prepare(&config, NOW_MS).expect_err("a driver of another minor may be alive");

    assert!(
        matches!(error, DirError::BusyIncompatible { .. }),
        "the answer is EBUSY, not a deletion: {error}"
    );
    assert!(
        synthetic.cnc_path().is_file(),
        "and the file it could not read is still there"
    );
}

#[test]
fn a_different_major_file_is_a_dead_directory() {
    // A major version this build cannot even describe is the one case the
    // reference does treat as not-a-driver: it reads the version, finds the
    // major different, and takes the directory (`aeron_driver_context.c:1613-1624`
    // returns "not active", and the caller deletes).
    let synthetic = SyntheticCnc::new(semantic_version_compose(2, 0, 0));
    let config = config(synthetic.path());

    dir::prepare(&config, NOW_MS).expect("prepare");

    assert!(
        !synthetic.cnc_path().exists(),
        "the unreadable file is gone"
    );
    assert!(
        synthetic.path().join("publications").is_dir(),
        "and the directory is a fresh one"
    );
}

#[test]
fn this_builds_own_version_is_still_a_busy_or_a_dead_directory() {
    // The control: a file this build *can* read is judged by its heartbeat.
    // The synthetic file has none, so it is a dead driver — which is what makes
    // the two cases above mean something.
    let synthetic = SyntheticCnc::new(CNC_VERSION);
    let config = config(synthetic.path());

    dir::prepare(&config, NOW_MS).expect("a file with no heartbeat is a dead driver");

    assert!(synthetic.path().join("publications").is_dir());
}
