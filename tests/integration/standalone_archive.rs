//! P2-S7's acceptance in Rust: an archive that **attaches**, driven end to end.
//!
//! The C suite is where the attach topology is really judged — 56 of its 276
//! archive spawns are `io.aeron.archive.Archive`, and the whole resilience binary
//! hangs without one — but a suite that needs a CMake build, a shim and the
//! reference checkout is not a criterion CI runs. This is: our driver in its own
//! aeron directory, our standalone archive attaching to it from a second process,
//! and one control connect over the IPC control channel the archive always opens.
//!
//! # What this can fail on that the archiving media driver cannot
//!
//! Everything in [`deepmsg_archive::launch::DriverHalf::Attached`], which until
//! this slice was dead code:
//!
//! * the CnC file is **another process's**, and this archive must read it and
//!   never create, publish or delete it;
//! * a command is answered by that driver on its own, so the archive's blocking
//!   client calls are the ones that work — under `Owned` they would deadlock,
//!   which is why the same code takes the asynchronous path there;
//! * the archive's error counter is **allocated**, type 101 keyed by archive id,
//!   instead of being the driver's own `ERRORS` slot;
//! * `aeron.dir.delete.on.start` is not read at all: the directory belongs to the
//!   driver, which is still running in it.
//!
//! # The archive directory is a `TempDir` and the aeron directory is not
//!
//! The aeron directory comes from [`OwnDriver`], which owns it and deletes it on
//! the way out; this test only borrows it. The archive's own directory is this
//! test's to place, because the archive is the one that makes it
//! (`Archive.java:1275-1282`, `IoUtil.ensureDirectoryExists`).

use std::time::Instant;

use deepmsg_archive::catalog::{Catalog, Recording};
use deepmsg_archive::mark::NULL_VALUE;
use deepmsg_client::client::Client;
use deepmsg_tests::archive::{self, DEADLINE, Session};
use deepmsg_tests::driver::{self, OwnDriver};
use deepmsg_tests::standalone_archive::{self, OwnStandaloneArchive};
use deepmsg_tests::temp::TempDir;

/// The connect's correlation id, which its answer echoes.
const CORRELATION_ID: i64 = 0x5ed1_0002;

/// How big the fabricated catalog's file is. A record is a few hundred bytes and
/// this holds the one the repair test writes with room to spare.
const CATALOG_CAPACITY: usize = 8 * 1024;

#[test]
fn a_standalone_archive_serves_a_connect_over_a_drivers_cnc() {
    let archive_dir = TempDir::new("standalone-archive-archive");

    let Some(driver) = OwnDriver::start("standalone-archive") else {
        driver::announce_skip();
        return;
    };

    let aeron_dir = driver.aeron_dir().to_path_buf();

    let Some(mut archive) =
        OwnStandaloneArchive::start(&aeron_dir, archive_dir.path(), &archive::PROPERTIES)
    else {
        standalone_archive::announce_skip();
        return;
    };

    archive
        .await_ready(DEADLINE)
        .expect("the standalone archive must come up and signal its mark file");

    let mut client = Client::connect(&aeron_dir).expect("connect to the driver");

    let session = Session::connect(
        &mut client,
        &aeron_dir,
        CORRELATION_ID,
        Instant::now() + DEADLINE,
    )
    .unwrap_or_else(|reason| {
        panic!(
            "{reason};\n--- the standalone archive ---\n{}",
            archive.log_tail(60)
        )
    });

    // The connect's answer names the session every later request on this
    // connection is addressed to, and a fresh archive hands out its first.
    assert!(
        session.control_session_id() >= 0,
        "the answer names control session {}",
        session.control_session_id()
    );

    let _ = archive.stop();
}

/// A recording that was being written when its archive died has **no stop
/// position** on disk, and the archive that starts over that catalog must repair
/// it before it serves anything.
///
/// The stop position is written when a recording stops
/// (`ArchiveConductor.java:1329-1363`, this build's `Catalog::recording_stopped`),
/// so a recording whose archive died first leaves a `VALID` record with a null
/// one. What it would have been is still in the segment files, and
/// `Catalog.refreshAndFixDescriptor` (`:1066-1097`) reads them — the reference
/// runs that over **every** descriptor as it loads the catalog
/// (`Catalog.java:228-231`).
///
/// This build's repair is a method on purpose — a reader like `ArchiveTool
/// describe` has no business writing to somebody's catalog because it looked at
/// it — and the archive starting up is the caller that means it. Without that
/// caller the C suite's kill-and-restart cases were handed `stopPosition = -1`
/// and refused with `replay length must be positive: replayLength=-33793,
/// length=-1, stopPosition=-1`, because "to the end" of a recording with no stop
/// position is not a length.
///
/// The recording here has **no segment files**, which is the shape this
/// criterion can fabricate without a recorder: `compute_stop_position`'s answer
/// to that is the recording's start position, so what is asserted is that the
/// null is gone and something with a meaning took its place.
#[test]
fn an_archive_repairs_a_stop_position_that_was_never_written() {
    let archive_dir = TempDir::new("standalone-archive-repair");

    let Some(driver) = OwnDriver::start("standalone-archive-repair") else {
        driver::announce_skip();
        return;
    };

    // Deliberately not zero, so that "repaired to the start position" cannot be
    // an uninitialised field agreeing with itself.
    let start_position = 64;

    let recording_id = {
        let mut catalog = Catalog::create(archive_dir.path(), CATALOG_CAPACITY, 0, 0)
            .expect("the catalog is made");

        catalog
            .add_recording(&Recording {
                recording_id: 0,
                start_timestamp: 1,
                stop_timestamp: NULL_VALUE,
                start_position,
                stop_position: NULL_VALUE,
                initial_term_id: 0,
                segment_file_length: 128 * 1024,
                term_buffer_length: 64 * 1024,
                mtu_length: 1408,
                session_id: 3,
                stream_id: 33,
                stripped_channel: "aeron:ipc".to_owned(),
                original_channel: "aeron:ipc".to_owned(),
                source_identity: "aeron:ipc".to_owned(),
            })
            .expect("the record fits")
    };

    let Some(mut archive) =
        OwnStandaloneArchive::start(driver.aeron_dir(), archive_dir.path(), &archive::PROPERTIES)
    else {
        standalone_archive::announce_skip();
        return;
    };

    archive
        .await_ready(DEADLINE)
        .expect("the standalone archive must come up over the catalog it was handed");

    let _ = archive.stop();
    drop(archive);

    // Read it back out of the file, not out of the archive: what the repair
    // wrote is what the next start will load.
    let catalog = Catalog::open(archive_dir.path(), 0).expect("the catalog opens again");
    let recording = catalog
        .recording(recording_id)
        .expect("the recording is still there");

    assert_eq!(
        start_position, recording.stop_position,
        "the null stop position is repaired before the archive serves"
    );
}
