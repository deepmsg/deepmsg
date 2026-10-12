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

use deepmsg_client::client::Client;
use deepmsg_tests::archive::{self, DEADLINE, Session};
use deepmsg_tests::driver::{self, OwnDriver};
use deepmsg_tests::standalone_archive::{self, OwnStandaloneArchive};
use deepmsg_tests::temp::TempDir;

/// The connect's correlation id, which its answer echoes.
const CORRELATION_ID: i64 = 0x5ed1_0002;

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
