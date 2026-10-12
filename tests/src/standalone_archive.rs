//! Harness for the *standalone* archive — an archive that attaches to a media
//! driver somebody else runs.
//!
//! The third of the reference's three archive main classes, and P2-S7's. 56 of
//! the 276 archive spawns the reference's C suite makes are this one
//! (`TestStandaloneArchive.h:136`), and what it exists for there is a topology
//! the one-process archiving media driver cannot give: an archive that can die
//! without the driver dying with it (`..._resilience_test.cpp:1671-1672`).
//!
//! It is built on [`ReferenceDriver`] rather than beside it for the reason
//! [`crate::archiving_driver`] gives for its own: one place decides how a test
//! starts a process — the `-D` property list, the removal of the inherited
//! `AERON_*`, the log, the signal — and what each harness adds is the one thing
//! that differs. Here that is **two** things:
//!
//! * the aeron directory is the caller's, not this process's: the driver is
//!   already running in it, and the archive attaches to it;
//! * the log is a file of its own, because two processes in one aeron directory
//!   sharing `<aeron_dir>.driver.log` would have the second to start truncate
//!   the first one's startup lines away.
//!
//! # Ready is a version, not a length
//!
//! The same probe as [`crate::archiving_driver::OwnArchivingMediaDriver::await_ready`],
//! and the reference's own C suite uses the weaker one: it polls the length of
//! `archive-mark.dat` until it passes 8192 (`TestStandaloneArchive.h:166`), which
//! the file reaches as soon as it is created. The genuine ready byte is the
//! version field [`OwnStandaloneArchive::await_ready`] waits on.
//!
//! # Why it is not behind the `interop` feature
//!
//! Nothing here needs the reference checkout — only this workspace's own two
//! binaries, which the same `cargo test` that runs the tests using them has just
//! built.

use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::{Duration, Instant};

use deepmsg_archive::mark_file::ArchiveMarkFile;
use deepmsg_cnc::file::RETRY_INTERVAL;

use crate::archiving_driver::ReadyError;
use crate::driver::{self, DriverError, ReferenceDriver};

/// Environment variable naming our own standalone archive binary.
pub const STANDALONE_ARCHIVE_ENV: &str = "DEEPMSG_ARCHIVE";

/// Where that binary is expected to be: this workspace's own build output,
/// beside the two `driver.rs` and `archiving_driver.rs` look for.
pub const DEFAULT_OWN_STANDALONE_ARCHIVE: &str = "../target/debug/deepmsg-archive";

/// Find our standalone archive, or `None` when it is not built.
///
/// Looked for rather than assumed, exactly as the other two are: a workspace
/// that has not built it skips the tests that need it instead of failing them,
/// and `DEEPMSG_ARCHIVE` names one that has to exist.
pub fn locate_own_standalone_archive() -> Option<PathBuf> {
    driver::locate_tool(STANDALONE_ARCHIVE_ENV, DEFAULT_OWN_STANDALONE_ARCHIVE)
}

/// A running standalone archive, attached to a driver another process runs.
pub struct OwnStandaloneArchive {
    archive: ReferenceDriver,
    archive_dir: PathBuf,
}

impl OwnStandaloneArchive {
    /// Start it in `aeron_dir` — where a driver is already running — with its
    /// archive in `archive_dir`, or skip when it is not built.
    ///
    /// `aeron_dir` is an argument here and is *made* by the other two harnesses,
    /// which is the whole difference: this process attaches to a directory it
    /// does not own and must not delete. Nothing here passes
    /// `aeron.dir.delete.on.start`, and the binary would not read it if it did.
    ///
    /// `extra_properties` are additional `-D` properties. The threading mode is
    /// set here rather than left to a caller: this build's archive runs on the
    /// calling thread and refuses anything but `SHARED`, whose reference default
    /// is `DEDICATED` — so a caller who forgets it gets a refusal rather than a
    /// test.
    /// There is no `test_name` here, unlike the other two harnesses: it is what
    /// they name the temporary aeron directory with, and this one is handed a
    /// directory that already exists.
    pub fn start(aeron_dir: &Path, archive_dir: &Path, extra_properties: &[&str]) -> Option<Self> {
        let binary = locate_own_standalone_archive()?;

        let mut properties: Vec<String> = vec![
            // The directory the *driver* is in, which is the whole point: the
            // archive reads the CnC file there and never writes one.
            format!("-Daeron.dir={}", aeron_dir.display()),
            format!("-Daeron.archive.dir={}", archive_dir.display()),
            // `aeron.archive.mark.file.dir` is deliberately **not** set. The C
            // suite sets it to the aeron directory (`TestStandaloneArchive.h:104`),
            // and that shape is covered there; here the default is what is
            // wanted — empty means "beside the archive" (`Archive.java:321`), so
            // the mark file this harness then waits on is in the one directory
            // it already has in hand. Setting it would split the archive's
            // directory from the file that says the archive is up, and
            // [`OwnStandaloneArchive::await_ready`] would wait on the wrong one.
            "-Daeron.archive.threading.mode=SHARED".to_owned(),
        ];
        properties.extend(extra_properties.iter().map(|p| (*p).to_string()));

        let log_path = aeron_dir.with_extension("standalone-archive.log");
        let archive = ReferenceDriver::spawn_with_args_and_log(
            &binary,
            aeron_dir,
            &log_path,
            &properties,
            &[],
        )
        .ok()?;

        Some(Self {
            archive,
            archive_dir: archive_dir.to_path_buf(),
        })
    }

    /// The aeron directory it attached to.
    pub fn aeron_dir(&self) -> &Path {
        self.archive.aeron_dir()
    }

    /// The archive directory it owns.
    pub fn archive_dir(&self) -> &Path {
        &self.archive_dir
    }

    /// Wait for the archive to say it is serving.
    ///
    /// Unlike [`crate::archiving_driver`] there is no CnC half to wait for
    /// first: the CnC file is the *driver's* and the driver started before this
    /// process did. What is waited on is the mark file's version — see the
    /// module note.
    ///
    /// # Errors
    ///
    /// [`ReadyError::Timeout`] when the archive never signals, which carries the
    /// tail of the archive's own log because a process that died on the way up
    /// says why there.
    pub fn await_ready(&mut self, timeout: Duration) -> Result<(), ReadyError> {
        let started = Instant::now();
        let deadline = started + timeout;

        loop {
            // Read afresh each turn rather than carried: what a caller wants
            // from a timeout is the state at the end, not at some earlier point
            // in the wait.
            let last = match ArchiveMarkFile::open(&self.archive_dir) {
                Ok(mark) if mark.version().is_some() => return Ok(()),
                Ok(_) => Some("the mark file carries no version yet".to_owned()),
                Err(error) => Some(error.to_string()),
            };

            if Instant::now() >= deadline {
                return Err(ReadyError::Timeout {
                    waited: started.elapsed(),
                    last,
                    log_tail: self.log_tail(40),
                });
            }

            std::thread::sleep(RETRY_INTERVAL);
        }
    }

    /// Stop it and wait for it.
    ///
    /// SIGTERM, which is what the C suite's teardown sends and what a
    /// kill-and-restart case sends too.
    pub fn stop(&mut self) -> Result<ExitStatus, DriverError> {
        self.archive.stop()
    }

    /// The last `lines` lines the archive wrote.
    pub fn log_tail(&self, lines: usize) -> String {
        self.archive.log_tail(lines)
    }
}

/// Announce that a test could not run because our standalone archive is not
/// built.
pub fn announce_skip() {
    eprintln!(
        "SKIPPED: not verified -- no deepmsg standalone archive binary found. \
         Build it (`cargo build -p deepmsg-archive --bin deepmsg-archive`) \
         or set {STANDALONE_ARCHIVE_ENV}."
    );
}
