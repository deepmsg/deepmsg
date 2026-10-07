//! Harness for the *archiving* media driver — a media driver and an archive in
//! one process, built from this checkout.
//!
//! A separate door from [`crate::driver`] for the same reason [`crate::driver`]
//! gives for its own two: the thing being started is a different binary with a
//! different readiness contract, and a test that spells out how to start one
//! will get it wrong in the same way every time. It is built on
//! [`ReferenceDriver`] rather than beside it, so the two cannot drift apart in
//! how a test starts a driver — the aeron directory, the `-D` property list,
//! the signal, the teardown are all that type's, and what this adds is the one
//! thing that differs: what "ready" means here.
//!
//! # Ready is a version, not a length
//!
//! The reference's own C suite waits for the archive by polling the length of
//! `archive-mark.dat` until it passes 8192 (`TestArchive.h:141-153`,
//! `TestProcessUtils.h:40`). That is a **file-length** probe, and the file
//! reaches 8192 as soon as it is created — its own comment calls it "an
//! indicator that Archive process is running", which is not the same claim as
//! "the archive is serving". The genuine ready byte is the mark file's
//! **version** field, which `signalReady` writes.
//!
//! [`OwnArchivingMediaDriver::await_ready`] waits on that version, and on the
//! CnC file underneath it. The binary this starts creates its mark file last,
//! after both control subscriptions are in place
//! (`crates/archive/src/bin/deepmsg-archiving-media-driver.rs`), so this is the
//! stricter of the two probes rather than a hazard of its own.
//!
//! # Why it is not behind the `interop` feature
//!
//! The same reason [`crate::driver`] is not: nothing here needs the reference
//! checkout. This binary is built from this workspace by the same `cargo test`
//! that runs the tests using it, and leaving it out of the default build would
//! leave it unlinted.

use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::{Duration, Instant};

use deepmsg_archive::mark_file::ArchiveMarkFile;
use deepmsg_cnc::file::RETRY_INTERVAL;

use crate::driver::{self, DriverError, ReferenceDriver};

/// Environment variable naming our own archiving media driver binary.
pub const ARCHIVING_MEDIA_DRIVER_ENV: &str = "DEEPMSG_ARCHIVING_MEDIA_DRIVER";

/// Where that binary is expected to be: this workspace's own build output,
/// beside the plain driver `driver.rs` looks for.
pub const DEFAULT_OWN_ARCHIVING_MEDIA_DRIVER: &str =
    "../target/debug/deepmsg-archiving-media-driver";

/// Find our archiving media driver, or `None` when it is not built.
///
/// Looked for rather than assumed, exactly as [`driver::locate_own`] is: a
/// workspace that has not built it skips the tests that need it instead of
/// failing them, and `DEEPMSG_ARCHIVING_MEDIA_DRIVER` names one that has to
/// exist.
pub fn locate_own_archiving_media_driver() -> Option<PathBuf> {
    driver::locate_tool(
        ARCHIVING_MEDIA_DRIVER_ENV,
        DEFAULT_OWN_ARCHIVING_MEDIA_DRIVER,
    )
}

/// Why an archiving media driver was not ready to be talked to.
#[derive(Debug)]
pub enum ReadyError {
    /// The driver half never came up, which [`DriverError`] says why.
    Driver(DriverError),
    /// The driver half came up and the archive half never signalled the mark
    /// file's version — the case a length probe calls ready.
    Timeout {
        /// How long we waited.
        waited: Duration,
        /// The last thing the reader objected to, if it got that far.
        last: Option<String>,
        /// The tail of its output, which is where the reason is.
        log_tail: String,
    },
}

impl std::fmt::Display for ReadyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Driver(error) => write!(f, "{error}"),
            Self::Timeout {
                waited,
                last,
                log_tail,
            } => write!(
                f,
                "the archive did not signal its mark file within {waited:?}{}; output:\n{log_tail}",
                match last {
                    Some(problem) => format!(": {problem}"),
                    None => String::new(),
                }
            ),
        }
    }
}

impl std::error::Error for ReadyError {}

/// A running archiving media driver, in its own aeron directory and with an
/// archive directory of its own.
pub struct OwnArchivingMediaDriver {
    driver: ReferenceDriver,
    archive_dir: PathBuf,
}

impl OwnArchivingMediaDriver {
    /// Start it for `test_name` with its archive in `archive_dir`, or skip when
    /// it is not built.
    ///
    /// The archive directory is an argument rather than one more entry in
    /// `extra_properties` because this harness has to read the readiness byte
    /// back **out of it**, and this build's default for it is the relative
    /// `aeron-archive` — a name that resolves against whatever working
    /// directory the test binary happens to have. Letting the two be set
    /// separately is letting them disagree; [`Self::await_ready`] would then
    /// wait on a mark file the process was never told to write.
    ///
    /// `extra_properties` are additional `-D` properties. They are one list for
    /// two configurations: the binary hands the same argv to its driver half
    /// and its archive half, and each reads the names it knows.
    pub fn start(test_name: &str, archive_dir: &Path, extra_properties: &[&str]) -> Option<Self> {
        let binary = locate_own_archiving_media_driver()?;

        let archive_dir_property = format!("-Daeron.archive.dir={}", archive_dir.display());
        let mut properties = vec![archive_dir_property.as_str()];
        properties.extend_from_slice(extra_properties);

        let driver = ReferenceDriver::start_with(&binary, test_name, &properties).ok()?;

        Some(Self {
            driver,
            archive_dir: archive_dir.to_path_buf(),
        })
    }

    /// The aeron directory this process owns — where its CnC file lives.
    pub fn aeron_dir(&self) -> &Path {
        self.driver.aeron_dir()
    }

    /// The archive directory it owns — where its mark file, and later its
    /// catalog and segments, live.
    pub fn archive_dir(&self) -> &Path {
        &self.archive_dir
    }

    /// Wait for the CnC file to be readable **and** for the archive to say it
    /// is serving.
    ///
    /// See the module note for why the second half is the mark file's version
    /// field and not its length.
    ///
    /// # Errors
    ///
    /// [`ReadyError::Driver`] when the driver half never publishes its CnC
    /// file, which carries that half's own log; [`ReadyError::Timeout`] when
    /// the archive never signals, which carries the same log because a process
    /// that died writing its mark file says why there.
    pub fn await_ready(&mut self, timeout: Duration) -> Result<(), ReadyError> {
        self.driver.await_cnc(timeout).map_err(ReadyError::Driver)?;

        let started = Instant::now();
        let deadline = started + timeout;

        loop {
            // Read afresh each turn rather than carried: what a caller wants
            // from a timeout is the state at the end, not at some earlier
            // point in the wait.
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
    pub fn stop(&mut self) -> Result<ExitStatus, DriverError> {
        self.driver.stop()
    }

    /// The last `lines` lines this process wrote.
    ///
    /// Both halves write here, so it is one place to look whichever of them
    /// refused.
    pub fn log_tail(&self, lines: usize) -> String {
        self.driver.log_tail(lines)
    }
}

/// Announce that a test could not run because our archiving media driver is not
/// built.
pub fn announce_skip() {
    eprintln!(
        "SKIPPED: not verified -- no deepmsg archiving media driver binary found. \
         Build it (`cargo build -p deepmsg-archive --bin deepmsg-archiving-media-driver`) \
         or set {ARCHIVING_MEDIA_DRIVER_ENV}."
    );
}
