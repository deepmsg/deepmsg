//! Harness for the **reference's** archiving media driver — the Java one, out of
//! the reference checkout.
//!
//! This is the driver a system test runs against (the C tree ships a client and
//! an archive *client*, not an archive server), and it is what makes the one case
//! that needs a real archive answerable: an archive id that is not the default
//! can only come from an archive that was started with one.
//!
//! # The command line, and why it is not [`ReferenceDriver::start_with`]
//!
//! `TestArchive.h:52-113` starts it as
//! `java <jvm flags> -D… -cp <aeron-all> io.aeron.archive.ArchivingMediaDriver` —
//! the class path and the main class go **after** the properties, so a list of
//! `-D`s cannot spell it. [`ReferenceDriver::spawn_with_args`] is the door for
//! that, and everything around the command line — the aeron directory, the log,
//! the removal of the inherited `AERON_*`, the signal that stops it — is that
//! type's, unchanged.
//!
//! # Ready is a version, not a length
//!
//! The same claim [`crate::archiving_driver`] makes about our own archive, and
//! for the same reason: the reference's C suite polls `archive-mark.dat` until it
//! passes 8192 (`TestArchive.h:141-153`), which is a **length**, and the file
//! reaches that as soon as it exists. The ready byte is the mark file's
//! **version** field, which the archive writes when it is serving.
//!
//! # Where it writes
//!
//! The aeron directory is this harness's, and the archive directory is the
//! caller's — one place to look for the mark file and the catalog.
//! `aeron.archive.mark.file.dir` is left **unset** so that the mark file lands in
//! the archive directory: the reference's `TestArchive` points it at the aeron
//! directory instead, and both work, but the default is what a caller setting
//! only `aeron.archive.dir` would get.

use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::{Duration, Instant};

use deepmsg_archive::mark_file::ArchiveMarkFile;
use deepmsg_cnc::file::RETRY_INTERVAL;

use crate::archiving_driver::ReadyError;
use crate::driver::{self, AGRONA_JVM_ARGS, DriverError, ReferenceDriver};

/// A running reference archiving media driver.
pub struct JavaArchivingMediaDriver {
    driver: ReferenceDriver,
    archive_dir: PathBuf,
}

impl JavaArchivingMediaDriver {
    /// Start one, or `None` when the reference build is not there to start.
    ///
    /// `archive_id` is `-Daeron.archive.id`; `None` leaves the reference's own
    /// default, which is `-1` — the same value a client that never asked about an
    /// archive reports, which is why a test that is *about* the id has to set it.
    ///
    /// `extra_properties` are further `-D` properties, for a test that needs the
    /// archive configured in some other way.
    #[must_use]
    pub fn start(
        test_name: &str,
        archive_dir: &Path,
        archive_id: Option<i64>,
        extra_properties: &[&str],
    ) -> Option<Self> {
        let jar = driver::locate_aeron_all()?;

        let aeron_dir = driver::temp_aeron_dir(test_name);
        let _ = std::fs::remove_dir_all(&aeron_dir);

        let mut args: Vec<String> = AGRONA_JVM_ARGS
            .iter()
            .map(|arg| (*arg).to_string())
            .collect();

        args.push("-Daeron.dir.delete.on.start=true".to_string());
        args.push("-Daeron.dir.delete.on.shutdown=true".to_string());
        args.push("-Daeron.archive.dir.delete.on.start=true".to_string());
        args.push("-Daeron.threading.mode=SHARED".to_string());
        args.push("-Daeron.archive.threading.mode=SHARED".to_string());
        args.push("-Daeron.perform.storage.checks=false".to_string());
        args.push("-Daeron.print.configuration=false".to_string());
        // **Required, not optional**: the reference's `Archive$Context.conclude`
        // refuses to start without it (`Archive.java:1237`), which is why
        // `TestArchive` always passes one. A replication channel nobody uses is
        // still a channel it insists on having.
        args.push("-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0".to_string());
        args.push(format!("-Daeron.dir={}", aeron_dir.display()));
        args.push(format!("-Daeron.archive.dir={}", archive_dir.display()));

        if let Some(archive_id) = archive_id {
            args.push(format!("-Daeron.archive.id={archive_id}"));
        }

        args.extend(extra_properties.iter().map(|p| (*p).to_string()));

        args.push("-cp".to_string());
        args.push(jar.display().to_string());
        args.push("io.aeron.archive.ArchivingMediaDriver".to_string());

        let driver =
            ReferenceDriver::spawn_with_args(Path::new("java"), &aeron_dir, &args, &[]).ok()?;

        Some(Self {
            driver,
            archive_dir: archive_dir.to_path_buf(),
        })
    }

    /// The aeron directory this process owns.
    #[must_use]
    pub fn aeron_dir(&self) -> &Path {
        self.driver.aeron_dir()
    }

    /// The archive directory it owns — the caller's, where the mark file and the
    /// catalog are.
    #[must_use]
    pub fn archive_dir(&self) -> &Path {
        &self.archive_dir
    }

    /// Wait for the CnC file to be readable **and** for the archive to say it is
    /// serving.
    ///
    /// # Errors
    ///
    /// As [`crate::archiving_driver::OwnArchivingMediaDriver::await_ready`].
    pub fn await_ready(&mut self, timeout: Duration) -> Result<(), ReadyError> {
        self.driver.await_cnc(timeout).map_err(ReadyError::Driver)?;

        let started = Instant::now();
        let deadline = started + timeout;

        loop {
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
    /// # Errors
    ///
    /// As [`ReferenceDriver::stop`].
    pub fn stop(&mut self) -> Result<ExitStatus, DriverError> {
        self.driver.stop()
    }

    /// The last `lines` lines of its output.
    #[must_use]
    pub fn log_tail(&self, lines: usize) -> String {
        self.driver.log_tail(lines)
    }
}

/// Announce that a test could not run because the reference build is not there.
pub fn announce_skip() {
    eprintln!(
        "SKIPPED: not verified -- no reference aeron-all jar found, so the Java \
         archive cannot be started. Check out the reference (docs/reference.md) \
         or set {}.",
        driver::AERON_ALL_ENV
    );
}
