//! Harness for the **reference's** media driver — the Java one, with **no archive**.
//!
//! [`crate::java_archiving_driver`] beside this starts
//! `io.aeron.archive.ArchivingMediaDriver`, which is a driver *and* an archive in one
//! process. This starts `io.aeron.driver.MediaDriver` and nothing else, which is the
//! half a **mixed gate** needs: the driver is the reference's and the archive is ours.
//! That pairing is what the archive-vs-Java benchmark plan calls **`B0′`**, and it is
//! the definition of its `R-attach` arm — the one arm of four that can say "the
//! archive is the difference".
//!
//! # Why the two harnesses are separate types rather than one with a flag
//!
//! They differ in the main class, in their required properties, and in what they wait
//! for. This one has **no** `aeron.archive.*` property at all — an archive that is a
//! different implementation is not configured by this process — and its readiness is
//! the CnC file alone, where the other also waits for a mark file this process knows
//! nothing about. A flag would make both of the shapes read as the exception.
//!
//! # The command line, and why it is not [`ReferenceDriver::start_with`]
//!
//! The class path and the main class go **after** the properties, so a list of `-D`s
//! cannot spell it: [`ReferenceDriver::spawn_with_args`] is the door for that, exactly
//! as it is for the sibling. Everything around the command line — the aeron directory,
//! the log, the removal of the inherited `AERON_*` environment, the signal that stops
//! it — is that type's, unchanged.
//!
//! # The properties, and where they come from
//!
//! The three below are the ones the reference's own benchmark harness launches its
//! embedded driver with (`ArchivingMediaDriver.launchArchiveWithEmbeddedDriver`,
//! `benchmarks-aeron/src/main/java/io/aeron/benchmarks/aeron/ArchivingMediaDriver.java:52-55`),
//! minus nothing: a gate that ran the driver differently from the way the benchmark
//! will would be measuring a different driver.
//!
//! ★ **The threading mode is deliberately not set.** The reference's default is
//! `DEDICATED`, which is a driver that advances its own agents on their own threads —
//! the only thing that can be true of a driver in another process. The sibling sets
//! `SHARED` because its archive runs in the same process and drives it; here there is
//! nobody to be shared with.

use std::path::Path;
use std::process::ExitStatus;

use crate::driver::{self, AGRONA_JVM_ARGS, DriverError, ReferenceDriver};

/// A running reference media driver, in its own aeron directory.
pub struct JavaMediaDriver {
    driver: ReferenceDriver,
}

impl JavaMediaDriver {
    /// Start one, or `None` when the reference build is not there to start.
    ///
    /// `extra_properties` are further `-D` properties, for a test that needs the
    /// driver configured in some other way.
    #[must_use]
    pub fn start(test_name: &str, extra_properties: &[&str]) -> Option<Self> {
        let jar = driver::locate_aeron_all()?;

        let aeron_dir = driver::temp_aeron_dir(test_name);
        let _ = std::fs::remove_dir_all(&aeron_dir);

        let mut args: Vec<String> = AGRONA_JVM_ARGS
            .iter()
            .map(|arg| (*arg).to_string())
            .collect();

        args.push("-Daeron.dir.delete.on.start=true".to_string());
        args.push("-Daeron.dir.delete.on.shutdown=true".to_string());
        // The reader of a **spy** subscription is what this makes count as a
        // subscriber, and it is what the benchmark harness sets on the driver it
        // launches (`ArchivingMediaDriver.java:55`). The reference's C archive harness
        // sets it for the same reason (`TestArchive.h:53`).
        args.push("-Daeron.spies.simulate.connection=true".to_string());
        args.push("-Daeron.print.configuration=false".to_string());
        args.push(format!("-Daeron.dir={}", aeron_dir.display()));

        args.extend(extra_properties.iter().map(|p| (*p).to_string()));

        args.push("-cp".to_string());
        args.push(jar.display().to_string());
        args.push("io.aeron.driver.MediaDriver".to_string());

        let driver =
            ReferenceDriver::spawn_with_args(Path::new("java"), &aeron_dir, &args, &[]).ok()?;

        Some(Self { driver })
    }

    /// The aeron directory this process owns — where its CnC file lives.
    #[must_use]
    pub fn aeron_dir(&self) -> &Path {
        self.driver.aeron_dir()
    }

    /// Wait for the CnC file to be readable.
    ///
    /// **The CnC file alone**, unlike the sibling: there is no mark file here to wait
    /// on, and the driver is up when its CnC is published (`CncFile::publish` writes
    /// the version *after* the first heartbeat, `crates/cnc/src/file.rs:186-199`).
    /// It is also the first of the gate's three faces: this file was written by
    /// another process, in another language.
    ///
    /// # Errors
    ///
    /// [`DriverError`] when it never appears.
    pub fn await_ready(&mut self, timeout: std::time::Duration) -> Result<(), DriverError> {
        self.driver.await_cnc(timeout).map(|_| ())
    }

    /// Stop it and wait for it.
    ///
    /// SIGTERM, which is what `ShutdownSignalBarrier` in `MediaDriver.main`
    /// (`:193-198`) is waiting for.
    ///
    /// # Errors
    ///
    /// [`DriverError`] as [`ReferenceDriver::stop`].
    pub fn stop(&mut self) -> Result<ExitStatus, DriverError> {
        self.driver.stop()
    }

    /// The last `lines` lines the driver wrote.
    #[must_use]
    pub fn log_tail(&self, lines: usize) -> String {
        self.driver.log_tail(lines)
    }
}

/// Announce that a test could not run because the reference build is not there.
pub fn announce_skip() {
    driver::announce_tool_skip("the reference build");
}
