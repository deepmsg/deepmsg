//! Harness for tests that need the reference C media driver.
//!
//! This module is deliberately **not** behind the `interop` feature. The
//! feature only gates the tests that call it, so that `cargo clippy
//! --all-targets` and `cargo test --no-run` compile and lint this code on
//! every CI run — neither needs the reference checkout, and leaving a few
//! hundred lines with no lint coverage is how they rot.
//!
//! The driver is a subprocess, never an in-process link: the point is to
//! exercise the real byte contract, and the reference's own C++ harness does
//! the same (`aeron-test-support/src/main/c/TestMediaDriver.h`).

use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use deepmsg_cnc::{CncFile, CncOpenError};

/// Environment variable naming the reference `aeronmd` to run.
///
/// Follows the `DEEPMSG_*` prefix ADR-0005 sets for this project's own
/// configuration.
pub const AERONMD_ENV: &str = "DEEPMSG_REF_AERONMD";

/// Where the driver is expected to be when the variable is unset: the sibling
/// checkout, relative to this crate's manifest directory.
pub const DEFAULT_AERONMD: &str = "../../aeron/cppbuild/Release/binaries/aeronmd";

/// The Aeron version the byte contracts are written against.
pub const EXPECTED_VERSION: &str = "1.53.2";

/// The commit that version was read from.
pub const EXPECTED_COMMIT: &str = "664f58e705";

/// How long to wait for a freshly spawned driver to publish its CnC metadata.
pub const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// How long to wait for a signalled driver to exit.
pub const STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// Why the harness could not do its job.
#[derive(Debug)]
pub enum DriverError {
    /// No reference driver binary was found.
    NotFound {
        /// Everywhere it looked, in order, including the override if set.
        searched: Vec<PathBuf>,
    },
    /// A binary was found but is not the version the contracts target.
    WrongVersion {
        /// What it reported, for the message.
        reported: String,
    },
    /// The process could not be spawned.
    Spawn(io::Error),
    /// The driver exited before its CnC became readable — the case a plain
    /// timeout would report as "not ready" after a 30-second wait.
    ExitedEarly {
        /// Its exit status.
        status: String,
        /// The tail of its output, which is where the reason is.
        log_tail: String,
    },
    /// The driver stayed alive but never published a usable CnC file.
    Timeout {
        /// How long we waited.
        waited: Duration,
        /// The last thing the reader objected to.
        last: Box<CncOpenError>,
        /// The tail of its output.
        log_tail: String,
    },
    /// Signalling or reaping the child failed.
    Signal(io::Error),
}

impl std::fmt::Display for DriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound { searched } => {
                write!(f, "no reference driver found; looked in {searched:?}")
            }
            Self::WrongVersion { reported } => write!(
                f,
                "reference driver is not {EXPECTED_VERSION} ({EXPECTED_COMMIT}): {reported}"
            ),
            Self::Spawn(error) => write!(f, "could not start the reference driver: {error}"),
            Self::ExitedEarly { status, log_tail } => write!(
                f,
                "reference driver exited before publishing its CnC file ({status}); output:\n{log_tail}"
            ),
            Self::Timeout {
                waited,
                last,
                log_tail,
            } => write!(
                f,
                "reference driver did not publish a usable CnC file within {waited:?}: {last}; output:\n{log_tail}"
            ),
            Self::Signal(error) => write!(f, "signalling the reference driver failed: {error}"),
        }
    }
}

impl std::error::Error for DriverError {}

/// Print the skip notice and return `None` if no driver can be found.
///
/// The `interop` feature is opt-in, so a missing driver means "this machine
/// cannot run these tests", not "these tests failed". The notice is loud and
/// names the fix, so a green run that proved nothing is never mistaken for a
/// green run that proved something — see `docs/reference.md`.
pub fn locate() -> Option<PathBuf> {
    if let Some(overridden) = std::env::var_os(AERONMD_ENV) {
        let path = PathBuf::from(overridden);
        if path.is_file() {
            return Some(path);
        }
    }

    let candidate = Path::new(env!("CARGO_MANIFEST_DIR")).join(DEFAULT_AERONMD);
    if candidate.is_file() {
        return Some(candidate);
    }

    search_path().filter(|path| path.is_file())
}

/// Announce that a test could not run, and why.
///
/// Loud on purpose. The `interop` feature is opt-in, so a missing driver is a
/// legitimate skip — but a silent one would make a run that verified nothing
/// look exactly like a run that verified everything.
pub fn announce_skip() {
    eprintln!(
        "SKIPPED: interop not verified -- no reference driver found. \
         Set {AERONMD_ENV} to an Aeron {EXPECTED_VERSION} `aeronmd` binary, \
         or check out the reference next to this repo (see docs/reference.md)."
    );
}

fn search_path() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join("aeronmd"))
        .next()
        .filter(|candidate| candidate.is_file())
}

/// Environment variable naming the reference `AeronStat` to run.
pub const AERONSTAT_ENV: &str = "DEEPMSG_REF_AERONSTAT";

/// Where `AeronStat` is expected to be when the variable is unset.
///
/// Beside the `aeronmd` this file already looks for: both come out of the same
/// reference build (`cppbuild/Release/binaries/`).
pub const DEFAULT_AERONSTAT: &str = "../../aeron/cppbuild/Release/binaries/AeronStat";

/// Find a reference *tool* — `AeronStat` today — in the same places the driver
/// is looked for.
///
/// A tool is not version-checked the way the driver is: it prints what it
/// finds, and what it prints is what the test compares, so a mismatched tool
/// fails loudly on its own. Returns `None` when the reference checkout is
/// absent, which is a skip rather than a failure for the same reason
/// [`locate`] is.
pub fn locate_tool(env: &str, default: &str) -> Option<PathBuf> {
    if let Some(overridden) = std::env::var_os(env) {
        let path = PathBuf::from(overridden);
        if path.is_file() {
            return Some(path);
        }
    }

    let candidate = Path::new(env!("CARGO_MANIFEST_DIR")).join(default);
    if candidate.is_file() {
        return Some(candidate);
    }

    let name = Path::new(default).file_name()?;
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

/// Find the reference `AeronStat`, or `None` when there is no reference build.
pub fn locate_aeron_stat() -> Option<PathBuf> {
    locate_tool(AERONSTAT_ENV, DEFAULT_AERONSTAT)
}

/// Announce that a tool-based test could not run, and why.
pub fn announce_tool_skip(tool: &str) {
    eprintln!(
        "SKIPPED: {tool} not verified -- no reference {tool} found. \
         Set the tool's DEEPMSG_REF_* variable, or check out the reference next \
         to this repo (see docs/reference.md)."
    );
}

/// Find the driver and prove it is the version the contracts were read from.
///
/// Running `-v` first turns "the wrong driver is on PATH" from a mysterious
/// decode failure deep in a test into a clear message here. The reference
/// prints `<version=…> major … minor … patch … git …`
/// (`aeron-driver/src/main/c/aeronmd.c:84-95`).
pub fn locate_verified() -> Option<PathBuf> {
    let path = locate()?;

    let output = Command::new(&path).arg("-v").output().ok()?;
    let reported = String::from_utf8_lossy(&output.stdout).trim().to_string();

    if !reported.contains(EXPECTED_VERSION) || !reported.contains(EXPECTED_COMMIT) {
        // Not a skip: something *was* found and it is the wrong thing, which
        // is worth failing over rather than quietly passing.
        panic!("{}", DriverError::WrongVersion { reported });
    }

    Some(path)
}

/// Environment variable naming **our own** driver binary.
pub const OWN_DRIVER_ENV: &str = "DEEPMSG_DRIVER";

/// Where our own driver is expected to be: this workspace's own build output.
///
/// One level up from this crate's directory, which is where the reference's
/// `../../aeron/...` defaults land two levels up — the reference is a *sibling*
/// of this repo, and our binary is inside it.
pub const DEFAULT_OWN_DRIVER: &str = "../target/debug/deepmsg-driver";

/// Find our own media driver.
///
/// Unlike the reference it is not version-checked — it is built from this
/// checkout, so "the wrong driver" is not a state a test can be in — but it is
/// still looked for rather than assumed, so a workspace that has not built it
/// skips the tests that need it instead of failing them.
pub fn locate_own() -> Option<PathBuf> {
    locate_tool(OWN_DRIVER_ENV, DEFAULT_OWN_DRIVER)
}

/// A running *deepmsg* media driver, in its own aeron directory.
///
/// The same shape as [`ReferenceDriver`] — a driver in a directory of its own,
/// configured by `-D` properties, stopped by a signal — and built on the same
/// code, so the two cannot drift apart in how a test starts a driver. What it
/// is for is the other direction of this suite: every other interop test runs
/// *our* client against the *reference* driver, and the ones that use this run
/// the reference client against ours. That is the only arrangement in which a
/// bug in our driver's own byte contracts can be falsified rather than
/// restated.
pub struct OwnDriver(ReferenceDriver);

impl OwnDriver {
    /// Start our driver for `test_name`, or skip when it is not built.
    pub fn start(test_name: &str) -> Option<Self> {
        Self::start_with(test_name, &[])
    }

    /// Start our driver with additional `-D` properties.
    ///
    /// `-Dname=value` is this driver's own configuration syntax
    /// (`crates/driver/src/config.rs`), which it shares with `aeronmd` on
    /// purpose: a property list written for one is a property list for the
    /// other, so a test can configure both drivers the same way.
    pub fn start_with(test_name: &str, extra_properties: &[&str]) -> Option<Self> {
        let binary = locate_own()?;

        ReferenceDriver::start_with(&binary, test_name, extra_properties)
            .ok()
            .map(Self)
    }

    /// The aeron directory this driver owns.
    pub fn aeron_dir(&self) -> &Path {
        self.0.aeron_dir()
    }

    /// Wait for the CnC file to be published and readable.
    pub fn await_cnc(&mut self, timeout: Duration) -> Result<CncFile, DriverError> {
        self.0.await_cnc(timeout)
    }

    /// Stop the driver and wait for it.
    pub fn stop(&mut self) -> Result<ExitStatus, DriverError> {
        self.0.stop()
    }

    /// The last lines the driver wrote to its log.
    pub fn log_tail(&self, lines: usize) -> String {
        self.0.log_tail(lines)
    }
}

/// Announce that a test could not run because our own driver is not built.
pub fn announce_own_skip() {
    eprintln!(
        "SKIPPED: not verified -- no deepmsg driver binary found. \
         Build it (`cargo build -p deepmsg-driver`) or set {OWN_DRIVER_ENV}."
    );
}

/// A running reference driver in its own aeron directory.
pub struct ReferenceDriver {
    child: Child,
    aeron_dir: PathBuf,
    log_path: PathBuf,
    stopped: bool,
}

impl ReferenceDriver {
    /// Spawn a driver with its own aeron directory.
    ///
    /// Everything is passed as `-Dname=value` rather than through the
    /// environment: `aeronmd` turns those into environment variables itself
    /// (`aeron-driver/src/main/c/aeronmd.c:72-81`), so each test's
    /// configuration is self-contained instead of depending on the ambient
    /// environment — which is also why inherited `AERON_*` are removed.
    pub fn start(binary: &Path, test_name: &str) -> Result<Self, DriverError> {
        Self::start_with(binary, test_name, &[])
    }

    /// Spawn a driver with additional `-D` properties.
    ///
    /// `aeronmd` turns each `-Dname=value` into an environment variable with
    /// the name uppercased and its dots replaced by underscores
    /// (`aeron-driver/src/main/c/aeronmd.c:72-81`), so this is how a test
    /// changes driver behaviour without touching the ambient environment —
    /// which it could not usefully do anyway, since inherited `AERON_*` are
    /// removed below.
    pub fn start_with(
        binary: &Path,
        test_name: &str,
        extra_properties: &[&str],
    ) -> Result<Self, DriverError> {
        let aeron_dir = temp_aeron_dir(test_name);
        let _ = std::fs::remove_dir_all(&aeron_dir);

        let log_path = aeron_dir.with_extension("driver.log");
        let log = std::fs::File::create(&log_path).map_err(DriverError::Spawn)?;
        let log_err = log.try_clone().map_err(DriverError::Spawn)?;

        // `delete.on.shutdown` is not merely tidiness: the driver removes its
        // own directory on a clean signal, which is what keeps a day of test
        // runs from filling /dev/shm.
        let mut properties: Vec<String> = vec![
            format!("-Daeron.dir={}", aeron_dir.display()),
            "-Daeron.dir.delete.on.start=true".to_string(),
            "-Daeron.dir.delete.on.shutdown=true".to_string(),
            "-Daeron.print.configuration=false".to_string(),
        ];
        properties.extend(extra_properties.iter().map(|p| (*p).to_string()));

        let mut command = Command::new(binary);
        command
            .args(&properties)
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err));

        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("AERON_") {
                command.env_remove(key);
            }
        }

        let child = command.spawn().map_err(DriverError::Spawn)?;

        Ok(Self {
            child,
            aeron_dir,
            log_path,
            stopped: false,
        })
    }

    /// The aeron directory this driver owns.
    pub fn aeron_dir(&self) -> &Path {
        &self.aeron_dir
    }

    /// The driver's process id.
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// The last `lines` lines of the driver's output.
    pub fn log_tail(&self, lines: usize) -> String {
        let text = std::fs::read_to_string(&self.log_path).unwrap_or_default();
        let all: Vec<&str> = text.lines().collect();
        all[all.len().saturating_sub(lines)..].join("\n")
    }

    /// Wait for the CnC file to become readable, watching the child as we go.
    ///
    /// Polling the child matters: if the driver dies at startup — a stale
    /// directory, a missing library — a bare CnC timeout reports "not ready"
    /// after the full wait and hides the reason, which is sitting in its
    /// output.
    ///
    /// Readiness is a **non-zero `cnc_version`**, not the file's existence.
    /// The driver maps the whole 48 MB file before publishing the version, so
    /// waiting on the file is waiting on the weaker signal.
    pub fn await_cnc(&mut self, timeout: Duration) -> Result<CncFile, DriverError> {
        let deadline = Instant::now() + timeout;
        let started = Instant::now();

        loop {
            if let Some(status) = self.child.try_wait().map_err(DriverError::Signal)? {
                return Err(DriverError::ExitedEarly {
                    status: status.to_string(),
                    log_tail: self.log_tail(40),
                });
            }

            match CncFile::try_open(&self.aeron_dir) {
                Ok(cnc) => return Ok(cnc),
                Err(last) => {
                    if Instant::now() >= deadline {
                        return Err(DriverError::Timeout {
                            waited: started.elapsed(),
                            last: Box::new(last),
                            log_tail: self.log_tail(40),
                        });
                    }
                }
            }

            std::thread::sleep(deepmsg_cnc::file::RETRY_INTERVAL);
        }
    }

    /// Ask the driver to stop and wait for it.
    ///
    /// Returns the exit status rather than asserting on it: a clean SIGTERM
    /// makes `aeronmd` exit with the *signal number*, not zero
    /// (`aeron-driver/src/main/c/aeronmd.c:41,112-113,185`), so callers that
    /// care should assert on it explicitly and everyone else should ignore it.
    pub fn stop(&mut self) -> Result<ExitStatus, DriverError> {
        if self.stopped {
            return self.child.wait().map_err(DriverError::Signal);
        }

        // `Child::kill` sends SIGKILL, and this crate is `#![forbid(unsafe_code)]`
        // so there is no `libc::kill`. `kill(1)` is the only libc-free way to
        // send SIGTERM, and it exists on every Linux this suite targets.
        //
        // Deliberately not `pgrep -xn aeronmd` as the reference harness does
        // (aeronmd_signal_test.cpp:58): that would signal whatever driver the
        // developer happens to be running.
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status();

        let deadline = Instant::now() + STOP_TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().map_err(DriverError::Signal)? {
                self.stopped = true;
                return Ok(status);
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let status = self.child.wait().map_err(DriverError::Signal)?;
                self.stopped = true;
                return Ok(status);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for ReferenceDriver {
    /// Never leave a process or a directory behind, however the test ended.
    fn drop(&mut self) {
        if !self.stopped {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.aeron_dir);
        let _ = std::fs::remove_file(&self.log_path);
    }
}

/// A per-test aeron directory.
///
/// `/dev/shm` is where the driver would put it by default and is what the
/// tests should exercise, but it is not guaranteed to exist — fall back rather
/// than fail. The name carries the test name and the process id so two
/// concurrent `cargo test` runs cannot collide.
fn temp_aeron_dir(test_name: &str) -> PathBuf {
    let base = if Path::new("/dev/shm").is_dir() {
        PathBuf::from("/dev/shm")
    } else {
        std::env::temp_dir()
    };

    base.join(format!(
        "deepmsg-interop-{test_name}-{}",
        std::process::id()
    ))
}
