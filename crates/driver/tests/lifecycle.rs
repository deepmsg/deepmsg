//! The driver binary, end to end: it creates a CnC file, a client finds it,
//! and it stops when it is told to.
//!
//! This is the first test in the repository where **both** ends are deepmsg's.
//! The client half is the one P0 proved against the reference driver, so using
//! it here is not two unknowns agreeing: it is a known-good peer asking this
//! driver to answer the same questions.
//!
//! The driver runs as a subprocess rather than in-process on purpose. What is
//! under test includes the parts only a process has: the exit code, the
//! signal path, and what it does to the directory on the way out.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_client::terminate::{TerminationOutcome, request_driver_termination};
use deepmsg_cnc::layout;
use deepmsg_cnc::{CNC_FILE_NAME, CncFile};

/// How long to wait for a freshly spawned driver to publish its CnC file.
const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait for a signalled or terminated driver to exit.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// A driver subprocess in a directory of its own, killed if it outlives the
/// test.
struct Driver {
    child: Child,
    dir: PathBuf,
}

impl Driver {
    /// Start the driver with `properties`, as `-D` arguments.
    fn start(dir: &Path, properties: &[&str]) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_deepmsg-driver"))
            .arg(format!("-Ddeepmsg.dir={}", dir.display()))
            .args(properties)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("the test binary is built alongside this test");

        Self {
            child,
            dir: dir.to_owned(),
        }
    }

    /// Wait for **this** driver's CnC file and open it.
    ///
    /// Identifying it by pid is not pedantry: the tests that reuse a directory
    /// start a driver over one a stopped driver left behind, and that file is
    /// perfectly readable until the new driver deletes it. A reader that took
    /// the first readable file would be reading the previous process, which is
    /// exactly the mistake `dir.rs` exists to prevent in the other direction.
    fn await_cnc(&self) -> CncFile {
        let pid = i64::from(self.child.id());
        let deadline = Instant::now() + READY_TIMEOUT;

        loop {
            if let Ok(cnc) = CncFile::try_open(&self.dir) {
                if cnc.metadata().pid == pid {
                    return cnc;
                }
            }

            assert!(
                Instant::now() < deadline,
                "no CnC file for pid {pid} in {} within {READY_TIMEOUT:?}",
                self.dir.display()
            );
            std::thread::sleep(Duration::from_millis(16));
        }
    }

    /// Whether the process is still running.
    fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Wait for the process to exit and return its code.
    fn wait_for_exit(&mut self) -> Option<i32> {
        let deadline = Instant::now() + STOP_TIMEOUT;
        loop {
            match self.child.try_wait().expect("waiting must succeed") {
                Some(status) => return status.code(),
                None if Instant::now() >= deadline => {
                    panic!(
                        "the driver did not exit; its stderr was:\n{}",
                        self.stderr()
                    )
                }
                None => std::thread::sleep(Duration::from_millis(20)),
            }
        }
    }

    /// Everything the driver wrote to stderr so far.
    ///
    /// Only readable once the process has exited, which is why it is called
    /// from the failure paths rather than from a running driver.
    fn stderr(&mut self) -> String {
        use std::io::Read as _;

        let mut out = String::new();
        if let Some(mut stderr) = self.child.stderr.take() {
            let _ = stderr.read_to_string(&mut out);
        }
        out
    }

    /// Wait for the heartbeat to become the clean-stop value.
    fn await_stop_signal(&self, cnc: &CncFile) {
        let deadline = Instant::now() + STOP_TIMEOUT;
        while cnc.consumer_heartbeat_ms() != Some(layout::NULL_VALUE) {
            assert!(
                Instant::now() < deadline,
                "the heartbeat never became NULL_VALUE, so the driver did not close cleanly"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A directory of our own in the system temp directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};

        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("deepmsg-lifecycle-{}-{n}", std::process::id()));
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn now_ms() -> i64 {
    deepmsg_core::clock::epoch_millis()
}

#[test]
fn a_client_connects_to_a_driver_this_build_started() {
    let dir = TempDir::new();
    let driver = Driver::start(&dir.0, &[]);
    let cnc = driver.await_cnc();

    let client = Client::connect(&dir.0).expect("a client can connect");

    assert_eq!(
        1,
        client.client_id(),
        "the driver burns correlation id 0 at startup, as the reference does"
    );
    assert!(
        cnc.driver_is_active(now_ms(), 10_000),
        "and it is alive while it runs"
    );
}

#[test]
fn the_default_policy_refuses_a_termination_and_keeps_running() {
    let dir = TempDir::new();
    let mut driver = Driver::start(&dir.0, &[]);
    let cnc = driver.await_cnc();

    let outcome = request_driver_termination(&dir.0, b"let me in").expect("sending succeeds");
    assert_eq!(TerminationOutcome::Committed, outcome);

    // Waiting for evidence rather than for a fixed number of milliseconds: the
    // driver refreshes its heartbeat on every timeout tier, so a *new* value is
    // positive proof that it is still running after the denial — and if it had
    // stopped, the refresh would never come and the wait would say so. The
    // sleep this replaces proved the same thing by hoping.
    let before = cnc
        .consumer_heartbeat_ms()
        .expect("the ring trailer is readable");
    let deadline = Instant::now() + Duration::from_secs(5);
    while cnc.consumer_heartbeat_ms() == Some(before) {
        assert!(
            Instant::now() < deadline,
            "the driver stopped refreshing its heartbeat after a denied termination"
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    assert!(driver.is_running(), "the default validator is deny");
    assert!(
        cnc.driver_is_active(now_ms(), 10_000),
        "and the heartbeat is still being refreshed"
    );
}

#[test]
fn an_allowed_termination_stops_the_driver_and_removes_the_directory() {
    let dir = TempDir::new();
    let mut driver = Driver::start(
        &dir.0,
        &[
            "-Ddeepmsg.driver.termination.validator=allow",
            "-Ddeepmsg.dir.delete.on.shutdown=true",
        ],
    );
    let cnc = driver.await_cnc();

    request_driver_termination(&dir.0, b"let me in").expect("sending succeeds");

    driver.await_stop_signal(&cnc);
    assert_eq!(
        Some(0),
        driver.wait_for_exit(),
        "an accepted termination exits 0"
    );
    assert!(
        !dir.0.join(CNC_FILE_NAME).exists(),
        "and the directory goes with it when delete.on.shutdown is set"
    );
}

#[test]
fn a_signal_stops_the_driver_and_reports_the_signal() {
    let dir = TempDir::new();
    let mut driver = Driver::start(&dir.0, &["-Ddeepmsg.dir.delete.on.shutdown=true"]);
    let cnc = driver.await_cnc();

    // Sent with `kill` rather than through `Child::kill`, which is SIGKILL and
    // would prove nothing: the point is that the handler runs.
    let signal = Command::new("kill")
        .arg("-TERM")
        .arg(driver.child.id().to_string())
        .status()
        .expect("kill runs");
    assert!(signal.success());

    driver.await_stop_signal(&cnc);
    assert_eq!(
        Some(15),
        driver.wait_for_exit(),
        "a signalled driver reports the signal, not success — the reference does the same"
    );
    assert!(
        !dir.0.exists(),
        "and it still went through the shutdown path"
    );
}

#[test]
fn a_signal_during_startup_still_takes_the_clean_path() {
    // The signal handler is installed *before* the directory work, so a SIGTERM
    // that arrives while the driver is settling its directory takes the clean
    // path out instead of killing the process outright.
    //
    // That window is made deterministic rather than raced for: a `cnc.dat` with
    // no published version puts `prepare` into its liveness spin for the whole
    // driver timeout, so a signal sent half a second in is provably *inside*
    // `prepare` — long past the point where a driver that installed its handler
    // after the directory work would have died. (The first version of this test
    // signalled as soon as the process existed and was caught by CI: on a loaded
    // runner the signal can beat `exec` and the handler both, which fails for a
    // reason that has nothing to do with the order being tested.)
    let dir = TempDir::new();

    // Version zero: not a live driver, and not one this driver can read — which
    // is exactly the state its liveness question waits out.
    std::fs::create_dir_all(&dir.0).expect("the directory the driver will settle");
    std::fs::write(dir.0.join("cnc.dat"), vec![0u8; 4_096]).expect("a version-zero CnC file");

    let mut driver = Driver::start(
        &dir.0,
        &[
            // Milliseconds, and long enough to still be spinning in half a
            // second — the driver's own create window is what it is waiting for.
            "-Ddeepmsg.driver.timeout=3000",
            "-Ddeepmsg.counters.buffer.length=1m",
        ],
    );

    std::thread::sleep(Duration::from_millis(500));

    let signal = Command::new("kill")
        .arg("-TERM")
        .arg(driver.child.id().to_string())
        .status()
        .expect("kill runs");
    assert!(signal.success());

    assert_eq!(
        Some(15),
        driver.wait_for_exit(),
        "the handler ran, so the exit code is the signal — a process killed by \
         the default disposition reports no code at all"
    );
}

#[test]
fn a_second_driver_refuses_a_directory_a_live_one_holds() {
    let dir = TempDir::new();
    let mut driver = Driver::start(&dir.0, &[]);
    driver.await_cnc();

    let second = Command::new(env!("CARGO_BIN_EXE_deepmsg-driver"))
        .arg(format!("-Ddeepmsg.dir={}", dir.0.display()))
        .output()
        .expect("the binary runs");

    assert_eq!(Some(1), second.status.code());
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(
        stderr.contains("active media driver"),
        "and it says why: {stderr}"
    );
    assert!(driver.is_running(), "the first driver is untouched");
}

#[test]
fn a_driver_that_stopped_leaves_a_directory_the_next_one_takes() {
    let dir = TempDir::new();
    let mut first = Driver::start(&dir.0, &["-Ddeepmsg.driver.termination.validator=allow"]);
    let cnc = first.await_cnc();

    request_driver_termination(&dir.0, b"").expect("sending succeeds");
    first.await_stop_signal(&cnc);
    assert_eq!(Some(0), first.wait_for_exit());
    assert!(
        dir.0.join(CNC_FILE_NAME).exists(),
        "delete.on.shutdown is off, so the file is still there"
    );
    assert!(
        !cnc.driver_is_active(now_ms(), i64::MAX),
        "and it reads as a stopped driver rather than a stale one"
    );

    // The next driver finds a heartbeat of -1, which is a dead driver, and
    // reclaims the directory — no operator has to clear anything.
    let second = Driver::start(&dir.0, &[]);
    let second_cnc = second.await_cnc();

    assert!(second_cnc.driver_is_active(now_ms(), 10_000));
    drop(cnc);
    assert!(Path::new(&dir.0).join(CNC_FILE_NAME).exists());
}
