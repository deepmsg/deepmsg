//! The driver's lifecycle lines, end to end.
//!
//! This is the binary-level judgement the log facade cannot make on its own:
//! the global logger is installed by the process, not by the library, so a
//! test that only calls `open` proves nothing about `deepmsg-driver`. The
//! driver is started as a subprocess with `DEEPMSG_LOG_DIR` set, and the file
//! it leaves behind is read after the process has stopped.
//!
//! The test skips (like the rest of the suite) when the driver binary is not
//! built; `announce_own_skip` says so and the run is still green.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use deepmsg_tests::driver::{OwnDriver, announce_own_skip};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let suffix = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "deepmsg-log-lifecycle-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("temp directory");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn wait_for(path: &Path, needle: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(text) = fs::read_to_string(path) {
            if text.contains(needle) {
                return text;
            }
        }
        assert!(
            Instant::now() < deadline,
            "{needle} never appeared in {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn the_driver_writes_its_start_and_stop() {
    let directory = TempDir::new();
    let log_dir = directory.0.join("log");
    let log_dir_text = log_dir.to_str().expect("a UTF-8 temporary path");

    let Some(mut driver) = OwnDriver::start_with_env(
        "logging-lifecycle",
        &[],
        &[
            ("DEEPMSG_LOG_DIR", log_dir_text),
            ("DEEPMSG_NODE_ID", "logging-test"),
        ],
    ) else {
        announce_own_skip();
        return;
    };

    driver
        .await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC");
    let path = log_dir.join("driver-logging-test.json");
    let started = wait_for(&path, "\"event\":\"driver.start\"");
    assert!(started.contains("\"component\":\"driver\""));
    assert!(started.contains("\"node_id\":\"logging-test\""));
    assert!(started.contains("\"version\":"));

    driver.stop().expect("the driver stops on SIGTERM");
    let text = fs::read_to_string(&path).expect("the log file after the stop");
    assert!(text.contains("\"event\":\"driver.stop\""));
    assert!(text.contains("\"reason\":\"signal\""));
}
