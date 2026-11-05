//! G9: our rescued error log is the reference's rescued error log.
//!
//! The reference looks at a dead driver's CnC file and, before deleting the
//! directory, writes a `<aeron_dir>-<datestamp>-error.log` beside it
//! (`aeron-driver/src/main/c/aeron_driver.c:83-132`, called at `:205-212`).
//! Our driver does the same thing. This test makes the two read the *same*
//! CnC bytes and compares what each wrote, body and stderr line.
//!
//! Two copies of one seeded directory, so the CnC bytes are identical and the
//! difference can only be the reader. The datestamp in the filename is the
//! time of the reclaim, so it is expected to differ; the body's entry
//! timestamps come from the CnC and must not.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use deepmsg_cnc::error_log::DistinctErrorLog;
use deepmsg_cnc::{CncFile, CncIdentity, CncLayout};
use deepmsg_tests::driver;

const NOW_MS: i64 = 1_700_000_000_000;
const HEARTBEAT_MS: i64 = NOW_MS - 60_000;
const RESCUE_WAIT: Duration = Duration::from_secs(20);

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let suffix = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "deepmsg-rescue-parity-{}-{suffix}",
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

/// A CnC a dead driver left: one distinct error, a stale heartbeat, published.
fn seed(dir: &Path) {
    fs::create_dir_all(dir).expect("the aeron directory");
    let mut cnc = CncFile::create(
        dir,
        &CncLayout::default(),
        &CncIdentity {
            liveness_timeout_ns: 10_000_000_000,
            start_timestamp_ms: NOW_MS,
            pid: 4242,
        },
    )
    .expect("create the CnC file");

    {
        let mut log = DistinctErrorLog::new();
        let region = cnc
            .error_log_writable()
            .expect("the layout has an error log");
        log.record(&region, NOW_MS, 7, "the error the reference will save")
            .expect("record the error");
    }

    cnc.write_consumer_heartbeat(HEARTBEAT_MS)
        .expect("write the heartbeat");
    cnc.publish().expect("publish");
}

/// The `<dir>-<datestamp>-error.log` the driver wrote beside `dir`.
fn rescue_file(dir: &Path) -> Option<PathBuf> {
    let parent = dir.parent()?;
    let prefix = format!("{}-", dir.file_name()?.to_string_lossy());
    fs::read_dir(parent).ok()?.find_map(|entry| {
        let entry = entry.ok()?;
        let name = entry.file_name().to_string_lossy().into_owned();
        (name.starts_with(&prefix) && name.ends_with("-error.log")).then(|| entry.path())
    })
}

/// Start a driver and wait until it has written the rescue file; it is killed
/// before the test proceeds (the file is the whole subject).
fn start_and_wait(binary: &Path, dir: &Path, stderr: &Path) -> Child {
    let stderr_file = fs::File::create(stderr).expect("the stderr file");
    let mut child = Command::new(binary)
        .arg(format!("-Daeron.dir={}", dir.display()))
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .spawn()
        .expect("spawn the driver");

    let deadline = Instant::now() + RESCUE_WAIT;
    while Instant::now() < deadline {
        if rescue_file(dir).is_some() {
            return child;
        }
        if let Some(status) = child.try_wait().expect("wait on the driver") {
            panic!(
                "the driver exited ({status}) before rescuing {}",
                dir.display()
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    let _ = child.kill();
    let _ = child.wait();
    let text = fs::read_to_string(stderr).unwrap_or_default();
    panic!(
        "no rescue file appeared for {}; driver stderr:\n{text}",
        dir.display()
    );
}

fn warning_line(stderr: &Path) -> String {
    let text = fs::read_to_string(stderr).expect("the driver's stderr");
    text.lines()
        .find(|line| line.contains("Existing errors saved to:"))
        .unwrap_or_else(|| panic!("no rescue warning in:\n{text}"))
        .to_owned()
}

#[test]
fn our_rescue_is_the_references_rescue() {
    let Some(reference) = driver::locate_verified() else {
        driver::announce_tool_skip("aeronmd");
        return;
    };
    let Some(own) = driver::locate_own() else {
        driver::announce_own_skip();
        return;
    };

    let temp = TempDir::new();
    let source = temp.0.join("source");
    seed(&source);

    let for_reference = temp.0.join("reference");
    let for_ours = temp.0.join("ours");
    for destination in [&for_reference, &for_ours] {
        fs::create_dir_all(destination).expect("a copy directory");
        fs::copy(
            source.join(deepmsg_cnc::CNC_FILE_NAME),
            destination.join(deepmsg_cnc::CNC_FILE_NAME),
        )
        .expect("copy the CnC file");
    }

    let reference_stderr = temp.0.join("reference.stderr");
    let ours_stderr = temp.0.join("ours.stderr");
    let mut reference_child = start_and_wait(&reference, &for_reference, &reference_stderr);
    let mut ours_child = start_and_wait(&own, &for_ours, &ours_stderr);

    let reference_file = rescue_file(&for_reference).expect("the reference's rescue file");
    let ours_file = rescue_file(&for_ours).expect("our rescue file");

    let reference_body = fs::read_to_string(&reference_file).expect("the reference's body");
    let ours_body = fs::read_to_string(&ours_file).expect("our body");
    assert_eq!(
        reference_body, ours_body,
        "the rescued bodies differ:\nreference: {reference_body}\nours: {ours_body}"
    );
    assert!(
        ours_body.contains("1 distinct errors observed."),
        "{ours_body}"
    );

    let reference_warning = warning_line(&reference_stderr);
    let ours_warning = warning_line(&ours_stderr);
    let expected_prefix = "WARNING: Existing errors saved to: ";
    assert!(
        reference_warning.starts_with(expected_prefix),
        "the reference's line: {reference_warning}"
    );
    assert!(
        ours_warning.starts_with(expected_prefix),
        "our line: {ours_warning}"
    );

    let _ = reference_child.kill();
    let _ = reference_child.wait();
    let _ = ours_child.kill();
    let _ = ours_child.wait();
}
