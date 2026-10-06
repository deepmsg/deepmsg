//! A real archive, a real client, one connect — over IPC, against each driver.
//!
//! P2-0c measured this failing in two independent suites and both languages: a
//! Java client with the archive in-process and a C client with the archive in a
//! process of its own, both on `aeron:ipc?control-mode=response`, both timing
//! out where every UDP variant of the same case passes.
//!
//! The driver was then measured on its own and is not where it is: seven shapes
//! of response channel — including the three-way pairing chain and the
//! parameters the archive puts on its response publication — come out identical
//! under this driver and the reference (`ipc_response_channel_probe.rs`). What
//! those shapes leave out is the archive, and this is the archive, minus
//! everything that is not the connect.
//!
//! What it asserts is only that **both runs produced readings**. The reading is
//! the difference between them, which is why the output is printed.

use std::path::{Path, PathBuf};
use std::process::Command;

use deepmsg_tests::driver::{self, AGRONA_JVM_ARGS, OwnDriver, READY_TIMEOUT, ReferenceDriver};
use deepmsg_tests::java;
use deepmsg_tests::temp::TempDir;

/// Compile the probe once per process, for the reason the other probes record:
/// two `javac` runs writing one class while a test thread loads it.
fn build_probe() -> Option<PathBuf> {
    static BUILT: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();

    BUILT
        .get_or_init(|| {
            let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let source = manifest.join("fixtures/archive-connect-probe/ArchiveConnectProbe.java");
            let into = manifest.join("../target/archive-connect-probe");

            std::fs::create_dir_all(&into).ok()?;
            java::compile_probe(&source, &into)?;

            Some(into)
        })
        .clone()
}

/// Run the probe and hand back everything it printed.
///
/// Not `java::run_probe`, which asserts a zero exit: a probe whose whole job is
/// to report whether a connect comes back must be allowed to come back with a
/// stack trace instead, and that trace is a reading.
fn readings(jar: &Path, classes: &Path, aeron_dir: &Path, archive_dir: &Path) -> String {
    let classpath = std::env::join_paths([jar, classes]).expect("two paths join");

    let output = Command::new("java")
        .args(AGRONA_JVM_ARGS)
        .arg("-cp")
        .arg(&classpath)
        .arg("ArchiveConnectProbe")
        .arg(aeron_dir)
        .arg(archive_dir)
        .output()
        .expect("java runs");

    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn the_connect_comes_back_on_both_drivers() {
    let Some(jar) = driver::locate_aeron_all() else {
        driver::announce_tool_skip("the reference's aeron-all jar");
        return;
    };

    let Some(classes) = build_probe() else {
        driver::announce_tool_skip("javac");
        return;
    };

    let mut reference = {
        let Some(binary) = driver::locate_verified() else {
            driver::announce_skip();
            return;
        };

        let mut driver = ReferenceDriver::start_with(&binary, "archive-connect-reference", &[])
            .expect("start the reference driver");
        let _ = driver
            .await_cnc(READY_TIMEOUT)
            .expect("the driver must publish a readable CnC file");

        driver
    };

    let Some(mut own) = OwnDriver::start("archive-connect-own") else {
        driver::announce_own_skip();
        return;
    };
    let _ = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let theirs_dir = TempDir::new("archive-connect-reference-data");
    let ours_dir = TempDir::new("archive-connect-own-data");

    let theirs = readings(&jar, &classes, reference.aeron_dir(), theirs_dir.path());
    let ours = readings(&jar, &classes, own.aeron_dir(), ours_dir.path());

    println!("--- reference driver ---\n{theirs}");
    println!("--- this driver ---\n{ours}");

    // The driver's own account of the run. A publication that never links has a
    // reason, and if this driver has one it is here rather than in the client's
    // timeout message.
    println!("--- this driver's log ---\n{}", own.log_tail(60));

    assert!(
        theirs.contains("CONNECT ") && ours.contains("CONNECT "),
        "both runs must report a connect attempt:\nreference:\n{theirs}\nthis driver:\n{ours}"
    );

    let _ = own.stop();
    let _ = reference.stop();
}
