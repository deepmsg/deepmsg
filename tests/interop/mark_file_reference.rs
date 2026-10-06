//! The mark file, both ways round, against the reference's own `ArchiveTool`.
//!
//! A mark file exists for a reader that is **not** the process that wrote it —
//! that is the whole of what it is for — so the only test that means anything
//! is one where the reader is somebody else's code. `ArchiveTool pid` and
//! `ArchiveTool errors` are that reader: the reference's own, run against a
//! file this build wrote.
//!
//! The other direction is already covered in CI, by
//! `crates/archive/tests/mark_file.rs` reading a committed file the reference
//! wrote. What is added here is the same file read by **both**, which is what
//! makes the two readings comparable: if this build and the reference disagree
//! about a golden, one of them is wrong and it is worth knowing which.
//!
//! Runs against the reference's jar rather than a driver, so it needs no
//! `aeronmd` — only the checkout P2-0a already needs for its goldens.

use std::path::{Path, PathBuf};
use std::process::Command;

use deepmsg_archive::mark::NULL_VALUE;
use deepmsg_archive::mark_file::{
    ArchiveMarkFile, ERROR_BUFFER_LENGTH_DEFAULT, FILENAME, HEADER_LENGTH, Header,
};
use deepmsg_cnc::create::{COUNTERS_VALUES_BUFFER_LENGTH_MIN, CncLayout};
use deepmsg_cnc::error_log::{DistinctErrorLog, ErrorLogRegion};
use deepmsg_cnc::{CncFile, CncIdentity};
use deepmsg_tests::driver::{self, AGRONA_JVM_ARGS};
use deepmsg_tests::temp::TempDir;

/// An epoch clock's reading, as the liveness rule is arithmetic on one.
const NOW: i64 = 1_700_000_000_000;
const PAGE_SIZE: usize = 4096;

/// The archive's own directory, with a **real CnC file** beside it.
///
/// `ArchiveTool errors` prints two logs, not one: the archive's, out of the
/// mark file's error buffer, and then the **driver's**, out of the CnC file in
/// the directory the mark header names. So a mark file that names a directory
/// with no `cnc.dat` in it makes the tool fail after printing half of what was
/// asked for — which is the reference being right, and worth building for
/// rather than working around.
fn header_for(aeron_directory: &str) -> Header<'_> {
    Header {
        start_timestamp: NOW,
        control_channel: Some("aeron:udp?endpoint=localhost:9010"),
        local_control_channel: "aeron:ipc",
        events_channel: None,
        aeron_directory,
        control_stream_id: 101,
        local_control_stream_id: 102,
        events_stream_id: 103,
        archive_id: 0x0102_0304_0506_0708,
    }
}

/// A CnC file this build wrote, in its own directory.
fn cnc_in(directory: &Path) -> PathBuf {
    let aeron_dir = directory.join("aeron");
    std::fs::create_dir_all(&aeron_dir).expect("the aeron directory");

    let layout = CncLayout {
        counters_values_length: COUNTERS_VALUES_BUFFER_LENGTH_MIN,
        ..CncLayout::default()
    };
    let identity = CncIdentity {
        liveness_timeout_ns: 10_000_000_000,
        start_timestamp_ms: NOW,
        pid: i64::from(std::process::id()),
    };

    let mut cnc = CncFile::create(&aeron_dir, &layout, &identity).expect("a CnC file");

    // The order a driver publishes in: its first heartbeat, then the version
    // (`aeron-driver/src/main/c/aeron_driver.c:971-972`). This file is standing
    // in for a driver's, so it says the same two things in the same order —
    // which is what `publish` refuses to do without.
    cnc.write_consumer_heartbeat(NOW).expect("the heartbeat");
    cnc.publish().expect("published");

    aeron_dir
}

/// Run `ArchiveTool <dir> <command>` from the reference's own jar.
fn archive_tool(jar: &Path, directory: &Path, command: &str) -> String {
    let output = Command::new("java")
        .args(AGRONA_JVM_ARGS)
        .arg("-cp")
        .arg(jar)
        .arg("io.aeron.archive.ArchiveTool")
        .arg(directory)
        .arg(command)
        .output()
        .expect("java runs");

    assert!(
        output.status.success(),
        "ArchiveTool {command} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/mark-file")
}

/// The reference's own reader on a file this build wrote: the pid it reports is
/// the one this build stamped, and the errors it prints are the ones this build
/// wrote into the error buffer.
///
/// That pair is the acceptance the plan asks for — `ArchiveTool pid` and
/// `ArchiveTool errors` reading what we write — and the second half is the one
/// with weight: the pid is a number in a header, while the errors are behind
/// the header at an offset **this build computed** and in a format this build
/// wrote with its own `DistinctErrorLog`.
#[test]
fn the_reference_reads_the_mark_file_this_build_writes() {
    let Some(jar) = driver::locate_aeron_all() else {
        driver::announce_tool_skip("ArchiveTool");
        return;
    };

    let dir = TempDir::new("mark-file-interop");
    let pid = i64::from(std::process::id());
    let aeron_dir = cnc_in(dir.path());
    let header = header_for(aeron_dir.to_str().expect("a path"));

    let mark = ArchiveMarkFile::create(
        dir.path(),
        &header,
        ERROR_BUFFER_LENGTH_DEFAULT,
        PAGE_SIZE,
        pid,
    )
    .expect("create");

    {
        let region = ErrorLogRegion::new(mark.error_buffer().expect("the error buffer"));
        let mut log = DistinctErrorLog::new();

        log.record(&region, NOW, 1, "the first error a mark file wrote")
            .expect("recorded");
        log.record(&region, NOW + 1, 2, "the second error a mark file wrote")
            .expect("recorded");
    }

    mark.signal_ready(NOW).expect("signalled");

    // The pid, as the reference's own tool reads it.
    let said = archive_tool(&jar, dir.path(), "pid");
    assert_eq!(
        pid.to_string(),
        said.trim(),
        "the reference reports the pid this build stamped"
    );

    // And the errors, which are at an offset this build computed and in a
    // format it wrote itself.
    let errors = archive_tool(&jar, dir.path(), "errors");
    assert!(
        errors.contains("the first error a mark file wrote"),
        "the reference's ErrorStat-equivalent did not find the first error:\n{errors}"
    );
    assert!(
        errors.contains("the second error a mark file wrote"),
        "nor the second:\n{errors}"
    );
}

/// Both readers, one file: the golden the reference wrote, read by this build
/// and by the reference's own tool, whose answers have to be the same.
///
/// The golden's own test (in CI) checks this build against the reference's
/// *recorded* reading. This checks the two against each other **live**, which
/// catches the one thing a recording cannot: a generator whose reading was
/// taken from the wrong field in the first place.
#[test]
fn both_readers_agree_about_the_golden() {
    let Some(jar) = driver::locate_aeron_all() else {
        driver::announce_tool_skip("ArchiveTool");
        return;
    };

    let golden = fixtures();
    let mark = ArchiveMarkFile::open(&golden).expect("the golden opens");

    let said = archive_tool(&jar, &golden, "pid");
    assert_eq!(
        mark.pid().expect("a pid").to_string(),
        said.trim(),
        "the two readings of the same file's pid"
    );

    // And the file's length, which the reference does not print but its mapping
    // has to have: a reader that mapped a different length would be reading a
    // different file's bytes from the same path.
    assert_eq!(
        std::fs::metadata(golden.join(FILENAME))
            .expect("the golden")
            .len(),
        u64::try_from(mark.length()).expect("positive"),
        "and the same file"
    );

    assert_eq!(
        mark.error_buffer_length(),
        ERROR_BUFFER_LENGTH_DEFAULT,
        "the golden's error buffer, which is where the errors the reference wrote are"
    );
    assert_eq!(
        Some(HEADER_LENGTH as i32),
        mark.header_length(),
        "and the header ends where the buffer begins"
    );

    // The golden was signalled by the generator with a fixed timestamp, and
    // neither reader is asked about liveness here — this is about the two
    // agreeing on the bytes.
    assert_ne!(Some(NULL_VALUE), mark.activity_timestamp());
}
