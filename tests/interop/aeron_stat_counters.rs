//! The reference tooling reads what this driver publishes.
//!
//! `AeronStat` is the C reference's own counter viewer: it opens a CnC file,
//! lists every allocated counter and prints the driver's version and heartbeat
//! age. It is the outside oracle for the counter region, and unlike the
//! messages a client exchanges, it looks at exactly the bytes an operator will
//! look at when something is wrong.
//!
//! The driver runs **in this process** rather than as a subprocess: the tool
//! only needs the file to exist and be published, and building the file here
//! keeps the test free of a path to a binary that CI cannot build anyway.
//!
//! # What makes this a test and not a screenshot
//!
//! `AeronStat`'s output is prose with numbers in it, so the assertions are
//! structural: the version line, one line per counter id, and the labels of
//! the counters whose names are contract rather than configuration. A tool
//! that read a *different* file would fail the first of those; one that read
//! the same file with a broken decoder would fail the second and third.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};

use deepmsg_cnc::counters::CLIENT_HEARTBEAT_TYPE_ID;
use deepmsg_cnc::{CncFile, CncIdentity, CncLayout};
use deepmsg_core::clock;
use deepmsg_driver::conductor::Conductor;
use deepmsg_driver::config::DriverConfig;
use deepmsg_tests::driver;

/// The reference's counter count (`AERON_SYSTEM_COUNTER_DUMMY_LAST`).
const SYSTEM_COUNTERS: i32 = 46;

/// A directory of our own in the system temp directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("deepmsg-aeronstat-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A conductor over its own CnC file, and the directory holding it.
fn running_driver() -> (TempDir, Conductor) {
    let temp = TempDir::new();
    let cnc = CncFile::create(
        &temp.0,
        &CncLayout::default(),
        &CncIdentity {
            liveness_timeout_ns: deepmsg_cnc::CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT,
            start_timestamp_ms: clock::epoch_millis(),
            pid: i64::from(std::process::id()),
        },
    )
    .expect("create the CnC file");

    let conductor = Conductor::new(
        cnc,
        &DriverConfig {
            aeron_dir: temp.0.clone(),
            ..DriverConfig::default()
        },
    )
    .expect("the conductor takes the file over");

    (temp, conductor)
}

/// Run `AeronStat` over a directory and return its output.
fn aeron_stat(binary: &Path, dir: &Path) -> String {
    let output = Command::new(binary)
        .arg("-d")
        .arg(dir)
        .arg("-w")
        .arg("false")
        .output()
        .expect("run AeronStat");

    assert!(
        output.status.success(),
        "AeronStat failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The counter lines of `AeronStat`'s output, as `(id, label)`.
///
/// The format is `<id>: <value> - <label>` with the id right-aligned, so the
/// id is what precedes the first colon and the label is what follows the last
/// `" - "`.
fn counter_lines(text: &str) -> Vec<(i32, String)> {
    text.lines()
        .filter_map(|line| {
            let (id, rest) = line.trim_start().split_once(':')?;
            let id = id.trim().parse::<i32>().ok()?;
            let label = rest.rsplit_once(" - ")?.1.trim().to_owned();
            Some((id, label))
        })
        .collect()
}

#[test]
fn aeron_stat_lists_the_counters_this_driver_published() {
    let Some(aeron_stat_binary) = driver::locate_aeron_stat() else {
        driver::announce_tool_skip("AeronStat");
        return;
    };

    let (temp, _conductor) = running_driver();
    let output = aeron_stat(&aeron_stat_binary, &temp.0);

    assert!(
        output.contains("CnC v0.2.0"),
        "the tool has to accept the file's version before it will read it:\n{output}"
    );

    let counters = counter_lines(&output);
    for id in 0..SYSTEM_COUNTERS {
        let (_, label) = counters
            .iter()
            .find(|(found, _)| *found == id)
            .unwrap_or_else(|| panic!("no counter {id} in:\n{output}"));
        assert!(!label.is_empty(), "counter {id} has a label");
    }

    // A few labels by name, including both ends of the table, because a
    // catalogue that is off by one reads correctly at a glance.
    assert_eq!(
        Some(&"Bytes sent".to_owned()),
        label_of(&counters, 0).as_ref()
    );
    assert_eq!(
        Some(&"Client liveness timeouts".to_owned()),
        label_of(&counters, 24).as_ref()
    );
    assert_eq!(
        Some(&"Failed offers to NativeResourceAgentProxy".to_owned()),
        label_of(&counters, 45).as_ref()
    );
    assert!(
        label_of(&counters, 34)
            .expect("the Aeron software counter")
            .starts_with("Aeron software: version=1.53.2 commit=deepmsg-"),
        "{:?}",
        label_of(&counters, 34)
    );
}

#[test]
fn aeron_stat_shows_a_client_heartbeat_after_a_client_registers() {
    let Some(aeron_stat_binary) = driver::locate_aeron_stat() else {
        driver::announce_tool_skip("AeronStat");
        return;
    };

    let (temp, mut conductor) = running_driver();

    // The client exists to the driver from its first resource command, and
    // that command is what allocates the counter this test looks for
    // (`aeron_driver_conductor.c:982-1035`).
    let mut add_counter = Vec::new();
    add_counter.extend_from_slice(&7i64.to_le_bytes()); // client_id
    add_counter.extend_from_slice(&99i64.to_le_bytes()); // correlation_id
    add_counter.extend_from_slice(&100i32.to_le_bytes()); // type_id
    add_counter.extend_from_slice(&0i32.to_le_bytes()); // key_length
    add_counter.extend_from_slice(&7i32.to_le_bytes()); // label_length
    add_counter.extend_from_slice(b"a-count");

    // Written through a second mapping of the same file, which is what a
    // client does: the command ring is the one region both ends touch.
    CncFile::try_open_writable(&temp.0)
        .expect("the file is published")
        .to_driver_ring()
        .expect("a writable command ring")
        .write(0x09, &add_counter)
        .expect("the command fits");
    conductor.do_work();

    let output = aeron_stat(&aeron_stat_binary, &temp.0);
    let counters = counter_lines(&output);

    assert_eq!(
        Some(&"a-count".to_owned()),
        label_of(&counters, 47).as_ref()
    );
    assert_eq!(
        Some(&"client-heartbeat: id=7".to_owned()),
        label_of(&counters, SYSTEM_COUNTERS).as_ref(),
        "a client's heartbeat is the next id after the catalogue:\n{output}"
    );

    // And the driver's own view agrees with the tool's.
    let reader_cnc = CncFile::try_open(&temp.0).expect("the file");
    let reader = reader_cnc.counters().expect("the counter regions");
    assert!(
        reader
            .find_by_type_and_registration(CLIENT_HEARTBEAT_TYPE_ID, 7)
            .is_some(),
        "the counter is findable by type and registration too"
    );
}

fn label_of(counters: &[(i32, String)], id: i32) -> Option<String> {
    counters
        .iter()
        .find(|(found, _)| *found == id)
        .map(|(_, label)| label.clone())
}
