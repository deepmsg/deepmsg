//! **`B0′`: our archive attached to the reference's Java media driver.**
//!
//! This is the definition of the archive-vs-Java benchmark's `R-attach` arm — the
//! one arm of four that can say "the archive is the difference" — and it is the one
//! pairing this project has never run: every other criterion has either **our** driver
//! under our archive (`tests/integration/standalone_archive.rs`, over
//! `tests/src/driver.rs`) or a **Java** driver under a **Java** archive
//! (`tests/src/java_archiving_driver.rs`). The other hard gate, `B0`
//! (`deepmsg-bench-plan-vs-aeron.md`), verified the opposite direction: a Java client
//! against our driver.
//!
//! It is a **gate, not a slice**: it produces no numbers, and until it passes every
//! cell of the benchmark table is measuring something unknown.
//!
//! ```text
//! Java MediaDriver   ──>   deepmsg-archive (Attached)   ──>   reference AeronArchive client
//!    (the driver)              (the archive)                     (the gate's instrument)
//! ```
//!
//! # What is already known, and what this adds
//!
//! Half of the first face is proven elsewhere: `tests/interop/java_archive_id.rs`
//! connects **our** `Client` to a CnC file the **Java** process published. What has
//! never happened is our **archive** — the server half, with its conductor loop, its
//! blocking subscriptions on the attached path, its catalog and mark file — coming up
//! beside somebody else's driver. And the one thing the benchmark harness needs before
//! it will send a message:
//!
//! ★ `io.aeron.archive.status.RecordingPos.findCounterIdBySession(counters, sessionId,
//! archiveId)` — polled in a **loop with no timeout** by
//! `AeronUtil.awaitRecordingStart:277-288`, called from `ArchiveNode.run():97`. The
//! counter is allocated by **us** into **their** counters region, and it has to carry
//! the type id and the key layout their reader searches for. A build that got either
//! wrong records perfectly and publishes nothing, and the harness **hangs** instead of
//! failing — which is why it is judged here rather than in the arm that would hang.
//!
//! # ★ Two readers, one slot, and why the probe is spawned rather than run
//!
//! A recording position counter lives exactly as long as its recording: the archive
//! asks for it when the image arrives and gives it back when the session ends
//! (`RecordingSession.java:120`). A second reader that arrives **after** the stop reads
//! a slot that has been given back — the first cut of this test did exactly that, and
//! an assertion about a freed counter is an assertion about nothing. So the probe
//! prints `LIVE` once it has found the counter and waits; this side reads the *same
//! slot* with **our** reader; then the probe is told to carry on.
//!
//! Run with `cargo test -p deepmsg-tests --features interop`. When the reference build
//! is not there this prints `SKIPPED` and passes, like the rest of this directory.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deepmsg_archive::client::archive::{Archive, Handlers};
use deepmsg_archive::client::context::{CONTROL_CHANNEL_ENV, CONTROL_RESPONSE_CHANNEL_ENV};
use deepmsg_archive::client::{ArchiveContext, NoCredentials};
use deepmsg_archive::server::recording_pos::{find_counter_id_by_session, parse_key};
use deepmsg_client::client::Client;
use deepmsg_cnc::counters::CountersReader;
use deepmsg_core::buffer::ReadOnly;
use deepmsg_tests::driver;
use deepmsg_tests::java;
use deepmsg_tests::java_driver::{self, JavaMediaDriver};
use deepmsg_tests::standalone_archive::{self, OwnStandaloneArchive};
use deepmsg_tests::temp::TempDir;

/// What the archive is started with, so that the id in the counter's key is one a
/// client can be told about.
///
/// ★ **The reference's default is `-1`, and `-1` is `Aeron.NULL_VALUE`** — which
/// `findCounterIdBySession` treats as "do not compare the archive id at all"
/// (`RecordingPos.java:230`). A gate run at the default would therefore never exercise
/// the one field of the key whose *value* has to match, so it is set, and wide enough
/// to be 64-bit (`java_archive_id.rs` sets the same number for the same reason).
const ARCHIVE_ID: i64 = 0x0423_6483_BEEF;

/// The stream the recording is made on.
const RECORD_STREAM_ID: i32 = 1001;

/// Where the replay arrives: its own stream, and `aeron:ipc` because this gate judges
/// the control plane and the counters rather than the network — the UDP replay shapes
/// are the reference's C suite's and `S3`/`S4`'s.
const REPLAY_CHANNEL: &str = "aeron:ipc?alias=b0-prime-replay";
const REPLAY_STREAM_ID: i32 = 1002;

/// How long any one wait may take.
const DEADLINE: Duration = Duration::from_secs(30);

/// A port this run owns, for the two channels that have to be UDP with one.
///
/// The reference's archive refuses a non-UDP control channel outright
/// (`Archive.java:1227-1231`), and its client refuses anything but a concrete port for
/// a publication, so neither channel can be left to the driver.
fn port(offset: u32) -> u32 {
    8210 + (std::process::id() % 400) * 2 + offset
}

fn control_channel() -> String {
    format!("aeron:udp?endpoint=localhost:{}", port(0))
}

fn response_channel() -> String {
    format!("aeron:udp?endpoint=localhost:{}", port(1))
}

/// Compile the probe against the reference jar, once per process.
fn build_probe() -> Option<PathBuf> {
    static BUILT: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();

    BUILT
        .get_or_init(|| {
            let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let into = manifest.join("../target/reference-archive-client-probe-classes");
            let _ = std::fs::create_dir_all(&into);

            java::compile_probe(
                &manifest.join(
                    "fixtures/reference-archive-client-probe/ReferenceArchiveClientProbe.java",
                ),
                &into,
            )?;

            Some(into)
        })
        .clone()
}

/// The reference's client, running, with its lines read by a thread of its own.
///
/// A thread rather than a blocking read because there is work to do in the middle: the
/// counter has to be read by this side **while the recording is live**, and the probe
/// is what is holding the recording open.
struct Probe {
    child: Child,
    into_stdin: ChildStdin,
    lines: Arc<Mutex<Vec<String>>>,
}

impl Probe {
    fn spawn(jar: &Path, classes: &Path, driver: &JavaMediaDriver, deadline: Duration) -> Self {
        let classpath = std::env::join_paths([jar, classes]).expect("two paths join");

        let mut child = Command::new("java")
            .args(driver::AGRONA_JVM_ARGS)
            .arg("-cp")
            .arg(classpath)
            .arg("ReferenceArchiveClientProbe")
            .arg(driver.aeron_dir())
            .arg(control_channel())
            .arg(response_channel())
            .arg("aeron:ipc?alias=b0-prime-record")
            .arg(RECORD_STREAM_ID.to_string())
            .arg(REPLAY_CHANNEL)
            .arg(REPLAY_STREAM_ID.to_string())
            .arg((deadline.as_millis() as u64).to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("java runs");

        let stdout = child.stdout.take().expect("piped");
        let into_stdin = child.stdin.take().expect("piped");
        let lines = Arc::new(Mutex::new(Vec::new()));
        let into = Arc::clone(&lines);

        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                into.lock().expect("no one panics holding this").push(line);
            }
        });

        Self {
            child,
            into_stdin,
            lines,
        }
    }

    fn lines(&self) -> Vec<String> {
        self.lines
            .lock()
            .expect("no one panics holding this")
            .clone()
    }

    /// Wait for a line beginning with `want`.
    ///
    /// # Panics
    ///
    /// When the probe exits first — which is the failure a probe that cannot reach the
    /// archive produces — or when the deadline passes.
    fn await_line(&mut self, want: &str, deadline: Instant) -> String {
        loop {
            if let Some(line) = self.lines().into_iter().find(|line| line.starts_with(want)) {
                return line;
            }

            assert!(
                self.child.try_wait().expect("waitable").is_none(),
                "the probe exited before {want:?}; it printed {:?}",
                self.lines()
            );
            assert!(
                Instant::now() < deadline,
                "the probe never printed {want:?}; it printed {:?}",
                self.lines()
            );

            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Tell it to carry on.
    fn go(&mut self) {
        self.into_stdin
            .write_all(b"go\n")
            .expect("the probe reads its stdin");
        self.into_stdin.flush().expect("flushed");
    }

    /// Wait for it to end, and hand back everything it printed.
    fn finish(&mut self, deadline: Instant) -> Vec<String> {
        while self.child.try_wait().expect("waitable").is_none() {
            assert!(
                Instant::now() < deadline,
                "the probe is still running with {:?}",
                self.lines()
            );

            std::thread::sleep(Duration::from_millis(20));
        }

        self.lines()
    }
}

/// A probe that is still running is killed rather than left behind: a failed
/// assertion unwinds, and a `java` process holding a CnC mapping open would outlive
/// the test that was watching it.
impl Drop for Probe {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The one line of the probe's output beginning with `prefix`.
fn line(printed: &[String], prefix: &str) -> String {
    printed
        .iter()
        .find(|line| line.starts_with(prefix))
        .unwrap_or_else(|| panic!("the probe never printed {prefix:?}; it printed:\n{printed:?}"))
        .to_owned()
}

/// The `at`-th space-separated field of a line, as a number.
fn field(line: &str, at: usize) -> i64 {
    line.split(' ')
        .nth(at)
        .and_then(|field| field.parse().ok())
        .unwrap_or_else(|| panic!("{line:?} has no number at {at}"))
}

/// ★★ The gate: the reference's driver, our archive, the reference's client.
#[test]
fn our_archive_serves_the_references_client_on_the_references_driver() {
    let Some(classes) = build_probe() else {
        java_driver::announce_skip();
        return;
    };

    let Some(jar) = driver::locate_aeron_all() else {
        java_driver::announce_skip();
        return;
    };

    let Some(mut driver) = JavaMediaDriver::start("b0-prime", &[]) else {
        java_driver::announce_skip();
        return;
    };

    driver
        .await_ready(DEADLINE)
        .unwrap_or_else(|error| panic!("the reference driver never published its CnC: {error}"));

    // Face one, and the half that has never been run: the archive comes up **beside** a
    // driver it does not own. `OwnStandaloneArchive` is the harness
    // `standalone_archive.rs` uses against our own driver; nothing about it knows whose
    // driver this is, which is the point.
    let archive_dir = TempDir::new("b0-prime");
    let properties = [
        format!("-Daeron.archive.control.channel={}", control_channel()),
        format!("-Daeron.archive.id={ARCHIVE_ID}"),
    ];

    let Some(mut archive) = OwnStandaloneArchive::start(
        driver.aeron_dir(),
        archive_dir.path(),
        &properties.iter().map(String::as_str).collect::<Vec<_>>(),
    ) else {
        standalone_archive::announce_skip();
        return;
    };

    archive.await_ready(DEADLINE).unwrap_or_else(|error| {
        panic!(
            "our archive never signalled ready on the reference's driver: {error}\n--- driver ---\n{}",
            driver.log_tail(40)
        )
    });

    // A client of our own, pointed at the **reference's** driver: not the instrument,
    // the second reader.
    let mut client =
        Client::connect(driver.aeron_dir()).expect("our client reads the reference driver's CnC");

    let mut probe = Probe::spawn(&jar, &classes, &driver, DEADLINE * 2);
    let live = probe.await_line("LIVE", Instant::now() + DEADLINE);

    let session_id = i32::try_from(field(&live, 1)).expect("a session id is an i32");
    let found = probe.lines();

    // ── ★ two readers, one slot, while it is live ────────────────────────────────
    let reference = line(&found, "COUNTER");
    let reference_counter = i32::try_from(field(&reference, 1)).expect("a counter id is an i32");
    let reference_recording = field(&reference, 2);

    let counters = client
        .counters_reader()
        .expect("the reference driver's counters region is readable");

    // ★ **Ours, over the Java driver's region.** This is the read that makes the
    // reference's answer evidence rather than a coincidence: its reader found a slot by
    // a session id, and ours has to find the same slot and read the same recording id,
    // the same archive id and the same type out of it.
    let ours = find_counter_id_by_session(&counters, session_id, ARCHIVE_ID).unwrap_or_else(|| {
        panic!(
            "our reader does not find the counter the reference found at {reference_counter} \
             over the same region; ours sees {:?}",
            ours_view(&counters)
        )
    });

    assert_eq!(
        reference_counter, ours,
        "the reference's `findCounterIdBySession` and ours answer the same slot"
    );

    let identity = counters
        .key(ours)
        .and_then(|key| parse_key(&key))
        .expect("the slot both readers found is a recording position");

    assert_eq!(
        reference_recording, identity.recording_id,
        "and both read the same recording id out of it"
    );
    assert_eq!(
        ARCHIVE_ID, identity.archive_id,
        "★ the id in the key is the one the client was told — the field a gate run at the \
         reference's default `-1` would never have compared"
    );
    assert_eq!(
        session_id, identity.session_id,
        "and it is the publication being recorded that the counter is keyed by"
    );
    assert!(
        !identity.source_identity.is_empty(),
        "the fourth field of the key is the image's source identity, as the reference writes it"
    );

    // The recording is still running: let the probe stop it and replay it.
    probe.go();

    let printed = probe.finish(Instant::now() + DEADLINE * 4);

    // ── the archive names itself, and the client hears it ────────────────────────
    assert_eq!(
        ARCHIVE_ID,
        field(&line(&printed, "ARCHIVE_ID"), 1),
        "the id the archive was started with, over the reference's own client"
    );
    assert_eq!(
        0,
        field(&line(&printed, "LISTED_COUNT"), 1),
        "nothing was recorded when the listing ran, and it says so rather than failing"
    );
    assert!(
        field(&line(&printed, "SUBSCRIPTION"), 1) > 0,
        "the start answer carries the recording subscription's registration id"
    );

    // ── the stop, and the replay ─────────────────────────────────────────────────
    let published = field(&line(&printed, "PUBLISHED"), 2);
    let stop_position = field(&line(&printed, "STOP_POSITION"), 1);

    assert!(
        stop_position >= published,
        "the catalog's stop ({stop_position}) is at least as far as the publication got ({published})"
    );
    assert_eq!(
        field(&line(&printed, "PUBLISHED"), 1),
        field(&line(&printed, "REPLAYED"), 1),
        "every message the recording was given came back through the replay"
    );
    assert!(
        printed.iter().any(|line| line == "DONE"),
        "the probe ran to the end; it printed:\n{printed:?}"
    );

    // And the archive the reference's client has been talking to is **ours**: read back
    // through our own archive client, over the same driver.
    let observer = Archive::connect(
        &ArchiveContext::resolve(&[
            (CONTROL_CHANNEL_ENV.to_owned(), control_channel()),
            (CONTROL_RESPONSE_CHANNEL_ENV.to_owned(), response_channel()),
        ]),
        &mut client,
        &mut NoCredentials,
        Handlers::default(),
    )
    .expect("our archive client connects to our archive");

    assert_eq!(
        ARCHIVE_ID,
        observer.archive_id(),
        "and it is the same archive the reference's client was answered by"
    );

    let _ = archive.stop();
    let _ = driver.stop();
}

/// What our reader can see, for a failure that has to say whether the slot is missing
/// from its view or merely unreachable.
fn ours_view(counters: &CountersReader<'_, ReadOnly>) -> Vec<(i32, i32)> {
    (0..128)
        .filter_map(|id| counters.get(id).map(|descriptor| (id, descriptor.type_id)))
        .collect()
}
