//! The counter and error-log surfaces, each pinned to the reference's own
//! reading of them.
//!
//! Three arrangements, one per direction the contracts run in:
//!
//! 1. **Our client, their driver** — a counter added, read, written, listed by
//!    the reference's `AeronStat`, and removed. This is the arrangement every
//!    `ADD_COUNTER`/`REMOVE_COUNTER` byte this build writes is falsified in.
//! 2. **Both drivers, their `ErrorStat`** — the same unknown command recorded
//!    by each driver, read out by the reference's own error-log viewer, and
//!    the two outputs compared with the timestamps masked. Golden text, not
//!    structure: the adapter's wording is the contract.
//! 3. **Their driver, our reader and their `ErrorStat`** — an entry the
//!    reference recorded, read by this build's error-log reader, matching the
//!    reference tool's report of the same region verbatim.
//!
//! The timestamps are the one thing two runs can never share, and the format
//! puts them on the summary line (`error_stat.c:57-79`), so the comparison
//! parses that line for the observation count and takes the text whole.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_client::counter::CounterEvent;
use deepmsg_cnc::error_log::ErrorLogEntry;
use deepmsg_cnc::{CncFile, CncIdentity, CncLayout};
use deepmsg_core::clock;
use deepmsg_driver::conductor::Conductor;
use deepmsg_driver::config::DriverConfig;
use deepmsg_tests::driver::{self, READY_TIMEOUT, ReferenceDriver};

/// A command the protocol does not define, for the error-log tests. The value
/// is arbitrary — any id the switch has no case for is recorded the same way.
const UNKNOWN_COMMAND_TYPE: i32 = 0x99;

/// How long to wait for a driver's duty cycle to record what it was handed.
const RECORD_TIMEOUT: Duration = Duration::from_secs(5);

/// A directory of our own in the system temp directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(prefix: &str) -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("deepmsg-{prefix}-{}-{n}", std::process::id()));
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
fn our_driver(dir: &Path) -> Conductor {
    let cnc = CncFile::create(
        dir,
        &CncLayout::default(),
        &CncIdentity {
            liveness_timeout_ns: deepmsg_cnc::CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT,
            start_timestamp_ms: clock::epoch_millis(),
            pid: i64::from(std::process::id()),
        },
    )
    .expect("create the CnC file");

    Conductor::new(
        cnc,
        &DriverConfig {
            aeron_dir: dir.to_owned(),
            ..DriverConfig::default()
        },
    )
    .expect("the conductor takes the file over")
}

/// Write a command the protocol does not define, through a second mapping of
/// the file — the way a client writes any command.
fn send_unknown_command(dir: &Path) {
    CncFile::try_open_writable(dir)
        .expect("the file is published")
        .to_driver_ring()
        .expect("a writable command ring")
        .write(UNKNOWN_COMMAND_TYPE, &[0u8; 16])
        .expect("the command fits");
}

/// Run `ErrorStat` over a directory and return its output.
fn error_stat(binary: &Path, dir: &Path) -> String {
    let output = Command::new(binary)
        .arg("-d")
        .arg(dir)
        .output()
        .expect("run ErrorStat");

    assert!(
        output.status.success(),
        "ErrorStat failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The entries of `ErrorStat`'s output, as `(observation_count, text)`.
///
/// Each entry prints as `***`, a summary line `<n> observations from <date>
/// to <date> for:`, the recorded description, and a blank line
/// (`aeron_error_stat_on_observation`, `error_stat.c:57-79`) — the first
/// line of the description indented one space by the print, the rest
/// verbatim. The two dates are when the entry was first and last seen — the
/// one part of the output that is about the run rather than the log — so
/// they are dropped here and everything else is compared. The blank line
/// after the description ends the text, which is the description's own
/// trailing newline becoming a line boundary: the parsed text is the
/// recorded one minus that final newline, and callers comparing against the
/// file add it back.
fn entries(output: &str) -> Vec<(i32, String)> {
    let mut lines = output.lines().peekable();
    let mut parsed = Vec::new();

    while let Some(line) = lines.next() {
        if line.trim_end() != "***" {
            continue;
        }

        let Some(summary) = lines.next() else {
            break;
        };
        let Some((count, _)) = summary.split_once(" observations from ") else {
            continue;
        };
        let Ok(count) = count.trim().parse::<i32>() else {
            continue;
        };

        // The text runs to the blank line the print puts after it — which is
        // also what keeps the footer (`N distinct errors observed.`) out of
        // the last entry. The reference's recorded descriptions are composed
        // of two lines (`aeron_error.c:326-375`), and only the first carries
        // the print's one-space indent.
        let mut text = String::new();
        while lines
            .peek()
            .is_some_and(|next| !next.trim_end().is_empty() && next.trim_end() != "***")
        {
            let body = lines.next().expect("peeked");
            if text.is_empty() {
                text.push_str(body.strip_prefix(' ').unwrap_or(body));
            } else {
                text.push('\n');
                text.push_str(body);
            }
        }

        parsed.push((count, text));
    }

    parsed
}

/// Read a directory's error log with this build's own reader, waiting out the
/// driver's duty cycle until an entry mentioning `needle` is there.
///
/// Returns every entry once the wait succeeds, and an empty vector once it
/// does not: the callers assert on the contents, and an honest empty beats a
/// timeout panic that hides what *was* recorded.
fn wait_for_our_read(dir: &Path, needle: &str) -> Vec<ErrorLogEntry> {
    let deadline = Instant::now() + RECORD_TIMEOUT;

    loop {
        let mut out = Vec::new();
        if let Ok(cnc) = CncFile::try_open(dir) {
            if let Some(reader) = cnc.error_log() {
                reader.read(i64::MIN, &mut out);
            }
        }

        if out.iter().any(|entry| entry.text.contains(needle)) {
            return out;
        }

        if Instant::now() >= deadline {
            return Vec::new();
        }

        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The counter lines of `AeronStat`'s output, as `(id, value, label)`.
///
/// The format is `<id>: <value> - <label>` with the id right-aligned and the
/// value thousands-grouped with commas, so the id is what precedes the first
/// colon and the label begins after the `" - "` that follows the value — a
/// label may contain `" - "` of its own, but the first occurrence is the
/// separator, because the value never contains one.
fn counter_lines(text: &str) -> Vec<(i32, i64, String)> {
    text.lines()
        .filter_map(|line| {
            let (id, rest) = line.trim_start().split_once(':')?;
            let id = id.trim().parse::<i32>().ok()?;
            let (value, label) = rest.trim_start().split_once(" - ")?;
            let value = value.trim().replace(',', "");
            let value = value.parse::<i64>().ok()?;
            Some((id, value, label.trim().to_owned()))
        })
        .collect()
}

/// A counter, added by this build's client on the reference's own driver.
#[test]
fn our_client_adds_and_removes_a_counter_on_the_reference_driver() {
    let Some(binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    let mut reference =
        ReferenceDriver::start_with(&binary, "p13_counter", &[]).expect("start driver");
    reference
        .await_cnc(READY_TIMEOUT)
        .expect("the driver must publish a readable CnC file");

    let mut client = Client::connect(reference.aeron_dir()).expect("the client connects");
    let counter = client
        .add_counter(100, b"a key", "a counter of ours", Duration::from_secs(5))
        .expect("the reference driver allocates the counter");

    // The file the reference wrote, read back through this build's own
    // reader: the id the reply named is the slot the reference allocated,
    // under the registration the request used, with the label whole. The
    // reader borrows the client, so each phase takes its own.
    {
        let reader = client.counters_reader().expect("the counter regions");
        assert_eq!(
            Some(counter.counter_id()),
            reader.find_by_type_and_registration(100, counter.registration_id())
        );
        assert_eq!(
            "a counter of ours",
            counter.descriptor(&reader).expect("allocated").label
        );
    }

    // The announcements the reference made while the add was waiting: our
    // heartbeat — keyed by the client id, which the reference's driver
    // allocates on first sight — and the counter the add asked for. That the
    // events arrive from *their* driver is what pins this build's event
    // shape to more than its own echo.
    let events = client.counter_events();
    assert!(
        events.iter().any(|event| matches!(
            event,
            CounterEvent::Ready { correlation_id, .. } if *correlation_id == client.client_id()
        )),
        "the reference announced our heartbeat: {events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            CounterEvent::Ready { correlation_id, counter_id }
                if *correlation_id == counter.registration_id()
                    && *counter_id == counter.counter_id()
        )),
        "and our counter: {events:?}"
    );

    // The value is ours to write, into their file, through a second mapping.
    {
        let file =
            CncFile::try_open_writable(reference.aeron_dir()).expect("the file is published");
        let writable = file.counters_writable().expect("writable counter regions");
        assert!(counter.set_value(&writable, 41));
    }
    assert_eq!(
        Some(41),
        client
            .counters_reader()
            .and_then(|reader| counter.value(&reader))
    );

    // And the reference's own viewer lists it, with the value we wrote.
    if let Some(aeron_stat) = driver::locate_aeron_stat() {
        let output = Command::new(&aeron_stat)
            .arg("-d")
            .arg(reference.aeron_dir())
            .arg("-w")
            .arg("false")
            .output()
            .expect("run AeronStat");
        assert!(output.status.success(), "AeronStat failed");
        let output = String::from_utf8_lossy(&output.stdout);

        let counters = counter_lines(&output);
        assert_eq!(
            Some(&(counter.counter_id(), 41, "a counter of ours".to_owned())),
            counters
                .iter()
                .find(|(id, _, _)| *id == counter.counter_id()),
            "the reference's viewer lists our counter:\n{output}"
        );
    } else {
        driver::announce_tool_skip("AeronStat");
    }

    // Removal: acknowledged, gone from the file the reference keeps, and the
    // reference's viewer no longer lists it.
    client
        .remove_counter(&counter, Duration::from_secs(5))
        .expect("the reference driver acknowledges the removal");
    assert_eq!(
        None,
        client
            .counters_reader()
            .and_then(|reader| reader.find_by_type_and_registration(100, counter.registration_id()))
    );

    if let Some(aeron_stat) = driver::locate_aeron_stat() {
        let output = Command::new(&aeron_stat)
            .arg("-d")
            .arg(reference.aeron_dir())
            .arg("-w")
            .arg("false")
            .output()
            .expect("run AeronStat");
        assert!(output.status.success(), "AeronStat failed");
        let output = String::from_utf8_lossy(&output.stdout);

        assert!(
            !counter_lines(&output)
                .iter()
                .any(|(id, _, _)| *id == counter.counter_id()),
            "the slot is back in the reference's pool:\n{output}"
        );
    }

    reference.stop().expect("stop the driver");
}

/// The same unknown command, recorded by both drivers and reported by the
/// reference's own error-log viewer — golden text, timestamps masked.
#[test]
fn error_stat_reads_what_both_drivers_record_for_an_unknown_command() {
    let Some(error_stat_binary) = driver::locate_error_stat() else {
        driver::announce_tool_skip("ErrorStat");
        return;
    };
    let Some(binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    // Ours, in this process: the conductor owns the file, so the entry lands
    // as soon as its duty cycle runs.
    let ours = TempDir::new("p13_errstat_ours");
    let mut conductor = our_driver(&ours.0);
    send_unknown_command(&ours.0);
    for _ in 0..10 {
        conductor.do_work();
    }

    // Theirs, as a process: the entry lands on its own duty cycle, so the
    // wait is bounded rather than assumed.
    let mut reference =
        ReferenceDriver::start_with(&binary, "p13_errstat", &[]).expect("start driver");
    reference
        .await_cnc(READY_TIMEOUT)
        .expect("the driver must publish a readable CnC file");
    send_unknown_command(reference.aeron_dir());
    assert!(
        !wait_for_our_read(reference.aeron_dir(), "command=").is_empty(),
        "the reference recorded the command"
    );

    // The reference's viewer reads both files, and both drivers record the
    // same composition: the negated code with its description, then the
    // recording site and the message — the shape `AERON_SET_ERR` leaves in
    // the per-thread buffer (`aeron_error.c:326-375`), carried into the log
    // verbatim. The final newline is a line boundary in the print, so
    // `entries` strips it.
    let expected = vec![(
        1,
        format!(
            "(-6) unknown command type id\n\
             [aeron_driver_conductor_on_command, aeron_driver_conductor.c:3219] \
             command={UNKNOWN_COMMAND_TYPE} unknown"
        ),
    )];
    assert_eq!(
        expected,
        entries(&error_stat(&error_stat_binary, &ours.0)),
        "our driver's entry, in the reference's own words"
    );
    assert_eq!(
        expected,
        entries(&error_stat(&error_stat_binary, reference.aeron_dir())),
        "the reference driver's entry"
    );

    reference.stop().expect("stop the driver");
}

/// An entry the reference recorded, read by our reader and matching the
/// reference's own report of the same region.
#[test]
fn our_reader_reads_the_reference_error_log_and_matches_error_stat() {
    let Some(error_stat_binary) = driver::locate_error_stat() else {
        driver::announce_tool_skip("ErrorStat");
        return;
    };
    let Some(binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    let mut reference =
        ReferenceDriver::start_with(&binary, "p13_reader", &[]).expect("start driver");
    reference
        .await_cnc(READY_TIMEOUT)
        .expect("the driver must publish a readable CnC file");
    send_unknown_command(reference.aeron_dir());

    let recorded = wait_for_our_read(reference.aeron_dir(), "command=");
    assert_eq!(1, recorded.len(), "one command, one entry: {recorded:?}");
    let entry = &recorded[0];
    assert_eq!(1, entry.observation_count);
    assert_eq!(
        format!(
            "(-6) unknown command type id\n\
             [aeron_driver_conductor_on_command, aeron_driver_conductor.c:3219] \
             command={UNKNOWN_COMMAND_TYPE} unknown\n"
        ),
        entry.text,
        "our reader reads the reference's composition, trailer newline and all"
    );

    // The same region, through the reference's own tool: the count and the
    // text are what our reader said, word for word, and the dates — the part
    // our reader holds as numbers and the tool formats — are the only thing
    // not compared. The description's trailing newline is a line boundary in
    // the print, so it is the one byte `entries` drops.
    let recorded_text = entry
        .text
        .strip_suffix('\n')
        .unwrap_or(&entry.text)
        .to_owned();
    let reported = entries(&error_stat(&error_stat_binary, reference.aeron_dir()));
    assert_eq!(
        vec![(entry.observation_count, recorded_text)],
        reported,
        "our reader and the reference's tool agree on the region"
    );

    reference.stop().expect("stop the driver");
}
