//! The loss report file, end to end: a stream loses data, and the file the
//! driver keeps about that is there and says so.
//!
//! The reference creates `loss-report.dat` in the aeron directory whether or
//! not anything is ever lost (`aeron-driver/src/main/c/aeron_driver.c:324-345`)
//! and appends one record per stream that lost something
//! (`aeron_publication_image.c:123-155`). Its own system tests read it back —
//! `SystemTests.verifyLossOccurredForStream` asserts the file exists and that a
//! record points at the stream — and so do two readers that share no code with
//! this build: the C client's `aeron_cnc_loss_reporter_read` and Java's
//! `LossReportReader`. The format is in `crates/cnc/src/loss_report.rs`.
//!
//! The loss here is made by **this driver's own** seam
//! (`deepmsg.debug.send.data.loss.drop.every`, the same one
//! `tests/integration/unreliable_stream.rs` drives), so the test needs no
//! reference checkout and runs in CI. `tests/interop/loss_report.rs` is the
//! half that does: the reference's own `LossStat` reading what this driver
//! wrote.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_cnc::loss_report::{LOSS_REPORT_FILE_NAME, LossReportFile};
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream the loss happens on.
const STREAM_ID: i32 = 1004;

/// One datagram in four is withheld from every endpoint this driver sends
/// through, so the image on the other side sees real holes.
const DROP_EVERY: u64 = 4;

/// How long the exchange may take before the test is a failure.
const DEADLINE: Duration = Duration::from_secs(30);

#[test]
fn a_stream_that_lost_data_is_in_the_loss_report() {
    let Some(mut own) = OwnDriver::start_with(
        "loss-report",
        &[&format!(
            "-Ddeepmsg.debug.send.data.loss.drop.every={DROP_EVERY}"
        )],
    ) else {
        driver::announce_own_skip();
        return;
    };

    let cnc = own
        .await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    // The file is the driver's, created at startup — before anything is lost
    // and before a client has asked for anything.
    let directory = own.aeron_dir().to_owned();
    let report = LossReportFile::open_readonly(&directory).expect("the driver created it");
    assert_eq!(
        1024 * 1024,
        report.length(),
        "a megabyte, which is the default and already a multiple of the page size"
    );
    assert!(entries(&report).is_empty(), "and nothing has been lost yet");

    let mut client = Client::connect(&directory).expect("connect to our driver");
    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{}|term-length=64k",
        free_port()
    );

    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication on the channel");
    let subscription = client
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription on it");

    // Publish until the report has a record for the stream.
    //
    // One loop and not two, which is the difference between a test and a coin
    // toss: the sender drops every fourth datagram, and a hole is only *seen*
    // once a frame arrives behind it — so a client that stopped offering at
    // the first message that got through would stop **one frame short of the
    // first drop**, with nothing to report and nothing being sent that could
    // change that. The first messages are lost to the handshake whatever else
    // happens; what this test is about is the hole the *sender* made, and the
    // offering has to keep going until the image has seen it.
    let deadline = Instant::now() + DEADLINE;
    let payload = [7_u8; 1024];
    let mut received = 0_usize;
    let mut session_id = None;
    let mut found = None;

    while found.is_none() && Instant::now() < deadline {
        let _ = client.offer(publication, &payload);
        client.poll();
        client.poll_subscription(subscription, 10, |message| {
            received += 1;
            session_id = Some(message.header.session_id);
        });

        // Read the report again rather than trusting the first mapping: what a
        // reader in another process would see is the file as it stands.
        let report = LossReportFile::open_readonly(&directory).expect("the file is still there");
        found = entries(&report)
            .into_iter()
            .find(|entry| entry.stream_id == STREAM_ID);

        std::thread::sleep(Duration::from_millis(2));
    }

    assert!(received > 0, "the stream never started");

    let Some(entry) = found else {
        panic!(
            "no record for stream {STREAM_ID} in the loss report; the driver said:\n{}",
            own.log_tail(40)
        );
    };

    assert!(entry.observation_count > 0, "at least one observation");
    assert!(entry.total_bytes_lost > 0, "and bytes it accounted for");
    assert!(
        entry.last_observation_timestamp >= entry.first_observation_timestamp,
        "the timestamps run the way time does"
    );
    assert_eq!(
        channel.as_bytes(),
        entry.channel.as_slice(),
        "the record names the channel the client asked for"
    );
    assert_eq!(
        session_id.expect("a message arrived, so there is a session"),
        entry.session_id,
        "and the session is the one the stream is actually running"
    );

    let _ = cnc;
}

/// The records in the file, which is what every reader of it does.
fn entries(report: &LossReportFile) -> Vec<deepmsg_cnc::loss_report::LossReportEntry> {
    report.entries()
}

/// A UDP port nothing else on this machine is likely to hold.
fn free_port() -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 42_000 + (std::process::id() as u16 % 20_000);

    base
}

#[test]
fn the_file_is_named_the_way_the_reference_names_it() {
    assert_eq!("loss-report.dat", LOSS_REPORT_FILE_NAME);
}
