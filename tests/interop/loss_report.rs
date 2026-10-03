//! The loss report, read by the **reference's own** reader.
//!
//! `LossStat` is the reference's sample that prints a driver's loss report
//! (`aeron-samples/src/main/c/loss_stat.c`): it maps `loss-report.dat` through
//! `aeron_cnc_loss_reporter_read` and writes one CSV line per record. Running
//! it against a directory this build's driver owns is the same kind of evidence
//! `BasicPublisher` is for the wire — the bytes are ours, the reading of them
//! is not.
//!
//! The other half is `tests/integration/loss_report.rs`, which reads the file
//! with this build's own reader and needs no reference checkout. What *cannot*
//! be the oracle is `io.aeron.GapFillLossTest`: its loss is injected through a
//! Java object in the test's own process
//! (`TestMediaDriver.enableRandomLoss`), which an external driver never sees.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_tests::driver::{self, OwnDriver};
use deepmsg_tests::samples;

/// The stream the loss happens on.
const STREAM_ID: i32 = 1005;

/// How long anything here may take.
const DEADLINE: Duration = Duration::from_secs(30);

#[test]
fn the_references_loss_stat_reads_what_this_driver_wrote() {
    let Some(loss_stat_binary) = samples::locate("LossStat") else {
        driver::announce_tool_skip("LossStat");
        return;
    };

    let Some(mut own) = OwnDriver::start_with(
        "loss-report-interop",
        &["-Ddeepmsg.debug.send.data.loss.drop.every=4"],
    ) else {
        driver::announce_own_skip();
        return;
    };

    let cnc = own
        .await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let directory = own.aeron_dir().to_owned();
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

    // Publish until the reference's reader has a record to print, and not
    // merely until a message gets back. One loop and not two, for the reason
    // `tests/integration/loss_report.rs` gives: the sender drops every fourth
    // datagram and a hole is only *seen* once a frame arrives behind it, so a
    // client that stopped offering at the first message would stop one frame
    // short of the first drop — and this test would then be waiting out its
    // whole deadline for a record nothing was going to make.
    let deadline = Instant::now() + DEADLINE;
    let payload = [9_u8; 1024];
    let mut received = 0_usize;
    let mut said = String::new();

    while Instant::now() < deadline {
        // A burst, and not one: the client's offer is what a term gets, and the
        // driver's sender thread is what puts frames on the wire between the
        // two reads below.
        for _ in 0..8 {
            let _ = client.offer(publication, &payload);
            client.poll();
            client.poll_subscription(subscription, 10, |_message| received += 1);
        }

        // `LossStat` is not one of the `-p` samples `deepmsg_tests::samples`
        // starts: its base path is `-d` (`loss_stat.c:94-99`), so it is run
        // here the way its own usage says.
        let output = std::process::Command::new(&loss_stat_binary)
            .arg("-d")
            .arg(&directory)
            .output()
            .expect("run the reference's LossStat");

        said = String::from_utf8_lossy(&output.stdout).to_string();

        assert!(
            output.status.success(),
            "the reference's LossStat failed; it said:\n{said}"
        );

        // Both halves, because the record is written by the image and read by
        // the client at different moments: the image can see the hole before
        // the subscription has handed a message up, and stopping there would
        // leave the sanity check below with nothing to check.
        if received > 0 && said.contains("entries read") && !said.contains("0 entries read") {
            break;
        }

        std::thread::sleep(Duration::from_millis(50));
    }

    assert!(received > 0, "the stream never started");
    assert!(
        said.contains("OBSERVATION_COUNT"),
        "LossStat must print its header; it said:\n{said}"
    );
    assert!(
        !said.contains("0 entries read"),
        "LossStat found no records in a file this driver wrote; it said:\n{said}"
    );

    // One line per record, and the stream is ours: `observation_count,
    // total_bytes_lost, first, last, session, stream, channel, source`
    // (`loss_stat.c:68-72`).
    let record = said
        .lines()
        .find(|line| {
            let mut fields = line.split(',');
            let _observation_count = fields.next();
            let _total_bytes_lost = fields.next();
            let _first = fields.next();
            let _last = fields.next();
            let _session = fields.next();

            fields
                .next()
                .and_then(|stream| stream.trim().parse::<i32>().ok())
                == Some(STREAM_ID)
        })
        .unwrap_or_else(|| panic!("no record for stream {STREAM_ID}; LossStat said:\n{said}"));

    assert!(
        record.contains(&channel),
        "the record names the channel: {record}"
    );

    let _ = cnc;
}

/// A UDP port nothing else on this machine is likely to hold.
fn free_port() -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 44_000 + (std::process::id() as u16 % 20_000);

    base
}
