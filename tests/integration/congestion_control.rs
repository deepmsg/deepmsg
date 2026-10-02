//! G3-5: `cc=cubic` on a live stream — the round trip, end to end.
//!
//! The strategy's own arithmetic is the reference's C++ test's, case for case
//! (`crates/driver/src/congestion_control.rs`), and this is the half that no
//! unit test can reach: an image that decides to **measure**, a publication
//! that answers, and an answer that comes back to the image and lands in the
//! counter a client reads.
//!
//! Everything here is our own — our driver, our client — so it runs in CI.
//!
//! `AERON_CUBICCONGESTIONCONTROL_MEASURERTT` is off by default and stays off in
//! every other test: without it an image runs CUBIC's window arithmetic and
//! measures nothing, which is the reference's own default. The driver here is
//! configured the way `aeronmd -Daeron.cubiccongestioncontrol.measurertt=true`
//! would be.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream this test publishes on.
const STREAM_ID: i32 = 1001;

/// One kilobyte of payload.
const LENGTH: usize = 1024;

/// How long the whole exchange may take before the test is a failure.
const DEADLINE: Duration = Duration::from_secs(30);

/// A UDP port to name in the channel, derived from the process id so that two
/// tests running at once do not choose the same one.
fn free_port(offset: u16) -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 20_000 + (std::process::id() as u16 % 20_000);

    base.saturating_add(offset)
}

/// What the counters of CUBIC's two names say, in the order the reference
/// allocates them: the measured round trip and the window it is running.
///
/// Read by label, because that is what a client has: the two are
/// `PER_IMAGE` counters named for what they carry
/// (`AERON_CUBICCONGESTIONCONTROL_RTT_INDICATOR_COUNTER_NAME` and its window
/// twin, `aeron_congestion_control.h:27-28`).
fn cubic_counters(client: &Client) -> (Option<i64>, Option<i64>) {
    let Some(reader) = client.counters_reader() else {
        return (None, None);
    };

    let mut rtt = None;
    let mut window = None;

    let _ = reader.for_each(|descriptor| {
        if descriptor.label.starts_with("rcv-cc-cubic-rtt") {
            rtt = Some(descriptor.value);
        } else if descriptor.label.starts_with("rcv-cc-cubic-wnd") {
            window = Some(descriptor.value);
        }
    });

    (rtt, window)
}

/// Wait until both counters are there, and answer with what they say.
fn await_cubic_counters(client: &mut Client) -> (i64, i64) {
    let deadline = Instant::now() + DEADLINE;

    loop {
        let (rtt, window) = cubic_counters(client);

        if let (Some(rtt), Some(window)) = (rtt, window) {
            return (rtt, window);
        }

        assert!(
            Instant::now() < deadline,
            "the image never took CUBIC's two counters: rtt={rtt:?} window={window:?}"
        );

        client.poll();
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// A subscription that names `cc=cubic` gets CUBIC's window and, with the
/// setting on, a **measured** round trip: the image asks, the publication
/// answers, and the answer lands in `rcv-cc-cubic-rtt`.
///
/// This is the whole loop in one test. A build that wired the strategy but not
/// `initiate_rttm` would still publish, still deliver, and leave the round-trip
/// counter at zero for ever — the window counter alone cannot tell the two
/// apart.
#[test]
fn a_cubic_subscription_measures_its_round_trip() {
    let Some(mut own) = OwnDriver::start_with(
        "cubic-round-trip",
        &["-Daeron.cubiccongestioncontrol.measurertt=true"],
    ) else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let channel = format!("aeron:udp?endpoint=127.0.0.1:{}|cc=cubic", free_port(7));

    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication on a cubic channel");
    let subscription = client
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription on a cubic channel");

    // A few messages, so the image exists, has a connection, and has something
    // to measure against.
    let payload = vec![0x5Au8; LENGTH];
    let deadline = Instant::now() + DEADLINE;
    let mut received = 0_i64;

    while received < 10 {
        assert!(
            Instant::now() < deadline,
            "{received} of 10 messages after {DEADLINE:?}"
        );

        let _ = client.offer(publication, &payload);
        client.poll();

        received += client.poll_subscription(subscription, 10, |_message| {}) as i64;
    }

    let (rtt, window) = await_cubic_counters(&mut client);

    assert!(
        rtt > 0,
        "a measured round trip is a positive number, and {rtt} is what this image measured"
    );
    assert!(
        window > 0,
        "and the window CUBIC is running is what the second counter carries: {window}"
    );
    assert_eq!(
        0,
        window % 1408,
        "a congestion window is whole MTUs, which is what the arithmetic multiplies"
    );

    let _ = own.stop();
}

/// The **driver's** supplier wins over the channel's: with
/// `aeron.congestioncontrol.supplier=static` no image takes CUBIC's counters
/// however its channel is spelled.
///
/// The reference's chooser is what the `default` supplier *is*
/// (`aeron_congestion_control.c:45-62`), so naming another one is naming the
/// strategy for every image — and this is the difference between a setting that
/// is read and one that is merely carried.
#[test]
fn a_named_supplier_decides_for_every_image() {
    let Some(mut own) = OwnDriver::start_with(
        "cubic-static-supplier",
        &["-Daeron.congestioncontrol.supplier=static"],
    ) else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let channel = format!("aeron:udp?endpoint=127.0.0.1:{}|cc=cubic", free_port(13));

    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication on a cubic channel");
    let subscription = client
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription on a cubic channel");

    let payload = vec![0x77u8; LENGTH];
    let deadline = Instant::now() + DEADLINE;
    let mut received = 0_i64;

    while received < 10 {
        assert!(
            Instant::now() < deadline,
            "{received} of 10 messages after {DEADLINE:?}"
        );

        let _ = client.offer(publication, &payload);
        client.poll();

        received += client.poll_subscription(subscription, 10, |_message| {}) as i64;
    }

    let (rtt, window) = cubic_counters(&client);

    assert_eq!(
        (None, None),
        (rtt, window),
        "the driver's supplier built the static window, so there is nothing cubic to count"
    );

    let _ = own.stop();
}

/// A `cc=` this driver cannot serve is a **subscription that exists and an
/// image that is never built** — the reference's answer
/// (`aeron_congestion_control.c:165-205` fails its supplier, and
/// `aeron_driver_conductor.c:6633-6640` fails the create with a line naming the
/// stream and the session).
///
/// This build used to refuse the name on `ADD_SUBSCRIPTION`, which is early and
/// loud, and wrong: a channel that a *different* driver could serve is not a
/// channel this client may not subscribe to. What is left of the refusal is a
/// recorded error, which is what the reference has.
#[test]
fn a_cc_this_driver_cannot_serve_is_an_image_that_is_never_built() {
    let Some(mut own) = OwnDriver::start("cubic-unknown-cc") else {
        driver::announce_own_skip();
        return;
    };

    let cnc = own
        .await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let channel = format!("aeron:udp?endpoint=127.0.0.1:{}|cc=nonsense", free_port(11));

    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the publication is made, whatever its cc says");
    let subscription = client
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("and so is the subscription");

    let payload = vec![0x33u8; LENGTH];
    let deadline = Instant::now() + Duration::from_secs(3);

    while Instant::now() < deadline {
        let _ = client.offer(publication, &payload);
        client.poll();

        let images = client
            .subscription(subscription)
            .expect("the subscription")
            .images()
            .len();

        assert_eq!(
            0, images,
            "a strategy that cannot be built is an image that is not built"
        );

        std::thread::sleep(Duration::from_millis(5));
    }

    let mut entries = Vec::new();
    let _ = cnc
        .error_log()
        .expect("the CnC's error log")
        .read(0, &mut entries);

    assert!(
        entries.iter().any(|entry| {
            let text = format!("{:?}", entry);
            text.contains(&format!("stream_id={STREAM_ID}")) && text.contains("session_id=")
        }),
        "the driver recorded why, in the reference's own words: {entries:?}"
    );

    let _ = own.stop();
}

/// The same channel without the setting measures nothing, and **still runs
/// CUBIC**: the window counter is there, the round-trip counter stays at zero.
///
/// That is the reference's default (`measure_rtt` is off unless a setting turns
/// it on, `aeron_congestion_control.c:392-402`), and it is the case a build that
/// hung the whole strategy off the setting would get wrong.
#[test]
fn a_cubic_subscription_that_measures_nothing_still_runs_cubic() {
    let Some(mut own) = OwnDriver::start("cubic-quiet") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let channel = format!("aeron:udp?endpoint=127.0.0.1:{}|cc=cubic", free_port(9));

    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a publication on a cubic channel");
    let subscription = client
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription on a cubic channel");

    let payload = vec![0x11u8; LENGTH];
    let deadline = Instant::now() + DEADLINE;
    let mut received = 0_i64;

    while received < 10 {
        assert!(
            Instant::now() < deadline,
            "{received} of 10 messages after {DEADLINE:?}"
        );

        let _ = client.offer(publication, &payload);
        client.poll();

        received += client.poll_subscription(subscription, 10, |_message| {}) as i64;
    }

    let (rtt, window) = await_cubic_counters(&mut client);

    assert_eq!(
        0, rtt,
        "nothing was measured, so the counter says what it was allocated with"
    );
    assert!(
        window > 0,
        "and the window is CUBIC's all the same: {window}"
    );

    let _ = own.stop();
}
