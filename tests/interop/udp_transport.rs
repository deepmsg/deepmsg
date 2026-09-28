//! A1 and A3: the network data plane, against the reference on the wire.
//!
//! The first test in this tree where a datagram is the unit of truth. Every
//! other interop test shares a memory file between two processes or reads a
//! file one process wrote; here our driver puts frames on a socket and a
//! program that shares no code with this build — the reference's own
//! `basic_subscriber`, behind the reference's own driver — has to believe them.
//!
//! The arrangement is the whole point, so it is worth stating plainly: `basic_subscriber`
//! runs against the **reference** driver in its own aeron directory, and our
//! client runs against **our** driver in another. Two drivers, one wire. That
//! is the only arrangement in which this build's SETUP, DATA and heartbeat
//! frames can be falsified rather than restated: a test that ran both ends
//! would agree with itself about a frame neither has seen the reference accept.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT, ReferenceDriver};
use deepmsg_tests::samples;

/// The stream every test here publishes on, so a failure names its own driver.
const STREAM_ID: i32 = 1001;

/// How long to keep offering while the far end has not answered.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// The counters a stalled publication is diagnosed with: what arrived, what
/// was refused, and how far the two limits let it go.
///
/// Read from the CnC file *while the driver is still running*, which is the
/// only time these have anything to say.
fn driver_diagnostics(aeron_dir: &std::path::Path) -> String {
    let Ok(cnc) = deepmsg_cnc::CncFile::open(aeron_dir, Duration::from_secs(5)) else {
        return "the CnC file could not be read".to_owned();
    };
    let Some(counters) = cnc.counters() else {
        return "the counter regions could not be read".to_owned();
    };

    let mut lines = Vec::new();
    counters.for_each(|descriptor| {
        // The system counters a UDP publication moves, by their reference
        // names (`aeron-driver/src/main/c/aeron_system_counters.c:24-71`).
        let named = match descriptor.counter_id {
            0 => "bytes sent",
            1 => "bytes received",
            8 => "status messages received",
            9 => "heartbeats sent",
            11 => "retransmits sent",
            14 => "invalid packets",
            _ => "",
        };

        if !named.is_empty() {
            lines.push(format!("  {named}: {}", descriptor.value));
        }

        if descriptor.label.starts_with("snd-lmt")
            || descriptor.label.starts_with("pub-lmt")
            || descriptor.label.starts_with("snd-pos")
            || descriptor.label.starts_with("pub-pos")
        {
            lines.push(format!("  {}: {}", descriptor.label, descriptor.value));
        }
    });

    lines.join("\n")
}

/// A UDP port nobody is listening on.
///
/// The reference subscriber binds the port when it starts, so the test needs a
/// number rather than an open socket: a socket held open by the test would be
/// the thing that received the frames. High ports are picked from a range the
/// ephemeral allocator rarely touches, and two drivers on one machine collide
/// only if something else chose exactly this pair — which is why the pair is
/// derived from the process id rather than fixed.
fn free_udp_port(offset: u16) -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 20_000 + (std::process::id() as u16 % 20_000);

    base.saturating_add(offset)
}

/// Offer until the driver's window permits it, or give up.
///
/// The window is closed until a status message has told the publication the
/// subscriber has room, and the subscriber learns where to send one only after
/// the first SETUP or DATA frame reaches it — so the first attempts are
/// expected to come back `NotConnected`, and that is the *point*: it is the
/// implicit-unicast setup chain this test exercises.
fn offer_within(
    client: &Client,
    registration_id: i64,
    payload: &[u8],
    within: Duration,
) -> Result<Appended, Vec<Appended>> {
    let deadline = Instant::now() + within;
    let mut seen = Vec::new();

    loop {
        match client.offer(registration_id, payload) {
            Some(outcome @ Appended::Ok { .. }) => return Ok(outcome),
            Some(other) => seen.push(other),
            None => return Err(seen),
        }

        if Instant::now() >= deadline {
            return Err(seen);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn our_udp_publication_reaches_a_reference_subscriber() {
    // The C++ sample rather than the C one, for a reason worth writing down:
    // `BasicSubscriber.cpp` ends every line with `std::endl`, which flushes,
    // and the C sample's `printf` into a *file* is fully buffered — a test that
    // watched the C sample would see nothing until it exited.
    let Some(subscriber_binary) = samples::locate("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };

    let Some(reference_binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    let mut reference = ReferenceDriver::start(&reference_binary, "udp-reference-subscriber")
        .expect("start the reference driver");

    let Some(mut own) = OwnDriver::start("udp-our-publisher") else {
        driver::announce_own_skip();
        return;
    };

    let port = free_udp_port(1);
    let channel = format!("aeron:udp?endpoint=localhost:{port}");
    let reference_dir = reference.aeron_dir().to_path_buf();

    // The subscriber first: it is the one that binds the port, and a publisher
    // that starts before it has a socket to send to would only say SETUP into
    // a closed port for a while.
    let mut subscriber = samples::Sample::start(
        &subscriber_binary,
        "subscriber",
        &reference_dir,
        &["-c", &channel, "-s", &STREAM_ID.to_string()],
    );

    subscriber.await_output(Duration::from_secs(20), "its channel", |output| {
        output.contains("Subscribing to channel")
    });

    let _reference_cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the reference driver must publish a readable CnC file");
    let _own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let mut publisher = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication_id = publisher
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP publication");

    let payload = b"deepmsg on the wire";
    let outcome = offer_within(&publisher, publication_id, payload, CONNECT_TIMEOUT);

    if outcome.is_err() {
        // Everything our driver counted while the window stayed shut: the
        // frames it received, the invalid ones it refused, and the two limits
        // the publisher is held between.
        let diagnostics = driver_diagnostics(own.aeron_dir());

        panic!(
            "the publication never opened its window: {} attempts, first {}\n\
             the subscriber saw:\n{}\nthe counters say:\n{diagnostics}\n\
             our driver said:\n{}",
            outcome.as_ref().err().map_or(0, Vec::len),
            outcome
                .as_ref()
                .err()
                .and_then(|seen| seen.first())
                .map_or_else(|| "none".to_owned(), |first| format!("{first:?}")),
            subscriber.output(),
            own.log_tail(40)
        );
    }

    let output = subscriber.await_output(Duration::from_secs(20), "the message", |output| {
        output.contains("Message to stream")
    });
    assert!(
        output.contains(std::str::from_utf8(payload).expect("ascii")),
        "the reference subscriber must receive the bytes this build sent:\n{output}"
    );
    assert!(
        output.contains(&format!("Message to stream {STREAM_ID}")),
        "on the stream the publication named:\n{output}"
    );

    let _ = subscriber.terminate(Duration::from_secs(5));
    let _ = own.stop();
    let _ = reference.stop();
}
