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
use deepmsg_driver::position::type_id::RECEIVER_NAKS_SENT;
use deepmsg_driver::protocol::{SetupFrame, StatusMessageFrame};
use deepmsg_driver::sys::AddressFamily;
use deepmsg_driver::sys::socket::{DatagramSocket, Datagrams};
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
/// What the *client* sees: the subscriptions it holds and the images it has
/// been told about. A driver that built an image and a client that never heard
/// of one is a different failure from a client that has one and cannot read it.
fn client_view(client: &Client, subscription_id: i64) -> String {
    let orphaned = client.orphan_images();
    let unknown = client.unknown_responses();
    let discarded = client.discarded();
    let laps = client.laps();
    let Some(subscription) = client.subscription(subscription_id) else {
        return format!(
            "  the client has no such subscription (orphaned={orphaned} unknown={unknown} \
             discarded={discarded} laps={laps})"
        );
    };

    let images = subscription.images();

    if images.is_empty() {
        return format!(
            "  the subscription has no image (orphaned={orphaned} unknown={unknown} \
             discarded={discarded} laps={laps})"
        );
    }

    images
        .iter()
        .map(|image| {
            format!(
                "  image {} session {} position {}",
                image.registration_id(),
                image.session_id(),
                image.position()
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn counters_of(cnc: &deepmsg_cnc::CncFile) -> String {
    let Some(counters) = cnc.counters() else {
        return "the counter regions could not be read".to_owned();
    };

    let mut lines = Vec::new();
    let mut total = 0;
    let mut every = Vec::new();
    counters.for_each(|descriptor| {
        total += 1;
        every.push(format!(
            "    #{} {:?}",
            descriptor.counter_id, descriptor.label
        ));
        // The system counters a UDP publication moves, by their reference
        // names (`aeron-driver/src/main/c/aeron_system_counters.c:24-71`).
        let named = match descriptor.counter_id {
            0 => "bytes sent",
            1 => "bytes received",
            8 => "status messages received",
            7 => "status messages sent",
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
            || descriptor.label.starts_with("rcv-hwm")
            || descriptor.label.starts_with("rcv-pos")
            || descriptor.label.starts_with("rcv-channel")
            || descriptor.label.starts_with("sub-pos")
            || descriptor.label.starts_with("snd-channel")
        {
            lines.push(format!("  {}: {}", descriptor.label, descriptor.value));
        }
    });

    lines.insert(0, format!("  ({total} counters)"));
    lines.extend(every.iter().take(8).cloned());

    // The error log is where a driver says what it could not do.
    if let Some(log) = cnc.error_log() {
        let mut entries = Vec::new();
        let _ = log.read(0, &mut entries);

        for entry in entries {
            lines.push(format!("  error: {}", entry.text));
        }
    }

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
    let own_cnc = own
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
        let diagnostics = counters_of(&own_cnc);

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
            own.log_tail(60)
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

    // A17: a session the reference believes refuses no status message.
    //
    // The two numbers are asserted together because either alone is worthless.
    // A zero on a counter that was never given anything to refuse is not the
    // check working, it is the check not being reached — and a count of status
    // messages received is the proof that this session used the path this
    // counts. What it does *not* prove is that the check is switched on: with
    // the comparison in `sender.rs` removed nothing is refused either, and this
    // assertion stays green. The test that holds the check itself in place is
    // `a_status_message_is_only_valid_where_the_publication_can_place_it`
    // (`network_publication.rs`), against
    // `aeron_network_publication_is_valid_status_message`
    // (`aeron_network_publication.c:841-856`).
    let received_status_messages = counter_like(&own_cnc, "Status Messages received")
        .and_then(|(counter_id, _, _, _)| counter_value_of(&own_cnc, counter_id));
    let rejected_status_messages = counter_like(&own_cnc, "Status Messages rejected")
        .and_then(|(counter_id, _, _, _)| counter_value_of(&own_cnc, counter_id));
    // Read before the driver goes, which is when this file still exists.
    let dump = counters_of(&own_cnc);

    let _ = subscriber.terminate(Duration::from_secs(5));
    let _ = own.stop();
    let _ = reference.stop();

    assert!(
        matches!(received_status_messages, Some(count) if count > 0),
        "the reference subscriber has to have sent status messages before a count of refused \
         ones means anything: {received_status_messages:?} received, \
         {rejected_status_messages:?} rejected.\ncounter dump:\n{dump}"
    );
    assert_eq!(
        Some(0),
        rejected_status_messages,
        "a status message the reference sent about a stream it is reading is one this publication \
         can place, so none of them may be refused (`aeron_network_publication.c:841-856`)."
    );
}

/// A5, the half that needs a second process: a frame this driver withholds is
/// a frame a real receiver has to notice missing, ask for, and get on the
/// second attempt.
///
/// Every other test of loss recovery in this build drives one frame through
/// one function: the gap detector, the NAK, the resend. Each agrees with
/// itself about what the other end would do. Here the wire is made to lose
/// something — the one thing loopback never does — and the reference's own
/// subscriber, behind the reference's own driver, is the judge of whether the
/// stream came out whole anyway.
///
/// The two halves of the claim are separate assertions on purpose: that the
/// subscriber got every message, and that this driver's own counters say it
/// retransmitted. A message that arrived without a retransmission would mean
/// the injection never hit a data frame, and a retransmission without the
/// messages would mean the recovery made things worse.
#[test]
fn a_withheld_frame_is_retransmitted_until_the_reference_subscriber_has_it() {
    let Some(subscriber_binary) = samples::locate("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };

    let Some(reference_binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    let mut reference = ReferenceDriver::start(&reference_binary, "udp-loss-reference-subscriber")
        .expect("start the reference driver");

    // One data frame in every four is withheld. The count is over frames and
    // the slot is the reference's, so the SETUP and heartbeat frames that go
    // the same way are counted with the data frames
    // (`media/aeron_send_channel_endpoint.c:391-403`, which is also where the
    // reference's own generator is consulted).
    let Some(mut own) = OwnDriver::start_with(
        "udp-our-lossy-publisher",
        &["-Ddeepmsg.debug.send.data.loss.drop.every=4"],
    ) else {
        driver::announce_own_skip();
        return;
    };

    // Offset four, because the tests in this file run in parallel and each
    // offset is a port: two of them naming the same number is two drivers
    // fighting over one socket.
    let port = free_udp_port(4);
    let channel = format!("aeron:udp?endpoint=localhost:{port}");
    let reference_dir = reference.aeron_dir().to_path_buf();

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
    let own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let mut publisher = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication_id = publisher
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP publication");

    // The stream has to be **many datagrams long**, which is what the count
    // the generator keeps is over: a datagram carries as many frames as fit in
    // the MTU, so a handful of small messages is one datagram and one withheld
    // call would be the entire stream. Two hundred and fifty-six of them is
    // sixteen kilobytes, a dozen datagrams, and several gaps in the middle of
    // the stream — each of which owes the receiver a NAK and this driver a
    // retransmission.
    const MESSAGES: usize = 256;
    let mut payloads = Vec::new();

    for index in 0..MESSAGES {
        let payload = format!("loss-frame-{index:03}");
        offer_within(
            &publisher,
            publication_id,
            payload.as_bytes(),
            CONNECT_TIMEOUT,
        )
        .unwrap_or_else(|seen| {
            panic!(
                "the publication never opened its window ({payload}): {} attempts, first {:?}\n{}",
                seen.len(),
                seen.first(),
                subscriber.output()
            )
        });
        payloads.push(payload);
    }

    // The last message is the one that needs every earlier gap filled: it
    // cannot arrive while the receiver is still waiting for something in
    // front of it.
    let last = payloads.last().cloned().expect("a last payload");
    let output = subscriber.await_output(Duration::from_secs(30), "every message", |output| {
        output.contains(&last)
    });

    let retransmits = counter_value_of(
        &own_cnc,
        deepmsg_driver::system_counters::id::RETRANSMITS_SENT,
    );
    let retransmitted_bytes = counter_value_of(
        &own_cnc,
        deepmsg_driver::system_counters::id::RETRANSMITTED_BYTES,
    );

    let _ = subscriber.terminate(Duration::from_secs(5));
    let _ = own.stop();
    let _ = reference.stop();

    for payload in &payloads {
        assert!(
            output.contains(payload),
            "the reference subscriber never received {payload}, so a withheld \
             frame was not recovered:\n{output}"
        );
    }

    assert!(
        retransmits.is_some_and(|value| value > 0),
        "a withheld frame is a frame a receiver has to ask for, and this \
         driver counted no retransmission: {retransmits:?}\n\
         the counters say:\n{}\nthis driver said:\n{}\nthe subscriber saw:\n{}",
        counters_of(&own_cnc),
        own.log_tail(60),
        output
    );
    assert!(
        retransmitted_bytes.is_some_and(|value| value > 0),
        "the bytes of the answer are counted too: {retransmitted_bytes:?}"
    );
}

/// A2: a reference publisher's messages, read by our client through our driver.
///
/// The other direction of A1, and the harder one: everything that has to work
/// is on our side. The reference's `BasicPublisher` sends `SETUP` and `DATA`
/// into a socket our driver bound, our driver has to notice a session nothing
/// serves, ask for the setup, build an image from it, tell our client where the
/// log buffer is, and then — the part no single-sided test can reach — send
/// status messages the reference's flow control will believe, or the publisher
/// stops after one window's worth of data.
#[test]
fn a_reference_publishers_messages_reach_our_subscriber() {
    let Some(publisher_binary) = samples::locate("BasicPublisher") else {
        driver::announce_tool_skip("BasicPublisher");
        return;
    };

    let Some(reference_binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    // Two names, because the harness names each driver's log after the test —
    // one name for both would put two drivers' output in one file.
    let Some(mut own) = OwnDriver::start("udp-our-subscriber") else {
        driver::announce_own_skip();
        return;
    };

    let mut reference = ReferenceDriver::start(&reference_binary, "udp-reference-publisher")
        .expect("start the reference driver");

    let port = free_udp_port(2);
    let channel = format!("aeron:udp?endpoint=localhost:{port}");

    let _reference_cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the reference driver must publish a readable CnC file");
    let own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    // The subscriber first: it is the side that binds the port, and the
    // reference publisher would be sending into a closed one.
    let mut subscriber = Client::connect(own.aeron_dir()).expect("connect our client");
    let subscription_id = subscriber
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP subscription");

    let messages = 5;
    let mut publisher = samples::Sample::start(
        &publisher_binary,
        "publisher",
        reference.aeron_dir(),
        &[
            "-c",
            &channel,
            "-s",
            &STREAM_ID.to_string(),
            "-m",
            &messages.to_string(),
        ],
    );

    publisher.await_output(Duration::from_secs(20), "its publication", |output| {
        output.contains("Publication") || output.contains("published")
    });

    let received = drain_messages(
        &mut subscriber,
        subscription_id,
        Duration::from_secs(30),
        messages,
    );

    let _ = publisher.terminate(Duration::from_secs(5));
    let _ = own.stop();
    let _ = reference.stop();

    assert!(
        !received.is_empty(),
        "the subscription never assembled a message.\nour driver's counters:\n{}\n\
         the client sees:\n{}\nthe publisher said:\n{}\nour driver said:\n{}",
        counters_of(&own_cnc),
        client_view(&subscriber, subscription_id),
        publisher.output(),
        own.log_tail(60)
    );

    let text = String::from_utf8_lossy(&received[0]).to_string();
    assert!(
        text.starts_with("Hello World!"),
        "the bytes are the reference's own: {text:?}"
    );
    assert_eq!(
        messages,
        received.len(),
        "every message the reference published arrived: {:?}",
        received
            .iter()
            .map(|message| String::from_utf8_lossy(message).to_string())
            .collect::<Vec<_>>()
    );
}

/// Collect `expected` messages, or as many as arrive before the deadline.
fn drain_messages(
    client: &mut Client,
    subscription_id: i64,
    within: Duration,
    expected: usize,
) -> Vec<Vec<u8>> {
    use deepmsg_client::client::FRAGMENT_LIMIT;
    use deepmsg_client::fragment_assembler::Message;

    let deadline = Instant::now() + within;
    let mut collected = Vec::new();

    while Instant::now() < deadline && collected.len() < expected {
        // `poll` is what reads the driver's events — an `ON_AVAILABLE_IMAGE`
        // does not arrive by itself — and `poll_subscription` is what reads
        // the image it attaches.
        client.poll();

        client.poll_subscription(subscription_id, FRAGMENT_LIMIT, |message: Message<'_>| {
            collected.push(message.payload.to_vec());
        });

        std::thread::sleep(Duration::from_millis(10));
    }

    collected
}

/// A11: a driver stopped while a UDP session is in flight stops cleanly.
///
/// The termination path is the one place where the data plane's threads have
/// to be joined rather than left behind: a receiver holding a socket, a sender
/// holding a publication's log buffer, and a conductor about to unmap the CnC
/// file they both read from. A driver that skipped a join would fail here in
/// one of two ways — a panic in its log, or an exit status that says it did not
/// stop on purpose.
#[test]
fn a_udp_session_stops_cleanly_when_the_driver_is_asked_to() {
    use deepmsg_client::terminate::{TerminationOutcome, request_driver_termination};

    // The validator is what makes TERMINATE_DRIVER take effect rather than be
    // refused; the delete-on-shutdown flag is what makes the clean stop
    // observable from outside.
    let Some(mut own) = OwnDriver::start_with(
        "udp-terminate",
        &[
            "-Ddirs.delete.on.shutdown=true",
            // The `deepmsg.` prefix is this build's own namespace for settings
            // it added (ADR-0005); `crates/driver/tests/lifecycle.rs` uses the
            // same spelling for the same reason.
            "-Ddeepmsg.driver.termination.validator=allow",
        ],
    ) else {
        driver::announce_own_skip();
        return;
    };

    let _own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    // A live session: a subscription bound to a port, and a publisher sending
    // into it from a plain socket. Both halves are up when the driver is asked
    // to stop.
    let mut subscriber = Client::connect(own.aeron_dir()).expect("connect our client");
    let port = free_udp_port(5);
    let channel = format!("aeron:udp?endpoint=127.0.0.1:{port}");
    let subscription_id = subscriber
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP subscription");

    let publisher = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
    publisher
        .bind("127.0.0.1:0".parse().expect("an address"))
        .expect("a bind");

    let setup = SetupFrame {
        term_offset: 0,
        session_id: 7,
        stream_id: STREAM_ID,
        initial_term_id: 1_000,
        active_term_id: 1_000,
        term_length: 64 * 1024,
        mtu: 1408,
        ttl: 0,
    };
    let mut frame = [0u8; SetupFrame::LENGTH];
    assert!(setup.write_with_flags(&mut frame, 0).is_some());

    let _ = publisher.send_batch(
        Some(format!("127.0.0.1:{port}").parse().expect("an address")),
        &[&frame],
    );

    // Let the driver build the image and answer with a status message, so both
    // sides of the data plane are doing something when it is stopped.
    for _ in 0..40 {
        subscriber.poll();

        if subscriber
            .subscription(subscription_id)
            .is_some_and(|subscription| !subscription.images().is_empty())
        {
            break;
        }

        std::thread::sleep(Duration::from_millis(10));
    }

    let outcome =
        request_driver_termination(own.aeron_dir(), b"").expect("the request is well formed");

    assert_eq!(
        TerminationOutcome::Committed,
        outcome,
        "a driver with an allowing policy is asked, not refused"
    );

    // The driver has to *run* the command: the request is a record on its
    // command ring, and a driver that is stopped by a signal before its next
    // pass exits by that signal instead. The directory it deletes on shutdown
    // is the observable proof that it got there on its own.
    let dir = own.aeron_dir().to_path_buf();
    let deadline = Instant::now() + Duration::from_secs(10);

    while Instant::now() < deadline && dir.exists() {
        std::thread::sleep(Duration::from_millis(10));
    }

    let status = own.stop().expect("the driver is stopped");
    let log = own.log_tail(20);

    assert!(
        status.success(),
        "a driver asked to stop exits successfully, got {status:?}: {log}"
    );
    assert!(
        !log.contains("panicked"),
        "and no thread it owns panicked on the way out: {log}"
    );
    assert!(
        !dir.exists(),
        "and it took its directory with it, which is the last thing it does"
    );
}

/// A6: the far end's window is what stops the producer, and what starts it
/// again.
///
/// The whole flow-control loop, with a plain socket playing the subscriber:
/// one status message opens a *small* window, the producer fills it and is
/// told to stop, and a second status message with more room is what lets it
/// write again. Nothing else in the suite crosses that boundary — the IPC
/// window tests are about *readers*, and this one is about a receiver's
/// advertised window arriving over a socket.
#[test]
fn a_full_window_stops_the_producer_and_a_larger_one_starts_it_again() {
    let Some(mut own) = OwnDriver::start("udp-flow-control") else {
        driver::announce_own_skip();
        return;
    };

    let _own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let mut subscriber = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
    subscriber
        .bind("127.0.0.1:0".parse().expect("an address"))
        .expect("a bind");
    subscriber.set_nonblocking().expect("non-blocking");
    let bound = subscriber.local_address().expect("a bound address");

    // A *small* publication window, so the test can fill the producer's
    // allowance in a handful of messages. Without it the producer may run half
    // a term ahead of what has been sent — which is the design, and 32 MiB of
    // offering is not a test.
    let channel = format!("aeron:udp?endpoint={bound}|pub-wnd=1408");

    let mut publisher = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication_id = publisher
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP publication");

    // The publisher's address, from its first SETUP.
    let mut buffers = vec![vec![0u8; 2048]];
    let mut datagrams = Datagrams::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut publisher_address = None;
    let mut initial_term_id = 0;

    while Instant::now() < deadline && publisher_address.is_none() {
        publisher.poll();

        let received = subscriber
            .receive_batch(&mut buffers, &mut datagrams)
            .unwrap_or(0);

        for (slot, datagram) in datagrams.as_slice()[..received].iter().enumerate() {
            if datagram.length == 0 {
                continue;
            }

            publisher_address = Some(datagram.source.expect("a source"));

            // The `SETUP` carries the term the stream started at, which is what
            // a status message's consumption position is *relative to*: an SM
            // that names term zero is naming a term that does not exist, and
            // the window edge it computes is nonsense.
            if let Some(setup) = SetupFrame::read(&buffers[slot][..datagram.length]) {
                initial_term_id = setup.initial_term_id;
            }
        }

        std::thread::sleep(Duration::from_millis(5));
    }

    let publisher_address = publisher_address.expect("the publication says SETUP before it sends");
    let session_id = session_of(&_own_cnc, publication_id);

    // A window of one frame, and nothing read from it: the *sender* may put
    // one frame on the wire, and the producer may fill its own window of
    // `pub-wnd` bytes ahead of that.
    let small_window = 32 + 100;
    send_status(
        &subscriber,
        publisher_address,
        session_id,
        initial_term_id,
        small_window,
    );

    let payload = [7u8; 100];

    // Offer until the window stops it. The first offers are `NotConnected`
    // until the status message has been read, so this waits for
    // `BackPressured` rather than treating anything else as an answer.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut blocked = false;

    while Instant::now() < deadline {
        publisher.poll();

        if let Some(Appended::BackPressured) = publisher.offer(publication_id, &payload) {
            blocked = true;
            break;
        }

        std::thread::sleep(Duration::from_millis(5));
    }

    if !blocked {
        let diagnostics = counters_of(&_own_cnc);
        let _ = own.stop();

        panic!(
            "a producer may run one window ahead of what has been sent, and no further.\
             \nthe counters:\n{diagnostics}"
        );
    }

    // Now the receiver says it has room: the producer may write again.
    send_status(
        &subscriber,
        publisher_address,
        session_id,
        initial_term_id,
        1024 * 1024,
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut unblocked = false;

    while Instant::now() < deadline {
        publisher.poll();

        if matches!(
            publisher.offer(publication_id, &payload),
            Some(deepmsg_core::logbuffer::append::Appended::Ok { .. })
        ) {
            unblocked = true;
            break;
        }

        std::thread::sleep(Duration::from_millis(5));
    }

    let _ = own.stop();

    assert!(
        unblocked,
        "a larger window is what lets the producer write again"
    );
}

/// The session id a publication runs under, read from its `pub-pos` counter's
/// key — the only place a client can learn it before an image exists.
fn session_of(cnc: &deepmsg_cnc::CncFile, _publication_id: i64) -> i32 {
    let Some(counters) = cnc.counters() else {
        return 0;
    };

    let mut session_id = 0;
    counters.for_each(|descriptor| {
        if descriptor.label.starts_with("pub-pos") && session_id == 0 {
            let key = counters.key(descriptor.counter_id).unwrap_or([0u8; 112]);
            session_id = i32::from_le_bytes(key[8..12].try_into().unwrap_or([0; 4]));
        }
    });

    session_id
}

/// One status message, from the socket playing the subscriber.
fn send_status(
    subscriber: &DatagramSocket,
    to: std::net::SocketAddr,
    session_id: i32,
    consumption_term_id: i32,
    window: i32,
) {
    let sm = StatusMessageFrame {
        session_id,
        stream_id: STREAM_ID,
        consumption_term_id,
        consumption_term_offset: 0,
        receiver_window: window,
        receiver_id: 1,
    };
    let mut frame = [0u8; StatusMessageFrame::LENGTH];
    assert!(sm.write_with_flags(&mut frame, 0).is_some());

    subscriber
        .send_batch(Some(to), &[&frame])
        .expect("a status message");
}

/// A3 and A8: the whole slice without the reference, and the two channel-status
/// counters a client can read.
///
/// One driver, two clients: one publishes over UDP, one subscribes to the same
/// channel, and the messages cross. A8 rides along in the same session — both
/// sides' `snd-channel` and `rcv-channel` counters go `ACTIVE` when their
/// sockets come up, which is what `channel_status_indicator_id` is for.
#[test]
fn two_of_our_clients_talk_over_udp_and_both_channels_report_active() {
    let Some(mut own) = OwnDriver::start("udp-two-clients") else {
        driver::announce_own_skip();
        return;
    };

    let own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let port = free_udp_port(3);
    let channel = format!("aeron:udp?endpoint=127.0.0.1:{port}");

    // The subscriber first: it binds the port the publisher will send to.
    let mut subscriber = Client::connect(own.aeron_dir()).expect("connect the subscriber");
    let subscription_id = subscriber
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP subscription");

    let mut publisher = Client::connect(own.aeron_dir()).expect("connect the publisher");
    let publication_id = publisher
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP publication");

    // A8, first half: the subscription's `rcv-channel` counter is up.
    let status_id = subscriber
        .subscription(subscription_id)
        .and_then(|subscription| subscription.channel_status_indicator_id())
        .expect("a UDP subscription has a channel status counter");

    assert_eq!(
        Some(CHANNEL_STATUS_ACTIVE),
        counter_value_of(&own_cnc, status_id),
        "the receive endpoint's socket is up"
    );

    // A8, second half: so is the publication's.
    let send_status_id = publisher
        .publication(publication_id)
        .expect("the publication")
        .channel_status_indicator_id();

    assert_eq!(
        Some(CHANNEL_STATUS_ACTIVE),
        counter_value_of(&own_cnc, send_status_id),
        "the send endpoint's socket is up"
    );

    // A3: the message crosses.
    let payload = b"from one of ours to the other";
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut received = Vec::new();

    while Instant::now() < deadline && received.is_empty() {
        publisher.poll();
        subscriber.poll();

        let _ = publisher.offer(publication_id, payload);

        received = drain_messages(
            &mut subscriber,
            subscription_id,
            Duration::from_millis(50),
            1,
        );
    }

    let _ = own.stop();

    assert_eq!(
        vec![payload.to_vec()],
        received,
        "the bytes crossed one driver and two sockets"
    );
}

/// A second `ADD_PUBLICATION` on a channel that already has one is *the same*
/// publication, and the answer it gets has to say where that publication's log
/// buffer is.
///
/// The reference answers both of its paths with `publication->log_file_name`
/// (`aeron_driver_conductor.c:4195-4196` is the shared one — the link path,
/// reached by a second caller rather than by a new publication). Answering with
/// nothing is not a smaller thing to say: `ON_PUBLICATION_READY`'s tail *is* the
/// name, so a client given an empty one has no buffer to map and cannot offer
/// at all — the sharing works and the caller it was done for cannot use it.
///
/// The delivery half is what makes the first half evidence. "It was answered"
/// is satisfied by an answer naming any file at all, including one no
/// publication is writing into, and the client maps whatever it is told: bytes
/// offered through the publication the second call returned, arriving at a
/// subscriber, are satisfied only by the buffer the two share.
#[test]
fn a_second_publication_on_a_channel_is_answered_with_the_buffer_they_share() {
    let Some(mut own) = OwnDriver::start("udp-shared-publication") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let port = free_udp_port(12);
    let channel = format!("aeron:udp?endpoint=127.0.0.1:{port}");

    let mut subscriber = Client::connect(own.aeron_dir()).expect("connect the subscriber");
    let subscription_id = subscriber
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP subscription");

    let mut publisher = Client::connect(own.aeron_dir()).expect("connect the publisher");
    let first = publisher
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP publication");

    // The same command on the same channel: one publication, two registration
    // ids. This is the call that is answered with no file name at all unless
    // the link path carries one.
    let second = publisher
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a second caller on the same channel is given the publication that exists");

    // Two handles on **one** publication, which is what the reference's reply
    // carries two ids for (`aeron_publication_buffers_ready_t`,
    // `aeron_driver_conductor.c:2387-2388`): the `correlation_id` of the add
    // being answered — the handle a client removes by, and the one the C client
    // keeps as `resource->registration_id` (`aeron_client_conductor.c:332`) —
    // and the publication's own `registration_id`, which is what the *log
    // buffer* is keyed by (`:385`) and what every client on the channel shares.
    assert_ne!(
        first, second,
        "each add is its own handle, though both are one publication"
    );

    let first_publication = publisher.publication(first).expect("the first handle");
    let second_publication = publisher.publication(second).expect("the second handle");

    assert_eq!(
        first_publication.position_limit_counter_id(),
        second_publication.position_limit_counter_id(),
        "and the handles are two views of one log buffer, which is what `pub-lmt` belongs to"
    );

    let payload = b"offered through the second add";
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut received = Vec::new();

    while Instant::now() < deadline && received.is_empty() {
        publisher.poll();
        subscriber.poll();

        let _ = publisher.offer(second, payload);

        received = drain_messages(
            &mut subscriber,
            subscription_id,
            Duration::from_millis(50),
            1,
        );
    }

    let _ = own.stop();

    assert_eq!(
        vec![payload.to_vec()],
        received,
        "the second registration writes into the same log buffer as the first"
    );
}

/// P1-4's untethered subscriptions, end to end: a reader that stops reading is
/// put aside, and then either woken or closed.
///
/// The state machine has its own tests, one frame at a time, and what they
/// cannot show is the half that leaves the driver: `ON_UNAVAILABLE_IMAGE` on a
/// client's ring, an image that comes back at the join position, and a counter
/// that goes back to the manager when the reader is not rejoining. Each of
/// those is a *decision the conductor makes about someone else's memory*, and
/// the only place to see one is a running driver with a real image.
///
/// The arrangement is three subscriptions on one image — one that keeps up and
/// one that stalls — because **"behind" is relative to the fastest reader**
/// (`untethered_window_limit = (max_sub_pos - window) + window / 4`,
/// `aeron_publication_image.c:1199-1215`), so a lone subscriber is never put
/// aside however slowly it reads. One reader that keeps up is what makes the
/// other late at all.
///
/// The channel is our own driver's on both ends — one driver publishing and
/// subscribing, which is the only arrangement in which the *receive* side of a
/// network publication exists in a test without a second process.
///
/// **`rejoin` is the image's and not a reader's**, which is why the two outcomes
/// are two tests rather than one image with a rejoining reader and a closing one
/// on it. The reference refuses two subscriptions on one endpoint and stream
/// that disagree about it (`aeron_driver_conductor_has_clashing_subscription`,
/// `aeron_driver_conductor.c:334-346` — probed against a live reference driver,
/// which answers with the same `option conflicts with existing subscription:
/// rejoin=false` this build does), and every position takes the value the image
/// was created with (`aeron_publication_image.c:279`). An earlier version of
/// this test had the two readers differ on it, and passed only because this
/// build did not check.
struct Untethered {
    own: OwnDriver,
    cnc: deepmsg_cnc::CncFile,
    client: Client,
    keeper: i64,
    staller: i64,
    publication: i64,
}

impl Untethered {
    /// Start the driver, add the two subscriptions and the publication, and
    /// stop reading on one of them.
    ///
    /// `rejoin` goes on **both** subscriptions: they share an image, so they
    /// have to agree. The keeper stays tethered and so is never put aside,
    /// which is what makes the stall measurable at all.
    fn stall(rejoin: bool, port_offset: u16, name: &str) -> Option<Self> {
        // The three stage timeouts are the driver's, not the channel's — and that
        // is not a shortcut: the image reads them from the **endpoint's** URI
        // (`aeron_publication_image.c:243`, `aeron_driver_uri_subscription_params`,
        // which starts from the context's defaults), so a subscription that names
        // them on a channel another subscription already created an endpoint for
        // is naming them at nobody. Configuring the driver is what the image
        // actually inherits.
        let mut own = OwnDriver::start_with(
            name,
            &[
                "-Daeron.untethered.window.limit.timeout=200ms",
                "-Daeron.untethered.linger.timeout=200ms",
                "-Daeron.untethered.resting.timeout=200ms",
                // The window the machine measures a reader's lag against — three
                // quarters of it before a reader is late
                // (`aeron_publication_image.c:1180-1181`). The 128 KiB default takes
                // a hundred-odd round trips through the reference driver to fill;
                // eight kibibytes is the same test with one.
                "-Daeron.rcv.initial.window.length=8k",
            ],
        )?;

        let cnc = own
            .await_cnc(READY_TIMEOUT)
            .expect("this driver must publish a readable CnC file");

        // Each test takes its own port offset: the tests in this file run in
        // parallel and the port is derived from the process, which they share.
        let port = free_udp_port(port_offset);
        let channel = format!("aeron:udp?endpoint=127.0.0.1:{port}");
        let untethered = format!("{channel}|tether=false|rejoin={rejoin}");

        let mut client = Client::connect(own.aeron_dir()).expect("connect our client");

        let keeper = client
            .add_subscription(
                &format!("{channel}|rejoin={rejoin}"),
                STREAM_ID,
                DEFAULT_TIMEOUT,
            )
            .expect("our driver must confirm the reading subscription");
        let staller = client
            .add_subscription(&untethered, STREAM_ID, DEFAULT_TIMEOUT)
            .expect("our driver must confirm the stalling subscription");

        // The publication last, so both subscriptions join where the image
        // starts: the lag this test needs has to come from the data, not from
        // the order the commands were sent in.
        let publication = client
            .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
            .expect("our driver must confirm the UDP publication");

        let mut fixture = Self {
            own,
            cnc,
            client,
            keeper,
            staller,
            publication,
        };

        let both = |client: &mut Client| {
            images_of(client, fixture.keeper) > 0 && images_of(client, fixture.staller) > 0
        };
        assert!(
            wait_for(&mut fixture.client, CONNECT_TIMEOUT, both),
            "both subscriptions read one image: keeper {}, staller {}",
            images_of(&fixture.client, keeper),
            images_of(&fixture.client, staller)
        );

        fixture.stall_the_reader();

        Some(fixture)
    }

    /// Hold the publication one window ahead of the staller and no further.
    ///
    /// A window's worth of messages, read by the keeper and not by the staller.
    /// The publisher is held one window ahead of the *slowest* reader, so a
    /// window is all the lag the image can be made to show — and three quarters
    /// of it is what makes a reader late.
    fn stall_the_reader(&mut self) {
        const MESSAGES: usize = 8;

        let mut offered = 0;
        let deadline = Instant::now() + CONNECT_TIMEOUT;

        while offered < MESSAGES && Instant::now() < deadline {
            self.client.poll();

            // Only the keeper reads. `poll_subscription` is what moves a
            // subscription's position, so leaving the other out of this loop
            // *is* the stall the test is about.
            let _ = drain_messages(&mut self.client, self.keeper, Duration::from_millis(1), 16);

            if let Some(Appended::Ok { .. }) =
                self.client.offer(self.publication, &bulk_payload(offered))
            {
                offered += 1;
            }
        }

        // Seven of eight: the eighth frame is past the window the staller is
        // still holding shut. It is the *bytes* that matter, not the count.
        assert!(
            offered >= MESSAGES - 1,
            "the publication has to take a window's worth before anything can be late: {offered} of {MESSAGES}"
        );
        assert!(
            sub_position(&self.cnc, self.keeper)
                .zip(sub_position(&self.cnc, self.staller))
                .is_some_and(|(keeper, lagging)| keeper.1 - lagging.1 > IMAGE_WINDOW / 4 * 3),
            "the keeper has to be three quarters of a window ahead: keeper {:?}, staller {:?}",
            sub_position(&self.cnc, self.keeper),
            sub_position(&self.cnc, self.staller)
        );
    }

    /// The staller is told its image is gone, and the keeper is untouched: the
    /// machine moved a reader, not the image.
    fn assert_put_aside(&mut self) {
        let (keeper, staller) = (self.keeper, self.staller);
        assert!(
            wait_for(
                &mut self.client,
                Duration::from_secs(20),
                |client| images_of(client, staller) == 0
            ),
            "the late reader is told its image is gone: staller {}",
            images_of(&self.client, staller)
        );
        assert_eq!(
            1,
            images_of(&self.client, keeper),
            "the reader that kept up is untouched"
        );
    }
}

#[test]
fn an_untethered_subscriber_that_rejoins_is_put_aside_and_woken() {
    let Some(mut fixture) = Untethered::stall(true, 6, "udp-untethered-rejoin") else {
        driver::announce_own_skip();
        return;
    };

    fixture.assert_put_aside();

    // And then woken at the join position, which is the whole of what
    // `is_rejoin` buys: the image comes back for the reader that asked.
    let staller = fixture.staller;
    assert!(
        wait_for(
            &mut fixture.client,
            Duration::from_secs(10),
            |client| images_of(client, staller) > 0
        ),
        "the rejoining reader is woken and told its image is back"
    );

    let counter = sub_position(&fixture.cnc, staller).map(|(counter_id, _)| counter_id);

    let _ = fixture.own.stop();

    assert!(
        counter.is_some(),
        "the one that was woken keeps the counter it came back with"
    );
}

#[test]
fn an_untethered_subscriber_that_does_not_rejoin_is_put_aside_and_closed() {
    let Some(mut fixture) = Untethered::stall(false, 13, "udp-untethered-close") else {
        driver::announce_own_skip();
        return;
    };

    fixture.assert_put_aside();

    // The other half: no wake-up, and "closed" means the position counter goes
    // back to the manager rather than sitting idle. The linger and resting
    // timeouts are both 200 ms, so a second is several of them.
    let staller = fixture.staller;
    let cnc = &fixture.cnc;
    assert!(
        wait_for(&mut fixture.client, Duration::from_secs(10), |_| {
            sub_position(cnc, staller).is_none()
        }),
        "a closing reader's position counter goes back to the manager"
    );

    let _ = fixture.own.stop();

    assert_eq!(
        0,
        images_of(&fixture.client, staller),
        "the reader that is not rejoining is never told anything again"
    );
}

/// `AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE`
/// (`aeron-client/src/main/c/concurrent/aeron_counters_manager.h:33`).
const CHANNEL_STATUS_ACTIVE: i64 = 1;

/// One counter's value out of a live CnC file.
fn counter_value_of(cnc: &deepmsg_cnc::CncFile, counter_id: i32) -> Option<i64> {
    cnc.counters()?.value(counter_id)
}

/// The window the untethered test gives its driver (`rcv.initial.window.length`
/// below): a reader is late once it is three quarters of this behind the
/// fastest one.
const IMAGE_WINDOW: i64 = 8 * 1024;

/// A message big enough that a window's worth of them is a hundred sends
/// rather than two thousand.
fn bulk_payload(index: usize) -> Vec<u8> {
    let mut payload = format!("untethered-{index:04}").into_bytes();
    payload.resize(1000, b'.');

    payload
}

/// The images a subscription holds, as the *client* sees them: an
/// `ON_UNAVAILABLE_IMAGE` that has been polled removes one, and an
/// `ON_AVAILABLE_IMAGE` adds one.
fn images_of(client: &Client, registration_id: i64) -> usize {
    client
        .subscription(registration_id)
        .map_or(0, |subscription| subscription.images().len())
}

/// Wait for the client to see something, polling events as it waits — an
/// `ON_UNAVAILABLE_IMAGE` does not arrive by itself.
fn wait_for<F>(client: &mut Client, within: Duration, mut predicate: F) -> bool
where
    F: FnMut(&mut Client) -> bool,
{
    let deadline = Instant::now() + within;

    loop {
        client.poll();

        if predicate(client) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }

        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A subscription's `sub-pos` counter: the id the driver allocated for it and
/// the position it holds, or `None` once the counter has been given back.
///
/// The label carries the subscription's registration id
/// (`crate::position::stream_counter_label`'s shape), which is what makes one
/// subscription's counter findable when a driver holds several.
fn sub_position(cnc: &deepmsg_cnc::CncFile, registration_id: i64) -> Option<(i32, i64)> {
    let counters = cnc.counters()?;
    let prefix = format!("sub-pos: {registration_id} ");
    let mut found = None;

    counters.for_each(|descriptor| {
        if found.is_none() && descriptor.label.starts_with(&prefix) {
            found = Some((descriptor.counter_id, descriptor.value));
        }
    });

    found
}

/// The payload of message `index`, which **says which message it is**.
///
/// A count cannot tell a message that arrived from a message that arrived
/// wearing another's bytes — which is what reading a frame out of a reused term
/// produces — so every assertion below compares bytes.
fn stream_payload(index: usize) -> Vec<u8> {
    /// A payload big enough that a thousand of them cross a dozen terms of the
    /// smallest term a channel may name.
    const PAYLOAD: usize = 1000;

    let mut payload = format!("msg-{index:06}-").into_bytes();
    payload.resize(PAYLOAD, b'a' + (index % 26) as u8);

    payload
}

/// Take whatever the subscription has right now, without waiting for more.
fn read_ready(client: &mut Client, subscription_id: i64, into: &mut Vec<Vec<u8>>) {
    use deepmsg_client::client::FRAGMENT_LIMIT;
    use deepmsg_client::fragment_assembler::Message;

    client.poll();
    client.poll_subscription(subscription_id, FRAGMENT_LIMIT, |message: Message<'_>| {
        into.push(message.payload.to_vec());
    });
}

/// A12: a stream long enough to fill terms, and to come back to one it filled
/// before, arrives whole and in order.
///
/// Every earlier UDP test here published a handful of messages into a buffer
/// they never left: a term holds about sixty of these payloads, so the last
/// datagram of a term — the one carrying the PAD that fills it out — and the
/// reuse of a term a full buffer later were both out of reach. They are the two
/// places where the receive path reads what a term *holds* rather than what just
/// arrived, and neither is visible until a stream gets there.
#[test]
fn a_stream_that_fills_terms_arrives_whole_and_in_order() {
    const MESSAGES: usize = 1000;

    let Some(mut own) = OwnDriver::start("udp-many-terms") else {
        driver::announce_own_skip();
        return;
    };

    let own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    // The smallest term length a channel may name, so a thousand of these
    // payloads cross sixteen of them and term 0 is met a second time at
    // message 186.
    let port = free_udp_port(7);
    let channel = format!("aeron:udp?endpoint=localhost:{port}|term-length=65536");

    let mut subscriber = Client::connect(own.aeron_dir()).expect("connect our client");
    let subscription_id = subscriber
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP subscription");

    let mut publisher = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication_id = publisher
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP publication");

    let mut received: Vec<Vec<u8>> = Vec::new();
    let mut offered = 0usize;
    let deadline = Instant::now() + Duration::from_secs(60);

    while Instant::now() < deadline && received.len() < MESSAGES {
        publisher.poll();
        subscriber.poll();

        while offered < MESSAGES {
            match publisher.offer(publication_id, &stream_payload(offered)) {
                Some(Appended::Ok { .. }) => offered += 1,
                // The window is closed until a status message says otherwise,
                // and the reader below is what moves it.
                Some(_) | None => break,
            }
        }

        read_ready(&mut subscriber, subscription_id, &mut received);

        std::thread::sleep(Duration::from_millis(1));
    }

    let log = own.log_tail(60);
    let _ = own.stop();

    assert_eq!(
        MESSAGES,
        received.len(),
        "every message must arrive.\ncounter dump:\n{}\nthe client sees:\n{}\nour driver said:\n{log}",
        counters_of(&own_cnc),
        client_view(&subscriber, subscription_id)
    );

    for (index, message) in received.iter().enumerate() {
        assert_eq!(
            &stream_payload(index),
            message,
            "message {index} is not the one that was sent.\ncounter dump:\n{}\nour driver said:\n{log}",
            counters_of(&own_cnc)
        );
    }

    // `bytes-sent` is a **byte** count, not a datagram count. A thousand of
    // these payloads is more than a megabyte of frames — each one 1000 bytes
    // plus a 32-byte header, aligned to 32 — and the sender adds what each send
    // path reports, which used to be datagrams and so read about a thousand.
    let bytes_sent = counter_value_of(&own_cnc, deepmsg_driver::system_counters::id::BYTES_SENT);
    assert!(
        bytes_sent.is_some_and(|bytes| bytes >= 1_000_000),
        "bytes-sent counts bytes, and a megabyte went out: {bytes_sent:?}\n{}",
        counters_of(&own_cnc)
    );
}

/// Pump both clients until the subscriber has `expected` messages, offering
/// from `index` on as the far end's window allows, and no further than
/// `offer_until`.
///
/// Returns what arrived. `index` advances as messages are accepted, so a second
/// phase continues the stream rather than repeating it — and `offer_until`,
/// rather than a global cap, is what keeps the first phase from spending the
/// messages the second one needs.
/// How many messages one pass of [`pump_until`] may publish. Small enough that
/// the stream is still moving when the next subscriber joins, large enough that
/// a test does not take all day.
const OFFER_BATCH: usize = 8;

#[allow(clippy::too_many_arguments)] // two clients, their two ids, and the bounds
fn pump_until(
    publisher: &mut Client,
    publication_id: i64,
    subscriber: &mut Client,
    subscription_id: i64,
    offer_until: usize,
    expected: usize,
    index: &mut usize,
    within: Duration,
) -> Vec<Vec<u8>> {
    let mut received = Vec::new();
    let deadline = Instant::now() + within;

    while Instant::now() < deadline && received.len() < expected {
        publisher.poll();
        subscriber.poll();

        // A few at a time, not as many as the window takes. A publisher that
        // empties its whole allowance in one pass is a stream that has already
        // finished by the time a subscriber arrives, which is not the stream a
        // late subscriber meets — and the point of the second phase is that the
        // stream is still running when it joins.
        //
        // Offering also stops by itself once the window closes: a log buffer
        // with no reader filling it answers `EndOfLog`.
        let mut batch = OFFER_BATCH;

        while batch > 0 && *index < offer_until {
            match publisher.offer(publication_id, &stream_payload(*index)) {
                Some(Appended::Ok { .. }) => {
                    *index += 1;
                    batch -= 1;
                }
                Some(_) | None => break,
            }
        }

        read_ready(subscriber, subscription_id, &mut received);

        std::thread::sleep(Duration::from_millis(5));
    }

    received
}

/// A counter the CnC file holds, found by a fragment of its label: its id, its
/// type, its owner, and the label it was found by.
fn counter_like(cnc: &deepmsg_cnc::CncFile, needle: &str) -> Option<(i32, i32, i64, String)> {
    let counters = cnc.counters()?;
    let mut found = None;

    counters.for_each(|descriptor| {
        if found.is_none() && descriptor.label.contains(needle) {
            found = Some((
                descriptor.counter_id,
                descriptor.type_id,
                descriptor.owner_id,
                descriptor.label.clone(),
            ));
        }
    });

    found
}

/// A16: an image's counters are named, typed and *owned* from the moment the
/// image exists.
///
/// `rcv-naks-sent` is how a client sees whether its own stream is being asked to
/// retransmit, and it is a counter of the image beside `rcv-hwm` and `rcv-pos`:
/// the reference allocates all three together and hands each the subscribing
/// client's id (`aeron_driver_conductor.c:6650`, `:6665`, `:6680-6683`). A
/// counter born without an owner is one a tool that groups counters by owner —
/// or reclaims them when a client goes — files under the driver rather than
/// under the client that reads it.
#[test]
fn an_images_counters_are_owned_by_its_subscriber() {
    let Some(mut own) = OwnDriver::start("udp-image-counters") else {
        driver::announce_own_skip();
        return;
    };

    let own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let port = free_udp_port(10);
    let channel = format!("aeron:udp?endpoint=localhost:{port}|term-length=65536");

    let mut subscriber = Client::connect(own.aeron_dir()).expect("connect our client");
    let subscription_id = subscriber
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP subscription");

    let mut publisher = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication_id = publisher
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP publication");

    // An image exists once frames have been read, and the counters are
    // allocated before it can read any.
    let mut index = 0usize;
    let received = pump_until(
        &mut publisher,
        publication_id,
        &mut subscriber,
        subscription_id,
        10,
        10,
        &mut index,
        Duration::from_secs(30),
    );

    let dump = counters_of(&own_cnc);
    let _ = own.stop();

    assert_eq!(10, received.len(), "the image must exist.\n{dump}");

    let mut owners = Vec::new();

    for label in ["rcv-hwm", "rcv-pos", "rcv-naks-sent"] {
        let Some((counter_id, type_id, owner_id, found)) = counter_like(&own_cnc, label) else {
            panic!("no {label} counter on the image.\n{dump}");
        };

        assert_ne!(0, owner_id, "{found} (#{counter_id}) has no owner.\n{dump}");
        owners.push((label, type_id, owner_id));
    }

    assert_eq!(
        (3, 5, 20),
        (owners[0].1, owners[1].1, owners[2].1),
        "the three type ids are the reference's: {owners:?}"
    );
    assert!(
        owners.windows(2).all(|pair| pair[0].2 == pair[1].2),
        "one image's counters are owned by one client: {owners:?}"
    );
}

/// A15: a receiver that restarts is answered, not ignored.
///
/// A publication that has met one receiver has closed its `SETUP` path for good
/// — `!has_initial_connection || is_setup_elicited` is false from then on
/// (`aeron_network_publication.c:586`). The only thing that re-opens it is a
/// receiver saying `SEND_SETUP`, which is what a driver sends when a frame
/// arrives for a session it holds no image for. A publisher that reads that
/// status message as an ordinary position report records the receiver, never
/// describes the stream, and leaves the restarted subscriber waiting for an
/// image that is never offered.
///
/// The two drivers are separate on purpose: restarting the subscriber has to
/// not restart the publisher, and the publication's memory of the first
/// receiver has to survive into the second.
#[test]
fn a_receiver_that_restarts_is_answered_with_a_setup() {
    let Some(mut publisher_driver) = OwnDriver::start("udp-restart-publisher") else {
        driver::announce_own_skip();
        return;
    };

    let publisher_cnc = publisher_driver
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let port = free_udp_port(9);
    let channel = format!("aeron:udp?endpoint=localhost:{port}|term-length=65536");

    let mut publisher = Client::connect(publisher_driver.aeron_dir()).expect("connect our client");
    let publication_id = publisher
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP publication");

    // Phase one: a subscriber driver the publication meets and answers.
    let Some(mut first_driver) = OwnDriver::start("udp-restart-subscriber-1") else {
        driver::announce_own_skip();
        return;
    };
    let _ = first_driver
        .await_cnc(READY_TIMEOUT)
        .expect("the subscriber's driver must publish a readable CnC file");

    let mut first = Client::connect(first_driver.aeron_dir()).expect("connect our client");
    let first_id = first
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP subscription");

    let mut index = 0usize;
    let received = pump_until(
        &mut publisher,
        publication_id,
        &mut first,
        first_id,
        100,
        100,
        &mut index,
        Duration::from_secs(30),
    );

    let _ = first_driver.stop();

    assert!(
        !received.is_empty(),
        "the first subscriber must be served before the restart means anything.\n\
         our driver said:\n{}",
        publisher_driver.log_tail(60)
    );

    // Phase two: a driver that has never seen this stream, subscribing to the
    // same channel. The publication is the same one, and its `SETUP` path is
    // shut until something opens it again.
    let Some(mut second_driver) = OwnDriver::start("udp-restart-subscriber-2") else {
        driver::announce_own_skip();
        return;
    };
    let second_cnc = second_driver
        .await_cnc(READY_TIMEOUT)
        .expect("the restarted driver must publish a readable CnC file");

    let mut second = Client::connect(second_driver.aeron_dir()).expect("connect our client");
    let second_id = second
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP subscription");

    let after_restart = pump_until(
        &mut publisher,
        publication_id,
        &mut second,
        second_id,
        110,
        1,
        &mut index,
        Duration::from_secs(8),
    );

    let log = publisher_driver.log_tail(60);
    let view = client_view(&second, second_id);
    let second_counters = counters_of(&second_cnc);
    let publisher_counters = counters_of(&publisher_cnc);
    let _ = second_driver.stop();
    let _ = publisher_driver.stop();

    // Two claims, and the second is the one that took the work. The image is
    // what the `SETUP` buys; the message is what the image is for. A receiver
    // that restarts has to be answered *and* has to go on reading from where
    // the stream is — an image whose position counters start at zero is owed
    // everything from the beginning, and the beginning is not there any more
    // (`aeron_publication_image.c:391-392`).
    assert!(
        !after_restart.is_empty(),
        "a subscriber that restarted asked for a `SETUP` and then read nothing.\n\
         the restarted subscriber sees:\n{view}\nrestarted driver's counters:\n\
         {second_counters}\npublisher's counters:\n{publisher_counters}\n\
         publisher's log:\n{log}"
    );
}

/// A13: a stream with nothing left to send asks for nothing.
///
/// A heartbeat carries no payload, so the position it reports is the position it
/// arrived at. A receiver that adds a frame header's worth on top of that
/// advertises a high-water mark for eight bytes no sender will ever send, and
/// the loss detector reports the hole every NAK period for as long as the stream
/// stays idle — a retransmission storm over a stream with nothing to retransmit,
/// and a high-water mark that walks into the next term on its own.
#[test]
fn an_idle_stream_asks_for_nothing() {
    const MESSAGES: usize = 8;

    let Some(mut own) = OwnDriver::start("udp-idle-stream") else {
        driver::announce_own_skip();
        return;
    };

    let own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    let port = free_udp_port(8);
    let channel = format!("aeron:udp?endpoint=localhost:{port}|term-length=65536");

    let mut subscriber = Client::connect(own.aeron_dir()).expect("connect our client");
    let subscription_id = subscriber
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP subscription");

    let mut publisher = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication_id = publisher
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP publication");

    // A few messages, so the two ends have found each other and the stream is
    // live; then nothing at all.
    let mut received: Vec<Vec<u8>> = Vec::new();
    let mut offered = 0usize;
    let deadline = Instant::now() + Duration::from_secs(30);

    while Instant::now() < deadline && received.len() < MESSAGES {
        publisher.poll();
        subscriber.poll();

        while offered < MESSAGES {
            match publisher.offer(publication_id, &stream_payload(offered)) {
                Some(Appended::Ok { .. }) => offered += 1,
                Some(_) | None => break,
            }
        }

        read_ready(&mut subscriber, subscription_id, &mut received);

        std::thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        MESSAGES,
        received.len(),
        "the messages that set the stream up must arrive before it goes idle.\n\
         counter dump:\n{}\nour driver said:\n{}",
        counters_of(&own_cnc),
        own.log_tail(60)
    );

    // Whatever the NAK count is now is the baseline. Heartbeats are what keeps
    // the two ends believing in each other while nothing is sent, and a
    // heartbeat is not a reason to ask for a retransmission.
    let nak_id = deepmsg_driver::system_counters::id::NAK_MESSAGES_SENT;
    let before = counter_value_of(&own_cnc, nak_id);
    let deadline = Instant::now() + Duration::from_secs(3);

    while Instant::now() < deadline {
        publisher.poll();
        subscriber.poll();
        read_ready(&mut subscriber, subscription_id, &mut received);

        std::thread::sleep(Duration::from_millis(5));
    }

    let after = counter_value_of(&own_cnc, nak_id);
    let dump = counters_of(&own_cnc);
    let log = own.log_tail(60);
    let _ = own.stop();

    assert_eq!(
        before, after,
        "an idle stream must send no NAKs: the counter went from {before:?} to {after:?} over \
         three seconds in which nothing was published.\ncounter dump:\n{dump}\nour driver said:\n{log}"
    );
}

/// The message number the reference's streaming sample writes into the first
/// eight bytes of every payload (`StreamingPublisher.cpp:206`), or `None` for a
/// payload too short to hold one.
///
/// A count of messages cannot say whether a subscriber that joined a running
/// stream began at the beginning, at the end, or in the middle of a term it
/// read out of a buffer that had been reused — and those are the three things
/// that look identical from a count. The number says which.
fn reference_message_number(payload: &[u8]) -> Option<i64> {
    payload
        .get(..8)
        .and_then(|head| <[u8; 8]>::try_from(head).ok())
        .map(i64::from_le_bytes)
}

/// A16: a subscriber that meets a stream already running is told where the
/// stream *is*, not where it began.
///
/// An image is born at the position its `SETUP` describes, and the reference
/// seeds its two position counters with that position the moment it builds one
/// (`aeron_publication_image.c:391-392`). A receiver whose counters start at
/// zero is owed everything from the beginning of a stream that has been reused
/// many times over, and a subscription that links to an image begins reading at
/// `rcv-pos` (`aeron_publication_image.h:376-396`) — so a late subscriber
/// reads where the frames were overwritten a full buffer ago.
///
/// The publisher is the reference's own streaming sample behind the reference's
/// own driver: our driver is the only thing under test, and the position it
/// hands its own reader has to be the one the reference's `SETUP` named. The
/// sample writes the message number into the first eight bytes of every payload
/// (`StreamingPublisher.cpp:206`), which is what lets the late subscriber say
/// *where* it joined rather than only that it read something.
///
/// One unicast endpoint holds one binder, so "a subscriber that joins late" is
/// reached the only way this channel shape allows: the driver holding the
/// endpoint goes away, and a driver that has never seen this stream takes it.
/// That is also the shape a multi-destination receiver has to meet on a manual
/// channel — arriving after the stream started — and the shape `ReplayMerge`
/// meets when it crosses from replay to live, so the case met here is the same
/// case, met earlier.
#[test]
fn a_late_subscriber_meets_a_reference_publisher_where_the_stream_is() {
    /// Enough messages to carry the stream past several of the smallest terms,
    /// so that the beginning is long gone by the time the late subscriber
    /// arrives.
    const EARLY: usize = 200;

    /// A payload about a sixteenth of a 64 KiB term, so `EARLY` of them are
    /// several terms rather than part of one.
    const LENGTH: &str = "1000";

    /// More than any run of this test will publish, so the sample is still
    /// streaming when the late subscriber joins rather than having finished.
    const ENOUGH: &str = "2000000";

    let Some(publisher_binary) = samples::locate("StreamingPublisher") else {
        driver::announce_tool_skip("StreamingPublisher");
        return;
    };

    let Some(reference_binary) = driver::locate_verified() else {
        driver::announce_skip();
        return;
    };

    let mut reference = ReferenceDriver::start(&reference_binary, "udp-late-reference-publisher")
        .expect("start the reference driver");
    let reference_dir = reference.aeron_dir().to_path_buf();
    let _reference_cnc = reference
        .await_cnc(READY_TIMEOUT)
        .expect("the reference driver must publish a readable CnC file");

    let port = free_udp_port(11);
    let channel = format!("aeron:udp?endpoint=localhost:{port}|term-length=65536");

    // The subscriber that binds the endpoint, and the one that lets the
    // publication move at all: a unicast channel has one endpoint, and this
    // driver is holding it.
    let Some(mut early_driver) = OwnDriver::start("udp-late-early") else {
        driver::announce_own_skip();
        return;
    };
    let _ = early_driver
        .await_cnc(READY_TIMEOUT)
        .expect("the early subscriber's driver must publish a readable CnC file");

    let mut early = Client::connect(early_driver.aeron_dir()).expect("connect our client");
    let early_id = early
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP subscription");

    let mut publisher = samples::Sample::start(
        &publisher_binary,
        "publisher",
        &reference_dir,
        &[
            "-c",
            &channel,
            "-s",
            &STREAM_ID.to_string(),
            "-m",
            ENOUGH,
            "-L",
            LENGTH,
        ],
    );

    let early_received = drain_messages(&mut early, early_id, Duration::from_secs(60), EARLY);
    let early_count = early_received.len();
    let _ = early_driver.stop();

    assert!(
        early_count >= EARLY,
        "the stream has to have run before a late subscriber means anything: {early_count} of \
         {EARLY} messages arrived.\nthe publisher said:\n{}",
        publisher.output()
    );

    // Phase two: a driver that has never seen this stream takes the endpoint.
    // The publication is the same one, and it has been running for several
    // terms.
    let Some(mut late_driver) = OwnDriver::start("udp-late-joiner") else {
        driver::announce_own_skip();
        return;
    };
    let late_cnc = late_driver
        .await_cnc(READY_TIMEOUT)
        .expect("the late subscriber's driver must publish a readable CnC file");

    let mut late = Client::connect(late_driver.aeron_dir()).expect("connect our client");
    let late_id = late
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP subscription");

    let mut received: Vec<Vec<u8>> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);

    while Instant::now() < deadline && received.len() < 10 {
        late.poll();
        read_ready(&mut late, late_id, &mut received);

        std::thread::sleep(Duration::from_millis(5));
    }

    let view = client_view(&late, late_id);
    let late_counters = counters_of(&late_cnc);
    let publisher_output = publisher.output();

    // Read while the driver is still running, which is the only time the image's
    // counters have anything to say.
    let image_counter = |label: &str| {
        counter_like(&late_cnc, label)
            .and_then(|(counter_id, _, _, _)| counter_value_of(&late_cnc, counter_id))
    };

    let rcv_pos = image_counter("rcv-pos");
    let rcv_hwm = image_counter("rcv-hwm");
    let sub_pos = sub_position(&late_cnc, late_id).map(|(_, value)| value);

    let _ = publisher.terminate(Duration::from_secs(5));
    let _ = late_driver.stop();
    let _ = reference.stop();

    let evidence = format!(
        "the late subscriber sees:\n{view}\n\
         its image's counters: rcv-pos {rcv_pos:?}, rcv-hwm {rcv_hwm:?}, \
         its subscription's sub-pos {sub_pos:?}\n\
         the late driver's counters:\n{late_counters}\n\
         the publisher said:\n{publisher_output}"
    );

    assert!(
        !received.is_empty(),
        "a subscriber that joined a stream already running read nothing.\n{evidence}"
    );

    // The claim the whole test is for. A subscriber that began at message zero
    // began at a position the stream left behind several terms ago, which is
    // what an image with counters seeded at zero makes it do.
    let first = reference_message_number(&received[0]);
    assert!(
        matches!(first, Some(number) if number > 0),
        "a subscriber that met a running stream must begin where the stream is, not at message \
         zero; the first payload it assembled carries {first:?}.\n{evidence}"
    );

    assert!(
        matches!(rcv_pos, Some(position) if position > 0)
            && matches!(sub_pos, Some(position) if position > 0),
        "an image built from a `SETUP` naming a position must take that position, not zero \
         (`aeron_publication_image.c:391-392`).\n{evidence}"
    );
}

/// The UDP channel's incoming interceptors, end to end — the setting
/// `CTestMediaDriver` injects loss with, which `docs/compat.md` said this build
/// accepted and ignored.
///
/// The arrangement is the harness's own: `enableFixedLoss` names `fixed-loss`
/// and hands it a range of one term (`CTestMediaDriver.java:409-426`), and the
/// driver drops exactly that range, **once per stream and session**. The two
/// assertions are the pair that says the drop was real and not a skipped test:
/// the stream still arrives whole — which it can only do by retransmission —
/// and the image sent at least one NAK, which it can only do after seeing a
/// hole.
///
/// `AERON_NAK_UNICAST_DELAY` is set the way the harness sets it
/// (`CTestMediaDriver.java:449-452`), so the NAK is not coalesced into a delay
/// this test would have to wait out.
#[test]
fn a_channel_that_names_an_interceptor_loses_exactly_what_it_names() {
    let Some(mut own) = OwnDriver::start_with_env(
        "udp-fixed-loss",
        &[],
        &[
            ("AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS", "fixed-loss"),
            // The first two kibibytes of term 0, which is where a stream
            // starts: the frame at offset 0 is the one the rule reaches.
            (
                "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_FIXED_LOSS_ARGS",
                "term-id=0|term-offset=0|length=2048",
            ),
            ("AERON_NAK_UNICAST_DELAY", "0"),
        ],
    ) else {
        driver::announce_own_skip();
        return;
    };

    let _cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    // Offset fourteen: the tests in this file run in parallel and each offset
    // is a port.
    let port = free_udp_port(14);
    // The term the interceptor names has to be the term the stream starts in,
    // and a publication that names no position picks a **random** initial term
    // id — so the position triple is what pins `term-id=0` to a real term. The
    // reference's own loss tests do the same (`DataLossAndRecoverySystemTest`
    // passes `init-term-id=0|term-id=0|term-offset=0`).
    let channel = format!(
        "aeron:udp?endpoint=127.0.0.1:{port}|term-length=64k|init-term-id=0|term-id=0|term-offset=0"
    );

    let mut client = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication = client
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the publication");
    let subscription = client
        .add_subscription(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the subscription");

    // A term's worth, so the hole at the start is well behind the reader by the
    // time the last message arrives.
    const MESSAGES: usize = 64;
    let payload = [0x5Au8; 1024];
    let deadline = Instant::now() + Duration::from_secs(30);
    let (mut offered, mut received) = (0usize, 0usize);

    while received < MESSAGES && Instant::now() < deadline {
        if offered < MESSAGES
            && let Some(Appended::Ok { .. }) = client.offer(publication, &payload)
        {
            offered += 1;
        }

        client.poll();
        client.poll_subscription(subscription, 10, |_| received += 1);
        std::thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        MESSAGES, received,
        "the stream arrives whole, which the drop cannot prevent: a lost frame is what a NAK \
         is for (offered {offered})"
    );

    let reader = client.counters_reader().expect("the counter regions");
    let naks = reader
        .find_by_type_id(RECEIVER_NAKS_SENT)
        .and_then(|counter| reader.value(counter.counter_id))
        .expect("the image's `receiver-naks-sent` counter");

    assert!(
        naks > 0,
        "the hole the interceptor made was seen and asked for: receiver-naks-sent={naks}. \
         A zero here with every message delivered means nothing was dropped — the \
         setting went unread, which is exactly what this test is for"
    );
}
