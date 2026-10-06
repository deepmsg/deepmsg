//! A4: the reference's own counter viewer reads this driver's network counters.
//!
//! `AeronStat` opens a CnC file and prints every allocated counter. It is the
//! outside oracle for the data plane's accounting: the numbers a *client*
//! cannot check (how many bytes left the socket, how many status messages
//! arrived, whether a channel's endpoint ever came up) are exactly the ones an
//! operator reads there, and a driver that moved them differently would look
//! healthy to its own tests and wrong to every tool.
//!
//! The session is arranged without a second driver: our driver publishes on a
//! UDP channel, a plain socket plays the subscriber, and one hand-built status
//! message is what makes the publication connect and send. That is enough for
//! every counter this asserts to move, and it fails loudly if any of them does
//! not.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_driver::protocol::{SetupFrame, StatusMessageFrame};
use deepmsg_driver::sys::AddressFamily;
use deepmsg_driver::sys::socket::{DatagramSocket, Datagrams};
use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT};
use deepmsg_tests::samples;

/// The stream every test here uses.
const STREAM_ID: i32 = 1001;

#[test]
fn aeron_stat_reads_the_network_counters_a_udp_session_moves() {
    let Some(stat) = samples::locate("AeronStat") else {
        driver::announce_tool_skip("AeronStat");
        return;
    };

    let Some(mut own) = OwnDriver::start("udp-counters") else {
        driver::announce_own_skip();
        return;
    };

    let own_cnc = own
        .await_cnc(READY_TIMEOUT)
        .expect("this driver must publish a readable CnC file");

    // The subscriber: a socket the driver will send to, and which answers once.
    let mut subscriber = DatagramSocket::open(AddressFamily::Inet).expect("a socket");
    subscriber
        .bind("127.0.0.1:0".parse().expect("an address"))
        .expect("a bind");
    subscriber.set_nonblocking().expect("non-blocking");
    let bound = subscriber.local_address().expect("a bound address");

    let channel = format!("aeron:udp?endpoint={bound}");

    let mut publisher = Client::connect(own.aeron_dir()).expect("connect our client");
    let publication_id = publisher
        .add_publication(&channel, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("our driver must confirm the UDP publication");

    // Wait for the driver's first `SETUP`, which is what tells this socket the
    // publisher's address — the implicit-unicast control address, from the
    // other side of the wire.
    let mut buffers = vec![vec![0u8; 2048]];
    let mut datagrams = Datagrams::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut publisher_address = None;
    let mut setup = None;

    while Instant::now() < deadline && publisher_address.is_none() {
        publisher.poll();

        let received = subscriber
            .receive_batch(&mut buffers, &mut datagrams)
            .unwrap_or(0);

        for (slot, datagram) in datagrams.as_slice()[..received].iter().enumerate() {
            if datagram.length > 0 {
                publisher_address = Some(datagram.source.expect("a source"));
                setup = SetupFrame::read(&buffers[slot][..datagram.length]);
            }
        }

        std::thread::sleep(Duration::from_millis(5));
    }

    let publisher_address = publisher_address.expect("the publication says SETUP before it sends");
    let setup = setup.expect("the first thing a publication says is a SETUP");

    // One status message: a receiver with room for 64 KiB, which is what opens
    // the publication's window.
    //
    // It reports the position the `SETUP` described — where the publisher is —
    // because that is what a receiver reports, and because the publisher is
    // required to refuse anything else: a position outside half a term behind
    // `snd-pos` to a term and a half in front of it is not one the publication
    // can act on (`aeron_network_publication.c:841-856`). Zeros are not a
    // position either — against a random `initial_term_id` they resolve to
    // somewhere astronomically far from the stream, and this fixture used to
    // send exactly that, which only worked while the driver was laxer than the
    // reference.
    let session_id = session_of(&own_cnc, publication_id);
    let sm = StatusMessageFrame {
        session_id,
        stream_id: STREAM_ID,
        consumption_term_id: setup.active_term_id,
        consumption_term_offset: setup.term_offset,
        receiver_window: 64 * 1024,
        receiver_id: 1,
    };
    let mut frame = [0u8; StatusMessageFrame::LENGTH];
    assert!(sm.write_with_flags(&mut frame, 0).is_some());

    let _ = subscriber.send_batch(Some(publisher_address), &[&frame]);

    // And then the publication sends: a message the driver puts on the wire.
    let payload = b"counted on the wire";
    let deadline = Instant::now() + Duration::from_secs(10);

    while Instant::now() < deadline {
        publisher.poll();
        let _ = publisher.offer(publication_id, payload);

        let received = subscriber
            .receive_batch(&mut buffers, &mut datagrams)
            .unwrap_or(0);

        if received > 0 {
            break;
        }

        std::thread::sleep(Duration::from_millis(5));
    }

    // Then let the stream go quiet: a publication that has connected and has
    // nothing to send says so with a heartbeat after
    // `aeron.network.publication.heartbeat.timeout` (`HEARTBEAT_TIMEOUT_NS`,
    // 100 ms), which is the other counter this asserts.
    std::thread::sleep(Duration::from_millis(250));
    publisher.poll();
    let _ = subscriber.receive_batch(&mut buffers, &mut datagrams);

    // The oracle: the reference's own tool, on the same directory.
    let output = std::process::Command::new(&stat)
        .arg("-d")
        .arg(own.aeron_dir())
        // `-w false` is what makes it print the counters once and exit; the
        // default is to watch them for ever.
        .arg("-w")
        .arg("false")
        .output()
        .expect("AeronStat runs");
    let text = String::from_utf8_lossy(&output.stdout).to_string();

    let _ = own.stop();

    assert!(
        text.contains("Bytes sent"),
        "AeronStat lists the traffic counters:\n{text}"
    );

    // Every counter this session moved, with what it should have counted: the
    // assertion is "more than zero" rather than an exact number, because the
    // number of heartbeats depends on how long the session took.
    for counter in ["Bytes sent", "Heartbeats sent", "Status Messages received"] {
        let value = counter_value(&text, counter);
        assert!(
            value.is_some_and(|value| value > 0),
            "{counter} should have moved: {value:?}\n{text}"
        );
    }

    // And the channel status counters exist with their own names, one per
    // endpoint, which is what a client reads to learn a socket is up.
    assert!(
        text.contains("snd-channel"),
        "the send endpoint's counter is there:\n{text}"
    );

    // The publication's own counters carry the stream-position **key** the
    // reference's allocators build, which is what lets a tool attribute a
    // counter to a stream: the test above read the session id out of it to
    // address the status message, and a driver that allocated them with an
    // empty key would have answered every counter with zeroes.
    assert!(
        text.contains("pub-pos (concurrent)"),
        "the producer's position is named as the reference names it:\n{text}"
    );
    assert!(
        text.contains("snd-naks-received"),
        "and the sender's NAK counter is there:\n{text}"
    );
}

/// The session id a publication runs under, read from the CnC counters it owns
/// — the `pub-pos` key carries it, and that is the only place a client can
/// learn it before an image exists.
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

/// One counter's value out of `AeronStat`'s output: the line is
/// `<id>: <value> - <label>`.
fn counter_value(text: &str, label: &str) -> Option<i64> {
    text.lines()
        .find(|line| line.contains(label))
        .and_then(|line| line.split(':').nth(1))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|value| value.parse().ok())
}
