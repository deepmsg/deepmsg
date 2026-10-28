//! A datagram the receiver cannot read as a frame is **counted**, not dropped
//! in silence.
//!
//! Two shapes arrive at the same place and both must land on the same counter:
//!
//! * **Too short to hold a frame header.** The reference's validity check begins
//!   with the header's length — `packet_length >= AERON_FRAME_HEADER_LENGTH`
//!   (`aeron-client/src/main/c/protocol/aeron_udp_protocol.h:229-231`) — so a
//!   short datagram is invalid by that first clause, and its dispatch counts it
//!   (`media/aeron_receive_channel_endpoint.c:524-535`). In this build the
//!   header *read* failed before the check was reached and the datagram went
//!   nowhere: no counter, no log, nothing to see from outside.
//! * **Long enough and invalid anyway.** This one was counted all along; it is
//!   here as the control for the first, because both write the same counter and
//!   a test that only ever sees one of them cannot tell the two apart from a
//!   counter that is stuck.
//!
//! The counter is the reference's own `Invalid packets`
//! (`AERON_SYSTEM_COUNTER_ID_INVALID_PACKETS`, id 14,
//! `crates/driver/src/system_counters.rs:172`), read out of the CnC the way a
//! client reads it. The datagrams are sent from a plain socket at a
//! subscription's endpoint, which is what binds the port they arrive on.

use std::net::UdpSocket;
use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_tests::driver::OwnDriver;

/// The stream the subscription listens on.
const STREAM_ID: i32 = 1001;

/// How long a counter may take to move before the test is a failure.
const DEADLINE: Duration = Duration::from_secs(10);

/// The label the driver gives that counter (`system_counters.rs:172`).
const INVALID_PACKETS: &str = "Invalid packets";

/// What the counter says, or `None` before the driver has published it.
fn invalid_packets(client: &Client) -> Option<i64> {
    let reader = client.counters_reader()?;
    let mut found = None;

    let _ = reader.for_each(|descriptor| {
        if descriptor.label.starts_with(INVALID_PACKETS) {
            found = Some(descriptor.value);
        }
    });

    found
}

/// Wait until the counter reaches `at_least`, and answer what it says.
fn await_invalid_packets(client: &mut Client, at_least: i64) -> i64 {
    let deadline = Instant::now() + DEADLINE;

    loop {
        let value = invalid_packets(client).unwrap_or(0);
        if value >= at_least {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "the invalid-packet counter stayed at {value}, and {at_least} was expected"
        );

        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A UDP port this process owns, above `ip_local_port_range`
/// ([`deepmsg_tests::ports`]).
///
/// Linux hands outbound sockets ports from `ip_local_port_range` — 32768 to
/// 60999 here, the same numbers this build's own `EPHEMERAL_PORTS` carries — so
/// a test that binds inside it is racing every socket the machine opens. This
/// file was the first to start past its top; the window it started in is now
/// `tests/src/ports.rs`, shared with every other single-driver test.
fn free_port() -> u16 {
    deepmsg_tests::ports::port(0)
}

#[test]
fn a_datagram_that_is_not_a_frame_is_counted() {
    let Some(own) = OwnDriver::start("short-datagram") else {
        deepmsg_tests::driver::announce_own_skip();
        return;
    };

    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");
    let port = free_port();
    let channel = format!("aeron:udp?endpoint=127.0.0.1:{port}");
    client
        .add_subscription(&channel, STREAM_ID, DEADLINE)
        .expect("the subscription is added, and its port is bound with it");

    let socket = UdpSocket::bind("127.0.0.1:0").expect("a socket");
    let target = format!("127.0.0.1:{port}");

    // **Four bytes: shorter than a frame header**, which is eight
    // (`crates/driver/src/protocol.rs:98`).
    let before = invalid_packets(&client).unwrap_or(0);
    socket.send_to(&[0_u8; 4], &target).expect("a send");
    let counted = await_invalid_packets(&mut client, before + 1);
    assert_eq!(
        before + 1,
        counted,
        "a datagram too short to be a frame is an invalid packet, and one of them \
         arrived"
    );

    // **Long enough, and invalid all the same.** This is the shape the counter
    // was already written for, so the two halves of this test are one counter
    // moving twice — by exactly one, each time.
    //
    // The version byte is what makes it invalid, and it has to be said why a
    // zeroed buffer will not do: version zero *is* the version this build reads
    // (`protocol.rs:103`) and type zero is `PAD` (`protocol.rs:125`), so 32
    // zeroed bytes are a **valid** padding frame — 32 is the aligned length a
    // data frame has — and the first cut of this test watched a counter that
    // was never going to move.
    let mut not_a_frame = [0_u8; 32];
    not_a_frame[4] = 9; // the header's version, which this build reads as zero
    socket.send_to(&not_a_frame, &target).expect("a send");
    let counted_twice = await_invalid_packets(&mut client, counted + 1);
    assert_eq!(
        counted + 1,
        counted_twice,
        "and a long one whose header is invalid is counted on the same counter"
    );
}
