//! A message whose datagrams are larger than the driver's default MTU.
//!
//! The receiver's buffers are `AERON_DRIVER_RECEIVER_MAX_UDP_PACKET_LENGTH`,
//! **not** `aeron.mtu.length` (`aeron-driver/src/main/c/aeron_driver_context.h:60`;
//! both the allocation and the `iov_len` are that constant at
//! `aeron_driver_receiver.c:48-62`). The distinction only shows when a
//! *publication* names a larger `mtu=` than the driver's default — which is
//! what P3-6b's second arm does (`ClusterTest.java:1831-1839` restarts a
//! cluster on `mtu=8992`).
//!
//! Sizing the buffers from the default truncates every datagram past 1408
//! bytes. Nothing reports it: the receive is short, the frame does not parse,
//! and the message is simply not there. This test is the one that would have
//! said so.

use std::time::{Duration, Instant};

use deepmsg_client::client::Client;
use deepmsg_tests::driver::OwnDriver;

const STREAM_ID: i32 = 1001;
const DEADLINE: Duration = Duration::from_secs(10);

/// One message of `payload_len` bytes over a channel with this shape, from a
/// publication to a subscription in the same driver.
fn round_trip(term_length: &str, mtu: &str, payload_len: usize, port_offset: u16) {
    let Some(driver) = OwnDriver::start("udp-receive-mtu") else {
        deepmsg_tests::driver::announce_own_skip();
        return;
    };
    let mut client = Client::connect(driver.aeron_dir()).expect("a client");
    let port = 41_000 + (std::process::id() as u16 % 400) * 10 + port_offset;
    let channel =
        format!("aeron:udp?endpoint=localhost:{port}|term-length={term_length}|mtu={mtu}");
    let subscription = client
        .add_subscription(&channel, STREAM_ID, DEADLINE)
        .expect("a subscription");
    let publication = client
        .add_publication(&channel, STREAM_ID, DEADLINE)
        .expect("a publication");

    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        client.poll();
        if client
            .publication(publication)
            .is_some_and(|publication| publication.is_connected().unwrap_or(false))
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    let payload = vec![0xA5_u8; payload_len];
    let deadline = Instant::now() + DEADLINE;
    let mut received = 0;
    while Instant::now() < deadline {
        client.poll();
        let _ = client.offer(publication, &payload);
        client.poll_subscription(subscription, 10, |message| {
            received += message.payload.len();
        });
        if received >= payload_len {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!(
        "a {payload_len}-byte message did not cross mtu={mtu} term-length={term_length}: \
         {received} bytes came back"
    );
}

/// A message whose frame spans several datagrams at an MTU larger than the
/// driver's default.
///
/// `262144` is one eighth of the 2 MiB term — the largest message the
/// reference's second arm sends after the restart (`ClusterTest.java:1906`).
#[test]
fn a_message_larger_than_the_drivers_default_mtu_crosses_udp() {
    round_trip("2m", "8992", 4_000, 0);
    round_trip("2m", "8992", 262_144, 1);
    round_trip("256k", "8992", 32_768, 2);
}
