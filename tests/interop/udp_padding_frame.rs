//! A padding frame that arrives on its own, over UDP.
//!
//! A term is filled by frames, and the last message rarely fits: the sender
//! covers the remainder with a **padding** frame, a data header whose length is
//! the whole remainder, sent as **its header alone** (`aeron_udp_protocol.h:237`
//! never compares a data frame's length against the datagram).
//!
//! A padding rides inside the datagram that carries the frame before it —
//! *unless* the datagram budget or the sender's window ends exactly at the term
//! boundary, and then it goes **alone**, as a 32-byte datagram whose first and
//! only frame is a PAD. Retransmission always sends it alone, because a NAK
//! that names the term's tail has nothing but the padding left to send.
//!
//! The receiver's dispatch reads the first frame of a datagram with a reader
//! that **refuses PAD by design** (`DataFrame::read`, whose doc says so). Every
//! lone padding was therefore dropped silently: no counter, no log line. The
//! term's tail stayed zeroed, the image's gap scanner read it as a hole, and
//! the receiver asked for those bytes forever — NAKs a few hundred times a
//! second, each answered by a retransmission of the very frame being thrown
//! away. Both ends stopped at the term boundary: the publisher's position froze
//! with the subscriber's, and the round trip never completed again.
//!
//! # Why the reference's own instrument
//!
//! `Ping` and `Pong` are the smallest publisher/subscriber pair in the reference
//! build, and they are what makes this reproducible: a 64 KiB term with
//! kilobyte messages reaches the first boundary after 62 messages, and
//! `Ping`'s own loop is a round trip, so a stall is a number that never comes
//! back rather than a counter a test has to interpret. The acceptance is the
//! reference's own percentile table — printed only when all its messages made
//! it — which no code of ours produced.
//!
//! `tests/integration/udp_term_boundary.rs` is the other half: the *batched*
//! padding, with our own client at both ends.

use std::time::Duration;

use deepmsg_tests::driver::{self, OwnDriver};
use deepmsg_tests::samples::{self, Sample};

/// Messages to ping, and how many to warm up with.
///
/// A 64 KiB term holds 62 of them, so three boundaries are crossed — and the
/// stall this test exists for is reached by the first one.
const MESSAGES: usize = 200;
const WARM_UP: usize = 20;
const LENGTH: usize = 1024;

/// How long the reference's Ping may take before the test is a failure.
///
/// It finishes in well under a second when the stream is healthy; this is a
/// bound on a hang, not a performance budget.
const DEADLINE: Duration = Duration::from_secs(60);

#[test]
fn a_padding_frame_that_arrives_alone_is_written_to_the_term() {
    let Some(mut own) = OwnDriver::start("udp-padding-frame") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");

    let Some(pong_binary) = samples::locate("Pong") else {
        driver::announce_tool_skip("Pong");
        return;
    };
    let Some(ping_binary) = samples::locate("Ping") else {
        driver::announce_tool_skip("Ping");
        return;
    };

    let dir = own.aeron_dir();
    let port = free_port();
    let ping_channel = format!("aeron:udp?endpoint=localhost:{port}|term-length=64k");
    let pong_channel = format!("aeron:udp?endpoint=localhost:{}|term-length=64k", port + 1);

    let channels = [
        "-c",
        ping_channel.as_str(),
        "-C",
        pong_channel.as_str(),
        "-s",
        "1002",
        "-S",
        "1003",
    ];

    let mut pong = Sample::start_silent(&pong_binary, "bench-pong", dir, &channels);
    let mut ping = Sample::start_silent(
        &ping_binary,
        "bench-ping",
        dir,
        &[
            &channels[..],
            &[
                "-L",
                &LENGTH.to_string(),
                "-m",
                &MESSAGES.to_string(),
                "-w",
                &WARM_UP.to_string(),
            ],
        ]
        .concat(),
    );

    let finished = ping.await_exit(DEADLINE);
    let output = ping.output();
    pong.terminate(Duration::from_secs(5));

    assert!(
        finished.is_some(),
        "the reference's Ping never finished: a receiver that drops a lone padding frame stops at \
         the term boundary and never starts again.\n{output}"
    );
    assert_eq!(
        Some(MESSAGES),
        counted(&output),
        "the round trips that came back were not all of them:\n{output}"
    );

    let _ = own.stop();
}

/// How many round trips the reference's own summary counted
/// (`hdr_percentiles_print`'s `Total count` line, `cping.c:388`).
fn counted(output: &str) -> Option<usize> {
    let after = output.split("Total count").nth(1)?;
    let value = after.split('=').nth(1)?;
    let digits: String = value
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();

    digits.parse().ok()
}

/// A UDP port nothing else on this machine is likely to hold.
fn free_port() -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 20_000 + (std::process::id() as u16 % 20_000);

    base
}
