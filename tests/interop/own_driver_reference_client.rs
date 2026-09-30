//! The reference client against **our** driver.
//!
//! Every other interop test in this tree runs our client against the reference
//! driver. This one turns the pair around, and it is the only arrangement in
//! which the driver-side byte contracts can be *falsified* rather than
//! restated: `BasicPublisher` and `BasicSubscriber` share no code with this
//! build, so a log buffer whose metadata is subtly wrong, an image whose path
//! is not where the reference puts it, a session id that never reaches the
//! frame header, or a publisher limit that never opens all show up here — as a
//! wrong payload, a "not connected" message, or a hang.
//!
//! What the pair exercises, in the reference's own terms:
//!
//! 1. `ADD_PUBLICATION` and `ON_PUBLICATION_READY` — the sample maps the log
//!    buffer this driver created, at the path this driver named.
//! 2. `ADD_SUBSCRIPTION` and `ON_AVAILABLE_IMAGE` — the sample maps the same
//!    file as a reader and joins at the position the driver chose.
//! 3. The **publisher limit**, which is the assertion no single-sided test can
//!    reach: `BasicPublisher` sleeps a second between messages and does not
//!    retry one, so a window that never opens loses messages rather than
//!    failing to start.
//! 4. The subscriber's position counter coming back — the driver recomputes
//!    the limit from it every duty cycle, so a driver that never reads it
//!    stops the publisher after one message's worth of window.
//!
//! # What the reference samples cannot cover
//!
//! Fragmentation and an MTU-sized payload are **not** reachable through these
//! binaries: `BasicPublisher`'s `-f` is not in the installed build (the samples
//! are built from whatever revision last built them, not from the checkout's
//! source), and `Throughput` writes through `tryClaim`, which refuses anything
//! over `maxPayloadLength` — it says so out loud. Both are covered from our own
//! side instead: a client offering more than one frame is our client's own
//! test, and the driver sees the same frames either way.

use std::process::Command;
use std::time::Duration;

use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT};
use deepmsg_tests::samples::{self, Sample};

/// The stream both samples use.
const STREAM_ID: i32 = 3001;

/// How long a sample gets to finish. The publisher sleeps a second per message,
/// and this has to cover its messages, the driver's start-up and the map of a
/// log buffer.
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Start our driver, or skip the test.
///
/// A one-megabyte term buffer rather than the default sixty-four: the samples'
/// messages are tiny, and a log buffer is *three* terms — a test that used the
/// default would write 192 MiB per publication to prove something that 3 MiB
/// proves as well. The cost is that the publisher's window is half a megabyte,
/// which is still two orders of magnitude more than these tests send.
fn start(case: &str) -> Option<OwnDriver> {
    start_with_term_length(case, "aeron.ipc.term.buffer.length")
}

/// The same, for a case whose traffic is UDP.
///
/// The term length is a **different property** per transport
/// (`aeron.ipc.term.buffer.length` against `aeron.term.buffer.length`,
/// `crates/driver/src/config.rs:281` and `:340`), and a network publication
/// reads the second one. Passing the IPC property here would leave the
/// publication at the sixteen-megabyte default and write forty-eight megabytes
/// per run to prove what three proves.
fn start_udp(case: &str) -> Option<OwnDriver> {
    start_with_term_length(case, "aeron.term.buffer.length")
}

fn start_with_term_length(case: &str, property: &str) -> Option<OwnDriver> {
    // The case is part of the directory's name because a test that panics
    // leaves its driver running until the harness drops it: two cases in one
    // process must not want the same directory.
    let name = format!("own-driver-ref-client-{case}");
    let property = format!("-D{property}=1m");
    let driver = OwnDriver::start_with(&name, &[property.as_str()]);

    if driver.is_none() {
        driver::announce_own_skip();
    }

    driver
}

/// The value of a `key=value` field on the first line containing `marker`.
///
/// The samples print one line per event with `key=value` pairs on it, and the
/// values are exactly what a driver decided — so a test reads them instead of
/// asserting against strings copied from another transport's output.
fn token(output: &str, marker: &str, key: &str) -> Option<String> {
    let line = output.lines().find(|line| line.contains(marker))?;
    let rest = line.split(key).nth(1)?;

    Some(rest.split_whitespace().next()?.to_owned())
}

/// A UDP port nothing else on this machine is likely to hold.
///
/// The same arithmetic `tests/interop/udp_transport.rs` uses: high ports, from
/// a range derived from the process id. Not "bind a socket and read its port" —
/// that is a race, and this file's two cases run in one process.
fn free_udp_port() -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 20_000 + (std::process::id() as u16 % 20_000);

    base.saturating_add(1)
}

#[test]
fn the_reference_client_publishes_to_our_driver_and_reads_it_back() {
    let Some(publisher_binary) = samples::locate("BasicPublisher") else {
        driver::announce_tool_skip("BasicPublisher");
        return;
    };
    let Some(subscriber_binary) = samples::locate("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };
    let Some(mut driver) = start("pubsub") else {
        return;
    };

    driver
        .await_cnc(READY_TIMEOUT)
        .expect("our driver publishes a readable CnC file");
    let dir = driver.aeron_dir().to_owned();

    // The subscriber first. A publication with no reader cannot be offered to
    // — the driver holds its limit at the producer's position — and the sample
    // does not retry a message it could not send.
    let mut subscriber = Sample::start(
        &subscriber_binary,
        "subscriber",
        &dir,
        &["-c", "aeron:ipc", "-s", &STREAM_ID.to_string()],
    );
    subscriber.await_output(Duration::from_secs(20), "its subscription", |output| {
        output.contains("Subscription channel status")
    });

    let mut publisher = Sample::start(
        &publisher_binary,
        "publisher",
        &dir,
        &["-c", "aeron:ipc", "-s", &STREAM_ID.to_string(), "-m", "4"],
    );

    // While it is publishing, the counters this driver is supposed to publish
    // are readable by the reference's own tool — one row per position, with
    // the labels the reference gives them. This is the only place they are
    // checked against a reader that is not ours.
    publisher.await_output(Duration::from_secs(30), "its first offer", |output| {
        output.contains("yay!")
    });

    if let Some(aeron_stat) = driver::locate_aeron_stat() {
        let output = Command::new(aeron_stat)
            .arg("-d")
            .arg(&dir)
            .arg("-w")
            .arg("false")
            .output()
            .expect("run AeronStat");

        let text = String::from_utf8_lossy(&output.stdout);
        for row in ["pub-pos (concurrent)", "pub-lmt", "sub-pos"] {
            assert!(
                text.contains(row),
                "AeronStat does not see `{row}` while a publication and a subscription are live:\n{text}"
            );
        }
    } else {
        driver::announce_tool_skip("AeronStat");
    }

    // The publisher finishes on its own; four messages is a window that has to
    // open four times, since the sample does not retry.
    let status = publisher
        .await_exit(SAMPLE_TIMEOUT)
        .expect("the publisher finishes");
    let output = publisher.output();
    assert!(status.success(), "the publisher failed:\n{output}");
    assert_eq!(
        4,
        output.matches("yay!").count(),
        "every message has to be offered into an open window:\n{output}"
    );
    assert!(
        !output.contains("not connected") && !output.contains("back pressure"),
        "the driver never opened the window:\n{output}"
    );

    // And the reader has them, in the frames this driver's publication carried.
    let received = subscriber.await_output(Duration::from_secs(20), "every message", |output| {
        output.matches("Message to stream").count() >= 4
    });

    for index in 0..4 {
        assert!(
            received.contains(&format!("<<Hello World! {index}>>")),
            "message {index} is missing from:\n{received}"
        );
    }
    assert!(
        received.contains(&format!("Message to stream {STREAM_ID} from session ")),
        "the frame header carries the stream:\n{received}"
    );

    // The session id the subscriber reads out of a *frame* is the one the
    // driver put in the log's metadata template and in `ON_AVAILABLE_IMAGE` —
    // three places that have to agree for this line to be well formed.
    // The session id the subscriber reads out of a *frame* is the one the
    // driver wrote into the log's metadata template and sent in
    // `ON_AVAILABLE_IMAGE` — three places that have to agree for this line to
    // be well formed at all. The digits are taken rather than the whole field:
    // the installed sample's format string is not the one in the 1.53.2 source
    // (`from session 123(14@0)` against `from session 123 (14 bytes)`), which
    // is itself worth knowing — the reference's sample binaries are built from
    // whatever revision last built them, and `docs/reference.md` says so.
    let rest = received
        .split("from session ")
        .nth(1)
        .expect("a session id in the subscriber's output");
    let digits: String = rest
        .char_indices()
        .take_while(|(index, character)| {
            character.is_ascii_digit() || (*index == 0 && *character == '-')
        })
        .map(|(_, character)| character)
        .collect();
    let session_id: i32 = digits.parse().expect("a session id");

    // A speculated session id starts from a random `i32`, so it is outside the
    // range the driver keeps for itself on **both** sides of zero.
    assert!(
        !(-1..=1000).contains(&session_id),
        "the driver speculates session ids outside its reserved range: {session_id}"
    );

    // When the publisher's client goes, the driver drains the publication and
    // tells every reader its image is gone — with the position the stream ended
    // at, which is where the producer got to: four frames of sixty-four bytes.
    // This is our side of the life cycle (`REMOVE`/client death) read by the
    // reference's own client, and the position is the end-of-stream byte this
    // driver wrote into the log reaching a reader that is not ours.
    let ended =
        subscriber.await_output(Duration::from_secs(30), "its image to go away", |output| {
            output.contains("Unavailable image on correlationId=")
        });
    assert!(
        ended.contains("Unavailable image on correlationId=4"),
        "the unavailable image names the publication:\n{ended}"
    );
    assert!(
        ended.contains(&format!("sessionId={session_id}")),
        "and the session this reader was reading:\n{ended}"
    );
    assert!(
        ended.contains("from aeron:ipc"),
        "the source identity is the constant the driver sends:\n{ended}"
    );

    // The position in that line is the *reader's* own, and the reference
    // reports it whenever its handler runs — so it is not asserted to be the
    // end of the stream. What is asserted is that the client was never told the
    // driver had died: a force-close produces the same handler call, and this
    // is what tells the two apart.
    for complaint in ["keepalive", "driver timeout", "shutdown"] {
        assert!(
            !ended.to_lowercase().contains(complaint),
            "the client must not have timed the driver out (`{complaint}`):\n{ended}"
        );
    }

    // A sample stopped the way the reference stops one: a signal, a clean exit.
    let status = subscriber
        .terminate(Duration::from_secs(10))
        .expect("the subscriber stops");
    assert!(
        status.success(),
        "the subscriber did not shut down cleanly:\n{}",
        subscriber.output()
    );

    assert!(
        driver.stop().is_ok(),
        "the driver stops on a signal:\n{}",
        driver.log_tail(20)
    );
}

/// The same arrangement as the test above, over **UDP**.
///
/// What the IPC case cannot reach is everything the wire adds: the publication
/// is a *network* one, so the log buffer this driver creates carries a network
/// term length and an MTU; the image is derived from a **SETUP** datagram
/// rather than from a shared buffer, so its join position, its session id and
/// its control address come from the wire; the window is driven by **status
/// messages** that travelled; and an unavailable image names the **channel**
/// rather than the constant `aeron:ipc` the IPC path sends.
///
/// The assertions are the IPC case's, plus the two the network makes possible:
/// a live session's counters read by the reference's own `AeronStat` — among
/// them the `rcv-channel` label, which is where the address the endpoint is
/// bound to is readable — and the channel in the unavailable-image line, which
/// for a network image is the URI the subscriber named.
#[test]
fn the_reference_client_publishes_to_our_driver_over_udp_and_reads_it_back() {
    let Some(publisher_binary) = samples::locate("BasicPublisher") else {
        driver::announce_tool_skip("BasicPublisher");
        return;
    };
    let Some(subscriber_binary) = samples::locate("BasicSubscriber") else {
        driver::announce_tool_skip("BasicSubscriber");
        return;
    };
    let Some(mut driver) = start_udp("udp-pubsub") else {
        return;
    };

    driver
        .await_cnc(READY_TIMEOUT)
        .expect("our driver publishes a readable CnC file");
    let dir = driver.aeron_dir().to_owned();
    let port = free_udp_port();
    let channel = format!("aeron:udp?endpoint=localhost:{port}");
    let stream = STREAM_ID.to_string();

    // The subscriber first, for the reason the IPC case gives: a publication
    // with no reader cannot be offered to, and the sample does not retry.
    let mut subscriber = Sample::start(
        &subscriber_binary,
        "subscriber",
        &dir,
        &["-c", &channel, "-s", &stream],
    );
    subscriber.await_output(Duration::from_secs(20), "its subscription", |output| {
        output.contains("Subscription channel status")
    });

    let mut publisher = Sample::start(
        &publisher_binary,
        "publisher",
        &dir,
        &["-c", &channel, "-s", &stream, "-m", "4"],
    );
    publisher.await_output(Duration::from_secs(30), "its first offer", |output| {
        output.contains("yay!")
    });

    // A live UDP session, read by the reference's own tool. The rows are the
    // network ones — a subscription's channel status, a publication's position
    // and limit, and the NAKs its sender answers — and `rcv-channel` carries
    // the address the endpoint is *bound* to, which is the port the channel
    // named and is only readable from that label
    // (`aeron_driver_conductor.c:2157-2165`).
    if let Some(aeron_stat) = driver::locate_aeron_stat() {
        let text = String::from_utf8_lossy(
            &Command::new(&aeron_stat)
                .arg("-d")
                .arg(&dir)
                .arg("-w")
                .arg("false")
                .output()
                .expect("run AeronStat")
                .stdout,
        )
        .into_owned();

        for row in [
            "pub-pos (concurrent)",
            "pub-lmt",
            "sub-pos",
            "snd-naks-received",
        ] {
            assert!(
                text.contains(row),
                "AeronStat does not see `{row}` while a UDP publication and subscription are \
                 live:\n{text}"
            );
        }

        for label in [
            // The subscription's endpoint is bound where the channel said, and
            // the label is where a reader finds that out.
            format!("rcv-channel: {channel} 127.0.0.1:{port}"),
            // The publication's endpoint binds a port of its own choosing, so
            // only the channel half of this one can be asserted.
            format!("snd-channel: {channel} "),
        ] {
            assert!(
                text.contains(&label),
                "AeronStat does not see `{label}` on a live UDP session:\n{text}"
            );
        }
    } else {
        driver::announce_tool_skip("AeronStat");
    }

    // Four messages is a window that has to open four times: the sample sleeps
    // a second between them and does not retry one it could not send.
    let status = publisher
        .await_exit(SAMPLE_TIMEOUT)
        .expect("the publisher finishes");
    let output = publisher.output();
    assert!(status.success(), "the publisher failed:\n{output}");
    assert_eq!(
        4,
        output.matches("yay!").count(),
        "every message has to be offered into an open window:\n{output}"
    );
    assert!(
        !output.contains("not connected") && !output.contains("back pressure"),
        "the driver never opened the window:\n{output}"
    );

    // And the reader has them, in the frames this driver's publication carried
    // over the wire.
    let received = subscriber.await_output(Duration::from_secs(20), "every message", |output| {
        output.matches("Message to stream").count() >= 4
    });

    for index in 0..4 {
        assert!(
            received.contains(&format!("<<Hello World! {index}>>")),
            "message {index} is missing from:\n{received}"
        );
    }

    // The session id the subscriber reads out of a **frame on the wire** is the
    // one the driver wrote into the log's metadata template and sent in
    // `ON_AVAILABLE_IMAGE` — three places that have to agree, and over UDP the
    // frame is the one that travelled.
    let rest = received
        .split("from session ")
        .nth(1)
        .expect("a session id in the subscriber's output");
    let digits: String = rest
        .char_indices()
        .take_while(|(index, character)| {
            character.is_ascii_digit() || (*index == 0 && *character == '-')
        })
        .map(|(_, character)| character)
        .collect();
    let session_id: i32 = digits.parse().expect("a session id");

    assert!(
        !(-1..=1000).contains(&session_id),
        "the driver speculates session ids outside its reserved range: {session_id}"
    );

    // When the publisher's client goes, the driver drains the publication and
    // tells every reader its image is gone.
    let ended =
        subscriber.await_output(Duration::from_secs(30), "its image to go away", |output| {
            output.contains("Unavailable image on correlationId=")
        });
    assert!(
        ended.contains(&format!("sessionId={session_id}")),
        "the image that went away is the session this reader was reading:\n{ended}"
    );

    // The **same image** on both lines, compared rather than asserted against a
    // literal: the correlation id a network image gets depends on how many
    // registrations came before it, which is not a contract — while "the image
    // that appeared is the image that went away" is one.
    assert_eq!(
        token(
            &received,
            "Available image correlationId=",
            "correlationId="
        ),
        token(
            &ended,
            "Unavailable image on correlationId=",
            "correlationId="
        ),
        "the unavailable image is not the one that was available:\n{ended}"
    );

    // The samples print `image.sourceIdentity()`, and what that is depends on
    // the transport: the IPC path sends the constant `aeron:ipc`
    // (`crates/driver/src/ipc_publications.rs`), while a network image's is
    // **the address its frames come from**, formatted the way the reference
    // formats it (`aeron_publication_image.c:336-338`). So over UDP this field
    // is an address — the send endpoint's, which the kernel chose — and the two
    // lines agree on it because they are the same image.
    let appeared = token(&received, "Available image", "from ").expect("a source identity");
    let went_away = token(&ended, "Unavailable image", "from ").expect("a source identity");

    assert_eq!(appeared, went_away, "one image, one source:\n{ended}");
    assert!(
        appeared.parse::<std::net::SocketAddr>().is_ok(),
        "a network image's source identity is the address its frames come from, not {appeared:?}"
    );
    assert_ne!(
        "aeron:ipc", appeared,
        "the IPC path sends a constant, and this is not it"
    );

    for complaint in ["keepalive", "driver timeout", "shutdown"] {
        assert!(
            !ended.to_lowercase().contains(complaint),
            "the client must not have timed the driver out (`{complaint}`):\n{ended}"
        );
    }

    let status = subscriber
        .terminate(Duration::from_secs(10))
        .expect("the subscriber stops");
    assert!(
        status.success(),
        "the subscriber did not shut down cleanly:\n{}",
        subscriber.output()
    );

    assert!(
        driver.stop().is_ok(),
        "the driver stops on a signal:\n{}",
        driver.log_tail(20)
    );
}
