//! A8: the reference's own response samples, end to end, on **our** driver.
//!
//! `response_channel.rs` drives the handshake from our side, with hand-built
//! frames and a socket standing in for the far end. That pins each step but it
//! cannot pin the *sequence*, because the sequence is the one thing a test that
//! writes both ends of it decides for itself. These two programs share no code
//! with this build, and they decide it the way the reference does — so if the
//! nine steps are wired in a different order, or one of them never happens,
//! nothing arrives and the client says so.
//!
//! It is also the only place the **response channel's address discipline** is
//! exercised as the reference ships it: both samples name
//! `aeron:udp?control=<addr>` and nothing else (`samples_configuration.h:34`),
//! so the requester's receive endpoint takes an ephemeral port and elicits *to*
//! the control address, while the responder's publication is bound there and
//! has nowhere of its own to send until it is asked (`aeron_network_publication.c:355-378`).
//!
//! # What only this arrangement covers
//!
//! 1. `response-correlation-id` as the samples actually fill it: the client
//!    names its own response subscription when it creates the *request*
//!    publication (`response_client.c:390`), and the server reads the id back
//!    off its **image** (`response_server.c:311`, `:326`). That round trip is
//!    an id crossing a client boundary, not a number one side chose.
//! 2. The `SETUP`'s `SEND_RESPONSE` flag being what makes an image eligible to
//!    answer at all, decided by a program that never read this build's code.
//! 3. The `RSP_SETUP` → elicit-by-name → `endpoint_address` chain, which is the
//!    ⑩-5 slice and has no other end-to-end cover.

use std::time::Duration;

use deepmsg_tests::driver::{self, OwnDriver, READY_TIMEOUT};
use deepmsg_tests::samples::{self, Sample};

/// How long the pair gets. The client sleeps a second between messages and
/// polls only once per message, so this covers ten of those plus the driver's
/// start-up and the handshake behind them.
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(60);

/// A port this process may use, off a base that keeps two test binaries running
/// at once apart.
fn free_udp_port(offset: u16) -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 20_000 + (std::process::id() as u16 % 20_000);

    base.saturating_add(offset)
}

#[test]
fn the_reference_response_samples_finish_the_handshake_on_our_driver() {
    let Some(client_binary) = samples::locate("response_client") else {
        driver::announce_tool_skip("response_client");
        return;
    };
    let Some(server_binary) = samples::locate("response_server") else {
        driver::announce_tool_skip("response_server");
        return;
    };

    // A one-megabyte term buffer rather than the default sixty-four: these
    // messages are a dozen bytes, and a log buffer is *three* terms, so the
    // default would write hundreds of megabytes to prove what one does.
    let Some(mut own) = OwnDriver::start_with(
        "own-driver-response-samples",
        &["-Daeron.term.buffer.length=1m"],
    ) else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(READY_TIMEOUT)
        .expect("our driver must publish a readable CnC file");
    let dir = own.aeron_dir().to_owned();

    // The two channels, at ports this process owns rather than the samples'
    // defaults, so a driver someone else is running cannot be mistaken for
    // ours.
    //
    // `-c` is the **request** channel, which the client publishes on and the
    // server subscribes to. `-d` is the **response** channel, which the
    // client's subscription and the server's publication share — and the
    // sharing is the design: it is the `control=`-only form, which is what
    // gives the requester somewhere to elicit and the responder nowhere to
    // send until it is asked.
    let request = format!("aeron:udp?endpoint=127.0.0.1:{}", free_udp_port(301));
    let response = format!("aeron:udp?control=127.0.0.1:{}", free_udp_port(302));

    // The server first. Its request subscription has to be up before the client
    // offers, because an offer to a publication with no reader is not
    // connected — and the client, like the reference's other samples, does not
    // retry one.
    //
    // Its readiness is **not** waited on, and that is not laziness: both samples
    // write their progress with `printf` and neither calls `setvbuf`, so to a
    // file their stdout is fully buffered and nothing reaches it until the
    // process exits. The server never exits on its own, so its output is
    // unreadable while it matters. The client is a different case only because
    // it flushes after each offer (`response_client.c:255`), which is what the
    // two waits below stand on.
    let mut server = Sample::start(
        &server_binary,
        "response-server",
        &dir,
        &["-c", &request, "-d", &response],
    );

    let mut client = Sample::start(
        &client_binary,
        "response-client",
        &dir,
        &["-c", &request, "-d", &response, "-m", "5"],
    );

    // The first wait is the readiness the server's own output cannot give: an
    // offer that came back `yay!` is one the server's subscription was there to
    // read, so everything before it is up. An earlier offer may fail while the
    // server is still starting, which is why this is not the assertion.
    client.await_output(SAMPLE_TIMEOUT, "an offer the server read", |output| {
        output.contains("yay!")
    });

    // The assertion, and the only one that covers the whole handshake. The
    // client has no image on its response subscription until the server's image
    // has said which session it answers on, the client has elicited by name,
    // and the server's publication has learned from that ask where it may send.
    // Any of the nine steps missing and this line never prints.
    let output = client.await_output(SAMPLE_TIMEOUT, "a response", |output| {
        output.contains("Response to stream")
    });

    assert!(
        output.contains("yay!"),
        "the client's offers were answered:\n{output}"
    );

    // The client exits on its own. Its exit code is **not** the assertion: the
    // sample initialises `status` to `EXIT_FAILURE` and never sets it
    // (`response_client.c:118`, against the server's `:266`), so a run that did
    // everything right still reports failure. What it printed is the evidence.
    let _ = client.await_exit(SAMPLE_TIMEOUT);

    let _ = server.terminate(Duration::from_secs(10));
    let _ = own.stop();
}
