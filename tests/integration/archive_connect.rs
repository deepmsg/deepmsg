//! P2-S1's acceptance: the archive's control plane, answered end to end.
//!
//! Everything the control plane had until this test was unit tests over a
//! fabricated adapter. Two of the defects this slice's own handover records —
//! a response channel built the wrong way round, and a default configuration
//! whose authorisation service could not be constructed at all — were invisible
//! to those and were found by starting the binary. This is the smallest thing
//! that starts it: our archiving media driver, our client, one connect.
//!
//! # It is not `interop/archive_connect_over_ipc.rs`
//!
//! That test is a *driver-side* differential probe: its Java probe launches the
//! **reference's** archive in its own process and compares two readings. Our
//! archive is not in it. This one is our archive or nothing.
//!
//! # Why it runs in CI rather than behind `interop`
//!
//! It needs no reference checkout — only this workspace's own binary, which
//! `cargo test --workspace` builds before it runs anything. That is the same
//! criterion the rest of `tests/integration/` is filed under, and unlike a
//! `required-features` target it is actually *executed* by CI, which only
//! compiles the interop ones.
//!
//! # The channel shape is the reference client's, not an invention
//!
//! A response channel is not a channel either side picks; it is derived, and
//! the two sides have to agree on the derivation. The reference client gets
//! there in one step that looks arbitrary until the driver's side of it is
//! read (`AeronArchive.java:4043-4048`): it subscribes to the response channel
//! **first**, then writes *that subscription's registration id* into the
//! request channel as `response-correlation-id`.
//!
//! From there the chain is three registration ids
//! (`aeron_driver_conductor.c:1711-1760`, implemented in this build at
//! `crates/driver/src/ipc_subscriptions.rs`):
//!
//! ```text
//! the archive's response publication
//!   names the request publication   (response-correlation-id = image.correlationId())
//!     which names the response subscription   (the id the client wrote)
//! ```
//!
//! So the archive answers on a channel it computes from the *response* channel
//! the client asked for — `conductor::response_channel`, stripped and rebuilt
//! with the client's term length, sparse flag and MTU — and the driver pairs
//! the two ends by id rather than by channel text.
//!
//! The rewrite of the request channel is done with [`ChannelUri`] rather than
//! by appending `&response-correlation-id=…` to the text, because a channel's
//! parameters are separated by `|` and not by `&` (`ChannelUri.java:333`, its
//! parser at `:424-456`). A hand-appended `&` is not a separator anywhere in
//! Aeron: it makes the parameter before it carry the rest of the string as its
//! value, so the id the chain is built on silently never appears — and the
//! connect times out with every channel looking right in the log.
//!
//! The UDP control subscription is deliberately left in place. The archive
//! serves two control subscriptions on stream 10 — one UDP, one local IPC —
//! and connecting over the IPC one while the UDP one is live is exactly the
//! shape P2-1c had to fix. This test connects over IPC.

use std::time::Instant;

use deepmsg_client::client::Client;
use deepmsg_tests::archive::{self, DEADLINE, Session};
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// The connect's correlation id, which its answer echoes.
const CORRELATION_ID: i64 = 0x5ed1_0001;

#[test]
fn the_control_plane_answers_a_connect() {
    let archive_dir = TempDir::new("archive-connect-archive");

    let Some(mut media_driver) =
        OwnArchivingMediaDriver::start("archive-connect", archive_dir.path(), &archive::PROPERTIES)
    else {
        archiving_driver::announce_skip();
        return;
    };

    media_driver
        .await_ready(DEADLINE)
        .expect("the archiving media driver must come up and signal its mark file");

    let mut client = Client::connect(media_driver.aeron_dir()).expect("connect to the driver");

    let session = Session::connect(
        &mut client,
        media_driver.aeron_dir(),
        CORRELATION_ID,
        Instant::now() + DEADLINE,
    )
    .unwrap_or_else(|reason| {
        panic!(
            "{reason};\n--- the archiving media driver ---\n{}",
            media_driver.log_tail(60)
        )
    });

    // The connect's answer names the session every later request on this
    // connection is addressed to, and a fresh archive hands out its first.
    assert!(
        session.control_session_id() >= 0,
        "the answer names control session {}",
        session.control_session_id()
    );

    let _ = media_driver.stop();
}
