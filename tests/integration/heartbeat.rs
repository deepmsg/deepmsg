//! The driver's heartbeat while a channel's names are being resolved.
//!
//! A name that does not answer is the one thing a control plane cannot do
//! anything about: `getaddrinfo` on a host whose nameserver is unreachable takes
//! seconds, and this build used to spend them **on the conductor** — which stops
//! the heartbeat, which every client reads as a driver that has died
//! (`AERON_DRIVER_TIMEOUT_MS_DEFAULT`, ten seconds; the reference's own
//! `AsyncResourceTest.shouldDetectUnknownHost` is that case). The resolution is
//! the native resource agent's now.
//!
//! Proving that the conductor does not wait needs a resolution that stalls, and
//! no honest setting makes a nameserver stall on demand — so the driver has one
//! that is not honest by design: `debug.resolver.delay.millis` holds every
//! resolution on the agent (`docs/compat.md` records it, as it does
//! `debug.send.data.loss.drop.every`). The channel below names a **literal
//! address**, so the test asks no nameserver anything and says nothing about
//! this host's DNS: the delay is the whole of what makes it slow.
//!
//! What is asserted is the property itself. While an add waits for its channel
//! to be parsed, the heartbeat the driver publishes keeps advancing — read out
//! of the CnC file the way a client reads it.

use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream every test here asks on.
const STREAM_ID: i32 = 1001;

/// How long the driver holds each resolution — comfortably longer than the
/// window the heartbeat is watched through, so that a conductor which waited
/// for the resolution could not publish inside it by luck.
const DELAY_MS: u64 = 4_000;

/// How long the heartbeat is watched for while the add is outstanding. Shorter
/// than the delay by design: a heartbeat that arrives only after the delay
/// elapsed is a heartbeat the conductor published *after* the wait, which is
/// the thing this test is not allowed to accept.
const WINDOW_MS: u64 = 2_500;

/// A channel that needs no nameserver at all.
const CHANNEL: &str = "aeron:udp?endpoint=127.0.0.1:40123";

#[test]
fn the_heartbeat_advances_while_a_channel_is_being_parsed() {
    let Some(mut own) = OwnDriver::start_with(
        "heartbeat-while-parsing",
        &[&format!("-Ddeepmsg.debug.resolver.delay.millis={DELAY_MS}")],
    ) else {
        driver::announce_own_skip();
        return;
    };

    let cnc = own
        .await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let dir = own.aeron_dir().to_path_buf();

    // The add runs on a thread of its own, because what this test is about is
    // what the driver does *while* it is outstanding.
    let add = std::thread::spawn(move || {
        let mut client = Client::connect(&dir).expect("connect to our driver");
        let started = Instant::now();
        let publication = client.add_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT);
        (publication.is_ok(), started.elapsed())
    });

    let before = cnc
        .consumer_heartbeat_ms()
        .expect("the driver heartbeat is readable");
    let deadline = Instant::now() + Duration::from_millis(WINDOW_MS);
    let mut advanced_to = None;

    while Instant::now() < deadline {
        if let Some(now) = cnc.consumer_heartbeat_ms() {
            if now > before {
                advanced_to = Some(now);
                break;
            }
        }

        std::thread::sleep(Duration::from_millis(5));
    }

    let (served, elapsed) = add.join().expect("the add finishes");

    assert!(
        advanced_to.is_some(),
        "the driver published no heartbeat in the {WINDOW_MS} ms after an add whose parse its \
         own agent held for {DELAY_MS} ms: the conductor waited for the resolution, which is \
         what stops a client reading the driver as dead (heartbeat before: {before})"
    );
    assert!(
        served,
        "the publication is served once its channel is parsed"
    );
    assert!(
        elapsed >= Duration::from_millis(DELAY_MS),
        "the delay is what made this slow, so the test measured what it meant to: {elapsed:?}"
    );
}
