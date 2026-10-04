//! Claiming through a log that rotates, over and over, without losing the plot.
//!
//! A producer rotates its own log: a claim that would run past the end of the
//! term turns that remainder into a padding frame and moves the log to the next
//! term, and answers "try again" while it does. Try again is bounded work — the
//! producer's cached term and offset move with the rotation — and a producer
//! whose cache does *not* move tries for ever, in userspace, spinning at a
//! hundred percent of a core and never touching the driver again.
//!
//! That is what this is for. The term is deliberately small and the messages are
//! deliberately large, so a few thousand claims cross a few hundred rotations —
//! the rate matters not at all, but the number of rotations is everything.
//!
//! The claim path is exercised directly rather than through the bench rig, so
//! that a failure names the outcome rather than arriving as a run that stopped
//! printing.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH;
use deepmsg_tests::driver::{self, OwnDriver};

/// The stream every test here publishes on.
const STREAM_ID: i32 = 1002;

/// A term small enough that the log rotates every few dozen messages, which is
/// the whole point: at a normal term length this would need half a million
/// messages a second to rotate as often.
const CHANNEL: &str = "aeron:ipc?term-length=64k";

/// How many messages to claim. A 1 KiB message takes a 1056-byte frame, so a
/// 64 KiB term holds about sixty of them and this crosses the log some six
/// hundred times.
const MESSAGES: u64 = 40_000;

/// Under the MTU, whatever the channel's is: a claim larger than one frame
/// carries answers `MessageTooLarge`, which is a refusal to retry rather than a
/// rotation.
const MESSAGE_LENGTH: usize = 1024;

/// How long the whole thing may take before it is a failure rather than a slow
/// machine.
const DEADLINE: Duration = Duration::from_secs(60);

/// How many times in a row the same refusal may be answered before it is a
/// stall rather than contention. The reference's own senders retry an
/// administrative action for ever, so something has to say when "for ever" has
/// arrived.
const STALL: u64 = 1_000_000;

/// A publication's window, which a claim is given rather than looking up.
fn window(client: &Client, publication: i64) -> i64 {
    let Some(counter_id) = client
        .exclusive_publication(publication)
        .map(|publication| publication.position_limit_counter_id())
    else {
        return 0;
    };

    client
        .counters_reader()
        .and_then(|counters| counters.value(counter_id))
        .unwrap_or(0)
}

#[test]
fn claiming_through_rotation_keeps_making_progress() {
    let Some(mut own) = OwnDriver::start("claim-rotation") else {
        driver::announce_own_skip();
        return;
    };

    own.await_cnc(Duration::from_secs(10))
        .expect("the driver publishes its CnC file");
    let mut client = Client::connect(own.aeron_dir()).expect("connect to our driver");

    let publication = client
        .add_exclusive_publication(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("an exclusive publication");
    let subscription = client
        .add_subscription(CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("a subscription");

    // Something has to read, or the window never opens and every claim answers
    // back pressure — which is a different stall with a different cause.
    let deadline = Instant::now() + Duration::from_secs(20);
    while client
        .subscription(subscription)
        .expect("the subscription")
        .images()
        .is_empty()
    {
        assert!(Instant::now() < deadline, "no image arrived");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    let frame_length = i32::try_from(MESSAGE_LENGTH + DATA_HEADER_LENGTH).expect("small");
    let mut claimed = 0_u64;
    let mut received = 0_u64;
    let mut refusals: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut last: Option<&'static str> = None;
    let mut in_a_row = 0_u64;
    let started = Instant::now();

    while claimed < MESSAGES && started.elapsed() < DEADLINE {
        let limit = window(&client, publication);

        let outcome = client
            .exclusive_publication(publication)
            .expect("the publication")
            .try_claim(limit, MESSAGE_LENGTH);

        match outcome {
            Ok(claim) => {
                let frame = claim.frame();
                frame
                    .write_payload(&[0x5a; MESSAGE_LENGTH])
                    .expect("in range");
                frame.publish(frame_length).expect("in range");

                claimed += 1;
                last = None;
                in_a_row = 0;
            }
            Err(appended) => {
                let name = match appended {
                    Appended::EndOfLog => "EndOfLog",
                    Appended::MidRotation => "MidRotation",
                    Appended::BackPressured => "BackPressured",
                    Appended::NotConnected => "NotConnected",
                    other => panic!("a claim answered {other:?}, which is not a retry"),
                };

                *refusals.entry(name).or_default() += 1;

                if last == Some(name) {
                    in_a_row += 1;
                } else {
                    last = Some(name);
                    in_a_row = 1;
                }

                assert!(
                    in_a_row < STALL,
                    "{name} has answered {in_a_row} times in a row after {claimed} claims; \
                     the producer is spinning and the log is not rotating past it. \
                     refusals so far: {refusals:?}"
                );
            }
        }

        // Drain, or the window closes and the run measures back pressure rather
        // than rotation.
        let mut polled = 0;
        client.poll_subscription(subscription, 64, |_| polled += 1);
        received += polled;

        client.poll();
    }

    assert_eq!(
        claimed, MESSAGES,
        "every message should have been claimed: {refusals:?}"
    );

    // Whatever is still in the log has to be readable, or the rotation lost
    // messages rather than moving past them.
    let deadline = Instant::now() + Duration::from_secs(10);
    while received < MESSAGES && Instant::now() < deadline {
        let mut polled = 0;
        client.poll_subscription(subscription, 64, |_| polled += 1);
        received += polled;
        client.poll();
    }

    assert_eq!(
        received, MESSAGES,
        "every claimed message should have arrived: {refusals:?}"
    );
}
