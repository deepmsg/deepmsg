//! P2-C2's acceptance: a replay merged into a live stream, end to end.
//!
//! The C suite has no case for this — `ReplayMerge` is a client, and the
//! reference's own tests for it (`aeron-system-tests/.../ReplayMergeTest.java`,
//! `DualReplayMergeTest.java`) are Java system tests, which can swap the driver
//! but not the client (see the P2 plan's §2.4). So this is the criterion, and it
//! is the one the slice exists for: **our** merge over **our** archive, over
//! UDP, with the message stream asserted rather than the state machine.
//!
//! # What makes a merge a merge
//!
//! A reader cannot subscribe to a recording and a live stream at once — the two
//! would arrive in an order nobody controls. The merge replays onto the
//! subscription the live stream will use and moves the subscription from one
//! destination to the other, and the claim is that this is a **move**: every
//! message the publisher wrote comes out once, in order, with nothing lost at
//! the seam and nothing delivered twice.
//!
//! That seam is the whole reason the test publishes **while the merge runs**, and
//! why the recording is **stopped** before it starts. While the recording is
//! open the replay chases it and the seam never has to be crossed; stopped, the
//! replay has a fixed end, and a message published after it is a message only
//! the live transport can deliver. One that goes missing is a hole in the
//! stream, and one delivered twice is a hole's twin.
//!
//! # The one shape this needs that a plain subscription does not
//!
//! The subscription is `control-mode=manual` and the replay channel asks for the
//! **live stream's session id** (`ReplayMerge.java:131`). Both are what make the
//! two sources arrive as **one image with two transports** rather than two
//! images a reader would have to interleave: the driver joins publications that
//! agree on session and stream, and
//! `ReplayMerge.shouldStopAndRemoveReplay` reads exactly that count
//! (`:569-574`) to decide the live transport is carrying the stream. Measured —
//! with two same-session publications on two destinations, the subscription
//! holds one image whose `activeTransportCount` is 2.
//!
//! Everything here is our own driver, client and archive, so it needs no
//! reference checkout and runs in CI.

use std::time::{Duration, Instant};

use deepmsg_archive::client::replay_merge::{
    ClientMergePeer, MERGE_PROGRESS_TIMEOUT_DEFAULT_MS, ReplayMerge,
};
use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_tests::archive::{self, Archive};

/// The connect's correlation id, which its answer echoes. Its own, because every
/// test file picks one.
const CONNECT_CORRELATION_ID: i64 = 0x5ed1_0006;

/// The stream the recording, the live publication and the merge all use.
const STREAM_ID: i32 = 3437;

/// The live publication's **control** address, which is the only address it
/// binds.
///
/// A port of its own for `archive_replay.rs`'s reason — two drivers on one host,
/// and `cargo test` runs test files at once.
///
/// The publication is `control-mode=dynamic`, and that is not decoration: a
/// unicast publication binds the endpoint of its channel as soon as it is
/// connected, and the merge's live **destination** is a receive destination,
/// which binds the address rather than joining the endpoint already there. On a
/// unicast live stream the merge therefore fails to add it — *Address already in
/// use*, which is how this was found — and the reference's own test streams the
/// live half on a dynamic channel (`ReplayMergeTest.java:81-88`) for the same
/// reason: the subscriber binds its own port and talks to the publication's
/// control address.
const LIVE_CONTROL_ENDPOINT: &str = "127.0.0.1:33800";

/// The port the subscription binds as its **live destination**, and the address
/// it sends its status messages from.
const LIVE_ENDPOINT: &str = "127.0.0.1:33803";

/// Where the archive's replay is published, and equally the address the
/// subscription is given as its **replay destination**.
///
/// A port of its own rather than the wildcard (`:0`) the reference's own test
/// uses: a wildcard replay destination is one the subscription resolves, and
/// this build's `Subscription.resolvedEndpoint` has no answer for a manual
/// subscription that names no endpoint. The reference's other path — a
/// destination that named a port — is the one taken here, and it is the same
/// merge (`ReplayMerge.java:144-152`).
const REPLAY_ENDPOINT: &str = "127.0.0.1:33801";

/// The port the archive's recording subscription — and the reader beside it —
/// binds. The merge never names it: a subscriber of a dynamic publication is
/// reached by its own socket, and the merge's live destination names one of its
/// own ([`LIVE_ENDPOINT`]).
const RECORDING_ENDPOINT: &str = "127.0.0.1:33802";

/// The live publication, which binds [`LIVE_CONTROL_ENDPOINT`] and sends to
/// whoever talks to it.
///
/// The term length is pinned on the channel *and* on the driver below, because
/// the archive's replay publishes at the **recording's** geometry and the two
/// sources have to agree on it to be one image.
fn live_channel() -> String {
    format!("aeron:udp?control={LIVE_CONTROL_ENDPOINT}|control-mode=dynamic|term-length=64k")
}

/// The channel the archive records, and the channel a reader reads.
///
/// Its own endpoint, and its control frames go to the publication's control
/// address — the shape a subscriber of a dynamic publication has
/// (`aeron_udp_channel.c:465-487`).
fn recording_channel() -> String {
    format!(
        "aeron:udp?endpoint={RECORDING_ENDPOINT}|control={LIVE_CONTROL_ENDPOINT}|term-length=64k"
    )
}

/// The **live destination** the merge adds: the subscriber's own socket, and the
/// publication's control address to talk to.
fn live_destination() -> String {
    format!("aeron:udp?endpoint={LIVE_ENDPOINT}|control={LIVE_CONTROL_ENDPOINT}")
}

/// The subscription the merge moves from one destination to the other.
///
/// `control-mode=manual` because that is what a merge requires
/// (`ReplayMerge.java:128-132`): the destinations are the merge's to name, and
/// the subscription reads from nothing else.
const SUBSCRIPTION_CHANNEL: &str = "aeron:udp?control-mode=manual";

/// The term length both sides use, and the archive's segment file length.
const TERM_LENGTH: i32 = 64 * 1024;

/// How many messages the recording holds, and how long each is.
///
/// Small enough to replay in a turn or two, and enough of them that the
/// recording is not one frame.
const RECORDED_MESSAGES: usize = 40;
/// See [`RECORDED_MESSAGES`].
const MESSAGE_SIZE: usize = 256;

/// How many messages are published **after** the recording is stopped, which are
/// the ones only the live transport can deliver.
///
/// One per turn of the merge, so they are spread across the move rather than
/// arriving in a bunch before it or after it.
const LIVE_MESSAGES: usize = 20;

/// How long the whole thing may take.
const DEADLINE: Duration = Duration::from_secs(30);

/// How long the merge is given to reach `MERGED` once it has started.
const MERGE_DEADLINE: Duration = Duration::from_secs(20);

/// The archive's properties, plus the two this file needs.
///
/// The term length is the pair's: the recording's geometry comes from the
/// driver's `aeron.term.buffer.length`, the replay is published at it, and the
/// live publication's channel names the same one — three places, one number.
fn properties() -> Vec<String> {
    let mut properties: Vec<String> = archive::PROPERTIES
        .iter()
        .map(|property| (*property).to_owned())
        .collect();

    properties.push(format!("-Daeron.term.buffer.length={TERM_LENGTH}"));
    properties.push(format!("-Daeron.archive.segment.file.length={TERM_LENGTH}"));

    properties
}

/// The bytes of message `index`, the same on both sides of the seam.
fn message(index: usize) -> Vec<u8> {
    archive::message(index, MESSAGE_SIZE)
}

/// Offer one message, polling until the driver takes it.
fn publish(client: &mut Client, publication: i64, payload: &[u8]) {
    let deadline = Instant::now() + DEADLINE;

    loop {
        assert!(
            Instant::now() < deadline,
            "the publication never took a {} byte message",
            payload.len()
        );

        client.poll();

        match client.offer_exclusive(publication, payload) {
            Some(Appended::Ok { .. }) => return,
            Some(_) | None => std::thread::sleep(Duration::from_millis(1)),
        }
    }
}

/// A merge of a recording into the live stream that produced it, from the first
/// message to the last, with the seam in the middle.
#[test]
fn a_replay_merges_into_the_live_stream_without_a_gap() {
    let Some(mut archive) = Archive::start(
        "archive-replay-merge",
        CONNECT_CORRELATION_ID,
        &properties(),
    ) else {
        return;
    };

    // The live publication, which is what the archive records and what the merge
    // joins at the end. It is **one** publication for the whole test: a merge
    // that was handed a different one to join would be a different stream.
    let publication = archive
        .client
        .add_exclusive_publication(&live_channel(), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the driver must accept the live publication");
    let session_id = archive
        .client
        .exclusive_publication(publication)
        .map(|publication| publication.session_id())
        .expect("the publication has a session");

    // A reader, so that the publication has a receiver and its position advances
    // — `Archive::record` makes one for the same reason.
    let reader = archive
        .client
        .add_subscription(&recording_channel(), STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the driver must accept a reader");

    // **REMOTE**, which is `SourceLocation.REMOTE` and not a detail: a `LOCAL`
    // recording of a UDP channel is recorded through a **spy** on the local
    // publication (`ArchiveConductor.java:559-560`, this build's
    // `decide_start`), which records whatever the local publication wrote and
    // never asks the network for it. The reference's own merge test records
    // REMOTE (`ReplayMergeTest.java:198`) — the recording has to be a reader of
    // the stream the merge is going to join.
    let recording_subscription = archive
        .session
        .archive_mut()
        .start_recording(
            &mut archive.client,
            &recording_channel(),
            STREAM_ID,
            false,
            false,
        )
        .expect("the archive starts a recording");

    for index in 0..RECORDED_MESSAGES {
        publish(&mut archive.client, publication, &message(index));
    }

    let recording_id = archive.recording_of_a_live_stream(publication, reader, session_id);

    // **Stopped**, which is what puts the seam in the middle of the test: the
    // recording now ends where the last message does, so the replay has a fixed
    // target and everything published from here is the live transport's to
    // deliver.
    archive
        .session
        .archive_mut()
        .stop_recording_subscription(&mut archive.client, recording_subscription)
        .expect("the archive stops the recording");

    let subscription = archive
        .client
        .add_subscription(SUBSCRIPTION_CHANNEL, STREAM_ID, DEFAULT_TIMEOUT)
        .expect("the driver must accept the merge's subscription");

    // The replay channel is a template: the archive writes the recording's
    // geometry onto it. `session-id` is the one thing the merge has to say
    // itself — the live stream's session, without which the replay arrives as a
    // second image rather than a second transport of the same one.
    let mut merge = ReplayMerge::new(
        &format!("aeron:udp?session-id={session_id}"),
        &format!("aeron:udp?endpoint={REPLAY_ENDPOINT}"),
        &live_destination(),
        recording_id,
        0,
        MERGE_PROGRESS_TIMEOUT_DEFAULT_MS,
    )
    .expect("a channel");

    let mut received: Vec<Vec<u8>> = Vec::new();
    let mut published = RECORDED_MESSAGES;
    let deadline = Instant::now() + MERGE_DEADLINE;

    while !merge.is_merged() {
        assert!(
            Instant::now() < deadline,
            "the merge did not merge: {:?} in {}, live added {}, {} messages read; \
             the subscription holds {:?}\n{}",
            merge.failure_reason(),
            merge.state().name(),
            merge.is_live_added(),
            received.len(),
            images(&archive.client, subscription),
            archive.log_tail(20),
        );

        // The live stream keeps writing while the merge moves the subscription,
        // which is what an archive's merge is for. One message a turn, and
        // **not waited on**: between the recording stopping and the live
        // destination going on there is no reader of this stream at all, so an
        // offer is refused until the merge is the one that turns it on — a
        // blocking offer here would be waiting for the merge's own next turn.
        if published < RECORDED_MESSAGES + LIVE_MESSAGES {
            let payload = message(published);

            if let Some(Appended::Ok { .. }) = archive.client.offer_exclusive(publication, &payload)
            {
                published += 1;
            }
        }

        // The peer borrows the client and the archive client, which is also what
        // publishes above — so it is built around the turn rather than held
        // across the loop. It holds nothing of its own but the subscription's id
        // and stream.
        let mut peer = ClientMergePeer::new(
            &mut archive.client,
            archive.session.archive_mut(),
            subscription,
        )
        .expect("a peer over the merge's subscription");

        merge.poll(&mut peer, epoch_ms(), 16, |fragment| {
            let mut payload = vec![0u8; fragment.payload_length()];

            fragment
                .copy_payload(&mut payload)
                .expect("the payload copies");

            received.push(payload);
        });

        std::thread::sleep(Duration::from_millis(1));
    }

    // What the merge left behind is live, so the rest of the stream goes out the
    // ordinary way — and comes back, which is the other half of the claim: not
    // only did nothing go missing at the seam, the subscription is still a
    // subscription afterwards.
    while published < RECORDED_MESSAGES + LIVE_MESSAGES {
        publish(&mut archive.client, publication, &message(published));
        published += 1;
    }

    let deadline = Instant::now() + DEADLINE;

    while received.len() < RECORDED_MESSAGES + LIVE_MESSAGES {
        assert!(
            Instant::now() < deadline,
            "{} of {} messages came back after the merge: {:?}",
            received.len(),
            RECORDED_MESSAGES + LIVE_MESSAGES,
            images(&archive.client, subscription),
        );

        let mut peer = ClientMergePeer::new(
            &mut archive.client,
            archive.session.archive_mut(),
            subscription,
        )
        .expect("a peer over the merge's subscription");

        merge.poll(&mut peer, epoch_ms(), 16, |fragment| {
            let mut payload = vec![0u8; fragment.payload_length()];

            fragment
                .copy_payload(&mut payload)
                .expect("the payload copies");

            received.push(payload);
        });

        std::thread::sleep(Duration::from_millis(1));
    }

    // Everything the publisher wrote, in the order it wrote it, once each: the
    // assertion the whole slice is for. A message lost at the seam is a short
    // read; one delivered by both transports is a duplicate; one out of order is
    // neither and is worse.
    let expected: Vec<Vec<u8>> = (0..RECORDED_MESSAGES + LIVE_MESSAGES)
        .map(message)
        .collect();

    assert_eq!(
        expected.len(),
        received.len(),
        "every message comes out once: merged in {}, live added {}, {} published",
        merge.state().name(),
        merge.is_live_added(),
        published
    );

    for (index, (expected, received)) in expected.iter().zip(received.iter()).enumerate() {
        assert_eq!(
            expected, received,
            "message {index}, and the {} before it, are the stream in order",
            index
        );
    }

    // And the merge says so, from the other side: the replay destination is off
    // the subscription and the live one is on.
    assert!(merge.is_merged());
    assert!(merge.is_live_added());
    assert_eq!(
        1,
        archive
            .client
            .subscription(subscription)
            .expect("the subscription")
            .images()
            .len(),
        "one image, fed by the live transport alone"
    );

    merge.close(
        &mut ClientMergePeer::new(
            &mut archive.client,
            archive.session.archive_mut(),
            subscription,
        )
        .expect("a peer"),
    );
    let _ = archive.stop();
}

/// What the subscription holds, for a failure message.
fn images(client: &Client, subscription: i64) -> Vec<(i32, i64, Option<i32>)> {
    client
        .subscription(subscription)
        .map(|subscription| {
            subscription
                .images()
                .iter()
                .map(|image| {
                    (
                        image.session_id(),
                        image.position(),
                        image.active_transport_count(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The merge's clock, which only has to move forward.
fn epoch_ms() -> i64 {
    use std::sync::atomic::{AtomicI64, Ordering};

    static NOW: AtomicI64 = AtomicI64::new(0);

    NOW.fetch_add(1, Ordering::Relaxed)
}
