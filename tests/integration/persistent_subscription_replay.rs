//! P2-C4: the persistent subscription's **replay half**, against a real archive.
//!
//! # What is judged here
//!
//! A recording made and stopped before the subscription exists, and then
//! **replayed into the application through the subscription's own poll**, in
//! order, while the subscription says it is replaying. That is the whole of the
//! first half of `shouldReplayExistingRecordingThenJoinLive` — the reference's
//! case (`…_persistent_subscription_test.cpp:1011`) — up to the point where the
//! live stream would be joined, which is the next commit.
//!
//! # What the state machine has to get right for this to pass
//!
//! * the four counters, allocated at create over the async client's own
//!   connection;
//! * `list_recording`, whose answer arrives as a **recording descriptor** and
//!   not as a control response;
//! * `validate_descriptor`, which is also where `FROM_START` becomes the
//!   recording's actual start position;
//! * the replay channel's shape: this one names its session, so the replay is
//!   asked for first and the subscription goes on the channel **with the session
//!   the archive named written into it** — which is what makes the replay's
//!   image findable by session id afterwards;
//! * and `REPLAY`, which finds that image and polls it with the application's
//!   own handler.
//!
//! # What this criterion does *not* judge, measured rather than assumed
//!
//! The session id the archive names is written into the replay channel before
//! the subscription goes on it (`:1448-1471`). **Leaving that write out does not
//! fail this test** — tried — because one channel with one publication on it is
//! found by its session id whether the subscription asked for that session or
//! not. What *is* judged is the value written: handing the driver the whole
//! sixty-four-bit replay session id (the reference truncates it) fails this test
//! at the channel, which is how the truncation here was found. The case that
//! needs the write itself is several replays on one channel — the reference's
//! `shouldReplayOverConfiguredChannel`, fourteen combinations — which is a later
//! commit's.

use std::time::{Duration, Instant};

use deepmsg_archive::client::context::{
    AERON_DIR_ENV, CONTROL_CHANNEL_ENV, CONTROL_RESPONSE_CHANNEL_ENV,
};
use deepmsg_archive::client::persistent_subscription::{
    FROM_START, PersistentSubscription, PersistentSubscriptionContext, State,
};
use deepmsg_archive::client::{Archive, ArchiveContext, Handlers, NoCredentials};
use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message};
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// The channel the recording is made on, and **the stream the live subscription
/// would be on**: the descriptor's stream id has to match the context's live
/// stream id, which is what `validate_descriptor` refuses when it does not.
const RECORDING_CHANNEL: &str = "aeron:ipc?alias=ps-replay";
const RECORDING_STREAM_ID: i32 = 4216;

/// Where the replay arrives. Its own stream, so nothing else in this test is on
/// it, and **an IPC channel**, which is the session-specific shape.
const REPLAY_CHANNEL: &str = "aeron:ipc?alias=ps-replay-out";
const REPLAY_STREAM_ID: i32 = 4217;

/// What was recorded, and what the subscription must hand back in this order.
const PAYLOADS: [&[u8]; 5] = [
    b"the first recorded message",
    b"the second",
    b"the third recorded message, longer than the two before it",
    b"the fourth",
    b"the fifth and last",
];

/// What the application's handler was handed.
#[derive(Default)]
struct Collector {
    messages: Vec<Vec<u8>>,
}

impl ControlledHandler for Collector {
    fn on_message(&mut self, message: Message<'_>) -> Action {
        self.messages.push(message.payload.to_vec());

        Action::Continue
    }
}

/// An archive of ours, running.
fn rig(test_name: &str) -> Option<(OwnArchivingMediaDriver, TempDir)> {
    let archive_dir = TempDir::new(test_name);
    let properties: Vec<&str> = archive::PROPERTIES.to_vec();

    let Some(mut media_driver) =
        OwnArchivingMediaDriver::start(test_name, archive_dir.path(), &properties)
    else {
        archiving_driver::announce_skip();
        return None;
    };

    media_driver
        .await_ready(archive::DEADLINE)
        .expect("the archiving media driver comes up");

    Some((media_driver, archive_dir))
}

/// The archive's control channel, and a response channel to be answered on — for
/// both the client that records and the subscription that replays.
fn context(aeron_dir: &std::path::Path) -> ArchiveContext {
    ArchiveContext::resolve(&[
        (AERON_DIR_ENV.to_owned(), aeron_dir.display().to_string()),
        (
            CONTROL_CHANNEL_ENV.to_owned(),
            archive::CONTROL_CHANNEL.to_owned(),
        ),
        (
            CONTROL_RESPONSE_CHANNEL_ENV.to_owned(),
            archive::RESPONSE_CHANNEL.to_owned(),
        ),
    ])
}

/// **The replay half of `shouldReplayExistingRecordingThenJoinLive`**: five
/// recorded messages arrive, in order, through the subscription's poll.
#[test]
fn a_recording_is_replayed_through_the_subscriptions_own_poll() {
    let Some((media_driver, _archive_dir)) = rig("ps-replay") else {
        return;
    };

    let recording_id = record(media_driver.aeron_dir());

    let mut subscription_context = PersistentSubscriptionContext::new();
    subscription_context.archive_context = Some(context(media_driver.aeron_dir()));
    subscription_context.recording_id = recording_id;
    subscription_context.live_channel = Some(RECORDING_CHANNEL.to_owned());
    subscription_context.live_stream_id = RECORDING_STREAM_ID;
    subscription_context.replay_channel = Some(REPLAY_CHANNEL.to_owned());
    subscription_context.replay_stream_id = REPLAY_STREAM_ID;
    // The reference's case asks for the recording from its start, which is the
    // position `validate_descriptor` turns into the recording's own (`:1287-1290`).
    subscription_context.start_position = FROM_START;

    let mut subscription =
        PersistentSubscription::new(subscription_context, Box::new(NoCredentials), None)
            .expect("the subscription is created");

    let mut collector = Collector::default();
    let deadline = Instant::now() + archive::DEADLINE;

    while collector.messages.len() < PAYLOADS.len() {
        assert!(
            Instant::now() < deadline,
            "{} of {} messages arrived; the subscription is in {:?}{}",
            collector.messages.len(),
            PAYLOADS.len(),
            subscription.state(),
            subscription
                .failure_reason()
                .map_or(String::new(), |reason| format!(", and it failed: {reason}"))
        );

        // The application's own handler, handed straight to the subscription —
        // which is where the reference's `controlled_poll` puts it too.
        subscription.controlled_poll(&mut collector, 10);
        std::thread::sleep(Duration::from_millis(1));
    }

    let expected: Vec<Vec<u8>> = PAYLOADS.iter().map(|payload| payload.to_vec()).collect();
    assert_eq!(expected, collector.messages);

    // And it is finished replaying rather than merely past the messages: the
    // state it stops in is the one the live half takes over from.
    assert_eq!(State::Replay, subscription.state());
    assert!(subscription.is_replaying());
    assert!(
        !subscription.has_failed(),
        "{:?}",
        subscription.failure_reason()
    );
}

/// Record [`PAYLOADS`] into a stopped recording, and answer its id — with a
/// client of its own, because the subscription owns the one it connects with.
fn record(aeron_dir: &std::path::Path) -> i64 {
    let mut client = Client::connect(aeron_dir).expect("connect to our driver");

    let mut archive = Archive::connect(
        &context(aeron_dir),
        &mut client,
        &mut NoCredentials,
        Handlers::default(),
    )
    .expect("the archive is connected");

    let subscription_id = archive
        .start_recording(
            &mut client,
            RECORDING_CHANNEL,
            RECORDING_STREAM_ID,
            false,
            false,
        )
        .expect("the archive starts recording");

    let publication = client
        .add_exclusive_publication(
            RECORDING_CHANNEL,
            RECORDING_STREAM_ID,
            archive::COMMAND_TIMEOUT,
        )
        .expect("the driver takes a publication on the recording channel");

    until(&mut client, "the recording publication linking", |client| {
        client
            .exclusive_publication(publication)
            .and_then(deepmsg_client::publication::ExclusivePublication::is_connected)
            .unwrap_or(false)
    });

    // **Offered once.** The first version of this helper offered it inside the
    // wait for the catalog row, so the recording held three copies of it and the
    // replay was right to hand all three back.
    let _ = client.offer_exclusive(publication, PAYLOADS[0]);

    let mut recording_id = None;

    until(&mut client, "the recording's catalog row", |client| {
        recording_id = the_only_recording(&mut archive, client).map(|row| row.recording_id);

        recording_id.is_some()
    });

    let recording_id = recording_id.expect("the loop ends with a row");

    for payload in &PAYLOADS[1..] {
        let _ = client.offer_exclusive(publication, payload);
    }

    // **The archive writes a recording on a thread of its own**, so the stop
    // below can land before the last message is in the log — and a replay of a
    // log that does not hold it replays something else. Waiting for the position
    // to stop moving is what makes the assertion at the top of this file a claim
    // rather than a race (the idiom, and the reason, are
    // `archive_client_api.rs`'s).
    let mut last = -1;

    until(&mut client, "the recording to catch up", |client| {
        let position = archive
            .get_recording_position(client, recording_id)
            .unwrap_or(0);
        let settled = position == last && position > 0;
        last = position;

        settled
    });

    archive
        .stop_recording_subscription(&mut client, subscription_id)
        .expect("the archive stops the subscription it opened");

    until(&mut client, "the recording's stop position", |client| {
        archive
            .get_stop_position(client, recording_id)
            .is_ok_and(|stop| stop >= 0)
    });

    // The publisher goes before the replay is asked for: an IPC subscription
    // matches on stream and session, and one left open on this stream would be
    // feeding the replay's subscription by itself (`archive_client_api.rs`
    // writes the long version of this).
    client
        .remove_publication(publication, archive::COMMAND_TIMEOUT)
        .expect("the source publication is given back");

    recording_id
}

/// The one recording in the catalog, while there is exactly one.
fn the_only_recording(
    archive: &mut Archive,
    client: &mut Client,
) -> Option<deepmsg_archive::client::RecordingDescriptor> {
    let mut rows = Vec::new();

    let _ = archive.list_recordings(
        client,
        0,
        10,
        &mut |descriptor: &deepmsg_archive::client::RecordingDescriptor| {
            rows.push(descriptor.clone());
        },
    );

    match rows.len() {
        1 => rows.pop(),
        _ => None,
    }
}

/// Drive the client until `done`, or fail.
fn until(client: &mut Client, what: &str, mut done: impl FnMut(&mut Client) -> bool) {
    let deadline = Instant::now() + archive::DEADLINE;

    while !done(client) {
        assert!(Instant::now() < deadline, "{what} never happened");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}
