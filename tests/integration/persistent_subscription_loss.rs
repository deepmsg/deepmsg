//! P2-C4's eighth commit, second half: **the alignment, judged at last**.
//!
//! # What is judged here
//!
//! `shouldCatchupLiveGapDuringAttemptSwitchWithReplayLoss` — the reference's case
//! (`…_persistent_subscription_resilience_test.cpp:2946`) — which is the one the
//! fourth commit **measured** as unreachable from this build's own tests.
//!
//! The machine has two ways through `ATTEMPT_SWITCH`, and until now every case
//! this slice wrote took the short one. When the live stream is **idle** at the
//! moment the subscription joins it, the live image's position and the replay's
//! are equal and the switch is immediate (`:2072-2074`); the two catch-up
//! handlers that align a live stream already **ahead** (`:2002-2058`) never run.
//! The fourth commit said so from the other side: mutating the live probe's
//! `ABORT` to `Continue` left all four of its cells green, which is what a test
//! that never reaches the code under it looks like.
//!
//! What makes live run ahead is loss. Loopback loses nothing, so the replay
//! always keeps up; drop a share of the replay's own frames as they arrive —
//! `aeron_stream_id_frame_data_loss_generator`, at the rate the reference's case
//! asks for — publish a batch while the replay is slowed, and the live stream is
//! **ahead** when the replay finally catches up to where the recording stopped.
//!
//! # The witness, and why it is `join_difference`
//!
//! `join_difference` is how far ahead the live stream was when it was joined, and
//! it is written **by the alignment** — a switch that took the short path never
//! had a difference to write and leaves it at [`NOTHING_JOINED`]. So the
//! assertion is not "it became live", which the short path also gives, but "a
//! difference was measured", which only the alignment can say. The reference's
//! own case asserts the same thing for the same reason
//! (`:3004-3007`, and its comment on why the snapshot is taken inside the
//! callback rather than read after: the value is reset when the catch-up
//! finishes).
//!
//! # What is not here, and why
//!
//! The reference enables its generator **after** the subscription is created, so
//! that the recording is made without loss. Here the rule is fixed before the
//! driver starts, so it is in force from the beginning — which changes nothing
//! for this case, because the rule names the **replay** stream and the recording
//! is made on the live one. A case whose recording *is* the lossy stream would
//! need the runtime switch this build does not have; `docs/compat.md` records
//! that deviation.
//!
//! Unicast, multicast and multi-destination live streams are not here, for the
//! reason the live half's own file gives: they are the same state machine over
//! media this build's other tests cover, and reproducing them would be a test of
//! the driver rather than of this machine.
//!
//! # ★ Why the producer keeps writing instead of bursting
//!
//! The reference publishes its fifty and then waits for the switch. That shape
//! **does not reach the alignment here**, and it took measuring to find out: a
//! live image joins at the publisher's position **at the moment it appears**, and
//! for a publisher that has stopped that is exactly the end of the recording —
//! which is where the replay is going. The two positions meet, the switch takes
//! the short path, and nothing was aligned. Measured both ways with the burst
//! shape: three runs in five left `join_difference` at zero.
//!
//! What leaves the live stream ahead is a producer that is **still writing when
//! the image appears**, so this case writes one more message on each turn of the
//! poll loop and stops the moment the subscription says it is live. That is
//! deterministic in both directions: ten runs in ten reach the alignment and
//! measure a difference of about four messages, and with the loss rule removed
//! the same case fails every time.

use std::time::{Duration, Instant};

use deepmsg_archive::client::context::{
    AERON_DIR_ENV, CONTROL_CHANNEL_ENV, CONTROL_RESPONSE_CHANNEL_ENV,
};
use deepmsg_archive::client::persistent_subscription::{
    FROM_START, PersistentSubscription, PersistentSubscriptionContext, SubscriptionError,
    SubscriptionListener,
};
use deepmsg_archive::client::{Archive, ArchiveContext, Handlers, NoCredentials};
use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message};
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// The stream that is recorded and then live.
const LIVE_CHANNEL: &str = "aeron:ipc?alias=ps-loss-live";
const LIVE_STREAM_ID: i32 = 4238;

/// Where the replay arrives, and **a UDP channel on purpose**: an IPC channel has
/// no receive endpoint, so a rule about what a receive endpoint drops has nothing
/// to act on. Its port is the driver's to pick, which is the shape whose endpoint
/// the archive has to be told about.
const REPLAY_CHANNEL: &str = "aeron:udp?endpoint=localhost:0";
const REPLAY_STREAM_ID: i32 = 4239;

/// What the recording holds before the subscription exists — the reference's
/// five (`:2960`).
const REPLAYED: usize = 5;

/// The **most** this case will write to the live stream while the switch is
/// happening, and an upper bound rather than a count: the loop stops when the
/// subscription goes live, which in practice is after twenty or so. The
/// reference writes a fixed fifty and then waits (`:2996-3002`); see the note on
/// the loop for why that shape cannot be deterministic here.
const AT_MOST_PUBLISHED_DURING_REPLAY: usize = 200;

/// One kilobyte, so that messages are their own frames: a rule that counts
/// frames would otherwise be counting whatever a sender happened to batch
/// together, and "half the frames" would not be half the messages.
const MESSAGE_LENGTH: usize = 1024;

/// The rule the driver is started with: one replay frame in every two.
///
/// The reference asks for 50% (`configure_drop_rate(0.5)`, `:2983`), which is
/// the same share; this build spells a rate as a count (`docs/compat.md`).
const LOSS_RULE: &str = "-Ddeepmsg.debug.receive.data.loss.stream.id=4239";
const LOSS_RATE: &str = "-Ddeepmsg.debug.receive.data.loss.every=2";

/// A message of `MESSAGE_LENGTH` bytes, named so that a mis-ordering is visible.
fn message(index: usize) -> Vec<u8> {
    let mut payload = format!("message {index:04}").into_bytes();
    payload.resize(MESSAGE_LENGTH, b'.');

    payload
}

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

/// What the application's listener heard.
#[derive(Default)]
struct Joins {
    live_joined: usize,
    errors: Vec<String>,
}

/// The listener, with the subscription it belongs to — the difference is read off
/// the subscription at the moment of the callback.
struct SharedJoins(std::rc::Rc<std::cell::RefCell<Joins>>);

impl SubscriptionListener for SharedJoins {
    fn on_live_joined(&mut self) {
        // ★ The **count** is all this records, and the difference is read off
        // the subscription by the test instead. The reference snapshots it inside
        // this callback because it then polls `join_difference()` after an
        // unbounded wait, by which time the catch-up has finished and the value
        // has been reset (`:3005-3007`); here the test reads it the instant
        // `is_live()` holds, which is the same moment, and a listener cannot see
        // the subscription anyway.
        self.0.borrow_mut().live_joined += 1;
    }

    fn on_error(&mut self, error: &SubscriptionError) {
        self.0.borrow_mut().errors.push(format!("{error}"));
    }
}

/// The recording, and the publication still writing to it.
struct Source {
    client: Client,
    archive: Archive,
    publication: i64,
    recording_id: i64,
}

impl Source {
    /// Start a recording, publish the first [`REPLAYED`] messages into it, and
    /// keep it live.
    fn start(aeron_dir: &std::path::Path) -> Self {
        let mut client = Client::connect(aeron_dir).expect("connect to our driver");

        let mut archive = Archive::connect(
            &context(aeron_dir),
            &mut client,
            &mut NoCredentials,
            Handlers::default(),
        )
        .expect("the archive is connected");

        archive
            .start_recording(&mut client, LIVE_CHANNEL, LIVE_STREAM_ID, false, false)
            .expect("the archive starts recording");

        let publication = client
            .add_exclusive_publication(LIVE_CHANNEL, LIVE_STREAM_ID, archive::COMMAND_TIMEOUT)
            .expect("the driver takes a publication on the live channel");

        until(&mut client, "the recording publication linking", |client| {
            client
                .exclusive_publication(publication)
                .and_then(deepmsg_client::publication::ExclusivePublication::is_connected)
                .unwrap_or(false)
        });

        let mut source = Self {
            client,
            archive,
            publication,
            recording_id: 0,
        };

        for index in 0..REPLAYED {
            let payload = message(index);
            source.publish(&payload);
        }

        source.recording_id = source.await_row();

        source
    }

    /// Offer one message to the live stream, and let the driver turn.
    fn publish(&mut self, payload: &[u8]) {
        self.client
            .offer_exclusive(self.publication, payload)
            .expect("the live stream takes a message");

        self.client.poll();
    }

    /// The one recording in the catalog, once it is there.
    fn await_row(&mut self) -> i64 {
        let mut recording_id = None;

        until(&mut self.client, "the recording's catalog row", |client| {
            let mut rows = Vec::new();
            let _ = self.archive.list_recordings(
                client,
                0,
                10,
                &mut |descriptor: &deepmsg_archive::client::RecordingDescriptor| {
                    rows.push(descriptor.clone());
                },
            );

            if rows.len() == 1 {
                recording_id = rows.first().map(|row| row.recording_id);
            }

            recording_id.is_some()
        });

        recording_id.expect("the loop ends with a row")
    }
}

/// ★ The case this file exists for: the replay is slowed by loss while fifty more
/// messages go to the live stream, so the live stream is **ahead** when the
/// replay catches up — and the switch has to align them rather than be handed
/// two positions that already agree.
#[test]
fn the_replay_catches_up_with_a_live_stream_that_ran_ahead() {
    let Some((media_driver, _archive_dir)) = rig("ps-catchup-live-gap") else {
        return;
    };

    let mut source = Source::start(media_driver.aeron_dir());

    let mut context = PersistentSubscriptionContext::new();
    context.archive_context = Some(context_for(media_driver.aeron_dir()));
    context.recording_id = source.recording_id;
    context.live_channel = Some(LIVE_CHANNEL.to_owned());
    context.live_stream_id = LIVE_STREAM_ID;
    context.replay_channel = Some(REPLAY_CHANNEL.to_owned());
    context.replay_stream_id = REPLAY_STREAM_ID;
    context.start_position = FROM_START;

    let joins = std::rc::Rc::new(std::cell::RefCell::new(Joins::default()));

    let mut subscription = PersistentSubscription::new(
        context,
        Box::new(NoCredentials),
        Some(Box::new(SharedJoins(std::rc::Rc::clone(&joins)))),
    )
    .expect("the subscription is created");

    let mut collector = Collector::default();
    let deadline = Instant::now() + archive::DEADLINE;

    // One message through, so that the replay is demonstrably under way before
    // the live stream is advanced — which is what "the replay is slowed while
    // live runs ahead" means.
    while collector.messages.is_empty() {
        assert!(
            Instant::now() < deadline,
            "the replay never delivered anything: state {:?}{}",
            subscription.state(),
            reason(&subscription, &joins),
        );

        subscription.controlled_poll(&mut collector, 1);
        source.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    assert!(
        subscription.is_replaying(),
        "the recording should still be being replayed, but the state is {:?}{}",
        subscription.state(),
        reason(&subscription, &joins),
    );

    // The live stream keeps moving **until the switch happens**, and that is the
    // whole of making this case deterministic.
    //
    // A burst published before the polling starts is a live stream that is
    // *idle* again by the time the live subscription attaches — and a live image
    // joins at the publisher's position **then**, which for an idle publisher is
    // exactly where the replay is going. The two positions agree, the switch
    // takes the short path, and nothing was aligned. Measured both ways: a burst
    // of fifty, and of two hundred, left `join_difference` at zero. What leaves
    // the live stream ahead is a producer that is still writing when the image
    // appears, so each turn of this loop writes one more message.
    let mut next = REPLAYED;
    let last = REPLAYED + AT_MOST_PUBLISHED_DURING_REPLAY;

    while !subscription.is_live() {
        assert!(
            Instant::now() < deadline,
            "the subscription never became live: state {:?}{}",
            subscription.state(),
            reason(&subscription, &joins),
        );

        subscription.controlled_poll(&mut collector, 10);

        if next < last {
            let payload = message(next);
            source.publish(&payload);
            next += 1;
        }

        source.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        1,
        joins.borrow().live_joined,
        "the application should have been told once that the live stream was \
         joined{}",
        reason(&subscription, &joins),
    );

    // ★ **The alignment ran.** A switch that took the short path — the two
    // positions already equal — never had a difference to measure and leaves
    // this at `NOTHING_JOINED`, which is exactly what every case before this one
    // saw. Read here rather than in the callback because this build reads it at
    // the same moment the reference snapshots it: as soon as the join is known.
    let join_difference = subscription.join_difference();

    assert!(
        join_difference > 0,
        "the live stream was not ahead when the switch began (difference \
         {join_difference}), so the alignment was never reached{}",
        reason(&subscription, &joins),
    );

    // And the seam has no gap and no repeat: every message, once, in order.
    let expected: Vec<Vec<u8>> = (0..next).map(message).collect();

    while collector.messages.len() < expected.len() {
        assert!(
            Instant::now() < deadline,
            "only {} of {} message(s) arrived{}",
            collector.messages.len(),
            expected.len(),
            reason(&subscription, &joins),
        );

        subscription.controlled_poll(&mut collector, 10);
        source.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        expected,
        collector.messages,
        "the seam between the replay and the live stream is not clean{}",
        reason(&subscription, &joins),
    );

    assert!(
        joins.borrow().errors.is_empty(),
        "the application heard about errors it should not have: {:?}",
        joins.borrow().errors,
    );
}

/// Why the subscription is where it is, for a failure message.
fn reason(
    subscription: &PersistentSubscription,
    joins: &std::rc::Rc<std::cell::RefCell<Joins>>,
) -> String {
    let failure = subscription
        .failure_reason()
        .map_or(String::new(), |reason| format!(", failed: {reason}"));
    let errors = joins.borrow().errors.join("; ");

    if errors.is_empty() {
        failure
    } else {
        format!("{failure}, errors: {errors}")
    }
}

/// An archive of ours, running, with the driver told to lose replay frames.
fn rig(test_name: &str) -> Option<(OwnArchivingMediaDriver, TempDir)> {
    let archive_dir = TempDir::new(test_name);

    let mut properties: Vec<&str> = archive::PROPERTIES.to_vec();
    properties.push(LOSS_RULE);
    properties.push(LOSS_RATE);

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

/// The archive's control channel, and a response channel to be answered on.
fn context_for(aeron_dir: &std::path::Path) -> ArchiveContext {
    context(aeron_dir)
}

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

/// Drive a client until `done`, or fail.
fn until(client: &mut Client, what: &str, mut done: impl FnMut(&mut Client) -> bool) {
    let deadline = Instant::now() + archive::DEADLINE;

    while !done(client) {
        assert!(Instant::now() < deadline, "{what} never happened");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}
