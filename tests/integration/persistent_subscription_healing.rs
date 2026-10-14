//! P2-C4: the persistent subscription **heals** — the archive half of it.
//!
//! # What is judged here
//!
//! The reference's `shouldRecoverFromArchiveRestartDuringReplay` and
//! `shouldReconnectToTheArchiveAfterArchiveRestart` (`…_resilience_test.cpp:835`,
//! `:1235`), in the shape this build can drive deterministically: a subscription
//! **in the middle of a replay**, whose archive is killed and started again over
//! the same recordings.
//!
//! What it must do (`on_archive_disconnected`, `:804-829`): keep the position it
//! had read to, let go of everything that belonged to the connection it lost,
//! and wait for another one — **without failing**. Nothing about that is
//! observable through messages here (the recording does not grow while the
//! archive is down), so what is asserted is the state: it waits, then it
//! replays again, and it never reports a terminal failure.
//!
//! # What is *not* judged here, and why it needs a different rig
//!
//! `shouldContinueConsumingFromLiveWhileArchiveIsUnavailable` (`:942`) — the
//! live stream keeps being delivered while the archive is away — **cannot** be
//! driven with this build's archiving media driver, in which the archive and the
//! driver are one process: killing it kills the live subscription too. The
//! reference's own PS tests use a **standalone archive** for exactly this
//! (`TestStandaloneArchive.h`, and the resilience binary is built around it), and
//! so must this: `OwnStandaloneArchive` is in this build's harness
//! (`tests/src/standalone_archive.rs`, P2-S7) but no test here pairs it with a
//! subscription yet. Recorded rather than half-done.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use deepmsg_archive::client::context::{
    AERON_DIR_ENV, CONTROL_CHANNEL_ENV, CONTROL_RESPONSE_CHANNEL_ENV,
};
use deepmsg_archive::client::persistent_subscription::{
    FROM_START, PersistentSubscription, PersistentSubscriptionContext, State, SubscriptionError,
    SubscriptionListener,
};
use deepmsg_archive::client::{Archive, ArchiveContext, Handlers, NoCredentials};
use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{Action, ControlledHandler, Message};
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

const LIVE_CHANNEL: &str = "aeron:ipc?alias=ps-heal";
const LIVE_STREAM_ID: i32 = 4220;
const REPLAY_CHANNEL: &str = "aeron:ipc?alias=ps-heal-replay";
const REPLAY_STREAM_ID: i32 = 4221;

const PAYLOADS: [&[u8]; 5] = [b"heal 0", b"heal 1", b"heal 2", b"heal 3", b"heal 4"];

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
struct Report {
    joined: usize,
    left: usize,
    errors: Vec<String>,
}

struct SharedReport(Rc<RefCell<Report>>);

impl SubscriptionListener for SharedReport {
    fn on_live_joined(&mut self) {
        self.0.borrow_mut().joined += 1;
    }

    fn on_live_left(&mut self) {
        self.0.borrow_mut().left += 1;
    }

    fn on_error(&mut self, error: &SubscriptionError) {
        self.0.borrow_mut().errors.push(format!("{error}"));
    }
}

/// **The archive goes away in the middle of a replay, and the subscription comes
/// back to it.**
#[test]
fn a_subscription_recovers_when_its_archive_is_restarted_mid_replay() {
    let test_name = "ps-healing";
    let archive_dir = TempDir::new(test_name);
    let properties: Vec<&str> = archive::PROPERTIES.to_vec();

    let Some(mut media_driver) =
        OwnArchivingMediaDriver::start(test_name, archive_dir.path(), &properties)
    else {
        archiving_driver::announce_skip();
        return;
    };

    media_driver
        .await_ready(archive::DEADLINE)
        .expect("the archiving media driver comes up");

    let aeron_dir = media_driver.aeron_dir().to_path_buf();
    let recording_id = record(&aeron_dir);

    let mut context = PersistentSubscriptionContext::new();
    context.archive_context = Some(context_for(&aeron_dir));
    context.recording_id = recording_id;
    context.live_channel = Some(LIVE_CHANNEL.to_owned());
    context.live_stream_id = LIVE_STREAM_ID;
    context.replay_channel = Some(REPLAY_CHANNEL.to_owned());
    context.replay_stream_id = REPLAY_STREAM_ID;
    context.start_position = FROM_START;

    let report = Rc::new(RefCell::new(Report::default()));

    let mut subscription = PersistentSubscription::new(
        context,
        Box::new(NoCredentials),
        Some(Box::new(SharedReport(Rc::clone(&report)))),
    )
    .expect("the subscription is created");

    let mut collector = Collector::default();

    poll_until(
        &mut subscription,
        &mut collector,
        "the recording replayed",
        |_, collector| collector.messages.len() == PAYLOADS.len(),
    );
    assert!(subscription.is_replaying(), "{:?}", subscription.state());

    // **The archive goes away.** It is the whole process, driver included, so
    // the subscription loses its connection — and what it must do about that is
    // in `on_archive_disconnected`: keep the position, let go of the rest, wait.
    media_driver.stop().expect("the archive stops");

    poll_until(
        &mut subscription,
        &mut collector,
        "the subscription to wait",
        |subscription, _| subscription.state() == State::AwaitArchiveConnection,
    );

    assert!(
        !subscription.has_failed(),
        "losing the archive is a state, not a failure: {:?}",
        subscription.failure_reason()
    );

    // And another archive over the same recordings, which is what our archive
    // does unless it is told otherwise (`aeron.archive.dir.delete.on.start`,
    // default false).
    let Some(mut media_driver) =
        OwnArchivingMediaDriver::start(test_name, archive_dir.path(), &properties)
    else {
        archiving_driver::announce_skip();
        return;
    };

    media_driver
        .await_ready(archive::DEADLINE)
        .expect("the second archiving media driver comes up");

    // It reconnects, re-lists and replays again — from the position it had read
    // to, which is the whole point of saving it: the messages it already
    // delivered are not delivered twice, and nothing is reported as failed.
    poll_until(
        &mut subscription,
        &mut collector,
        "the subscription to recover",
        |subscription, _| matches!(subscription.state(), State::Replay | State::AttemptSwitch),
    );

    assert!(
        !subscription.has_failed(),
        "{:?}",
        subscription.failure_reason()
    );
    assert_eq!(
        PAYLOADS.len(),
        collector.messages.len(),
        "recovering must not deliver anything again: {:?}",
        collector.messages
    );

    let report = report.borrow();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(0, report.joined, "the live stream was never reached");
}

/// Record `PAYLOADS` into a live recording, and answer its id.
fn record(aeron_dir: &std::path::Path) -> i64 {
    let mut client = Client::connect(aeron_dir).expect("connect to our driver");

    let mut archive = Archive::connect(
        &context_for(aeron_dir),
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

    let _ = client.offer_exclusive(publication, PAYLOADS[0]);

    let mut recording_id = None;

    until(&mut client, "the recording's catalog row", |client| {
        let mut rows = Vec::new();
        let _ = archive.list_recordings(
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

    let recording_id = recording_id.expect("the loop ends with a row");

    for payload in &PAYLOADS[1..] {
        let _ = client.offer_exclusive(publication, payload);
    }

    // The archive writes on a thread of its own, so the messages have to be in
    // the log before this returns — a replay of a log that does not hold them
    // replays something else.
    let mut last = -1;

    until(&mut client, "the recording to catch up", |client| {
        let position = archive
            .get_recording_position(client, recording_id)
            .unwrap_or(0);
        let settled = position == last && position > 0;
        last = position;

        settled
    });

    client
        .remove_publication(publication, archive::COMMAND_TIMEOUT)
        .expect("the source publication is given back");

    recording_id
}

/// Poll the subscription until it is where the test is waiting for it to be.
fn poll_until(
    subscription: &mut PersistentSubscription,
    collector: &mut Collector,
    what: &str,
    done: impl Fn(&PersistentSubscription, &Collector) -> bool,
) {
    let deadline = Instant::now() + archive::DEADLINE;

    while !done(subscription, collector) {
        assert!(
            Instant::now() < deadline,
            "{what} never happened; the subscription is in {:?}{}",
            subscription.state(),
            subscription
                .failure_reason()
                .map_or(String::new(), |reason| format!(", failed: {reason}"))
        );

        subscription.controlled_poll(collector, 10);
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The archive's control channel, and a response channel to be answered on.
fn context_for(aeron_dir: &std::path::Path) -> ArchiveContext {
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
