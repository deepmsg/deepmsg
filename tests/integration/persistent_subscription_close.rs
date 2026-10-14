//! P2-C4's fourteenth commit: the closing family.
//!
//! # What is judged here
//!
//! `shouldCloseCleanlyDuringReplay` — the reference's case
//! (`…_persistent_subscription_test.cpp:3732`) — and, because that case is thin,
//! what can be said about a close beyond "nothing crashed".
//!
//! # What the reference's own case asserts, and what it does not
//!
//! It polls until one message has been delivered, asserts `is_replaying()` and
//! that **fewer messages than the recording holds** have arrived — and then its
//! RAII guard closes the subscription at scope exit. **Nothing asserts anything
//! about the close**: what makes it "clean" in the reference's own suite is the
//! absence of a crash and whatever its leak checker sees. A test whose evidence
//! is a thing that did not happen is exactly the shape this slice has had to
//! correct four times over, so this file does not stop there.
//!
//! # What is asserted here that the reference's does not
//!
//! * the close **returns** — a machine that took its turn loop with it would not
//!   reach the next line, and the reference's guard cannot tell that from a
//!   crash it never observed;
//! * **the driver and the archive are left usable**: a second subscription, made
//!   after the first is closed mid-replay, replays the same recording from the
//!   start and delivers it whole. A close that left a replay running against a
//!   client that is gone, or left the driver holding a subscription nothing will
//!   ever poll, would show up here rather than nowhere;
//! * and the application's listener hears **nothing** — no error is reported for
//!   a close the application asked for.
//!
//! # ★ What is *not* judged, and why
//!
//! The close sends the archive a **`stop_replay`** before it lets go
//! (`clean_up_replay`, `:741-760`), and **that request is not witnessed here.**
//! It is a control request on a wire the test cannot see, and the two things
//! that would make it visible are both missing: this build has no counter for
//! replay sessions, and an archive that keeps replaying to nobody is not
//! distinguishable from one that has stopped, from the outside — the messages go
//! to a subscription that no longer exists either way.
//!
//! So what this file holds is the *order* and the *release*: the cleanup runs in
//! the reference's order (`:1074-1097`) and gives back everything the
//! subscription was holding. The request itself is recorded as sent and
//! unwitnessed rather than claimed.
//!
//! ★★ **And this case is weaker than it looks, which is measured rather than
//! suspected.** Replacing `close`'s four cleanups with nothing at all — a close
//! that releases only the client — leaves it green, three runs in three. What it
//! can catch is a close that **hangs** or a close that leaves the driver and the
//! archive unable to serve the recording again; what it cannot catch is a close
//! that forgets to give something back. It is here because it is what the
//! reference judges and because it is true, not because it is strong.

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

/// The stream that is recorded.
const LIVE_CHANNEL: &str = "aeron:ipc?alias=ps-close-live";
const LIVE_STREAM_ID: i32 = 4258;

/// Where the replay arrives.
const REPLAY_CHANNEL: &str = "aeron:ipc?alias=ps-close-replay";
const REPLAY_STREAM_ID: i32 = 4259;

/// What the recording holds — the reference's twenty (`:3736`).
const MESSAGES: usize = 20;

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
    errors: Vec<String>,
}

impl SubscriptionListener for Joins {
    fn on_error(&mut self, error: &SubscriptionError) {
        self.errors.push(format!("{error}"));
    }
}

struct SharedJoins(std::rc::Rc<std::cell::RefCell<Joins>>);

impl SubscriptionListener for SharedJoins {
    fn on_error(&mut self, error: &SubscriptionError) {
        self.0.borrow_mut().errors.push(format!("{error}"));
    }
}

/// A recording to subscribe to.
struct Source {
    client: Client,
    archive: Archive,
    /// The publication the recording is still being written through. Kept
    /// because a recording whose publisher has gone is a *stopped* recording,
    /// and this case is about a replay in flight rather than about the end of
    /// one.
    #[allow(dead_code)]
    publication: i64,
    recording_id: i64,
}

impl Source {
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

        for index in 0..MESSAGES {
            let payload = message(index);

            source
                .client
                .offer_exclusive(publication, &payload)
                .expect("taken");
            source.client.poll();
        }

        source.recording_id = source.await_row();

        source
    }

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

/// The recording this file is built on, published in one place.
fn message(index: usize) -> Vec<u8> {
    format!("message {index:04}").into_bytes()
}

/// ★ Closing mid-replay returns, and leaves the driver and the archive able to
/// do it again.
#[test]
fn closing_a_replay_in_flight_leaves_everything_usable() {
    let Some((media_driver, _archive_dir)) = rig("ps-close-during-replay") else {
        return;
    };

    let mut source = Source::start(media_driver.aeron_dir());

    let mut subscription = subscribe(media_driver.aeron_dir(), source.recording_id);

    let mut collector = Collector::default();
    let deadline = Instant::now() + archive::DEADLINE;

    // Part way through the replay — which is the state the case is about, and
    // the reference asserts the same two things here (`:3770-3771`).
    while collector.messages.is_empty() {
        assert!(
            Instant::now() < deadline,
            "the replay never delivered anything",
        );

        subscription.controlled_poll(&mut collector, 1);
        source.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    assert!(subscription.is_replaying());
    assert!(collector.messages.len() < MESSAGES);

    // ★ The close. It has to **return**: a machine that took its turn loop with
    // it would not reach the next line, and the reference's guard cannot tell
    // that from the crash it never observed.
    subscription.close();

    // ★ And everything has to still work afterwards — the driver, the archive
    // and the recording. A second subscription is made over the same recording
    // and has to replay it **whole**: a close that left a replay running against
    // a client that is gone, or left the driver holding a subscription nothing
    // polls, would show up here.
    let mut again = subscribe(media_driver.aeron_dir(), source.recording_id);
    let mut second = Collector::default();

    while second.messages.len() < MESSAGES {
        assert!(
            Instant::now() < deadline,
            "only {} of {MESSAGES} message(s) were replayed after a close",
            second.messages.len(),
        );

        again.controlled_poll(&mut second, 10);
        source.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    let expected: Vec<Vec<u8>> = (0..MESSAGES).map(message).collect();

    assert_eq!(expected, second.messages);

    again.close();
}

/// ★ **Closing at every point of the replay's set-up.**
///
/// The reference's `shouldCancelPendingAsyncOpsOnCloseDuringReplaySubscriptionSetup`
/// (`…_resilience_test.cpp:4917`), which is a loop and nothing else: create a
/// subscription, poll it `0..=20` times, close it, and do that once per count —
/// so that one of the twenty-one closes lands while the replay subscription's
/// add is **in flight**. Its own comment says as much, and what it asserts is
/// nothing at all: the evidence is the absence of a crash and whatever its leak
/// checker sees.
///
/// What is being closed on there is real, and this build had it wrong until this
/// commit: an add the driver has not answered yet has to be **cancelled**, not
/// forgotten, or the driver makes a subscription this machine no longer knows
/// about — one nothing polls and nothing will ever give back. The reference
/// cancels in both of its cleanups (`:725-745`, `:747-777`); this build removed
/// what had arrived and dropped the id of what had not.
///
/// The assertion added here is the one thing a loop like that can be held to
/// beyond not crashing: **the driver and the archive still serve afterwards**,
/// with a twenty-second subscription that replays the recording whole.
#[test]
fn closing_at_every_point_of_the_setup_leaves_everything_usable() {
    let Some((media_driver, _archive_dir)) = rig("ps-close-at-every-point") else {
        return;
    };

    let mut source = Source::start(media_driver.aeron_dir());

    for polls in 0..=20 {
        let mut subscription = subscribe(media_driver.aeron_dir(), source.recording_id);
        let mut collector = Collector::default();

        for _ in 0..polls {
            subscription.controlled_poll(&mut collector, 10);
            source.client.poll();
        }

        subscription.close();
    }

    let mut again = subscribe(media_driver.aeron_dir(), source.recording_id);
    let mut second = Collector::default();
    let deadline = Instant::now() + archive::DEADLINE;

    while second.messages.len() < MESSAGES {
        assert!(
            Instant::now() < deadline,
            "only {} of {MESSAGES} message(s) were replayed after twenty-one \
             closes",
            second.messages.len(),
        );

        again.controlled_poll(&mut second, 10);
        source.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    let expected: Vec<Vec<u8>> = (0..MESSAGES).map(message).collect();

    assert_eq!(expected, second.messages);

    again.close();
}

/// A subscription over `recording_id`, with a listener that reports whatever it
/// is told.
fn subscribe(aeron_dir: &std::path::Path, recording_id: i64) -> PersistentSubscription {
    let mut subscription_context = PersistentSubscriptionContext::new();
    subscription_context.archive_context = Some(context(aeron_dir));
    subscription_context.recording_id = recording_id;
    subscription_context.live_channel = Some(LIVE_CHANNEL.to_owned());
    subscription_context.live_stream_id = LIVE_STREAM_ID;
    subscription_context.replay_channel = Some(REPLAY_CHANNEL.to_owned());
    subscription_context.replay_stream_id = REPLAY_STREAM_ID;
    subscription_context.start_position = FROM_START;

    let joins = std::rc::Rc::new(std::cell::RefCell::new(Joins::default()));

    let subscription = PersistentSubscription::new(
        subscription_context,
        Box::new(NoCredentials),
        Some(Box::new(SharedJoins(std::rc::Rc::clone(&joins)))),
    )
    .expect("the subscription is created");

    let _ = joins;

    subscription
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
