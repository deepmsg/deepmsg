//! P2-C4's eighth commit, third piece: **a deadline that expires**, and what the
//! machine does about it.
//!
//! # What is judged here
//!
//! `shouldTriggerListRecordingTimeoutViaTimeoutManipulation` — the reference's
//! case (`…_persistent_subscription_resilience_test.cpp:4125`) — which asks a
//! question none of this slice's other cases do: not "does the machine take the
//! right step", but "what does it do when an answer **never comes**".
//!
//! Every state that waits on the archive has a deadline, and every one of those
//! deadlines has a branch behind it. The list-recording one is the simplest to
//! reach: a quarter of the machine's work is done before it, so shrinking the
//! deadline to a nanosecond and watching what happens next is watching that
//! branch. With the archive **connected**, the answer is to ask again — the
//! subscription is not failed, nothing is torn down, and the retry costs nothing
//! but a turn.
//!
//! # Why the loop does not sleep, and why it counts states
//!
//! Two things about this build have to be dealt with before the case means
//! anything, and both are the same fact seen twice.
//!
//! **`controlled_poll` reads the archive client before it runs the state**
//! (`:2281`), so an answer that has already arrived is always taken before the
//! deadline is even looked at. A deadline of a nanosecond therefore fires only
//! if a *turn* is shorter than the round trip to the archive — so this case polls
//! in a tight loop and does not sleep, which is what the reference's own loop
//! does too.
//!
//! And **the poll's return value says nothing about whether a deadline
//! expired** — it is a count of work done, which is positive either way. The
//! reference's case asserts only that the poll did not error and that the
//! subscription has not failed, and those two are true of a machine that never
//! timed out at all. So this case watches the **states**: a request that times
//! out is asked again, which means the machine enters
//! [`State::SendListRecordingRequest`] a second time. Once means the answer
//! arrived and the branch was never taken; twice or more means it was.
//!
//! # One thing measured and not explained
//!
//! Run alone this case takes about a tenth of a second. Under a **loaded**
//! machine — the full `cargo test --workspace`, a hundred binary's worth of
//! processes — it has been seen to take ten, which is the deadline that is
//! restored at the end. It passes either way, and it is written down because a
//! future reader who meets the slow run should know it was seen, not guess it:
//! the retry loop makes a hundred requests in a burst, and what that does to the
//! archive under contention is not something this case has tried to bound.
//!
//! # What this build does not have
//!
//! The hook is `set_message_timeout_ns_for_testing`, which is the reference's own
//! (`:2425`) and is on the client side, so nothing here needs the driver or the
//! archive to cooperate. What is *not* here is the rest of the family: the same
//! manipulation for `max_recorded_position` and for the replay token, and the
//! cases that drop responses instead of shrinking deadlines. Those need either a
//! replay far enough along to be asking, or a way to drop one particular
//! response, and they are recorded rather than pretended.

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

/// The stream that is recorded and then live.
const LIVE_CHANNEL: &str = "aeron:ipc?alias=ps-timeout-live";
const LIVE_STREAM_ID: i32 = 4248;

/// Where the replay arrives.
const REPLAY_CHANNEL: &str = "aeron:ipc?alias=ps-timeout-replay";
const REPLAY_STREAM_ID: i32 = 4249;

/// What the recording holds — the reference's ten (`:4139`).
const MESSAGES: usize = 10;

const ARCHIVE_CONTROL_UDP: &str = "aeron:udp?endpoint=127.0.0.1:18010";
const RESPONSE_STREAM_ID: i32 = 20;

fn free_port(offset: u16) -> u16 {
    #[allow(clippy::cast_possible_truncation)] // the low bits of a pid
    let base = 20_000 + (std::process::id() as u16 % 20_000);
    base.saturating_add(offset)
}

/// A deadline that has always passed by the time it is looked at (`:4161`).
const INSTANT: i64 = 1;

/// The archive's own default, restored once the machine has been watched
/// (`:4157`, `:4176`).
const TEN_SECONDS_NS: i64 = 10_000_000_000;

/// How many turns the instant deadline is given.
///
/// Tallied rather than picked: a turn here is a few microseconds and a round trip
/// to the archive is tens of them, so this is far more turns than the retry loop
/// needs — and the assertion is about reaching **two** entries into the asking
/// state, not about how many.
const TURNS: usize = 200;

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

/// The listener, shared with the test that owns it.
struct SharedJoins(std::rc::Rc<std::cell::RefCell<Joins>>);

impl SubscriptionListener for SharedJoins {
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
            let payload = format!("message {index:04}").into_bytes();

            source
                .client
                .offer_exclusive(source.publication, &payload)
                .expect("the live stream takes a message");

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

/// ★ A list-recording request whose answer never comes in time is **asked
/// again**: the subscription is not failed, and once the deadline is a deadline
/// again the machine completes and delivers the recording.
#[test]
fn a_list_recording_request_that_times_out_is_asked_again() {
    let Some((media_driver, _archive_dir)) = rig("ps-timeout-list-recording") else {
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

    // First, connected, with a deadline that is a deadline: the machine reaches
    // the state that asks, which is where the archive is needed.
    let connected_by = Instant::now() + archive::DEADLINE;

    while State::AwaitArchiveConnection == subscription.state() {
        assert!(
            Instant::now() < connected_by,
            "the subscription never connected to the archive{}",
            reason(&subscription, &joins),
        );

        subscription.controlled_poll(&mut collector, 10);
        source.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    // A deadline that has always passed by the time it is looked at.
    subscription.set_message_timeout_ns_for_testing(INSTANT);

    // ★ **No sleep.** A turn has to stay shorter than the round trip to the
    // archive, or every answer is read before the deadline is looked at and the
    // branch is never reached.
    let mut asked = 0;

    for _ in 0..TURNS {
        // Counted **before** the turn: entering the state that asks is what a
        // retry looks like, and the turn itself ends in the state that waits.
        if State::SendListRecordingRequest == subscription.state() {
            asked += 1;
        }

        subscription.controlled_poll(&mut collector, 10);

        assert!(
            !subscription.has_failed(),
            "an expired deadline must not fail the subscription{}",
            reason(&subscription, &joins),
        );
    }

    assert!(
        asked >= 2,
        "the machine asked {asked} time(s) over {TURNS} turns of a \
         one-nanosecond deadline, so the retry was not taken — this case judged \
         nothing",
    );

    assert!(
        collector.messages.is_empty(),
        "the recording was replayed under a one-nanosecond deadline, so no \
         deadline ever expired",
    );

    // Let it go, and a deadline again: the machine completes — the connection
    // was never given up, so this is the same connection the retries were made
    // on rather than a new one.
    subscription.set_message_timeout_ns_for_testing(TEN_SECONDS_NS);

    let deadline = Instant::now() + archive::DEADLINE;

    while !subscription.is_live() {
        assert!(
            Instant::now() < deadline,
            "the subscription never became live after the deadline was restored: \
             state {:?}{}",
            subscription.state(),
            reason(&subscription, &joins),
        );

        subscription.controlled_poll(&mut collector, 10);
        source.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    let expected: Vec<Vec<u8>> = (0..MESSAGES)
        .map(|index| format!("message {index:04}").into_bytes())
        .collect();

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
        "the recording did not arrive whole and in order{}",
        reason(&subscription, &joins),
    );

    assert!(
        joins.borrow().errors.is_empty(),
        "the application heard about errors it should not have: {:?}",
        joins.borrow().errors,
    );
}

/// ★ **A control response that is withheld does not fail the subscription, and
/// the blackout ends by itself.**
///
/// The reference's `shouldTimeoutOnListRecordingAndReplayResponseWhenResponsesDropped`
/// (`…_resilience_test.cpp:4284`): pass the archive's connect handshake, withhold
/// everything after it, so the list-recording response never arrives and its
/// deadline fires. The reference then **turns the loss off** from its own thread
/// and waits for the machine to recover.
///
/// This build has no such moment — a rule is fixed before its driver starts — so
/// the bound is **in the rule**, and it is a **time**: the stream's first two
/// frames go through, everything after them is withheld for three seconds, and
/// then the responses arrive again with nothing having been switched off.
///
/// ★ Why a *time* and not a count of frames is written up on `Blackout`, and it
/// cost two measurements to learn: a frame budget is spent by the transport's
/// own retransmissions, and then — with that fixed — never spent at all, because
/// withholding everything stops the frames arriving long before the budget runs
/// out.
#[test]
fn a_withheld_control_response_does_not_fail_the_subscription() {
    let properties = [
        format!("-Ddeepmsg.debug.receive.data.loss.stream.id={RESPONSE_STREAM_ID}"),
        "-Ddeepmsg.debug.receive.data.loss.pass.first=2".to_owned(),
        "-Ddeepmsg.debug.receive.data.loss.until.millis=3000".to_owned(),
    ];
    let properties: Vec<&str> = properties.iter().map(String::as_str).collect();

    let Some((media_driver, _archive_dir)) =
        rig_on_udp_control("ps-timeout-response-drop", &properties)
    else {
        return;
    };

    // ★ The recording is made over the fixture's **own** channels, and only the
    // subscription is answered on UDP. The rule acts on a receive endpoint, and
    // a source sharing the subscription's endpoint would have its own control
    // responses withheld — the connect among them.
    let mut source = Source::start(media_driver.aeron_dir());

    let mut context = PersistentSubscriptionContext::new();
    context.archive_context = Some(context_with_udp_response(
        media_driver.aeron_dir(),
        free_port(60),
    ));
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

    // The reference's own 200 ms (`:4318`): short enough that a three-second
    // blackout is several deadlines long.
    subscription.set_message_timeout_ns_for_testing(200_000_000);

    let mut collector = Collector::default();

    // ★ **A window in which nothing gets through.** This is the witness, and it
    // had to be this rather than a count of retries: an earlier version counted
    // entries into the asking state and reported two **with and without the
    // blackout**, because that state is also where a turn ends when the send
    // itself could not happen. What a withheld response looks like is a machine
    // that has delivered **nothing**.
    let through_the_blackout = Instant::now() + Duration::from_millis(1500);

    while Instant::now() < through_the_blackout {
        subscription.controlled_poll(&mut collector, 10);
        source.client.poll();
        std::thread::sleep(Duration::from_millis(1));

        assert!(
            !subscription.has_failed(),
            "a withheld response must not fail the subscription{}",
            reason(&subscription, &joins),
        );
    }

    assert!(
        collector.messages.is_empty(),
        "the recording arrived while its responses were being withheld, so \
         nothing was withheld and this case judged nothing",
    );

    // And the window ends by itself: nothing is switched off, and the machine
    // finishes on the responses that arrive after it.
    let deadline = Instant::now() + archive::DEADLINE;

    while !subscription.is_live() {
        assert!(
            Instant::now() < deadline,
            "the subscription never became live after the blackout ended: state \
             {:?}{}",
            subscription.state(),
            reason(&subscription, &joins),
        );

        assert!(
            !subscription.has_failed(),
            "a withheld response must not fail the subscription{}",
            reason(&subscription, &joins),
        );

        subscription.controlled_poll(&mut collector, 10);
        source.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    let expected: Vec<Vec<u8>> = (0..MESSAGES)
        .map(|index| format!("message {index:04}").into_bytes())
        .collect();

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
        "the recording did not arrive whole and in order after the blackout \
         ended{}",
        reason(&subscription, &joins),
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

/// An archive of ours, running.
fn rig(test_name: &str) -> Option<(OwnArchivingMediaDriver, TempDir)> {
    rig_with(test_name, &[])
}

fn rig_with(test_name: &str, extra: &[&str]) -> Option<(OwnArchivingMediaDriver, TempDir)> {
    let archive_dir = TempDir::new(test_name);

    let mut properties: Vec<&str> = archive::PROPERTIES.to_vec();
    properties.extend_from_slice(extra);

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

/// The same channels, with the answers coming back over **UDP** — and the
/// request channel moved with them.
///
/// ★ **Both halves move together, and that is measured rather than assumed.** A
/// version that asked to be answered on a UDP response channel while
/// *requesting over IPC* timed out on the connect with **no rule configured at
/// all**, so what does not work is the pairing, not the rule. The pair that does
/// is the one the ninth commit's UDP cell uses.
fn context_with_udp_response(aeron_dir: &std::path::Path, port: u16) -> ArchiveContext {
    let mut context = context(aeron_dir);

    context.control_request_channel = Some(ARCHIVE_CONTROL_UDP.to_owned());
    context.control_response_channel = Some(format!(
        "aeron:udp?control-mode=response|control=127.0.0.1:{port}"
    ));

    context
}

fn rig_on_udp_control(
    test_name: &str,
    extra: &[&str],
) -> Option<(OwnArchivingMediaDriver, TempDir)> {
    let control = format!("-Daeron.archive.control.channel={ARCHIVE_CONTROL_UDP}");
    let mut properties: Vec<&str> = extra.to_vec();

    properties.push(control.as_str());

    rig_with(test_name, &properties)
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
