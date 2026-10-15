//! P2-C4: the persistent subscription's **live half**, against a real archive.
//!
//! # What is judged here
//!
//! `shouldReplayExistingRecordingThenJoinLive` — the reference's case
//! (`…_persistent_subscription_test.cpp:1011`), which is a table of twelve:
//! three fragment limits by four channel shapes. What each cell asks for is one
//! run of the whole thing:
//!
//! 1. five messages are recorded **before the subscription exists**;
//! 2. the subscription replays them, in order, through the application's own
//!    handler, while `is_replaying()`;
//! 3. five more are published to the **live** stream, which is the same stream
//!    the recording was made from;
//! 4. all ten arrive, in order, and the subscription says `is_live()` and not
//!    `is_replaying()` — with `on_live_joined` fired exactly once.
//!
//! # What this build reproduces of the reference's twelve cells, and what it does not
//!
//! The fragment limits are the reference's three (`1`, `10`, and an unlimited
//! one). The channel shapes are two of the reference's four: an **IPC** stream,
//! and a **UDP channel whose endpoint is port zero** — which is not one of the
//! reference's four cells at all, but is the shape its *own default context*
//! uses (`aeron:udp?endpoint=localhost:0`, `…_persistent_subscription_test.cpp:718`),
//! and it is the one that exercises `AWAIT_REPLAY_CHANNEL_ENDPOINT`: the driver
//! picks the port and the archive has to be told which one.
//!
//! # What none of these cells judge: the alignment itself, measured
//!
//! ★ `ATTEMPT_SWITCH` has two paths and these cells take the **short one**. The
//! live subscription joins a stream that is *idle* at that moment, so the live
//! image's position and the replay's are equal and the switch is immediate
//! (`:2072-2074`); the two catch-up handlers that align an **ahead** live stream
//! (`:2002-2058`) are never entered. Tried the other way round — publishing the
//! live batch after the subscription has reached the switch — and the image
//! still joins at the position the replay has read to, because an image's
//! position is where it has been *read*, and a live image's starts at its join
//! position.
//!
//! A mutation of the live probe's `ABORT` (consume instead of leave it)
//! **survives** every cell here, which is the same fact from the other side.
//! What reaches the alignment is a live stream that is already ahead when the
//! subscription joins it, and the reference's cases for that are in its
//! *resilience* binary (`shouldCatchUpToLiveDuringAttemptSwitchWith…`) — P2-C4's
//! eighth commit, with the loss generators.
//!
//! **Unicast, multicast and multi-destination live streams are not here.** They
//! are the same state machine over media this build's own tests cover elsewhere,
//! and reproducing them would need the recording, the live publication and the
//! live subscription on three more media — which is a test of the *driver*, not
//! of this machine. Recorded rather than pretended.
//!
//! # ★ And what one cell now judges that the reference's own case does not: the release
//!
//! Being live is the **end of the replay**, and the reference says what that
//! means where the machine finds out it is live (`:2123-2131`): the archive is
//! told to **stop** the replay (`clean_up_replay`) and the subscription it was
//! read through is **given back** (`clean_up_replay_subscription`) — both before
//! the application is told it is live. This build did neither, and measured: the
//! replay channel's receive endpoint stayed in the driver's counters from the
//! switch to the end of the run.
//!
//! The driver is what says the subscription went back: a **UDP** receive endpoint
//! has a `rcv-channel` counter (type 7) that is given back when the endpoint is
//! (`media/receive_endpoint.rs`), where an **IPC** subscription has none at all
//! (`crates/driver/src/ipc_subscriptions.rs:630` reports
//! `CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED`, as the reference does). So the
//! dynamic-port cell — the only UDP one — asserts that the counter for its
//! channel is **gone** once the machine is live, bounded because the endpoint
//! goes back only after the image it carried has lingered out (measured at
//! 2.6–3.1 s). That is also why this channel carries an alias
//! (`|alias=ps-live-replay`): the counter's label is the channel as the client
//! sent it, and an alias is what makes it *this* channel's counter.
//!
//! ★ Two ways this assertion was wrong before it was right, both measured:
//! **waiting without driving the machine** let its own client go stale, and the
//! driver then reaped it — which takes its subscriptions back and satisfies the
//! wait for the wrong reason, at about ten seconds (the helper drives the
//! subscription every turn for that reason); and the mutation that removes the
//! two cleanups leaves the counter standing until the deadline, which is the
//! criterion doing its job.

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
use deepmsg_driver::position::channel_type_id;
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// The stream that is **recorded and then live**: the subscription's live stream
/// id has to be this one, because a descriptor whose stream differs from the
/// configured live stream is refused (`:1204`).
const LIVE_CHANNEL: &str = "aeron:ipc?alias=ps-live";
const LIVE_STREAM_ID: i32 = 4218;

/// Where the replay arrives. Its own stream, and *not* the live one.
const REPLAY_CHANNEL_IPC: &str = "aeron:ipc?alias=ps-live-replay";
/// The dynamic-port replay channel, with an **alias**: the shape is the
/// reference's own (`endpoint=localhost:0`, which is what asks the driver for a
/// port), and the alias is what the driver's `rcv-channel` counter for the
/// subscription is labelled with — the one channel in this file a counter can be
/// asked about, and the reason it is the one the release is judged on.
const REPLAY_CHANNEL_DYNAMIC_PORT: &str = "aeron:udp?endpoint=localhost:0|alias=ps-live-replay";

/// The alias in [`REPLAY_CHANNEL_DYNAMIC_PORT`], which is what the label carries.
const REPLAY_ALIAS_DYNAMIC_PORT: &str = "ps-live-replay";
const REPLAY_STREAM_ID: i32 = 4219;

/// What the recording holds before the subscription exists, and what is
/// published to it afterwards.
const REPLAYED: [&[u8]; 5] = [
    b"replayed 0",
    b"replayed 1",
    b"replayed 2",
    b"replayed 3",
    b"replayed 4",
];
const LIVE: [&[u8]; 5] = [b"live 0", b"live 1", b"live 2", b"live 3", b"live 4"];

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
    live_left: usize,
    errors: Vec<String>,
}

impl SubscriptionListener for Joins {
    fn on_live_joined(&mut self) {
        self.live_joined += 1;
    }

    fn on_live_left(&mut self) {
        self.live_left += 1;
    }

    fn on_error(&mut self, error: &SubscriptionError) {
        self.errors.push(format!("{error}"));
    }
}

/// The same listener, shared with the test that owns it.
struct SharedJoins(std::rc::Rc<std::cell::RefCell<Joins>>);

impl SubscriptionListener for SharedJoins {
    fn on_live_joined(&mut self) {
        self.0.borrow_mut().live_joined += 1;
    }

    fn on_live_left(&mut self) {
        self.0.borrow_mut().live_left += 1;
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
    /// Start a recording, publish `REPLAYED` into it, and **keep it live** —
    /// which is the reference's own shape: its `PersistentPublication` holds an
    /// exclusive publication that the archive records
    /// (`…_persistent_subscription_test.cpp:225-557`), and the live half of the
    /// case publishes through it.
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

        for payload in REPLAYED {
            source.publish(payload);
        }

        let row = source.await_row();

        source.recording_id = row;

        source
    }

    /// Offer one message to the live stream, and let the driver turn.
    fn publish(&mut self, payload: &[u8]) {
        self.client
            .offer_exclusive(self.publication, payload)
            .expect("the live stream takes a message");

        self.client.poll();
    }

    /// **Take the publication away**: the publisher stops, and the live stream
    /// the subscription is reading is over.
    fn stop(&mut self) {
        self.client
            .remove_publication(self.publication, archive::COMMAND_TIMEOUT)
            .expect("the live publication is given back");
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

/// Run the reference's case once, for a `(replay channel, fragment limit)` cell.
fn replay_then_join(test_name: &str, replay_channel: &str, fragment_limit: usize) {
    let Some((media_driver, _archive_dir)) = rig(test_name) else {
        return;
    };

    let mut source = Source::start(media_driver.aeron_dir());

    let mut context = PersistentSubscriptionContext::new();
    context.archive_context = Some(context_for(media_driver.aeron_dir()));
    context.recording_id = source.recording_id;
    context.live_channel = Some(LIVE_CHANNEL.to_owned());
    context.live_stream_id = LIVE_STREAM_ID;
    context.replay_channel = Some(replay_channel.to_owned());
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

    // The recording first, through the same poll the application uses — and the
    // subscription has to say it is replaying while it does.
    while collector.messages.len() < REPLAYED.len() {
        assert!(
            Instant::now() < deadline,
            "{test_name}: {} of {} replayed; state {:?}{}",
            collector.messages.len(),
            REPLAYED.len(),
            subscription.state(),
            reason(&subscription, &joins)
        );

        subscription.controlled_poll(&mut collector, fragment_limit);
        std::thread::sleep(Duration::from_millis(1));
    }

    assert!(
        subscription.is_replaying(),
        "{test_name}: the recording arrived but the subscription is {:?}",
        subscription.state()
    );
    assert_eq!(
        0,
        joins.borrow().live_joined,
        "{test_name}: joined too early"
    );

    // ★ **The witness, for the one channel that has one, before it is used.**
    // A UDP replay channel is a receive endpoint the driver keeps a
    // `rcv-channel` counter for; an IPC one has no counter at all (the file head
    // says why). While the replay is running the subscription is there, and it is
    // there where the assertion below will look — without this, that zero could
    // be a matcher that matches nothing.
    let counted = replay_channel.starts_with("aeron:udp");

    if counted {
        assert_eq!(
            1,
            receive_channels(&source.client, REPLAY_ALIAS_DYNAMIC_PORT),
            "{test_name}: the driver is not holding the replay channel's counter, so \
             the release below would be judged by nothing"
        );
    }

    // And now the live stream: the same publication, still open, on the stream
    // the recording was made from.
    for payload in LIVE {
        source.publish(payload);
    }

    let deadline = Instant::now() + archive::DEADLINE;
    let wanted = REPLAYED.len() + LIVE.len();

    while collector.messages.len() < wanted || !subscription.is_live() {
        assert!(
            Instant::now() < deadline,
            "{test_name}: {} of {wanted} messages, state {:?}{}",
            collector.messages.len(),
            subscription.state(),
            reason(&subscription, &joins)
        );

        assert!(
            !subscription.has_failed(),
            "{test_name}: failed with {} messages in{}",
            collector.messages.len(),
            reason(&subscription, &joins)
        );

        subscription.controlled_poll(&mut collector, fragment_limit);
        std::thread::sleep(Duration::from_millis(1));
    }

    // **All ten, in order, with no seam between the two halves** — which is the
    // whole point of the state machine: the recording's last message is followed
    // by the live stream's first, once each.
    let expected: Vec<Vec<u8>> = REPLAYED
        .iter()
        .chain(LIVE.iter())
        .map(|payload| payload.to_vec())
        .collect();
    assert_eq!(expected, collector.messages, "{test_name}");

    // ★ **The replay is finished with the moment this machine is live** — and
    // the driver says so: the subscription the replay was read through is given
    // back, and the archive is told to stop it (`state.rs`'s `attempt_switch`,
    // the reference's `:2123-2131`). Until that was done, the endpoint stayed in
    // the counters for the rest of the run.
    //
    // Bounded, because the endpoint goes back only after the image it carried has
    // lingered out. Only the UDP channel has an endpoint to ask about.
    if counted {
        await_receive_channels(
            &mut source.client,
            &mut subscription,
            &mut collector,
            REPLAY_ALIAS_DYNAMIC_PORT,
            0,
            test_name,
        );
    }

    assert_eq!(1, joins.borrow().live_joined, "{test_name}");
    assert!(subscription.is_live(), "{test_name}");
    assert!(!subscription.is_replaying(), "{test_name}");
    assert_eq!(State::Live, subscription.state(), "{test_name}");
    assert!(
        joins.borrow().errors.is_empty(),
        "{:?}",
        joins.borrow().errors
    );
}

/// The reference's first fragment limit (`:988-1005`).
#[test]
fn one_fragment_a_turn() {
    replay_then_join("ps-live-1", REPLAY_CHANNEL_IPC, 1);
}

/// Its second, and its default (`aeron_archive_persistent_subscription_poll`'s
/// fragment limit of 10).
#[test]
fn ten_fragments_a_turn() {
    replay_then_join("ps-live-10", REPLAY_CHANNEL_IPC, 10);
}

/// Its third: no limit at all (`INT_MAX` there, which no `usize` can exceed
/// here).
#[test]
fn no_fragment_limit_at_all() {
    replay_then_join("ps-live-max", REPLAY_CHANNEL_IPC, usize::MAX);
}

/// ★ The replay channel whose endpoint is **port zero**, which is the shape the
/// reference's own default context uses and the only one that reaches
/// `AWAIT_REPLAY_CHANNEL_ENDPOINT`: the driver picks the port, and the archive
/// has to be told which one before it can replay anything.
#[test]
fn a_replay_channel_whose_port_the_driver_picks() {
    replay_then_join("ps-live-dynamic-port", REPLAY_CHANNEL_DYNAMIC_PORT, 10);
}

/// **`shouldHandlePublisherStoppingWhileLive`** (`…_persistent_subscription_test.cpp:1722`):
/// the publisher goes away while the subscription is reading it, and the
/// subscription goes **back to replaying** rather than failing — with the
/// messages it already delivered not delivered again.
#[test]
fn the_subscription_goes_back_to_replaying_when_the_publisher_stops() {
    let test_name = "ps-live-publisher-stops";

    let Some((media_driver, _archive_dir)) = rig(test_name) else {
        return;
    };

    let mut source = Source::start(media_driver.aeron_dir());

    let mut context = PersistentSubscriptionContext::new();
    context.archive_context = Some(context_for(media_driver.aeron_dir()));
    context.recording_id = source.recording_id;
    context.live_channel = Some(LIVE_CHANNEL.to_owned());
    context.live_stream_id = LIVE_STREAM_ID;
    context.replay_channel = Some(REPLAY_CHANNEL_IPC.to_owned());
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

    // Replayed, then live — the same run the cells above make.
    while collector.messages.len() < REPLAYED.len() {
        assert!(
            Instant::now() < deadline,
            "{test_name}: the recording never replayed"
        );
        subscription.controlled_poll(&mut collector, 10);
        std::thread::sleep(Duration::from_millis(1));
    }

    for payload in LIVE {
        source.publish(payload);
    }

    let wanted = REPLAYED.len() + LIVE.len();

    while collector.messages.len() < wanted || !subscription.is_live() {
        assert!(
            Instant::now() < deadline,
            "{test_name}: {} of {wanted}, state {:?}{}",
            collector.messages.len(),
            subscription.state(),
            reason(&subscription, &joins)
        );

        subscription.controlled_poll(&mut collector, 10);
        std::thread::sleep(Duration::from_millis(1));
    }

    // **The publisher stops.** The live image goes with it, and what the
    // subscription must do is the reference's `on_live_left`: tell the
    // application, and go back to replaying — which is where the re-listing it
    // does next takes it, because the recording is still there and it is still
    // the thing being followed.
    source.stop();

    let deadline = Instant::now() + archive::DEADLINE;

    while 1 != joins.borrow().live_left {
        assert!(
            Instant::now() < deadline,
            "{test_name}: never left the live stream; state {:?}{}",
            subscription.state(),
            reason(&subscription, &joins)
        );

        assert!(
            !subscription.has_failed(),
            "{test_name}: a publisher stopping is not a failure: {}",
            subscription.failure_reason().unwrap_or("")
        );

        subscription.controlled_poll(&mut collector, 10);
        std::thread::sleep(Duration::from_millis(1));
    }

    // **The edge's own contract, and only that.** The publisher stopping is not
    // a failure the application hears about through `on_error`, the live stream
    // is left once, and nothing already delivered comes back.
    //
    // ★ What the machine does *next* is not asserted, and cannot be yet: the
    // position it left at is exactly the recording's stop position, so the
    // reference's next step is `ADD_LIVE_SUBSCRIPTION` — look for a live stream
    // again — and that state, with `AWAIT_LIVE` beside it, is the live-start
    // path this slice has not built. Whichever commit builds it is the one that
    // can assert the recovery; what this one can assert is that the way there is
    // taken, by the right state, without a re-delivery.
    assert_eq!(wanted, collector.messages.len(), "{:?}", collector.messages);
    assert!(!subscription.is_live(), "{test_name}");

    let heard = joins.borrow();
    assert!(heard.errors.is_empty(), "{:?}", heard.errors);
    assert_eq!(1, heard.live_left, "{test_name}");
    assert_eq!(1, heard.live_joined, "{test_name}");
    drop(heard);

    match subscription.state() {
        State::SendListRecordingRequest | State::AwaitListRecordingResponse | State::Failed => {}
        other => panic!("{test_name}: left the live stream into {other:?}"),
    }
}

/// **`shouldStartFromLiveWithNoInitialReplayIfRequested`**
/// (`…_persistent_subscription_test.cpp:1101`): a subscription whose start
/// position is the live one **replays nothing** — the messages already recorded
/// are not for it — and joins the live stream directly.
///
/// This is the *other* way into the live stream, and the only one that goes
/// through `ADD_LIVE_SUBSCRIPTION` and `AWAIT_LIVE`: there is nothing to replay,
/// so there is no replay to catch up with and no switch to align.
#[test]
fn a_subscription_can_start_from_live_with_nothing_to_replay() {
    let test_name = "ps-live-from-live";

    let Some((media_driver, _archive_dir)) = rig(test_name) else {
        return;
    };

    let mut source = Source::start(media_driver.aeron_dir());

    let mut context = PersistentSubscriptionContext::new();
    context.archive_context = Some(context_for(media_driver.aeron_dir()));
    context.recording_id = source.recording_id;
    context.live_channel = Some(LIVE_CHANNEL.to_owned());
    context.live_stream_id = LIVE_STREAM_ID;
    context.replay_channel = Some(REPLAY_CHANNEL_IPC.to_owned());
    context.replay_stream_id = REPLAY_STREAM_ID;
    // ★ The whole of what is being asked: start where the stream is now.
    context.start_position = deepmsg_archive::client::persistent_subscription::FROM_LIVE;

    let joins = std::rc::Rc::new(std::cell::RefCell::new(Joins::default()));

    let mut subscription = PersistentSubscription::new(
        context,
        Box::new(NoCredentials),
        Some(Box::new(SharedJoins(std::rc::Rc::clone(&joins)))),
    )
    .expect("the subscription is created");

    let mut collector = Collector::default();
    let deadline = Instant::now() + archive::DEADLINE;

    // Joined, without a single replayed message: the recording has five in it
    // and the application has seen none of them.
    while !subscription.is_live() {
        assert!(
            Instant::now() < deadline,
            "{test_name}: never joined the live stream; state {:?}{}",
            subscription.state(),
            reason(&subscription, &joins)
        );

        assert!(
            !subscription.has_failed(),
            "{test_name}: {}",
            subscription.failure_reason().unwrap_or("")
        );

        subscription.controlled_poll(&mut collector, 10);
        std::thread::sleep(Duration::from_millis(1));
    }

    assert!(
        collector.messages.is_empty(),
        "starting from live replays nothing: {:?}",
        collector.messages
    );

    // And the live stream is the subscription's from here.
    for payload in LIVE {
        source.publish(payload);
    }

    let deadline = Instant::now() + archive::DEADLINE;

    while collector.messages.len() < LIVE.len() {
        assert!(
            Instant::now() < deadline,
            "{test_name}: {} of {} live messages; state {:?}{}",
            collector.messages.len(),
            LIVE.len(),
            subscription.state(),
            reason(&subscription, &joins)
        );

        subscription.controlled_poll(&mut collector, 10);
        std::thread::sleep(Duration::from_millis(1));
    }

    let expected: Vec<Vec<u8>> = LIVE.iter().map(|payload| payload.to_vec()).collect();
    assert_eq!(expected, collector.messages, "{test_name}");
    assert_eq!(1, joins.borrow().live_joined, "{test_name}");
    assert!(
        joins.borrow().errors.is_empty(),
        "{:?}",
        joins.borrow().errors
    );
}

/// Why the subscription is where it is, for a failure message.
/// How many `rcv-channel` counters the driver is holding for the endpoint an
/// alias names.
///
/// The label is `"<name>: <the channel the client sent> <address>"`
/// (`crates/driver/src/media/receive_endpoint.rs:618-625`), so an alias survives
/// into it.
fn receive_channels(client: &Client, alias: &str) -> usize {
    let region = client.counters_reader().expect("a counters region");
    let mut count = 0;

    let _ = region.for_each(|descriptor| {
        if channel_type_id::RECEIVE_CHANNEL_STATUS == descriptor.type_id
            && descriptor.label.contains(alias)
        {
            count += 1;
        }
    });

    count
}

/// Wait, bounded, for the driver to be holding `expected` counters for an alias.
///
/// ★ **Waited for, never read once**: an endpoint is given back only after the
/// image it carried is done, which is a linger — measured between 2.6 s and
/// 3.1 s on this machine.
fn await_receive_channels(
    client: &mut Client,
    subscription: &mut PersistentSubscription,
    collector: &mut Collector,
    alias: &str,
    expected: usize,
    what: &str,
) {
    let deadline = Instant::now() + archive::DEADLINE;
    let mut held = receive_channels(client, alias);

    while held != expected {
        assert!(
            Instant::now() < deadline,
            "{what}: the driver is still holding {held} rcv-channel counter(s) for \
             {alias}, not {expected}"
        );

        // ★ **The machine has to keep being driven.** Its own client goes stale
        // otherwise, and a driver that reaps a stale client takes that client's
        // subscriptions back — which would satisfy this wait for the wrong
        // reason, at about ten seconds.
        subscription.controlled_poll(collector, 10);
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
        held = receive_channels(client, alias);
    }
}

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
