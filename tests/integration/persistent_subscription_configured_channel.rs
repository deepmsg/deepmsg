//! P2-C4: **which channel a persistent subscription's replay comes back on**,
//! and what a replay over a *response channel* has to do first.
//!
//! # What is judged here
//!
//! `shouldReplayOverConfiguredChannel` — the reference's case
//! (`…_persistent_subscription_test.cpp:1997`), which is a table of fourteen:
//! six replay channels of the ordinary shapes, and eight that carry
//! `control-mode=response`. Every cell is the same run:
//!
//! 1. five messages are recorded **before the subscription exists**;
//! 2. the subscription replays them through the application's own handler,
//!    while `is_replaying()`;
//! 3. it catches up with the (idle) live stream and says `is_live()`.
//!
//! # The eight response-channel cells are the point of this file
//!
//! A replay channel that says `control-mode=response` is asked for over a
//! publication the **client** makes on the archive's control request channel
//! (`:1602-1657`), and is addressed by a **replay token** the archive hands out
//! (`:1729-1817`). Those four states, the publication, the proxy over it and the
//! token are what this commit adds; the six ordinary cells are here because the
//! same run has to keep working for them.
//!
//! # Which of the reference's fourteen cells are here, and which are not
//!
//! Three of them:
//!
//! * cells 13 and 14 (`:1993-1994`) — both control channels on IPC, the answer
//!   and the replay over `aeron:ipc?control-mode=response`;
//! * cell 7 (`:1987`) — the archive listening for control requests on the
//!   **fixed** UDP endpoint `localhost:8010`, the client answered on a UDP
//!   response channel of its own, and the replay coming back on a third.
//!
//! The other eleven are the same run over parameters that do not change the
//! path:
//!
//! * cells 1–6 differ from one another only in the **replay channel's** three
//!   ordinary shapes (`endpoint=…:0`, a fixed endpoint, `aeron:ipc`, each with
//!   and without a session id), and the ordinary shapes are judged by
//!   `persistent_subscription_live.rs` and `persistent_subscription_replay.rs`
//!   already;
//! * cells 8–12 are cell 7 with parameters a **response** channel overrides —
//!   `session-id=4`, `endpoint=localhost:5006`, `endpoint=localhost:0` — which
//!   is the same fact the reference's own classification encodes: for a
//!   response channel the endpoint and the session are the archive's to name,
//!   not the caller's (`:1037-1040`).
//!
//! ★ Fixed ports are why cell 7 is one test of its own rather than six: this
//! build's fixture starts one archiving media driver **per test** and `cargo
//! test` runs them in parallel, so two cells sharing `localhost:8010` would
//! fight over it. The reference's own fixture can afford them because gtest runs
//! one cell at a time.
//!
//! # What none of these cells judge
//!
//! The loss-driven cases over a response channel
//! (`shouldReplayOverResponseChannelWithLoss`) need the loss generators, which
//! are the eighth commit's.

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

/// The stream that is recorded: the subscription's live stream has to be this
/// one, because a descriptor whose stream differs from the configured live
/// stream is refused (`:1204`).
const LIVE_CHANNEL: &str = "aeron:ipc?alias=ps-configured-live";
const LIVE_STREAM_ID: i32 = 4228;

/// The reference's own response-channel replay channel for the IPC cells
/// (`…_persistent_subscription_test.cpp:1993-1994`).
///
/// Its stream id is `-11` there too — **negative on purpose**, and the same
/// stream id every one of the reference's response-channel cells uses. Nothing
/// in the protocol says a stream id is positive, and a build that assumed it
/// would fail here rather than in production.
const RESPONSE_REPLAY_CHANNEL: &str = "aeron:ipc?control-mode=response";
const RESPONSE_REPLAY_STREAM_ID: i32 = -11;

/// The reference's IPC cells put **both** control channels on IPC, with the
/// answer coming back over a `control-mode=response` channel
/// (`:1993-1994`).
const CONTROL_REQUEST_IPC: &str = "aeron:ipc";
const CONTROL_RESPONSE_IPC: &str = "aeron:ipc?control-mode=response";

/// The reference's UDP cells (`…_persistent_subscription_test.cpp:53-54`,
/// `:1987-1992`): the archive listens for control requests on a **fixed**
/// endpoint, the client asks to be answered on a response channel of its own,
/// and the replay comes back over a third one.
///
/// Fixed ports are what the reference's own fixture can afford — gtest runs its
/// cells one at a time in one process — and this build's cannot, which is why
/// the cells that use them are one test rather than six.
const CONTROL_REQUEST_UDP: &str = "aeron:udp?endpoint=localhost:8010";
const CONTROL_RESPONSE_UDP: &str = "aeron:udp?control-mode=response|control=localhost:10002";
const UDP_REPLAY_CHANNEL: &str = "aeron:udp?control=localhost:10001|control-mode=response";
const UDP_REPLAY_STREAM_ID: i32 = -11;

/// What the recording holds before the subscription exists.
const REPLAYED: [&[u8]; 5] = [
    b"replayed 0",
    b"replayed 1",
    b"replayed 2",
    b"replayed 3",
    b"replayed 4",
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

/// What the application's listener heard.
#[derive(Default)]
struct Joins {
    live_joined: usize,
    errors: Vec<String>,
}

impl SubscriptionListener for Joins {
    fn on_live_joined(&mut self) {
        self.live_joined += 1;
    }

    fn on_error(&mut self, error: &SubscriptionError) {
        self.errors.push(format!("{error}"));
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
    /// Start a recording, publish [`REPLAYED`] into it, and keep it live.
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

/// The reference's cell 13: the replay comes back over an IPC **response
/// channel**, and the request that asks for it goes out over a publication this
/// client made (`…_persistent_subscription_test.cpp:1993`).
#[test]
fn a_replay_over_an_ipc_response_channel() {
    replay_over_configured_channel(
        "a_replay_over_an_ipc_response_channel",
        RESPONSE_REPLAY_CHANNEL,
        RESPONSE_REPLAY_STREAM_ID,
        CONTROL_REQUEST_IPC,
        CONTROL_RESPONSE_IPC,
    );
}

/// The same, with the replay channel naming a session
/// (`…_persistent_subscription_test.cpp:1994`) — which for a response channel is
/// a parameter the archive's answer overrides, not a channel to subscript to by
/// hand.
#[test]
fn a_replay_over_an_ipc_response_channel_that_names_a_session() {
    replay_over_configured_channel(
        "a_replay_over_an_ipc_response_channel_that_names_a_session",
        "aeron:ipc?control-mode=response|session-id=42",
        RESPONSE_REPLAY_STREAM_ID,
        CONTROL_REQUEST_IPC,
        CONTROL_RESPONSE_IPC,
    );
}

/// The reference's cells 7–12: the same response-channel machinery, with the
/// archive's control channel on UDP and the replay coming back on a UDP
/// response channel (`…_persistent_subscription_test.cpp:1987-1992`).
///
/// One test and not six because the endpoint is fixed: these cells exist to
/// exercise the **transport**, and a fixture that renamed the port per cell
/// would be exercising the same code on six ports.
#[test]
fn a_replay_over_a_udp_response_channel() {
    replay_over_configured_channel_on_udp("a_replay_over_a_udp_response_channel");
}

/// Run the reference's `shouldReplayOverConfiguredChannel` once.
fn replay_over_configured_channel(
    test_name: &str,
    replay_channel: &str,
    replay_stream_id: i32,
    control_request_channel: &str,
    control_response_channel: &str,
) {
    let Some((media_driver, _archive_dir)) = rig(test_name) else {
        return;
    };

    run_cell(
        media_driver,
        replay_channel,
        replay_stream_id,
        control_request_channel,
        control_response_channel,
    );
}

/// The same, with the archive listening for control requests on a fixed UDP
/// endpoint — the reference's cells 7–12.
fn replay_over_configured_channel_on_udp(test_name: &str) {
    let Some((media_driver, _archive_dir)) = rig_on_udp(test_name) else {
        return;
    };

    run_cell(
        media_driver,
        UDP_REPLAY_CHANNEL,
        UDP_REPLAY_STREAM_ID,
        CONTROL_REQUEST_UDP,
        CONTROL_RESPONSE_UDP,
    );
}

/// One cell of the table: record, replay, and catch up with an idle live
/// stream.
fn run_cell(
    media_driver: OwnArchivingMediaDriver,
    replay_channel: &str,
    replay_stream_id: i32,
    control_request_channel: &str,
    control_response_channel: &str,
) {
    let mut source = Source::start(media_driver.aeron_dir());

    let mut context = PersistentSubscriptionContext::new();
    context.archive_context = Some(archive_context(
        media_driver.aeron_dir(),
        control_request_channel,
        control_response_channel,
    ));
    context.recording_id = source.recording_id;
    context.live_channel = Some(LIVE_CHANNEL.to_owned());
    context.live_stream_id = LIVE_STREAM_ID;
    context.replay_channel = Some(replay_channel.to_owned());
    context.replay_stream_id = replay_stream_id;
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

    // The recording first, through the same poll the application uses.
    while collector.messages.len() < REPLAYED.len() {
        assert!(
            Instant::now() < deadline,
            "the replay never got going: {} message(s) after {:?}{}",
            collector.messages.len(),
            archive::DEADLINE,
            reason(&subscription, &joins),
        );

        subscription.controlled_poll(&mut collector, 1);
        source.client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }

    assert!(
        subscription.is_replaying(),
        "the subscription should still be replaying while it delivers the \
         recording, but it is {:?}{}",
        subscription.state(),
        reason(&subscription, &joins),
    );

    // Then up to the live stream, which is idle: there is nothing left to
    // replay, so this is the catch-up gate and not the alignment.
    while !subscription.is_live() {
        assert!(
            Instant::now() < deadline,
            "the subscription never became live: state {:?}{}",
            subscription.state(),
            reason(&subscription, &joins),
        );

        subscription.controlled_poll(&mut collector, 10);
        std::thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        REPLAYED.map(<[u8]>::to_vec).to_vec(),
        collector.messages,
        "the recording did not arrive whole and in order{}",
        reason(&subscription, &joins),
    );

    assert_eq!(
        1,
        joins.borrow().live_joined,
        "the application should have been told once that the live stream was \
         joined{}",
        reason(&subscription, &joins),
    );

    assert!(
        joins.borrow().errors.is_empty(),
        "the application heard about errors it should not have: {:?}",
        joins.borrow().errors,
    );
}

/// The same listener, shared with the test that owns it.
struct SharedJoins(std::rc::Rc<std::cell::RefCell<Joins>>);

impl SubscriptionListener for SharedJoins {
    fn on_live_joined(&mut self) {
        self.0.borrow_mut().live_joined += 1;
    }

    fn on_error(&mut self, error: &SubscriptionError) {
        self.0.borrow_mut().errors.push(format!("{error}"));
    }
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
    let properties: Vec<&str> = archive::PROPERTIES.to_vec();

    start(test_name, &properties)
}

/// The same, with the archive's control channel on the reference's fixed
/// endpoint instead of the fixture's driver-picked one.
fn rig_on_udp(test_name: &str) -> Option<(OwnArchivingMediaDriver, TempDir)> {
    let control = format!("-Daeron.archive.control.channel={CONTROL_REQUEST_UDP}");
    let mut properties: Vec<&str> = archive::PROPERTIES
        .iter()
        .copied()
        .filter(|property| !property.starts_with("-Daeron.archive.control.channel="))
        .collect();

    properties.push(control.as_str());

    start(test_name, &properties)
}

fn start(test_name: &str, properties: &[&str]) -> Option<(OwnArchivingMediaDriver, TempDir)> {
    let archive_dir = TempDir::new(test_name);

    let Some(mut media_driver) =
        OwnArchivingMediaDriver::start(test_name, archive_dir.path(), properties)
    else {
        archiving_driver::announce_skip();
        return None;
    };

    media_driver
        .await_ready(archive::DEADLINE)
        .expect("the archiving media driver comes up");

    Some((media_driver, archive_dir))
}

/// The archive's control channels, as the cell asks for them.
fn archive_context(
    aeron_dir: &std::path::Path,
    control_request_channel: &str,
    control_response_channel: &str,
) -> ArchiveContext {
    ArchiveContext::resolve(&[
        (AERON_DIR_ENV.to_owned(), aeron_dir.display().to_string()),
        (
            CONTROL_CHANNEL_ENV.to_owned(),
            control_request_channel.to_owned(),
        ),
        (
            CONTROL_RESPONSE_CHANNEL_ENV.to_owned(),
            control_response_channel.to_owned(),
        ),
    ])
}

/// The context the fixture's own recording is made over.
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
