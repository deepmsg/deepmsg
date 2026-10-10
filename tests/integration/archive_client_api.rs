//! `Archive`: the object a caller holds, driven against a real archive.
//!
//! What is worth testing here is not any one request but the **waiting** — and
//! two claims in particular, neither of which is visible from a single call:
//!
//! * **Recording signals arrive, and are dispatched.** The archive sends
//!   lifecycle signals down the same subscription it sends answers on.
//!
//! # What these do not cover, and it was tried
//!
//! The claim worth having — *a signal that lands while an operation is waiting
//! does not end that wait* — is **not** tested here, and that is worth writing
//! down rather than leaving as an impression. Two attempts to falsify it failed
//! to go red: replacing the dispatch-and-continue with an early return, and
//! removing the dispatch altogether, both left every case passing. The reason is
//! in this file: the signals are collected by an explicit
//! [`Archive::poll_for_recording_signals`] in the driving loop, so nothing here
//! depends on a wait doing the dispatching.
//!
//! Closing that needs a wait that can be driven **without already knowing a
//! recording id** — the id only ever arrives on a signal, so the loop that waits
//! for the id cannot itself be a response wait for that recording. The listings
//! are the natural driver, and they arrive with the descriptor pollers.
//! * **A refusal is two codes.** The archive says `5`; the caller matches on
//!   `205`. Both have to survive the trip, with the archive's own words.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use deepmsg_archive::client::archive::{Archive, ArchiveError, Handlers, RecordingSignal};
use deepmsg_archive::client::context::{CONTROL_CHANNEL_ENV, CONTROL_RESPONSE_CHANNEL_ENV};
use deepmsg_archive::client::{ArchiveContext, NoCredentials, archive_to_client_error_code};
use deepmsg_client::client::Client;
use deepmsg_codec::archive::recording_signal::RecordingSignal as SignalCode;
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// Where a recording goes. Its own stream, so nothing else in the archive's
/// world is on it.
const RECORDING_CHANNEL: &str = "aeron:ipc?alias=client-api";
const RECORDING_STREAM_ID: i32 = 4213;

/// What the recording-signal handler saw.
///
/// A `static` because the handler is a **plain function pointer** and cannot
/// carry anything: the reference hands its handler a `clientd`, and this build
/// does not. Recorded in `Handlers`'s note beside the type — a handler that
/// needs state uses one of these, which is also why the two tests that set one
/// do not run beside each other.
static SIGNALS: Mutex<Vec<RecordingSignal>> = Mutex::new(Vec::new());

/// The handler itself: put it where the test can read it.
fn collect_signal(signal: &RecordingSignal) {
    SIGNALS
        .lock()
        .expect("no other test is holding the signals")
        .push(*signal);
}

/// The recording id out of the first signal of `kind` for `subscription_id`.
fn signal_for(subscription_id: i64, kind: SignalCode) -> Option<RecordingSignal> {
    SIGNALS
        .lock()
        .expect("no other test is holding the signals")
        .iter()
        .find(|signal| signal.subscription_id == subscription_id && signal.signal == kind)
        .copied()
}

/// Drive a client's own poll until something is true, or give up.
fn until(client: &mut Client, what: &str, mut done: impl FnMut(&mut Client) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        if done(client) {
            return;
        }

        assert!(Instant::now() < deadline, "{what} never happened");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Start an archiving media driver and a client connected to it.
fn rig(test_name: &str) -> Option<(OwnArchivingMediaDriver, TempDir, Client)> {
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

    let client = Client::connect(media_driver.aeron_dir()).expect("connect to our driver");

    Some((media_driver, archive_dir, client))
}

/// The context every test connects with: the archive's own control channel, and
/// a response channel the archive answers on.
fn context() -> ArchiveContext {
    ArchiveContext::resolve(&[
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

/// **A recording, from start to stop, waited for through the four waits.**
///
/// One test rather than four because the signal handler is a `static`: two of
/// these running beside each other would read each other's signals, which is a
/// flake rather than a finding.
///
/// What it exercises, in order: `start_recording` (an `offer_and_wait` that
/// answers the subscription id), the START signal, `get_recording_position` (a
/// second wait, whose answer is the position), and `stop_recording` — after
/// which the STOP signal arrives. **It does not exercise a signal landing
/// mid-wait** — see the module note.
#[test]
fn a_recording_is_started_queried_and_stopped_through_the_waits() {
    let Some((mut media_driver, _archive_dir, mut client)) = rig("archive-client-api-record")
    else {
        return;
    };

    let mut archive = Archive::connect(
        &context(),
        &mut client,
        &mut NoCredentials,
        Handlers {
            recording_signal: Some(collect_signal),
            ..Handlers::default()
        },
    )
    .expect("the archive is connected");

    let subscription_id = archive
        .start_recording(
            &mut client,
            RECORDING_CHANNEL,
            RECORDING_STREAM_ID,
            true,
            false,
        )
        .expect("the archive starts recording");

    // A recording begins when the archive's subscription **sees a publication**
    // — `start_recording` asks for one, it does not make one. So there has to be
    // something on the channel, and until there is, the START signal is not
    // late, it is not due.
    let recording_publication = client
        .add_exclusive_publication(
            RECORDING_CHANNEL,
            RECORDING_STREAM_ID,
            archive::COMMAND_TIMEOUT,
        )
        .expect("the driver takes a publication on the recording channel");

    until(&mut client, "the recording publication linking", |client| {
        client
            .exclusive_publication(recording_publication)
            .and_then(deepmsg_client::publication::ExclusivePublication::is_connected)
            .unwrap_or(false)
    });

    // The recording id is not in the answer: the *signal* carries it, which is
    // why a client that wants to ask about a recording has to be listening.
    until(&mut client, "the START signal", |client| {
        let _ = archive.poll_for_recording_signals(client);

        if signal_for(subscription_id, SignalCode::START).is_some() {
            return true;
        }

        // Keep the recording's publication alive so the archive's image has
        // something to read.
        let _ = client.offer_exclusive(recording_publication, b"a message");

        false
    });

    let started = signal_for(subscription_id, SignalCode::START).expect("just waited for it");
    let recording_id = started.recording_id;
    assert!(
        recording_id >= 0,
        "the archive named a recording: {recording_id}"
    );
    assert_eq!(
        archive.control_session_id(),
        started.control_session_id,
        "and the signal came on this client's session"
    );

    let position = archive
        .get_recording_position(&mut client, recording_id)
        .expect("the archive answers where it has got to");

    assert!(
        position >= 0,
        "a recording's position is a position: {position}"
    );

    archive
        .stop_recording(&mut client, RECORDING_CHANNEL, RECORDING_STREAM_ID)
        .expect("the archive stops recording");

    until(&mut client, "the STOP signal", |client| {
        let _ = archive.poll_for_recording_signals(client);

        signal_for(subscription_id, SignalCode::STOP).is_some()
    });

    let stopped = signal_for(subscription_id, SignalCode::STOP).expect("just waited for it");
    assert_eq!(recording_id, stopped.recording_id, "the same recording");

    archive.close(&client);
    media_driver.stop().expect("the driver stops");
}

/// **A refusal keeps both codes, and the archive's own words.**
///
/// The archive speaks `0..=16` and the client `200..=216`; a caller matches on
/// the second and greps for the first. Asking about a recording that does not
/// exist is the cheapest way to make the archive refuse on purpose.
#[test]
fn a_refusal_carries_the_archives_code_and_the_clients() {
    let Some((mut media_driver, _archive_dir, mut client)) = rig("archive-client-api-refused")
    else {
        return;
    };

    let mut archive = Archive::connect(
        &context(),
        &mut client,
        &mut NoCredentials,
        Handlers::default(),
    )
    .expect("the archive is connected");

    let error = archive
        .get_recording_position(&mut client, 999_999)
        .expect_err("there is no such recording");

    let ArchiveError::Refused {
        archive_error_code,
        message,
        ..
    } = &error
    else {
        panic!("the archive refused, so this is a refusal: {error}");
    };

    assert_eq!(
        5, *archive_error_code,
        "ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING, in the archive's own domain"
    );
    assert_eq!(
        Some(205),
        error.client_error_code(),
        "and the same code where a caller matches on it"
    );
    assert!(
        message.contains("unknown recording id"),
        "with the archive's own reason: {message}"
    );
    assert!(
        error.to_string().contains("errorCode=5"),
        "and the text a C client's errmsg would print: {error}"
    );

    archive.close(&client);
    media_driver.stop().expect("the driver stops");
}

/// The two domains, and the one code that is in neither.
#[test]
fn the_error_code_domains_map_the_way_the_reference_says() {
    // `ARCHIVE_ERROR_CODE_GENERIC` and the first of the sixteen.
    assert_eq!(200, archive_to_client_error_code(0));
    // `ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING`.
    assert_eq!(205, archive_to_client_error_code(5));
    // `ARCHIVE_ERROR_CODE_INVALID_POSITION`, the last of them.
    assert_eq!(216, archive_to_client_error_code(16));

    // One past the last is already a client code, and remapping it would make
    // it mean something else.
    assert_eq!(217, archive_to_client_error_code(217));
    assert_eq!(-1, archive_to_client_error_code(-1));
}
