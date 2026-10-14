//! P2-C4: the archive's **async client**, against a real archive.
//!
//! # These are the reference's cases
//!
//! `testAeronArchiveAsyncClient` (`…_async_client_test.cpp:501`) and
//! `shouldAllowToStopReplay` (`:630`), ported. The two things that make this
//! client different from the synchronous one are what the criteria are about:
//!
//! * **answers arrive as callbacks**, so every assertion here is made against
//!   what a [`Listener`] was handed rather than against a return value;
//! * **it heals**, so the second test kills the archive and starts another, and
//!   then asserts the client reconnected *and still works* — which is the part a
//!   client that kept its driver connection could not do.
//!
//! # What the reference asserts that this does not, and why
//!
//! * The replay's **data** is read back through an image
//!   (`…_async_client_test.cpp:611-627`). That is the archive replaying, which
//!   P2-S4 and P2-C2 already judge end to end; what belongs to this client is
//!   that the request goes out and `OK` comes back with the replay session id in
//!   it, which is asserted here.
//! * `shouldCloseItselfIfErrorIsTerminal` (`:696`) needs an archive that sends a
//!   message this client cannot read. Nothing in the archive's own protocol does
//!   that, so the reference reaches for a seam this build does not have yet;
//!   see the commit that lands this file.

use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant};

use deepmsg_archive::client::async_client::{
    AsyncArchiveClient, AsyncClientError, ControlResponse, Listener,
};
use deepmsg_archive::client::context::{
    AERON_DIR_ENV, CONTROL_CHANNEL_ENV, CONTROL_RESPONSE_CHANNEL_ENV,
};
use deepmsg_archive::client::{Archive, ArchiveContext, Handlers, NoCredentials};
use deepmsg_client::client::Client;
use deepmsg_codec::archive::control_response_code::ControlResponseCode;
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// `ArchiveException.UNKNOWN_RECORDING` (`client/ArchiveException.java:54`) as
/// the **client** sees it: the archive's own `5`, which
/// `archive_to_client_error_code` puts in the client's range. Asserted rather
/// than assumed, in the test that needs it.
const UNKNOWN_RECORDING: i64 = 205;

/// The archive's own code, before the mapping.
const ARCHIVE_UNKNOWN_RECORDING: i64 = 5;

/// Where the recorded stream goes. Its own stream, so nothing else in these
/// tests is on it.
const RECORDING_CHANNEL: &str = "aeron:ipc?alias=async-client";
const RECORDING_STREAM_ID: i32 = 4214;

/// What the recorded stream is replayed onto.
const REPLAY_CHANNEL: &str = "aeron:ipc?alias=async-client-replay";
const REPLAY_STREAM_ID: i32 = 4215;

/// What the listener saw, shared with the test that owns it.
///
/// An `Rc<RefCell<_>>` rather than the `static Mutex` the synchronous tests use:
/// that pattern exists because those callbacks are **plain function pointers**
/// with nowhere to put state, and this listener is a trait object with
/// `&mut self`, so it can carry its own.
#[derive(Debug, Default)]
struct Seen {
    connected: usize,
    disconnected: usize,
    errors: Vec<String>,
    responses: Vec<(i64, i64, ControlResponseCode, String)>,
    descriptors: Vec<deepmsg_archive::client::RecordingDescriptor>,
}

impl Seen {
    /// The answer to `correlation_id`, if it has arrived.
    fn response(&self, correlation_id: i64) -> Option<&(i64, i64, ControlResponseCode, String)> {
        self.responses
            .iter()
            .find(|response| response.0 == correlation_id)
    }
}

/// The listener: everything into the shared [`Seen`].
struct Recorder(Rc<RefCell<Seen>>);

impl Listener for Recorder {
    fn on_connected(&mut self) {
        self.0.borrow_mut().connected += 1;
    }

    fn on_disconnected(&mut self) {
        self.0.borrow_mut().disconnected += 1;
    }

    fn on_control_response(&mut self, response: &ControlResponse<'_>) {
        self.0.borrow_mut().responses.push((
            response.correlation_id,
            response.relevant_id,
            response.code,
            response.error_message.to_owned(),
        ));
    }

    fn on_recording_descriptor(
        &mut self,
        descriptor: &deepmsg_archive::client::RecordingDescriptor,
    ) {
        self.0.borrow_mut().descriptors.push(descriptor.clone());
    }

    fn on_error(&mut self, error: &AsyncClientError) {
        self.0.borrow_mut().errors.push(format!("{error}"));
    }
}

/// An archive, its directory, and an async client over it.
fn rig(
    test_name: &str,
) -> Option<(
    OwnArchivingMediaDriver,
    TempDir,
    AsyncArchiveClient,
    Rc<RefCell<Seen>>,
)> {
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

    let seen = Rc::new(RefCell::new(Seen::default()));
    let client = AsyncArchiveClient::new(
        context(media_driver.aeron_dir()),
        Box::new(NoCredentials),
        Box::new(Recorder(Rc::clone(&seen))),
    );

    Some((media_driver, archive_dir, client, seen))
}

/// What the client connects with: **which driver**, the archive's control
/// channel, and a response channel to be answered on.
///
/// The driver is named here, and that is the async client's requirement rather
/// than a habit: it makes its own `Client` on every connect out of this context
/// (`…_async_client.c:154-168`), so the context has to know where the driver is.
/// The reference's own test does the same thing one way further up — it hands
/// the archive context an `aeron` it has already built
/// (`…_async_client_test.cpp:510-511`), and this build's equivalent of "an
/// aeron the caller built" is the directory it was built from.
fn context(aeron_dir: &Path) -> ArchiveContext {
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

/// Poll the client until the listener has seen what the test is waiting for.
fn poll_until(
    client: &mut AsyncArchiveClient,
    seen: &Rc<RefCell<Seen>>,
    what: &str,
    done: impl Fn(&Seen) -> bool,
) {
    let deadline = Instant::now() + archive::DEADLINE;

    while !done(&seen.borrow()) {
        assert!(
            Instant::now() < deadline,
            "{what} never happened; errors: {:?}, responses: {:?}",
            seen.borrow().errors,
            seen.borrow().responses
        );

        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// **`testAeronArchiveAsyncClient`** (`:501`): connect, ask, be answered by
/// callback, and stop a replay.
#[test]
fn the_async_client_answers_every_kind_of_request_and_stops_its_replay() {
    let Some((media_driver, _archive_dir, mut client, seen)) = rig("async-client-answers") else {
        return;
    };

    // Nothing is connected before the first poll, and no session has been named
    // (`:522`, `:537`).
    assert!(!client.is_connected());
    assert_eq!(
        deepmsg_archive::client::proxy::NULL_VALUE,
        client.control_session_id()
    );

    poll_until(&mut client, &seen, "is connected", |seen| {
        seen.connected == 1
    });
    assert!(client.is_connected());
    assert_ne!(
        deepmsg_archive::client::proxy::NULL_VALUE,
        client.control_session_id()
    );

    // A recording that does not exist is answered with `RECORDING_UNKNOWN`, and
    // **the relevant id is the recording id** rather than an error code: only an
    // `ERROR` is remapped (`:459-461`), and this is not one (`:543-547`).
    assert!(client.try_send_list_recording_request(1, 10));
    poll_until(&mut client, &seen, "the first answer", |seen| {
        seen.response(1).is_some()
    });

    let (correlation_id, relevant_id, code, message) = seen.borrow().responses[0].clone();
    assert_eq!(1, correlation_id);
    assert_eq!(10, relevant_id);
    assert_eq!(ControlResponseCode::RECORDING_UNKNOWN, code);
    assert_eq!("", message);

    // The same request against a **different** operation is an `ERROR`, and
    // there the relevant id *is* remapped into the client's range (`:459-461`),
    // and the archive's message comes with it.
    assert!(client.try_send_max_recorded_position_request(2, 11));
    poll_until(&mut client, &seen, "the refusal", |seen| {
        seen.response(2).is_some()
    });

    let (_, relevant_id, code, message) = seen
        .borrow()
        .response(2)
        .expect("waited for exactly this")
        .clone();
    assert_eq!(UNKNOWN_RECORDING, relevant_id);
    assert_eq!(
        UNKNOWN_RECORDING,
        deepmsg_archive::client::archive_to_client_error_code(ARCHIVE_UNKNOWN_RECORDING),
        "the code this test names is the one the mapping produces"
    );
    assert_eq!(ControlResponseCode::ERROR, code);
    assert_eq!("unknown recording id: 11", message);

    // A replay of a recording that does not exist is refused the same way.
    assert!(client.try_send_replay_request(
        None,
        3,
        12,
        REPLAY_CHANNEL,
        REPLAY_STREAM_ID,
        &Default::default(),
    ));
    poll_until(&mut client, &seen, "the replay's refusal", |seen| {
        seen.response(3).is_some()
    });

    let (_, relevant_id, code, message) = seen
        .borrow()
        .response(3)
        .expect("waited for exactly this")
        .clone();
    assert_eq!(UNKNOWN_RECORDING, relevant_id);
    assert_eq!(ControlResponseCode::ERROR, code);
    assert_eq!("unknown recording id: 12", message);

    // Now record something, with a client of its own — the reference's own test
    // does the same (`:566-567`), and a second control session on the same
    // channel is what an archive is for.
    let (recording_id, stop_position) = record(media_driver.aeron_dir());

    // A recording that exists is answered with a **recording descriptor**, on
    // the same subscription as the control responses (`:569-576`).
    assert!(client.try_send_list_recording_request(4, recording_id));
    poll_until(&mut client, &seen, "the descriptor", |seen| {
        !seen.descriptors.is_empty()
    });

    let descriptor = seen.borrow().descriptors[0].clone();
    assert_eq!(4, descriptor.correlation_id);
    assert_eq!(recording_id, descriptor.recording_id);
    assert_eq!(0, descriptor.start_position);
    assert_eq!(stop_position, descriptor.stop_position);
    assert_eq!(RECORDING_STREAM_ID, descriptor.stream_id);

    // And the position of one that exists is an `OK` with the position in it.
    assert!(client.try_send_max_recorded_position_request(5, recording_id));
    poll_until(&mut client, &seen, "the position", |seen| {
        seen.response(5).is_some()
    });

    let (_, relevant_id, code, _) = seen
        .borrow()
        .response(5)
        .expect("waited for exactly this")
        .clone();
    assert_eq!(ControlResponseCode::OK, code);
    assert_eq!(stop_position, relevant_id);

    // A replay of it starts, and the answer carries the replay session id.
    assert!(client.try_send_replay_request(
        None,
        6,
        recording_id,
        REPLAY_CHANNEL,
        REPLAY_STREAM_ID,
        &Default::default(),
    ));
    poll_until(&mut client, &seen, "the replay", |seen| {
        seen.response(6).is_some()
    });

    let (_, replay_session_id, code, _) = seen
        .borrow()
        .response(6)
        .expect("waited for exactly this")
        .clone();
    assert_eq!(ControlResponseCode::OK, code);
    assert!(replay_session_id > 0, "a replay session id");

    // **`shouldAllowToStopReplay`** (`:630`): the replay goes away again on the
    // same client, which is what makes this client usable for more than one
    // thing at a time.
    assert!(client.try_send_stop_replay_request(7, replay_session_id));
    poll_until(&mut client, &seen, "the stopped replay", |seen| {
        seen.response(7).is_some()
    });

    let (_, _, code, _) = seen
        .borrow()
        .response(7)
        .expect("waited for exactly this")
        .clone();
    assert_eq!(ControlResponseCode::OK, code);

    assert!(
        seen.borrow().errors.is_empty(),
        "{:?}",
        seen.borrow().errors
    );
}

/// **The healing** (`…_async_client_test.cpp:583-604`): the archive is killed
/// and another one started over the same directory, and the client reconnects
/// and works.
///
/// This is the criterion that a client which kept its driver connection could
/// not pass: the driver that dies is the one this client was mapped onto.
#[test]
fn the_async_client_reconnects_after_the_archive_is_restarted() {
    let Some((mut media_driver, archive_dir, mut client, seen)) = rig("async-client-reconnect")
    else {
        return;
    };

    poll_until(&mut client, &seen, "is connected", |seen| {
        seen.connected == 1
    });
    let first_session = client.control_session_id();

    // Recorded **before** the restart, so that the request after it has an
    // answer that only a surviving archive could give.
    let (recording_id, stop_position) = record(media_driver.aeron_dir());

    // Kill it, and wait to be told.
    media_driver.stop().expect("the driver stops");
    poll_until(&mut client, &seen, "is disconnected", |seen| {
        seen.disconnected == 1
    });

    assert!(!client.is_connected());
    assert_eq!(
        deepmsg_archive::client::proxy::NULL_VALUE,
        client.control_session_id()
    );

    // A request on a client with no connection is refused rather than sent
    // (`:288-291` and the four like it).
    assert!(!client.try_send_max_recorded_position_request(8, recording_id));

    // Another archive, over the same recordings: our archive keeps its
    // directory unless it is told not to (`aeron.archive.dir.delete.on.start`,
    // default false), which is the reference's `deleteDirOnTearDown(false)`
    // turned the other way round.
    let properties: Vec<&str> = archive::PROPERTIES.to_vec();
    let Some(mut media_driver) =
        OwnArchivingMediaDriver::start("async-client-reconnect", archive_dir.path(), &properties)
    else {
        archiving_driver::announce_skip();
        return;
    };

    media_driver
        .await_ready(archive::DEADLINE)
        .expect("the second archiving media driver comes up");

    poll_until(&mut client, &seen, "is reconnected", |seen| {
        seen.connected == 2
    });

    assert!(client.is_connected());
    // The reference asserts only that a session was named (`:600`), and **not**
    // that it differs from the one before: what the id is is the archive's (and
    // the driver's) to choose, and both of these are fresh processes that draw
    // from the same first value. Recording the old one makes the two visible in
    // a failure without asserting something neither implementation promises.
    assert_ne!(
        deepmsg_archive::client::proxy::NULL_VALUE,
        client.control_session_id(),
        "the first session was {first_session}"
    );

    // And it is not merely connected: the recording that was made before the
    // restart is still there and still answered.
    assert!(client.try_send_max_recorded_position_request(9, recording_id));
    poll_until(
        &mut client,
        &seen,
        "the position after the restart",
        |seen| seen.response(9).is_some(),
    );

    let (_, relevant_id, code, _) = seen
        .borrow()
        .response(9)
        .expect("waited for exactly this")
        .clone();
    assert_eq!(ControlResponseCode::OK, code);
    assert_eq!(stop_position, relevant_id);

    assert!(
        seen.borrow().errors.is_empty(),
        "{:?}",
        seen.borrow().errors
    );
}

/// Record `RECORDING_CHANNEL`, stop it, and answer the recording id and the
/// position it stopped at — with a client of its own, over the same driver.
///
/// **A client of its own** because the async client under test owns the one it
/// connects with: the reference's own test makes a second synchronous client for
/// exactly this (`…_async_client_test.cpp:566-567`), and an archive serving two
/// control sessions at once is what an archive is for.
fn record(aeron_dir: &std::path::Path) -> (i64, i64) {
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

    // The row appears once the archive has taken the recording, which is when
    // the offers begin landing somewhere a reader can find.
    let mut recording_id = None;

    until(&mut client, "the recording's catalog row", |client| {
        let _ = client.offer_exclusive(publication, b"the async client replayed this");
        recording_id = the_only_recording(&mut archive, client).map(|row| row.recording_id);

        recording_id.is_some()
    });

    let recording_id = recording_id.expect("the loop ends with a row");

    for _ in 0..5 {
        let _ = client.offer_exclusive(publication, b"the async client replayed this");
    }

    archive
        .stop_recording_subscription(&mut client, subscription_id)
        .expect("the archive stops the subscription it opened");

    let mut stop_position = None;

    until(&mut client, "the recording's stop position", |client| {
        stop_position = archive
            .get_stop_position(client, recording_id)
            .ok()
            .filter(|stop| *stop >= 0);

        stop_position.is_some()
    });

    // The publisher goes, for the reason `archive_client_api.rs` writes at
    // length: an IPC subscription matches on stream and session, so one left
    // open on this stream would be feeding whatever reads it next.
    client
        .remove_publication(publication, archive::COMMAND_TIMEOUT)
        .expect("the source publication is given back");

    (
        recording_id,
        stop_position.expect("the loop ends with a position"),
    )
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
