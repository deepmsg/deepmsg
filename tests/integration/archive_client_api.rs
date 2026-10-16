//! `Archive`: the object a caller holds, driven against a real archive.
//!
//! What is worth testing here is not any one request but the **waiting** — and
//! two claims in particular, neither of which is visible from a single call:
//!
//! * **Recording signals arrive, and are dispatched.** The archive sends
//!   lifecycle signals down the same subscription it sends answers on.
//! * **A refusal is two codes.** The archive says `5`; the caller matches on
//!   `205`. Both have to survive the trip, with the archive's own words.
//!
//! # The claim about signals landing in a wait, and how it is now tested
//!
//! *A signal that lands while an operation is waiting does not end that wait* is
//! the one worth having, and the first version of this file did **not** test it:
//! two attempts to falsify it — replacing the dispatch-and-continue with an
//! early return, and removing the dispatch altogether — both left every case
//! passing, because the signals there were collected by an explicit
//! [`Archive::poll_for_recording_signals`] in the driving loop.
//!
//! [`a_listing_reads_while_a_signal_lands_in_it`] closes that, and the driver is
//! the **listing**: a listing is a wait that needs no recording id, which is
//! what the earlier version lacked — the id only ever arrives on a signal, so a
//! loop waiting for the id cannot itself be a response wait for that recording.
//! Nothing in that case polls for signals; the only way the signals it asserts
//! on can have been dispatched is by a wait doing it. It then makes the same
//! claim about a **response** wait, with the STOP signal in flight.
//!
//! ## What was done to falsify it, and the one arm that did not go red
//!
//! Four source changes, each run against this whole file:
//!
//! * **The listing's poller lets a signal end the listing** (its signal branch
//!   sets `is_dispatch_complete`). Red: the pass the case waits for is one that
//!   took a descriptor **and** dispatched a signal, and a listing a signal had
//!   ended takes no descriptor.
//! * **The listing's poller never dispatches a signal.** Red, by the deadline.
//! * **The response wait never dispatches a signal.** Red at the STOP
//!   assertion — the STOP signal goes out as the archive handles the stop, and
//!   the stop-position query that follows is the wait that has to hand it on.
//! * **The response wait returns on a signal instead of carrying on.** **Not**
//!   red, and the reason is a property of the design rather than a gap in the
//!   case: `wait_for_response` re-checks the poller's correlation id after every
//!   poll, and a `RecordingSignalEvent` carries the correlation id of the
//!   request that **caused** it — for a START, the start-recording request
//!   (`ArchiveConductor.java:2046`) — never the id of a query in flight. So an
//!   early return finds a correlation id that is not its own and polls again.
//!   The dispatch-and-continue is what makes a signal arrive at all; the
//!   correlation check is what makes an early return survivable.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use deepmsg_archive::client::archive::{
    Archive, ArchiveError, Handlers, RecordingSignal, channel_with_session_id,
};
use deepmsg_archive::client::context::{CONTROL_CHANNEL_ENV, CONTROL_RESPONSE_CHANNEL_ENV};
use deepmsg_archive::client::descriptor_poller::RecordingDescriptor;
use deepmsg_archive::client::proxy::{ReplayParams, ReplicationParams};
use deepmsg_archive::client::subscription_descriptor_poller::RecordingSubscriptionDescriptor;
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

/// `Aeron.NULL_POSITION`, which a **live** recording's stop position is — so
/// this is the test that tells a recording still in flight from one that has
/// stopped.
const NULL_POSITION: i64 = -1;

/// The source a replication is asked to copy **from**. Nothing reads these
/// until the session walks, so they are a shape rather than a subject: what the
/// four criteria below are about is the answer, not the copy.
const SRC_RECORDING_ID: i64 = 7;
const SRC_CONTROL_STREAM_ID: i32 = 10;
const SRC_CONTROL_CHANNEL: &str = "aeron:udp?endpoint=localhost:8999";

/// A destination recording id this archive certainly does not hold, for the
/// refusal that asks the **catalog**.
const DST_RECORDING_ID_NOT_HELD: i64 = 123_456;

/// What the **recording** test's signal handler saw.
///
/// A `static` because the handler is a **plain function pointer** and cannot
/// carry anything: the reference hands its handler a `clientd`, and this build
/// does not. Recorded in `Handlers`'s note beside the type — a handler that
/// needs state uses one of these.
///
/// **One sink per test, never a shared one.** The only field that could tell
/// two tests' signals apart is `subscription_id`, and that id is unique
/// *within a driver*, not within this process: two rigs started in the same
/// process have been measured handing out the same id. A shared sink plus a
/// lookup filtered on that id answers one test with the other test's signal —
/// which is measured in this file's history rather than supposed.
static SIGNALS: Mutex<Vec<RecordingSignal>> = Mutex::new(Vec::new());

/// The handler itself: put it where the test can read it.
fn collect_signal(signal: &RecordingSignal) {
    SIGNALS
        .lock()
        .expect("no other test is holding the signals")
        .push(*signal);
}

/// What the **listing** test's signal handler saw.
///
/// A second sink rather than a share of [`SIGNALS`], for the reason written
/// there: a subscription id does not name a test.
static LISTING_SIGNALS: Mutex<Vec<RecordingSignal>> = Mutex::new(Vec::new());

/// The listing test's handler.
fn collect_listing_signal(signal: &RecordingSignal) {
    LISTING_SIGNALS
        .lock()
        .expect("no other test is holding the listing signals")
        .push(*signal);
}

/// What each listing descriptor went into.
///
/// A `static` for the same reason [`SIGNALS`] is one: the consumer is a plain
/// function pointer and cannot carry anything.
static DESCRIPTORS: Mutex<Vec<RecordingDescriptor>> = Mutex::new(Vec::new());

/// The listing consumer.
fn collect_descriptor(descriptor: &RecordingDescriptor) {
    DESCRIPTORS
        .lock()
        .expect("no other test is holding the descriptors")
        .push(descriptor.clone());
}

/// The recording id out of the first signal of `kind` for `subscription_id`,
/// from the **recording** test's sink.
fn signal_for(subscription_id: i64, kind: SignalCode) -> Option<RecordingSignal> {
    SIGNALS
        .lock()
        .expect("no other test is holding the signals")
        .iter()
        .find(|signal| signal.subscription_id == subscription_id && signal.signal == kind)
        .copied()
}

/// The same, from the **listing** test's sink.
fn listing_signal_for(subscription_id: i64, kind: SignalCode) -> Option<RecordingSignal> {
    LISTING_SIGNALS
        .lock()
        .expect("no other test is holding the listing signals")
        .iter()
        .find(|signal| signal.subscription_id == subscription_id && signal.signal == kind)
        .copied()
}

/// What the **narrowed-listing** test's consumer has been handed.
///
/// A sink of its own for the reason written at [`SIGNALS`]: a listing is read by
/// the poller a test owns, but the consumer is a plain function pointer, so two
/// tests sharing one sink would read each other's descriptors.
static NARROWED: Mutex<Vec<RecordingDescriptor>> = Mutex::new(Vec::new());

/// The narrowed-listing test's consumer.
fn collect_narrowed(descriptor: &RecordingDescriptor) {
    NARROWED
        .lock()
        .expect("no other test is holding the narrowed descriptors")
        .push(descriptor.clone());
}

/// What the **narrowed-listing** test's subscription consumer has been handed.
static SUBSCRIPTIONS: Mutex<Vec<RecordingSubscriptionDescriptor>> = Mutex::new(Vec::new());

/// The narrowed-listing test's subscription consumer.
fn collect_subscription(descriptor: &RecordingSubscriptionDescriptor) {
    SUBSCRIPTIONS
        .lock()
        .expect("no other test is holding the subscriptions")
        .push(descriptor.clone());
}

/// What the **reshape** test's signal handler saw.
///
/// A sink of its own for the reason written at [`SIGNALS`], and the one this
/// file learned the hard way: two tests sharing a signal sink **did** answer one
/// with the other's signal, because a subscription id is unique within a driver
/// and not within this process.
static RESHAPE_SIGNALS: Mutex<Vec<RecordingSignal>> = Mutex::new(Vec::new());

/// The reshape test's handler.
fn collect_reshape_signal(signal: &RecordingSignal) {
    RESHAPE_SIGNALS
        .lock()
        .expect("no other test is holding the reshape signals")
        .push(*signal);
}

/// The recording id out of the first signal of `kind`, from that sink.
fn reshape_signal_for(subscription_id: i64, kind: SignalCode) -> Option<RecordingSignal> {
    RESHAPE_SIGNALS
        .lock()
        .expect("no other test is holding the reshape signals")
        .iter()
        .find(|signal| signal.subscription_id == subscription_id && signal.signal == kind)
        .copied()
}

/// What the **recorded-publication** test's signal handler saw.
///
/// Its own sink, for the reason written at [`SIGNALS`] — and it needs a lookup
/// that does not name the subscription, because the subscription id here comes
/// from `add_recorded_publication`, which does not answer with it.
static RECORDED_SIGNALS: Mutex<Vec<RecordingSignal>> = Mutex::new(Vec::new());

/// The recorded-publication test's handler.
fn collect_recorded_signal(signal: &RecordingSignal) {
    RECORDED_SIGNALS
        .lock()
        .expect("no other test is holding the recorded signals")
        .push(*signal);
}

/// The first signal of `kind` this test's handler saw, whoever it was for.
fn recorded_signal(kind: SignalCode) -> Option<RecordingSignal> {
    RECORDED_SIGNALS
        .lock()
        .expect("no other test is holding the recorded signals")
        .iter()
        .find(|signal| signal.signal == kind)
        .copied()
}

/// Empty the narrowed-listing test's sinks, which only that test reads.
fn forget_narrowed() {
    NARROWED
        .lock()
        .expect("no other test is holding the narrowed descriptors")
        .clear();
    SUBSCRIPTIONS
        .lock()
        .expect("no other test is holding the subscriptions")
        .clear();
}

/// How many descriptors the listing test's consumer has been handed.
fn descriptors_seen() -> usize {
    DESCRIPTORS
        .lock()
        .expect("no other test is holding the descriptors")
        .len()
}

/// How many signals the listing test's handler has been handed.
fn listing_signals_seen() -> usize {
    LISTING_SIGNALS
        .lock()
        .expect("no other test is holding the listing signals")
        .len()
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

/// The `EXTEND` an archive sent about `recording_id`, waited for by its signal.
///
/// **The signal is what a client can see of an extend; the subscription it makes
/// is not.** A replication's recording subscription is `autoStop`
/// (`ReplicationSession.java:544` passes `true`), so it is taken off the
/// registry the moment the copy ends (`ArchiveConductor.java:1355`) — and for a
/// source that has stopped that is **shorter than one client round trip**:
/// waiting on the *listing* was measured here as a criterion that failed one run
/// in three under load, and then, once it was triggered by this signal instead,
/// as one that failed **every** time with "the subscription it made was already
/// gone" (`analysis/archive-client-api-flake.md` §B has the whole chase).
///
/// So what is asserted is the signal — which stays, and which is the archive's
/// own statement that the extend happened, with the subscription's id and where
/// the copy starts from. The **channel** the extend was made on is the session's
/// own construction and is judged where it is built
/// (`crate::server::replication_session`'s `extend_channel`), not through a
/// window a client cannot look into.
fn the_extensions_signal(
    archive: &mut Archive,
    client: &mut Client,
    signals: &Mutex<Vec<RecordingSignal>>,
    recording_id: i64,
) -> RecordingSignal {
    let mut extended = None;

    until(client, "the extend's signal", |client| {
        let _ = archive.poll_for_recording_signals(client);

        extended = signals
            .lock()
            .expect("no other test is holding these signals")
            .iter()
            .find(|signal| {
                signal.signal == SignalCode::EXTEND && signal.recording_id == recording_id
            })
            .copied();

        extended.is_some()
    });

    extended.expect("the loop ends with the extend's signal")
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
/// One test rather than four because the signal handler is a `static`, and a
/// sink two tests share answers one of them with the other's signal — see
/// [`SIGNALS`]. This test owns that sink; the listing test owns
/// [`LISTING_SIGNALS`], so the two need not be kept apart.
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

    // The four questions about a recording, asked while it is **in flight**.
    let started_at = archive
        .get_start_position(&mut client, recording_id)
        .expect("the archive answers where it starts");

    let position = archive
        .get_recording_position(&mut client, recording_id)
        .expect("the archive answers where it has got to");

    let max_recorded = archive
        .get_max_recorded_position(&mut client, recording_id)
        .expect("the archive answers how far it got");

    assert!(
        started_at >= 0,
        "a recording's start position is a position: {started_at}"
    );
    assert!(
        position >= 0,
        "a recording's position is a position: {position}"
    );
    assert!(
        position >= started_at,
        "a recording has got at least as far as it started: {position} < {started_at}"
    );
    // Both read the recording session's position counter while there is one, and
    // nothing is appending between the two calls: the reference has them the same
    // number for a recording in flight (`ArchiveConductor.java:1167-1195`).
    assert_eq!(
        max_recorded, position,
        "a recording in flight has got exactly as far as it has got"
    );

    archive
        .stop_recording_channel_and_stream(&mut client, RECORDING_CHANNEL, RECORDING_STREAM_ID)
        .expect("the archive stops recording");

    until(&mut client, "the STOP signal", |client| {
        let _ = archive.poll_for_recording_signals(client);

        signal_for(subscription_id, SignalCode::STOP).is_some()
    });

    let stopped = signal_for(subscription_id, SignalCode::STOP).expect("just waited for it");
    assert_eq!(recording_id, stopped.recording_id, "the same recording");

    // And the same four with the recording **stopped**, which is where they stop
    // agreeing. Two of them read the catalog and two read the recording session,
    // and a recording that is not in flight has no session — so the pair that
    // read the session answer with `NULL_POSITION` and the fallback.
    let stopped_at = archive
        .get_stop_position(&mut client, recording_id)
        .expect("the archive answers where it stopped");

    assert!(
        stopped_at >= position,
        "a recording stops at or after where it was last seen: {stopped_at} < {position}"
    );
    assert_eq!(
        stopped_at,
        archive
            .get_max_recorded_position(&mut client, recording_id)
            .expect("the archive answers how far it got"),
        "with nothing recording, how far it got is where it stopped"
    );
    assert_eq!(
        -1,
        archive
            .get_recording_position(&mut client, recording_id)
            .expect("the archive answers, and its answer is that there is no position"),
        "a recording that is not in flight has no live position, which is an answer and not a refusal"
    );
    assert_eq!(
        started_at,
        archive
            .get_start_position(&mut client, recording_id)
            .expect("the archive answers where it starts"),
        "and the start position is the one of the four that stopping does not move"
    );

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

/// **A caller can send a request itself, and read the answer by the id it drew.**
///
/// Every named method on `Archive` offers and then waits (`offer_and_wait`),
/// which is the shape a caller that wants one answer needs — and the wrong shape
/// for a caller that has to stay non-blocking. `Archive::proxy_mut` is the door
/// for that caller: the proxy's encoders write into its reused buffer, the
/// correlation id comes from `Archive::next_correlation_id`, and the answer is
/// read later, keyed on the id the caller drew.
///
/// **The reference's replication session is that caller** — it reaches for
/// `archiveProxy().listRecording`, `getRecordingPosition`, `replay` and
/// `stopReplay` directly rather than the high-level calls
/// (`ReplicationSession.java`), because it runs inside the archive conductor's
/// work cycle and may take only one step per turn. The same door is what the
/// test fixture has always needed: `tests/src/archive.rs`'s `Session::send_only`
/// touches the publication itself for want of one.
///
/// What this asks is that the door is real. The request below is answered with a
/// refusal — there is no such recording — and *that* is the evidence: the
/// refusal comes back carrying the correlation id this caller drew, so the
/// request went out through the proxy and the archive answered **that** id.
#[test]
fn a_caller_can_offer_a_request_itself_and_read_its_answer() {
    let Some((mut media_driver, _archive_dir, mut client)) = rig("archive-client-api-own-request")
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

    let correlation_id = archive
        .next_correlation_id(&mut client)
        .expect("the driver's command ring answers");

    archive
        .proxy_mut()
        .get_recording_position(&client, correlation_id, 999_999)
        .expect("the proxy offers the request");

    let error = archive
        .wait_for_response(&mut client, "a request the caller built", correlation_id)
        .expect_err("there is no recording 999999");

    let ArchiveError::Refused {
        correlation_id: refused,
        archive_error_code,
        ..
    } = &error
    else {
        panic!("the archive refused, so this is a refusal: {error}");
    };

    assert_eq!(
        correlation_id, *refused,
        "the answer is the one to the id this caller drew, not to one the product drew"
    );
    assert_eq!(
        5, *archive_error_code,
        "ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING, in the archive's own domain"
    );
    assert_eq!(Some(205), error.client_error_code());

    archive.close(&client);
    media_driver.stop().expect("the driver stops");
}

/// `shouldMapErrorCodes` (`:4849-4867`) and `shouldNotMapActualErrorCodes`
/// (`:4870-4888`) — **both domains whole, not a sample of them**.
///
/// The reference sweeps all seventeen archive codes and then all seventeen
/// client codes, and the sweep *is* the claim: the two ranges have to meet
/// exactly, one code onto one, with nothing in between remapped twice. A sample
/// cannot catch a wrong constant in a code it never visits, which is why the
/// reference visits every one of them. (Falsified: send code `7` to `208` and
/// this goes red naming `archive code 7` — a sample of `0`, `5` and `16` never
/// looks at it.)
///
/// **The third case of that class — `shouldHaveErrorMessagesForArchiveErrorCodes`
/// (`:4891-4910`) — has no counterpart here, and its absence is on purpose.** It
/// asserts that `aeron_error_code_str` answers with something other than
/// `"unknown error code"` for each of the seventeen client codes, and that table
/// lives in Aeron's *client* (`aeron_archive_client_error.h:8+` defines the
/// codes, `util/aeron_error.c:252+` is the switch) because in C an error **is**
/// an `int`. This build's errors are typed: `ArchiveError::Refused` carries the
/// wire code, the client code and the archive's own text, and printing one says
/// what happened without a lookup (`crates/archive/src/client/archive.rs:259-270`).
/// There is no `error_code_str` anywhere in this workspace for that table to be
/// extended from, so porting the archive half alone would be an API with no
/// caller — if the client's error surface is built, these codes go in with it.
#[test]
fn the_error_code_domains_map_the_way_the_reference_says() {
    // `ARCHIVE_ERROR_CODE_GENERIC` (0) through `ARCHIVE_ERROR_CODE_INVALID_POSITION`
    // (16), onto `AERON_ARCHIVE_ERROR_CODE_GENERIC` (200) through
    // `AERON_ARCHIVE_ERROR_CODE_INVALID_POSITION` (216).
    for archive_code in 0..=16 {
        assert_eq!(
            archive_code + 200,
            archive_to_client_error_code(archive_code),
            "archive code {archive_code}"
        );
    }

    // A client code is left alone — remapping one would make it mean something
    // else. `217` is one past the last of them and `-1` is outside both
    // domains; neither is moved either.
    for client_code in 200..=216 {
        assert_eq!(
            client_code,
            archive_to_client_error_code(client_code),
            "client code {client_code}"
        );
    }

    assert_eq!(217, archive_to_client_error_code(217));
    assert_eq!(-1, archive_to_client_error_code(-1));
}

/// **A listing reads while a signal lands in it, and both survive.**
///
/// The START signal is **not due** when the listing starts: the recording's
/// publication is created after `start_recording` returns, so the archive has no
/// image to record from until it is. Where the two meet is a race the archive
/// decides — the signal and the listing's answer have been measured arriving
/// either way round — so what the loop waits for is not the first descriptor but
/// a **pass that did both**, which is the shape a listing a signal had ended
/// cannot have.
///
/// Nothing here calls `poll_for_recording_signals`, and the client's conductor
/// does not hand fragments to a handler, so the only thing that can have
/// dispatched the signal the assertions find is a listing's own wait.
///
/// Then the same again for a **response** wait, which is a different poller: the
/// recording is stopped, and the stop-position query that follows runs while the
/// STOP signal is in flight. It is the stop position rather than the recording
/// position because the archive's answer for a recording that is not in flight
/// is `NULL_POSITION` — [`Archive::get_recording_position`]'s note has it.
#[test]
fn a_listing_reads_while_a_signal_lands_in_it() {
    let Some((mut media_driver, _archive_dir, mut client)) = rig("archive-client-api-listing")
    else {
        return;
    };

    let mut archive = Archive::connect(
        &context(),
        &mut client,
        &mut NoCredentials,
        Handlers {
            recording_signal: Some(collect_listing_signal),
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

    // Now the publication. The archive's subscription has nothing to read until
    // this exists, so the START signal is not early here — it is not due.
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

    assert_eq!(
        None,
        listing_signal_for(subscription_id, SignalCode::START),
        "and it has not been emitted yet, which is what makes the listing loop a test"
    );

    let _ = client.offer_exclusive(recording_publication, b"a message");

    // The loop is the test's driving loop, and it is also its subject: every
    // pass is a wait, and `client.poll()` is not one.
    //
    // It has to be a loop because **the listing can finish before the signal is
    // sent**. The archive sends the START signal and answers the listing off the
    // same moment's work, and the two have been measured arriving either way
    // round, so a pass that returns the descriptor can be one the signal was
    // never in. The next pass reads it.
    //
    // **The pass that counts is one that did both** — took a descriptor *and*
    // dispatched a signal. That is what makes this a test of the claim rather
    // than of the race: a listing a signal had **ended** could not have carried
    // on to take the descriptor in the same pass, because a listing ends only
    // when the archive says there are no more or when the count is used up, and
    // a signal is neither.
    let deadline = Instant::now() + Duration::from_secs(10);
    let listed = loop {
        let descriptors_before = descriptors_seen();
        let signals_before = listing_signals_seen();

        let listed = archive
            .list_recordings(&mut client, 0, 10, &mut collect_descriptor)
            .expect("the listing completes");

        if descriptors_seen() > descriptors_before && listing_signals_seen() > signals_before {
            break listed;
        }

        assert!(
            Instant::now() < deadline,
            "no listing both dispatched the START signal and returned: listed={listed}, \
             descriptors={}, signals={}",
            descriptors_seen(),
            listing_signals_seen()
        );
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    };

    assert!(listed >= 1, "the recording is listed: {listed}");

    let recording_id = DESCRIPTORS
        .lock()
        .expect("no other test is holding the descriptors")
        .iter()
        .find(|descriptor| descriptor.stream_id == RECORDING_STREAM_ID)
        .map(|descriptor| descriptor.recording_id)
        .expect("the recording this test made");

    assert!(
        listing_signal_for(subscription_id, SignalCode::START).is_some(),
        "the START signal landed while the listing was reading, and the listing \
         dispatched it and carried on — no explicit signal poll ran in this test"
    );

    // The descriptor agrees with the signal about which recording this is —
    // `0`, which is the archive's first id rather than a missing one, and the
    // reason the id is taken from the descriptor here and from the signal in
    // the test above: a caller that lists does not have to be listening.
    assert_eq!(
        Some(recording_id),
        listing_signal_for(subscription_id, SignalCode::START).map(|signal| signal.recording_id),
        "the listing and the signal name the same recording"
    );

    // **And a consumer can be an object.** The same page, asked for again with a
    // closure that keeps its own list, answers into *that* list — the shape the
    // reference's replication session is built on (`ReplicationSession.java:53`
    // **is** its consumer, with its state on `this`) and the one a `fn` pointer
    // could not have. Every listing read in this file used to go through a
    // `static` slot like `DESCRIPTORS` — one slot per criterion, which is a rule
    // nothing enforces and two listings in one process can break.
    let mut mine = Vec::new();
    let listed_again = archive
        .list_recordings(
            &mut client,
            0,
            10,
            &mut |descriptor: &RecordingDescriptor| {
                mine.push(descriptor.recording_id);
            },
        )
        .expect("the listing completes");

    assert_eq!(
        listed, listed_again,
        "the same catalog answers the same page twice"
    );
    assert_eq!(
        vec![recording_id],
        mine,
        "the closure's own list, which the shared slot knows nothing about"
    );

    // And the same claim one layer over: a **response** wait, which is a
    // different poller from the listing's, with a signal in flight. Stopping
    // the recording is what sends the STOP signal, and the query below is the
    // wait that has to read it and carry on rather than answer with it.
    archive
        .stop_recording_channel_and_stream(&mut client, RECORDING_CHANNEL, RECORDING_STREAM_ID)
        .expect("the archive stops recording");

    assert!(
        listing_signal_for(subscription_id, SignalCode::STOP).is_none(),
        "the STOP signal is still in flight, which is what makes the query a test"
    );

    // **Not** `get_recording_position`: that one answers a recording **in
    // flight**, and the archive's answer for a recording that has stopped is
    // `NULL_POSITION`. Where a recording ended is its stop position.
    let stopped_at = archive
        .get_stop_position(&mut client, recording_id)
        .expect("the archive answers where it stopped");

    assert!(
        stopped_at >= 0,
        "a recording's stop position is a position: {stopped_at}"
    );
    assert!(
        listing_signal_for(subscription_id, SignalCode::STOP).is_some(),
        "the STOP signal was dispatched by a wait, not by an explicit poll"
    );

    archive.close(&client);
    media_driver.stop().expect("the driver stops");
}

/// **The listings narrow: by uri, by id, and to the archive's subscriptions.**
///
/// Four requests, all of which end in a listing, and three of them are ways of
/// asking for less than everything: a channel fragment, a single recording id,
/// and the archive's **subscriptions** rather than its recordings — which is a
/// different poller on the same subscription, and a different "there are no
/// more".
#[test]
fn the_listings_narrow_by_uri_by_id_and_to_subscriptions() {
    let Some((mut media_driver, _archive_dir, mut client)) = rig("archive-client-api-narrowed")
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

    let subscription_id = archive
        .start_recording(
            &mut client,
            RECORDING_CHANNEL,
            RECORDING_STREAM_ID,
            true,
            false,
        )
        .expect("the archive starts recording");

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

    let _ = client.offer_exclusive(recording_publication, b"a message");

    // The recording id is only ever in a descriptor or a signal, and this test
    // is about listings — so it lists until there is one. Every narrowed form
    // below is anchored to the id this finds, which is what makes each of them
    // a check on the **narrowing** rather than on the listing.
    let deadline = Instant::now() + Duration::from_secs(10);
    let recording = loop {
        forget_narrowed();

        let listed = archive
            .list_recordings(&mut client, 0, 10, &mut collect_narrowed)
            .expect("the listing completes");

        if listed >= 1 {
            break NARROWED
                .lock()
                .expect("no other test is holding the narrowed descriptors")
                .first()
                .cloned()
                .expect("the listing handed one over");
        }

        assert!(Instant::now() < deadline, "the recording is never listed");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    };

    // **By uri**: the fragment is matched against the channel that was recorded,
    // and `RECORDING_CHANNEL` begins with this one.
    forget_narrowed();
    let by_uri = archive
        .list_recordings_for_uri(
            &mut client,
            0,
            10,
            "aeron:ipc",
            RECORDING_STREAM_ID,
            &mut collect_narrowed,
        )
        .expect("the listing completes");

    assert!(by_uri >= 1, "the recording is on that channel: {by_uri}");
    assert_eq!(
        Some(recording.recording_id),
        NARROWED
            .lock()
            .expect("no other test is holding the narrowed descriptors")
            .first()
            .map(|descriptor| descriptor.recording_id),
        "and the one it found is this recording"
    );

    // A fragment the channel does not contain is an empty listing, not a
    // refusal — the archive answers `OK` with nothing to hand over.
    forget_narrowed();
    let nothing = archive
        .list_recordings_for_uri(
            &mut client,
            0,
            10,
            "aeron:udp",
            RECORDING_STREAM_ID,
            &mut collect_narrowed,
        )
        .expect("the archive answers a listing that holds nothing");

    assert_eq!(0, nothing, "no recording is on that channel");

    // **By id**: a listing that asks for exactly one, so there is nothing to
    // page through and the count it asked in for is not the caller's to choose.
    forget_narrowed();
    let by_id = archive
        .list_recording(&mut client, recording.recording_id, &mut collect_narrowed)
        .expect("the listing completes");

    assert_eq!(1, by_id, "the recording the id names");
    assert_eq!(
        Some(recording.recording_id),
        NARROWED
            .lock()
            .expect("no other test is holding the narrowed descriptors")
            .first()
            .map(|descriptor| descriptor.recording_id),
        "and it is the one that was asked for"
    );

    // An id the archive does not hold is `RECORDING_UNKNOWN` rather than an
    // error: the listing is over and there was nothing in it.
    forget_narrowed();
    let missing = archive
        .list_recording(
            &mut client,
            recording.recording_id + 999,
            &mut collect_narrowed,
        )
        .expect("the archive answers about a recording it does not hold");

    assert_eq!(
        0, missing,
        "and the answer is that there is no such recording"
    );

    // **The subscriptions**: the archive is recording this channel because this
    // client asked it to, so it holds one — and its id is the one
    // `start_recording` answered with.
    forget_narrowed();
    let by_subscription = archive
        .list_recording_subscriptions(
            &mut client,
            0,
            10,
            "",
            Some(RECORDING_STREAM_ID),
            &mut collect_subscription,
        )
        .expect("the listing completes");

    assert!(
        by_subscription >= 1,
        "the archive holds the subscription it made: {by_subscription}"
    );
    assert!(
        SUBSCRIPTIONS
            .lock()
            .expect("no other test is holding the subscriptions")
            .iter()
            .any(|held| held.subscription_id == subscription_id
                && held.stream_id == RECORDING_STREAM_ID),
        "and it is this client's, on this stream: {:?}",
        SUBSCRIPTIONS
            .lock()
            .expect("no other test is holding the subscriptions")
    );

    // And the same listing asking for **exactly** what there is, which is the
    // other of the two ways one of these ends: the count runs out, so there is
    // no "no more" to wait for. Asking for more than there is is what the call
    // above did.
    forget_narrowed();
    let exactly = archive
        .list_recording_subscriptions(
            &mut client,
            0,
            by_subscription,
            "",
            Some(RECORDING_STREAM_ID),
            &mut collect_subscription,
        )
        .expect("the listing completes");

    assert_eq!(
        by_subscription, exactly,
        "asking for exactly what is there gets exactly that"
    );

    // **The newest match**: by session, stream and channel fragment, from a
    // floor. This is the request `replay` is built on.
    let found = archive
        .find_last_matching_recording(
            &mut client,
            0,
            "aeron:ipc",
            RECORDING_STREAM_ID,
            recording.session_id,
        )
        .expect("the archive answers");

    assert_eq!(
        recording.recording_id, found,
        "the newest recording that matches is this one"
    );

    let none = archive
        .find_last_matching_recording(
            &mut client,
            0,
            "aeron:ipc",
            RECORDING_STREAM_ID + 1,
            recording.session_id,
        )
        .expect("a stream nothing is on is still a question the archive answers");

    assert_eq!(
        -1, none,
        "and its \"not found\" is a value rather than a refusal"
    );

    let refused = archive.find_last_matching_recording(
        &mut client,
        -1,
        "aeron:ipc",
        RECORDING_STREAM_ID,
        recording.session_id,
    );

    assert!(
        matches!(refused, Err(ArchiveError::Refused { .. })),
        "a floor below zero is refused, which is the one of these that is: {refused:?}"
    );

    archive.close(&client);
    media_driver.stop().expect("the driver stops");
}

/// **The requests that stop and reshape a recording.**
///
/// The four ways to stop, the two segments questions, and the `try_` forms whose
/// whole point is that "there was nothing to stop" is a `false` rather than an
/// error — driven against a real archive, in the order that leaves each one
/// something to do.
///
/// **Not here, and it is the archive's side rather than this one's**:
/// `extend_recording`, `replicate`, `stop_replication`, `try_stop_replication`
/// and `migrate_segments` have no arm in our archive server, so a round trip for
/// them would be a test of this client's **offer** and of nothing past it —
/// which `tests/integration/archive_proxy.rs` already is, for all 32.
#[test]
fn the_requests_that_stop_and_reshape_a_recording() {
    let Some((mut media_driver, _archive_dir, mut client)) = rig("archive-client-api-reshape")
    else {
        return;
    };

    let mut archive = Archive::connect(
        &context(),
        &mut client,
        &mut NoCredentials,
        Handlers {
            recording_signal: Some(collect_reshape_signal),
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

    until(&mut client, "the START signal", |client| {
        let _ = archive.poll_for_recording_signals(client);

        if reshape_signal_for(subscription_id, SignalCode::START).is_some() {
            return true;
        }

        let _ = client.offer_exclusive(recording_publication, b"a message");

        false
    });

    let recording_id = reshape_signal_for(subscription_id, SignalCode::START)
        .expect("just waited for it")
        .recording_id;

    for _ in 0..10 {
        let _ = client.offer_exclusive(recording_publication, b"a message");
    }

    // **Stopping, four ways.** The first stops it; the three `try_` forms after
    // it each meet an archive with nothing left to stop, which is the answer
    // they exist to make a `false` rather than a failure.
    archive
        .stop_recording_subscription(&mut client, subscription_id)
        .expect("the archive stops the subscription it opened");

    assert!(
        !archive
            .try_stop_recording_subscription(&mut client, subscription_id)
            .expect("stopping what is already stopped is an answer, not an error"),
        "the subscription was stopped, so there is nothing left for the try form to stop"
    );
    assert!(
        !archive
            .try_stop_recording_channel_and_stream(
                &mut client,
                RECORDING_CHANNEL,
                RECORDING_STREAM_ID
            )
            .expect("the same, by the channel this client asked for"),
        "and its subscription is what that channel and stream was"
    );
    assert!(
        !archive
            .try_stop_recording_by_identity(&mut client, recording_id)
            .expect("and the same, by the recording"),
        "which reads its `false` out of a `relevantId` of zero rather than out of a refusal"
    );

    // **Reshaping.** A position the archive will not truncate to is refused in
    // **two** domains at once, which is the other half of the code-mapping case
    // above met through a request rather than through a lookup.
    // A refusal is a refusal whichever of the archive's checks made it. Which
    // one fires depends on the position — a log picks its own bounds — so what
    // is asked here is that the refusal is one, and that it carries the
    // archive's own words rather than this client's summary of them.
    let refused = archive
        .truncate_recording(&mut client, recording_id, 32)
        .expect_err("a position inside a recording is not one it will be cut down to");

    assert!(
        matches!(refused, ArchiveError::Refused { .. }),
        "the archive refused, so this is a refusal: {refused}"
    );
    assert!(
        refused.to_string().contains("position"),
        "and its own words are what came back: {refused}"
    );

    let truncated = archive
        .truncate_recording(&mut client, recording_id, 0)
        .expect("the archive will cut it down to nothing");

    assert!(
        truncated >= 0,
        "and it answers how many segments that was: {truncated}"
    );

    // A **segment** start has to be a segment boundary, and a recording shorter
    // than one segment file has none to offer — the archive says so with the
    // length it would have to reach, its own `lowerBound` for the segment file
    // length. Both of these are that rule, and neither is this client's.
    for refused in [
        archive
            .detach_segments(&mut client, recording_id, 0)
            .expect_err("there is no segment of this recording to take out"),
        archive
            .purge_segments(&mut client, recording_id, 0)
            .expect_err("and none to purge"),
    ] {
        assert!(
            refused.to_string().contains("lowerBound"),
            "the archive said which bound was not met: {refused}"
        );
    }

    // Neither of these has anything to do, which is an answer of zero rather
    // than a refusal — `detach_segments` is the one of the five that answers
    // nothing at all, which is why it is not read here.
    assert!(
        archive
            .attach_segments(&mut client, recording_id)
            .expect("nothing was detached, so attaching nothing is not an error")
            >= 0,
        "the attach answers a count"
    );
    assert!(
        archive
            .delete_detached_segments(&mut client, recording_id)
            .expect("and the same for the delete")
            >= 0,
        "the delete answers a count"
    );

    // **And the end of one**: purging answers how many segments went, and the
    // listing after it is the recording being gone rather than an error.
    let purged = archive
        .purge_recording(&mut client, recording_id)
        .expect("the archive purges what it holds");

    assert!(
        purged >= 0,
        "and answers how many segments it deleted: {purged}"
    );

    forget_narrowed();
    assert_eq!(
        0,
        archive
            .list_recording(&mut client, recording_id, &mut collect_narrowed)
            .expect("listing a recording that is not there is an empty listing"),
        "the recording is not there any more"
    );

    archive.close(&client);
    media_driver.stop().expect("the driver stops");
}

/// Where a replay goes, and on which stream. Its own, so the replay is not a
/// subscription to the recording it replays.
const REPLAY_CHANNEL: &str = "aeron:ipc?alias=client-api-replay";
const REPLAY_STREAM_ID: i32 = 4214;

/// **A publication the archive is asked to record, and a replay of it.**
///
/// The two ends of one recording, and both are the reference's *other* way to do
/// what the tests above do by hand:
///
/// * [`Archive::add_recorded_publication`] is one call that adds a publication
///   and asks the archive to record **the session the driver gave it** — which is
///   what [`channel_with_session_id`] exists for.
/// * [`Archive::start_replay`] and [`Archive::replay`] are the replay, and the
///   subscription that receives it. `replay`'s whole job is the session id in the
///   channel, and the assertion that matters is at the bottom: the replayed bytes
///   arrive on it.
#[test]
fn a_publication_can_be_added_recorded_and_replayed() {
    let Some((mut media_driver, _archive_dir, mut client)) = rig("archive-client-api-recorded")
    else {
        return;
    };

    let mut archive = Archive::connect(
        &context(),
        &mut client,
        &mut NoCredentials,
        Handlers {
            recording_signal: Some(collect_recorded_signal),
            ..Handlers::default()
        },
    )
    .expect("the archive is connected");

    // One call, where the tests above make the publication and then name its
    // session by hand.
    let publication = archive
        .add_recorded_publication(&mut client, RECORDING_CHANNEL, RECORDING_STREAM_ID)
        .expect("the client takes the publication and the archive takes the recording");

    until(&mut client, "the recording publication linking", |client| {
        client
            .publication(publication)
            .and_then(deepmsg_client::publication::Publication::is_connected)
            .unwrap_or(false)
    });

    let session_id = client
        .publication(publication)
        .expect("just added")
        .session_id();

    // The signal is read by a poll of the archive's control subscription, and
    // `Client::poll` is not one — it drives the conductor and hands nothing to a
    // subscription's handler.
    let deadline = Instant::now() + Duration::from_secs(10);
    let recording_id = loop {
        let _ = client.offer(publication, b"a message");
        let _ = archive
            .poll_for_recording_signals(&mut client)
            .expect("polling is not an error");

        if let Some(signal) = recorded_signal(SignalCode::START) {
            break signal.recording_id;
        }

        assert!(Instant::now() < deadline, "the recording never starts");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    };

    let written = b"the archive replayed this";
    for _ in 0..5 {
        let _ = client.offer(publication, written);
    }

    // **The archive writes a recording on its own thread**, so the stop below
    // can land before the last message is in the log — and a replay of a log that
    // does not hold it replays something else. Waiting for the position to stop
    // moving is what makes the bytes at the bottom of this test a claim rather
    // than a race.
    let mut last = -1;
    until(&mut client, "the recording to catch up", |client| {
        let position = archive
            .get_recording_position(client, recording_id)
            .expect("the archive answers while it is recording");

        let settled = position == last && position > 0;
        last = position;

        settled
    });

    // Stopped the way the reference's `stop_recording_publication` does it, which
    // is the two lines that function is: the channel with the session on it, and
    // the stream. There is no method for it here because a `Publication` does not
    // carry its channel — see this module's note in `archive.rs`.
    let recording_channel =
        channel_with_session_id(RECORDING_CHANNEL, session_id).expect("a channel this test wrote");

    archive
        .stop_recording_channel_and_stream(&mut client, &recording_channel, RECORDING_STREAM_ID)
        .expect("the archive stops recording the session it was told about");

    let replay_session_id = archive
        .start_replay(
            &mut client,
            recording_id,
            REPLAY_CHANNEL,
            REPLAY_STREAM_ID,
            &ReplayParams::default(),
        )
        .expect("the archive starts a replay");

    assert!(
        replay_session_id >= 0,
        "a replay has a session id: {replay_session_id}"
    );

    let replay_subscription = archive
        .replay(
            &mut client,
            recording_id,
            REPLAY_CHANNEL,
            REPLAY_STREAM_ID,
            &ReplayParams::default(),
        )
        .expect("and the subscription that receives it");

    // **The bytes.** Nothing above this line proves that the replay's session id
    // went into the channel correctly; this does, because a subscription on the
    // wrong session receives nothing.
    let mut replayed = Vec::new();

    until(&mut client, "the replayed bytes", |client| {
        client.poll_subscription(replay_subscription, 10, |message| {
            replayed.extend_from_slice(message.payload);
        });

        // For the bytes, not for a length: the recording holds the message the
        // waiting loop above offered as well, so a count is reached before the
        // ones this test is about have arrived.
        replayed
            .windows(written.len())
            .any(|window| window == written)
    });

    assert!(
        replayed.len() >= written.len(),
        "the archive replayed what was written: {} bytes back",
        replayed.len()
    );

    archive.close(&client);
    media_driver.stop().expect("the driver stops");
}

/// The channel a replication's copy runs over, which the request has to name:
/// the archive's own `aeron.archive.replication.channel` is **not** set by
/// [`archive::PROPERTIES`], and a request that names none is refused for it.
/// The channel the copy itself runs over, and it is **UDP** for the reason the
/// reference's own fixture is (`aeron_archive_test.cpp:449`:
/// `aeron:udp?endpoint=localhost:8012`): `REPLAY` puts the destination
/// subscription's **resolved endpoint** on the channel it asks the source to
/// replay onto (`ReplicationSession.java:668-690`), and an IPC subscription has
/// no endpoint to resolve — `resolvedEndpoint()` answers `null` for one
/// (`Subscription.java:582-590` is a counter lookup, and `aeron:ipc` has no
/// address counter). A copy over IPC therefore stalls at `REPLAY` in the
/// reference too, which is why no reference test does it.
///
/// A wildcard port rather than a fixed one: the destination binds an ephemeral
/// port, exactly as its own replication channel does
/// (`TestContexts.LOCALHOST_REPLICATION_CHANNEL`).
const REPLICATION_CHANNEL: &str = "aeron:udp?endpoint=localhost:0";

/// **The four replication round trips.**
///
/// [`Archive::replicate`], [`Archive::stop_replication`] and
/// [`Archive::try_stop_replication`] have been able to *offer* their requests
/// since C1 and had no arm to answer them — the gap
/// [`the_requests_that_stop_and_reshape_a_recording`] writes down. This is the
/// arm, and these are the answers that say it is wired to the conductor rather
/// than to the adapter's `catch-all`.
///
/// **What is not tested here is a copy.** Nothing below moves a byte between
/// two archives: a successful replicate leaves a session that has not taken its
/// first step, and what is asserted is the *answer*. That is the reference's own
/// shape rather than a shortcut — `ArchiveConductor.replicate` answers
/// `sendOkResponse(correlationId, replicationId)` the moment the session is
/// registered (`:1472-1478`), long before anything is copied.
///
/// The four, in the order they are asked:
///
/// 1. **A replicate nothing refuses is answered with its id**, which is minted
///    from the conductor's **session** counter (`:1455`) and so is a positive
///    number rather than a constant.
/// 2. **A destination this archive does not hold is refused with `5`**, its own
///    `UNKNOWN_RECORDING` (`:1411-1417`) — the code the archive sends, not the
///    `205` the client reports.
/// 3. **A destination that is recording right now is refused with `2`**, its
///    `ACTIVE_RECORDING` (`:1419-1428`). The one of the four that needs a
///    recording to exist, and the only one that says anything about the copy
///    rather than about the request.
/// 4. **A stop for an id no session is registered under is a `false`** — the
///    whole point of the `try_` form (`aeron_archive_client.c:1816-1844`), and
///    the C fixture's own assertion (`aeron_archive_test.cpp:2861-2871`). It is
///    also the one that can be got wrong by being *helpful*: a stop that
///    answered `OK` for an unknown id would make every `tryStopReplication`
///    claim it had stopped something.
///
/// The last two assertions are the same claim from the other side: an id that
/// **is** registered stops, and then does not, because a stop gives the id up
/// (`ArchiveConductor.java:1482`) while the session goes on being driven until
/// its own close (`ReplicationSession.java:196-211`).
#[test]
fn the_four_replication_round_trips() {
    let Some((mut media_driver, _archive_dir, mut client)) = rig("archive-client-api-replicate")
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

    // A request with a source in it, so the fields that are not read yet are at
    // least the fields a real one carries. Nothing validates the source until
    // the session walks, which is the next commit's.
    let mut params = ReplicationParams::new();
    params.replication_channel = REPLICATION_CHANNEL.to_owned();
    params.file_io_max_length = 4096;

    let replicate = |archive: &mut Archive, client: &mut Client, params: &ReplicationParams| {
        archive.replicate(
            client,
            SRC_RECORDING_ID,
            SRC_CONTROL_STREAM_ID,
            SRC_CONTROL_CHANNEL,
            params,
        )
    };

    // ① Nothing in its way.
    let replication_id = replicate(&mut archive, &mut client, &params)
        .expect("the archive takes a replication it has no reason to refuse");

    assert!(
        replication_id > 0,
        "the id is minted from the session counter, which starts at {}: {replication_id}",
        deepmsg_archive::server::conductor::FIRST_SESSION_ID
    );

    // ② A destination that does not exist.
    params.dst_recording_id = DST_RECORDING_ID_NOT_HELD;
    let error = replicate(&mut archive, &mut client, &params)
        .expect_err("a destination the archive does not hold is refused");

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
        message.contains(&format!(
            "unknown destination recording id {DST_RECORDING_ID_NOT_HELD}"
        )),
        "the archive's own words, which name the recording and not this client's summary: {message}"
    );

    // ③ A destination that is being recorded right now.
    //
    // The id comes from a **listing** rather than from a signal: the client's
    // descriptor consumer is an object since C1's last commit, so the closure
    // below carries this test's own `Vec` and no `static` slot is needed for it.
    // That is the same door the replication's own session needs, used here for
    // the first time by a criterion.
    let _subscription_id = archive
        .start_recording(
            &mut client,
            RECORDING_CHANNEL,
            RECORDING_STREAM_ID,
            false,
            false,
        )
        .expect("the archive starts recording");

    let _publication = client
        .add_exclusive_publication(
            RECORDING_CHANNEL,
            RECORDING_STREAM_ID,
            archive::COMMAND_TIMEOUT,
        )
        .expect("the driver takes a publication on the recording channel");

    let mut active = Vec::new();

    until(
        &mut client,
        "the recording to reach the catalog",
        |client| {
            active.clear();
            let _ =
                archive.list_recordings(client, 0, 10, &mut |descriptor: &RecordingDescriptor| {
                    if descriptor.stop_position == NULL_POSITION {
                        active.push(descriptor.recording_id);
                    }
                });

            !active.is_empty()
        },
    );

    params.dst_recording_id = active[0];
    let error = replicate(&mut archive, &mut client, &params)
        .expect_err("a recording still in flight is not a place to copy into");

    let ArchiveError::Refused {
        archive_error_code,
        message,
        ..
    } = &error
    else {
        panic!("the archive refused, so this is a refusal: {error}");
    };

    assert_eq!(
        2, *archive_error_code,
        "ARCHIVE_ERROR_CODE_ACTIVE_RECORDING, in the archive's own domain"
    );
    assert_eq!(Some(202), error.client_error_code());
    assert!(
        message.contains(&format!(
            "cannot replicate to active recording {}",
            active[0]
        )),
        "and its reason names the recording that is in the way: {message}"
    );

    // ④ The three stops. An id nothing is under, an id that is, and that id
    // again — the C fixture's `/0` arm's `replication_id + 40000`, and then the
    // pair that shows the id really was given up rather than merely aborted.
    assert!(
        !archive
            .try_stop_replication(&mut client, replication_id + 40_000)
            .expect("an id no session is registered under is an answer, not an error"),
        "nothing was stopped, so the try form says so"
    );
    assert!(
        archive
            .try_stop_replication(&mut client, replication_id)
            .expect("the id the OK carried is one a stop finds"),
        "and the replication the first replicate made is one there was to stop"
    );
    assert!(
        !archive
            .try_stop_replication(&mut client, replication_id)
            .expect("the same id again is the same kind of answer"),
        "a stop takes the session out by its id, so a second one has nothing to find"
    );

    archive.close(&client);
    media_driver.stop().expect("the driver stops");
}

/// **A replication that cannot reach its source says so — under its own id.**
///
/// The first of the replication's twelve states is the only one that walks, and
/// it is the only one that can fail outright: `CONNECT` builds a session on the
/// source archive, and a source that will not take one ends the replication
/// before it has copied anything.
///
/// Three claims, and the third is the one that would be missed by a plausible
/// implementation:
///
/// * the client is told, which needs the failure to travel from inside the
///   session's own turn out to the control subscription
///   (`ReplicationProgress::Failed` → `Deferred::Error`);
/// * it is told the archive's code, `REPLICATION_CONNECTION_FAILURE`
///   (`ArchiveException.java:99`), which is the `relevantId` on the wire;
/// * and the **correlation id is the replication's**, not the request's —
///   `sendErrorResponse(replicationId, …)` (`ReplicationSession.java:925`). So
///   this arrives as an error about a request the client made earlier, under an
///   id it was *answered* with, and no wait of its own will ever match it.
///
/// The source here is a channel the client will not open a session with. That is
/// the reference's own failure path and not a stand-in for one: its
/// `ConfigurationException` from `AeronArchive.asyncConnect` is an
/// `AeronException` (`ConfigurationException.java:21`), so it lands in the same
/// `catch` as a connect that timed out (`ReplicationSession.java:437-442`).
#[test]
fn a_replication_that_cannot_reach_its_source_says_so() {
    let Some((mut media_driver, _archive_dir, mut client)) =
        rig("archive-client-api-replicate-src")
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

    let mut params = ReplicationParams::new();
    params.replication_channel = REPLICATION_CHANNEL.to_owned();

    // Accepted: the answer to a replicate is the session being registered, and
    // nothing about the copy is known yet — which is why this call succeeds and
    // the failure below arrives on its own, later.
    let replication_id = archive
        .replicate(
            &mut client,
            SRC_RECORDING_ID,
            SRC_CONTROL_STREAM_ID,
            "",
            &params,
        )
        .expect("the archive takes the replication, whatever the source turns out to be");

    let mut failures = Vec::new();

    until(
        &mut client,
        "the replication's connection failure",
        |client| match archive.check_for_error_response(client) {
            Ok(()) => false,
            Err(error) => {
                failures.push(error);

                true
            }
        },
    );

    let ArchiveError::Refused {
        correlation_id,
        archive_error_code,
        message,
    } = &failures[0]
    else {
        panic!(
            "the archive refused, so this is a refusal: {:?}",
            failures[0]
        );
    };

    assert_eq!(
        &replication_id, correlation_id,
        "the error is about the replication, under the id its OK carried"
    );
    assert_eq!(
        14, *archive_error_code,
        "ARCHIVE_ERROR_CODE_REPLICATION_CONNECTION_FAILURE, in the archive's own domain"
    );
    assert_eq!(
        Some(214),
        failures[0].client_error_code(),
        "and the same code where a caller matches on it"
    );
    assert!(
        message.starts_with("Replication connection failed="),
        "with the reference's own words in front of the reason: {message}"
    );

    archive.close(&client);
    media_driver.stop().expect("the driver stops");
}

/// What the **descriptor** test's signal handler saw.
///
/// A sink of its own, for the reason written at [`SIGNALS`].
static REPLICATION_SIGNALS: Mutex<Vec<RecordingSignal>> = Mutex::new(Vec::new());

/// Its handler.
fn collect_replication_signal(signal: &RecordingSignal) {
    REPLICATION_SIGNALS
        .lock()
        .expect("no other test is holding the replication signals")
        .push(*signal);
}

/// The one row a listing of this archive holds, or `None` while there is not
/// exactly one.
fn the_only_recording(archive: &mut Archive, client: &mut Client) -> Option<RecordingDescriptor> {
    let mut rows = Vec::new();

    let _ = archive.list_recordings(client, 0, 10, &mut |descriptor: &RecordingDescriptor| {
        rows.push(descriptor.clone());
    });

    match rows.len() {
        1 => rows.pop(),
        _ => None,
    }
}

/// **A recording on `archive`, with data in it and stopped**, and the row it
/// leaves.
///
/// The three things a replication's criteria need of a **source**, in the order
/// they need them: a catalog row (a recording, and a publication on the
/// recording channel), a **stop position** on that row — which is what keeps the
/// descriptor from taking the already-in-sync shortcut
/// (`ReplicationSession.java:393-398`) — and the publisher **given back**.
///
/// The last is not tidiness. The destination's subscription names the
/// **source's session** on the source's stream, and an IPC subscription matches
/// on stream and session alone: a publisher still open on that stream feeds the
/// destination's recording by itself, and a criterion that then says "the copy
/// has data in it" is measuring that publisher rather than the replay.
/// **Measured**: with it open, that assertion passed with `REPLAY` disabled.
fn a_stopped_recording(archive: &mut Archive, client: &mut Client) -> RecordingDescriptor {
    // The recording to copy. Made, written to, and **stopped**: a stopped
    // recording has a stop position, which is what keeps the descriptor from
    // taking the "already in sync" shortcut (`:393-398`) and lets a caller
    // assert on a row the source actually filled in.
    let subscription_id = archive
        .start_recording(client, RECORDING_CHANNEL, RECORDING_STREAM_ID, false, false)
        .expect("the archive starts recording");

    let publication = client
        .add_exclusive_publication(
            RECORDING_CHANNEL,
            RECORDING_STREAM_ID,
            archive::COMMAND_TIMEOUT,
        )
        .expect("the driver takes a publication on the recording channel");

    until(client, "the recording publication linking", |client| {
        client
            .exclusive_publication(publication)
            .and_then(deepmsg_client::publication::ExclusivePublication::is_connected)
            .unwrap_or(false)
    });

    let mut source = None;

    until(client, "the source's catalog row", |client| {
        let _ = client.offer_exclusive(publication, b"a message to copy");
        source = the_only_recording(archive, client);

        source.is_some()
    });

    let source = source.expect("the loop ends with a row");

    // ★ **And that the row has data in it, which is not the same as having a
    // row.** The row is written when the `startRecording` *request* is answered,
    // before any image exists (`ArchiveConductor.java:570` against `:2046-2051`),
    // so a caller that stops a recording the moment its row appears can stop one
    // that never read a byte — and an empty recording is exactly what a
    // replication's descriptor calls "already in sync" (`:393-398`) and never
    // copies.
    //
    // Under `SHARED` the recorder is driven inside the conductor's own turn, so
    // the read has happened by the time the stop arrives and this wait is a
    // formality. Under `DEDICATED` it is a thread of its own and the window is
    // real: **found here**, as a `DEDICATED` source failing the criterion below
    // about one run in six with "the extend's signal never happened" — the
    // destination had taken the in-sync shortcut over a recording with no bytes
    // in it.
    until(client, "the source's recording position", |client| {
        let _ = client.offer_exclusive(publication, b"a message to copy");

        archive
            .get_recording_position(client, source.recording_id)
            .is_ok_and(|position| position > source.start_position)
    });

    archive
        .stop_recording_subscription(client, subscription_id)
        .expect("the archive stops the subscription it opened");

    until(client, "the source's stop position", |client| {
        archive
            .get_stop_position(client, source.recording_id)
            .is_ok_and(|stop| stop >= 0)
    });

    // **The publisher goes before the copy is asked for, and it is not
    // tidiness.** The destination's subscription will name the **source's
    // session** on the source's stream — that is what `EXTEND` writes onto its
    // channel — and an IPC subscription matches on the stream and the session
    // alone. A publisher still open on that stream therefore feeds the
    // destination recording by itself, and the bytes a caller waits for would
    // not be the replay's. **Measured**: with it open, the assertion that the
    // copy has data in it passed with `REPLAY` disabled — the same shape of
    // mistake as the wait the plan's §9.5 records, and found the same way.
    client
        .remove_publication(publication, archive::COMMAND_TIMEOUT)
        .expect("the source publication is given back");

    the_only_recording(archive, client).expect("still one row")
}

/// What the **cross-process** replication test's signal handler saw.
///
/// A sink of its own, for the reason [`SIGNALS`] gives: two rigs started in one
/// process are two archives in one signal space, and a shared sink answers one
/// test with the other's signal.
static CROSS_SIGNALS: Mutex<Vec<RecordingSignal>> = Mutex::new(Vec::new());

/// Its handler.
fn collect_cross_signal(signal: &RecordingSignal) {
    CROSS_SIGNALS
        .lock()
        .expect("no other test is holding the cross-process signals")
        .push(*signal);
}

/// `n` UDP ports this process may use, **probed rather than computed**.
///
/// These are ports something *binds* — the source archive of
/// [`a_replication_reaches_an_archive_in_another_process`] binds the one its
/// control channel names — so they cannot be the hard-coded kind the rest of
/// this file uses for addresses nobody binds.
///
/// They used to be computed: `20_000 + pid % 20_000 + offset`, "a base that
/// keeps two test binaries running at once apart". It does not, in either
/// direction, and the failure that found this out was a **live replication that
/// waited for ever** (the D2/D4 record's flake, `analysis/archive-client-api-flake.md`
/// §B):
///
/// * **Two binaries' ports overlap by construction.** With pids one apart,
///   `base + 9` for one is `base + 8` for the next, and the offsets in play
///   here are 1, 7, 8 and 9 — smaller than the step between the pids. Two test
///   binaries running at once therefore name the *same* port for two different
///   roles, and the loser's bind is `EADDRINUSE`.
/// * **And the arithmetic can land inside the kernel's own ephemeral range**
///   (32 768-60 999): `base + offset` runs from 20 009 to 40 009, so any process
///   whose pid is past ~12 759 `% 20 000` is naming ports the kernel hands out
///   to anyone — including this test's own clients.
///
/// What that cost: the live destination's `add_receive_destination` was refused
/// with `(98) Address already in use`, the replication session **never reads
/// that answer** (neither does the reference — `ReplicationSession` polls its
/// destinations only to retire them), so it set `isLiveAdded` anyway and waited
/// for a stream that had never been added. One bind, and the merge never came.
///
/// So the kernel picks, all `n` at once so that they cannot collide with each
/// other, and the sockets go as soon as their numbers are known — which is the
/// driver's to bind next, the way every other fixed address in this file is
/// somebody else's to bind.
fn free_udp_ports<const N: usize>() -> [u16; N] {
    let probes: Vec<std::net::UdpSocket> = (0..N)
        .map(|_| std::net::UdpSocket::bind("127.0.0.1:0").expect("the kernel can lend a UDP port"))
        .collect();

    let mut ports = [0u16; N];
    for (port, probe) in ports.iter_mut().zip(&probes) {
        *port = probe
            .local_addr()
            .expect("a bound socket knows its port")
            .port();
    }

    ports
}

/// **A replicate fetches its source's descriptor, and makes the destination
/// row.**
///
/// `REPLICATE_DESCRIPTOR` (`ReplicationSession.java:444-483`) is the state where
/// the copy first touches anything: it asks the source archive — through the
/// client the `CONNECT` state built — for one recording's descriptor, and
/// `onRecordingDescriptor` (`:295-399`) turns what comes back into a row in
/// **this** archive's catalog.
///
/// **The source is this archive.** deepmsg serves both ends, and the shortest
/// path through the state is a replication of one of its recordings onto
/// itself: the control channel is the one this archive already listens on, and
/// the request goes out over it and comes back. What is exercised is the whole
/// state — the listing, the poller, the descriptor, and the row — with no
/// second archive to keep in step.
///
/// Two claims, and the second is the one a stub could not fake:
///
/// * the destination's client is told, with a `REPLICATE` signal
///   (`:351`) whose position is the source's **start**;
/// * and the row that appears is the source's descriptor — every field of it,
///   under an id of this archive's own, with the two positions collapsed to the
///   start (`catalog.addNewRecording(startPosition, startPosition, …)`,
///   `:337-349`). A row that agreed on one field would prove nothing; this
///   agrees on ten.
#[test]
fn a_replicate_fetches_its_source_descriptor_and_extends_the_destination() {
    let Some((mut media_driver, _archive_dir, mut client)) =
        rig("archive-client-api-replicate-descriptor")
    else {
        return;
    };

    let mut archive = Archive::connect(
        &context(),
        &mut client,
        &mut NoCredentials,
        Handlers {
            recording_signal: Some(collect_replication_signal),
            ..Handlers::default()
        },
    )
    .expect("the archive is connected");

    let source = a_stopped_recording(&mut archive, &mut client);

    // **The replicate.** Its source is this archive's own control channel, on
    // the stream both control subscriptions are on.
    let mut params = ReplicationParams::new();
    params.replication_channel = REPLICATION_CHANNEL.to_owned();

    let replication_id = archive
        .replicate(
            &mut client,
            source.recording_id,
            archive::CONTROL_STREAM_ID,
            archive::CONTROL_CHANNEL,
            &params,
        )
        .expect("the archive takes the replication");

    let mut copied = None;

    until(&mut client, "the destination's row", |client| {
        let mut rows = Vec::new();
        let _ = archive.list_recordings(client, 0, 10, &mut |descriptor: &RecordingDescriptor| {
            rows.push(descriptor.clone());
        });

        copied = rows
            .iter()
            .find(|row| row.recording_id != source.recording_id)
            .cloned();

        copied.is_some()
    });

    let copied = copied.expect("the loop ends with a row");

    // The descriptor, field for field, under an id of this archive's own.
    assert!(
        copied.recording_id > source.recording_id,
        "the copy is a new row, not the source's: {} against {}",
        copied.recording_id,
        source.recording_id
    );
    assert_eq!(
        source.start_position, copied.start_position,
        "the start is the source's"
    );
    // **A copy's stop is three things over its life, and the criterion can see
    // any of them.** The row is made empty — `addNewRecording(startPosition,
    // startPosition, …)` (`ReplicationSession.java:336-350`) — the `EXTEND`
    // state then writes `NULL_POSITION` over its stop because the copy is live
    // from then on (`Catalog.extendRecording`, `Catalog.java:653-659`), and
    // once the replay is feeding it the stop is wherever the copy has got to.
    // All three are the reference's own behaviour, and all three are reachable
    // *before a client can see the row at all*: the session's states run inside
    // the archive, and the listing that first finds the row may be after any of
    // them. Measured, twice: CI lost that race and saw the second
    // (`left: 0, right: -1`, in 1.23 s), and tightening this to the first two
    // lost it again and saw the third (`start=0 stop=320`).
    //
    // So what is asserted is the one thing true of every one of them, and it is
    // not nothing: a copy **never ends before it begins**. A stop below the
    // start is a row claiming to hold data from before its own beginning —
    // which is the shape a defect here would have.
    assert!(
        copied.stop_position == NULL_POSITION || copied.stop_position >= copied.start_position,
        "a copy never ends before it begins: start={} stop={}",
        copied.start_position,
        copied.stop_position
    );
    assert_eq!(source.initial_term_id, copied.initial_term_id);
    assert_eq!(source.segment_file_length, copied.segment_file_length);
    assert_eq!(source.term_buffer_length, copied.term_buffer_length);
    assert_eq!(source.mtu_length, copied.mtu_length);
    assert_eq!(
        source.session_id, copied.session_id,
        "the source publication's session, which the replay will arrive under"
    );
    assert_eq!(source.stream_id, copied.stream_id);
    assert_eq!(source.stripped_channel, copied.stripped_channel);
    assert_eq!(source.original_channel, copied.original_channel);
    assert_eq!(source.source_identity, copied.source_identity);

    // And the signal, under the replication's own id rather than a request's.
    let replicated = REPLICATION_SIGNALS
        .lock()
        .expect("no other test is holding the replication signals")
        .iter()
        .find(|signal| signal.signal == SignalCode::REPLICATE)
        .copied()
        .expect("a REPLICATE signal");

    let _ = replication_id;

    assert_eq!(
        copied.recording_id, replicated.recording_id,
        "it is about the row that was just made"
    );
    assert_eq!(
        source.start_position, replicated.position,
        "at the position the copy starts from, which is the source's start"
    );
    assert_eq!(
        -1, replicated.subscription_id,
        "and with no subscription: the signal goes out before EXTEND makes one"
    );

    // **And EXTEND makes one.** `SRC_RECORDING_POSITION` asks the source where
    // its recording got to (`:484-524`), and `EXTEND` (`:525-568`) turns the
    // destination's row into a recording: a subscription on the **replication**
    // channel, carrying the two parameters that make it *this* copy.
    //
    // What is asserted is the subscription as the archive recorded it, which is
    // the only half of the two that is observable from here: the other is the
    // `OK` answer the archive sends the client under the **replication's**
    // correlation id, and that is what a later state's criterion reads.

    // **The wait is for the replication's own subscription**, not for "a
    // subscription": the source recording's is on the same stream and may still
    // be listed for a turn or two after the test stopped it, and a loop that
    // broke on *any* of them would go on to fail on a real subscription that had
    // not arrived yet. Measured: that is what this criterion did before it was
    // written this way — one run in three. What names it is the **session** the
    // source's replay will arrive under, which no other subscription on this
    // archive carries.
    let extend = the_extensions_signal(
        &mut archive,
        &mut client,
        &REPLICATION_SIGNALS,
        copied.recording_id,
    );

    assert_eq!(
        copied.recording_id, extend.recording_id,
        "the extend is about the row the descriptor made"
    );
    assert_ne!(
        NULL_POSITION, extend.subscription_id,
        "and it made a recording subscription — the id is what the copy is fed \
         under, and `NULL_POSITION` is what the REPLICATE that came before it \
         carried because there was none yet"
    );
    // The **channel** the extend was made on — `rejoin=false`, the source's
    // session, or the tags and `control-mode=manual` a multi-destination one
    // takes — is the session's own construction and is judged where it is
    // built (`the_extensions_signal` says why it cannot be judged here).
    // **And the session ends by saying so.** `REPLICATE` sends `SYNC` when the
    // copy has caught up with where the source's recording stopped (`:784-800`),
    // and `close()` sends `REPLICATE_END` **once** — the signal the reference's
    // own fixture waits on (`aeron_archive_test.cpp:3001-3006`).
    //
    // The wait goes through a wait that **dispatches**. A bare `client.poll()`
    // reads the driver's replies and nothing else; archive signals are handed to
    // the handler by the archive's own waits (this file's module note says the
    // same thing about the listing), so what this polls is the signal method.
    until(&mut client, "the replication's end", |client| {
        let _ = archive.poll_for_recording_signals(client);

        REPLICATION_SIGNALS
            .lock()
            .expect("no other test is holding the replication signals")
            .iter()
            .any(|signal| signal.signal == SignalCode::REPLICATE_END)
    });

    let signals = REPLICATION_SIGNALS
        .lock()
        .expect("no other test is holding the replication signals")
        .clone();

    let synced = signals
        .iter()
        .find(|signal| signal.signal == SignalCode::SYNC)
        .copied()
        .expect("a SYNC signal");

    assert_eq!(
        copied.recording_id, synced.recording_id,
        "the sync is about the copy the descriptor made"
    );
    assert!(
        synced.position >= source.stop_position,
        "at or past where the source's recording stopped: {} against {}",
        synced.position,
        source.stop_position
    );
    assert_eq!(
        extend.subscription_id, synced.subscription_id,
        "and it names the destination's recording subscription, which is still \
         there — `stopRecording` has not run yet"
    );

    let ended = signals
        .iter()
        .find(|signal| signal.signal == SignalCode::REPLICATE_END)
        .copied()
        .expect("a REPLICATE_END signal");

    assert_eq!(
        copied.recording_id, ended.recording_id,
        "the end is about the same recording"
    );
    assert_eq!(
        NULL_POSITION, ended.position,
        "and carries no position: there is nothing left to be at"
    );
    assert_eq!(
        -1, ended.subscription_id,
        "nor a subscription — `close` stops the recording **before** it says so \
         (`ReplicationSession.java:197`, `:925`)"
    );

    // **And the copy is what the destination is left holding.** The row the
    // descriptor made now stops where the **source's** row stops, which is the
    // archive's own record of what was recorded — read after the session is over,
    // so it is not racing the teardown `close()` does.
    //
    // This is also the assertion that says the bytes are the **replay's**: the
    // source's publisher is given back before the copy is asked for, so nothing
    // else can have fed the destination's recording.
    let mut rows: Vec<RecordingDescriptor> = Vec::new();

    let after = archive
        .list_recordings(&mut client, 0, 10, &mut |d: &RecordingDescriptor| {
            rows.push(d.clone())
        })
        .expect("the listing completes");

    assert!(after >= 2, "the source's row and the copy's: {after}");

    let copied_after = rows
        .iter()
        .find(|row| row.recording_id == copied.recording_id)
        .expect("the copy is still there");

    let source_after = rows
        .iter()
        .find(|row| row.recording_id == source.recording_id)
        .expect("and so is what it was copied from");

    assert!(
        copied_after.stop_position > copied_after.start_position,
        "the copy has data in it: {} against a start of {}",
        copied_after.stop_position,
        copied_after.start_position
    );
    assert_eq!(
        source_after.stop_position, copied_after.stop_position,
        "and it stops exactly where the source's recording stopped"
    );

    assert_eq!(
        1,
        signals
            .iter()
            .filter(|signal| signal.signal == SignalCode::REPLICATE_END)
            .count(),
        "exactly one REPLICATE_END, which is the whole point of it being in close()"
    );

    archive.close(&client);
    media_driver.stop().expect("the driver stops");
}

/// **A replication reaches an archive in another process.**
///
/// The criterion above copies an archive **onto itself**: one process, one
/// driver, one catalog, and — the part that matters — one aeron directory, so
/// the response channel, the copy's channel and the source's control channel
/// all belong to the same driver.
///
/// What this slice was blocked on for a day is the other arrangement, and it is
/// the one the reference's own suite puts in front of it (`TestArchive.h` starts
/// one archive, `startDestArchive` the other): two archives, **two processes,
/// two directories**. A replication session derives its response channel from
/// the **destination's** control channel (`Archive.java:1519-1541`), so that
/// channel cannot be IPC — the source is another process, and the answers have
/// to cross a socket. That is the shape the plan's §8 settled on, and the shape
/// nothing had measured until there was an archive to measure it against.
///
/// So this is the same walk with the source moved out: its own driver, its own
/// aeron directory, its own archive directory, its own catalog, reached over the
/// wire on a port of its own.
#[test]
fn a_replication_reaches_an_archive_in_another_process() {
    // The destination first, because its rig is what says whether this build has
    // a driver to run at all.
    let Some((mut media_driver, _archive_dir, mut client)) =
        rig("archive-client-api-replication-cross-process")
    else {
        return;
    };

    // **The source archive**, on a control channel the destination can dial.
    let [source_control_port] = free_udp_ports::<1>();
    let source_control = format!("aeron:udp?endpoint=localhost:{source_control_port}");
    let source_control_property = format!("-Daeron.archive.control.channel={source_control}");
    // The driver's mode, said out loud because the archiving media driver
    // refuses the `DEDICATED` its own default would be. **The archive's is
    // deliberately left unsaid**, so that this source takes the default — the
    // mode with threads of its own (P2-9a) — and the criterion below is a
    // replication over an archive that is not `SHARED` as well as over this
    // destination, which [`rig`] pins. It is the one place in this file where
    // the two archives run different modes, and it earned its keep: it is what
    // caught [`a_stopped_recording`] stopping a recording that had read
    // nothing.
    let source_properties = ["-Daeron.threading.mode=SHARED", &source_control_property];
    let source_archive_dir = TempDir::new("archive-client-api-replication-source");

    let Some(mut source_driver) = OwnArchivingMediaDriver::start(
        "archive-client-api-replication-source",
        source_archive_dir.path(),
        &source_properties,
    ) else {
        archiving_driver::announce_skip();
        return;
    };

    source_driver
        .await_ready(archive::DEADLINE)
        .expect("the source archive comes up");

    let mut source_client =
        Client::connect(source_driver.aeron_dir()).expect("connect to the source driver");

    // **The source's own client reaches it over its local control channel**,
    // which is [`context()`] and the same string the destination's client uses —
    // the same channel name in two different drivers. It is not the UDP channel
    // the replication will dial: a *network* publication that names a
    // response-correlation-id has to name a **network** response subscription,
    // and the driver refuses it otherwise
    // (`aeron_driver_conductor_validate_response_subscription`, `:631-651`) —
    // which is a rule about the driver, not about this test.
    let mut source_archive = Archive::connect(
        &context(),
        &mut source_client,
        &mut NoCredentials,
        Handlers::default(),
    )
    .expect("the source archive is connected");

    // What is to be copied, made on the **source**: its row lives in the
    // source's catalog and in no other.
    let source = a_stopped_recording(&mut source_archive, &mut source_client);

    // **The destination**, which is this archive and its own driver.
    let mut archive = Archive::connect(
        &context(),
        &mut client,
        &mut NoCredentials,
        Handlers {
            recording_signal: Some(collect_cross_signal),
            ..Handlers::default()
        },
    )
    .expect("the destination archive is connected");

    let mut params = ReplicationParams::new();
    params.replication_channel = REPLICATION_CHANNEL.to_owned();

    let replication_id = archive
        .replicate(
            &mut client,
            source.recording_id,
            archive::CONTROL_STREAM_ID,
            &source_control,
            &params,
        )
        .expect("the destination archive takes the replication");

    // **The row the descriptor made.** The source's catalog is another
    // process's, so everything in this row crossed a socket.
    let mut copied = None;

    until(&mut client, "the destination's row", |client| {
        copied = the_only_recording(&mut archive, client);

        copied.is_some()
    });

    let copied = copied.expect("the loop ends with a row");

    // **Two catalogs, and the same id in each names a different recording** —
    // the destination's first row and the source's are both `0` here, which is
    // the one thing about this arrangement that a self-replication hides. What
    // says the row is the copy is what it *holds*, and what says it is alone is
    // the count.
    let mut rows = 0;

    let listed = archive
        .list_recordings(
            &mut client,
            0,
            10,
            &mut |_descriptor: &RecordingDescriptor| {
                rows += 1;
            },
        )
        .expect("the listing completes");

    assert_eq!(1, listed, "the destination holds the copy and nothing else");
    assert_eq!(1, rows, "and it listed exactly that one");
    assert_eq!(
        source.start_position, copied.start_position,
        "the descriptor's start crossed the wire"
    );
    assert_eq!(source.stream_id, copied.stream_id);
    assert_eq!(
        source.stripped_channel, copied.stripped_channel,
        "and so did the channel the source recorded on"
    );
    assert_eq!(
        source.session_id, copied.session_id,
        "and the source's own session, which is what the replay will arrive under"
    );

    // **The destination's recording subscription**, on the copy's channel and
    // under the source's session — the two things `EXTEND` writes onto it.
    let extend = the_extensions_signal(
        &mut archive,
        &mut client,
        &CROSS_SIGNALS,
        copied.recording_id,
    );

    assert_eq!(
        copied.recording_id, extend.recording_id,
        "the extend is about the row the descriptor made"
    );
    assert_ne!(
        NULL_POSITION, extend.subscription_id,
        "and it made a recording subscription, which is what the replay is \
         filed under — see `the_extensions_signal` for why the channel it was \
         made on is not asserted from here"
    );

    // **And the session ends by saying so**, once — the same signals the
    // self-replication criterion reads, through this test's own sink. The wait
    // has to go through a wait that **dispatches**: `client.poll()` reads the
    // driver's replies, not the archive's signals.
    until(&mut client, "the replication's end", |client| {
        let _ = archive.poll_for_recording_signals(client);

        CROSS_SIGNALS
            .lock()
            .expect("no other test is holding the cross-process signals")
            .iter()
            .any(|signal| signal.signal == SignalCode::REPLICATE_END)
    });

    let signals = CROSS_SIGNALS
        .lock()
        .expect("no other test is holding the cross-process signals")
        .clone();

    let replicated = signals
        .iter()
        .find(|signal| signal.signal == SignalCode::REPLICATE)
        .copied()
        .expect("a REPLICATE signal");

    assert_eq!(
        copied.recording_id, replicated.recording_id,
        "told about the row the descriptor made, under the replication's own id"
    );
    assert_eq!(-1, replicated.subscription_id, "before EXTEND has made one");

    let synced = signals
        .iter()
        .find(|signal| signal.signal == SignalCode::SYNC)
        .copied()
        .expect("a SYNC signal");

    assert!(
        synced.position >= source.stop_position,
        "the copy caught up with the source's stop: {} against {}",
        synced.position,
        source.stop_position
    );

    let ended = signals
        .iter()
        .find(|signal| signal.signal == SignalCode::REPLICATE_END)
        .copied()
        .expect("a REPLICATE_END signal");

    assert_eq!(copied.recording_id, ended.recording_id);
    assert_eq!(NULL_POSITION, ended.position);
    assert_eq!(-1, ended.subscription_id);
    assert_eq!(
        1,
        signals
            .iter()
            .filter(|signal| signal.signal == SignalCode::REPLICATE_END)
            .count(),
        "exactly one REPLICATE_END"
    );

    // **And the bytes crossed the process boundary.** The destination's row now
    // stops where the **source's** row stops — read on both archives after the
    // session is over, so it is not racing the teardown `close()` does, and read
    // on the source to say that the two catalogs agree.
    let copied_after = the_only_recording(&mut archive, &mut client).expect("still one row");

    let source_after =
        the_only_recording(&mut source_archive, &mut source_client).expect("still one row");

    assert!(
        copied_after.stop_position > copied_after.start_position,
        "the copy has data in it: {} against a start of {}",
        copied_after.stop_position,
        copied_after.start_position
    );
    assert_eq!(
        source_after.stop_position, copied_after.stop_position,
        "and it stops exactly where the source's recording stopped — in another process"
    );

    let _ = replication_id;

    archive.close(&client);
    source_archive.close(&source_client);

    media_driver.stop().expect("the destination driver stops");
    source_driver.stop().expect("the source driver stops");
}

/// The row a **self**-replication made: the one in this catalog that is not the
/// source's.
///
/// [`the_only_recording`] is the cross-process criterion's, where the
/// destination archive holds the copy and nothing else. Here both rows are in
/// one catalog, and the copy is the one with an id of its own.
fn the_copy(
    archive: &mut Archive,
    client: &mut Client,
    source_id: i64,
) -> Option<RecordingDescriptor> {
    let mut rows = Vec::new();

    let _ = archive.list_recordings(client, 0, 10, &mut |descriptor: &RecordingDescriptor| {
        rows.push(descriptor.clone());
    });

    rows.into_iter().find(|row| row.recording_id != source_id)
}

/// What the tagged replication's signal handler saw
/// ([`a_tagged_replication_subscribes_by_tag_and_still_receives_the_replay`]).
///
/// A sink of its own, for the reason [`SIGNALS`] gives: the handlers are `fn`
/// pointers, and two rigs in one process share whatever they write to.
static TAGGED_SIGNALS: Mutex<Vec<RecordingSignal>> = Mutex::new(Vec::new());

fn collect_tagged_signal(signal: &RecordingSignal) {
    TAGGED_SIGNALS
        .lock()
        .expect("no other test is holding the tagged signals")
        .push(*signal);
}

/// The two tags the tagged replication asks for (`ReplicationParams`).
const CHANNEL_TAG_ID: i64 = 4242;
const SUBSCRIPTION_TAG_ID: i64 = 4243;

/// **A replication by tag subscribes by tag — and the copy still arrives.**
///
/// The `isMds` half of `EXTEND` (`ReplicationSession.java:527-555`): a request
/// that carries tags, or a live destination, gets a destination recording whose
/// channel is **rewritten** — the endpoint comes off, the tags go on,
/// `control-mode=manual` — and then the endpoint is put back as an explicitly
/// added **destination**.
///
/// The two halves are one mechanism, and this is the criterion that says so.
/// A manual subscription binds nothing on its own: the channel above names no
/// address at all, so the added destination is what has a socket, and the
/// replay arrives through it.
///
/// **Measured, and the measurement is what says which assertion bites**: with
/// the `async_add_rcv_destination` call removed, the taps in this criterion
/// still see the channel rewritten and still see a `REPLICATE_END` — the
/// session gives up in `AWAIT_IMAGE` and its teardown sends one — so what
/// fails is the `SYNC` below, ten seconds later, along with the bytes that
/// never arrived. The signals alone would not have caught it.
///
/// The source is this archive, as in
/// [`a_replicate_fetches_its_source_descriptor_and_extends_the_destination`] —
/// the shortest path that reaches the branch at all — and what the copy does
/// with the bytes is still asked of the sources' two catalogs, which is what
/// makes the destination load-bearing rather than decorative.
#[test]
fn a_tagged_replication_subscribes_by_tag_and_still_receives_the_replay() {
    let Some((mut media_driver, _archive_dir, mut client)) =
        rig("archive-client-api-replication-tagged")
    else {
        return;
    };

    let mut archive = Archive::connect(
        &context(),
        &mut client,
        &mut NoCredentials,
        Handlers {
            recording_signal: Some(collect_tagged_signal),
            ..Handlers::default()
        },
    )
    .expect("the archive is connected");

    let source = a_stopped_recording(&mut archive, &mut client);

    let mut params = ReplicationParams::new();
    params.replication_channel = REPLICATION_CHANNEL.to_owned();
    params.channel_tag_id = CHANNEL_TAG_ID;
    params.subscription_tag_id = SUBSCRIPTION_TAG_ID;

    let _replication_id = archive
        .replicate(
            &mut client,
            source.recording_id,
            archive::CONTROL_STREAM_ID,
            archive::CONTROL_CHANNEL,
            &params,
        )
        .expect("the archive takes the tagged replication");

    // The row the descriptor made, which is the copy's recording. **The row
    // that is not the source's** — a self-replication leaves two in one
    // catalog, unlike the cross-process criterion, whose destination holds the
    // copy alone.
    let mut copied = None;

    until(&mut client, "the destination's row", |client| {
        copied = the_copy(&mut archive, client, source.recording_id);

        copied.is_some()
    });

    let copied = copied.expect("the loop ends with a row");

    // **The `EXTEND` the subscription was made by**, on the channel it
    // rewrote. The tags are the request's two ids in the reference's order —
    // `channelTagId` then `subscriptionTagId` (`:539`) — and the endpoint is
    // what is *not* there; both are the channel's, and the channel is judged
    // where it is built (`the_extensions_signal`).
    let tagged = the_extensions_signal(
        &mut archive,
        &mut client,
        &TAGGED_SIGNALS,
        copied.recording_id,
    );

    assert_eq!(
        copied.recording_id, tagged.recording_id,
        "the extend is about the copy's row"
    );
    assert_ne!(
        NULL_POSITION, tagged.subscription_id,
        "and it made a subscription — one that reads from where it is told \
         (`tags=` and `control-mode=manual`, asserted where the channel is \
         built), which is why nothing here names an endpoint"
    );

    until(&mut client, "the replication's end", |client| {
        let _ = archive.poll_for_recording_signals(client);

        TAGGED_SIGNALS
            .lock()
            .expect("no other test is holding the tagged signals")
            .iter()
            .any(|signal| signal.signal == SignalCode::REPLICATE_END)
    });

    let signals = TAGGED_SIGNALS
        .lock()
        .expect("no other test is holding the tagged signals")
        .clone();

    let synced = signals
        .iter()
        .find(|signal| signal.signal == SignalCode::SYNC)
        .copied()
        .expect("a SYNC signal");

    assert!(
        synced.position >= source.stop_position,
        "the copy caught up with the source's stop: {} against {}",
        synced.position,
        source.stop_position
    );

    assert_eq!(
        1,
        signals
            .iter()
            .filter(|signal| signal.signal == SignalCode::REPLICATE_END)
            .count(),
        "exactly one REPLICATE_END"
    );

    // **The bytes went through the destination.** The destination's row stops
    // where the source's does, and it is not empty — which no subscription
    // that bound nothing could have managed.
    let copied_after =
        the_copy(&mut archive, &mut client, source.recording_id).expect("still there");

    assert_eq!(copied.recording_id, copied_after.recording_id);
    assert!(
        copied_after.stop_position > copied_after.start_position,
        "the copy has data in it: {} against a start of {}",
        copied_after.stop_position,
        copied_after.start_position
    );
    assert_eq!(
        source.stop_position, copied_after.stop_position,
        "and it stops exactly where the source's recording stopped"
    );

    archive.close(&client);
    media_driver.stop().expect("the driver stops");
}

/// What the live merge's signal handler saw
/// ([`a_live_replication_merges_into_the_stream_it_is_copying`]).
///
/// A sink of its own, for the reason [`SIGNALS`] gives.
static LIVE_SIGNALS: Mutex<Vec<RecordingSignal>> = Mutex::new(Vec::new());

fn collect_live_signal(signal: &RecordingSignal) {
    LIVE_SIGNALS
        .lock()
        .expect("no other test is holding the live signals")
        .push(*signal);
}

/// The stream the live recording is made on — the one the copy ends up
/// following.
const LIVE_STREAM_ID: i32 = 4215;

/// **A replication of a live recording merges into it, and follows it.**
///
/// The second half of `isMds`, and the one the branch exists for:
/// `ATTEMPT_LIVE_JOIN` (`ReplicationSession.java:834-900`). A request that
/// names a `live_destination` gets a destination recording whose subscription
/// is wired to the replay, and then — once the replay has caught up — wired to
/// the **live stream** as well, in the same image under the same session. The
/// replay is then taken off (`:873-880`) and the copy is live.
///
/// Three things are asserted and each is a different half of that:
///
/// * the `MERGE` signal arrives, carrying **no subscription id** — the
///   subscription is let go before the signal is sent (`:876-878`), which is
///   how a client tells a merge from a sync;
/// * there is **no `SYNC`**: a live copy ends by merging, not by catching up
///   with a stop position — so the session took the live route through
///   `CATCHUP` rather than the bounded one through `REPLICATE`;
/// * and the copy **follows the stream**: bytes published *after* the merge
///   move the destination's recording position, which is only possible if the
///   live destination was added to the subscription the recording is being
///   written from.
///
/// The source is this archive's own live recording, as in the criteria above —
/// but **live**, which is the difference: it is never stopped, and the
/// publication writing to it stays open for the whole test.
#[test]
fn a_live_replication_merges_into_the_stream_it_is_copying() {
    let Some((mut media_driver, _archive_dir, mut client)) =
        rig("archive-client-api-replication-live")
    else {
        return;
    };

    let mut archive = Archive::connect(
        &context(),
        &mut client,
        &mut NoCredentials,
        Handlers {
            recording_signal: Some(collect_live_signal),
            ..Handlers::default()
        },
    )
    .expect("the archive is connected");

    // **The source is live**, and the live stream is a **dynamic** one: the
    // publication binds its control address alone and sends to whoever
    // announces itself, so the archive's recording subscription and the
    // replication's live destination can be two sockets of their own.
    //
    // That is the shape `archive_replay_merge.rs` arrived at for the same
    // reason, and the reason is worth repeating: a unicast publication binds
    // its endpoint as soon as it is connected, and a **live destination** is a
    // receive destination — it binds the address rather than joining the
    // endpoint already there, so a merge onto a unicast stream fails to add it
    // (*Address already in use*) wherever the source's own reader is on the
    // same host. The reference's test streams its live half dynamically too
    // (`ReplayMergeTest.java:81-88`).
    let [control_port, source_port, live_port] = free_udp_ports::<3>();
    let control = format!("127.0.0.1:{control_port}");
    let source_endpoint = format!("127.0.0.1:{source_port}");
    let live_endpoint = format!("127.0.0.1:{live_port}");

    let live_channel = format!("aeron:udp?control={control}|control-mode=dynamic|term-length=64k");

    // The channel the **archive records**: a subscriber of the dynamic
    // publication, which is an endpoint of its own and the control address to
    // talk to (`aeron_udp_channel.c:465-487`).
    let recording_channel =
        format!("aeron:udp?endpoint={source_endpoint}|control={control}|term-length=64k");

    // And what the merge is told to join: the same stream, read at a socket of
    // its own — which is what makes the two halves one image instead of two.
    let live_destination = format!("aeron:udp?endpoint={live_endpoint}|control={control}");

    let _source_subscription = archive
        .start_recording(
            &mut client,
            &recording_channel,
            LIVE_STREAM_ID,
            false,
            false,
        )
        .expect("the archive records the live stream");

    let live = client
        .add_exclusive_publication(&live_channel, LIVE_STREAM_ID, archive::COMMAND_TIMEOUT)
        .expect("the driver takes a publication on the live channel");

    until(&mut client, "the live publication linking", |client| {
        client
            .exclusive_publication(live)
            .and_then(deepmsg_client::publication::ExclusivePublication::is_connected)
            .unwrap_or(false)
    });

    let mut source = None;

    until(&mut client, "the source's row", |client| {
        let _ = client.offer_exclusive(live, b"a message on the live stream");
        source = the_only_recording(&mut archive, client);

        source.is_some()
    });

    let source = source.expect("the loop ends with a row");

    // **The replicate**, with a live destination and no stop position — the
    // shape `onRecordingDescriptor` sends to `SRC_RECORDING_POSITION`
    // (`:380-399`) and the only one that reaches the live pair.
    let mut params = ReplicationParams::new();
    params.replication_channel = REPLICATION_CHANNEL.to_owned();
    params.live_destination = live_destination.clone();

    let _replication_id = archive
        .replicate(
            &mut client,
            source.recording_id,
            archive::CONTROL_STREAM_ID,
            archive::CONTROL_CHANNEL,
            &params,
        )
        .expect("the archive takes the live replication");

    let mut copied = None;

    until(&mut client, "the destination's row", |client| {
        copied = the_copy(&mut archive, client, source.recording_id);

        copied.is_some()
    });

    let copied = copied.expect("the loop ends with a row");

    // **The channel the live pair is built on**, before it merges.
    //
    // A live-only replication is multi-destination (`isMds` is true for a live
    // destination alone), so the reference builds this channel with the tags
    // parameter too (`:539`) — and with no tags named that is `tags=-1,-1`,
    // which the C driver refuses outright and the Java driver reads as a real
    // tag of `-1`. This build leaves the parameter out, which is how the
    // reference's own client library says "no tags"
    // (`ChannelUriStringBuilder.tags(Long, Long)`,
    // `ChannelUriStringBuilder.java:1250-1264`); the reasoning and the
    // measurements are in `docs/compat.md`.
    let mut subscriptions: Vec<RecordingSubscriptionDescriptor> = Vec::new();

    until(&mut client, "the destination's subscription", |client| {
        subscriptions.clear();

        let _ = archive.list_recording_subscriptions(
            client,
            0,
            10,
            "",
            Some(LIVE_STREAM_ID),
            &mut |descriptor: &RecordingSubscriptionDescriptor| {
                subscriptions.push(descriptor.clone());
            },
        );

        subscriptions.iter().any(|subscription| {
            subscription
                .stripped_channel
                .contains("control-mode=manual")
        })
    });

    let extended = subscriptions
        .iter()
        .find(|subscription| {
            subscription
                .stripped_channel
                .contains("control-mode=manual")
        })
        .expect("the subscription the extend made");

    assert!(
        !extended.stripped_channel.contains("tags="),
        "a live-only replication names no tags, and does not write the null one: {}",
        extended.stripped_channel
    );
    assert!(
        !extended.stripped_channel.contains("endpoint="),
        "and it names no address of its own: {}",
        extended.stripped_channel
    );

    // **The merge.** The stream is kept moving while it is waited for, because
    // a merge is the copy *joining* it: `shouldStopReplay` waits for the image
    // to be level with the source and for the stream to be arriving from two
    // places, and a publication that goes quiet is neither.
    until(&mut client, "the merge", |client| {
        let _ = client.offer_exclusive(live, b"still live");
        let _ = archive.poll_for_recording_signals(client);

        LIVE_SIGNALS
            .lock()
            .expect("no other test is holding the live signals")
            .iter()
            .any(|signal| signal.signal == SignalCode::MERGE)
    });

    until(&mut client, "the replication's end", |client| {
        let _ = archive.poll_for_recording_signals(client);

        LIVE_SIGNALS
            .lock()
            .expect("no other test is holding the live signals")
            .iter()
            .any(|signal| signal.signal == SignalCode::REPLICATE_END)
    });

    let signals = LIVE_SIGNALS
        .lock()
        .expect("no other test is holding the live signals")
        .clone();

    let merged = signals
        .iter()
        .find(|signal| signal.signal == SignalCode::MERGE)
        .copied()
        .expect("a MERGE signal");

    assert_eq!(
        copied.recording_id, merged.recording_id,
        "the merge is about the copy"
    );
    assert_eq!(
        -1, merged.subscription_id,
        "and carries no subscription: `recordingSubscription = null` comes first (`:877`)"
    );
    assert_eq!(
        0,
        signals
            .iter()
            .filter(|signal| signal.signal == SignalCode::SYNC)
            .count(),
        "a live copy merges rather than syncing: no SYNC"
    );
    assert_eq!(
        1,
        signals
            .iter()
            .filter(|signal| signal.signal == SignalCode::REPLICATE_END)
            .count(),
        "and it ends once"
    );

    // **And the copy follows the stream.** This is the assertion the live
    // destination is load-bearing for: the recording the archive is writing is
    // fed by the subscription the replay was on, and after the merge the only
    // destination left on it is the live channel — so bytes offered **now**
    // moving the recording position is the stream itself arriving there.
    let before = archive
        .get_recording_position(&mut client, copied.recording_id)
        .expect("the copy's recording position");

    until(&mut client, "the copy to follow the stream", |client| {
        let _ = client.offer_exclusive(live, b"a message after the merge");

        archive
            .get_recording_position(client, copied.recording_id)
            .is_ok_and(|position| position > before)
    });

    archive.close(&client);
    media_driver.stop().expect("the driver stops");
}
