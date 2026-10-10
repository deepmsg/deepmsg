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
use deepmsg_archive::client::proxy::ReplayParams;
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
            .list_recordings(&mut client, 0, 10, collect_descriptor)
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
            .list_recordings(&mut client, 0, 10, collect_narrowed)
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
            collect_narrowed,
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
            collect_narrowed,
        )
        .expect("the archive answers a listing that holds nothing");

    assert_eq!(0, nothing, "no recording is on that channel");

    // **By id**: a listing that asks for exactly one, so there is nothing to
    // page through and the count it asked in for is not the caller's to choose.
    forget_narrowed();
    let by_id = archive
        .list_recording(&mut client, recording.recording_id, collect_narrowed)
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
        .list_recording(&mut client, recording.recording_id + 999, collect_narrowed)
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
            collect_subscription,
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
            collect_subscription,
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
            .list_recording(&mut client, recording_id, collect_narrowed)
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
