//! **A replication whose source is the reference's own archive.**
//!
//! Every other criterion this slice has is our build talking to our build — one
//! process or two, it is one implementation on both ends, and a protocol that
//! two copies of the same code agree about is not yet a protocol. What the
//! reference's suite asks of a replication is the other thing: a session that
//! connects to a **Java** archive, asks it for a descriptor, extends a recording
//! on it, replays from it, and finds the bytes where it expected them.
//!
//! That is what this is. The source is `io.aeron.archive.ArchivingMediaDriver`
//! out of the reference checkout (the harness `java_archive_id.rs` introduced);
//! the destination is this build's archiving media driver; the client that asks
//! for the copy is the product's. The replication channel, the source's control
//! channel and the response channel the session derives are all UDP, because
//! that is the only kind of channel an archive can be reached on — and the last
//! of them is the shape the plan's §8 settled on, carried to a **different
//! implementation** for the first time.
//!
//! Run with `cargo test -p deepmsg-tests --features interop`. When the reference
//! build is not there this prints `SKIPPED` and passes, like the rest of this
//! directory.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use deepmsg_archive::client::archive::{Archive, Handlers, RecordingSignal};
use deepmsg_archive::client::context::{CONTROL_CHANNEL_ENV, CONTROL_RESPONSE_CHANNEL_ENV};
use deepmsg_archive::client::descriptor_poller::RecordingDescriptor;
use deepmsg_archive::client::proxy::ReplicationParams;
use deepmsg_archive::client::subscription_descriptor_poller::RecordingSubscriptionDescriptor;
use deepmsg_archive::client::{ArchiveContext, NoCredentials};
use deepmsg_client::client::Client;
use deepmsg_codec::archive::recording_signal::RecordingSignal as SignalCode;
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::java_archiving_driver::{self, JavaArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// Where the recording to be copied goes. The recording is made **through the
/// reference archive's own client path**, in its own driver, so IPC is what it
/// can use.
const RECORDING_CHANNEL: &str = "aeron:ipc?alias=interop-replication";
const RECORDING_STREAM_ID: i32 = 4413;

/// The channel the **copy** runs over, which must be UDP: `REPLAY` puts the
/// destination subscription's resolved endpoint on the channel it asks the
/// source to replay onto, and an IPC subscription has no endpoint to resolve.
const REPLICATION_CHANNEL: &str = "aeron:udp?endpoint=localhost:0";

/// The reference archive's own control stream default.
const CONTROL_STREAM_ID: i32 = 10;

/// A port for this run, so that two test processes do not take the same one.
fn port(offset: u32) -> u32 {
    13_000 + (std::process::id() % 400) * 2 + offset
}

/// Where the **reference** archive listens for clients — UDP, and it has to be:
/// a Java archive refuses anything else outright (`Archive.java:1225`).
fn source_control_channel() -> String {
    format!("aeron:udp?endpoint=localhost:{}", port(0))
}

/// What our client asks to be answered on **by the reference archive**.
///
/// A concrete port, and no `control-mode=response`: this client is the source's
/// own, and a *network* publication that carries a response-correlation-id has
/// to name a **network** response subscription
/// (`aeron_driver_conductor_validate_response_subscription`, `:631-651`). Plain
/// is also what `java_archive_id.rs` uses against the same archive.
fn source_response_channel() -> String {
    format!("aeron:udp?endpoint=localhost:{}", port(1))
}

/// The context the **reference** archive is reached with.
fn source_context() -> ArchiveContext {
    ArchiveContext::resolve(&[
        (CONTROL_CHANNEL_ENV.to_owned(), source_control_channel()),
        (
            CONTROL_RESPONSE_CHANNEL_ENV.to_owned(),
            source_response_channel(),
        ),
    ])
}

/// The context **our** archive is reached with: its own local control channel,
/// and the response channel the fixture uses.
fn destination_context() -> ArchiveContext {
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

fn until(client: &mut Client, what: &str, mut done: impl FnMut(&mut Client) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        if done(client) {
            return;
        }

        assert!(Instant::now() < deadline, "{what} never happened");
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The one row a listing of `archive` holds, or `None` while there is not
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

/// **A recording on `archive`, written to and stopped**, and the row it leaves —
/// the source's three requirements, in the order a replication meets them.
fn a_stopped_recording(archive: &mut Archive, client: &mut Client) -> RecordingDescriptor {
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

    archive
        .stop_recording_subscription(client, subscription_id)
        .expect("the archive stops the subscription it opened");

    until(client, "the source's stop position", |client| {
        archive
            .get_stop_position(client, source.recording_id)
            .is_ok_and(|stop| stop >= 0)
    });

    // The publisher goes back before anything copies this: a subscription that
    // names the source's session is fed by a publisher still open on that
    // stream, and then "the copy has data in it" would be measuring that.
    client
        .remove_publication(publication, archive::COMMAND_TIMEOUT)
        .expect("the source publication is given back");

    the_only_recording(archive, client).expect("still one row")
}

/// What this test's signal handler saw.
static SIGNALS: Mutex<Vec<RecordingSignal>> = Mutex::new(Vec::new());

fn collect_signal(signal: &RecordingSignal) {
    SIGNALS
        .lock()
        .expect("no other test is holding the signals")
        .push(*signal);
}

/// **Our archive copies a recording that lives in the reference's archive.**
#[test]
fn a_replication_copies_a_recording_out_of_the_reference_archive() {
    let source_archive_dir = TempDir::new("interop-replication-java-source");
    let control_property = format!(
        "-Daeron.archive.control.channel={}",
        source_control_channel()
    );

    let Some(mut source_driver) = JavaArchivingMediaDriver::start(
        "interop-replication-java-source",
        source_archive_dir.path(),
        None,
        &[control_property.as_str()],
    ) else {
        java_archiving_driver::announce_skip();
        return;
    };

    source_driver
        .await_ready(archive::DEADLINE)
        .expect("the reference archive comes up");

    let mut source_client =
        Client::connect(source_driver.aeron_dir()).expect("connect to the reference driver");

    let mut source_archive = Archive::connect(
        &source_context(),
        &mut source_client,
        &mut NoCredentials,
        Handlers::default(),
    )
    .expect("the reference archive is connected");

    let source = a_stopped_recording(&mut source_archive, &mut source_client);

    // **Our archive**, and the client that asks it for the copy.
    let destination_dir = TempDir::new("interop-replication-destination");
    let properties: Vec<&str> = archive::PROPERTIES.to_vec();

    let Some(mut media_driver) = OwnArchivingMediaDriver::start(
        "interop-replication-destination",
        destination_dir.path(),
        &properties,
    ) else {
        archiving_driver::announce_skip();
        return;
    };

    media_driver
        .await_ready(archive::DEADLINE)
        .expect("our archiving media driver comes up");

    let mut client = Client::connect(media_driver.aeron_dir()).expect("connect to our driver");

    let mut archive = Archive::connect(
        &destination_context(),
        &mut client,
        &mut NoCredentials,
        Handlers {
            recording_signal: Some(collect_signal),
            ..Handlers::default()
        },
    )
    .expect("our archive is connected");

    let mut params = ReplicationParams::new();
    params.replication_channel = REPLICATION_CHANNEL.to_owned();

    let replication_id = archive
        .replicate(
            &mut client,
            source.recording_id,
            CONTROL_STREAM_ID,
            &source_control_channel(),
            &params,
        )
        .expect("our archive takes the replication");

    // **The row the descriptor made**, from the reference's catalog.
    let mut copied = None;

    until(&mut client, "the destination's row", |client| {
        copied = the_only_recording(&mut archive, client);

        copied.is_some()
    });

    let copied = copied.expect("the loop ends with a row");

    assert_eq!(
        source.start_position, copied.start_position,
        "the descriptor's start came across"
    );
    assert_eq!(source.stream_id, copied.stream_id);
    assert_eq!(source.stripped_channel, copied.stripped_channel);
    assert_eq!(
        source.session_id, copied.session_id,
        "and the session the replay will arrive under"
    );

    // **The destination's recording subscription**, on the copy's channel and
    // under the source's session — what `EXTEND` writes onto it, asked of the
    // **reference** archive.
    let mut extended: Vec<RecordingSubscriptionDescriptor> = Vec::new();

    until(
        &mut client,
        "the destination's recording subscription",
        |client| {
            extended.clear();

            let _ = archive.list_recording_subscriptions(
                client,
                0,
                10,
                "",
                Some(RECORDING_STREAM_ID),
                &mut |descriptor: &RecordingSubscriptionDescriptor| {
                    extended.push(descriptor.clone());
                },
            );

            extended.iter().any(|subscription| {
                subscription
                    .stripped_channel
                    .contains(&format!("session-id={}", source.session_id))
            })
        },
    );

    // **And the session ends by saying so**, once.
    until(&mut client, "the replication's end", |client| {
        let _ = archive.poll_for_recording_signals(client);

        SIGNALS
            .lock()
            .expect("no other test is holding the signals")
            .iter()
            .any(|signal| signal.signal == SignalCode::REPLICATE_END)
    });

    let signals = SIGNALS
        .lock()
        .expect("no other test is holding the signals")
        .clone();

    let synced = signals
        .iter()
        .find(|signal| signal.signal == SignalCode::SYNC)
        .copied()
        .expect("a SYNC signal");

    assert!(
        synced.position >= source.stop_position,
        "the copy caught up with the reference's stop: {} against {}",
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

    // **And the bytes came out of the reference archive.** The destination's row
    // stops where the **source's** row stops, read on both archives after the
    // session is over.
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
        "and it stops exactly where the reference's recording stopped"
    );

    let _ = replication_id;

    archive.close(&client);
    source_archive.close(&source_client);

    media_driver.stop().expect("our driver stops");
}
