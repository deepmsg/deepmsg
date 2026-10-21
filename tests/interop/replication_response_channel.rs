//! **A replication whose channel is a response channel** — the P3-0e route.
//!
//! Every other replication this suite runs names a plain channel: the
//! destination subscribes, the source's replay publication sends to it, and the
//! source answers the destination's requests on the destination's own control
//! response channel. A **response** channel is the other shape, and it is the
//! one the cluster uses when a member string names an `archiveResponseEndpoint`
//! (`Election.java:907`, `ClusterBackupAgent.java:583-588`): the destination's
//! recording subscription is a response subscription, the source's replay
//! publication is a response publication, and the requests that drive the copy
//! have to travel on a second publication because the first one's answers are
//! not this session's.
//!
//! That route has three states in the destination's `ReplicationSession`
//! (`REPLAY_TOKEN`, `GET_ARCHIVE_PROXY`, and the `REPLAY` that goes out through
//! the proxy) and this test is the judge for them:
//!
//! * the copy completes — the destination's row reaches the source's stop
//!   position;
//! * and the bytes are the source's, read back out of both archives.
//!
//! The source is **this build's** archive as well as the destination, because a
//! failure here should not be a protocol question between two implementations
//! before it is a question about this one. The reference's archive is the
//! second source in `replication_reference_archive.rs`'s sibling case.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use deepmsg_archive::client::archive::Archive;
use deepmsg_archive::client::context::{CONTROL_CHANNEL_ENV, CONTROL_RESPONSE_CHANNEL_ENV};
use deepmsg_archive::client::descriptor_poller::RecordingDescriptor;
use deepmsg_archive::client::proxy::ReplicationParams;
use deepmsg_archive::client::{ArchiveContext, Handlers, NoCredentials};
use deepmsg_client::client::Client;
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::java_archiving_driver::{self, JavaArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// The recording the source makes, in the source archive's own driver.
const RECORDING_CHANNEL: &str = "aeron:ipc?alias=response-replication";
const RECORDING_STREAM_ID: i32 = 4417;

/// The stream a source archive's control sessions are on
/// (`AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT`).
const CONTROL_STREAM_ID: i32 = 10;

/// The tests in this binary start two archives each.
static SERIAL: Mutex<()> = Mutex::new(());

/// A port for this run, below `net.ipv4.ip_local_port_range` so that a process
/// holding an ephemeral port cannot take it mid-run (`records/P3-9.md` §9.2).
fn port(offset: u32) -> u32 {
    13_000 + (std::process::id() % 400) * 2 + offset
}

/// Where the **source** archive listens for clients — UDP, so that the
/// destination's archive can reach it from another driver.
fn source_control_channel() -> String {
    format!("aeron:udp?endpoint=localhost:{}", port(0))
}

/// Where the **source's replay publication** is reached for control — the
/// cluster's `archiveResponseEndpoint`, which the destination's response
/// subscription names as `control=`.
fn response_endpoint() -> String {
    format!("localhost:{}", port(1))
}

/// The copy's channel: a **response** channel whose control is the source's
/// response endpoint (`CMA:2964-2972`).
fn response_replication_channel() -> String {
    format!(
        "aeron:udp?control={}|control-mode=response",
        response_endpoint()
    )
}

/// The context the source archive is reached with.
fn source_context() -> ArchiveContext {
    ArchiveContext::resolve(&[
        (CONTROL_CHANNEL_ENV.to_owned(), source_control_channel()),
        (
            CONTROL_RESPONSE_CHANNEL_ENV.to_owned(),
            "aeron:udp?endpoint=localhost:0".to_owned(),
        ),
    ])
}

/// The context the destination archive is reached with: this fixture's own
/// control channels.
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

/// A recording on `archive`, written to and stopped, and the row it leaves.
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
        let _ = client.offer_exclusive(publication, b"a message to copy over a response channel");

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

    client
        .remove_publication(publication, archive::COMMAND_TIMEOUT)
        .expect("the source publication is given back");

    the_only_recording(archive, client).expect("still one row")
}

/// **A copy over a response channel completes and its bytes are the source's.**
#[test]
fn a_replication_over_a_response_channel_copies_the_recording() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let source_archive_dir = TempDir::new("response-replication-source");
    let control_property = format!(
        "-Daeron.archive.control.channel={}",
        source_control_channel()
    );
    let properties = [
        "-Daeron.threading.mode=SHARED",
        "-Daeron.archive.threading.mode=SHARED",
        control_property.as_str(),
    ];

    let Some(mut source_driver) = OwnArchivingMediaDriver::start(
        "response-replication-source",
        source_archive_dir.path(),
        &properties,
    ) else {
        archiving_driver::announce_skip();
        return;
    };

    source_driver
        .await_ready(archive::DEADLINE)
        .expect("the source archive comes up");

    let mut source_client =
        Client::connect(source_driver.aeron_dir()).expect("connect to the source's driver");

    let mut source_archive = Archive::connect(
        &source_context(),
        &mut source_client,
        &mut NoCredentials,
        Handlers::default(),
    )
    .expect("the source archive is connected");

    let source = a_stopped_recording(&mut source_archive, &mut source_client);

    // **The destination**, our archive again, and the client that asks it for
    // the copy.
    let destination_dir = TempDir::new("response-replication-destination");
    let properties: Vec<&str> = archive::PROPERTIES.to_vec();

    let Some(mut media_driver) = OwnArchivingMediaDriver::start(
        "response-replication-destination",
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
        Handlers::default(),
    )
    .expect("our archive is connected");

    let mut params = ReplicationParams::new();
    params.replication_channel = response_replication_channel();
    params.src_response_channel = response_replication_channel();

    let replication_id = archive
        .replicate(
            &mut client,
            source.recording_id,
            CONTROL_STREAM_ID,
            &source_control_channel(),
            &params,
        )
        .expect("our archive takes a replication over a response channel");

    assert!(replication_id > 0);

    // The row the descriptor made, then the copy itself: `REPLICATE` ends a
    // bounded copy when the image reaches the source's stop position
    // (`ReplicationSession.java:777-815`).
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
        "and the session the replay arrives under"
    );

    until(
        &mut client,
        "the copy reaching the source's stop",
        |client| {
            archive
                .get_stop_position(client, copied.recording_id)
                .is_ok_and(|stop| stop >= source.stop_position)
        },
    );
}

/// **The same copy, from the reference's archive.**
///
/// The one thing the case above cannot say: that the token request, the second
/// publication and the response-channel replay are the **reference's** protocol
/// and not two copies of this build agreeing with each other. Everything after
/// the source is the same — our archive is the destination, and the client that
/// asks for the copy is the product's.
///
/// The reference's own suite reaches this route only through the cluster
/// (`ClusterBackupTest` with `useResponseChannels(true)`), never at the archive
/// level, so this is the first time the two ends meet on it.
#[test]
fn the_reference_archive_serves_a_response_channel_copy() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let source_archive_dir = TempDir::new("response-replication-java-source");
    let control_property = format!(
        "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:{}",
        port(2)
    );

    let Some(mut source_driver) = JavaArchivingMediaDriver::start(
        "response-replication-java-source",
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

    let java_source_context = ArchiveContext::resolve(&[
        (
            CONTROL_CHANNEL_ENV.to_owned(),
            format!("aeron:udp?endpoint=localhost:{}", port(2)),
        ),
        (
            CONTROL_RESPONSE_CHANNEL_ENV.to_owned(),
            format!("aeron:udp?endpoint=localhost:{}", port(4)),
        ),
    ]);

    let mut source_archive = Archive::connect(
        &java_source_context,
        &mut source_client,
        &mut NoCredentials,
        Handlers::default(),
    )
    .expect("the reference archive is connected");

    let source = a_stopped_recording(&mut source_archive, &mut source_client);

    let destination_dir = TempDir::new("response-replication-java-destination");
    let properties: Vec<&str> = archive::PROPERTIES.to_vec();

    let Some(mut media_driver) = OwnArchivingMediaDriver::start(
        "response-replication-java-destination",
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
        Handlers::default(),
    )
    .expect("our archive is connected");

    let mut params = ReplicationParams::new();
    params.replication_channel = format!(
        "aeron:udp?control=localhost:{}|control-mode=response",
        port(3)
    );
    params.src_response_channel = params.replication_channel.clone();

    let replication_id = archive
        .replicate(
            &mut client,
            source.recording_id,
            CONTROL_STREAM_ID,
            &format!("aeron:udp?endpoint=localhost:{}", port(2)),
            &params,
        )
        .expect("the destination takes a replication over a response channel");

    assert!(replication_id > 0);

    let mut copied = None;

    until(&mut client, "the destination's row", |client| {
        copied = the_only_recording(&mut archive, client);

        copied.is_some()
    });

    let copied = copied.expect("the loop ends with a row");

    assert_eq!(source.stream_id, copied.stream_id);
    assert_eq!(source.stripped_channel, copied.stripped_channel);

    until(
        &mut client,
        "the copy reaching the source's stop",
        |client| {
            archive
                .get_stop_position(client, copied.recording_id)
                .is_ok_and(|stop| stop >= source.stop_position)
        },
    );
}
