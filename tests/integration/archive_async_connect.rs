//! The archive client's connect, driven a turn at a time against a real archive.
//!
//! A state machine is its own kind of thing to test: the interesting claims are
//! not about one call but about an *order* — that the request carries the id of
//! the subscription the answer has to come back on, that a challenge is answered
//! on a new correlation id, that the deadline taken at the start is the one
//! enforced at the end. Two of those are decided before anything is sent and are
//! checked here without a driver; the rest need an archive, and ours is the one
//! they get — which is the whole point of the exercise, because a connect is
//! only correct if the *archive* agrees it is.

use std::time::{Duration, Instant};

use deepmsg_archive::client::async_connect::{AsyncConnect, NoCredentials, Polled};
use deepmsg_archive::client::context::{CONTROL_CHANNEL_ENV, CONTROL_RESPONSE_CHANNEL_ENV};
use deepmsg_archive::client::{ArchiveContext, ControlChannels};
use deepmsg_client::client::Client;
use deepmsg_core::uri::ChannelUri;
use deepmsg_tests::archive;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// `shouldResolveArchiveId`'s id (`aeron_archive_test.cpp:3395`), and the whole
/// reason that case exists: it is **wider than 32 bits**, so an `as i32`
/// anywhere on the path truncates it and the client answers with a different
/// archive than the one it is talking to.
const ARCHIVE_ID: i64 = 0x0423_6483_BEEF;

/// Drive the connect to its end, or answer `None` when it did not finish in
/// time.
fn drive(
    connect: &mut AsyncConnect,
    client: &mut Client,
) -> Result<Box<deepmsg_archive::client::async_connect::Connected>, String> {
    let deadline = Instant::now() + archive::DEADLINE;

    loop {
        match connect.poll(client, &mut NoCredentials) {
            Ok(Polled::Done(connected)) => return Ok(connected),
            Ok(Polled::Awaiting) => {}
            Err(error) => {
                return Err(format!(
                    "the connect failed: {error}; last state {:?}, correlation {}",
                    connect.state(),
                    connect.correlation_id()
                ));
            }
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "the connect did not finish; last state {:?}",
                connect.state()
            ));
        }

        // The driver's answers to the adds, and the archive's answer to the
        // connect, are all read through this client.
        client.poll();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// **The connect finishes against a real archive.**
///
/// Which is the only test that can say so: the archive has to accept the
/// connect request, name a control session, be asked which archive it is, and
/// answer that too. Nothing in this file's other cases touches the state
/// machine's two most fragile steps — the challenge branch and the version
/// branch — and this is where they either work or do not.
///
/// It is also `shouldResolveArchiveId` (`aeron_archive_test.cpp:3394-3406`) in
/// substance: a connect against a real archive, and the `archive_id` that comes
/// back being the one that archive was told it is — a value wider than 32 bits,
/// so nothing on the way may be an `i32`.
#[test]
fn the_connect_completes_against_an_archive() {
    let archive_dir = TempDir::new("archive-async-connect");
    // The archive is told which archive it is, because its default is `-1` —
    // which is also `AERON_NULL_VALUE`, the value a connect that never asked
    // leaves behind. Without this the assertion below could not tell "the
    // archive answered" from "the archive was never asked".
    let archive_id_property = format!("-Daeron.archive.id={ARCHIVE_ID}");
    let properties: Vec<&str> = archive::PROPERTIES
        .iter()
        .copied()
        .chain(std::iter::once(archive_id_property.as_str()))
        .collect();

    let Some(mut media_driver) =
        OwnArchivingMediaDriver::start("archive-async-connect", archive_dir.path(), &properties)
    else {
        archiving_driver::announce_skip();
        return;
    };

    media_driver
        .await_ready(archive::DEADLINE)
        .expect("the archiving media driver comes up");

    let mut client = Client::connect(media_driver.aeron_dir()).expect("connect to our driver");

    // The archive's own local control channel and stream, and an IPC response
    // channel in `control-mode=response` — which is the shape that makes the
    // request carry the subscription id rather than a session id.
    let context = ArchiveContext::resolve(&[
        (
            CONTROL_CHANNEL_ENV.to_owned(),
            archive::CONTROL_CHANNEL.to_owned(),
        ),
        (
            CONTROL_RESPONSE_CHANNEL_ENV.to_owned(),
            archive::RESPONSE_CHANNEL.to_owned(),
        ),
    ]);

    let mut connect = AsyncConnect::new(&context, &mut client, &mut NoCredentials)
        .expect("the context concludes");

    let connected = drive(&mut connect, &mut client).unwrap_or_else(|error| {
        panic!(
            "{error};\n{}\n--- counters ---\n{}",
            media_driver.log_tail(60),
            archive::counters(media_driver.aeron_dir())
        )
    });

    assert!(
        connected.control_session_id >= 0,
        "the archive named a session, and it is not the `-1` a connect starts at: {}",
        connected.control_session_id
    );
    assert_eq!(
        ARCHIVE_ID, connected.archive_id,
        "the archive answered which archive it is — the wide id, whole"
    );
    assert_eq!(
        deepmsg_archive::client::async_connect::ConnectState::Done,
        connect.state()
    );

    // What it assembled is usable: the proxy already carries the session, and
    // the poller is over the subscription the answers come back on.
    assert_eq!(
        connected.control_session_id,
        connected.proxy.control_session_id()
    );
    assert_eq!(
        connected.subscription,
        connected.control_response_poller.subscription()
    );

    media_driver.stop().expect("the driver stops");
}

/// **A response-mode response channel makes the request name the subscription.**
///
/// The one decision `AsyncConnect::new` makes before anything is sent, and it is
/// decided from *two* channels: the response channel says `control-mode=response`
/// and the request channel is the one that changes. Which is why it is worth a
/// case of its own — getting it backwards is a request that goes to an archive
/// with nowhere to answer.
#[test]
fn a_response_mode_request_carries_the_subscription_id() {
    let channels = ControlChannels {
        request: "aeron:ipc?term-length=64k".to_owned(),
        response: "aeron:ipc?control-mode=response".to_owned(),
    };

    let request =
        deepmsg_archive::client::async_connect::check_and_setup_response_channel(&channels, 4242)
            .expect("a channel");

    let request = ChannelUri::parse(&request).expect("a channel");
    assert_eq!(Some("4242"), request.get("response-correlation-id"));
    assert_eq!(
        Some("64k"),
        request.get("term-length"),
        "and what was there stays"
    );
}

/// **And a request that already knows where the answer goes is left alone.**
///
/// A non-response-mode response channel is a channel the *client* opened, and
/// the archive sends to the address the connect request named — so the request
/// channel carries nothing extra. Writing a correlation id into it here would
/// tell the archive to answer somewhere it was not asked to.
#[test]
fn a_request_is_left_alone_when_the_response_channel_is_the_clients_own() {
    let channels = ControlChannels {
        request: "aeron:ipc?term-length=64k".to_owned(),
        response: "aeron:udp?endpoint=localhost:0".to_owned(),
    };

    let request =
        deepmsg_archive::client::async_connect::check_and_setup_response_channel(&channels, 4242)
            .expect("a channel");

    assert_eq!(channels.request, request);
    assert_eq!(
        None,
        ChannelUri::parse(&request)
            .expect("a channel")
            .get("response-correlation-id")
    );
}

/// **A request channel that is not a channel is refused, and says so.**
///
/// The one way `check_and_setup_response_channel` fails. It matters more than it
/// looks: this runs *inside* `AsyncConnect::new`, after the response channel's
/// subscription has already been added — which is why the reference concludes
/// the context first (`:70-74`) and why a bad channel is a connect that never
/// starts rather than one that times out.
#[test]
fn a_request_channel_that_is_not_a_channel_is_refused() {
    let error = deepmsg_archive::client::async_connect::check_and_setup_response_channel(
        &ControlChannels {
            request: "not a channel".to_owned(),
            response: "aeron:ipc?control-mode=response".to_owned(),
        },
        1,
    )
    .expect_err("a request channel that is not a channel");

    assert!(
        error.contains("aeron"),
        "and the reason says what was wrong: {error}"
    );
}

/// A context with no channels is refused, which is what [`AsyncConnect::new`]
/// asks first.
///
/// Checked without a driver because it needs none: `conclude` is the refusal
/// half, and it is the half `new` calls before it touches the client.
#[test]
fn a_context_without_a_control_channel_is_refused() {
    assert!(ArchiveContext::new().conclude().is_err());
}
