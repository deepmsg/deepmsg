//! P2-S1's acceptance: the archive's control plane, answered end to end.
//!
//! Everything the control plane had until this test was unit tests over a
//! fabricated adapter. Two of the defects this slice's own handover records —
//! a response channel built the wrong way round, and a default configuration
//! whose authorisation service could not be constructed at all — were invisible
//! to those and were found by starting the binary. This is the smallest thing
//! that starts it: our archiving media driver, our client, one connect.
//!
//! # It is not `interop/archive_connect_over_ipc.rs`
//!
//! That test is a *driver-side* differential probe: its Java probe launches the
//! **reference's** archive in its own process and compares two readings. Our
//! archive is not in it. This one is our archive or nothing.
//!
//! # Why it runs in CI rather than behind `interop`
//!
//! It needs no reference checkout — only this workspace's own binary, which
//! `cargo test --workspace` builds before it runs anything. That is the same
//! criterion the rest of `tests/integration/` is filed under, and unlike a
//! `required-features` target it is actually *executed* by CI, which only
//! compiles the interop ones.
//!
//! # The channel shape is the reference client's, not an invention
//!
//! A response channel is not a channel either side picks; it is derived, and
//! the two sides have to agree on the derivation. The reference client gets
//! there in one step that looks arbitrary until the driver's side of it is
//! read (`AeronArchive.java:4043-4048`): it subscribes to the response channel
//! **first**, then writes *that subscription's registration id* into the
//! request channel as `response-correlation-id`.
//!
//! From there the chain is three registration ids
//! (`aeron_driver_conductor.c:1711-1760`, implemented in this build at
//! `crates/driver/src/ipc_subscriptions.rs`):
//!
//! ```text
//! the archive's response publication
//!   names the request publication   (response-correlation-id = image.correlationId())
//!     which names the response subscription   (the id the client wrote)
//! ```
//!
//! So the archive answers on a channel it computes from the *response* channel
//! the client asked for — `conductor::response_channel`, stripped and rebuilt
//! with the client's term length, sparse flag and MTU — and the driver pairs
//! the two ends by id rather than by channel text.
//!
//! The rewrite of the request channel is done with [`ChannelUri`] rather than
//! by appending `&response-correlation-id=…` to the text, because a channel's
//! parameters are separated by `|` and not by `&` (`ChannelUri.java:333`, its
//! parser at `:424-456`). A hand-appended `&` is not a separator anywhere in
//! Aeron: it makes the parameter before it carry the rest of the string as its
//! value, so the id the chain is built on silently never appears — and the
//! connect times out with every channel looking right in the log.
//!
//! The UDP control subscription is deliberately left in place. The archive
//! serves two control subscriptions on stream 10 — one UDP, one local IPC —
//! and connecting over the IPC one while the UDP one is live is exactly the
//! shape P2-1c had to fix. This test connects over IPC.

use std::path::Path;
use std::time::{Duration, Instant};

use deepmsg_archive::server::response_proxy::PROTOCOL_SEMANTIC_VERSION;
use deepmsg_client::client::Client;
use deepmsg_codec::archive::auth_connect_request_codec::AuthConnectRequestEncoder;
use deepmsg_codec::archive::control_response_code::ControlResponseCode;
use deepmsg_codec::archive::control_response_codec::{self, ControlResponseDecoder};
use deepmsg_codec::archive::message_header_codec::{self, MessageHeaderDecoder};
use deepmsg_codec::archive::{ReadBuf, WriteBuf};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::uri::ChannelUri;
use deepmsg_tests::archiving_driver::{self, OwnArchivingMediaDriver};
use deepmsg_tests::temp::TempDir;

/// The channel the archive's local control subscription listens on —
/// `LOCAL_CONTROL_CHANNEL_DEFAULT` (`crates/archive/src/server/config.rs`).
///
/// The request publication has to land on this one, and on its stream: it is a
/// channel the archive opened, and a publication that does not reach it is a
/// connect nobody hears.
const CONTROL_CHANNEL: &str = "aeron:ipc?term-length=64k";

/// `AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT`, which both control
/// subscriptions share.
const CONTROL_STREAM_ID: i32 = 10;

/// The channel the client asks to be answered on, and subscribes to before it
/// asks (`AeronArchive.java:4043`).
const RESPONSE_CHANNEL: &str = "aeron:ipc?control-mode=response";

/// `AeronArchive.Configuration.CONTROL_RESPONSE_STREAM_ID_DEFAULT`.
const RESPONSE_STREAM_ID: i32 = 20;

/// The connect's correlation id, which its answer echoes. Any number will do;
/// one that is recognisable makes a failure readable.
const CORRELATION_ID: i64 = 0x5ed1_0001;

/// How long the whole exchange may take.
const DEADLINE: Duration = Duration::from_secs(30);

/// How long a client command may wait for the driver.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn the_control_plane_answers_a_connect() {
    let archive_dir = TempDir::new("archive-connect-archive");

    let properties = [
        // This driver's default is `DEDICATED`; the archiving media driver
        // drives the driver itself and refuses a mode that would leave the
        // work on the threads it does not own.
        "-Daeron.threading.mode=SHARED",
        // Left on, so that the two control subscriptions on stream 10 are both
        // real. The client below reaches the archive over the IPC one.
        "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:0",
    ];

    let Some(mut media_driver) =
        OwnArchivingMediaDriver::start("archive-connect", archive_dir.path(), &properties)
    else {
        archiving_driver::announce_skip();
        return;
    };

    media_driver
        .await_ready(DEADLINE)
        .expect("the archiving media driver must come up and signal its mark file");

    let mut client = Client::connect(media_driver.aeron_dir()).expect("connect to the driver");

    // The response subscription first: its registration id is what the request
    // publication has to name. See the module note.
    let response_subscription = client
        .add_subscription(RESPONSE_CHANNEL, RESPONSE_STREAM_ID, COMMAND_TIMEOUT)
        .expect("the driver must accept a response subscription");

    let request_channel = request_channel(response_subscription);
    let request_publication = client
        .add_exclusive_publication(&request_channel, CONTROL_STREAM_ID, COMMAND_TIMEOUT)
        .expect("the driver must accept the request publication");

    let connect = connect_request(CORRELATION_ID, RESPONSE_STREAM_ID, RESPONSE_CHANNEL);

    // A publication that has not linked yet answers `NotConnected`, and the
    // archive's control subscription is a link like any other. The reference
    // client retries here for the same reason.
    let deadline = Instant::now() + DEADLINE;
    let mut attempts = 0;

    loop {
        match client.offer_exclusive(request_publication, &connect) {
            Some(Appended::Ok { .. }) => break,
            Some(Appended::NotConnected) => {
                attempts += 1;
                assert!(
                    Instant::now() < deadline,
                    "the request publication never linked to the archive's control \
                     subscription after {attempts} attempts;\n{}",
                    counters(media_driver.aeron_dir())
                );
                client.poll();
                std::thread::sleep(Duration::from_millis(1));
            }
            other => panic!("the connect could not be published ({other:?})"),
        }
    }

    let response =
        await_response(&mut client, response_subscription, deadline).unwrap_or_else(|reason| {
            panic!(
                "{reason};\n{}\n--- the archiving media driver ---\n{}",
                counters(media_driver.aeron_dir()),
                media_driver.log_tail(60)
            )
        });

    assert_eq!(
        ControlResponseCode::OK,
        response.code,
        "the archive refused the connect: {}",
        String::from_utf8_lossy(&response.error_message)
    );
    assert_eq!(
        CORRELATION_ID, response.correlation_id,
        "the answer must be for the connect that asked"
    );
    // The connect's `relevantId` is the **control session id**, not the
    // correlation id it echoes (`ControlSession.java:961-971`): the echoed id
    // is already `correlationId`, and the point of `relevantId` here is to name
    // the session every later request on this connection is addressed to. A
    // fresh archive hands out its first one, so what is checked is that there
    // is one and that the answer's own session field agrees with it.
    assert!(
        response.control_session_id >= 0 && response.relevant_id == response.control_session_id,
        "the answer names control session {} as its relevant id, on session {}",
        response.relevant_id,
        response.control_session_id
    );

    let _ = media_driver.stop();
}

/// The request channel: the archive's control channel with the id of the
/// subscription the answer has to reach written into it.
///
/// `AeronArchive.java:4043-4048`, which is a `ChannelUri.put` for the reason
/// the module note gives.
fn request_channel(response_subscription: i64) -> String {
    let mut uri = ChannelUri::parse(CONTROL_CHANNEL).expect("a channel this test wrote");

    uri.put("response-correlation-id", response_subscription.to_string());

    uri.build()
}

/// What a decoded `ControlResponse` is read for.
struct Response {
    control_session_id: i64,
    correlation_id: i64,
    relevant_id: i64,
    code: ControlResponseCode,
    error_message: Vec<u8>,
}

/// Poll the client until a control response arrives on `subscription`.
///
/// The three ways this can come to nothing are kept apart on purpose: an
/// answer that never arrives, an answer channel that never forms an image, and
/// frames that are not control responses are three different faults, and a
/// bare "timed out" would name none of them.
fn await_response(
    client: &mut Client,
    subscription: i64,
    deadline: Instant,
) -> Result<Response, String> {
    let mut answer = None;
    let mut images = 0usize;
    let mut frames = 0usize;

    while answer.is_none() {
        if Instant::now() >= deadline {
            return Err(format!(
                "no control response arrived: {images} images formed and {frames} frames were \
                 read on subscription {subscription}"
            ));
        }

        client.poll();

        // The image is the archive's response publication, and it only exists
        // once the connect has been answered for — so its absence and the
        // absence of a response are the same wait.
        let image = match client.subscription(subscription) {
            Some(subscription) => subscription
                .images()
                .first()
                .map(deepmsg_client::image::Image::registration_id),
            None => {
                return Err(format!(
                    "the client holds no subscription with registration id {subscription}; \
                     it holds {:?} and reports {:?}",
                    client
                        .subscriptions()
                        .iter()
                        .map(deepmsg_client::subscription::Subscription::registration_id)
                        .collect::<Vec<_>>(),
                    client.error()
                ));
            }
        };

        if let Some(image) = image {
            images += 1;
            let mut buffer = vec![0u8; 1024];

            client.poll_image(subscription, image, 10, |fragment| {
                if answer.is_some() || !fragment.is_unfragmented() {
                    return;
                }

                let length = fragment.payload_length();
                buffer.resize(length, 0);
                assert!(
                    fragment.copy_payload(&mut buffer).is_some(),
                    "the frame fits"
                );
                frames += 1;
                answer = decode(&buffer);
            });
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    Ok(answer.expect("the loop only leaves with an answer"))
}

/// Every counter the driver publishes. What an archive that has taken a
/// connect has that one which has not, is here and in few other places: the
/// response publication, and the position the archive's control subscription
/// has read to.
fn counters(aeron_dir: &Path) -> String {
    let Ok(cnc) = deepmsg_cnc::CncFile::try_open(aeron_dir) else {
        return "--- counters: the CnC file will not open ---".to_owned();
    };
    let Some(counters) = cnc.counters() else {
        return "--- counters: the region will not read ---".to_owned();
    };

    let mut out = String::from("--- counters ---\n");

    counters.for_each(|descriptor| {
        out.push_str(&format!(
            "  [{}] {} = {:?}\n",
            descriptor.counter_id,
            descriptor.label,
            counters.value(descriptor.counter_id)
        ));
    });

    out
}

/// Decode one `ControlResponse`, or `None` if the bytes are not one.
fn decode(payload: &[u8]) -> Option<Response> {
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);

    if header.template_id() != control_response_codec::SBE_TEMPLATE_ID {
        return None;
    }

    let mut decoder = ControlResponseDecoder::default().header(header, 0);
    let coordinates = decoder.error_message_decoder();

    Some(Response {
        control_session_id: decoder.control_session_id(),
        correlation_id: decoder.correlation_id(),
        relevant_id: decoder.relevant_id(),
        code: decoder.code(),
        error_message: decoder.error_message_slice(coordinates).to_vec(),
    })
}

/// The `AuthConnectRequest` a client opens a control session with.
///
/// `version`'s **major** is the whole of what the archive checks
/// (`ArchiveConductor.java:483-488`), so this sends the archive's own semantic
/// version rather than a number of its own choosing.
fn connect_request(
    correlation_id: i64,
    response_stream_id: i32,
    response_channel: &str,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 1024];

    let length = {
        let encoder = AuthConnectRequestEncoder::default().wrap(
            WriteBuf::new(&mut buffer),
            message_header_codec::ENCODED_LENGTH,
        );
        let mut header = encoder.header(0);
        let mut encoder = header
            .parent()
            .expect("the encoder the header was wrapped on");

        encoder
            .correlation_id(correlation_id)
            .response_stream_id(response_stream_id)
            .version(PROTOCOL_SEMANTIC_VERSION)
            .response_channel(response_channel.as_bytes())
            .encoded_credentials(&[])
            .client_info(&[]);

        message_header_codec::ENCODED_LENGTH + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}
