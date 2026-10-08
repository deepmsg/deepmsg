//! A client for our archive, written by hand.
//!
//! `crates/archive/src/client.rs` is a stub: the reference's `AeronArchive` —
//! the type a *user* of an archive holds — is a slice of its own and this
//! workspace has not built it. So a test that drives the archive writes its
//! requests with the generated codecs and reads the answers with them too, which
//! is what this module is.
//!
//! It was extracted from `tests/integration/archive_connect.rs`, which had all
//! of it inline. Two tests now need the same door, and the reason to share it is
//! the one [`crate::driver`] gives for the driver harness: a test that spells
//! out how to connect will get it wrong in the same way every time.
//!
//! # The channel shape is the reference client's, not an invention
//!
//! A response channel is not a channel either side picks; it is derived, and
//! the two sides have to agree on the derivation. The reference client gets
//! there in one step that looks arbitrary until the driver's side of it is read
//! (`AeronArchive.java:4043-4048`): it subscribes to the response channel
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
//! The rewrite of the request channel is done with [`ChannelUri`] rather than by
//! appending `&response-correlation-id=…` to the text, because a channel's
//! parameters are separated by `|` and not by `&` (`ChannelUri.java:333`, its
//! parser at `:424-456`). A hand-appended `&` is not a separator anywhere in
//! Aeron: it makes the parameter before it carry the rest of the string as its
//! value, so the id the chain is built on silently never appears — and the
//! connect times out with every channel looking right in the log.

#![allow(dead_code)] // one test uses what another does not

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

/// The channel the archive's local control subscription listens on —
/// `LOCAL_CONTROL_CHANNEL_DEFAULT` (`crates/archive/src/server/config.rs`).
///
/// A request publication has to land on this one, and on its stream: it is a
/// channel the archive opened, and a publication that does not reach it is a
/// request nobody reads.
pub const CONTROL_CHANNEL: &str = "aeron:ipc?term-length=64k";

/// The stream both control subscriptions are on
/// (`CONTROL_STREAM_ID_DEFAULT`).
pub const CONTROL_STREAM_ID: i32 = 10;

/// The channel a client asks to be answered on, and the stream.
pub const RESPONSE_CHANNEL: &str = "aeron:ipc?control-mode=response";
/// See [`RESPONSE_CHANNEL`].
pub const RESPONSE_STREAM_ID: i32 = 20;

/// How long anything in these tests may take.
pub const DEADLINE: Duration = Duration::from_secs(30);

/// How long a command to the driver may take.
pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// The properties every one of these tests starts the archive with.
///
/// `SHARED` because this driver's default is `DEDICATED` and the archiving media
/// driver drives the driver itself — it refuses a mode that would leave the work
/// on threads it does not own. And the control channel is left on so that both
/// control subscriptions on stream 10 are real; the client reaches the archive
/// over the IPC one, which is the shape P2-1c had to fix.
pub const PROPERTIES: [&str; 2] = [
    "-Daeron.threading.mode=SHARED",
    "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:0",
];

/// What a decoded `ControlResponse` is read for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// The session the answer is on.
    pub control_session_id: i64,
    /// The request it answers.
    pub correlation_id: i64,
    /// The id the answer is *about* — a recording, a session, or 0.
    pub relevant_id: i64,
    /// `OK`, `ERROR`, `RECORDING_UNKNOWN` or `SUBSCRIPTION_UNKNOWN`.
    pub code: ControlResponseCode,
    /// The message an `ERROR` carries, empty otherwise.
    pub error_message: Vec<u8>,
}

impl Response {
    /// The message as text, which is what a failing assertion wants to print.
    #[must_use]
    pub fn message(&self) -> String {
        String::from_utf8_lossy(&self.error_message).into_owned()
    }
}

/// One control session, and the two ends of the channel it answers on.
pub struct Session {
    request_publication: i64,
    response_subscription: i64,
    control_session_id: i64,
    next_correlation_id: i64,
}

impl Session {
    /// Open one control session, or say why not.
    ///
    /// # Errors
    ///
    /// The reason nothing was answered, with every counter the driver publishes
    /// printed after it — an archive that will not start and one that will not
    /// answer look the same from here, and the counters are what tells them
    /// apart.
    pub fn connect(
        client: &mut Client,
        aeron_dir: &Path,
        correlation_id: i64,
        deadline: Instant,
    ) -> Result<Self, String> {
        let response_subscription = client
            .add_subscription(RESPONSE_CHANNEL, RESPONSE_STREAM_ID, COMMAND_TIMEOUT)
            .map_err(|error| {
                format!("the driver would not take a response subscription: {error}")
            })?;

        let request_channel = request_channel(response_subscription);
        let request_publication = client
            .add_exclusive_publication(&request_channel, CONTROL_STREAM_ID, COMMAND_TIMEOUT)
            .map_err(|error| {
                format!("the driver would not take the request publication: {error}")
            })?;

        let mut session = Self {
            request_publication,
            response_subscription,
            control_session_id: -1,
            next_correlation_id: correlation_id,
        };

        let connect = connect_request(correlation_id, RESPONSE_STREAM_ID, RESPONSE_CHANNEL);
        let response = session
            .send(client, correlation_id, &connect, deadline)
            .map_err(|reason| format!("{reason};\n{}", counters(aeron_dir)))?;

        if response.code != ControlResponseCode::OK {
            return Err(format!(
                "the archive refused the connect: {}",
                response.message()
            ));
        }

        // The connect's `relevantId` is the **control session id**, not the
        // correlation id it echoes (`ControlSession.java:961-971`): the echoed id
        // is already `correlationId`, and the point of `relevantId` here is to
        // name the session every later request on this connection is addressed
        // to.
        session.control_session_id = response.relevant_id;

        Ok(session)
    }

    /// The session every request on this connection is addressed to.
    #[must_use]
    pub const fn control_session_id(&self) -> i64 {
        self.control_session_id
    }

    /// The next correlation id to build a request with, one per request.
    pub fn next_correlation_id(&mut self) -> i64 {
        let id = self.next_correlation_id;
        self.next_correlation_id += 1;
        id
    }

    /// The publication requests go out on, for a caller whose answer is not a
    /// `ControlResponse` — a listing's descriptor is one such
    /// (`ControlResponseProxy.java:54-89`).
    #[must_use]
    pub const fn request_publication(&self) -> i64 {
        self.request_publication
    }

    /// The subscription the answers come back on.
    #[must_use]
    pub const fn response_subscription(&self) -> i64 {
        self.response_subscription
    }

    /// Publish one request and wait for the answer that echoes its id.
    ///
    /// # Errors
    ///
    /// The reason nothing was answered — see [`Session::connect`].
    pub fn send(
        &mut self,
        client: &mut Client,
        correlation_id: i64,
        payload: &[u8],
        deadline: Instant,
    ) -> Result<Response, String> {
        self.send_only(client, payload, deadline)?;

        await_response(client, self.response_subscription, correlation_id, deadline)
    }

    /// Publish one request and do not wait for anything.
    ///
    /// # Errors
    ///
    /// The reason it could not be published.
    pub fn send_only(
        &mut self,
        client: &mut Client,
        payload: &[u8],
        deadline: Instant,
    ) -> Result<(), String> {
        // A publication that has not linked yet answers `NotConnected`, and the
        // archive's control subscription is a link like any other. The reference
        // client retries here for the same reason.
        let mut attempts = 0;

        loop {
            match client.offer_exclusive(self.request_publication, payload) {
                Some(Appended::Ok { .. }) => return Ok(()),
                Some(Appended::NotConnected) => {
                    attempts += 1;
                    if Instant::now() >= deadline {
                        return Err(format!(
                            "the request publication never linked to the archive's control \
                             subscription after {attempts} attempts"
                        ));
                    }
                    client.poll();
                    std::thread::sleep(Duration::from_millis(1));
                }
                other => return Err(format!("the request could not be published ({other:?})")),
            }
        }
    }
}

/// The request channel: the archive's control channel with the id of the
/// subscription the answer has to reach written into it.
///
/// `AeronArchive.java:4043-4048`, which is a `ChannelUri.put` for the reason the
/// module note gives.
#[must_use]
pub fn request_channel(response_subscription: i64) -> String {
    let mut uri = ChannelUri::parse(CONTROL_CHANNEL).expect("a channel this module wrote");

    uri.put("response-correlation-id", response_subscription.to_string());

    uri.build()
}

/// Read frames off `subscription` until `read` answers with something.
///
/// The general shape of waiting on this channel: what arrives is whatever the
/// archive sent — a control response, a recording signal, a descriptor emitted
/// by a listing session — and the caller is the one that knows which of them it
/// is waiting for.
///
/// # Errors
///
/// Which of the three faults happened, in words: nothing arrived, the answer
/// channel never formed an image, or the client does not hold the subscription.
pub fn await_frame<T>(
    client: &mut Client,
    subscription: i64,
    deadline: Instant,
    mut read: impl FnMut(&[u8]) -> Option<T>,
) -> Result<T, String> {
    let mut answer = None;
    let mut images = 0usize;
    let mut frames = 0usize;

    while answer.is_none() {
        if Instant::now() >= deadline {
            return Err(format!(
                "nothing arrived: {images} images formed and {frames} frames were read on \
                 subscription {subscription}"
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
            let mut buffer = vec![0u8; 4096];

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
                answer = read(&buffer);
            });
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    Ok(answer.expect("the loop only leaves with an answer"))
}

/// Poll the client until the control response for `correlation_id` arrives.
///
/// A response for **another** correlation id — a listing's descriptors and a
/// recording's signals share this channel — is read past rather than answered
/// with: what this waits for is one answer to one request.
///
/// # Errors
///
/// See [`await_frame`].
pub fn await_response(
    client: &mut Client,
    subscription: i64,
    correlation_id: i64,
    deadline: Instant,
) -> Result<Response, String> {
    await_frame(client, subscription, deadline, |payload| {
        decode(payload).filter(|response| response.correlation_id == correlation_id)
    })
}

/// Every counter the driver publishes.
///
/// What an archive that has taken a connect has that one which has not is here
/// and in few other places: the response publication, and the position the
/// archive's control subscription has read to.
#[must_use]
pub fn counters(aeron_dir: &Path) -> String {
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
#[must_use]
pub fn decode(payload: &[u8]) -> Option<Response> {
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
#[must_use]
pub fn connect_request(
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
