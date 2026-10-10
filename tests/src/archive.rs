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
//!
//! # Two halves, and why they are one module
//!
//! The **client** half — [`Session`], [`Response`], the request builders — is
//! what a test needs to say anything to the archive. The **archive** half —
//! [`Archive`], [`Recording`], [`Recorded`], [`Replay`] — is a driver, a client
//! and a control session, and the four things a test does with them: make a
//! recording, replay it, read the frames, and ask the catalog where it stopped.
//!
//! They are here together because the second was extracted from
//! `tests/integration/archive_replay.rs`, which had all of it inline, at the
//! point a second scenario file needed it — and because the two halves are
//! always used together: what makes the archive half worth sharing *is* that
//! three files now spell the recording the same way.
//!
//! What is deliberately **not** here is anything a single scenario is about.
//! The segment requests are the segments test's, the listings are the queries
//! test's, and the properties a driver is started with are the calling test's:
//! this module knows how to run an archive, not what one is being asked.

#![allow(dead_code)] // one test uses what another does not

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

// The product's connected client is named `Archive` where it lives and the
// fixture's own running-archive harness is named `Archive` here, so the import
// is the one that moves aside.
use deepmsg_archive::client::{Archive as ArchiveClient, ArchiveContext, Handlers, NoCredentials};
use deepmsg_archive::server::conductor::ARCHIVE_ID_DEFAULT;
use deepmsg_archive::server::recording_pos::{find_counter_id_by_session, parse_key};
use deepmsg_archive::server::response_proxy::PROTOCOL_SEMANTIC_VERSION;
use deepmsg_client::client::Client;
use deepmsg_codec::archive::auth_connect_request_codec::AuthConnectRequestEncoder;
use deepmsg_codec::archive::boolean_type::BooleanType;
use deepmsg_codec::archive::bounded_replay_request_codec::BoundedReplayRequestEncoder;
use deepmsg_codec::archive::control_response_code::ControlResponseCode;
use deepmsg_codec::archive::control_response_codec::{self, ControlResponseDecoder};
use deepmsg_codec::archive::message_header_codec::{self, MessageHeaderDecoder};
use deepmsg_codec::archive::recording_signal::RecordingSignal;
use deepmsg_codec::archive::recording_signal_event_codec::{self, RecordingSignalEventDecoder};
use deepmsg_codec::archive::replay_request_codec::ReplayRequestEncoder;
use deepmsg_codec::archive::source_location::SourceLocation;
use deepmsg_codec::archive::start_recording_request_2_codec::StartRecordingRequest2Encoder;
use deepmsg_codec::archive::stop_position_request_codec::StopPositionRequestEncoder;
use deepmsg_codec::archive::stop_recording_subscription_request_codec::StopRecordingSubscriptionRequestEncoder;
use deepmsg_codec::archive::{ReadBuf, WriteBuf};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::uri::ChannelUri;

use crate::archiving_driver::{self, OwnArchivingMediaDriver};
use crate::temp::TempDir;

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
///
/// **A thin wrapper over the product's [`ArchiveClient`] now** (plan §2.3's
/// second step). The session, the publication requests go out on, the
/// subscription the answers come back on, and the encoding of both directions
/// are the product's; what is left here is the fixture's own correlation counter
/// and the one door the product has no equivalent of (see [`Session::send_only`]).
///
/// This is the shape the reference's own test base has: it holds an
/// `aeron_archive_t` and the `aeron_t` it came from, and it reaches for the raw
/// client only to make the **data-plane** channels a scenario records
/// (`AeronCArchiveTestBase::connect`, `aeron_archive_test.cpp`, ends in
/// `aeron_archive_connect`). It does not hand-build a control request anywhere.
pub struct Session {
    /// The product's archive: the control session, the two ends of the channel,
    /// and the waits.
    archive: ArchiveClient,
    /// The next correlation id this fixture hands out.
    ///
    /// **The fixture's own, and deliberately not the product's yet** (plan §2.3's
    /// step 3). The product draws correlation ids from the driver's command ring
    /// (`Archive::next_correlation_id` → `Client::next_correlation_id`, a
    /// `fetch_add` on the CnC's MPSC counter) and its `next_correlation_id` needs
    /// the client — so adopting it is a signature change at twenty call sites,
    /// and mixing that with a change of *ownership* would make a failure
    /// impossible to attribute to one or the other.
    ///
    /// The two counters coexist safely because they live in different ranges and
    /// the ring's is the low one: the ring starts at nought and is at single
    /// digits on a driver this young, while every caller seeds this one with a
    /// `0x5ed1_000X`-shaped constant. Nothing else draws from the ring on this
    /// path either — the connect takes one, and the product's own request methods
    /// take one each, and neither is called here.
    next_correlation_id: i64,
}

impl Session {
    /// Open one control session, or say why not.
    ///
    /// **This is the product's connect now** (plan §2.3's second step):
    /// `Archive::connect` builds the response subscription and the request
    /// publication, runs the ten-state `AsyncConnect` over them, and hands back
    /// an [`ArchiveClient`]. The two channels the fixture used to open by hand —
    /// and the `response-correlation-id` derivation it used to do — are the
    /// product's, from the context below.
    ///
    /// `correlation_id` **no longer names the connect**. `AsyncConnect` takes the
    /// connect's own correlation id from the driver's command ring, which is what
    /// the reference does (`aeron_archive_client.c:408-411` for the counter, and
    /// no entry point lets a caller choose one), so the parameter survives only
    /// as the seed for [`Session::next_correlation_id`]. The callers' constants
    /// are unchanged and still name a request; they just no longer also name the
    /// connect.
    ///
    /// `deadline` is still the caller's budget, but **it is handed to the context
    /// rather than to the connect** — because that is where the reference keeps
    /// it, and because it is what every *later* wait on this session will read
    /// too. See [`connect_context`].
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
        let context = connect_context(deadline);

        let archive =
            ArchiveClient::connect(&context, client, &mut NoCredentials, Handlers::default())
                .map_err(|error| format!("{error};\n{}", counters(aeron_dir)))?;

        Ok(Self {
            archive,
            next_correlation_id: correlation_id,
        })
    }

    /// The session every request on this connection is addressed to.
    #[must_use]
    pub const fn control_session_id(&self) -> i64 {
        self.archive.control_session_id()
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
        self.archive.proxy().request_publication()
    }

    /// The subscription the answers come back on.
    #[must_use]
    pub const fn response_subscription(&self) -> i64 {
        self.archive.subscription()
    }

    /// Publish one request and wait for the answer that echoes its id.
    ///
    /// **The wait is the product's now**: `Archive::wait_for_response`, which is
    /// the reference's `aeron_archive_poll_for_response` — so the answer is read
    /// by the product's `ControlResponsePoller`, the `relevantId` is what comes
    /// back, and a refusal is an [`ArchiveError::Refused`] carrying the archive's
    /// own text rather than a `Response` this module decoded.
    ///
    /// **`deadline` no longer governs the wait**, and that is the reference's
    /// arrangement rather than a loss: every wait in `aeron_archive_client.c`
    /// starts from `ctx->message_timeout_ns` and no entry point takes a caller's
    /// (`:156` and its siblings). The budget is set once, in
    /// [`connect_context`]. The parameter survives because the *offer* still
    /// needs it — see [`Session::send_only`].
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
    ) -> Result<i64, String> {
        self.send_only(client, payload, deadline)?;

        self.archive
            .wait_for_response(client, "a request the fixture built", correlation_id)
            .map_err(|error| error.to_string())
    }

    /// Publish one request and do not wait for anything.
    ///
    /// **The one door the product has no equivalent of, and it is why this
    /// method still touches the publication directly.** Every request the
    /// product's proxy can send is a *named* one — `start_recording`,
    /// `list_recordings`, `get_stop_position` and the rest — so there is nowhere
    /// to hand it a payload this module built. The reference has the same gap and
    /// the same answer: its test reaches past the client to the proxy itself
    /// (`aeron_archive_proxy_get_start_position(archive->archive_proxy, …)`,
    /// `aeron_archive_test.cpp:1117`), and the proxy is where a named request
    /// gets its bytes. The calls that need this are the ones driving the server
    /// with a request the product has no method for yet — plan §2.3's third step
    /// is where they move, and this is what disappears when they have.
    ///
    /// It offers through the **product's** publication, read off
    /// [`Session::request_publication`], so the id is the one `Archive::connect`
    /// made rather than one this module made.
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
        // **Three answers mean *not now*, and they are the same three the
        // fixture's own publisher waits on** (see `offer_recorded_message`) and
        // the same ones the reference client retries: a publication that has not
        // linked yet answers `NotConnected`, one whose window the driver has not
        // advertised yet answers `BackPressured`, and one whose term just ran out
        // answers `EndOfLog`.
        //
        // **`BackPressured` used to be fatal here, and it cost a red CI run.**
        // The connect request is offered the moment the publication is made, and
        // a cold runner has not had the window advertised by then — so the very
        // first offer answered `BackPressured` and a fixture that treated it as
        // final failed instantly rather than waiting a millisecond. It is the
        // same shape of race the proxy fixture met in P2-C1's third commit, where
        // the rule was recorded as "a publication must be `is_connected` before
        // it is used"; waiting is the other way to keep it.
        let mut attempts = 0;

        loop {
            match client.offer_exclusive(self.archive.proxy().request_publication(), payload) {
                Some(Appended::Ok { .. }) => return Ok(()),
                Some(
                    retry @ (Appended::NotConnected | Appended::BackPressured | Appended::EndOfLog),
                ) => {
                    attempts += 1;
                    if Instant::now() >= deadline {
                        return Err(format!(
                            "the request would not go out in {attempts} attempts, the last \
                             answer {retry:?}"
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

/// The context the fixture connects with.
///
/// Three of these are the settings the fixture's own connect used to spell out
/// when it opened its two channels by hand — the archive's control channel and
/// its stream, and the channel it asks to be answered on and its stream. The
/// reference's own test base does the same thing with the same four values
/// (`AeronCArchiveTestBase::connect`: `set_control_request_channel` and
/// `set_control_response_channel`), which is why they live in the context now
/// rather than in the connect.
///
/// The fourth is the budget, and **it is the one setting here that is not merely
/// moved**. In the reference there is exactly one owner of a deadline —
/// `ctx->message_timeout_ns`, which every wait in `aeron_archive_client.c` starts
/// from (`:156` and its siblings) and which no entry point lets a caller
/// override; a test that wants another one sets it on the context
/// (`aeron_archive_test.cpp:1114` sets 500 ms for exactly that reason). The
/// fixture used to pass `DEADLINE` per call, on both the connect and every wait
/// after it. So the caller's deadline is handed over here, once, as whatever is
/// left of it — which for the callers that pass `Instant::now() + DEADLINE` is
/// the same 30 s they always had, and for any future caller that wants less is
/// the one place to say so.
///
/// It is the budget for the *adds* as well as the waits, since those take their
/// timeout from the same field: 30 s where `COMMAND_TIMEOUT` used to give 10.
/// More generous, and the direction the reference's own test base sits in.
fn connect_context(deadline: Instant) -> ArchiveContext {
    let mut context = ArchiveContext::resolve(&[]);

    context.control_request_channel = Some(CONTROL_CHANNEL.to_owned());
    context.control_request_stream_id = CONTROL_STREAM_ID;
    context.control_response_channel = Some(RESPONSE_CHANNEL.to_owned());
    context.control_response_stream_id = RESPONSE_STREAM_ID;
    context.message_timeout_ns = deadline
        .saturating_duration_since(Instant::now())
        .as_nanos()
        .try_into()
        .unwrap_or(i64::MAX);

    context
}

/// The request channel: the archive's control channel with the id of the
/// subscription the answer has to reach written into it.
///
/// `AeronArchive.java:4043-4048`, which is a `ChannelUri.put` for the reason the
/// module note gives.
///
/// **As of plan §2.3's second step this has no caller**: the derivation is the
/// product's — `check_and_setup_response_channel` in `async_connect`, reached
/// from `conclude_with` — and [`Session::connect`] no longer opens its own
/// channels. It is kept because it is the readable statement of a chain that is
/// otherwise three registration ids deep, and it goes with the other dead
/// constructors when §2.3's fourth step deletes them in one pass.
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
                if !fragment.is_unfragmented() {
                    return;
                }

                let length = fragment.payload_length();
                buffer.resize(length, 0);
                assert!(
                    fragment.copy_payload(&mut buffer).is_some(),
                    "the frame fits"
                );
                frames += 1;

                // **Every** fragment is handed to `read`, including the ones
                // after the answer — and the answer is kept, not overwritten.
                //
                // A fragment arrives in a poll of up to ten, so "the answer and
                // the frame behind it" is one call and not two, and what is
                // behind an answer is not always nothing: a recording signal
                // shares this channel with the answers, and a delete's is sent a
                // turn or two after the OK that started the delete
                // (`DeleteSegmentsSession.java:76-80`). A reader that stopped at
                // the answer would be handed the OK and never the signal.
                let found = read(&buffer);

                if answer.is_none() {
                    answer = found;
                }
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
/// **As of plan §2.3's second step this has no caller**: [`Session::send`] waits
/// through the product's `Archive::wait_for_response`, whose poller is what
/// recognises a response and whose loop is what skips the rest. This was the
/// fixture's version of it. It goes with the other dead members when §2.3's
/// fourth step deletes them.
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

/// The value of the first counter whose label starts with `prefix`, or `None`
/// when the client holds no such counter.
///
/// The labels are the reference's own (`Archive`'s `AeronCounters`), which is
/// what a reader of the counters region — `AeronStat`, or anybody's dashboard —
/// finds them by, so a test that reads one is reading what that reader reads.
///
/// # Panics
///
/// When the client does not hold a counters region at all, which is a driver
/// that did not come up rather than a counter that is not there.
#[must_use]
pub fn counter_by_label(client: &Client, prefix: &str) -> Option<i64> {
    let counters = client.counters_reader().expect("a counters region");
    let mut found = None;

    counters.for_each(|descriptor| {
        if descriptor.label.starts_with(prefix) {
            found = counters.value(descriptor.counter_id);
        }
    });

    found
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
///
/// **As of plan §2.3.1's first step this has no caller**: [`Session::connect`]
/// sends the connect through the product's `ArchiveProxy::try_connect` now, and
/// this is the hand-written twin that says what that request is supposed to
/// contain. It is kept rather than deleted for two reasons — it is the readable
/// statement of the request's field set, and §2.3's last step deletes the
/// fixture's constructors in one pass once every one of them has stopped being
/// called. The other five constructors below still have callers.
///
/// It is deliberately **not** used as a byte-for-byte cross-check on the proxy,
/// which is what a reader would expect of a twin: the two differ in exactly one
/// field, `clientInfo` (empty here, `name=… version=… commit=…` there —
/// `proxy.rs:254-257`), so such a check would fail by design.
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

// ---------------------------------------------------------------------------
// A running archive, and the client that drives it
// ---------------------------------------------------------------------------
//
// Everything below was `tests/integration/archive_replay.rs`'s, and it is here
// because a second scenario file needs the same door — which is the reason this
// module gives for itself above, and the reason [`crate::driver`] gives for the
// driver harness. A test that spells out how to start an archive, make a
// recording and read a replay will get it wrong in the same way every time, and
// the three request builders below are the ones two files now share.

/// `ArchiveConductor.ARCHIVE_ID_DEFAULT`: the id every recording-position
/// counter this archive allocates is keyed by.
pub const ARCHIVE_ID: i64 = ARCHIVE_ID_DEFAULT;

/// A running archiving media driver, the client connected to it, and the one
/// control session a test drives it through.
pub struct Archive {
    media_driver: OwnArchivingMediaDriver,
    /// The client every request is published through and every answer read
    /// through.
    pub client: Client,
    /// The control session those requests are addressed to.
    pub session: Session,
    /// The driver's own aeron directory, which is where its CnC file is — the
    /// one a counter is written through.
    aeron_dir: PathBuf,
    /// Every recording signal read on the way to an answer.
    ///
    /// A signal arrives on the same channel as the answers and is **not** the
    /// answer to anything the client is waiting for, so a reader that keeps only
    /// what it asked for loses it — and it is not sent again. A real client has
    /// the same problem and the same answer: `AeronArchive` polls this channel on
    /// a thread of its own and hands signals to a listener as they go past.
    signals: Vec<Signal>,
    /// Every answer read on the way to **another** answer.
    ///
    /// The same reason as [`Archive::signals`], one level up. This channel
    /// carries the answers of every request on the session, so two requests in
    /// flight — which is what a test of a refusal needs, because the state the
    /// refusal is about is made by the first — would otherwise have the first
    /// one's wait eat the second one's answer. Both are answered in one turn, so
    /// both frames are in one poll, and the second is the one behind the first.
    answers: Vec<Response>,
    /// A writable mapping of the counters region, which is how an application
    /// that owns a counter writes it: the client hands out a reader, because a
    /// counter is written by whoever asked for it through the address the
    /// driver gave them (`aeron_counter_set_release`).
    cnc: deepmsg_cnc::CncFile,
    /// The archive's own directory, and held rather than merely made: a
    /// `TempDir` removes its directory when it is dropped, and this is the one
    /// the archive asks `statvfs` about before every recording
    /// (`isLowStorageSpace`, `ArchiveConductor.java:2597-2617`) as well as the
    /// one its segment files are in. A dropped one is a start refused for want
    /// of room on a filesystem that is not there.
    archive_dir: TempDir,
}

impl Archive {
    /// Start one, or `None` when our archiving media driver is not built.
    ///
    /// `connect_correlation_id` is the id the connect is sent with and answered
    /// by; it is a parameter because every test file picks its own, and the
    /// properties are a parameter for the reason they exist: they are the
    /// question the test is asking (segment length, spies, limits).
    pub fn start(
        test_name: &str,
        connect_correlation_id: i64,
        properties: &[String],
    ) -> Option<Self> {
        let archive_dir = TempDir::new(test_name);

        let properties: Vec<&str> = properties.iter().map(String::as_str).collect();

        let Some(mut media_driver) =
            OwnArchivingMediaDriver::start(test_name, archive_dir.path(), &properties)
        else {
            archiving_driver::announce_skip();
            return None;
        };

        media_driver
            .await_ready(DEADLINE)
            .expect("the archiving media driver must come up and signal its mark file");

        let aeron_dir = media_driver.aeron_dir().to_path_buf();
        let mut client = Client::connect(&aeron_dir).expect("connect to the driver");

        let session = Session::connect(
            &mut client,
            &aeron_dir,
            connect_correlation_id,
            Instant::now() + DEADLINE,
        )
        .unwrap_or_else(|reason| panic!("{reason};\n{}", media_driver.log_tail(40)));

        let cnc = deepmsg_cnc::CncFile::try_open_writable(&aeron_dir)
            .expect("the CnC file this test's driver wrote");

        Some(Self {
            media_driver,
            client,
            session,
            aeron_dir,
            signals: Vec::new(),
            answers: Vec::new(),
            cnc,
            archive_dir,
        })
    }

    /// Stop the driver, which is also the end of the archive in it.
    ///
    /// # Panics
    ///
    /// When the process will not take the signal, which is a driver that has
    /// already gone.
    pub fn stop(&mut self) -> std::process::ExitStatus {
        self.media_driver.stop().expect("the driver stops")
    }

    /// The tail of the archiving media driver's output.
    ///
    /// What an archive that will not do something says about why is here and
    /// nowhere else: the requests it refuses are answered, and the ones it does
    /// not get to are not.
    pub fn log_tail(&mut self, lines: usize) -> String {
        self.media_driver.log_tail(lines)
    }

    /// The archive's own directory: where its segment files and its catalog
    /// file are, which a test that reads a file rather than a field needs.
    #[must_use]
    pub fn archive_dir(&self) -> &Path {
        self.archive_dir.path()
    }

    /// The segment files this archive holds for `recording_id`, as
    /// `(base position, path)` pairs in name order.
    ///
    /// Read off the **directory**, not off the catalog: the two disagreeing is
    /// exactly what a truncate's erase and a delete's file removal are about,
    /// and a test that asked the catalog would be asking the thing under test.
    #[must_use]
    pub fn segment_files(&self, recording_id: i64) -> Vec<(i64, PathBuf)> {
        deepmsg_archive::segment::segment_files(self.archive_dir(), recording_id)
    }

    /// A counters slot the archive will read as a limit, holding `value`.
    ///
    /// # Panics
    ///
    /// When the driver will not allocate one, or the region will not take the
    /// value.
    pub fn limit_counter(&mut self, type_id: i32, value: i64) -> deepmsg_client::counter::Counter {
        let counter = self
            .client
            .add_counter(
                type_id,
                &value.to_be_bytes(),
                "the archive acceptance tests' limit counter",
                COMMAND_TIMEOUT,
            )
            .expect("the driver must allocate the counter");

        let counters = self.cnc.counters_writable().expect("a writable region");
        assert!(counter.set_value(&counters, value), "the limit is set");

        counter
    }

    /// A subscription on `channel`, which a publication — a replay's, or a
    /// recording's reader — links to.
    ///
    /// # Panics
    ///
    /// When the driver will not take the subscription.
    pub fn subscribe(&mut self, channel: &str, stream_id: i32) -> i64 {
        self.client
            .add_subscription(channel, stream_id, COMMAND_TIMEOUT)
            .expect("the driver must accept the subscription")
    }

    /// Send one request and answer with the response, whatever it says.
    ///
    /// This is the door for a request that is **expected to be refused**: the
    /// refusals are behaviour too, and several of them are reached in no other
    /// way.
    ///
    /// # Panics
    ///
    /// When nothing is answered at all, which is a different failure from a
    /// refusal and printed as one.
    pub fn ask(&mut self, correlation_id: i64, payload: &[u8]) -> Response {
        self.session
            .send_only(&mut self.client, payload, Instant::now() + DEADLINE)
            .unwrap_or_else(|reason| panic!("{reason};\n{}", counters(&self.aeron_dir)));

        self.await_answer(correlation_id)
    }

    /// Wait for the answer to a request already sent with
    /// [`Session::send_only`], whatever it says.
    ///
    /// This is the other half of [`Archive::ask`], and the reason it is a method
    /// of its own is a test that wants **two requests in flight before either is
    /// answered**: the archive handles both in the turn it reads them, in the
    /// order they were published, and what one of them does is what the other
    /// then sees.
    ///
    /// **Every recording signal read on the way is kept** ([`Archive::signals`]),
    /// and that is not tidiness. The response channel carries answers and signals
    /// interleaved, and a signal is a frame the reader *reads past* — so waiting
    /// for an answer throws away every signal that arrived before it. A delete
    /// answers its request before it has deleted anything and signals when the
    /// work is over (`DeleteSegmentsSession.java:76-80`), which puts the two
    /// frames a turn or two apart: the ordinary case, not a race.
    ///
    /// # Panics
    ///
    /// When nothing is answered at all — see [`Archive::ask`].
    pub fn await_answer(&mut self, correlation_id: i64) -> Response {
        if let Some(index) = self.answer_index(correlation_id) {
            return self.answers.remove(index);
        }

        let subscription = self.session.response_subscription();
        let mut signals = Vec::new();
        let mut answers = Vec::new();

        let found = await_frame(
            &mut self.client,
            subscription,
            Instant::now() + DEADLINE,
            |payload| {
                if let Some(signal) = decode_signal(payload) {
                    signals.push(signal);
                }

                let response = decode(payload)?;
                let wanted = response.correlation_id == correlation_id;

                if !wanted {
                    answers.push(response.clone());
                }

                wanted.then_some(response)
            },
        );

        self.signals.append(&mut signals);
        self.answers.append(&mut answers);

        found.unwrap_or_else(|reason| panic!("{reason};\n{}", counters(&self.aeron_dir)))
    }

    /// Where the answer to `correlation_id` is in the buffer, if it is there.
    fn answer_index(&self, correlation_id: i64) -> Option<usize> {
        self.answers
            .iter()
            .position(|answer| answer.correlation_id == correlation_id)
    }

    /// Wait for a recording signal, answering with it.
    ///
    /// The buffer is checked first, because the signal is very often already
    /// there: it is sent turns after the answer that told the client the work had
    /// started, and reading that answer is what read past it
    /// ([`Archive::await_answer`]).
    ///
    /// # Panics
    ///
    /// When it does not arrive inside [`DEADLINE`].
    pub fn await_signal(&mut self, correlation_id: i64) -> Signal {
        if let Some(index) = self.signal_index(correlation_id) {
            return self.signals.remove(index);
        }

        let subscription = self.session.response_subscription();
        let mut seen = Vec::new();

        let _ = await_frame(
            &mut self.client,
            subscription,
            Instant::now() + DEADLINE,
            |payload| {
                let signal = decode_signal(payload)?;
                let wanted = signal.correlation_id == correlation_id;
                seen.push(signal);

                wanted.then_some(())
            },
        );

        self.signals.append(&mut seen);

        let Some(index) = self.signal_index(correlation_id) else {
            panic!(
                "no signal for correlation id {correlation_id} arrived;\n{}\n--- archive ---\n{}",
                counters(&self.aeron_dir),
                self.media_driver.log_tail(40)
            );
        };

        self.signals.remove(index)
    }

    /// Where a signal for `correlation_id` is in the buffer, if it is there.
    fn signal_index(&self, correlation_id: i64) -> Option<usize> {
        self.signals
            .iter()
            .position(|signal| signal.correlation_id == correlation_id)
    }

    /// Send one request that is answered with an `OK`, and assert that it was.
    ///
    /// # Panics
    ///
    /// When the answer is anything but `OK`, printing the archive's reason.
    pub fn ok_answer(&mut self, correlation_id: i64, payload: &[u8]) -> Response {
        let answer = self.ask(correlation_id, payload);

        assert_eq!(
            ControlResponseCode::OK,
            answer.code,
            "the archive refused: {}",
            answer.message()
        );

        answer
    }

    /// Read a replay until its reader reaches `position`.
    ///
    /// # Panics
    ///
    /// When it does not get there inside [`DEADLINE`].
    pub fn read_replay(&mut self, subscription: i64, position: i64) -> Replay {
        read_replay(&mut self.client, subscription, position)
    }

    /// Where a recording stops `(15)`, waiting for the catalog to say it does.
    ///
    /// The **catalog's** answer, which is the half a truncate moves first: a
    /// test that read the position off the files would be asking what the erase
    /// did rather than what the archive now says about the recording.
    ///
    /// # Panics
    ///
    /// When the archive refuses the request, or the row never has a stop.
    pub fn stop_position(&mut self, recording_id: i64) -> i64 {
        let deadline = Instant::now() + DEADLINE;

        loop {
            let correlation_id = self.session.next_correlation_id();
            let payload = stop_position_request(
                self.session.control_session_id(),
                correlation_id,
                recording_id,
            );
            let answer = self.ok_answer(correlation_id, &payload);

            if answer.relevant_id >= 0 {
                return answer.relevant_id;
            }

            assert!(
                Instant::now() < deadline,
                "the recording's catalog row never got a stop position;\n{}",
                counters(&self.aeron_dir)
            );

            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Read every data frame the subscription has, appending them to `frames`.
    pub fn read_frames(&mut self, subscription: i64, frames: &mut Vec<Frame>) {
        read_frames(&mut self.client, subscription, frames);
    }

    /// The one recording a test makes: `recording.messages` messages on its
    /// channel, with a mark at `recording.mark`, stopped.
    ///
    /// **Stopped** is what makes it the recording every segment operation needs:
    /// a truncate and a purge both refuse a recording that is still being
    /// written (`invalid_truncate`, `invalid_purge`).
    ///
    /// # Panics
    ///
    /// When the publication will not link, the recording never catches up with
    /// it, or the archive refuses any of the three requests this makes.
    pub fn record(&mut self, recording: &Recording) -> Recorded {
        let publication = self
            .client
            .add_exclusive_publication(&recording.channel, recording.stream_id, COMMAND_TIMEOUT)
            .expect("the driver must accept the publication");
        let reader = self
            .client
            .add_subscription(&recording.channel, recording.stream_id, COMMAND_TIMEOUT)
            .expect("the driver must accept a reader");

        let publisher_session = self
            .client
            .exclusive_publication(publication)
            .map(|publication| publication.session_id())
            .expect("the publication has a session");

        // The start (63), whose answer is the **subscription's** registration
        // id.
        let correlation_id = self.session.next_correlation_id();
        let payload = start_recording_request(
            self.session.control_session_id(),
            correlation_id,
            recording.stream_id,
            &recording.channel,
        );
        let subscription_id = self.ok_answer(correlation_id, &payload).relevant_id;

        let (mark, end) = publish(&mut self.client, publication, reader, recording);

        // The recording follows the publication, and this is where the recording
        // id comes from: nothing else names it until a listing or a stop does.
        let recording_id = wait_for_the_counter(
            &mut self.client,
            publication,
            reader,
            publisher_session,
            &self.aeron_dir,
        );

        let correlation_id = self.session.next_correlation_id();
        let payload = stop_recording_request(
            self.session.control_session_id(),
            correlation_id,
            subscription_id,
        );
        self.ok_answer(correlation_id, &payload);

        let stop = self.stop_position(recording_id);

        assert_eq!(
            end, stop,
            "a recording stops where its publication did, which is what makes a mark read off the \
             publication a position the recording is bounded at"
        );

        Recorded {
            recording_id,
            stop,
            first: mark,
        }
    }
}

/// What a test asks the archive to record.
///
/// The four numbers are per-test because they are what the test is about: a
/// recording has to span more than one segment before a replay can cross one, a
/// truncate can cut inside one, or an attach has one to walk back to — and the
/// mark is the position a bounded replay is stopped at and a truncate cuts.
pub struct Recording {
    channel: String,
    stream_id: i32,
    messages: usize,
    message_size: usize,
    mark: usize,
}

impl Recording {
    /// `messages` messages of `message_size` bytes on `channel`, with a mark
    /// read after the `mark`th.
    #[must_use]
    pub fn new(
        channel: &str,
        stream_id: i32,
        messages: usize,
        message_size: usize,
        mark: usize,
    ) -> Self {
        Self {
            channel: channel.to_owned(),
            stream_id,
            messages,
            message_size,
            mark,
        }
    }

    /// The message at `index`, as the publication writes it.
    #[must_use]
    pub fn message(&self, index: usize) -> Vec<u8> {
        message(index, self.message_size)
    }
}

/// What a recorded run leaves behind.
pub struct Recorded {
    /// The recording, as the catalog allocated it.
    pub recording_id: i64,
    /// Where the publication stood when the recording was stopped, which is
    /// where the recording stopped.
    pub stop: i64,
    /// Where the recording stood at the mark.
    ///
    /// It is read off the **publication**, whose position and the recording's
    /// are the same number: the archive's spy subscription joins before anything
    /// is written, so both count from the same place — which [`Archive::record`]
    /// asserts rather than assumes.
    pub first: i64,
}

/// One replayed frame, kept for the assertions.
pub struct Frame {
    /// The **frame's** session id, read off its own header.
    pub session_id: i32,
    /// The frame's stream id.
    pub stream_id: i32,
    /// Its payload.
    pub payload: Vec<u8>,
}

/// What a replay left: where it stopped, whose frames it wrote, and the frames.
pub struct Replay {
    /// The replay publication's session id, which every frame must carry.
    pub publication_session_id: i32,
    /// Where the reader got to.
    pub position: i64,
    /// The frames it read.
    pub frames: Vec<Frame>,
}

/// Publish every message, answering with the publication's position at the mark
/// and at the end.
fn publish(
    client: &mut Client,
    publication: i64,
    reader: i64,
    recording: &Recording,
) -> (i64, i64) {
    let deadline = Instant::now() + DEADLINE;
    let mut offered = 0;
    let mut mark = 0;

    while offered < recording.messages {
        match client.offer_exclusive(publication, &recording.message(offered)) {
            Some(Appended::Ok { .. }) => {
                offered += 1;

                // The mark is where the publication stood **after** the message
                // before it: a frame boundary, with whatever padding the term
                // before it needed already counted in.
                if offered == recording.mark {
                    mark = publication_position(client, publication);
                }
            }
            // Three answers mean *not now* rather than a failure, and a term
            // short enough to hold a few messages makes two of them ordinary: a
            // frame that did not fit the rest of the term (`EndOfLog`, which is
            // the rotation the log has already begun), a window used up because
            // a reader has not caught up (`BackPressured`), and no reader at all
            // yet (`NotConnected`). A publisher waits on all three.
            Some(Appended::EndOfLog | Appended::BackPressured | Appended::NotConnected) => {}
            other => panic!("the message could not be published ({other:?})"),
        }

        // Every turn, whether or not the offer landed: the reader is what lets
        // the publication rotate, so a publisher that only read on success would
        // wait forever on the frame that filled the term.
        client.poll();
        read_whatever_there_is(client, reader);

        assert!(
            Instant::now() < deadline,
            "the publication never took message {offered} of {}",
            recording.messages
        );
    }

    (mark, publication_position(client, publication))
}

/// The `message {index}` bytes, padded to `size`.
///
/// A fixed length on purpose: a frame is its payload plus a 32-byte header, so a
/// fixed payload makes every frame the same size and the marks above land where
/// the arithmetic says they will.
#[must_use]
pub fn message(index: usize, size: usize) -> Vec<u8> {
    let mut payload = format!("message {index:06}").into_bytes();
    payload.resize(size, b'.');

    payload
}

/// Read whatever the reader has, which is what keeps the publisher going: a UDP
/// publication nobody reads is one the driver stops sending on.
fn read_whatever_there_is(client: &mut Client, reader: i64) {
    let Some(image) = image(client, reader) else {
        return;
    };

    client.poll_image(reader, image.registration_id, 10, |_| {});
}

/// Where a publication has got to.
fn publication_position(client: &Client, publication: i64) -> i64 {
    client
        .exclusive_publication(publication)
        .and_then(|publication| publication.position())
        .expect("the publication is still held")
}

/// Wait for the recording's position counter to catch up with the publication,
/// answering with the recording id it names
/// (`aeron_archive_test.cpp:267-286`).
fn wait_for_the_counter(
    client: &mut Client,
    publication: i64,
    reader: i64,
    session_id: i32,
    aeron_dir: &Path,
) -> i64 {
    let deadline = Instant::now() + DEADLINE;
    let position = publication_position(client, publication);

    while Instant::now() < deadline {
        if let Some((recording_id, value)) = recording_counter(client, session_id) {
            if value >= position {
                return recording_id;
            }
        }

        client.poll();
        read_whatever_there_is(client, reader);
        std::thread::sleep(Duration::from_millis(1));
    }

    panic!(
        "the recording never caught up to {position};\n{}",
        counters(aeron_dir)
    );
}

/// The recording's position counter — its recording id and value — if the driver
/// has one for this session.
fn recording_counter(client: &Client, session_id: i32) -> Option<(i64, i64)> {
    let counters = client.counters_reader()?;
    let counter_id = find_counter_id_by_session(&counters, session_id, ARCHIVE_ID)?;
    let recording_id = parse_key(&counters.key(counter_id)?)?.recording_id;

    Some((recording_id, counters.value(counter_id)?))
}

/// A `StopPositionRequest` (15).
fn stop_position_request(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 64];

    let length = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder = StopPositionRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id);

        body + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}

/// What an image says about itself, copied out of the client so it can be polled
/// again.
pub struct Image {
    /// The client's handle for it.
    pub registration_id: i64,
    /// The publication behind it.
    pub session_id: i32,
    /// Where the reader has got to, padding included.
    pub position: i64,
}

/// The image a subscription holds, if there is one yet.
#[must_use]
pub fn image(client: &Client, subscription: i64) -> Option<Image> {
    let image = client.subscription(subscription)?.images().first()?;

    Some(Image {
        registration_id: image.registration_id(),
        session_id: image.session_id(),
        position: image.position(),
    })
}

/// Read a replay until its reader reaches `position`, answering with every data
/// frame it carried on the way.
///
/// The position is the image's own, which is where the *reader* has got to —
/// padding included, since the reader steps over it. That is what makes "it
/// reached the recording's stop position" a claim about the padding too.
fn read_replay(client: &mut Client, subscription: i64, position: i64) -> Replay {
    let deadline = Instant::now() + DEADLINE;
    let mut frames = Vec::new();
    let mut publication_session_id = None;
    let mut reached_from = None;

    loop {
        // The image is announced on the CnC broadcast, so it is a poll that
        // makes it exist — and the publication behind it is made several turns
        // after the OK, because the OK does not wait for it
        // (`ReplaySession.java:338`).
        client.poll();

        if let Some(current) = image(client, subscription) {
            publication_session_id.get_or_insert(current.session_id);
            read_frames(client, subscription, &mut frames);

            // Read again: the poll above may have taken the image away as well
            // as moved it, and what the reader got to is the last thing it was
            // seen at.
            if let Some(current) = image(client, subscription) {
                reached_from = Some(current.position);

                if current.position >= position {
                    return Replay {
                        publication_session_id: publication_session_id
                            .expect("an image was seen before this could be returned"),
                        position: current.position,
                        frames,
                    };
                }
            }
        }

        assert!(
            Instant::now() < deadline,
            "the replay never reached {position}; it is at {reached_from:?} with {} frames",
            frames.len()
        );

        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Read every data frame the subscription has, appending them to `frames`.
fn read_frames(client: &mut Client, subscription: i64, frames: &mut Vec<Frame>) {
    let Some(current) = image(client, subscription) else {
        return;
    };

    client.poll_image(subscription, current.registration_id, 10, |fragment| {
        // A replay hands over whole frames — it writes blocks of them, not
        // messages split across frames — so a fragment that is not whole is a
        // different claim about how the frames were written, and not this one.
        assert!(
            fragment.is_unfragmented(),
            "a replayed frame is a whole frame"
        );

        let mut payload = vec![0u8; fragment.payload_length()];
        assert!(
            fragment.copy_payload(&mut payload).is_some(),
            "the frame fits"
        );

        frames.push(Frame {
            session_id: fragment.session_id().expect("a frame has a header"),
            stream_id: fragment.stream_id().expect("a frame has a header"),
            payload,
        });
    });
}

/// One `RecordingSignalEvent` (24), which is how a client learns that a delete
/// is over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signal {
    /// The session it was sent on.
    pub control_session_id: i64,
    /// The request it answers.
    pub correlation_id: i64,
    /// The recording it is about.
    pub recording_id: i64,
    /// `START`, `STOP`, `DELETE` and the rest.
    pub signal: RecordingSignal,
}

/// Decode one `RecordingSignalEvent`, or `None` if the bytes are not one.
#[must_use]
pub fn decode_signal(payload: &[u8]) -> Option<Signal> {
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);

    if header.template_id() != recording_signal_event_codec::SBE_TEMPLATE_ID {
        return None;
    }

    let decoder = RecordingSignalEventDecoder::default().header(header, 0);
    let control_session_id = decoder.control_session_id();

    Some(Signal {
        control_session_id,
        correlation_id: decoder.correlation_id(),
        recording_id: decoder.recording_id(),
        signal: decoder.signal(),
    })
}

/// A `StartRecordingRequest2` (63) for the channel a test records.
#[must_use]
pub fn start_recording_request(
    control_session_id: i64,
    correlation_id: i64,
    stream_id: i32,
    channel: &str,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 256];

    let length = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder =
            StartRecordingRequest2Encoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .stream_id(stream_id)
            .source_location(SourceLocation::LOCAL)
            .auto_stop(BooleanType::FALSE)
            .channel(channel.as_bytes());

        body + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}

/// A `StopRecordingSubscriptionRequest` (14) for one subscription.
#[must_use]
pub fn stop_recording_request(
    control_session_id: i64,
    correlation_id: i64,
    subscription_id: i64,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 64];

    let length = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder = StopRecordingSubscriptionRequestEncoder::default()
            .wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .subscription_id(subscription_id);

        body + encoder.encoded_length()
    };

    buffer.truncate(length);
    buffer
}

/// A `ReplayRequest` (6).
#[allow(clippy::too_many_arguments)] // one per field the request carries
#[must_use]
pub fn replay_request(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    position: i64,
    length: i64,
    file_io_max_length: i32,
    replay_stream_id: i32,
    replay_channel: &str,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 512];

    let written = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder = ReplayRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id)
            .position(position)
            .length(length)
            .replay_stream_id(replay_stream_id)
            .file_io_max_length(file_io_max_length)
            // `Aeron.NULL_VALUE`: this replay is asked for on the session's own
            // image, so it carries no token — and the two are told apart by
            // exactly this comparison, which is why the guard on the field
            // matters.
            .replay_token(-1)
            .replay_channel(replay_channel.as_bytes());

        body + encoder.encoded_length()
    };

    buffer.truncate(written);
    buffer
}

/// A `BoundedReplayRequest` (18): the same request with the counter that bounds
/// it.
#[allow(clippy::too_many_arguments)] // one per field the request carries
#[must_use]
pub fn bounded_replay_request(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    position: i64,
    length: i64,
    file_io_max_length: i32,
    limit_counter_id: i32,
    replay_stream_id: i32,
    replay_channel: &str,
) -> Vec<u8> {
    let mut buffer = vec![0u8; 512];

    let written = {
        let body = message_header_codec::ENCODED_LENGTH;
        let encoder = BoundedReplayRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), body);
        let mut header = encoder.header(0);
        let mut encoder = header.parent().unwrap();

        encoder
            .control_session_id(control_session_id)
            .correlation_id(correlation_id)
            .recording_id(recording_id)
            .position(position)
            .length(length)
            .limit_counter_id(limit_counter_id)
            .replay_stream_id(replay_stream_id)
            .file_io_max_length(file_io_max_length)
            .replay_token(-1)
            .replay_channel(replay_channel.as_bytes());

        body + encoder.encoded_length()
    };

    buffer.truncate(written);
    buffer
}
