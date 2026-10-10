//! The requests that arrive, and which session each one belongs to.
//!
//! The reference's `ControlSessionAdapter` is a `FragmentHandler` the conductor
//! polls (`ControlSessionAdapter.java:77`): it reads two subscriptions, turns
//! the fragments back into whole messages, and switches on the message's
//! `templateId` to decide what the request is. This is the same class of work
//! with the same switch, and two of its decisions are worth reading before the
//! code:
//!
//! * **Which image a session belongs to is part of the session's identity.**
//!   Every request that is not a connect goes through
//!   [`ControlAdapter::on_message`]'s gate, and a request that arrives on a
//!   *different* image than the one its session was created from is refused
//!   without a word to the client (`ControlSessionAdapter.java:1198-1203`) —
//!   the reference logs it and drops it. A session created over UDP is
//!   therefore not drivable over IPC, and the two control subscriptions, which
//!   share stream 10, are what makes that matter
//!   (`ArchiveConductor.java:239-240`).
//! * **Authorisation is asked once per request, by template id**, and a refusal
//!   is the one gate that answers the client: `ERROR` carrying
//!   `ArchiveException.UNAUTHORISED_ACTION` as the relevant id and
//!   `"unauthorised action"` as the message (`:1205-1216`).
//!
//! # Two subscriptions, one assembler, ten fragments each
//!
//! `poll()` reads the control subscription **if there is one** and then the
//! local one, each for `FRAGMENT_LIMIT` fragments, both through the *same*
//! assembler (`ControlSessionAdapter.java:87`, `:107-119`). The local
//! subscription is the one the reference always creates
//! (`ArchiveConductor.java:240`); the remote one is the one
//! `aeron.archive.control.channel.enabled` can take away.
//!
//! # What is not the reference's shape, and why
//!
//! * **The sessions are not here.** The reference's adapter holds a map from
//!   session id to `(image, controlSession)` (`:88`) while the conductor's
//!   `SessionWorker` holds the same sessions in its own list
//!   (`SessionWorker.java:56-81`) — two owners of one object, which Java
//!   permits and this language does not. The split here is the one the borrow
//!   checker forces and nothing more: the adapter keeps each session's
//!   **identity**, and [`ControlPlane`] is asked for everything that has to
//!   touch the session itself.
//!
//!   That is also why the control plane is **lent per call** rather than held.
//!   The reference hands its conductor to the adapter's constructor and the
//!   conductor keeps the adapter (`ArchiveConductor.java:242-243`) — one
//!   object reachable from the other both ways, which is free in Java and
//!   impossible for a value. The client's publication side gets the same
//!   treatment for the same reason.
//! * **The image is identified by what the driver assigned it.** The reference
//!   compares `Image` *objects*; this build's `Image` is borrowed from the
//!   subscription for the length of a poll and cannot be held. [`ImageId`] is
//!   what the driver put on the publication, which is the same distinction.
//! * **A message's schema is refused with a value, not an exception.** The
//!   reference throws `ArchiveException` out of `onFragment` (`:136`),
//!   which the archive's main catches and exits on; [`ControlError`] is that
//!   throw, one stack frame earlier.
//! * **The decoders are built per message rather than held.** The reference's
//!   `ControlRequestDecoders` preallocates one per template
//!   (`ControlRequestDecoders.java:60-104`) because each is an object holding a
//!   buffer reference it must be `wrap`ped onto afresh anyway. Here a decoder
//!   *is* the offsets — `Copy` to a pair of `usize` — so building one is the
//!   wrap the reference performs, and there is no allocation to hoist.
//! * **The log lines name the image differently.** The reference prints
//!   `source=image.sourceIdentity()`; that string belongs to the `Image`, which
//!   this adapter does not hold, so the lines carry the publication's
//!   correlation id and session instead. The reference's log text is not a
//!   contract — nothing reads it but a person.

use std::collections::HashMap;
use std::fmt;

use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::{FragmentAssembler, Message};
use deepmsg_client::image::Fragment;
use deepmsg_codec::archive::archive_id_request_codec::ArchiveIdRequestDecoder;
use deepmsg_codec::archive::attach_segments_request_codec::{self, AttachSegmentsRequestDecoder};
use deepmsg_codec::archive::auth_connect_request_codec::AuthConnectRequestDecoder;
use deepmsg_codec::archive::boolean_type::BooleanType;
use deepmsg_codec::archive::bounded_replay_request_codec::{self, BoundedReplayRequestDecoder};
use deepmsg_codec::archive::challenge_response_codec::ChallengeResponseDecoder;
use deepmsg_codec::archive::close_session_request_codec::CloseSessionRequestDecoder;
use deepmsg_codec::archive::delete_detached_segments_request_codec::{
    self, DeleteDetachedSegmentsRequestDecoder,
};
use deepmsg_codec::archive::detach_segments_request_codec::{self, DetachSegmentsRequestDecoder};
use deepmsg_codec::archive::extend_recording_request_2_codec::{
    self, ExtendRecordingRequest2Decoder,
};
use deepmsg_codec::archive::find_last_matching_recording_request_codec::{
    self, FindLastMatchingRecordingRequestDecoder,
};
use deepmsg_codec::archive::keep_alive_request_codec::KeepAliveRequestDecoder;
use deepmsg_codec::archive::list_recording_request_codec::{self, ListRecordingRequestDecoder};
use deepmsg_codec::archive::list_recording_subscriptions_request_codec::{
    self, ListRecordingSubscriptionsRequestDecoder,
};
use deepmsg_codec::archive::list_recordings_for_uri_request_codec::{
    self, ListRecordingsForUriRequestDecoder,
};
use deepmsg_codec::archive::list_recordings_request_codec::{self, ListRecordingsRequestDecoder};
use deepmsg_codec::archive::max_recorded_position_request_codec::{
    self, MaxRecordedPositionRequestDecoder,
};
use deepmsg_codec::archive::message_header_codec::{self, MessageHeaderDecoder};
use deepmsg_codec::archive::purge_recording_request_codec::{self, PurgeRecordingRequestDecoder};
use deepmsg_codec::archive::purge_segments_request_codec::{self, PurgeSegmentsRequestDecoder};
use deepmsg_codec::archive::recording_position_request_codec::{
    self, RecordingPositionRequestDecoder,
};
use deepmsg_codec::archive::replay_request_codec::{self, ReplayRequestDecoder};
use deepmsg_codec::archive::replay_token_request_codec::{self, ReplayTokenRequestDecoder};
use deepmsg_codec::archive::source_location::SourceLocation;
use deepmsg_codec::archive::start_position_request_codec::{self, StartPositionRequestDecoder};
use deepmsg_codec::archive::start_recording_request_2_codec::{
    self, StartRecordingRequest2Decoder,
};
use deepmsg_codec::archive::start_recording_request_codec::{self, StartRecordingRequestDecoder};
use deepmsg_codec::archive::stop_all_replays_request_codec::{self, StopAllReplaysRequestDecoder};
use deepmsg_codec::archive::stop_position_request_codec::{self, StopPositionRequestDecoder};
use deepmsg_codec::archive::stop_recording_by_identity_request_codec::{
    self, StopRecordingByIdentityRequestDecoder,
};
use deepmsg_codec::archive::stop_recording_request_codec::{self, StopRecordingRequestDecoder};
use deepmsg_codec::archive::stop_recording_subscription_request_codec::{
    self, StopRecordingSubscriptionRequestDecoder,
};
use deepmsg_codec::archive::stop_replay_request_codec::{self, StopReplayRequestDecoder};
use deepmsg_codec::archive::truncate_recording_request_codec::{
    self, TruncateRecordingRequestDecoder,
};
use deepmsg_codec::archive::update_channel_request_codec::{self, UpdateChannelRequestDecoder};
use deepmsg_codec::archive::{
    ReadBuf, SBE_SCHEMA_ID, archive_id_request_codec, auth_connect_request_codec,
    challenge_response_codec, close_session_request_codec, keep_alive_request_codec,
};

use deepmsg_core::uri::ChannelUri;

use crate::mark::NULL_VALUE;
use crate::server::auth::AuthorisationService;
use crate::server::conductor::{CONTROL_MODE_RESPONSE, Query};
use crate::server::control_session::SESSION_CLOSED_MSG;

/// The version a replay request's `replayToken` arrived in
/// (`ControlSessionAdapter.java:83`), which both replay templates gate the
/// field on.
///
/// It matters here and not only in the decoder. The generated reader answers
/// `i64::MIN` for a request older than the field
/// (`replay_request_codec.rs:336-342`), where the reference substitutes
/// `Aeron.NULL_VALUE` (`:226-227`) — and the replay asks the token a question
/// `NULL_VALUE` is the answer to, so the two are not interchangeable.
/// `file_io_max_length`, the other version-gated field, only has to be
/// **not positive**, which both values are.
pub const REPLAY_TOKEN_VERSION: u16 = 10;

/// How many fragments one subscription is read for in a turn
/// (`ControlSessionAdapter.java:79`).
pub const FRAGMENT_LIMIT: usize = 10;

/// `ArchiveException.UNAUTHORISED_ACTION` (`client/ArchiveException.java:94`),
/// the relevant id of the one `ERROR` a gate here sends.
///
/// The rest of that class's table is the client track's: it is what a *client*
/// turns a response's relevant id back into, and this slice needs the one
/// number it sends. The other id that class has is its `GENERIC`, zero
/// (`:29`) — what a session sends when its error is about nothing in
/// particular.
pub const UNAUTHORISED_ACTION: i32 = 13;

/// Where the error text of a refusal comes from
/// (`ControlSessionAdapter.java:1212-1213`).
pub const UNAUTHORISED_ACTION_MSG: &str = "unauthorised action";

/// Which control image a request arrived on.
///
/// The reference compares the `Image` **object** the request arrived on with
/// the one its session was created from (`ControlSessionAdapter.java:1198`) —
/// object identity, and the only thing that keeps a session made over the UDP
/// control channel from being driven by a request that came in over IPC. There
/// is no object to compare here: an `Image` is borrowed from the subscription
/// for the length of a poll, so what is kept is what the driver put on the
/// publication — its registration id, which is `Image.correlationId()`, the id
/// the conductor writes into a response channel that asked for one
/// (`ArchiveConductor.java:478-481`) — together with the session the driver
/// gave it, which is what a log line wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageId {
    /// The **publication's** registration id (`Image.correlationId()`).
    correlation_id: i64,
    /// The session id the driver gave that publication.
    session_id: i32,
}

impl ImageId {
    /// Name an image by the two ids the driver assigned it.
    pub const fn new(correlation_id: i64, session_id: i32) -> Self {
        Self {
            correlation_id,
            session_id,
        }
    }

    /// The publication's registration id.
    pub const fn correlation_id(self) -> i64 {
        self.correlation_id
    }

    /// The publication's session id.
    pub const fn session_id(self) -> i32 {
        self.session_id
    }
}

impl fmt::Display for ImageId {
    /// The part of a warning that says which image it was about. See the module
    /// note for why this is not `sourceIdentity()`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "correlationId={} sessionId={}",
            self.correlation_id, self.session_id
        )
    }
}

/// The parts of an `AuthConnectRequest` a session is built from
/// (`ControlSessionAdapter.java:766-802`).
///
/// The two strings are decoded here rather than in the conductor because that
/// is where the reference decodes them: its generated decoder hands back
/// `String`s and the adapter passes them on. They are declared US-ASCII and
/// decoded leniently, which is what a Java `String` built from bytes does with
/// them too.
#[derive(Debug, Clone, Copy)]
pub struct ConnectRequest<'a> {
    /// The connect's correlation id, which its answer echoes.
    pub correlation_id: i64,
    /// The stream the client wants its responses on.
    pub response_stream_id: i32,
    /// The client's protocol version, whose **major** decides acceptance
    /// (`ArchiveConductor.java:483-488`). Absent reads as zero, as it does in
    /// the reference's decoder.
    pub version: i32,
    /// The channel it wants them on.
    pub response_channel: &'a str,
    /// What it offered to be authenticated with.
    pub encoded_credentials: &'a [u8],
    /// What it calls itself, which the session counter's label carries.
    pub client_info: &'a str,
}

/// One `StartRecordingRequest`, as the adapter decoded it
/// (`ControlSessionAdapter.java:162-186` for the version without `autoStop`,
/// `:891-915` for the one with it).
///
/// Owned rather than borrowed, unlike [`ConnectRequest`]: the channel is
/// variable-length data at the end of the message, and what the conductor does
/// with it happens a turn later — the request is deferred, and a borrow of the
/// fragment could not outlive the poll that read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartRecordingRequest {
    /// The session that asked.
    pub session_id: i64,
    /// The request's correlation id, which its answer echoes.
    pub correlation_id: i64,
    /// The stream to record.
    pub stream_id: i32,
    /// Whether the archive is local to the driver, which is what decides
    /// whether the recording goes through a spy link
    /// (`ArchiveConductor.java:559-560`).
    pub source_location: SourceLocation,
    /// Whether the recording stops when the client that asked goes away
    /// (`:1329-1363`). The request version that has no such field is passed
    /// `false` for it (`ControlSessionAdapter.java:180-185`).
    pub auto_stop: bool,
    /// The channel as the client wrote it.
    pub original_channel: String,
}

/// One `ReplayRequest` (template **6**) or `BoundedReplayRequest` (template
/// **18**), as the adapter decoded it
/// (`ControlSessionAdapter.java:209-252`, `:515-560`).
///
/// The two templates differ by one field, `limitCounterId`, and that one field
/// is the whole of what "bounded" means — so this carries it as an `Option`
/// rather than having two types that agree on everything else.
///
/// **`length` is not resolved here.** Its two sentinels — `-1` follows, `-2`
/// counts to the stop — are resolved in the conductor, because resolving them
/// needs the recording's stop position and this side has not read the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartReplayRequest {
    /// The session that asked.
    pub session_id: i64,
    /// The request's correlation id, which its answer echoes.
    pub correlation_id: i64,
    /// The recording to replay.
    pub recording_id: i64,
    /// Where to start; `Aeron.NULL_VALUE` means the recording's own beginning.
    pub position: i64,
    /// How much to send, sentinels and all.
    pub length: i64,
    /// The channel as the client wrote it. The **recording's** geometry is
    /// written onto it by the conductor (`AC:891-901`), which is why what
    /// travels is the client's channel and not a built one.
    pub replay_channel: String,
    /// The stream the replayed frames go out on.
    pub replay_stream_id: i32,
    /// How big a block one turn may read; not positive means "the whole buffer"
    /// (`AC:958-966`). Absent in the request's first version, which the adapter
    /// passes `NULL_VALUE` for (`CSA:220-221`).
    pub file_io_max_length: i32,
    /// A `BoundedReplayRequest`'s limit counter, which a plain `ReplayRequest`
    /// does not name. **Not read yet** — the bounded slice is its own commit.
    pub limit_counter_id: Option<i64>,
    /// The image this request arrived on, when the request came in on a
    /// **response-channel** token rather than on the session's own image
    /// (`ControlSessionAdapter.java:1183`).
    ///
    /// It is `Image.correlationId()` — the publication's registration id — and
    /// the conductor writes it onto the replay channel as
    /// `response-correlation-id`, which is what tells the archive's own driver
    /// which of its images the replay is the answer to.
    ///
    /// `None` on every other path, and the field is named for what it becomes
    /// rather than for where it came from for the same reason the channel is
    /// carried as the client wrote it: the adapter hands on a fact and the
    /// conductor decides what to do with it.
    pub response_correlation_id: Option<i64>,
}

/// One `ExtendRecordingRequest2`, as the adapter decoded it
/// (`ControlSessionAdapter.java:917-943`).
///
/// [`StartRecordingRequest`]'s twin, field for field with one more: the
/// **recording** the extension belongs to (`ArchiveConductor.java:1067-1076`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtendRecordingRequest {
    /// The session that asked.
    pub session_id: i64,
    /// The request's correlation id, which its answer echoes.
    pub correlation_id: i64,
    /// The recording being appended to.
    pub recording_id: i64,
    /// The stream it must already be on (`:1090-1097` refuses any other).
    pub stream_id: i32,
    /// Whether the archive is local to the driver, which is what decides
    /// whether the recording goes through a spy link (`:1129-1131`).
    pub source_location: SourceLocation,
    /// Whether the recording stops when the client that asked goes away
    /// (`:2044`).
    pub auto_stop: bool,
    /// The channel as the client wrote it.
    pub original_channel: String,
}

/// A message the adapter will not read, and why.
///
/// Each of these is an exception in the reference — thrown out of `onFragment`
/// and out of `ArchiveConductor.doWork` (`:136-139`), which the archive's
/// `main` catches and exits on. A value is the same decision taken one frame
/// earlier, where the conductor can still name what it was doing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlError {
    /// A message shorter than a message header, which cannot name a schema.
    /// The reference reads the header anyway and its buffer throws.
    ShortMessage {
        /// How long the message was.
        length: usize,
    },
    /// The message belongs to another schema (`ControlSessionAdapter.java:136`).
    UnexpectedSchemaId {
        /// The archive schema's id.
        expected: u16,
        /// What the message carried.
        actual: u16,
    },
}

impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShortMessage { length } => write!(
                f,
                "control message of {length} bytes is shorter than a message header"
            ),
            Self::UnexpectedSchemaId { expected, actual } => {
                write!(f, "expected schemaId={expected}, actual={actual}")
            }
        }
    }
}

impl std::error::Error for ControlError {}

/// The archive's control plane, as the adapter uses it.
///
/// Every method is one thing the reference's adapter does to a `ControlSession`
/// or to the conductor, with the same caller and the same arguments. The
/// methods that read or move a session exist here rather than on
/// [`crate::server::control_session::ControlSession`] for the one reason the
/// module note gives: the sessions live with the conductor, and the adapter has
/// their ids.
///
/// **It is lent per call**, not held — see the module note. A caller therefore
/// passes the same control plane to [`ControlAdapter::poll`] and to
/// [`ControlAdapter::on_message`] every turn, and may use it in between.
pub trait ControlPlane {
    /// A connect request, which is the only request that makes a session
    /// (`ControlSessionAdapter.java:766-802`). Answers with the id the client
    /// will be answered on (`ArchiveConductor.java:490`).
    fn new_session(&mut self, image: ImageId, request: ConnectRequest<'_>, now_ms: i64) -> i64;

    /// What the authenticator vouched for, for the authorisation gate
    /// (`ControlSessionAdapter.java:1206`). `None` until it has vouched for
    /// anything.
    fn session_principal(&self, session_id: i64) -> Option<&[u8]>;

    /// `ControlSession.abort(reason)` (`ControlSession.java:145-157`).
    fn abort_session(&mut self, session_id: i64, reason: &str);

    /// `ControlSession.onChallengeResponse` (`ControlSession.java:308-315`).
    fn on_challenge_response(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        encoded_credentials: &[u8],
        now_ms: i64,
    );

    /// `ControlSession.onKeepAlive` (`ControlSession.java:317-320`), whose
    /// whole body is `attemptToActivate()`.
    fn on_keep_alive(&mut self, session_id: i64);

    /// `ControlSession.onArchiveId` (`ControlSession.java:541-548`), which the
    /// conductor answers with `ctx.archiveId()`
    /// (`ArchiveConductor.java:525-528`).
    fn on_archive_id(&mut self, session_id: i64, correlation_id: i64, now_ms: i64);

    /// `ControlSession.sendErrorResponse(correlationId, relevantId, message)`
    /// (`ControlSession.java:697-701`). The relevant id is the wire field's
    /// type; the reference's own error codes are `int`s it widens on the way in.
    fn send_error_response(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        relevant_id: i64,
        message: &str,
        now_ms: i64,
    );

    /// `ArchiveConductor.logWarning` (`ArchiveConductor.java:443-446`), which
    /// the reference routes into the archive's error handler.
    fn log_warning(&mut self, message: &str);

    /// A question about a recording (`ArchiveConductor.java:1159-1195`), which
    /// the conductor answers out of its catalog.
    ///
    /// The reference has four methods here and they differ only in which field
    /// they read, so this is one — see [`Query`] for the whole of that
    /// argument. What is *not* collapsed is the answer: it is the reference's,
    /// field for field.
    fn on_query(&mut self, session_id: i64, correlation_id: i64, query: Query, now_ms: i64);

    /// `ControlSession.onListRecording` (`ControlSession.java:383-390`), which
    /// the conductor answers with the recording's descriptor or with
    /// `RECORDING_UNKNOWN`.
    ///
    /// The two refusals in front of it are the conductor's, so this carries the
    /// intent and no decision — see the conductor's `Deferred::ListRecording`.
    fn on_list_recording(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        now_ms: i64,
    );

    /// `ControlSession.onListRecordings` (`ControlSession.java:374-382`), which
    /// asks for a **page** of this archive's recordings
    /// (`ArchiveConductor.newListRecordingsSession`, `:638-657`).
    ///
    /// One gate, and it is the asking session's own — unlike
    /// [`ControlPlane::on_list_recording`] there is no catalog question in
    /// front of it, a page about recordings the catalog does not hold being a
    /// page of nothing. So this carries the request and the conductor starts
    /// the listing there and then.
    fn on_list_recordings(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        from_recording_id: i64,
        count: i32,
        now_ms: i64,
    );

    /// `ControlSession.onListRecordingsForUri` (`ControlSession.java:354-372`),
    /// the same page narrowed to one stream on one channel
    /// (`ArchiveConductor.newListRecordingsForUriSession`, `:659-687`).
    ///
    /// The channel fragment is **bytes**: the request's field is a `varData`,
    /// and the test it is put to on the far side is over the channel's own
    /// bytes (`ControlSessionAdapter.java:305-313`,
    /// `Catalog.originalChannelContains` at `Catalog.java:585-625`).
    #[allow(clippy::too_many_arguments)] // one per field the request carries
    fn on_list_recordings_for_uri(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        from_recording_id: i64,
        count: i32,
        stream_id: i32,
        channel_fragment: &[u8],
        now_ms: i64,
    );

    /// `ControlSession.onStartRecording` (`ControlSession.java:340-352`), which
    /// the conductor answers with the recording subscription's registration id
    /// (`ArchiveConductor.java:530-583`).
    ///
    /// The request asks the archive to **add a subscription**, which the
    /// reference does inside this call and this build cannot: the add is a
    /// command to a driver the callback's own turn is what drives. So this
    /// carries the intent and the conductor runs it — see `Deferred::Action`.
    ///
    /// `auto_stop` is the request's own field and is not read here: it is read
    /// when the recording session ends, and the session is what the recorder
    /// brings (`:1329-1363`).
    fn on_start_recording(&mut self, request: StartRecordingRequest, now_ms: i64);

    /// `ArchiveConductor.getReplaySession` (`:2654-2667`): which session a
    /// replay token stands for, or `None` for a token that was never issued,
    /// belongs to another recording, or has expired.
    ///
    /// The reference calls this from
    /// `setupSessionAndChannelForReplay` (`ControlSessionAdapter.java:1177`)
    /// **in place of** its session gate — a token names its session, so there
    /// is no `controlSessionId` to look up and no image to compare. That is
    /// what makes a response-channel replay possible at all: its request
    /// arrives on an image the session was never opened on, which the gate
    /// would refuse (`:1196-1203`).
    fn replay_session_for_token(
        &self,
        replay_token: i64,
        recording_id: i64,
        now_ms: i64,
    ) -> Option<i64>;

    /// `ControlSessionAdapter`'s `ReplayTokenRequest` case (`:1084-1105`),
    /// which is `conductor.generateReplayToken(controlSession, recordingId)`
    /// and an `OK` that carries the token.
    ///
    /// The token is generated here rather than deferred: unlike everything
    /// else that is deferred, it needs neither the client nor the catalog, and
    /// the reference generates it the moment it reads the request.
    fn on_replay_token(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        now_ms: i64,
    );

    /// `ControlSession.onDetachSegments` (`ControlSession.java:437-444`), which is
    /// `ArchiveConductor.detachSegments` (`:1495-1507`).
    ///
    /// The only one of this family that is **not** deferred behind a session:
    /// it moves the recording's start and answers, and the files it left behind
    /// stay where they are until something deletes them.
    #[allow(clippy::too_many_arguments)] // the gate's, plus what the request carried
    fn on_detach_segments(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        new_start_position: i64,
        now_ms: i64,
    );

    /// `ControlSession.onDeleteDetachedSegments` (`ControlSession.java:446-453`),
    /// which is `ArchiveConductor.deleteDetachedSegments` (`:1509-1533`).
    ///
    /// Deferred for the reason every delete is: what it removes is files, and
    /// the answer to a client is what a session several turns long is for.
    fn on_delete_detached_segments(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        now_ms: i64,
    );

    /// `ControlSession.onUpdateChannel` (`ControlSession.java:531-539`), which
    /// is `ArchiveConductor.updateChannel` (`:708-735`).
    ///
    /// The channel travels as a `varAscii`, so this is one of the two requests
    /// whose payload is more than its fields.
    fn on_update_channel(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        channel: &str,
        now_ms: i64,
    );

    /// `ControlSession.onAttachSegments`, which is
    /// `ArchiveConductor.attachSegments` (`:1556-1617`).
    ///
    /// The reverse of a detach: it moves the recording's start **back** over
    /// the segments that are still there, and answers with how many it took.
    fn on_attach_segments(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        now_ms: i64,
    );

    /// `ControlSession.onTruncateRecording`, which is
    /// `ArchiveConductor.truncateRecording` (`:1199-1246`).
    ///
    /// The one request here that **writes a segment file** as well as deleting
    /// some: the file the new stop falls inside is cut and left zero beyond it.
    fn on_truncate_recording(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        position: i64,
        now_ms: i64,
    );

    /// `ControlSession.onPurgeRecording` (`ControlSession.java:481-488` in the
    /// reference's own numbering; the method is
    /// `ArchiveConductor.purgeRecording`, `:1251-1263`).
    ///
    /// The one request here that takes the recording **out of the catalog** as
    /// well as off the disk: the row's state goes `DELETED` and the index drops
    /// it, so a listing after a purge finds nothing.
    fn on_purge_recording(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        now_ms: i64,
    );

    /// `ControlSession.onPurgeSegments`, which is
    /// `ArchiveConductor.purgeSegments` (`:1535-1554`).
    ///
    /// A detach and a delete in one request: move the start, then remove what
    /// the old start was still claiming.
    fn on_purge_segments(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        new_start_position: i64,
        now_ms: i64,
    );

    /// `ControlSession.onStartReplay` (`ControlSession.java:412-435`), which is
    /// `ArchiveConductor.startReplay` (`:764-929`).
    ///
    /// Deferred for the reason [`ControlPlane::on_start_recording`] is: the
    /// answer waits for a publication to appear, and the channel is built from
    /// the **catalog**, which a callback runs without.
    fn on_start_replay(&mut self, request: StartReplayRequest, now_ms: i64);

    /// `ControlSession.onStopReplay` (`:463-470`). Answered with an `OK` whether
    /// or not there is such a replay (`AC:1037-1054`).
    fn on_stop_replay(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        replay_session_id: i64,
        now_ms: i64,
    );

    /// `ControlSession.onStopAllReplays` (`:472-479`); `NULL_VALUE` for the
    /// recording id means every replay the archive has (`AC:1056-1064`).
    fn on_stop_all_replays(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        now_ms: i64,
    );

    /// `ControlSession.onStopRecordingSubscription`
    /// (`ControlSession.java:331-338`), which the conductor answers with an `OK`
    /// or with `UNKNOWN_SUBSCRIPTION` (`ArchiveConductor.java:612-624`).
    fn on_stop_recording_subscription(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        subscription_id: i64,
        now_ms: i64,
    );

    /// `ControlSession.onStopRecording` (`ControlSession.java:322-329`), which
    /// stops the recording registered under a channel and stream
    /// (`ArchiveConductor.java:585-610`).
    fn on_stop_recording(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        stream_id: i32,
        original_channel: &str,
        now_ms: i64,
    );

    /// `ControlSession.onListRecordingSubscriptions`
    /// (`ControlSession.java:550-568`), which asks what this archive has been
    /// told to record (`ArchiveConductor.java:1266-1300`).
    #[allow(clippy::too_many_arguments)] // one per field the request carries
    fn on_list_recording_subscriptions(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        pseudo_index: i32,
        subscription_count: i32,
        apply_stream_id: bool,
        stream_id: i32,
        channel_fragment: &str,
        now_ms: i64,
    );

    /// `ControlSession.onExtendRecording` (`ControlSession.java:481-494`), which
    /// the conductor answers with the new subscription's registration id
    /// (`ArchiveConductor.java:1067-1157`) — or with one of its six refusals.
    fn on_extend_recording(&mut self, request: ExtendRecordingRequest, now_ms: i64);

    /// `ControlSession.onStopRecordingByIdentity` (`ControlSession.java:572-579`),
    /// whose answer carries whether a recording was stopped
    /// (`ArchiveConductor.java:1301-1327`).
    fn on_stop_recording_by_identity(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        now_ms: i64,
    );
}

/// The fragments of a client's control requests, and the sessions they belong
/// to.
pub struct ControlAdapter<A: AuthorisationService> {
    /// The control subscription, absent when `aeron.archive.control.channel
    /// .enabled` is false (`ArchiveConductor.java:227-237`).
    remote_subscription_id: Option<i64>,
    /// The local one, which is always there (`:239-240`).
    local_subscription_id: i64,
    /// Who decides whether a request may be performed at all.
    ///
    /// The reference hands this to the adapter's constructor and nothing else
    /// (`:243`), so it is the one collaborator the adapter does own.
    authorisation: A,

    /// `controlSessionByIdMap` (`ControlSessionAdapter.java:88`), holding the
    /// half of each entry that is the adapter's to hold — see the module note.
    sessions: HashMap<i64, ImageId>,
    /// The one assembler both subscriptions are read through
    /// (`ControlSessionAdapter.java:87`).
    ///
    /// It is the adapter's and not the subscriptions': the reference hands one
    /// assembler to both polls, and the messages it delivers are keyed by the
    /// publication's session, so a shared assembler is what keeps one
    /// subscription's half-read message from being completed by the other's.
    assembler: FragmentAssembler,
    /// The images of the subscription being polled, gathered before the poll
    /// starts.
    ///
    /// [`Client::poll_image`] names the image it reads by id and needs the
    /// client mutably, so the ids have to be taken out of the subscription
    /// first and the borrow dropped. The list is reused between polls, which is
    /// what keeps this from being an allocation per poll per image.
    images: Vec<ImageId>,
}

impl<A: AuthorisationService> ControlAdapter<A> {
    /// An adapter over two subscriptions, one of which may not exist.
    pub fn new(
        remote_subscription_id: Option<i64>,
        local_subscription_id: i64,
        authorisation: A,
    ) -> Self {
        Self {
            remote_subscription_id,
            local_subscription_id,
            authorisation,
            sessions: HashMap::new(),
            assembler: FragmentAssembler::new(),
            images: Vec::new(),
        }
    }

    /// Read both subscriptions, in the reference's order, and dispatch what
    /// arrives (`ControlSessionAdapter.java:107-119`).
    ///
    /// The remote subscription first and the local one second, each for
    /// [`FRAGMENT_LIMIT`] fragments — the limit is per subscription, not shared
    /// between them, which is what the reference's two separate `poll` calls
    /// say.
    ///
    /// # Errors
    ///
    /// [`ControlError`] for a message the archive will not read. The reference
    /// throws from here, so a turn that fails is a turn the caller should not
    /// continue past.
    pub fn poll<C: ControlPlane>(
        &mut self,
        client: &mut Client,
        control: &mut C,
        now_ms: i64,
    ) -> Result<usize, ControlError> {
        let mut fragments = 0;

        if let Some(subscription_id) = self.remote_subscription_id {
            fragments += self.poll_subscription(client, control, subscription_id, now_ms)?;
        }

        fragments += self.poll_subscription(client, control, self.local_subscription_id, now_ms)?;

        Ok(fragments)
    }

    /// Read one subscription's images for up to [`FRAGMENT_LIMIT`] fragments
    /// between them.
    fn poll_subscription<C: ControlPlane>(
        &mut self,
        client: &mut Client,
        control: &mut C,
        subscription_id: i64,
        now_ms: i64,
    ) -> Result<usize, ControlError> {
        self.images.clear();
        if let Some(subscription) = client.subscription(subscription_id) {
            self.images.extend(
                subscription
                    .images()
                    .iter()
                    .map(|image| ImageId::new(image.registration_id(), image.session_id())),
            );
        }

        let mut fragments = 0;
        // The first refusal ends the dispatch of what is already in hand, the
        // way the reference's throw ends the poll. The rest of the fragments
        // are still read — they are in the log buffer and reading them moves
        // the reader's position, which the publisher's window depends on — and
        // the error is handed back once the subscription is drained.
        let mut failure = None;

        // Borrowed apart for the reason `Subscription::poll_messages` does it:
        // the images are what is read, the assembler is where their fragments
        // go, and the rest is what a whole message is dispatched with.
        let Self {
            images,
            assembler,
            sessions,
            authorisation,
            ..
        } = self;

        for image in images.iter().copied() {
            let remaining = FRAGMENT_LIMIT.saturating_sub(fragments);
            if 0 == remaining {
                break;
            }

            let read = client.poll_image(
                subscription_id,
                image.correlation_id(),
                remaining,
                |fragment: &Fragment<'_>| {
                    assembler.push(fragment, &mut |message: Message<'_>| {
                        if failure.is_none() {
                            failure =
                                dispatch(message, image, sessions, control, authorisation, now_ms)
                                    .err();
                        }
                    });
                },
            );

            // `None` is an image the client no longer holds — one that went
            // away between being listed and being read. There is nothing to
            // read and nothing to say about it.
            match read {
                Some(count) => fragments += count,
                None => continue,
            }
        }

        match failure {
            Some(error) => Err(error),
            None => Ok(fragments),
        }
    }

    /// Dispatch one whole control message (`ControlSessionAdapter.java:126-1130`).
    ///
    /// Public because it is the whole of what the adapter decides: a caller
    /// with a message and the image it arrived on can put it through here
    /// without a driver, and the tests below do.
    ///
    /// # Errors
    ///
    /// [`ControlError`] for a message the archive will not read.
    pub fn on_message<C: ControlPlane>(
        &mut self,
        control: &mut C,
        image: ImageId,
        message: Message<'_>,
        now_ms: i64,
    ) -> Result<(), ControlError> {
        let Self {
            sessions,
            authorisation,
            ..
        } = self;

        dispatch(message, image, sessions, control, authorisation, now_ms)
    }

    /// Which image a session was created on, if it is still known.
    pub fn session_image(&self, session_id: i64) -> Option<ImageId> {
        self.sessions.get(&session_id).copied()
    }

    /// How many sessions the adapter is holding.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Forget a session, answering with the image it was created on.
    ///
    /// This is the map's half of `removeControlSession`
    /// (`ControlSessionAdapter.java:1150-1163`). The other half belongs to the
    /// conductor and is the reason the image comes back: an aborted session
    /// **rejects its image** (`:1152-1155`), which is a command to the driver
    /// and needs the image's position, and the session counter is the
    /// conductor's to release.
    pub fn remove_session(&mut self, session_id: i64) -> Option<ImageId> {
        self.sessions.remove(&session_id)
    }

    /// What has been read and reassembled so far, and what was abandoned on the
    /// way (`FragmentAssembler`'s own counts).
    pub const fn assembler(&self) -> &FragmentAssembler {
        &self.assembler
    }
}

/// The switch, and the two gates in front of most of it.
fn dispatch<C: ControlPlane, A: AuthorisationService>(
    message: Message<'_>,
    image: ImageId,
    sessions: &mut HashMap<i64, ImageId>,
    control: &mut C,
    authorisation: &A,
    now_ms: i64,
) -> Result<(), ControlError> {
    let payload = message.payload;

    // The reference reads the header without looking at the length first and
    // lets its buffer throw for a message too short to hold one. This build's
    // generated reader panics on an index past the end instead of reporting it,
    // so the length is checked here, before anything is read.
    if payload.len() < message_header_codec::ENCODED_LENGTH {
        return Err(ControlError::ShortMessage {
            length: payload.len(),
        });
    }

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);

    let schema_id = header.schema_id();
    if schema_id != SBE_SCHEMA_ID {
        return Err(ControlError::UnexpectedSchemaId {
            expected: SBE_SCHEMA_ID,
            actual: schema_id,
        });
    }

    let template_id = header.template_id();

    match template_id {
        // The only request that makes a session (`ControlSessionAdapter.java:766`).
        auth_connect_request_codec::SBE_TEMPLATE_ID => {
            let mut decoder = AuthConnectRequestDecoder::default().header(header, 0);

            // Every `_decoder` call moves the decoder's limit over its own
            // length, so they are all made before any slice is taken: a slice
            // borrows the decoder for as long as the message lives.
            let channel_coordinates = decoder.response_channel_decoder();
            let credentials_coordinates = decoder.encoded_credentials_decoder();
            let client_info_coordinates = decoder.client_info_decoder();

            let encoded_credentials = decoder.encoded_credentials_slice(credentials_coordinates);
            let response_channel =
                String::from_utf8_lossy(decoder.response_channel_slice(channel_coordinates));
            let client_info =
                String::from_utf8_lossy(decoder.client_info_slice(client_info_coordinates));

            let request = ConnectRequest {
                correlation_id: decoder.correlation_id(),
                response_stream_id: decoder.response_stream_id(),
                version: decoder.version().unwrap_or(0),
                response_channel: &response_channel,
                encoded_credentials,
                client_info: &client_info,
            };

            let session_id = control.new_session(image, request, now_ms);
            sessions.insert(session_id, image);
        }

        // Not gated on the session, the image, or being ACTIVE: a session that
        // is being challenged has no principal yet, so there is nothing to
        // authorise, and the reference looks the session up in its map directly
        // (`ControlSessionAdapter.java:804-833`). A challenge answered on
        // another image is therefore *taken*, which is the reference's
        // behaviour and not an oversight to fix here.
        challenge_response_codec::SBE_TEMPLATE_ID => {
            let mut decoder = ChallengeResponseDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let coordinates = decoder.encoded_credentials_decoder();
            let encoded_credentials = decoder.encoded_credentials_slice(coordinates);

            if sessions.contains_key(&control_session_id) {
                control.on_challenge_response(
                    control_session_id,
                    correlation_id,
                    encoded_credentials,
                    now_ms,
                );
            }
        }

        keep_alive_request_codec::SBE_TEMPLATE_ID => {
            let decoder = KeepAliveRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_keep_alive(session_id);
            }
        }

        // The four questions whose answer is a position
        // (`ArchiveConductor.java:1159-1195`). They are the same request shape
        // four times over — session, correlation, recording — so they are the
        // same arm four times over, and the difference is which
        // [`Query`] is named.
        start_position_request_codec::SBE_TEMPLATE_ID
        | recording_position_request_codec::SBE_TEMPLATE_ID
        | stop_position_request_codec::SBE_TEMPLATE_ID
        | max_recorded_position_request_codec::SBE_TEMPLATE_ID => {
            let (control_session_id, correlation_id, recording_id) = match template_id {
                start_position_request_codec::SBE_TEMPLATE_ID => {
                    let decoder = StartPositionRequestDecoder::default().header(header, 0);
                    (
                        decoder.control_session_id(),
                        decoder.correlation_id(),
                        decoder.recording_id(),
                    )
                }
                recording_position_request_codec::SBE_TEMPLATE_ID => {
                    let decoder = RecordingPositionRequestDecoder::default().header(header, 0);
                    (
                        decoder.control_session_id(),
                        decoder.correlation_id(),
                        decoder.recording_id(),
                    )
                }
                stop_position_request_codec::SBE_TEMPLATE_ID => {
                    let decoder = StopPositionRequestDecoder::default().header(header, 0);
                    (
                        decoder.control_session_id(),
                        decoder.correlation_id(),
                        decoder.recording_id(),
                    )
                }
                _ => {
                    let decoder = MaxRecordedPositionRequestDecoder::default().header(header, 0);
                    (
                        decoder.control_session_id(),
                        decoder.correlation_id(),
                        decoder.recording_id(),
                    )
                }
            };

            let query = match template_id {
                start_position_request_codec::SBE_TEMPLATE_ID => {
                    Query::StartPosition { recording_id }
                }
                recording_position_request_codec::SBE_TEMPLATE_ID => {
                    Query::RecordingPosition { recording_id }
                }
                stop_position_request_codec::SBE_TEMPLATE_ID => {
                    Query::StopPosition { recording_id }
                }
                _ => Query::MaxRecordedPosition { recording_id },
            };

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_query(session_id, correlation_id, query, now_ms);
            }
        }

        // The last of the questions about a recording, and the one that does not
        // name one: it asks which recording matches a session, a stream and a
        // piece of a channel (`ArchiveConductor.java:742-761`).
        find_last_matching_recording_request_codec::SBE_TEMPLATE_ID => {
            let mut decoder = FindLastMatchingRecordingRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let min_recording_id = decoder.min_recording_id();
            let matching_session_id = decoder.session_id();
            let stream_id = decoder.stream_id();
            let coordinates = decoder.channel_decoder();
            let channel_fragment = decoder.channel_slice(coordinates).to_vec();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_query(
                    session_id,
                    correlation_id,
                    Query::FindLastMatching {
                        min_recording_id,
                        session_id: matching_session_id,
                        stream_id,
                        channel_fragment,
                    },
                    now_ms,
                );
            }
        }

        // One recording's descriptor (`ArchiveConductor.java:688-706`). The
        // answer is not this turn's to send: the listing session the conductor
        // starts is what offers the message until the client takes it.
        list_recording_request_codec::SBE_TEMPLATE_ID => {
            let decoder = ListRecordingRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let recording_id = decoder.recording_id();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_list_recording(session_id, correlation_id, recording_id, now_ms);
            }
        }

        // A page of recordings (`ArchiveConductor.java:638-657`), which is the
        // same session shape as the one above with a walk in it instead of one
        // descriptor. Where it starts and how many it answers with are the
        // request's own two fields.
        list_recordings_request_codec::SBE_TEMPLATE_ID => {
            let decoder = ListRecordingsRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let from_recording_id = decoder.from_recording_id();
            let count = decoder.record_count();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_list_recordings(
                    session_id,
                    correlation_id,
                    from_recording_id,
                    count,
                    now_ms,
                );
            }
        }

        // The same page narrowed to a stream and a channel
        // (`ArchiveConductor.java:659-687`). The C client sends both this and
        // the one above (`aeron_archive_proxy.c:530-563` against `:505-528`),
        // and `streamId` with the channel fragment are the whole of the
        // difference between them.
        list_recordings_for_uri_request_codec::SBE_TEMPLATE_ID => {
            let mut decoder = ListRecordingsForUriRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let from_recording_id = decoder.from_recording_id();
            let count = decoder.record_count();
            let stream_id = decoder.stream_id();
            let coordinates = decoder.channel_decoder();
            let channel_fragment = decoder.channel_slice(coordinates).to_vec();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_list_recordings_for_uri(
                    session_id,
                    correlation_id,
                    from_recording_id,
                    count,
                    stream_id,
                    &channel_fragment,
                    now_ms,
                );
            }
        }

        // Asking an archive to record a channel. Two templates, and the only
        // difference between them is what the second one added: `autoStop`
        // (`ArchiveConductor.java:530-536` against `:162-186` of the adapter,
        // whose v1 arm reads four fields and not five). The C client uses the
        // second (`aeron_archive_proxy.c:248-280`), and the first is answered
        // the same way, as a request that did not ask for it.
        start_recording_request_2_codec::SBE_TEMPLATE_ID
        | start_recording_request_codec::SBE_TEMPLATE_ID => {
            let (
                control_session_id,
                correlation_id,
                stream_id,
                source_location,
                auto_stop,
                channel,
            ) = if template_id == start_recording_request_2_codec::SBE_TEMPLATE_ID {
                let mut decoder = StartRecordingRequest2Decoder::default().header(header, 0);
                let coordinates = decoder.channel_decoder();

                (
                    decoder.control_session_id(),
                    decoder.correlation_id(),
                    decoder.stream_id(),
                    decoder.source_location(),
                    decoder.auto_stop() == BooleanType::TRUE,
                    decoder.channel_slice(coordinates).to_vec(),
                )
            } else {
                let mut decoder = StartRecordingRequestDecoder::default().header(header, 0);
                let coordinates = decoder.channel_decoder();

                (
                    decoder.control_session_id(),
                    decoder.correlation_id(),
                    decoder.stream_id(),
                    decoder.source_location(),
                    // The version without the field is passed `false` for it
                    // (`ControlSessionAdapter.java:180-185`), which leaves the
                    // recording running when the client goes away.
                    false,
                    decoder.channel_slice(coordinates).to_vec(),
                )
            };

            let original_channel = String::from_utf8_lossy(&channel).into_owned();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_start_recording(
                    StartRecordingRequest {
                        session_id,
                        correlation_id,
                        stream_id,
                        source_location,
                        auto_stop,
                        original_channel,
                    },
                    now_ms,
                );
            }
        }

        // Asking an archive to replay a recording. Two templates again, and the
        // one field the second added is the whole of "bounded"
        // (`ControlSessionAdapter.java:515-560` against `:209-252`).
        //
        // `file_io_max_length` needs no guard here: the generated decoder
        // answers `int32::MIN` for a request older than its version
        // (`replay_request_codec.rs:326-333`), and the reference's own guard
        // passes `NULL_VALUE` for exactly the same case (`:220-221`). Both are
        // "not positive", which is all the replay asks of that field.
        // `replay_token` is **not** like it, and is guarded below.
        replay_request_codec::SBE_TEMPLATE_ID | bounded_replay_request_codec::SBE_TEMPLATE_ID => {
            // Read before the decoders, which take the header by value.
            let acting_version = header.version();

            let (
                control_session_id,
                correlation_id,
                recording_id,
                position,
                length,
                replay_stream_id,
                file_io_max_length,
                replay_token,
                limit_counter_id,
                channel,
            ) = if template_id == replay_request_codec::SBE_TEMPLATE_ID {
                let mut decoder = ReplayRequestDecoder::default().header(header, 0);
                let coordinates = decoder.replay_channel_decoder();

                (
                    decoder.control_session_id(),
                    decoder.correlation_id(),
                    decoder.recording_id(),
                    decoder.position(),
                    decoder.length(),
                    decoder.replay_stream_id(),
                    decoder.file_io_max_length(),
                    token(decoder.replay_token(), acting_version),
                    None,
                    decoder.replay_channel_slice(coordinates).to_vec(),
                )
            } else {
                let mut decoder = BoundedReplayRequestDecoder::default().header(header, 0);
                let coordinates = decoder.replay_channel_decoder();

                (
                    decoder.control_session_id(),
                    decoder.correlation_id(),
                    decoder.recording_id(),
                    decoder.position(),
                    decoder.length(),
                    decoder.replay_stream_id(),
                    decoder.file_io_max_length(),
                    token(decoder.replay_token(), acting_version),
                    Some(i64::from(decoder.limit_counter_id())),
                    decoder.replay_channel_slice(coordinates).to_vec(),
                )
            };

            let channel = String::from_utf8_lossy(&channel).into_owned();

            // **Which session answers** is decided before anything else, and on
            // this path it is not the gate's answer
            // (`setupSessionAndChannelForReplay`, `:1165-1190`): a request
            // carrying a token for a `control-mode=response` channel arrived on
            // the short-lived publication the client made to ask for one, so
            // its session's image is not the image it came in on and the gate
            // would drop it in silence. The token stands in for both halves of
            // the gate — see [`ControlPlane::replay_session_for_token`].
            let (asked, response_correlation_id) =
                if is_response_channel(&channel) && replay_token != NULL_VALUE {
                    match control.replay_session_for_token(replay_token, recording_id, now_ms) {
                        // The image the request arrived on, which is the one the
                        // replay answers (`:1183`).
                        Some(session_id) => (Some(session_id), Some(image.correlation_id())),
                        None => {
                            // The reference throws here (`:1180`), which reaches
                            // the archive's error handler and sends the client
                            // nothing; this is that outcome as a value, one
                            // frame earlier.
                            control.log_warning(&format!(
                                "Unknown session or token timeout for \
                                 replayToken={replay_token} recordingId={recording_id}"
                            ));
                            (None, None)
                        }
                    }
                } else {
                    (
                        gate(
                            sessions,
                            control,
                            authorisation,
                            image,
                            control_session_id,
                            template_id,
                            correlation_id,
                            now_ms,
                        ),
                        None,
                    )
                };

            if let Some(session_id) = asked {
                control.on_start_replay(
                    StartReplayRequest {
                        session_id,
                        correlation_id,
                        recording_id,
                        position,
                        length,
                        replay_channel: channel,
                        replay_stream_id,
                        file_io_max_length,
                        limit_counter_id,
                        response_correlation_id,
                    },
                    now_ms,
                );
            }
        }

        stop_replay_request_codec::SBE_TEMPLATE_ID => {
            let decoder = StopReplayRequestDecoder::default().header(header, 0);
            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let replay_session_id = decoder.replay_session_id();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_stop_replay(session_id, correlation_id, replay_session_id, now_ms);
            }
        }

        stop_all_replays_request_codec::SBE_TEMPLATE_ID => {
            let decoder = StopAllReplaysRequestDecoder::default().header(header, 0);
            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let recording_id = decoder.recording_id();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_stop_all_replays(session_id, correlation_id, recording_id, now_ms);
            }
        }

        // A client asking for a **token** to replay with
        // (`ControlSessionAdapter.java:1084-1105`), which is what lets the
        // replay itself arrive on a channel of the client's own — see
        // [`ControlPlane::replay_session_for_token`].
        //
        // The request is gated like any other, by the ordinary two gates, even
        // though the request it enables is not: the token is a capability, and
        // this is where it is earned.
        //
        // It names any recording at all. The reference does **not** ask the
        // catalog whether there is one (`:1097-1101`), and neither does this:
        // the replay that spends the token is where a recording is looked up,
        // and a token for a recording that does not exist is an `OK` followed
        // by `UNKNOWN_RECORDING`.
        replay_token_request_codec::SBE_TEMPLATE_ID => {
            let decoder = ReplayTokenRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let recording_id = decoder.recording_id();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_replay_token(session_id, correlation_id, recording_id, now_ms);
            }
        }

        stop_recording_subscription_request_codec::SBE_TEMPLATE_ID => {
            let decoder = StopRecordingSubscriptionRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let subscription_id = decoder.subscription_id();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_stop_recording_subscription(
                    session_id,
                    correlation_id,
                    subscription_id,
                    now_ms,
                );
            }
        }

        // What this archive has been told to record
        // (`ArchiveConductor.java:1266-1300`). Its two gates are the session's,
        // so the decision is the callback's — see the conductor's
        // `on_list_recording_subscriptions`.
        list_recording_subscriptions_request_codec::SBE_TEMPLATE_ID => {
            let mut decoder = ListRecordingSubscriptionsRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let pseudo_index = decoder.pseudo_index();
            let subscription_count = decoder.subscription_count();
            let apply_stream_id = decoder.apply_stream_id() == BooleanType::TRUE;
            let stream_id = decoder.stream_id();
            let coordinates = decoder.channel_decoder();
            let channel_fragment =
                String::from_utf8_lossy(decoder.channel_slice(coordinates)).into_owned();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_list_recording_subscriptions(
                    session_id,
                    correlation_id,
                    pseudo_index,
                    subscription_count,
                    apply_stream_id,
                    stream_id,
                    &channel_fragment,
                    now_ms,
                );
            }
        }

        // Asking an archive to go on recording a channel whose session ended.
        // The older version of this request (11) has no `autoStop` and is not
        // answered here: the C client sends the second
        // (`aeron_archive_proxy.c:758-793`), and this slice answers what the
        // acceptance sends.
        extend_recording_request_2_codec::SBE_TEMPLATE_ID => {
            let mut decoder = ExtendRecordingRequest2Decoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let recording_id = decoder.recording_id();
            let stream_id = decoder.stream_id();
            let source_location = decoder.source_location();
            let auto_stop = decoder.auto_stop() == BooleanType::TRUE;
            let coordinates = decoder.channel_decoder();
            let original_channel =
                String::from_utf8_lossy(decoder.channel_slice(coordinates)).into_owned();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_extend_recording(
                    ExtendRecordingRequest {
                        session_id,
                        correlation_id,
                        recording_id,
                        stream_id,
                        source_location,
                        auto_stop,
                        original_channel,
                    },
                    now_ms,
                );
            }
        }

        // The two stops that are not named by a subscription id: one names the
        // channel and stream a recording was started with
        // (`ArchiveConductor.java:585-610`), the other the recording itself
        // (`:1301-1327`).
        stop_recording_request_codec::SBE_TEMPLATE_ID => {
            let mut decoder = StopRecordingRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let stream_id = decoder.stream_id();
            let coordinates = decoder.channel_decoder();
            let channel = String::from_utf8_lossy(decoder.channel_slice(coordinates)).into_owned();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_stop_recording(session_id, correlation_id, stream_id, &channel, now_ms);
            }
        }

        stop_recording_by_identity_request_codec::SBE_TEMPLATE_ID => {
            let decoder = StopRecordingByIdentityRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let recording_id = decoder.recording_id();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_stop_recording_by_identity(
                    session_id,
                    correlation_id,
                    recording_id,
                    now_ms,
                );
            }
        }

        archive_id_request_codec::SBE_TEMPLATE_ID => {
            let decoder = ArchiveIdRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_archive_id(session_id, correlation_id, now_ms);
            }
        }

        // Like the challenge answer, not gated — but this one is not silent
        // either: the reference asks the map directly, tests the image itself
        // and says nothing at all when either fails
        // (`ControlSessionAdapter.java:144-160`).
        close_session_request_codec::SBE_TEMPLATE_ID => {
            let decoder = CloseSessionRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            if sessions.get(&control_session_id) == Some(&image) {
                control.abort_session(control_session_id, SESSION_CLOSED_MSG);
            }
        }

        // The two that move a recording's **start** (`ControlSessionAdapter.java`
        // `DetachSegmentsRequestDecoder` / `DeleteDetachedSegmentsRequestDecoder`,
        // `:1408-1446`). A detach is one catalog write and answers immediately;
        // deleting what it detached is files, and files are a session.
        detach_segments_request_codec::SBE_TEMPLATE_ID => {
            let decoder = DetachSegmentsRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let recording_id = decoder.recording_id();
            let new_start_position = decoder.new_start_position();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_detach_segments(
                    session_id,
                    correlation_id,
                    recording_id,
                    new_start_position,
                    now_ms,
                );
            }
        }

        delete_detached_segments_request_codec::SBE_TEMPLATE_ID => {
            let decoder = DeleteDetachedSegmentsRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let recording_id = decoder.recording_id();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_delete_detached_segments(
                    session_id,
                    correlation_id,
                    recording_id,
                    now_ms,
                );
            }
        }

        update_channel_request_codec::SBE_TEMPLATE_ID => {
            let mut decoder = UpdateChannelRequestDecoder::default().header(header, 0);
            let coordinates = decoder.channel_decoder();

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let recording_id = decoder.recording_id();
            let channel = String::from_utf8_lossy(decoder.channel_slice(coordinates)).into_owned();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_update_channel(
                    session_id,
                    correlation_id,
                    recording_id,
                    &channel,
                    now_ms,
                );
            }
        }

        attach_segments_request_codec::SBE_TEMPLATE_ID => {
            let decoder = AttachSegmentsRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let recording_id = decoder.recording_id();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_attach_segments(session_id, correlation_id, recording_id, now_ms);
            }
        }

        truncate_recording_request_codec::SBE_TEMPLATE_ID => {
            let decoder = TruncateRecordingRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let recording_id = decoder.recording_id();
            let position = decoder.position();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_truncate_recording(
                    session_id,
                    correlation_id,
                    recording_id,
                    position,
                    now_ms,
                );
            }
        }

        purge_recording_request_codec::SBE_TEMPLATE_ID => {
            let decoder = PurgeRecordingRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let recording_id = decoder.recording_id();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_purge_recording(session_id, correlation_id, recording_id, now_ms);
            }
        }

        purge_segments_request_codec::SBE_TEMPLATE_ID => {
            let decoder = PurgeSegmentsRequestDecoder::default().header(header, 0);

            let control_session_id = decoder.control_session_id();
            let correlation_id = decoder.correlation_id();
            let recording_id = decoder.recording_id();
            let new_start_position = decoder.new_start_position();

            if let Some(session_id) = gate(
                sessions,
                control,
                authorisation,
                image,
                control_session_id,
                template_id,
                correlation_id,
                now_ms,
            ) {
                control.on_purge_segments(
                    session_id,
                    correlation_id,
                    recording_id,
                    new_start_position,
                    now_ms,
                );
            }
        }

        // Everything else is a request this slice does not answer yet: the
        // recording, replay, listing and replication families arrive with the
        // slices that can carry them out. The reference's switch has no default
        // arm, so an unknown template id is silent there; here it is named,
        // because a client is waiting for an answer and a request nobody
        // answers is a client timing out rather than a request that never came.
        _ => {
            control.log_warning(&format!(
                "control request for unimplemented templateId={template_id} \
                 image={image}"
            ));
        }
    }

    Ok(())
}

/// A replay token as the reference reads it (`ControlSessionAdapter.java:226-227`).
///
/// A request older than [`REPLAY_TOKEN_VERSION`] has no such field, and both
/// Java and this build substitute a value for it — but **not the same one**:
/// the reference substitutes `Aeron.NULL_VALUE` and the generated decoder
/// answers `i64::MIN` (`replay_request_codec.rs:336-342`), and the replay
/// distinguishes a token from no token by comparing against `NULL_VALUE`. Read
/// the field only when the request has it.
fn token(decoded: i64, acting_version: u16) -> i64 {
    if acting_version >= REPLAY_TOKEN_VERSION {
        decoded
    } else {
        NULL_VALUE
    }
}

/// Whether the channel a client asked to be replayed on is one it wants the
/// answer on (`ChannelUri.hasControlModeResponse`, `ChannelUri.java:653-656`).
///
/// A channel that will not parse is not one: the conductor parses it again to
/// build the replay's channel, and refuses the request there, which is the only
/// refusal that can name a reason.
fn is_response_channel(requested: &str) -> bool {
    ChannelUri::parse(requested)
        .is_ok_and(|uri| uri.get("control-mode") == Some(CONTROL_MODE_RESPONSE))
}

/// The two gates every request but a connect, a challenge answer and a close
/// passes through (`ControlSessionAdapter.java:1192-1216`).
///
/// Answers with the session id when the request may go ahead.
#[allow(clippy::too_many_arguments)] // one per thing the request carried
fn gate<C: ControlPlane, A: AuthorisationService>(
    sessions: &HashMap<i64, ImageId>,
    control: &mut C,
    authorisation: &A,
    image: ImageId,
    control_session_id: i64,
    template_id: u16,
    correlation_id: i64,
    now_ms: i64,
) -> Option<i64> {
    let Some(known_image) = sessions.get(&control_session_id) else {
        control.log_warning(&format!(
            "control request for unknown session: controlSessionId={control_session_id} \
             templateId={template_id}"
        ));
        return None;
    };

    // A session belongs to the image it was opened on. A request that arrives
    // on another one is dropped in silence — the client is told nothing, which
    // is what the reference does and what makes this look like a request that
    // never arrived.
    if *known_image != image {
        control.log_warning(&format!(
            "unauthorised archive action={template_id} \
             controlSessionId={control_session_id} source={image}"
        ));
        return None;
    }

    let principal = control.session_principal(control_session_id);
    if !authorisation.is_authorised(i32::from(SBE_SCHEMA_ID), i32::from(template_id), principal) {
        control.log_warning(&format!(
            "unauthorised archive action={template_id} \
             controlSessionId={control_session_id} source={image}"
        ));

        control.send_error_response(
            control_session_id,
            correlation_id,
            i64::from(UNAUTHORISED_ACTION),
            UNAUTHORISED_ACTION_MSG,
            now_ms,
        );

        return None;
    }

    Some(control_session_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;

    use crate::server::auth::{AllowAll, DenyAll};

    use deepmsg_codec::archive::WriteBuf;
    use deepmsg_codec::archive::attach_segments_request_codec::AttachSegmentsRequestEncoder;
    use deepmsg_codec::archive::auth_connect_request_codec::AuthConnectRequestEncoder;
    use deepmsg_codec::archive::challenge_response_codec::ChallengeResponseEncoder;
    use deepmsg_codec::archive::close_session_request_codec::CloseSessionRequestEncoder;
    use deepmsg_codec::archive::delete_detached_segments_request_codec::DeleteDetachedSegmentsRequestEncoder;
    use deepmsg_codec::archive::detach_segments_request_codec::DetachSegmentsRequestEncoder;
    use deepmsg_codec::archive::keep_alive_request_codec::KeepAliveRequestEncoder;
    use deepmsg_codec::archive::purge_recording_request_codec::PurgeRecordingRequestEncoder;
    use deepmsg_codec::archive::purge_segments_request_codec::PurgeSegmentsRequestEncoder;
    use deepmsg_codec::archive::replay_request_codec::ReplayRequestEncoder;
    use deepmsg_codec::archive::replay_token_request_codec::ReplayTokenRequestEncoder;
    use deepmsg_codec::archive::start_recording_request_codec::{
        SBE_TEMPLATE_ID as START_RECORDING, StartRecordingRequestEncoder,
    };
    use deepmsg_codec::archive::truncate_recording_request_codec::TruncateRecordingRequestEncoder;
    use deepmsg_codec::archive::update_channel_request_codec::UpdateChannelRequestEncoder;

    use deepmsg_client::fragment_assembler::MessageHeader;

    /// Where the body of a hand-built message starts.
    const BODY: usize = message_header_codec::ENCODED_LENGTH;

    /// The image a session is created on, and another one it is not.
    const IMAGE: ImageId = ImageId::new(11, 22);
    const OTHER_IMAGE: ImageId = ImageId::new(33, 44);

    /// What the adapter asked the control plane to do, in order.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Call {
        NewSession {
            session_id: i64,
            image: ImageId,
            correlation_id: i64,
            response_stream_id: i32,
            version: i32,
            response_channel: String,
            encoded_credentials: Vec<u8>,
            client_info: String,
        },
        Abort {
            session_id: i64,
            reason: String,
        },
        ChallengeResponse {
            session_id: i64,
            correlation_id: i64,
            encoded_credentials: Vec<u8>,
        },
        KeepAlive {
            session_id: i64,
        },
        ArchiveId {
            session_id: i64,
            correlation_id: i64,
        },
        ErrorResponse {
            session_id: i64,
            correlation_id: i64,
            relevant_id: i64,
            message: String,
        },
        Query {
            session_id: i64,
            correlation_id: i64,
            query: Query,
        },
        ReplayToken {
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
        },
        DetachSegments {
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            new_start_position: i64,
        },
        DeleteDetachedSegments {
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
        },
        AttachSegments {
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
        },
        UpdateChannel {
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            channel: String,
        },
        TruncateRecording {
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            position: i64,
        },
        PurgeRecording {
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
        },
        PurgeSegments {
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            new_start_position: i64,
        },
        StartReplay {
            request: StartReplayRequest,
        },
        StopReplay {
            session_id: i64,
            correlation_id: i64,
            replay_session_id: i64,
        },
        StopAllReplays {
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
        },
        ListRecording {
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
        },
        ListRecordings {
            session_id: i64,
            correlation_id: i64,
            from_recording_id: i64,
            count: i32,
        },
        ListRecordingsForUri {
            session_id: i64,
            correlation_id: i64,
            from_recording_id: i64,
            count: i32,
            stream_id: i32,
            channel_fragment: Vec<u8>,
        },
        StartRecording(StartRecordingRequest),
        StopRecordingSubscription {
            session_id: i64,
            correlation_id: i64,
            subscription_id: i64,
        },
        StopRecording {
            session_id: i64,
            correlation_id: i64,
            stream_id: i32,
            original_channel: String,
        },
        ExtendRecording(ExtendRecordingRequest),
        ListRecordingSubscriptions {
            session_id: i64,
            correlation_id: i64,
            pseudo_index: i32,
            subscription_count: i32,
            apply_stream_id: bool,
            stream_id: i32,
            channel_fragment: String,
        },
        StopRecordingByIdentity {
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
        },
    }

    /// A control plane that writes down what it was asked and hands out session
    /// ids from one.
    #[derive(Default)]
    struct Recorder {
        calls: Vec<Call>,
        warnings: Vec<String>,
        /// What the sessions' authenticator is taken to have vouched for.
        principal: Option<Vec<u8>>,
        next_session_id: i64,
        /// The tokens that name a session, by `(token, recording id)`, which is
        /// the pair a real one is checked against.
        tokens: HashMap<(i64, i64), i64>,
    }

    impl ControlPlane for Recorder {
        fn new_session(
            &mut self,
            image: ImageId,
            request: ConnectRequest<'_>,
            _now_ms: i64,
        ) -> i64 {
            self.next_session_id += 1;

            self.calls.push(Call::NewSession {
                session_id: self.next_session_id,
                image,
                correlation_id: request.correlation_id,
                response_stream_id: request.response_stream_id,
                version: request.version,
                response_channel: request.response_channel.to_owned(),
                encoded_credentials: request.encoded_credentials.to_vec(),
                client_info: request.client_info.to_owned(),
            });

            self.next_session_id
        }

        fn session_principal(&self, _session_id: i64) -> Option<&[u8]> {
            self.principal.as_deref()
        }

        fn abort_session(&mut self, session_id: i64, reason: &str) {
            self.calls.push(Call::Abort {
                session_id,
                reason: reason.to_owned(),
            });
        }

        fn on_challenge_response(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            encoded_credentials: &[u8],
            _now_ms: i64,
        ) {
            self.calls.push(Call::ChallengeResponse {
                session_id,
                correlation_id,
                encoded_credentials: encoded_credentials.to_vec(),
            });
        }

        fn on_keep_alive(&mut self, session_id: i64) {
            self.calls.push(Call::KeepAlive { session_id });
        }

        fn on_archive_id(&mut self, session_id: i64, correlation_id: i64, _now_ms: i64) {
            self.calls.push(Call::ArchiveId {
                session_id,
                correlation_id,
            });
        }

        fn send_error_response(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            relevant_id: i64,
            message: &str,
            _now_ms: i64,
        ) {
            self.calls.push(Call::ErrorResponse {
                session_id,
                correlation_id,
                relevant_id,
                message: message.to_owned(),
            });
        }

        fn log_warning(&mut self, message: &str) {
            self.warnings.push(message.to_owned());
        }

        fn on_query(&mut self, session_id: i64, correlation_id: i64, query: Query, _now_ms: i64) {
            self.calls.push(Call::Query {
                session_id,
                correlation_id,
                query,
            });
        }

        fn on_list_recording(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            _now_ms: i64,
        ) {
            self.calls.push(Call::ListRecording {
                session_id,
                correlation_id,
                recording_id,
            });
        }

        fn on_list_recordings(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            from_recording_id: i64,
            count: i32,
            _now_ms: i64,
        ) {
            self.calls.push(Call::ListRecordings {
                session_id,
                correlation_id,
                from_recording_id,
                count,
            });
        }

        #[allow(clippy::too_many_arguments)] // one per field the request carries
        fn on_list_recordings_for_uri(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            from_recording_id: i64,
            count: i32,
            stream_id: i32,
            channel_fragment: &[u8],
            _now_ms: i64,
        ) {
            self.calls.push(Call::ListRecordingsForUri {
                session_id,
                correlation_id,
                from_recording_id,
                count,
                stream_id,
                channel_fragment: channel_fragment.to_vec(),
            });
        }

        /// The tokens this double knows, which is how a test says which
        /// session a token names. Empty unless a test put something in it.
        fn replay_session_for_token(
            &self,
            replay_token: i64,
            recording_id: i64,
            _now_ms: i64,
        ) -> Option<i64> {
            self.tokens.get(&(replay_token, recording_id)).copied()
        }

        fn on_replay_token(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            _now_ms: i64,
        ) {
            self.calls.push(Call::ReplayToken {
                session_id,
                correlation_id,
                recording_id,
            });
        }

        #[allow(clippy::too_many_arguments)] // mirrors the trait
        fn on_detach_segments(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            new_start_position: i64,
            _now_ms: i64,
        ) {
            self.calls.push(Call::DetachSegments {
                session_id,
                correlation_id,
                recording_id,
                new_start_position,
            });
        }

        fn on_delete_detached_segments(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            _now_ms: i64,
        ) {
            self.calls.push(Call::DeleteDetachedSegments {
                session_id,
                correlation_id,
                recording_id,
            });
        }

        #[allow(clippy::too_many_arguments)] // mirrors the trait
        fn on_update_channel(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            channel: &str,
            _now_ms: i64,
        ) {
            self.calls.push(Call::UpdateChannel {
                session_id,
                correlation_id,
                recording_id,
                channel: channel.to_owned(),
            });
        }

        fn on_attach_segments(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            _now_ms: i64,
        ) {
            self.calls.push(Call::AttachSegments {
                session_id,
                correlation_id,
                recording_id,
            });
        }

        #[allow(clippy::too_many_arguments)] // mirrors the trait
        fn on_truncate_recording(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            position: i64,
            _now_ms: i64,
        ) {
            self.calls.push(Call::TruncateRecording {
                session_id,
                correlation_id,
                recording_id,
                position,
            });
        }

        fn on_purge_recording(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            _now_ms: i64,
        ) {
            self.calls.push(Call::PurgeRecording {
                session_id,
                correlation_id,
                recording_id,
            });
        }

        #[allow(clippy::too_many_arguments)] // mirrors the trait
        fn on_purge_segments(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            new_start_position: i64,
            _now_ms: i64,
        ) {
            self.calls.push(Call::PurgeSegments {
                session_id,
                correlation_id,
                recording_id,
                new_start_position,
            });
        }

        fn on_start_replay(&mut self, request: StartReplayRequest, _now_ms: i64) {
            self.calls.push(Call::StartReplay { request });
        }

        fn on_stop_replay(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            replay_session_id: i64,
            _now_ms: i64,
        ) {
            self.calls.push(Call::StopReplay {
                session_id,
                correlation_id,
                replay_session_id,
            });
        }

        fn on_stop_all_replays(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            _now_ms: i64,
        ) {
            self.calls.push(Call::StopAllReplays {
                session_id,
                correlation_id,
                recording_id,
            });
        }

        fn on_start_recording(&mut self, request: StartRecordingRequest, _now_ms: i64) {
            self.calls.push(Call::StartRecording(request));
        }

        fn on_stop_recording_subscription(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            subscription_id: i64,
            _now_ms: i64,
        ) {
            self.calls.push(Call::StopRecordingSubscription {
                session_id,
                correlation_id,
                subscription_id,
            });
        }

        fn on_extend_recording(&mut self, request: ExtendRecordingRequest, _now_ms: i64) {
            self.calls.push(Call::ExtendRecording(request));
        }

        fn on_list_recording_subscriptions(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            pseudo_index: i32,
            subscription_count: i32,
            apply_stream_id: bool,
            stream_id: i32,
            channel_fragment: &str,
            _now_ms: i64,
        ) {
            self.calls.push(Call::ListRecordingSubscriptions {
                session_id,
                correlation_id,
                pseudo_index,
                subscription_count,
                apply_stream_id,
                stream_id,
                channel_fragment: channel_fragment.to_owned(),
            });
        }

        fn on_stop_recording(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            stream_id: i32,
            original_channel: &str,
            _now_ms: i64,
        ) {
            self.calls.push(Call::StopRecording {
                session_id,
                correlation_id,
                stream_id,
                original_channel: original_channel.to_owned(),
            });
        }

        fn on_stop_recording_by_identity(
            &mut self,
            session_id: i64,
            correlation_id: i64,
            recording_id: i64,
            _now_ms: i64,
        ) {
            self.calls.push(Call::StopRecordingByIdentity {
                session_id,
                correlation_id,
                recording_id,
            });
        }
    }

    /// One authorisation question: the two ids and the principal it was asked
    /// about.
    type Question = (i32, i32, Option<Vec<u8>>);

    /// An authorisation service that writes down what it was asked and answers
    /// `true`.
    #[derive(Default)]
    struct Recording {
        calls: RefCell<Vec<Question>>,
    }

    impl AuthorisationService for Recording {
        fn is_authorised(
            &self,
            protocol_id: i32,
            action_id: i32,
            encoded_principal: Option<&[u8]>,
        ) -> bool {
            self.calls.borrow_mut().push((
                protocol_id,
                action_id,
                encoded_principal.map(<[u8]>::to_vec),
            ));
            true
        }
    }

    /// An adapter over a `Recorder`, with the authorisation service given.
    fn adapter_with<A: AuthorisationService>(authorisation: A) -> (ControlAdapter<A>, Recorder) {
        (
            ControlAdapter::new(Some(7), 8, authorisation),
            Recorder::default(),
        )
    }

    /// The message the tests dispatch, as the assembler would hand it over.
    ///
    /// The header is the publication's, and the adapter reads the image from
    /// its own argument rather than this — the two are separate on purpose, so
    /// that a test that gets them out of step is testing what it means to.
    fn message(payload: &[u8]) -> Message<'_> {
        Message {
            header: MessageHeader {
                session_id: IMAGE.session_id(),
                stream_id: 10,
                term_offset: 0,
                flags: 0,
                position: 0,
                frame_length: 0,
                fragmented_frame_length: 0,
            },
            payload,
        }
    }

    /// A connect request, encoded by the same codec a client writes it with.
    fn auth_connect(
        correlation_id: i64,
        response_stream_id: i32,
        version: i32,
        response_channel: &str,
        encoded_credentials: &[u8],
        client_info: &str,
    ) -> Vec<u8> {
        let mut buffer = vec![0u8; 1024];

        let length = {
            let encoder =
                AuthConnectRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();

            encoder
                .correlation_id(correlation_id)
                .response_stream_id(response_stream_id)
                .version(version)
                .response_channel(response_channel.as_bytes())
                .encoded_credentials(encoded_credentials)
                .client_info(client_info.as_bytes());

            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// A connect whose only interesting field is the correlation id.
    fn a_connect(correlation_id: i64) -> Vec<u8> {
        auth_connect(
            correlation_id,
            20,
            0x0001_0000,
            "aeron:udp?endpoint=localhost:0",
            &[],
            "",
        )
    }

    fn keep_alive(control_session_id: i64, correlation_id: i64) -> Vec<u8> {
        let mut buffer = vec![0u8; 64];

        let length = {
            let encoder = KeepAliveRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id);
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    fn close_session(control_session_id: i64) -> Vec<u8> {
        let mut buffer = vec![0u8; 64];

        let length = {
            let encoder =
                CloseSessionRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder.control_session_id(control_session_id);
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    fn challenge_answer(
        control_session_id: i64,
        correlation_id: i64,
        encoded_credentials: &[u8],
    ) -> Vec<u8> {
        let mut buffer = vec![0u8; 256];

        let length = {
            let encoder =
                ChallengeResponseEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .encoded_credentials(encoded_credentials);
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// An `UpdateChannelRequest` (template 107).
    fn update_channel(
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        channel: &str,
    ) -> Vec<u8> {
        let mut buffer = vec![0u8; 512];

        let length = {
            let encoder =
                UpdateChannelRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id)
                .channel(channel.as_bytes());
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// An `AttachSegmentsRequest` (template 56).
    fn attach_segments(control_session_id: i64, correlation_id: i64, recording_id: i64) -> Vec<u8> {
        let mut buffer = vec![0u8; 128];

        let length = {
            let encoder =
                AttachSegmentsRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// A `TruncateRecordingRequest` (template 13).
    fn truncate_recording(
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        position: i64,
    ) -> Vec<u8> {
        let mut buffer = vec![0u8; 128];

        let length = {
            let encoder =
                TruncateRecordingRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id)
                .position(position);
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// A `PurgeRecordingRequest` (template 104).
    fn purge_recording(control_session_id: i64, correlation_id: i64, recording_id: i64) -> Vec<u8> {
        let mut buffer = vec![0u8; 128];

        let length = {
            let encoder =
                PurgeRecordingRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// A `PurgeSegmentsRequest` (template 55).
    fn purge_segments(
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        new_start_position: i64,
    ) -> Vec<u8> {
        let mut buffer = vec![0u8; 128];

        let length = {
            let encoder =
                PurgeSegmentsRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id)
                .new_start_position(new_start_position);
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// A `DetachSegmentsRequest` (template 53).
    fn detach_segments(
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        new_start_position: i64,
    ) -> Vec<u8> {
        let mut buffer = vec![0u8; 128];

        let length = {
            let encoder =
                DetachSegmentsRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id)
                .new_start_position(new_start_position);
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// A `DeleteDetachedSegmentsRequest` (template 54).
    fn delete_detached_segments(
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    ) -> Vec<u8> {
        let mut buffer = vec![0u8; 128];

        let length = {
            let encoder = DeleteDetachedSegmentsRequestEncoder::default()
                .wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// A `ReplayTokenRequest` (template 105).
    fn replay_token_request(
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    ) -> Vec<u8> {
        let mut buffer = vec![0u8; 64];

        let length = {
            let encoder =
                ReplayTokenRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// A `ReplayRequest` (template 6), with both of the fields the version
    /// guard covers.
    fn replay_request(
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        replay_token: i64,
        replay_channel: &str,
    ) -> Vec<u8> {
        let mut buffer = vec![0u8; 512];

        let length = {
            let encoder = ReplayRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id)
                .position(0)
                .length(4096)
                .replay_stream_id(66)
                .file_io_max_length(4096)
                .replay_token(replay_token)
                .replay_channel(replay_channel.as_bytes());
            BODY + encoder.encoded_length()
        };

        buffer.truncate(length);
        buffer
    }

    /// The channel the reference's own response-channel replay tests use
    /// (`client/aeron_archive_test.cpp:3434`).
    const RESPONSE_CHANNEL: &str = "aeron:udp?control-mode=response|control=localhost:10002";

    /// The recording id every request below names, which is what a token is
    /// checked against.
    const RECORDING_ID: i64 = 7;

    /// The token every request below carries when it carries one.
    const REPLAY_TOKEN: i64 = 4_242;

    /// Open a session on [`IMAGE`] and hand back its id.
    fn an_open_session<A: AuthorisationService>(
        adapter: &mut ControlAdapter<A>,
        control: &mut Recorder,
    ) -> i64 {
        let connect = a_connect(1);
        adapter
            .on_message(control, IMAGE, message(&connect), 0)
            .expect("a connect is read");

        let Call::NewSession { session_id, .. } = control.calls[0] else {
            panic!("the connect did not make a session");
        };

        session_id
    }

    /// The connect is the request that makes a session, and everything it
    /// carried reaches the control plane (`ControlSessionAdapter.java:766-802`).
    #[test]
    fn a_connect_makes_a_session_from_what_it_carried() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let connect = auth_connect(
            17,
            21,
            0x0001_0002,
            "aeron:udp?endpoint=localhost:0|control-mode=response",
            b"credentials",
            "a client",
        );

        adapter
            .on_message(&mut control, IMAGE, message(&connect), 1_000)
            .expect("read");

        assert_eq!(
            vec![Call::NewSession {
                session_id: 1,
                image: IMAGE,
                correlation_id: 17,
                response_stream_id: 21,
                version: 0x0001_0002,
                response_channel: "aeron:udp?endpoint=localhost:0|control-mode=response".to_owned(),
                encoded_credentials: b"credentials".to_vec(),
                client_info: "a client".to_owned(),
            }],
            control.calls
        );
        assert_eq!(Some(IMAGE), adapter.session_image(1));
        assert!(control.warnings.is_empty());
    }

    /// An empty credentials blob and an absent client info are read as the
    /// empty ones rather than left undecoded — the reference decodes all three
    /// var-data fields unconditionally (`ControlSessionAdapter.java:775-793`).
    #[test]
    fn a_connect_with_empty_var_data_is_still_read() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let connect = auth_connect(1, 20, 0x0001_0000, "aeron:ipc", &[], "");

        adapter
            .on_message(&mut control, IMAGE, message(&connect), 0)
            .expect("read");

        let Call::NewSession {
            encoded_credentials,
            client_info,
            response_channel,
            ..
        } = &control.calls[0]
        else {
            panic!("not a connect: {:?}", control.calls);
        };

        assert!(encoded_credentials.is_empty());
        assert!(client_info.is_empty());
        assert_eq!("aeron:ipc", response_channel);
    }

    /// A client that says nothing about its version is read as version zero,
    /// which is what the reference's decoder does with an absent optional field
    /// and what makes it fail the archive's version gate.
    #[test]
    fn an_absent_version_reads_as_zero() {
        let (mut adapter, mut control) = adapter_with(AllowAll);

        let mut buffer = vec![0u8; 512];
        let length = {
            let encoder =
                AuthConnectRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .correlation_id(1)
                .response_stream_id(20)
                .version_opt(None)
                .response_channel(b"aeron:ipc")
                .encoded_credentials(&[])
                .client_info(&[]);
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);

        adapter
            .on_message(&mut control, IMAGE, message(&buffer), 0)
            .expect("read");

        let Call::NewSession { version, .. } = control.calls[0] else {
            panic!("not a connect");
        };
        assert_eq!(0, version);
    }

    /// A `ReplayTokenRequest` reaches the conductor, which is what mints the
    /// token (`ControlSessionAdapter.java:1084-1105`).
    ///
    /// It goes through the same two gates as any other request — the reference
    /// looks its session up with `getControlSession` (`:1094-1096`), the method
    /// that checks the image and the authorisation service — because the token
    /// is a capability and this is where it is earned.
    #[test]
    fn a_replay_token_request_reaches_the_session_that_asked() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = replay_token_request(session_id, 99, RECORDING_ID);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::ReplayToken {
                session_id,
                correlation_id: 99,
                recording_id: RECORDING_ID,
            }],
            control.calls[1..]
        );
    }

    /// And it is gated like any other, on both gates — the reference reaches
    /// its session through `getControlSession` (`ControlSessionAdapter.java:1097-1098`)
    /// and has nothing else in front of the token.
    #[test]
    fn a_replay_token_request_is_gated() {
        let (mut adapter, mut control) = adapter_with(DenyAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = replay_token_request(session_id, 99, RECORDING_ID);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::ErrorResponse {
                session_id,
                correlation_id: 99,
                relevant_id: i64::from(UNAUTHORISED_ACTION),
                message: UNAUTHORISED_ACTION_MSG.to_owned(),
            }],
            control.calls[1..],
            "refused before a token was minted"
        );
    }

    /// A replay on a `control-mode=response` channel that carries a token is
    /// let through by the token: the **token's** session answers, and the image
    /// the request arrived on goes with it
    /// (`ControlSessionAdapter.java:1175-1184`).
    ///
    /// The session the token names is deliberately not the one the request
    /// names. That is the whole of what the token does — its request arrives on
    /// an image the session was never opened on, so the gate could not have
    /// answered it — and a test that let the two coincide would pass with the
    /// gate still running.
    #[test]
    fn a_response_channel_replay_is_answered_by_the_session_its_token_names() {
        const TOKEN_SESSION: i64 = 5;

        let (mut adapter, mut control) = adapter_with(DenyAll);
        let session_id = an_open_session(&mut adapter, &mut control);
        control
            .tokens
            .insert((REPLAY_TOKEN, RECORDING_ID), TOKEN_SESSION);

        let payload = replay_request(session_id, 99, RECORDING_ID, REPLAY_TOKEN, RESPONSE_CHANNEL);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        let [Call::StartReplay { request }] = &control.calls[1..] else {
            panic!("not one replay: {:?}", control.calls);
        };

        assert_eq!(TOKEN_SESSION, request.session_id, "the token's session");
        assert_eq!(
            Some(IMAGE.correlation_id()),
            request.response_correlation_id,
            "and the image the request came in on"
        );
        assert!(
            control.warnings.is_empty(),
            "a token is not a gate that complains: {:?}",
            control.warnings
        );
    }

    /// A token the archive never issued, or one for another recording, or one
    /// that has expired, is **dropped in silence** — the reference throws
    /// (`ControlSessionAdapter.java:1180`), which reaches the error handler and
    /// sends the client nothing.
    #[test]
    fn a_response_channel_replay_with_a_token_that_names_nothing_is_dropped() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);
        // A token issued for a **different** recording, which is the second of
        // the three things `getReplaySession` checks.
        control
            .tokens
            .insert((REPLAY_TOKEN, RECORDING_ID + 1), session_id);

        let payload = replay_request(session_id, 99, RECORDING_ID, REPLAY_TOKEN, RESPONSE_CHANNEL);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(1, control.calls.len(), "only the connect");
        assert!(
            control.warnings[0].starts_with("Unknown session or token timeout for replayToken="),
            "{:?}",
            control.warnings
        );
    }

    /// A replay that carries **no** token goes through the gate even on a
    /// response channel, which is the ordinary path S4 already had.
    ///
    /// It reaches no session because `DenyAll` refuses it — the point is
    /// *which* gate answered, and the error says so.
    #[test]
    fn a_response_channel_replay_without_a_token_is_gated_as_usual() {
        let (mut adapter, mut control) = adapter_with(DenyAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = replay_request(session_id, 99, RECORDING_ID, NULL_VALUE, RESPONSE_CHANNEL);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::ErrorResponse {
                session_id,
                correlation_id: 99,
                relevant_id: i64::from(UNAUTHORISED_ACTION),
                message: UNAUTHORISED_ACTION_MSG.to_owned(),
            }],
            control.calls[1..],
            "the authorisation service was asked"
        );
    }

    /// A token on a channel that is **not** a response channel earns nothing:
    /// the reference's condition is `hasControlModeResponse() && NULL_VALUE !=
    /// replayToken` (`ControlSessionAdapter.java:1175`), and the first half is
    /// not decoration.
    ///
    /// The request names a session that does not exist, so the two paths are
    /// told apart by their answers: the gate warns and drops it, the token path
    /// would have answered the token's session.
    #[test]
    fn a_token_does_not_let_a_plain_channel_through() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);
        control
            .tokens
            .insert((REPLAY_TOKEN, RECORDING_ID), session_id);

        let payload = replay_request(
            4_242,
            99,
            RECORDING_ID,
            REPLAY_TOKEN,
            "aeron:udp?endpoint=localhost:6666",
        );
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(1, control.calls.len(), "only the connect");
        assert!(
            control.warnings[0].starts_with("control request for unknown session:"),
            "{:?}",
            control.warnings
        );
    }

    /// A request written before the token field existed has **no** token, even
    /// though the bytes here carry one.
    ///
    /// This is the one place the generated decoder and the reference disagree
    /// on an absent field — `i64::MIN` against `Aeron.NULL_VALUE`
    /// (`replay_request_codec.rs:336-342`, `ControlSessionAdapter.java:226-227`)
    /// — and the disagreement would be visible: the replay's token path is
    /// entered by comparing the field against `NULL_VALUE`, so `i64::MIN` would
    /// walk a version-9 client into a branch its request has no words for.
    #[test]
    fn a_replay_from_before_the_token_version_has_no_token() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);
        control.tokens.insert((REPLAY_TOKEN, RECORDING_ID), 5);

        let mut payload =
            replay_request(session_id, 99, RECORDING_ID, REPLAY_TOKEN, RESPONSE_CHANNEL);
        // `version` is the header's fourth field (`message_header_codec.rs:185`).
        payload[6..8].copy_from_slice(&(REPLAY_TOKEN_VERSION - 1).to_le_bytes());

        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        let [Call::StartReplay { request }] = &control.calls[1..] else {
            panic!("not one replay: {:?}", control.calls);
        };

        assert_eq!(session_id, request.session_id, "the gate's session");
        assert_eq!(
            None, request.response_correlation_id,
            "and no response-correlation-id, because there was no token"
        );
    }

    /// The two requests that move a recording's start reach the conductor with
    /// what they carried — a detach with its new position, and the delete that
    /// takes what the detach gave away with no position at all
    /// (`ControlSessionAdapter.java:1408-1446`).
    #[test]
    fn the_two_segment_requests_reach_the_session_with_their_own_fields() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = detach_segments(session_id, 99, RECORDING_ID, 262_144);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        let payload = delete_detached_segments(session_id, 100, RECORDING_ID);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![
                Call::DetachSegments {
                    session_id,
                    correlation_id: 99,
                    recording_id: RECORDING_ID,
                    new_start_position: 262_144,
                },
                Call::DeleteDetachedSegments {
                    session_id,
                    correlation_id: 100,
                    recording_id: RECORDING_ID,
                },
            ],
            control.calls[1..]
        );
    }

    /// An update-channel carries its channel as var data, which is the one
    /// thing about it that is not a field.
    #[test]
    fn an_update_channel_reaches_the_session_with_its_channel() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = update_channel(session_id, 99, RECORDING_ID, "aeron:ipc?alias=test42");
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::UpdateChannel {
                session_id,
                correlation_id: 99,
                recording_id: RECORDING_ID,
                channel: "aeron:ipc?alias=test42".to_owned(),
            }],
            control.calls[1..]
        );
    }

    /// And an attach, whose whole request is the recording it is about.
    #[test]
    fn an_attach_reaches_the_session() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = attach_segments(session_id, 99, RECORDING_ID);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::AttachSegments {
                session_id,
                correlation_id: 99,
                recording_id: RECORDING_ID,
            }],
            control.calls[1..]
        );
    }

    /// A truncate reaches the conductor with the position it stops at.
    #[test]
    fn a_truncate_reaches_the_session_with_its_position() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = truncate_recording(session_id, 99, RECORDING_ID, 131_072);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::TruncateRecording {
                session_id,
                correlation_id: 99,
                recording_id: RECORDING_ID,
                position: 131_072,
            }],
            control.calls[1..]
        );
    }

    /// The two purge requests reach the conductor too, one with a position and
    /// one without.
    #[test]
    fn the_two_purge_requests_reach_the_session() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = purge_recording(session_id, 99, RECORDING_ID);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        let payload = purge_segments(session_id, 100, RECORDING_ID, 131_072);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![
                Call::PurgeRecording {
                    session_id,
                    correlation_id: 99,
                    recording_id: RECORDING_ID,
                },
                Call::PurgeSegments {
                    session_id,
                    correlation_id: 100,
                    recording_id: RECORDING_ID,
                    new_start_position: 131_072,
                },
            ],
            control.calls[1..]
        );
    }

    /// And both are gated like anything else.
    #[test]
    fn the_segment_requests_are_gated() {
        let (mut adapter, mut control) = adapter_with(DenyAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = detach_segments(session_id, 99, RECORDING_ID, 0);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::ErrorResponse {
                session_id,
                correlation_id: 99,
                relevant_id: i64::from(UNAUTHORISED_ACTION),
                message: UNAUTHORISED_ACTION_MSG.to_owned(),
            }],
            control.calls[1..]
        );
    }

    /// A keep-alive reaches the session it names, and activating is the whole
    /// of what a session does with one (`ControlSession.java:317-320`).
    #[test]
    fn a_keep_alive_reaches_the_session_it_names() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = keep_alive(session_id, 99);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::KeepAlive { session_id }],
            control.calls[1..],
            "the connect is the first call"
        );
    }

    /// An archive id request reaches the session, which asks the conductor for
    /// the id (`ControlSession.java:541-548`).
    #[test]
    fn an_archive_id_request_reaches_the_session() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = archive_id_request(session_id, 7);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::ArchiveId {
                session_id,
                correlation_id: 7,
            }],
            control.calls[1..]
        );
    }

    /// A close names a session and ends it, with the reference's reason
    /// (`ControlSessionAdapter.java:144-160`).
    #[test]
    fn a_close_ends_the_session_it_names() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = close_session(session_id);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 3_000)
            .expect("read");

        assert_eq!(
            vec![Call::Abort {
                session_id,
                reason: SESSION_CLOSED_MSG.to_owned(),
            }],
            control.calls[1..]
        );
    }

    /// A close that names an image the session is not on changes nothing, and
    /// nothing is said to the client about it
    /// (`ControlSessionAdapter.java:155-158`).
    #[test]
    fn a_close_from_another_image_is_ignored() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = close_session(session_id);
        adapter
            .on_message(&mut control, OTHER_IMAGE, message(&payload), 3_000)
            .expect("read");

        assert_eq!(1, control.calls.len(), "only the connect");
        assert_eq!(Some(IMAGE), adapter.session_image(session_id));
        assert!(control.warnings.is_empty(), "and no word about it");
    }

    /// The first gate: a request that arrives on an image other than the one
    /// its session was created on is dropped, and the client is not told
    /// (`ControlSessionAdapter.java:1196-1203`).
    #[test]
    fn a_request_on_another_image_is_dropped_in_silence() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = keep_alive(session_id, 99);
        adapter
            .on_message(&mut control, OTHER_IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(1, control.calls.len(), "only the connect");
        assert_warned_unauthorised(&control);
    }

    /// A request that names no session at all warns and is dropped — the
    /// reference's third outcome, and the one a client cannot tell apart from a
    /// request that never arrived (`ControlSessionAdapter.java:1219-1224`).
    #[test]
    fn a_request_for_an_unknown_session_is_dropped() {
        let (mut adapter, mut control) = adapter_with(AllowAll);

        let payload = keep_alive(4_242, 99);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert!(control.calls.is_empty());
        assert_eq!(1, control.warnings.len());
        assert!(
            control.warnings[0].starts_with("control request for unknown session:"),
            "{:?}",
            control.warnings
        );
    }

    /// The second gate: a refusal **is** answered, with
    /// `UNAUTHORISED_ACTION` as the relevant id and the reference's message
    /// (`ControlSessionAdapter.java:1205-1216`).
    #[test]
    fn a_denied_request_is_answered_with_the_error_the_reference_sends() {
        let (mut adapter, mut control) = adapter_with(DenyAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = keep_alive(session_id, 99);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::ErrorResponse {
                session_id,
                correlation_id: 99,
                relevant_id: i64::from(UNAUTHORISED_ACTION),
                message: UNAUTHORISED_ACTION_MSG.to_owned(),
            }],
            control.calls[1..],
            "refused before it reached the session, and the refusal was sent"
        );
        assert_warned_unauthorised(&control);
    }

    /// The authorisation service is asked about the **schema** and the
    /// **template** — which is what its two ids are for — and about what the
    /// session's authenticator vouched for
    /// (`ControlSessionAdapter.java:1206-1207`).
    #[test]
    fn the_authorisation_service_is_asked_about_the_template_and_the_principal() {
        let (mut adapter, mut control) = adapter_with(Recording::default());
        control.principal = Some(b"a principal".to_vec());
        let session_id = an_open_session(&mut adapter, &mut control);
        adapter.authorisation.calls.borrow_mut().clear();

        let payload = keep_alive(session_id, 99);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![(
                i32::from(SBE_SCHEMA_ID),
                i32::from(keep_alive_request_codec::SBE_TEMPLATE_ID),
                Some(b"a principal".to_vec()),
            )],
            *adapter.authorisation.calls.borrow()
        );
    }

    /// The challenge answer is **not** gated on anything but the session being
    /// known: a session being challenged has no principal to authorise against,
    /// and the reference looks it up in its map directly
    /// (`ControlSessionAdapter.java:804-833`).
    #[test]
    fn a_challenge_answer_is_not_gated() {
        let (mut adapter, mut control) = adapter_with(DenyAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let payload = challenge_answer(session_id, 5, b"an answer");
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");

        assert_eq!(
            vec![Call::ChallengeResponse {
                session_id,
                correlation_id: 5,
                encoded_credentials: b"an answer".to_vec(),
            }],
            control.calls[1..]
        );
    }

    /// A message in another schema stops the turn. The reference throws
    /// (`ControlSessionAdapter.java:136`), which the archive's main exits
    /// on; this is the same statement as a value, one frame earlier.
    #[test]
    fn a_message_in_another_schema_is_refused() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let mut connect = a_connect(1);
        // `schemaId` is the third of the header's four fields.
        connect[4..6].copy_from_slice(&999u16.to_le_bytes());

        let error = adapter
            .on_message(&mut control, IMAGE, message(&connect), 0)
            .expect_err("refused");

        assert_eq!(
            ControlError::UnexpectedSchemaId {
                expected: SBE_SCHEMA_ID,
                actual: 999,
            },
            error
        );
        assert!(control.calls.is_empty());
    }

    /// A message too short to carry a header cannot name a schema, and reading
    /// one would index past the end of the buffer rather than throw.
    #[test]
    fn a_message_shorter_than_a_header_is_refused() {
        let (mut adapter, mut control) = adapter_with(AllowAll);

        let error = adapter
            .on_message(&mut control, IMAGE, message(&[1, 2, 3]), 0)
            .expect_err("refused");

        assert_eq!(ControlError::ShortMessage { length: 3 }, error);
    }

    /// A request this slice does not answer is named rather than dropped in
    /// silence: the reference's switch has no default arm
    /// (`ControlSessionAdapter.java:142-1129`), and a client waiting on a
    /// request nobody answers is a client timing out.
    #[test]
    fn a_template_this_slice_does_not_answer_is_named() {
        let (mut adapter, mut control) = adapter_with(AllowAll);

        let mut buffer = vec![0u8; 128];
        let length = {
            let encoder =
                StartRecordingRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(1)
                .correlation_id(2)
                .stream_id(3)
                .channel(b"aeron:ipc");
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);

        adapter
            .on_message(&mut control, IMAGE, message(&buffer), 0)
            .expect("read");

        assert!(control.calls.is_empty());
        assert_eq!(1, control.warnings.len());
        assert!(
            control.warnings[0].contains(&format!("templateId={START_RECORDING}")),
            "{:?}",
            control.warnings
        );
    }

    /// Forgetting a session answers with the image it was on, because the
    /// conductor rejects that image when the session was aborted
    /// (`ControlSessionAdapter.java:1150-1155`).
    #[test]
    fn forgetting_a_session_hands_back_its_image() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        assert_eq!(Some(IMAGE), adapter.remove_session(session_id));
        assert_eq!(None, adapter.session_image(session_id));
        assert_eq!(0, adapter.session_count());

        // And a request for it is now a request for an unknown session.
        let payload = keep_alive(session_id, 99);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 2_000)
            .expect("read");
        assert!(
            control.warnings[0].starts_with("control request for unknown session:"),
            "{:?}",
            control.warnings
        );
    }

    /// Two sessions on two images are told apart, which is the whole of what
    /// the first gate is for: the UDP control subscription and the local IPC
    /// one are two images on one stream
    /// (`ArchiveConductor.java:239-240`).
    #[test]
    fn two_images_are_two_sessions() {
        let (mut adapter, mut control) = adapter_with(AllowAll);

        let connect = a_connect(1);
        adapter
            .on_message(&mut control, IMAGE, message(&connect), 0)
            .expect("read");
        adapter
            .on_message(&mut control, OTHER_IMAGE, message(&connect), 0)
            .expect("read");

        assert_eq!(2, adapter.session_count());
        assert_eq!(Some(IMAGE), adapter.session_image(1));
        assert_eq!(Some(OTHER_IMAGE), adapter.session_image(2));

        let payload = keep_alive(1, 99);
        adapter
            .on_message(&mut control, IMAGE, message(&payload), 1_000)
            .expect("read");
        assert_eq!(vec![Call::KeepAlive { session_id: 1 }], control.calls[2..]);
    }

    fn archive_id_request(control_session_id: i64, correlation_id: i64) -> Vec<u8> {
        use deepmsg_codec::archive::archive_id_request_codec::ArchiveIdRequestEncoder;

        let mut buffer = vec![0u8; 64];
        let length = {
            let encoder = ArchiveIdRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id);
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);
        buffer
    }

    /// The four position questions, which are the same request shape four times
    /// over — session, correlation, recording.
    macro_rules! position_request {
        ($name:ident, $codec:ident, $encoder:ident) => {
            fn $name(control_session_id: i64, correlation_id: i64, recording_id: i64) -> Vec<u8> {
                use deepmsg_codec::archive::$codec::$encoder;

                let mut buffer = vec![0u8; 64];
                let length = {
                    let encoder = $encoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
                    let mut header = encoder.header(0);
                    let mut encoder = header.parent().unwrap();
                    encoder
                        .control_session_id(control_session_id)
                        .correlation_id(correlation_id)
                        .recording_id(recording_id);
                    BODY + encoder.encoded_length()
                };
                buffer.truncate(length);
                buffer
            }
        };
    }

    position_request!(
        start_position_request,
        start_position_request_codec,
        StartPositionRequestEncoder
    );
    position_request!(
        recording_position_request,
        recording_position_request_codec,
        RecordingPositionRequestEncoder
    );
    position_request!(
        stop_position_request,
        stop_position_request_codec,
        StopPositionRequestEncoder
    );
    position_request!(
        max_recorded_position_request,
        max_recorded_position_request_codec,
        MaxRecordedPositionRequestEncoder
    );

    /// A start, in the version that carries `autoStop` (63).
    fn start_recording_request_2(
        control_session_id: i64,
        correlation_id: i64,
        stream_id: i32,
        source_location: deepmsg_codec::archive::source_location::SourceLocation,
        auto_stop: bool,
        channel: &str,
    ) -> Vec<u8> {
        use deepmsg_codec::archive::boolean_type::BooleanType;
        use deepmsg_codec::archive::start_recording_request_2_codec::StartRecordingRequest2Encoder;

        let mut buffer = vec![0u8; 128];
        let length = {
            let encoder =
                StartRecordingRequest2Encoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .stream_id(stream_id)
                .source_location(source_location)
                .auto_stop(if auto_stop {
                    BooleanType::TRUE
                } else {
                    BooleanType::FALSE
                })
                .channel(channel.as_bytes());
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);
        buffer
    }

    /// The same request in the version that does not (4).
    fn start_recording_request_v1(
        control_session_id: i64,
        correlation_id: i64,
        stream_id: i32,
        source_location: deepmsg_codec::archive::source_location::SourceLocation,
        channel: &str,
    ) -> Vec<u8> {
        use deepmsg_codec::archive::start_recording_request_codec::StartRecordingRequestEncoder;

        let mut buffer = vec![0u8; 128];
        let length = {
            let encoder =
                StartRecordingRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .stream_id(stream_id)
                .source_location(source_location)
                .channel(channel.as_bytes());
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);
        buffer
    }

    fn list_recording_subscriptions_request(
        control_session_id: i64,
        correlation_id: i64,
        pseudo_index: i32,
        subscription_count: i32,
        apply_stream_id: bool,
        stream_id: i32,
        channel_fragment: &str,
    ) -> Vec<u8> {
        use deepmsg_codec::archive::boolean_type::BooleanType;
        use deepmsg_codec::archive::list_recording_subscriptions_request_codec::ListRecordingSubscriptionsRequestEncoder;

        let mut buffer = vec![0u8; 128];
        let length = {
            let encoder = ListRecordingSubscriptionsRequestEncoder::default()
                .wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .pseudo_index(pseudo_index)
                .subscription_count(subscription_count)
                .apply_stream_id(if apply_stream_id {
                    BooleanType::TRUE
                } else {
                    BooleanType::FALSE
                })
                .stream_id(stream_id)
                .channel(channel_fragment.as_bytes());
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);
        buffer
    }

    fn extend_recording_request(
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        stream_id: i32,
        source_location: SourceLocation,
        auto_stop: bool,
        channel: &str,
    ) -> Vec<u8> {
        use deepmsg_codec::archive::boolean_type::BooleanType;
        use deepmsg_codec::archive::extend_recording_request_2_codec::ExtendRecordingRequest2Encoder;

        let mut buffer = vec![0u8; 128];
        let length = {
            let encoder =
                ExtendRecordingRequest2Encoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id)
                .stream_id(stream_id)
                .source_location(source_location)
                .auto_stop(if auto_stop {
                    BooleanType::TRUE
                } else {
                    BooleanType::FALSE
                })
                .channel(channel.as_bytes());
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);
        buffer
    }

    fn stop_recording_request(
        control_session_id: i64,
        correlation_id: i64,
        stream_id: i32,
        channel: &str,
    ) -> Vec<u8> {
        use deepmsg_codec::archive::stop_recording_request_codec::StopRecordingRequestEncoder;

        let mut buffer = vec![0u8; 128];
        let length = {
            let encoder =
                StopRecordingRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .stream_id(stream_id)
                .channel(channel.as_bytes());
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);
        buffer
    }

    fn stop_recording_by_identity_request(
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    ) -> Vec<u8> {
        use deepmsg_codec::archive::stop_recording_by_identity_request_codec::StopRecordingByIdentityRequestEncoder;

        let mut buffer = vec![0u8; 64];
        let length = {
            let encoder = StopRecordingByIdentityRequestEncoder::default()
                .wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);
        buffer
    }

    fn stop_recording_subscription_request(
        control_session_id: i64,
        correlation_id: i64,
        subscription_id: i64,
    ) -> Vec<u8> {
        use deepmsg_codec::archive::stop_recording_subscription_request_codec::StopRecordingSubscriptionRequestEncoder;

        let mut buffer = vec![0u8; 64];
        let length = {
            let encoder = StopRecordingSubscriptionRequestEncoder::default()
                .wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .subscription_id(subscription_id);
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);
        buffer
    }

    /// Both start templates reach the session, and the one thing they differ by
    /// is the field the second one added: `autoStop` (`ArchiveConductor.java
    /// :530-536`), which the version without it answers as `false`.
    ///
    /// The channel arrives whole — it is the last thing in the message and the
    /// only variable-length field, so a fragment of it would be a fragment of a
    /// channel that a recording is registered under.
    #[test]
    fn the_two_start_requests_differ_by_auto_stop() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let channel = "aeron:udp?endpoint=localhost:3333";

        adapter
            .on_message(
                &mut control,
                IMAGE,
                message(&start_recording_request_2(
                    session_id,
                    7,
                    33,
                    SourceLocation::LOCAL,
                    true,
                    channel,
                )),
                2_000,
            )
            .expect("read");

        assert_eq!(
            Some(&Call::StartRecording(StartRecordingRequest {
                session_id,
                correlation_id: 7,
                stream_id: 33,
                source_location: SourceLocation::LOCAL,
                auto_stop: true,
                original_channel: channel.to_owned(),
            })),
            control.calls.last()
        );

        adapter
            .on_message(
                &mut control,
                IMAGE,
                message(&start_recording_request_v1(
                    session_id,
                    8,
                    33,
                    SourceLocation::REMOTE,
                    channel,
                )),
                2_001,
            )
            .expect("read");

        assert_eq!(
            Some(&Call::StartRecording(StartRecordingRequest {
                session_id,
                correlation_id: 8,
                stream_id: 33,
                source_location: SourceLocation::REMOTE,
                auto_stop: false,
                original_channel: channel.to_owned(),
            })),
            control.calls.last()
        );
    }

    /// The request that asks what this archive records reaches the session with
    /// every field its two gates and its walk need, the channel fragment whole
    /// (`ArchiveConductor.java:1266-1300`).
    #[test]
    fn a_listing_of_subscriptions_reaches_the_session_with_its_whole_question() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        adapter
            .on_message(
                &mut control,
                IMAGE,
                message(&list_recording_subscriptions_request(
                    session_id,
                    7,
                    0,
                    5,
                    true,
                    33,
                    "endpoint=localhost:3333",
                )),
                2_000,
            )
            .expect("read");

        assert_eq!(
            Some(&Call::ListRecordingSubscriptions {
                session_id,
                correlation_id: 7,
                pseudo_index: 0,
                subscription_count: 5,
                apply_stream_id: true,
                stream_id: 33,
                channel_fragment: "endpoint=localhost:3333".to_owned(),
            }),
            control.calls.last()
        );
    }

    /// An extend reaches the session with everything the six gates in front of
    /// it need — including the recording it is appending to, which is the one
    /// field a start does not have (`ArchiveConductor.java:1067-1076`).
    #[test]
    fn an_extend_reaches_the_session_with_the_recording_it_appends_to() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let channel = "aeron:udp?endpoint=localhost:3333";

        adapter
            .on_message(
                &mut control,
                IMAGE,
                message(&extend_recording_request(
                    session_id,
                    7,
                    4_242,
                    33,
                    SourceLocation::LOCAL,
                    false,
                    channel,
                )),
                2_000,
            )
            .expect("read");

        assert_eq!(
            Some(&Call::ExtendRecording(ExtendRecordingRequest {
                session_id,
                correlation_id: 7,
                recording_id: 4_242,
                stream_id: 33,
                source_location: SourceLocation::LOCAL,
                auto_stop: false,
                original_channel: channel.to_owned(),
            })),
            control.calls.last()
        );
    }

    /// The two stops that name something other than a subscription id: the
    /// channel and stream a recording was started with
    /// (`ArchiveConductor.java:585-610`), and the recording itself (`:1301-1327`).
    ///
    /// The channel travels whole, because it is what `makeKey` is built from —
    /// a fragment of it would name a different recording.
    #[test]
    fn the_two_other_stops_name_a_channel_and_a_recording() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let channel = "aeron:udp?endpoint=localhost:3333";

        adapter
            .on_message(
                &mut control,
                IMAGE,
                message(&stop_recording_request(session_id, 7, 33, channel)),
                2_000,
            )
            .expect("read");

        assert_eq!(
            Some(&Call::StopRecording {
                session_id,
                correlation_id: 7,
                stream_id: 33,
                original_channel: channel.to_owned(),
            }),
            control.calls.last()
        );

        adapter
            .on_message(
                &mut control,
                IMAGE,
                message(&stop_recording_by_identity_request(session_id, 8, 4_242)),
                2_001,
            )
            .expect("read");

        assert_eq!(
            Some(&Call::StopRecordingByIdentity {
                session_id,
                correlation_id: 8,
                recording_id: 4_242,
            }),
            control.calls.last()
        );
    }

    /// A stop names the subscription the start answered with, and nothing else
    /// (`ArchiveConductor.java:612-624`).
    #[test]
    fn a_stop_names_the_subscription_the_start_answered_with() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        adapter
            .on_message(
                &mut control,
                IMAGE,
                message(&stop_recording_subscription_request(session_id, 7, 11)),
                2_000,
            )
            .expect("read");

        assert_eq!(
            Some(&Call::StopRecordingSubscription {
                session_id,
                correlation_id: 7,
                subscription_id: 11,
            }),
            control.calls.last()
        );
    }

    /// A match, which is the one of these requests that carries a channel — as
    /// a length and then the bytes, the shape every variable-length field in
    /// this protocol has (`FindLastMatchingRecordingRequestDecoder`).
    fn find_last_matching_request(
        control_session_id: i64,
        correlation_id: i64,
        min_recording_id: i64,
        session_id: i32,
        stream_id: i32,
        channel_fragment: &str,
    ) -> Vec<u8> {
        use deepmsg_codec::archive::find_last_matching_recording_request_codec::FindLastMatchingRecordingRequestEncoder;

        let mut buffer = vec![0u8; 96];
        let length = {
            let encoder = FindLastMatchingRecordingRequestEncoder::default()
                .wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .min_recording_id(min_recording_id)
                .session_id(session_id)
                .stream_id(stream_id)
                .channel(channel_fragment.as_bytes());
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);
        buffer
    }

    fn list_recording_request(
        control_session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    ) -> Vec<u8> {
        use deepmsg_codec::archive::list_recording_request_codec::ListRecordingRequestEncoder;

        let mut buffer = vec![0u8; 64];
        let length = {
            let encoder =
                ListRecordingRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .recording_id(recording_id);
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);
        buffer
    }

    fn list_recordings_request(
        control_session_id: i64,
        correlation_id: i64,
        from_recording_id: i64,
        record_count: i32,
    ) -> Vec<u8> {
        use deepmsg_codec::archive::list_recordings_request_codec::ListRecordingsRequestEncoder;

        let mut buffer = vec![0u8; 64];
        let length = {
            let encoder =
                ListRecordingsRequestEncoder::default().wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .from_recording_id(from_recording_id)
                .record_count(record_count);
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);
        buffer
    }

    fn list_recordings_for_uri_request(
        control_session_id: i64,
        correlation_id: i64,
        from_recording_id: i64,
        record_count: i32,
        stream_id: i32,
        channel_fragment: &str,
    ) -> Vec<u8> {
        use deepmsg_codec::archive::list_recordings_for_uri_request_codec::ListRecordingsForUriRequestEncoder;

        let mut buffer = vec![0u8; 128];
        let length = {
            let encoder = ListRecordingsForUriRequestEncoder::default()
                .wrap(WriteBuf::new(&mut buffer), BODY);
            let mut header = encoder.header(0);
            let mut encoder = header.parent().unwrap();
            encoder
                .control_session_id(control_session_id)
                .correlation_id(correlation_id)
                .from_recording_id(from_recording_id)
                .record_count(record_count)
                .stream_id(stream_id)
                .channel(channel_fragment.as_bytes());
            BODY + encoder.encoded_length()
        };
        buffer.truncate(length);
        buffer
    }

    /// A match reaches the session as a question with its channel fragment
    /// whole — the fragment is bytes on the wire and is compared as the client
    /// wrote it (`ArchiveConductor.java:742-761` over `Catalog.findLast`).
    #[test]
    fn a_match_reaches_the_session_with_its_fragment_whole() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let fragment = "endpoint=localhost:3333";
        adapter
            .on_message(
                &mut control,
                IMAGE,
                message(&find_last_matching_request(
                    session_id, 7, 0, 1001, 33, fragment,
                )),
                2_000,
            )
            .expect("read");

        assert_eq!(
            Some(&Call::Query {
                session_id,
                correlation_id: 7,
                query: Query::FindLastMatching {
                    min_recording_id: 0,
                    session_id: 1001,
                    stream_id: 33,
                    channel_fragment: fragment.as_bytes().to_vec(),
                },
            }),
            control.calls.last()
        );
    }

    /// A listing request reaches the session as an intent of its own — not as a
    /// question, because the answer is a message the conductor offers over
    /// several turns (`ArchiveConductor.java:688-706`).
    #[test]
    fn a_listing_request_reaches_the_session_as_its_own_intent() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        adapter
            .on_message(
                &mut control,
                IMAGE,
                message(&list_recording_request(session_id, 7, 100)),
                2_000,
            )
            .expect("read");

        assert_eq!(
            Some(&Call::ListRecording {
                session_id,
                correlation_id: 7,
                recording_id: 100,
            }),
            control.calls.last()
        );
    }

    /// The two requests for a page of recordings reach the session as their own
    /// calls, and the second one carries its filter — a stream and a channel
    /// fragment that is **bytes**, because that is what the far side compares
    /// (`ArchiveConductor.java:638-657`, `:659-687`;
    /// `ControlSessionAdapter.java:275-294`, `:296-318`).
    ///
    /// One test for both because the two arms decode the same first four fields
    /// and differ in what they do with the rest: what can go wrong is the
    /// fragment arriving as text, or the two templates decoding as each other's
    /// call.
    #[test]
    fn a_page_of_recordings_reaches_the_session_with_its_filter_whole() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        adapter
            .on_message(
                &mut control,
                IMAGE,
                message(&list_recordings_request(session_id, 7, i64::MIN, 10)),
                2_000,
            )
            .expect("read");

        assert_eq!(
            Some(&Call::ListRecordings {
                session_id,
                correlation_id: 7,
                from_recording_id: i64::MIN,
                count: 10,
            }),
            control.calls.last()
        );

        let fragment = "endpoint=localhost:3333";
        adapter
            .on_message(
                &mut control,
                IMAGE,
                message(&list_recordings_for_uri_request(
                    session_id, 8, 5, 2, 33, fragment,
                )),
                2_000,
            )
            .expect("read");

        assert_eq!(
            Some(&Call::ListRecordingsForUri {
                session_id,
                correlation_id: 8,
                from_recording_id: 5,
                count: 2,
                stream_id: 33,
                channel_fragment: fragment.as_bytes().to_vec(),
            }),
            control.calls.last()
        );
    }

    /// The four questions whose answer is a position, each reaching the session
    /// as the same kind of [`Query`] and differing only in which one
    /// (`ArchiveConductor.java:1159-1195`).
    ///
    /// One test rather than four because the arm is one arm: what can go wrong
    /// is a template decoded as its neighbour's question, which only four
    /// payloads side by side can show.
    #[test]
    fn the_four_position_questions_reach_the_session_as_queries() {
        let (mut adapter, mut control) = adapter_with(AllowAll);
        let session_id = an_open_session(&mut adapter, &mut control);

        let questions = [
            (
                start_position_request(session_id, 7, 100),
                Query::StartPosition { recording_id: 100 },
            ),
            (
                recording_position_request(session_id, 7, 100),
                Query::RecordingPosition { recording_id: 100 },
            ),
            (
                stop_position_request(session_id, 7, 100),
                Query::StopPosition { recording_id: 100 },
            ),
            (
                max_recorded_position_request(session_id, 7, 100),
                Query::MaxRecordedPosition { recording_id: 100 },
            ),
        ];

        for (payload, query) in questions {
            adapter
                .on_message(&mut control, IMAGE, message(&payload), 2_000)
                .expect("read");

            assert_eq!(
                Some(&Call::Query {
                    session_id,
                    correlation_id: 7,
                    query: query.clone(),
                }),
                control.calls.last(),
                "{query:?}"
            );
        }
    }

    /// Both warnings in the gate are the reference's one line
    /// (`ControlSessionAdapter.java:1200-1202`, `:1209-1213`).
    fn assert_warned_unauthorised(control: &Recorder) {
        assert_eq!(1, control.warnings.len());
        assert!(
            control.warnings[0].starts_with("unauthorised archive action="),
            "{:?}",
            control.warnings
        );
    }
}
