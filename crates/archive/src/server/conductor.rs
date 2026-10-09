//! The archive's conductor.
//!
//! The reference's `ArchiveConductor` is a `SessionWorker` — an agent whose
//! `doWork` runs the adapter's poll, then the sessions, then the pieces this
//! slice does not have (`ArchiveConductor.java:364-396`, `:125-127`). What is
//! here is the first piece of it: the channel an archive answers a client on,
//! which the conductor **derives** rather than chooses.
//!
//! # The channel is rebuilt, not taken
//!
//! A client asks to be answered on a channel of its own choosing, and the
//! archive does not use it as given. It is **stripped down to a fixed list of
//! parameters and written out again** (`strippedChannelBuilder`,
//! `ArchiveConductor.java:1880-1905`) with three parameters overridden from
//! the archive's own control settings, and — when the client asked for a
//! response channel — one more added: the correlation id of the image the
//! request arrived on (`:458-481`).
//!
//! The list is not the same as "the parameters that were there". Read the
//! probe below: `term-length`, `mtu` and `sparse` in the client's channel are
//! **dropped**, because the archive sets those itself; `nak-delay` and
//! everything unnamed are dropped too. What survives is what a channel is
//! *identified* by, which is exactly the set a second archive can be told to
//! reproduce.
//!
//! # What was checked against, rather than reasoned about
//!
//! The strip list is a list of twenty-three calls, and the quickest way to be
//! wrong about it is to reason about which ones matter. It was read off the
//! reference instead: `aeron-archive-1.53.2.jar` is built in the sibling
//! checkout, `strippedChannelBuilder` is reachable by reflection, and the
//! outputs are in the test at the bottom of this file.
//!
//! Every parameter is read into the type its field has and written back out,
//! which is what the reference does — `ttl=007` comes back as `ttl=7` and
//! `so-sndbuf=1m` comes back as `1m`, and both are in the test below. What
//! remains different is only the failure: a value that will not read at all,
//! `ttl=seven`, makes the reference's setter throw and takes the archive with
//! it, where this leaves the parameter out. Dying is not obviously the better
//! answer, but it is the reference's, and a channel is the thing that differs
//! when the two choose differently.
//!
//! # The conductor, and the one seam that shapes it
//!
//! Everything below is the rest of `ArchiveConductor`: the store of live
//! control sessions, the `ControlPlane` the adapter dispatches into, and the
//! per-turn loop that drives them. `ArchiveConductor` itself is thin — the
//! adapter, the authenticator, the mark file and the loop — and
//! [`Sessions`] holds the sessions and answers the adapter's callbacks.
//!
//! The two are split because of **one borrow**, and the whole shape follows
//! from it. The adapter dispatches from inside `Client::poll_image`
//! (`control_adapter.rs:412-425`), so every callback runs while the client is
//! already lent out; but a control session answers by offering on a
//! publication, which needs the client. The reference has no such seam — its
//! adapter and its `SessionWorker` both run inside one `doWork` (`:389`,
//! `:395`) — so this build does not invent a new moment, it moves the *work*
//! to the moment the client is back, in the same turn: a callback records an
//! intent ([`Deferred`]) and [`Sessions::drive`] replays it before the
//! sessions are driven, which is the order the reference's two calls already
//! have.
//!
//! More things the callbacks cannot do for the same reason, and which
//! therefore also happen in `drive`:
//!
//! - **Making a session** needs the image's `sourceIdentity` for the session
//!   counter's label and an async add for the session's own counter, both of
//!   which want the client (`ArchiveConductor.java:492-498`), so
//!   [`Sessions::new_session`] records the request and returns the id.
//! - **The aggregate session counter** (102) is written through the counters
//!   region (`:520`, `ControlSessionAdapter.java:1158-1161`), which the
//!   conductor holds and the callbacks do not.
//! - **`logWarning`** (`:443-446`) goes to the error handler, which the
//!   conductor owns.
//! - **The questions about a recording** (`:1159-1195`, `:742-761`) are
//!   answered out of the catalog, which is the *conductor's* — so what a
//!   callback records is the question ([`Deferred::Query`]) and `drive` is
//!   what reads the catalog for the answer.
//! - **Serving a recording's descriptor** (`:688-706`) needs the same catalog
//!   for the two refusals in front of it, so a callback records the intent
//!   ([`Deferred::ListRecording`]) and the listing is started in `drive` —
//!   which then offers the message once a turn until the client takes it
//!   ([`Sessions::drive_listings`]).
//!
//! `Sessions` consequently does not hold a `CncFile`: the region arrives each
//! turn as an argument. That also makes it testable without one.
//!
//! # What this slice does not have
//!
//! The reference's `doWork` also runs the replayer and the replay-token sweep
//! (`:389-395`), which are the replay slice's. What is here is the control
//! plane and the recording half of the data plane: connect, authenticate,
//! answer, count, remember what it has been asked to record — and, when an
//! image arrives for one of those, write the catalog row, publish a position
//! and hand a [`crate::server::recording_session::RecordingSession`] to the
//! recorder.
//!
//! The recorder itself is **not** here: [`ArchiveConductor`] holds it, and the
//! one parameter that costs is on [`Sessions::drive`], which is lent it for the
//! length of a turn. See that method for why two of the asks reach into it.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use deepmsg_client::client::{
    AsyncAdd, AsyncAddPoll, AsyncRemove, Client, DEFAULT_TIMEOUT, RemovePoll,
};
use deepmsg_client::image::Image;
use deepmsg_client::image_event::ImageEvent;
use deepmsg_cnc::counters::CountersReader;
use deepmsg_cnc::file::CncFile;
use deepmsg_codec::archive::recording_signal::RecordingSignal;
use deepmsg_codec::archive::source_location::SourceLocation;
use deepmsg_core::buffer::ReadWrite;
use deepmsg_core::logbuffer::descriptor::FRAME_ALIGNMENT;
use deepmsg_core::logbuffer::position::bits_to_shift;
use deepmsg_core::pal::usable_space;
use deepmsg_core::uri::{ChannelUri, ChannelUriStringBuilder, UriError, parse_size};
use deepmsg_core::version::{format_version, semantic_version_major};

use crate::catalog::{Catalog, CatalogError, Recording, channel_contains};
use crate::mark::NULL_VALUE;
use crate::mark_file::{ArchiveMarkFile, MARK_FILE_UPDATE_INTERVAL_MS};
use crate::segment::SegmentSummary;
use crate::server::auth::{
    AuthError, Authenticator, AuthorisationService, authenticator, authorisation_service,
};
use crate::server::config::ArchiveConfig;
use crate::server::control_adapter::{
    ConnectRequest, ControlAdapter, ControlError, ControlPlane, ExtendRecordingRequest, ImageId,
    StartRecordingRequest, StartReplayRequest,
};
use crate::server::control_session::{
    ControlSession, REQUEST_IMAGE_NOT_AVAILABLE_MSG, RESPONSE_NOT_CONNECTED_MSG, SESSION_CLOSED_MSG,
};
use crate::server::counters::{
    ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID, ArchiveIdCounter, ControlSessionCounter,
    ControlSessionsCounter, ErrorCounter, RECORDING_SESSIONS_NAME, claim_archive_id_counter,
    claim_control_sessions_counter, request_archive_id_counter, request_control_sessions_counter,
};
use crate::server::create_replay_publication::{
    CreateReplayPublicationSession, LimitCounter, Progress as CreationProgress, Replay,
};
use crate::server::recorder::Recorder;
use crate::server::recording_pos::RecordingPos;
use crate::server::recording_session::RecordingSession;
use crate::server::replay_session::{
    Progress as ReplayProgress, Published, ReplayPublications, ReplaySession,
};
use crate::server::replayer::Replayer;
use crate::server::response_proxy::{ControlResponseProxy, PROTOCOL_SEMANTIC_VERSION};

/// `AeronArchive.Configuration.CONTROL_MODE_RESPONSE` — the `control-mode` a
/// client writes when it wants a channel of its own to be answered on
/// (`ArchiveConductor.java:469`).
pub const CONTROL_MODE_RESPONSE: &str = "response";

/// What the archive's own control settings contribute to a response channel
/// (`ArchiveConductor.java:460-474`, falling back to `ctx.control*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseChannelDefaults {
    /// `control.term.buffer.length`.
    pub term_buffer_length: i32,
    /// `control.term.buffer.sparse`.
    pub term_buffer_sparse: bool,
    /// `control.mtu.length`, or `None` for the driver's own — the reference's
    /// `controlMtuLength` is an `int` and this build's is an `Option`, so an
    /// archive that has not set one leaves it out rather than writing a zero.
    pub mtu_length: Option<i32>,
}

/// `strippedChannelBuilder` (`ArchiveConductor.java:1880-1905`): a builder
/// carrying the parameters an archive keeps from a client's channel.
///
/// Every `.xxx(channelUri)` call in the reference is one line here, in the same
/// order, and the ones it makes are the whole of the list. A parameter the
/// reference does not copy is one this does not copy — the omission is the
/// behaviour, not an oversight.
pub fn stripped_channel_builder(uri: &ChannelUri) -> ChannelUriStringBuilder {
    let mut builder = ChannelUriStringBuilder::default();

    // `media(channelUri)` is the transport, which is not a parameter.
    builder.media(uri.media());

    copy_text(&mut builder, uri, "tags");
    copy_text(&mut builder, uri, "endpoint");
    copy_text(&mut builder, uri, "interface");
    copy_text(&mut builder, uri, "control");
    copy_text(&mut builder, uri, "control-mode");
    copy_text(&mut builder, uri, "gtag");
    copy_text(&mut builder, uri, "tether");
    copy_text(&mut builder, uri, "group");
    copy_text(&mut builder, uri, "rejoin");
    copy_text(&mut builder, uri, "fc");
    copy_text(&mut builder, uri, "cc");
    copy_text(&mut builder, uri, "so-rcvbuf");
    copy_text(&mut builder, uri, "so-sndbuf");
    copy_text(&mut builder, uri, "rcv-wnd");
    copy_text(&mut builder, uri, "channel-snd-ts-offset");
    copy_text(&mut builder, uri, "channel-rcv-ts-offset");
    copy_text(&mut builder, uri, "media-rcv-ts-offset");
    copy_text(&mut builder, uri, "session-id");
    copy_text(&mut builder, uri, "alias");
    copy_text(&mut builder, uri, "response-correlation-id");
    copy_text(&mut builder, uri, "response-endpoint");
    copy_text(&mut builder, uri, "ttl");

    builder
}

/// The channel a session is answered on (`ArchiveConductor.java:458-481`).
///
/// The client's channel, stripped, with its term length, sparse flag and MTU
/// put back and, when the client asked for a response channel, the correlation
/// id of the image the request arrived on. That last one is what lets a client
/// tell an answer meant for it from one meant for another subscription on the
/// same endpoint.
///
/// # The three the strip list drops are the client's, not the archive's
///
/// `strippedChannelBuilder` drops `term-length`, `mtu` and `sparse`, and
/// `newControlSession` writes all three back — but it writes **the client's
/// values**, reading them out of the channel before they are dropped, and
/// reaches for `ctx.control*` only for the ones the client did not name
/// (`:460-474`):
///
/// ```text
/// final String termLengthStr = channelUri.get(TERM_LENGTH_PARAM_NAME);
/// final int termLength = null == termLengthStr ?
///     ctx.controlTermBufferLength() : (int)SystemUtil.parseSize(..., termLengthStr);
/// ```
///
/// So a client that asks for a 128 KiB term and a 2048 MTU is answered on a
/// channel that has them, and one that asks for neither is answered on the
/// archive's own settings. Reading the strip list as "the archive sets these"
/// is the easy mistake here, and it is the wrong way round: what the archive
/// sets is only the fallback.
pub fn response_channel(
    requested: &str,
    image_correlation_id: i64,
    defaults: &ResponseChannelDefaults,
) -> Result<String, UriError> {
    let uri = ChannelUri::parse(requested)?;
    let mut builder = stripped_channel_builder(&uri);

    builder
        .term_length(term_length(&uri, defaults))
        .sparse(sparse(&uri, defaults));

    if let Some(mtu) = mtu_length(&uri, defaults) {
        builder.mtu(mtu);
    }

    if uri.get("control-mode") == Some(CONTROL_MODE_RESPONSE) {
        builder.response_correlation_id(image_correlation_id.to_string());
    }

    Ok(builder.build())
}

/// The client's `term-length`, or the archive's when it named none
/// (`ArchiveConductor.java:463-465`).
///
/// A value that is there and will not read falls back too, where the reference
/// throws; that is the same divergence [`copy_text`] makes for the parameters
/// it keeps, and the module note says why.
fn term_length(uri: &ChannelUri, defaults: &ResponseChannelDefaults) -> i32 {
    uri.get("term-length")
        .and_then(parse_size)
        .unwrap_or(defaults.term_buffer_length)
}

/// The client's `sparse`, or the archive's when it named none (`:466-468`).
///
/// `Boolean.parseBoolean` is false for anything that is not "true", which is
/// what the fallback here matches.
fn sparse(uri: &ChannelUri, defaults: &ResponseChannelDefaults) -> bool {
    uri.get("sparse")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(defaults.term_buffer_sparse)
}

/// The client's `mtu`, or the archive's when it named none (`:460-462`).
///
/// The reference's `controlMtuLength` is an `int` with a default of its own;
/// this build's is an `Option`, so an archive that has not set one and a
/// client that did not ask for one leave the parameter out rather than writing
/// the zero the reference would have resolved away.
fn mtu_length(uri: &ChannelUri, defaults: &ResponseChannelDefaults) -> Option<i32> {
    match uri.get("mtu") {
        Some(value) => parse_size(value).or(defaults.mtu_length),
        None => defaults.mtu_length,
    }
}

/// `ArchiveConductor.makeKey` (`ArchiveConductor.java:1909-1948`): what an
/// archive remembers a recording request by.
///
/// It is a **process-local map key**. It never reaches the wire and never
/// reaches the catalog, so no byte contract rests on it — what rests on it is
/// whether two starts are the same recording, and that is decided by five
/// parameters in this order, each followed by a `|`.
///
/// The last line is the one worth reading twice: the reference strikes the last
/// character off **unconditionally** (`sb.setLength(sb.length() - 1)`), so with
/// all five absent it is the `?` that goes, not a separator. Two channels that
/// differ only in a parameter this key does not carry are one recording.
fn make_key(stream_id: i32, uri: &ChannelUri) -> String {
    // `CommonContext`'s own names for the five, in the order the reference
    // appends them (`:1913-1943`).
    const PARAMETERS: [&str; 5] = ["endpoint", "interface", "control", "session-id", "tags"];

    let mut key = format!("{stream_id}:{}?", uri.media());

    for parameter in PARAMETERS {
        if let Some(value) = uri.get(parameter) {
            key.push_str(parameter);
            key.push('=');
            key.push_str(value);
            key.push('|');
        }
    }

    key.truncate(key.len() - 1);

    key
}

/// `max(ctx.segmentFileLength(), termBufferLength)`
/// (`ArchiveConductor.java:2006`): a segment has to be able to hold one term,
/// whatever the archive was configured with. The result is what the catalog
/// records, so a recording made by an archive with a smaller setting than the
/// driver's own term is still a recording a reader can walk.
fn segment_file_length(configured: usize, term_buffer_length: i32) -> i32 {
    i32::try_from(configured)
        .unwrap_or(i32::MAX)
        .max(term_buffer_length)
}

/// Copy a parameter's text into the matching field, if the channel carries it.
///
/// The reference has a typed overload per parameter and re-spells the value on
/// the way through — see the module note for the one place this differs. What
/// the name-to-field mapping has to get right is only which fields exist.
fn copy_text(builder: &mut ChannelUriStringBuilder, uri: &ChannelUri, name: &str) {
    let Some(value) = uri.get(name) else {
        return;
    };

    match name {
        "tags" => {
            builder.tags(value);
        }
        "endpoint" => {
            builder.endpoint(value);
        }
        "interface" => {
            builder.network_interface(value);
        }
        "control" => {
            builder.control_endpoint(value);
        }
        "control-mode" => {
            builder.control_mode(value);
        }
        "fc" => {
            builder.flow_control(value);
        }
        "cc" => {
            builder.congestion_control(value);
        }
        "alias" => {
            builder.alias(value);
        }
        "response-correlation-id" => {
            builder.response_correlation_id(value);
        }
        "response-endpoint" => {
            builder.response_endpoint(value);
        }
        "channel-snd-ts-offset" => {
            builder.channel_send_timestamp_offset(value);
        }
        "channel-rcv-ts-offset" => {
            builder.channel_receive_timestamp_offset(value);
        }
        "media-rcv-ts-offset" => {
            builder.media_receive_timestamp_offset(value);
        }
        // The numeric ones whose field is a number rather than text: a value
        // that will not read as one is not a value this builder can carry, and
        // the reference's own setters throw where this skips.
        "ttl" => {
            if let Ok(ttl) = value.parse() {
                builder.ttl(ttl);
            }
        }
        "gtag" => {
            if let Ok(tag) = value.parse() {
                builder.group_tag(tag);
            }
        }
        "session-id" => {
            if let Ok(session_id) = value.parse() {
                builder.session_id(session_id);
            }
        }
        // Sizes, which are read and re-spelled rather than copied:
        // `so-sndbuf=2048` comes back as `2k` in both, because both write
        // through `format_size`.
        "so-rcvbuf" => {
            if let Some(size) = parse_size(value) {
                builder.socket_rcvbuf_length(size);
            }
        }
        "so-sndbuf" => {
            if let Some(size) = parse_size(value) {
                builder.socket_sndbuf_length(size);
            }
        }
        "rcv-wnd" => {
            if let Some(size) = parse_size(value) {
                builder.receiver_window_length(size);
            }
        }
        // Flags. Their fields are `bool`, so the text has to read as one; the
        // reference's `Boolean.valueOf` is false for anything that is not
        // "true", which is what the fallback here matches.
        "tether" => {
            builder.tether(value.eq_ignore_ascii_case("true"));
        }
        "group" => {
            builder.group(value.eq_ignore_ascii_case("true"));
        }
        "rejoin" => {
            builder.rejoin(value.eq_ignore_ascii_case("true"));
        }
        _ => {}
    }
}

/// The protocol major this archive speaks (`ArchiveConductor.java:484`).
///
/// The reference names this (`AeronArchive.Configuration.PROTOCOL_MAJOR_VERSION`)
/// and derives it from the semantic version it stamps on every response
/// (`client/AeronArchive.java:2648-2652`). This derives it the same way rather
/// than repeating the digit in two places.
pub const PROTOCOL_MAJOR_VERSION: u8 = semantic_version_major(PROTOCOL_SEMANTIC_VERSION);

/// The archive id used when none is configured.
///
/// `Archive.Configuration.ARCHIVE_ID_DEFAULT` is `-1`, which the reference
/// resolves against the driver's own `aeron.client.id`. Resolving it is the
/// launcher's job; until it does, the configured value is used as it stands.
pub const ARCHIVE_ID_DEFAULT: i64 = -1;

/// Where a fresh archive starts numbering its control sessions.
///
/// The reference seeds this from `ThreadLocalRandom.nextInt(MAX_VALUE)`
/// (`ArchiveConductor.java:136`) so two archives on one driver do not hand out
/// the same id. Nothing observable depends on the starting number — the id is
/// handed to the client that asked, in the connect answer — so this build
/// starts at one and says so, rather than seeding from a random source this
/// build has no other use for.
pub const FIRST_SESSION_ID: i64 = 1;

/// A control-plane callback that could not do what it was asked, kept until
/// the turn in which it can.
///
/// See the module note: the adapter dispatches from inside `Client::poll_image`,
/// so the client is lent out for the whole of a callback, and a response needs
/// it. What is recorded here is the *intent*, and [`Sessions::drive`] replays
/// it in the same turn, before the sessions are driven.
///
/// The `now_ms` a callback was handed is deliberately not carried: the replay
/// runs in the same turn as the callback, so the turn's own clock is the same
/// number, and passing it through would suggest otherwise.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Deferred {
    /// An `OK` a session owes a client (`ControlSession.sendOkResponse`), which
    /// for an archive-id request is the archive's own id
    /// (`ArchiveConductor.java:525-528`).
    Ok {
        session_id: i64,
        correlation_id: i64,
        relevant_id: i64,
    },
    /// ...or an `ERROR` (`ControlSession.sendErrorResponse`), which is what a
    /// refused request is answered with (`ControlSessionAdapter.java:1212-1213`).
    Error {
        session_id: i64,
        correlation_id: i64,
        relevant_id: i64,
        message: String,
    },
    /// The answer to a challenge — the one request not gated on being `ACTIVE`
    /// (`ControlSession.java:308-315`) — which needs both the client and the
    /// authenticator.
    ChallengeResponse {
        session_id: i64,
        correlation_id: i64,
        encoded_credentials: Vec<u8>,
    },
    /// One of the questions about a recording whose answer is in the
    /// **catalog** (`ArchiveConductor.java:1159-1195`, `:742-761`).
    ///
    /// It is the first thing the callbacks defer that is not a message: the
    /// answer is not known yet, because the catalog belongs to the conductor
    /// and a callback runs holding the adapter. So what is recorded is the
    /// question, and [`Sessions::drive`] answers it — into the same
    /// `Ok`/`Error` shape as everything else.
    Query {
        session_id: i64,
        correlation_id: i64,
        query: Query,
    },
    /// `ArchiveConductor.listRecording` (`:688-706`): a request for one
    /// recording's **descriptor**, which is not answered once but by a session
    /// that keeps offering the message until the client takes it
    /// (`ListRecordingByIdSession.java:60-80`).
    ///
    /// Deferred for the same reason the questions are, and with a second one on
    /// top: the two refusals in front of the session are both catalog
    /// questions — `hasRecording`, and whether this session already has a
    /// listing in flight (`:690-706`). What is deferred is the intent, and the
    /// listing starts in the turn the intent is replayed in.
    ListRecording {
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    },
    /// The answer a listing with nothing to send gets
    /// (`ControlSession.sendSubscriptionUnknown`, `:708-711`): an `OK`'s shape
    /// with `SUBSCRIPTION_UNKNOWN` for a code.
    SubscriptionUnknown {
        session_id: i64,
        correlation_id: i64,
    },
    /// A request that has to move something (`ArchiveConductor.java:562`,
    /// `:1766-1780`) — see [`Action`].
    Action(Action),
}

/// A question about a recording that the catalog can answer.
///
/// The reference has one `ArchiveConductor` method per question
/// (`getStartPosition`, `getRecordingPosition`, `getStopPosition`,
/// `getMaxRecordedPosition`, `findLastMatchingRecording` —
/// `ArchiveConductor.java:1159-1195`, `:742-761`), four of them four lines long
/// and identical but for which field they read. Here the question is the value
/// and the reading is in one place; the *answers* are the reference's, field for
/// field.
///
/// Not `Copy`, which the four positions were: a match has a channel fragment,
/// and a fragment is however many bytes the client sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    /// `catalog.startPosition(recordingId)` (`:1159-1165`).
    StartPosition {
        /// The recording asked about.
        recording_id: i64,
    },
    /// The recording's **live** position, or `NULL_POSITION` for one that is
    /// not being recorded (`:1167-1176`).
    RecordingPosition {
        /// The recording asked about.
        recording_id: i64,
    },
    /// `catalog.stopPosition(recordingId)` (`:1178-1184`).
    StopPosition {
        /// The recording asked about.
        recording_id: i64,
    },
    /// The live position, or the stop position for a recording that is not
    /// active (`:1186-1195`).
    MaxRecordedPosition {
        /// The recording asked about.
        recording_id: i64,
    },
    /// The id of the newest recording at or after a floor whose session,
    /// stream and channel match (`:742-761` over `Catalog.findLast`,
    /// `Catalog.java:544-578`).
    ///
    /// The one question here with no recording id: the id is what it is
    /// looking for. `-1` is its "no match", which is an answer rather than a
    /// refusal (`:756-757`).
    FindLastMatching {
        /// The lowest id that may be answered with, which the reference
        /// refuses to take a negative of (`:749-753`).
        min_recording_id: i64,
        /// The session the client is looking for.
        session_id: i32,
        /// The stream it is looking for.
        stream_id: i32,
        /// A fragment the **original** channel must contain.
        channel_fragment: Vec<u8>,
    },
}

impl Query {
    /// The recording this question is about, for the four that name one.
    #[must_use]
    pub const fn recording_id(&self) -> Option<i64> {
        match self {
            Self::StartPosition { recording_id }
            | Self::RecordingPosition { recording_id }
            | Self::StopPosition { recording_id }
            | Self::MaxRecordedPosition { recording_id } => Some(*recording_id),
            Self::FindLastMatching { .. } => None,
        }
    }
}

/// The answer to a question about a recording
/// (`ArchiveConductor.java:1159-1195`, `:742-761`).
///
/// The four that name a recording all start with `hasRecording`, which refuses
/// for one the catalog does not hold and says so in the words the C client
/// prints (`:1950-1960` over `client/ArchiveException.java:272-275`). A match
/// has no such id to check — the floor is checked instead, and it is the one
/// question whose "not found" is a value.
///
/// # Errors
///
/// The refusal's message, for [`Sessions::run_deferred`] to send as
/// [`UNKNOWN_RECORDING`].
fn answer_query(
    catalog: &Catalog,
    live: &HashMap<i64, RecordingHandle>,
    counters: &CountersReader<'_, ReadWrite>,
    query: &Query,
) -> Result<i64, String> {
    // A match first, because it is the question with no recording id to check.
    if let Query::FindLastMatching {
        min_recording_id,
        session_id,
        stream_id,
        channel_fragment,
    } = query
    {
        // A floor below zero is refused before the catalog is asked at all, and
        // the refusal is the reference's own line (`:749-753`). It carries
        // `UNKNOWN_RECORDING` as its relevant id like the refusals below,
        // because that is the id `ArchiveConductor` hands `sendErrorResponse`
        // there too.
        if *min_recording_id < 0 {
            return Err(format!("minRecordingId={min_recording_id} < 0"));
        }

        // A match that is not found is `NULL_RECORD_ID` on the wire rather than
        // a refusal: the reference answers `OK` with the value its `findLast`
        // returned and says so in a comment (`:754-757`).
        return catalog
            .find_last(*min_recording_id, *session_id, *stream_id, channel_fragment)
            .map(|found| found.unwrap_or(NULL_RECORD_ID))
            .map_err(|error| format!("catalog could not search for a recording: {error}"));
    }

    let recording_id = query
        .recording_id()
        .expect("a match is the only question without a recording id, and it returned");

    if !catalog.has_recording(recording_id) {
        return Err(unknown_recording_message(recording_id));
    }

    let recording = catalog
        .recording(recording_id)
        .map_err(|error| format!("catalog could not read recording {recording_id}: {error}"))?;

    // The two that ask a recording **in flight** where it has got to
    // (`:1167-1176`, `:1186-1195`). The reference asks the session first and
    // falls back when there is none; the session's answer is its position
    // counter, which is the number a client reads for a live recording and the
    // number `MaxRecordedPosition` means by "recorded so far".
    let live_position = live
        .get(&recording_id)
        .and_then(|handle| handle.position.value(counters));

    Ok(match query {
        Query::StartPosition { .. } => recording.start_position,
        Query::StopPosition { .. } => recording.stop_position,
        Query::RecordingPosition { .. } => live_position.unwrap_or(NULL_POSITION),
        Query::MaxRecordedPosition { .. } => live_position.unwrap_or(recording.stop_position),
        Query::FindLastMatching { .. } => unreachable!("a match returns above"),
    })
}

/// `AeronArchive.NULL_POSITION`, which is `Aeron.NULL_VALUE`: what a
/// recording-position question answers for a recording that is not in flight
/// (`ArchiveConductor.java:1170-1173`).
pub const NULL_POSITION: i64 = -1;

/// `Catalog.NULL_RECORD_ID` (`Catalog.java:111`), which is `Aeron.NULL_VALUE`
/// as well: the id a match answers with when nothing matched. It is an `OK`
/// carrying `-1` and not a refusal — "matches client side specification", as
/// the reference puts it (`ArchiveConductor.java:756-757`).
pub const NULL_RECORD_ID: i64 = -1;

/// `ArchiveException.UNKNOWN_RECORDING` (`client/ArchiveException.java:54`),
/// which is what the C client prints as `errorCode=5`
/// (`aeron_archive_client.c:189-192`, and `aeron_archive_test.cpp:1125` asserts
/// the whole line).
pub const UNKNOWN_RECORDING: i64 = 5;

/// `ArchiveException.ACTIVE_LISTING` (`client/ArchiveException.java:34`), the
/// relevant id of the refusal a second listing on one session gets
/// (`ArchiveConductor.java:690-694`).
pub const ACTIVE_LISTING: i64 = 1;

/// That refusal's message, which is the reference's own line
/// (`ArchiveConductor.java:692`).
pub const ACTIVE_LISTING_MSG: &str = "active listing already in progress";

/// `ArchiveException.ACTIVE_RECORDING` (`client/ArchiveException.java:39`), the
/// refusal an extend gets for a recording that is still in flight
/// (`ArchiveConductor.java:1093-1098`, `:2066-2072`).
pub const ACTIVE_RECORDING: i64 = 2;

/// `ArchiveException.INVALID_EXTENSION` (`:74`), the refusal an image that does
/// not continue the recording gets (`ArchiveConductor.java:2155-2194`).
pub const INVALID_EXTENSION: i64 = 9;

/// `ArchiveException.MAX_RECORDINGS` (`client/ArchiveException.java:69`), the
/// refusal a start gets while too many recordings are in flight
/// (`ArchiveConductor.java:538-543`).
pub const MAX_RECORDINGS: i64 = 8;

/// `ArchiveException.ACTIVE_SUBSCRIPTION` (`:44`), the refusal a second start
/// on one channel and stream gets (`ArchiveConductor.java:574-577`).
pub const ACTIVE_SUBSCRIPTION: i64 = 3;

/// `ArchiveException.UNKNOWN_SUBSCRIPTION` (`:49`), the refusal a stop gets for
/// a subscription id no recording was registered under
/// (`ArchiveConductor.java:621-622`).
pub const UNKNOWN_SUBSCRIPTION: i64 = 4;

/// `ArchiveException.STORAGE_SPACE` (`:84`), the refusal a start gets when the
/// archive's filesystem is below the configured threshold
/// (`ArchiveConductor.java:2597-2617`).
pub const STORAGE_SPACE: i64 = 11;

/// `CommonContext.SPY_PREFIX` (`CommonContext.java:246`): the channel prefix a
/// **local** UDP publication is recorded through, because a driver's own
/// publication is not a network one and a spy link is what carries it to a
/// subscriber (`ArchiveConductor.java:559-560`).
pub const SPY_PREFIX: &str = "aeron-spy:";

/// `CommonContext.UDP_MEDIA`, which `ChannelUri.isUdp` compares the media
/// against (`ChannelUri.java:152-155`) — the test that decides whether a local
/// publication is a spy's or a network subscription's.
const UDP_MEDIA: &str = "udp";

/// `ArchiveException.buildUnknownRecordingErrorMsg` (`:272-275`), which is the
/// **message** half of that same line and has to match it word for word.
#[must_use]
pub fn unknown_recording_message(recording_id: i64) -> String {
    format!("unknown recording id: {recording_id}")
}

/// What an archive was configured to record with
/// (`Archive.Context.maxConcurrentRecordings` and
/// `lowStorageSpaceThreshold`, `Archive.java:429-438`, `:342-350`).
///
/// One argument to [`Sessions::new`] rather than three, and the same shape
/// [`ResponseChannelDefaults`] has for the control settings: what the archive
/// was configured with, as the requests that use it see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingSettings {
    /// How many recordings may be in flight at once
    /// (`aeron.archive.max.concurrent.recordings`, default 20).
    pub max_concurrent_recordings: usize,
    /// The free space below which a start is refused
    /// (`aeron.archive.low.storage.space.threshold`), in bytes.
    pub low_storage_space_threshold: u64,
    /// How long a segment file is
    /// (`aeron.archive.segment.file.length`, default 128 MiB) — which the
    /// catalog records, so it is written down per recording and not read back.
    pub segment_file_length: usize,
    /// `aeron.archive.file.io.max.length` (default 1 MiB), which bounds how
    /// much one turn of a recording may read (`RecordingSession.java:81`).
    pub file_io_max_length: usize,
    /// `aeron.archive.file.sync.level` (default 0), passed to the segment
    /// writer.
    pub file_sync_level: i32,
    /// The directory whose filesystem the threshold is about
    /// (`ctx.archiveFileStore()`, `:2602`) — and where the segments are
    /// written, which is the same place.
    pub archive_dir: PathBuf,
}

/// The settings a replay is answered from (`Archive.java:445-452`), one argument
/// to [`Sessions::new`] for the reason [`RecordingSettings`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplaySettings {
    /// How many replays may be in flight at once
    /// (`aeron.archive.max.concurrent.replays`, default 20). The bound is tested
    /// with `==` rather than `>=` (`ArchiveConductor.java:775`), which is only
    /// the same thing while nothing can push the count past it.
    pub max_concurrent_replays: usize,
}

/// A request that **moves a resource** rather than answering about one.
///
/// The second kind of thing a callback defers, after the messages and the
/// questions, and the same reason: the reference's `startRecording` calls
/// `aeron.addSubscription` and its stop calls `subscription.close()`
/// (`ArchiveConductor.java:562`, `:1766-1780`), and both need the client a
/// callback is running without.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Action {
    /// `ArchiveConductor.startRecording` (`:530-583`), all of it but the
    /// answer, which waits for the driver.
    StartRecording(StartRecordingRequest),
    /// `ArchiveConductor.stopRecordingSubscription` (`:612-624`).
    StopRecordingSubscription {
        session_id: i64,
        correlation_id: i64,
        subscription_id: i64,
    },
    /// `ArchiveConductor.stopRecording` (`:585-610`): the same stop, named by
    /// the **channel and stream** a client started it with rather than by the
    /// registration id it was answered with.
    StopRecording {
        session_id: i64,
        correlation_id: i64,
        stream_id: i32,
        original_channel: String,
    },
    /// `ArchiveConductor.extendRecording` (`:1067-1157`): a client asking to
    /// go on recording a channel whose first session ended.
    ExtendRecording(ExtendRecordingRequest),
    /// `ArchiveConductor.stopRecordingByIdentity` (`:1301-1327`), which names
    /// the *recording* — and answers with whether there was one to stop, not
    /// with a position.
    StopRecordingByIdentity {
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    },
    /// `ArchiveConductor.startReplay` (`:764-929`). Everything up to the answer,
    /// which waits for a publication to appear.
    StartReplay(StartReplayRequest),
    /// `ArchiveConductor.stopReplay` (`:1037-1054`): one replay, by the id its
    /// OK carried.
    StopReplay {
        session_id: i64,
        correlation_id: i64,
        replay_session_id: i64,
    },
    /// `ArchiveConductor.stopAllReplays` (`:1056-1064`): every replay, or every
    /// replay **of one recording** — `NULL_VALUE` for the id means all of them.
    StopAllReplays {
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
    },
}

/// What one image says about itself (`ArchiveConductor.java:1999-2005`), read
/// out of the client in one go.
///
/// A value rather than a borrow, because everything below needs the client
/// **mutably** — to add the position counter, to validate an extension, to
/// answer — and an image is borrowed from the client it came out of.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ImageFacts {
    session_id: i32,
    stream_id: i32,
    /// Where the image joined the stream: where a start begins (`:2005`), and
    /// what an extension is checked against (`:2161`).
    join_position: i64,
    initial_term_id: i32,
    term_buffer_length: i32,
    mtu_length: i32,
    source_identity: String,
}

/// What a stop named by a channel resolves to
/// (`ArchiveConductor.java:591-600`).
#[derive(Debug, Clone, PartialEq, Eq)]
enum StopDecision {
    /// The registration id the key names, which is what the stop takes off.
    Subscription(i64),
    /// Refuse, with the words the reference composes.
    Refuse { relevant_id: i64, message: String },
}

/// What a start is going to do, once the four checks in front of it have run
/// (`ArchiveConductor.java:538-577`).
#[derive(Debug, Clone, PartialEq, Eq)]
enum StartDecision {
    /// Add a subscription on `channel`, and remember it under `key`.
    Add {
        /// The `makeKey` the recording is registered by (`:553`).
        key: String,
        /// The channel to subscribe on: the stripped one, with the spy prefix
        /// when the publication is local to this driver (`:558-560`).
        channel: String,
        /// The stripped channel **without** that prefix, which is the one the
        /// catalog records and the position counter's label names
        /// (`:2008-2019`, `RecordingPos.java:114-128`).
        ///
        /// The two are not interchangeable and the reference is easy to misread
        /// here: `strippedChannel` is what the handler closure captures
        /// (`:558-566`), and `channel` is only ever the argument to
        /// `addSubscription`.
        stripped_channel: String,
    },
    /// Answer the client with this refusal instead
    /// (`ControlSession.sendErrorResponse`).
    Refuse {
        /// The relevant id the refusal carries, which is one of
        /// [`MAX_RECORDINGS`], [`STORAGE_SPACE`], [`ACTIVE_SUBSCRIPTION`] or
        /// the generic zero (`client/ArchiveException.java:29`).
        relevant_id: i64,
        /// The refusal's words.
        message: String,
    },
}

/// What a recording subscription was asked for: a recording that **starts**
/// here, or one that is being **appended to** (`ArchiveConductor.java:562-570`
/// against `:1124-1150`).
///
/// The two are the same request from the driver's side — add a subscription on
/// this channel — and they differ in everything after that: what is written to
/// the catalog, which signal the client gets, and whether the image is checked
/// against a recording that already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordingRequest {
    /// Record this channel, from wherever its image joins.
    Start,
    /// Append the image's bytes to the recording this id names.
    Extend {
        /// The recording the bytes belong to — the **same** recording id the
        /// first session had, which is what makes this an append and not a
        /// second recording.
        recording_id: i64,
    },
}

/// One recording subscription this archive holds
/// (`ArchiveConductor.recordingSubscriptionByKeyMap`'s value, `:152`).
///
/// The reference keeps the `Subscription` object, which is why its
/// `AvailableImageHandler` closure can capture the two channels and `autoStop`
/// and still know which subscription an image belongs to. The object is the
/// client's here and a registration id is not a handle on it, so the facts the
/// conductor would have had from the object are kept beside its id.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordingSubscription {
    /// The registration id the start answered with (`:570`), and what an image
    /// arrives for.
    ///
    /// The `makeKey` is **not** a field: it is what the map is keyed by, which
    /// is how the reference holds it too (`Subscription` does not know its own
    /// key) and how a stop finds it again (`:2139-2153`).
    registration_id: i64,
    /// The stripped channel, **without** the spy prefix the subscription was
    /// added on (`:558-560`).
    ///
    /// This is what the catalog records and what the position counter's label
    /// names (`:2008-2019`) — not the key, which is a different string for the
    /// same channel, and not the channel the subscription was added on, which
    /// carries `aeron-spy:` in front of it for a local publication.
    stripped_channel: String,
    /// The channel the client asked for, which the catalog records too.
    original_channel: String,
    /// Whether the recording stops when the client that asked goes away
    /// (`:1329-1363`), captured per start because the request is where it comes
    /// from.
    is_auto_stop: bool,
    /// The channel the subscription was **added on**, which is the stripped one
    /// with the spy prefix in front of it for a local UDP publication
    /// (`:558-560`).
    ///
    /// This is what a recording-subscription descriptor carries, because it is
    /// what the reference has: it sends `subscription.channel()`
    /// (`ControlResponseProxy.java:91-107`), the object's own channel, however
    /// odd that reads next to a field called `strippedChannel`.
    channel: String,
    /// The stream it was added for, which the descriptor carries too and which
    /// `applyStreamId` filters on (`:104-105`).
    stream_id: i32,
    /// The control session that asked and the correlation id of its request,
    /// which are what the `START`/`EXTEND` signal is sent with (`:2046-2051`,
    /// `:2121-2122`) — captured per request, as the reference's handler closure
    /// captures them.
    session_id: i64,
    correlation_id: i64,
    /// Whether this subscription is making a recording or appending to one.
    request: RecordingRequest,
}

/// A start whose subscription the driver has not answered yet.
///
/// The reference's `aeron.addSubscription` **waits** and answers with the
/// `Subscription` in the same call (`ArchiveConductor.java:562-565`), because
/// the reference drives the driver's conductor from inside its own turn
/// (`invokeDriverConductor`, `:392`). This build's launcher owns that loop, so
/// the command goes out on one turn and the answer is read on a later one —
/// which is the two steps the counters and the control publications already
/// take, and the answer the client gets is still the registration id the
/// `ADD_SUBSCRIPTION` drew (`:570`).
struct PendingStartRecording {
    session_id: i64,
    correlation_id: i64,
    /// The `makeKey` the start was accepted under, which is what the registry
    /// is filed by (`:567`).
    key: String,
    /// The registration id the add drew — the id the start will answer with.
    registration_id: i64,
    /// The stripped channel, which is what the catalog records and what the
    /// position counter's label names (`:2008-2019`) — **without** the spy
    /// prefix the subscription was added on, and a different string again from
    /// the key.
    stripped_channel: String,
    /// The channel the subscription was actually added on, which a
    /// recording-subscription descriptor carries (`:91-107` of the response
    /// proxy sends the subscription's own channel).
    channel: String,
    /// The stream it is for.
    stream_id: i32,
    /// The channel the client asked for.
    original_channel: String,
    /// Whether the recording ends with the client that asked (`:2044`).
    is_auto_stop: bool,
    /// Whether this subscription is making a recording or appending to one.
    request: RecordingRequest,
}

/// What the conductor remembers about one live recording
/// (`ArchiveConductor.recordingSessionByIdMap`'s value, `:158`).
///
/// Two facts, and both are asked by id: the **position counter**, which is what
/// `getRecordingPosition` and `getMaxRecordedPosition` answer with while a
/// recording is in flight (`:1167-1195`), and the **subscription**, which is
/// what a stop by identity takes off (`:1301-1327`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordingHandle {
    position: RecordingPos,
    subscription_id: i64,
}

/// A recording whose row is in the catalog and whose position counter the
/// driver has not allocated yet.
///
/// `RecordingPos.allocate` is synchronous in the reference
/// (`RecordingPos.java:130-131`) because its archive drives the driver from the
/// turn it is in. Here the counter is asked for on one turn and taken up on a
/// later one — see [`Sessions::claim_recordings`] — so a recording spends a turn
/// between its catalog row and its session. What the acceptance sees is a
/// counter that appears a turn later, which the C harness's own polling
/// accommodates (`aeron_archive_test.cpp:275-286`).
struct PendingRecording {
    /// The control session that asked, which the `START` signal is sent to.
    session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    /// Where the recording starts: the **image's join position**
    /// (`ArchiveConductor.java:2005`), which is also where the counter is set
    /// before anything is read (`:2029-2030`).
    start_position: i64,
    /// Where the **writer** starts, which is what a segment's base positions are
    /// counted from. A start's is its join position; an extend's is where the
    /// recording began the first time (`:2109`), because the bytes go into the
    /// same segments (`:2005` against `:2109` is the whole of that difference).
    session_start_position: i64,
    /// Whether this is the recording's first session or a continuation
    /// (`:2046-2051` against `:2121-2122`).
    request: RecordingRequest,
    /// The **image's** session id, which is what an extension writes into the
    /// row (`:2120`: `image.sessionId()`, not the archive's).
    image_session_id: i32,
    segment_file_length: usize,
    subscription_id: i64,
    image_id: i64,
    term_buffer_length: i32,
    is_auto_stop: bool,
    position: RecordingPos,
}

/// A connect request whose session has not been made yet.
///
/// [`Sessions::new_session`] runs inside the adapter's poll and so cannot reach
/// the client; everything the session needs is copied out of the request here
/// and used when the connect is run. The request borrows its channel, its
/// credentials and its client info, so those are owned by this struct.
struct PendingConnect {
    session_id: i64,
    image: ImageId,
    correlation_id: i64,
    response_stream_id: i32,
    response_channel: String,
    invalid_version_message: Option<String>,
    encoded_credentials: Vec<u8>,
    client_info: String,
    /// Set when the adapter aborted this session in the same poll that made
    /// it — see [`Sessions::abort_session`].
    abort_reason: Option<String>,
}

/// One recording's descriptor, on its way to the client that asked for it.
///
/// This is the reference's `ListRecordingByIdSession`
/// (`ListRecordingByIdSession.java:26-96`), which is a `Session` in the
/// conductor's own `SessionWorker` — the same worker control sessions live in —
/// and which the asking session holds a reference to in `activeListing`
/// (`ControlSession.java:91`, `:298-306`).
///
/// **What is not the reference's shape**: the two objects are one here. Java has
/// the listing in the worker's list *and* the control session pointing at it;
/// a value cannot be in two places, so the listing lives in this list and the
/// asking session is named by id — the same one-owner rule the adapter's module
/// note gives for the sessions themselves.
///
/// The state is three fields because that is all the reference's session has
/// beyond the collaborator references it is handed each turn: a listing does its
/// work against the catalog and the session it was created from, both of which
/// are arguments here.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Listing {
    /// One recording's descriptor (`ListRecordingByIdSession`, `:60-80`).
    Recording(RecordingListing),
    /// A page of recording **subscriptions**
    /// (`ListRecordingSubscriptionsSession`, `:96-127`).
    Subscriptions(SubscriptionListing),
    /// A page of **recordings** (`AbstractListRecordingsSession`, `:79-142`
    /// over its two subclasses).
    Recordings(RecordingsListing),
}

impl Listing {
    /// The session being served, which is what a listing that outlived its
    /// session is found by.
    const fn control_session_id(&self) -> i64 {
        match self {
            Self::Recording(listing) => listing.control_session_id,
            Self::Subscriptions(listing) => listing.control_session_id,
            Self::Recordings(listing) => listing.control_session_id,
        }
    }

    /// Whether it is finished with.
    const fn is_done(&self) -> bool {
        match self {
            Self::Recording(listing) => listing.is_done,
            Self::Subscriptions(listing) => listing.is_done,
            Self::Recordings(listing) => listing.is_done,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordingListing {
    /// The control session that asked, and the one the descriptor goes to.
    control_session_id: i64,
    /// The correlation id of the request (`ListRecordingByIdSession.sessionId`
    /// is this one, `:83-86`).
    correlation_id: i64,
    /// The recording whose descriptor it is.
    recording_id: i64,
    /// Set when the descriptor went out, or when the recording turned out not
    /// to be there any more — and by [`Sessions::drive_listings`] when the
    /// session that asked is gone (`Session.abort`, `:42-46`).
    is_done: bool,
}

/// A walk through this archive's recording subscriptions, `subscriptionCount` at
/// a time (`ListRecordingSubscriptionsSession`, `:26-39`).
///
/// The three fields that move are the reference's: `pseudo_index` is where the
/// walk resumes — updated after **every** entry it passes, sent or not — and
/// `sent` is how many descriptors the client has taken. What makes the walk
/// resumable is that both survive a turn in which the send did not take
/// (`:109-113`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SubscriptionListing {
    control_session_id: i64,
    correlation_id: i64,
    /// How many entries of the registry to skip before answering.
    pseudo_index: i32,
    /// How many descriptors the client asked for.
    subscription_count: i32,
    /// How many have gone out.
    sent: i32,
    /// The stream to answer about, when `apply_stream_id`.
    stream_id: i32,
    /// Whether the stream is part of the question at all (`:104`).
    apply_stream_id: bool,
    /// A piece of channel every answered subscription's must contain.
    channel_fragment: String,
    is_done: bool,
}

/// How many of the catalog's entries one turn of a recordings listing **looks
/// at** (`AbstractListRecordingsSession.MAX_SCANS_PER_WORK_CYCLE`, `:22`).
///
/// It bounds the scan and not the page: an entry the filter turns down counts
/// against it and does not fill the page any faster, which is what keeps a
/// for-uri request over a large catalog from spending a whole turn here.
const MAX_SCANS_PER_WORK_CYCLE: i32 = 64;

/// A page of this archive's **recordings**, `count` at a time
/// (`AbstractListRecordingsSession`, `:79-142`).
///
/// The reference has the walk in an abstract session and the one thing the two
/// requests differ in — which recordings are taken — in two subclasses. That is
/// the shape here too: one walk, and [`RecordingsFilter`] for the subclasses'
/// `acceptDescriptor`.
///
/// The three fields that move are the reference's: `from_recording_id` is the
/// cursor the walk resumes from, `sent` is how many descriptors the client has
/// taken, and both survive a turn in which a send did not take (`:117-121`).
/// The cursor is an **id** and not a position in the catalog, which is what
/// lets a walk that was interrupted pick up recordings added while it was away.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordingsListing {
    control_session_id: i64,
    correlation_id: i64,
    /// The id the walk resumes at, and what a walk that ran out of recordings
    /// names in its terminal answer (`AbstractListRecordingsSession
    /// .recordingId`, `:29`, read at `:110`).
    from_recording_id: i64,
    /// How many descriptors the client asked for (`count`, `:26`).
    count: i32,
    /// How many have gone out (`sent`, `:30`).
    sent: i32,
    /// Which recordings the page answers about.
    filter: RecordingsFilter,
    is_done: bool,
}

/// Which of the catalog's recordings a page answers about — the whole of what
/// separates the reference's two listing sessions
/// (`ListRecordingsSession.acceptDescriptor`, `:39-42`, against
/// `ListRecordingsForUriSession`, `:52-61`).
#[derive(Debug, Clone, PartialEq, Eq)]
enum RecordingsFilter {
    /// Every recording the catalog holds (`ListRecordingsRequest`, template 8).
    All,
    /// One stream on one channel (`ListRecordingsForUriRequest`, template 9).
    Uri {
        stream_id: i32,
        /// A piece of the recording's **original** channel, compared as bytes
        /// and anywhere in it (`Catalog.originalChannelContains`,
        /// `Catalog.java:585-625`). An empty fragment is in every channel, so a
        /// client asking about a stream on any channel passes nothing.
        channel_fragment: Vec<u8>,
    },
}

impl RecordingsListing {
    /// Whether this listing's filter takes a recording
    /// (`acceptDescriptor`, `ListRecordingsSession.java:39-42` against
    /// `ListRecordingsForUriSession.java:52-61`).
    ///
    /// The reference reads the fields out of the descriptor buffer it has
    /// **already wrapped** and cannot fail; this build reads the record back by
    /// id to get the same two fields, and that can. A record the filter cannot
    /// read is answered for the way a record that cannot be wrapped is — see
    /// [`walk_recordings`].
    ///
    /// # Errors
    ///
    /// [`CatalogError::Malformed`] when the record the index holds cannot be
    /// decoded.
    fn accepts(&self, catalog: &Catalog, recording_id: i64) -> Result<bool, CatalogError> {
        match &self.filter {
            RecordingsFilter::All => Ok(true),
            RecordingsFilter::Uri {
                stream_id,
                channel_fragment,
            } => {
                let recording = catalog.recording(recording_id)?;

                Ok(*stream_id == recording.stream_id
                    && channel_contains(&recording.original_channel, channel_fragment))
            }
        }
    }
}

/// How one turn of a recordings listing ended.
///
/// The two ends that need the client are handed back rather than performed,
/// because the walk is a pure function of the catalog and the listing — the
/// same split [`Sessions::start_listing`] makes, and what makes the cursor, the
/// scan cap and the terminal answers testable without a driver to talk to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordingsWalk {
    /// The walk ran out of recordings in the page it was asked for: the client
    /// is told with `RECORDING_UNKNOWN` naming this id, and the listing is done
    /// (`AbstractListRecordingsSession.java:107-113`).
    Unknown(i64),
    /// A descriptor did not go out — a full window, or a log turning over. The
    /// cursor has not moved and the listing is tried again next turn (`:117-121`),
    /// which is the whole of its pagination. Whether the listing is over anyway
    /// is the asking session's answer, and so the caller's question.
    SendFailed,
    /// The turn is over: the page is full, or the scan cap was reached with the
    /// listing still live.
    Idle,
}

/// Serve one turn of a recordings listing (`AbstractListRecordingsSession
/// .doWork`, `:79-142`).
///
/// The walk is the reference's, in the reference's order: find where the cursor
/// falls, then look at up to [`MAX_SCANS_PER_WORK_CYCLE`] entries, sending the
/// ones the filter takes, until the page is full or the catalog runs out.
///
/// Two of the three ways out need the client, and both are handed back rather
/// than performed: the walk running out of recordings, which is answered with
/// `RECORDING_UNKNOWN`, and a descriptor that did not go out, which is a turn to
/// try again.
///
/// **What is not the reference's shape** is where the entries come from. Java
/// walks a flat `long[]` of `(id, offset)` pairs, searching it and then stepping
/// two at a time; this takes [`Catalog::recording_ids`] and reads each record by
/// id. Same answers, different index — the same call [`Catalog::find_last`]
/// made. What it costs is a copy of the ids and one lookup per candidate, both
/// on a path a client is waiting on rather than a publish path.
///
/// `send` is handed the descriptor and answers whether it went out — the
/// client's own `sendDescriptor`, and a parameter because this build's
/// `drive_listings` holds a real `Client` and a test cannot.
fn walk_recordings(
    listing: &mut RecordingsListing,
    catalog: &Catalog,
    mut send: impl FnMut(Vec<u8>) -> bool,
) -> RecordingsWalk {
    let ids: Vec<i64> = catalog.recording_ids().collect();

    // `CatalogIndex.find` followed by the linear scan it falls back to
    // (`:90-102`): where the walk starts is the first entry whose id is at or
    // after the cursor, and an id no entry carries is not a sentinel — a
    // request naming one starts at the next recording there is, and a request
    // naming a very low one starts at the first.
    let start = ids.partition_point(|&id| id < listing.from_recording_id);

    // `recordsScanned < MAX_SCANS_PER_WORK_CYCLE` (`:105`), with `position` the
    // loop's own count: Java starts at a position found by search and steps it
    // two at a time; here the position is the walk and the two bounds are the
    // same bound.
    let scanned_to = start.saturating_add(MAX_SCANS_PER_WORK_CYCLE as usize);

    for position in start..scanned_to {
        if listing.sent >= listing.count {
            break;
        }

        // `position > lastPosition` (`:107`): there is nothing at or after the
        // cursor, so the client is told there is nothing more — naming the
        // **cursor**, which is the id it asked from or the one past the last
        // recording the walk answered about (`:110`, `:129-132`).
        let Some(&recording_id) = ids.get(position) else {
            return RecordingsWalk::Unknown(listing.from_recording_id);
        };

        // `wrapDescriptorAtOffset(…) < 0` (`:108`): a record that cannot be
        // wrapped or read is answered for by name, and that name is the
        // **entry's** id rather than the cursor's (`:110`) — a second turn that
        // starts from the cursor could otherwise walk past it.
        let Ok(Some(descriptor)) = catalog.descriptor_body(recording_id) else {
            return RecordingsWalk::Unknown(recording_id);
        };

        match listing.accepts(catalog, recording_id) {
            Ok(true) => {
                if !send(descriptor) {
                    return RecordingsWalk::SendFailed;
                }

                listing.sent += 1;
            }
            Ok(false) => {}
            // The record the filter has to read is not readable: the same
            // answer as a record that cannot be wrapped, and the same id — see
            // the plan's Q5. Letting the cursor stand still instead would pin
            // the listing on that record for every turn that followed.
            Err(_) => return RecordingsWalk::Unknown(recording_id),
        }

        // `:125-133`, and the difference between the two arms matters: the
        // cursor moves to the **next entry's** id, so ids the catalog does not
        // hold are stepped over in one go rather than one at a time — and past
        // the last entry it moves to one past the **cursor**, which is where a
        // walk that fell in a gap has to resume from.
        listing.from_recording_id = match ids.get(position + 1) {
            Some(&next) => next,
            None => listing.from_recording_id + 1,
        };
    }

    // `if (sent == count)` (`:136-139`) — **not** `==` here, which is the one
    // place this build departs from the reference: with a negative `count` the
    // reference's test is never true, so the listing never ends and the
    // session's `activeListing` slot is held for good. The two are the same
    // test wherever `count` is positive, because the loop above adds one to
    // `sent` at a time under `sent < count`. See the plan's Q6.
    if listing.sent >= listing.count {
        listing.is_done = true;
    }

    RecordingsWalk::Idle
}

/// Finish a listing whichever kind it is — what the reference does with
/// `abort` (`Session.abort`, `ListRecordingByIdSession.java:42-46`).
fn mark_done(listing: &mut Listing) {
    match listing {
        Listing::Recording(listing) => listing.is_done = true,
        Listing::Subscriptions(listing) => listing.is_done = true,
        Listing::Recordings(listing) => listing.is_done = true,
    }
}

/// Whether a subscription is one the client asked about
/// (`ListRecordingSubscriptionsSession.doWork`, `:104-105`).
///
/// The channel test is a **substring** of the channel the subscription was
/// added on, and an empty fragment is in every string — which is why a client
/// with nothing to match on passes nothing. The stream is part of the question
/// only when `apply_stream_id`, which is what lets one request ask "this stream
/// on any channel" or "any stream on this channel".
fn matches_listing(
    subscription: &RecordingSubscription,
    stream_id: i32,
    apply_stream_id: bool,
    channel_fragment: &str,
) -> bool {
    !(apply_stream_id && subscription.stream_id != stream_id)
        && subscription.channel.contains(channel_fragment)
}

/// Why a listing request is not going to be served
/// (`ArchiveConductor.listRecording`, `ArchiveConductor.java:690-698`).
///
/// Both are answers the client is sent, and they are two different answers:
/// one is an `ERROR` naming the action that is already in progress, the other is
/// `RECORDING_UNKNOWN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListingRefusal {
    /// The session that asked is already being served (`:690-694`).
    ActiveListing,
    /// The catalog does not hold the recording (`:695-698`).
    UnknownRecording,
}

/// A live control session and the identity it is reachable by.
///
/// The reference's `SessionInfo` is the same pair (`ControlSessionAdapter
/// .java:88`). What it does *not* keep is the client info: that is consumed
/// once, building the session counter's label.
struct SessionEntry {
    control: ControlSession<ControlResponseProxy>,
    image: ImageId,
}

/// A session the conductor has finished with.
///
/// The session is closed (publication and counter given back) before this is
/// recorded; what is left is what the *adapter* still has to be told, which is
/// the image it was holding and whether to reject it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndedSession {
    /// The session that ended.
    pub session_id: i64,
    /// The image it was reachable by.
    pub image: ImageId,
    /// Why it ended, in the reference's words.
    pub abort_reason: Option<String>,
    /// Whether its image is to be rejected, by the reference's rule — see
    /// [`EndedSession::is_aborted`].
    pub aborted: bool,
}

impl EndedSession {
    /// Whether this session's image is rejected, which is not the same as
    /// "the session ended".
    ///
    /// `ControlSession.close` (`ControlSession.java:186-190`) rejects unless the
    /// reason is a clean close, a response publication that never connected, or
    /// a request image that went away — the three ways a session ends without
    /// the client having done anything wrong, and so without anything to
    /// punish the image for.
    fn reason_rejects_image(reason: Option<&str>) -> bool {
        match reason {
            None => false,
            Some(reason) => {
                reason != SESSION_CLOSED_MSG
                    && reason != RESPONSE_NOT_CONNECTED_MSG
                    && !reason.starts_with(REQUEST_IMAGE_NOT_AVAILABLE_MSG)
            }
        }
    }
}

/// The live control sessions, and the `ControlPlane` the adapter dispatches
/// into.
///
/// See the module note for why this is separate from [`ArchiveConductor`]: the
/// adapter needs something to call while it holds the client, and the
/// conductor needs to hold the adapter. The callbacks therefore reach the
/// sessions directly and defer everything that needs the client.
pub struct Sessions {
    /// `ctx.archiveId()`, which the archive-id answer carries
    /// (`ArchiveConductor.java:525-528`).
    archive_id: i64,
    /// `nextSessionId` (`:136`, `:490`).
    next_session_id: i64,
    /// `ctx.connectTimeoutNs` in milliseconds (`:200`), which a session's
    /// pending response is bounded by.
    connect_timeout_ms: i64,
    /// `ctx.sessionLivenessCheckIntervalNs` in milliseconds (`:201`).
    liveness_check_interval_ms: i64,
    /// How long a command to the driver may take.
    command_timeout: Duration,
    /// What the archive's own control settings contribute to a response
    /// channel (`:460-474`).
    response_channel_defaults: ResponseChannelDefaults,
    /// The subscriptions a control request can arrive on, which is how an
    /// image is found from the id the adapter named it by.
    subscription_ids: Vec<i64>,

    /// The sessions, by the id the client is answered with.
    sessions: HashMap<i64, SessionEntry>,
    /// Connects that arrived this turn and have not been made into sessions.
    pending_connects: Vec<PendingConnect>,
    /// Responses the callbacks owed and could not send.
    pending: Vec<Deferred>,
    /// `recordingSubscriptionByKeyMap` (`ArchiveConductor.java:152`): what this
    /// archive has been asked to record, by the key [`make_key`] builds.
    ///
    /// The reference holds a `Subscription` here — the object itself, which
    /// carries its own registration id and stream id and which the
    /// `AvailableImageHandler` closure captured the two channels and `autoStop`
    /// beside. The object is the client's in this build, so what is kept is
    /// [`RecordingSubscription`]: the same facts, without the resource.
    recording_subscriptions: HashMap<String, RecordingSubscription>,
    /// `subscriptionRefCountMap` (`:151`): how many things hold each recording
    /// subscription, which is what decides when it is given back
    /// (`abortRecordingSessionAndCloseSubscription`, `:1766-1780`).
    subscription_ref_counts: HashMap<i64, i64>,
    /// `numActiveRecordings` (`:139`): how many recordings are in flight, which
    /// is what [`RecordingSettings::max_concurrent_recordings`] bounds
    /// (`:538-543`).
    ///
    /// **Two movements, and they are not the pair they look like.** It goes
    /// **up where the subscription is added** (`:569`, in
    /// [`Sessions::claim_pending_starts`]) and **down where a recording
    /// *session* closes** (`:1361`, in [`Sessions::close_recording_session`]) —
    /// *not* where an image arrives, and *not* where a subscription is stopped.
    /// Getting either wrong is invisible until the bound is reached: an extra
    /// increment per recording walks the count up to the maximum and leaves it
    /// there, refusing every later recording for the life of the process.
    num_active_recordings: usize,
    /// The settings those requests are answered from.
    recording: RecordingSettings,
    /// Starts whose `ADD_SUBSCRIPTION` has gone out and not been answered.
    pending_starts: Vec<PendingStartRecording>,
    /// Removals sent and not answered (`Client::remove_subscription_poll` is
    /// what collects them, and what takes the subscription out of the client).
    pending_removals: Vec<AsyncRemove>,

    /// `recordingSessionByIdMap` (`ArchiveConductor.java:158`): the recordings
    /// that are in flight, by the id a client names them by.
    ///
    /// The reference holds the session **objects**, because the two things it
    /// asks them are the session's own: where it has got to, and which
    /// subscription it reads. A session is the recorder's here, so what the
    /// conductor keeps is those two facts — [`RecordingHandle`] — and the
    /// recorder is asked for everything else.
    recording_session_by_id: HashMap<i64, RecordingHandle>,
    /// Recordings whose row is written and whose position counter the driver
    /// has not answered about yet (see [`PendingRecording`]).
    pending_recordings: Vec<PendingRecording>,
    /// The recording-session counter (111), asked for once and taken up when
    /// the driver answers (`Archive.java:1565-1574`).
    recording_session_counter_registration_id: Option<i64>,
    /// That counter, which every recording session moves in and out of
    /// (`ArchiveConductor.java:2056`, `:1362`).
    recording_session_counter: Option<ArchiveIdCounter>,

    /// Descriptors on their way out, one per listing request that is still
    /// being served (`SessionWorker.sessions`, `SessionWorker.java:23`).
    ///
    /// A list rather than one slot on the asking session: the reference's
    /// worker holds every session in one array, and a client is allowed one
    /// listing *per session* — which is what
    /// [`Sessions::has_active_listing`] is the check for.
    listings: Vec<Listing>,
    /// `replayId` (`ArchiveConductor.java:138`): the counter the **high** half
    /// of a `replaySessionId` comes from.
    ///
    /// **Starts at 1**, never 0 (`:138`). The low half is the replay
    /// publication's own Aeron session id, and a zero high half would make a
    /// `replaySessionId` that reads as a bare session id.
    next_replay_id: i32,
    /// `numActiveReplays` (`:139`), which [`ReplaySettings`] bounds.
    ///
    /// Two movements, in two different places, and they are not a pair in the
    /// way a reader expects: **up** in `start_replay`, before a publication is
    /// even asked for (`:931-934`), and **down** either where a publication
    /// could not be made (`CreateReplayPublicationSession.java:152-153`, which is
    /// a rollback of that `++`) or where a replay session closes (`:936-939`).
    /// Losing one rolls the count down twice and lets the bound be passed;
    /// losing the other leaves it up and refuses every later replay.
    num_active_replays: usize,
    /// `replaySessionByIdMap` (`:146`): the replays in flight, by the id the
    /// client's OK carried.
    replay_sessions: HashMap<i64, ReplayEntry>,
    /// Publications being made, before there is a session to put one in. The
    /// reference keeps these in the same session array as everything else
    /// (`addSession`, `:921`); here they are their own list, and a failure in
    /// one is a rollback rather than a session ending.
    creating_replays: Vec<CreateReplayPublicationSession>,
    /// The settings a replay is answered from.
    replay: ReplaySettings,
    /// `ArchiveConductor.Replayer`'s counters (`ArchiveConductor.java:2745-2791`):
    /// how many replays are open (112) and what the reads have cost (108-110).
    ///
    /// The replay sessions are driven from this same struct rather than by a
    /// worker of their own — that is P2-9a's — so the totals are fed from
    /// [`Sessions::drive_replay_sessions`] and the four are allocated here.
    replayer: Replayer,
    /// `controlSessionByReplayToken` (`ArchiveConductor.java:153`): the tokens
    /// a replay on a client's **response channel** is asked for and answered
    /// with, by the token that names them.
    ///
    /// A map keyed by a random `i64` and not a counter, and it is the one place
    /// in this file where that is true. What it buys is that a token cannot be
    /// guessed from the one before it, because the request it authorises
    /// arrives on an image the session was never opened on and so passes none of
    /// the gate's checks.
    replay_tokens: HashMap<i64, ReplayToken>,

    /// Sessions finished this turn, waiting for the conductor to collect them.
    ended: Vec<EndedSession>,
    /// The net movement of the aggregate session counter (102), applied once
    /// per turn (`:520`, `ControlSessionAdapter.java:1158-1161`).
    ///
    /// A callback cannot write it — the region is the conductor's — and the
    /// reference's two writes are a release on creation and a release on
    /// removal, so the net of one turn is what the region has to move by.
    session_count_delta: i64,
    /// The aggregate counter's add, from the turn it was asked for until the
    /// driver answers it.
    session_counter_registration_id: Option<i64>,
    /// The aggregate counter itself, taken up on the turn the driver answers
    /// (`Archive.java:1560-1561`).
    control_sessions: Option<ControlSessionsCounter>,
    /// Warnings the callbacks raised, for the conductor to log (`:443-446`).
    warnings: Vec<String>,
}

/// `ArchiveException.MAX_REPLAYS` (`ArchiveException.java:64`): what a replay
/// refused for the concurrency bound carries as its `relevantId`
/// (`ArchiveConductor.java:777-778`).
pub const MAX_REPLAYS: i64 = 7;

/// `ArchiveException.EMPTY_RECORDING` (`ArchiveException.java:99`).
pub const EMPTY_RECORDING: i64 = 15;

/// `ArchiveException.INVALID_POSITION` (`ArchiveException.java:109`).
pub const INVALID_POSITION: i64 = 16;

/// `AeronArchive.REPLAY_ALL_AND_FOLLOW` (`client/AeronArchive.java:117`), which
/// is `NULL_LENGTH` — the same `-1` a null position is.
pub const REPLAY_ALL_AND_FOLLOW: i64 = -1;

/// `AeronArchive.REPLAY_ALL_AND_STOP` (`client/AeronArchive.java:122`).
pub const REPLAY_ALL_AND_STOP: i64 = -2;

/// What a turn reads about a replay's limit, per replay in flight.
///
/// The two arms are `ReplaySession.notExtended`'s (`ReplaySession.java:561-577`),
/// and the difference between them is the whole of how a bounded replay ends:
/// [`LimitUpdate::ExtendTo`] moves the stop position forward and nothing else,
/// and [`LimitUpdate::Gone`] clamps what the replay may still send to where it
/// has already got to — which is what takes it to `INACTIVE`.
enum LimitUpdate {
    /// The counter, or the live recording, says this.
    ExtendTo(i64),
    /// There is nothing left to read: the counter was closed, or its slot has
    /// been taken by another counter.
    Gone,
}

/// `new Counter(aeron.countersReader(), limitCounterId)`
/// (`ArchiveConductor.startBoundedReplay`, `:1000-1006`), which the reference
/// wraps in a `try`: a bounded replay whose counter is not there is **refused**,
/// not replayed to a limit nobody can read.
///
/// The three things it keeps are the three the reading needs, and they are kept
/// rather than the value because the value moves — the whole point of a bounded
/// replay is that the client can raise the limit while it runs.
fn resolve_limit_counter<Access>(
    counters: &CountersReader<'_, Access>,
    limit_counter_id: i64,
) -> Option<LimitCounter> {
    let counter_id = i32::try_from(limit_counter_id).ok()?;
    let descriptor = counters.get(counter_id)?;

    Some(LimitCounter {
        counter_id,
        type_id: descriptor.type_id,
        registration_id: descriptor.registration_id,
    })
}

/// Read a bounded replay's limit now, or `None` for "stop extending"
/// (`ReplaySession.notExtended`, `:544-578`).
///
/// The reference's answer has three cases — the counter is live, it is closed
/// and still ours, or its slot has been **reused** — and this build can tell
/// two of them apart: [`CountersReader::get`] walks allocated slots only, so
/// *closed* and *reused* both come back as `None`. They are answered the way
/// the reference answers **reuse**, which is to freeze: [`None`] here means the
/// stop position stops moving, and it never moves backwards either, because
/// [`ReplaySession::extend_stop_position`] only ever raises it.
fn read_limit_counter<Access>(
    counters: &CountersReader<'_, Access>,
    limit: LimitCounter,
) -> Option<i64> {
    let descriptor = counters.get(limit.counter_id)?;

    (descriptor.type_id == limit.type_id && descriptor.registration_id == limit.registration_id)
        .then_some(descriptor.value)
}

/// `ArchiveConductor.isInvalidReplayPosition` (`:2256-2288`).
///
/// Three refusals, and the codes are **not** all the same: a position that is
/// not frame-aligned is answered with `GENERIC`, because the reference's
/// `sendErrorResponse(correlationId, msg)` there is the overload that carries no
/// code (`:2262-2270`), while the two that name a recording are
/// `INVALID_POSITION`.
///
/// Returns the code and the message, or `None` when the position is one this
/// recording can be replayed from.
fn invalid_replay_position(
    recording_id: i64,
    replay_position: i64,
    recording: &Recording,
) -> Option<(i64, String)> {
    // The three texts are the reference's own, word for word
    // (`ArchiveConductor.java:2262-2286` and the two builders in
    // `client/ArchiveException.java:244-268`): they are the strings a client
    // matches on, and the archive's errors are the one part of the wire a test
    // asserts verbatim.
    if replay_position % i64::from(FRAME_ALIGNMENT) != 0 {
        return Some((
            0,
            format!(
                "requested replay start position={replay_position} is not a multiple of \
                 FRAME_ALIGNMENT ({FRAME_ALIGNMENT}) for recording {recording_id}"
            ),
        ));
    }

    if replay_position < recording.start_position {
        return Some((
            INVALID_POSITION,
            format!(
                "requested replay start position={replay_position} is less than recording start \
                 position={} for recording {recording_id}",
                recording.start_position
            ),
        ));
    }

    if recording.stop_position != NULL_VALUE && replay_position >= recording.stop_position {
        return Some((
            INVALID_POSITION,
            exceeds_limit_message(replay_position, recording.stop_position, recording_id),
        ));
    }

    None
}

/// `ArchiveException.buildReplayExceedsLimitErrorMsg` (`client/ArchiveException.java:244-251`).
///
/// One builder for two refusals, and both call it: a position at or past a
/// stopped recording's stop (`ArchiveConductor.java:2282`) and one past a
/// bounded replay's limit (`:837-839`). The word is **limit** in both, which is
/// the reference's choice and not the recording's field name.
fn exceeds_limit_message(replay_position: i64, limit_position: i64, recording_id: i64) -> String {
    format!(
        "requested replay start position={replay_position} must be less than the limit \
         position={limit_position} for recording {recording_id}"
    )
}

/// The channel a replay publishes on (`ArchiveConductor.java:891-901`).
///
/// The client's channel **stripped**, with the recording's own geometry written
/// back on. `initialPosition` is four parameters out of three numbers, and it is
/// spelled out here rather than left as a method on the builder because its
/// three refusals — a negative position, one that is not frame-aligned, a term
/// length that is not a power of two — are the **same refusals** the archive
/// makes of a replay request (`:2256-2288`), and they are made there, where the
/// answer is a `ControlResponseCode.ERROR` rather than a `UriError`.
///
/// # `response_correlation_id`, and why it is written **before** the strip
///
/// A replay the client asked for on a channel of its own carries the id of the
/// image its request arrived on (`ControlSessionAdapter.java:1183`), and
/// `strippedChannelBuilder` copies that parameter along with the rest
/// (`ArchiveConductor.java:1896`) — the id is written onto the client's channel
/// and read back off it by the same line that reads `control-mode` back. So
/// what this does with it is what the reference does with it.
///
/// # Errors
///
/// [`UriError`] when the client's own channel will not parse.
pub fn replay_channel(
    requested: &str,
    replay_position: i64,
    initial_term_id: i32,
    term_buffer_length: i32,
    mtu_length: i32,
    response_correlation_id: Option<i64>,
) -> Result<String, UriError> {
    let uri = ChannelUri::parse(requested)?;
    let mut builder = stripped_channel_builder(&uri);

    if let Some(correlation_id) = response_correlation_id {
        builder.response_correlation_id(correlation_id.to_string());
    }

    // `positionBitsToShift` and a bitmask, which is what the reference's
    // `initialPosition` does (`ChannelUriStringBuilder.java:1635-1641`) — not
    // `Aeron.termId`/`termOffset`'s division and modulo, which some other
    // callers use. The distinction only shows for a term length that is not a
    // power of two, where both sides produce a term id neither would defend;
    // ours shifts by nothing and the reference shifts by `floor(log2)`. No
    // recording the archive can hold has such a length — the driver refuses
    // one, and `ReplaySession::new` refuses it again — so this is recorded and
    // not repaired.
    let bits_to_shift = bits_to_shift(term_buffer_length).unwrap_or(0);
    let term_id = i32::try_from(replay_position >> bits_to_shift)
        .unwrap_or(0)
        .wrapping_add(initial_term_id);
    let term_offset =
        i32::try_from(replay_position & i64::from(term_buffer_length - 1)).unwrap_or(0);

    builder
        .term_length(term_buffer_length)
        .initial_term_id(initial_term_id)
        .term_id(term_id)
        .term_offset(term_offset)
        .sparse(flag(&uri, "sparse"))
        .eos(flag(&uri, "eos"))
        .mtu(mtu_length);

    // `linger` is the client's when it named one, and the driver's own default
    // when it did not. The reference falls back to
    // `aeron.archive.replay.linger.timeout`, whose own default **is** the
    // driver's publication linger timeout — and neither is read here yet
    // (P2-9c), so leaving it out lands on the same number.
    // `Long.parseLong(lingerValue)` (`:900-901`), a bare number of nanoseconds.
    if let Some(linger_ns) = uri
        .get("linger")
        .and_then(|value| value.parse::<i64>().ok())
    {
        builder.linger_ns(linger_ns);
    }

    Ok(builder.build())
}

/// A URI flag, as the reference's `ChannelUri.getBoolean` reads one.
fn flag(uri: &ChannelUri, name: &str) -> bool {
    uri.get(name).is_some_and(|value| value == "true")
}

/// A random `i64` for a replay token.
///
/// The reference draws from `ctx.secureRandom()` (`ArchiveConductor.java:207`),
/// a `SecureRandom` — and here the choice of source is not the formality it is
/// for a session id. A replay token is a **capability**: the request that spends
/// it arrives on an image its session was never opened on and is let through
/// without the image comparison or the authorisation service
/// (`ControlSessionAdapter.java:1175-1184`), so a token that can be predicted is
/// a replay of somebody else's recording.
///
/// `/dev/urandom` is the source the driver's own `sys::random_i32` reads
/// (`crates/driver/src/sys.rs:375-390`), and it is read the same way. The
/// fallback is **weaker than that one's**, and deliberately says so: a clock, a
/// process id and a counter are guessable by anyone who knows the archive is
/// running, so an archive that cannot read the device issues tokens that hold
/// against a client that is not trying and against nothing else.
fn random_i64() -> i64 {
    let mut bytes = [0u8; 8];
    let read = std::fs::File::open("/dev/urandom")
        .and_then(|mut file| std::io::Read::read_exact(&mut file, &mut bytes));

    match read {
        Ok(()) => i64::from_ne_bytes(bytes),
        Err(_) => {
            static DRAWS: AtomicU64 = AtomicU64::new(0);

            let draws = DRAWS.fetch_add(1, Ordering::Relaxed);
            let mixed = (deepmsg_core::clock::epoch_nano_time() as u64)
                ^ draws.wrapping_mul(0x9E37_79B9_7F4A_7C15)
                ^ u64::from(std::process::id());

            // A splitmix64 finaliser, so that the low bits are not the clock's
            // low bits — which is what a naive reader would guess first.
            let mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            let mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);

            (mixed ^ (mixed >> 31)) as i64
        }
    }
}

/// One replay in flight, as the conductor keeps it.
///
/// The reference holds the `ReplaySession` in `replaySessionByIdMap` and reads
/// its ids and state back off the object. The session here is an owned value
/// with the same accessors, so what this adds is the two ids that belong to the
/// **conversation** rather than to the replay: which control session asked, and
/// under what correlation id — because a replay's OK and its errors are sent to
/// the client that asked, several turns after it asked, and the session knows
/// nothing about the control plane.
struct ReplayEntry {
    /// The control session that asked for it.
    control_session_id: i64,
    /// The request being answered.
    correlation_id: i64,
    /// The publication the frames go out on, which the conductor gives back when
    /// the replay ends.
    registration_id: i64,
    /// Whether the OK has gone out.
    ///
    /// It goes out **once**, on the turn the segment opens and its header checks
    /// out (`ReplaySession.java:338`) — which is before the publication is
    /// waited for, so this is a fact about the conversation and not something
    /// the session can be asked.
    started: bool,
    /// The bounded limit counter, when the request named one, which is what
    /// [`Sessions::drive_replay_sessions`] reads each turn in place of the live
    /// recording's position.
    limit: Option<LimitCounter>,
    /// Where the recording began, which the live-position fallback needs and a
    /// session does not keep.
    start_position: i64,
    /// The replay itself.
    session: ReplaySession,
}

impl ReplayEntry {
    /// The id the client's OK carried, which is the map's key too.
    fn session_id(&self) -> i64 {
        self.session.replay_session_id()
    }
}

/// `ArchiveConductor.SessionForReplay` (`:2793-2805`): what a replay token
/// stands for.
///
/// The reference keeps the `ControlSession` **object**; this keeps its id,
/// because the sessions live with the same struct the tokens do and an id is
/// what every other map here is keyed by.
struct ReplayToken {
    /// The recording the token was issued for, which the replay that spends it
    /// must name too (`ArchiveConductor.java:2659`).
    recording_id: i64,
    /// The session the token was issued to, which is the session the replay is
    /// answered on.
    session_id: i64,
    /// When it stops being usable, in the conductor's millisecond clock
    /// (`:2648`).
    deadline_ms: i64,
}

impl Sessions {
    /// A session store for one archive.
    #[allow(clippy::too_many_arguments)] // one per setting the archive was configured with
    pub fn new(
        archive_id: i64,
        connect_timeout_ms: i64,
        liveness_check_interval_ms: i64,
        command_timeout: Duration,
        response_channel_defaults: ResponseChannelDefaults,
        recording: RecordingSettings,
        replay: ReplaySettings,
        subscription_ids: Vec<i64>,
    ) -> Self {
        Self {
            archive_id,
            next_session_id: FIRST_SESSION_ID,
            connect_timeout_ms,
            liveness_check_interval_ms,
            command_timeout,
            response_channel_defaults,
            recording,
            replay,
            subscription_ids,
            sessions: HashMap::new(),
            pending_connects: Vec::new(),
            pending: Vec::new(),
            recording_subscriptions: HashMap::new(),
            subscription_ref_counts: HashMap::new(),
            num_active_recordings: 0,
            pending_starts: Vec::new(),
            pending_removals: Vec::new(),
            recording_session_by_id: HashMap::new(),
            pending_recordings: Vec::new(),
            recording_session_counter_registration_id: None,
            recording_session_counter: None,
            listings: Vec::new(),
            ended: Vec::new(),
            session_count_delta: 0,
            session_counter_registration_id: None,
            control_sessions: None,
            warnings: Vec::new(),
            next_replay_id: 1,
            num_active_replays: 0,
            replay_sessions: HashMap::new(),
            creating_replays: Vec::new(),
            replayer: Replayer::new(archive_id),
            replay_tokens: HashMap::new(),
        }
    }

    /// How many sessions are live.
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Whether there are no live sessions.
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// Where a session's image has read up to, for the rejection that ends an
    /// aborted session's image (`Image.reject` rejects at the image's own
    /// position).
    ///
    /// `None` when the image is no longer held, which is also when there is
    /// nothing left to reject.
    pub fn image_position(&self, client: &Client, image: ImageId) -> Option<i64> {
        self.find_image(client, image).map(Image::position)
    }

    /// The sessions that finished since this was last called.
    pub fn take_ended(&mut self) -> Vec<EndedSession> {
        std::mem::take(&mut self.ended)
    }

    /// The warnings raised since this was last called.
    pub fn take_warnings(&mut self) -> Vec<String> {
        std::mem::take(&mut self.warnings)
    }

    /// One turn of the control plane, in the reference's order.
    ///
    /// `SessionWorker.doWork` (`SessionWorker.java:56-81`) walks the sessions
    /// backwards and collects the finished ones; everything before that is the
    /// part the callbacks deferred, and it runs first because the reference's
    /// own `controlSessionAdapter.poll()` precedes `super.doWork()`.
    ///
    /// `counters` is the writable counters region, which only the conductor
    /// holds — hence the argument rather than a field.
    ///
    /// `recorder` is the conductor's, and is lent for the length of the call:
    /// two of the things a control session can ask for — a stop, and a
    /// recording session ending — are things the *recorder* has to be told
    /// about (`ArchiveConductor.java:1766-1772`, `:1356-1359`), and this build
    /// keeps the recorder where the reference keeps it, on the conductor.
    #[allow(clippy::too_many_arguments)] // one per collaborator and one per clock
    pub fn drive(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        authenticator: &mut dyn Authenticator,
        catalog: &mut Catalog,
        recorder: &mut Recorder,
        now_ms: i64,
    ) {
        self.allocate_session_counter(client, counters);
        self.allocate_recording_session_counter(client, counters);
        self.replayer
            .allocate(client, counters, self.command_timeout);
        // Expired replay tokens are swept before anything is asked of them,
        // which is where the reference sweeps them: right after the adapter has
        // read the turn's requests (`ArchiveConductor.java:389-391`).
        self.check_replay_tokens(now_ms);
        self.claim_pending_starts(client, now_ms);
        self.collect_pending_removals(client);
        self.run_pending_connects(client, authenticator, now_ms);
        self.run_deferred(client, counters, authenticator, catalog, recorder, now_ms);
        self.start_recordings(client, catalog, now_ms);
        self.claim_recordings(client, catalog, counters, recorder, now_ms);
        self.drive_listings(client, catalog, now_ms);
        // The replays get their turn after the listings and before the control
        // sessions are swept, which is where the reference's `SessionWorker`
        // leaves them: everything a turn *does* happens before the sessions that
        // asked for it are looked at again.
        self.drive_replays(client, counters, catalog, now_ms);
        self.drive_sessions(client, counters, authenticator, now_ms);
        self.apply_session_count_delta(counters);
    }

    /// The recording-session counter (111), asked for once and taken up when
    /// the driver answers (`Archive.java:1565-1574`).
    ///
    /// **Not** [`ArchiveIdCounter::allocate`], which blocks, for the reason
    /// [`Sessions::allocate_session_counter`] gives: this runs inside the turn
    /// that drives the driver, so a command that waits for an answer waits for
    /// a driver nobody is driving.
    fn allocate_recording_session_counter(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
    ) {
        if self.recording_session_counter.is_some() {
            return;
        }

        let Some(registration_id) = self.recording_session_counter_registration_id else {
            match request_archive_id_counter(
                client,
                ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID,
                RECORDING_SESSIONS_NAME,
                self.archive_id,
                self.command_timeout,
            ) {
                Ok(registration_id) => {
                    self.recording_session_counter_registration_id = Some(registration_id);
                }
                Err(error) => self.warnings.push(format!(
                    "could not ask for the archive recording sessions counter: {error}"
                )),
            }
            return;
        };

        match claim_archive_id_counter(
            client,
            counters,
            ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID,
            registration_id,
        ) {
            Ok(Some(counter)) => {
                self.recording_session_counter = Some(counter);
                self.recording_session_counter_registration_id = None;
            }
            Ok(None) => {}
            Err(error) => {
                // A refusal can never become a counter, so the archive runs on
                // without one rather than asking forever.
                self.recording_session_counter_registration_id = None;
                self.warnings.push(format!(
                    "could not allocate the archive recording sessions counter: {error}"
                ));
            }
        }
    }

    /// Answer the images that arrived with a recording row and a position
    /// counter (`ArchiveConductor.startRecordingSession`, `:1991-2057`, up to
    /// the counter).
    ///
    /// This is the one place the plan calls "the same turn", and it is why this
    /// runs right after `client.poll()` and before anything else that could
    /// take a turn: an image is announced at a **join position**, and a
    /// recording session made a turn later would still start there — but a
    /// *subscription* the archive has stopped serving would not have a session
    /// made for it at all, so the events are drained every turn whether or not
    /// anything is waiting for them.
    ///
    /// An image whose subscription this archive does not record is not an
    /// error: the client is shared (the archive's own control subscriptions
    /// live in it too) and an image for something else is simply not ours.
    fn start_recordings(&mut self, client: &mut Client, catalog: &mut Catalog, now_ms: i64) {
        for event in client.image_events() {
            let ImageEvent::Available {
                subscription_registration_id,
                publication_registration_id,
                ..
            } = event
            else {
                // An image going away is the recording session's to notice: it
                // asks for its image every turn and records the `None`
                // (`recording_session.rs`, the module note on `isClosed`).
                continue;
            };

            let Some(subscription) =
                self.subscription_by_registration_id(subscription_registration_id)
            else {
                continue;
            };

            // The image itself, for the five things only it knows: the catalog
            // row keeps every one of them (`:1999-2019`).
            // Everything the image says about itself, read out **before**
            // anything below needs the client mutably: an image is borrowed
            // from the client it came out of, and the decisions this method
            // makes are about to call it.
            let facts = match client
                .subscription(subscription_registration_id)
                .and_then(|subscription| subscription.image(publication_registration_id))
            {
                Some(image) => ImageFacts {
                    session_id: image.session_id(),
                    stream_id: image.stream_id(),
                    join_position: image.join_position(),
                    initial_term_id: image.initial_term_id(),
                    term_buffer_length: image.term_buffer_length(),
                    mtu_length: image.mtu_length().unwrap_or(0),
                    source_identity: image.source_identity().to_owned(),
                },
                None => continue,
            };

            let start_position = facts.join_position;
            let term_buffer_length = facts.term_buffer_length;

            // What the row says and which recording the bytes belong to: a start
            // writes a new one, an extend appends to the one it named (`:2059-2137`).
            let (recording_id, session_start_position, segment_file_length) = match subscription
                .request
            {
                RecordingRequest::Start => {
                    let segment_file_length =
                        segment_file_length(self.recording.segment_file_length, term_buffer_length);

                    let recording = Recording {
                        // The catalog allocates the id and writes it over this
                        // (`Catalog.addNewRecording`), which is why the field is
                        // not read here.
                        recording_id: 0,
                        start_timestamp: now_ms,
                        stop_timestamp: NULL_VALUE,
                        start_position,
                        stop_position: NULL_VALUE,
                        initial_term_id: facts.initial_term_id,
                        segment_file_length,
                        term_buffer_length,
                        mtu_length: facts.mtu_length,
                        session_id: facts.session_id,
                        stream_id: facts.stream_id,
                        stripped_channel: subscription.stripped_channel.clone(),
                        original_channel: subscription.original_channel.clone(),
                        source_identity: facts.source_identity.clone(),
                    };

                    match catalog.add_recording(&recording) {
                        Ok(recording_id) => (
                            recording_id,
                            start_position,
                            usize::try_from(segment_file_length).unwrap_or(0),
                        ),
                        Err(error) => {
                            self.warnings
                                .push(format!("could not write a recording row: {error}"));
                            continue;
                        }
                    }
                }
                RecordingRequest::Extend { recording_id } => {
                    let segment_file_length = catalog
                        .recording(recording_id)
                        .map(|summary| summary.segment_file_length)
                        .unwrap_or(i32::MAX);

                    match self.validate_extension(
                        client,
                        catalog,
                        subscription.session_id,
                        subscription.correlation_id,
                        recording_id,
                        &subscription,
                        &facts,
                        now_ms,
                    ) {
                        Some(session_start_position) => (
                            recording_id,
                            // An extend's writer starts where the **recording**
                            // started (`:2109`), not where this image joined:
                            // the bytes go into the same segments.
                            session_start_position,
                            usize::try_from(segment_file_length).unwrap_or(0),
                        ),
                        None => continue,
                    }
                }
            };

            // `RecordingPos.allocate` (`:2021-2030`) is synchronous in the
            // reference; here it is asked for now and taken up by
            // `claim_recordings`.
            match RecordingPos::request(
                client,
                self.archive_id,
                recording_id,
                facts.session_id,
                facts.stream_id,
                &subscription.stripped_channel,
                &facts.source_identity,
                self.command_timeout,
            ) {
                Ok(position) => self.pending_recordings.push(PendingRecording {
                    session_id: subscription.session_id,
                    correlation_id: subscription.correlation_id,
                    recording_id,
                    start_position,
                    session_start_position,
                    segment_file_length,
                    request: subscription.request,
                    image_session_id: facts.session_id,
                    subscription_id: subscription_registration_id,
                    image_id: publication_registration_id,
                    term_buffer_length,
                    is_auto_stop: subscription.is_auto_stop,
                    position,
                }),
                Err(error) => self
                    .warnings
                    .push(format!("could not ask for a recording position: {error}")),
            }
        }
    }

    /// Take up the position counters the driver has allocated, and make the
    /// sessions they belong to (`ArchiveConductor.java:2032-2057`).
    ///
    /// This is the second half of [`Sessions::start_recordings`], one turn
    /// later: the reference's `RecordingPos.allocate` waits for the driver
    /// (`RecordingPos.java:130-131`) and this build's cannot, so the counter is
    /// asked for where the image arrives and taken up here. What the delay
    /// costs is a counter that appears a turn after the row — which the C
    /// harness's own `while` loop over it accommodates
    /// (`aeron_archive_test.cpp:275-286`).
    ///
    /// A counter the driver **refuses** is not a counter that will arrive: the
    /// recording is dropped with a warning and its row stays as one that never
    /// recorded, which is what the reference's `refreshAndFixDescriptor` is
    /// there to repair later.
    fn claim_recordings(
        &mut self,
        client: &mut Client,
        catalog: &mut Catalog,
        counters: &CountersReader<'_, ReadWrite>,
        recorder: &mut Recorder,
        now_ms: i64,
    ) {
        let mut index = 0;
        while index < self.pending_recordings.len() {
            match self.pending_recordings[index]
                .position
                .claim(client, counters)
            {
                Ok(true) => {
                    let pending = self.pending_recordings.swap_remove(index);
                    let session = self.finish_recording(client, catalog, counters, pending, now_ms);

                    recorder.add_session(session);
                }
                Ok(false) => index += 1,
                Err(error) => {
                    let pending = self.pending_recordings.swap_remove(index);
                    self.warnings.push(format!(
                        "could not take up the recording position for {}: {error}",
                        pending.recording_id
                    ));
                }
            }
        }
    }

    /// The rest of `startRecordingSession` (`:2029-2057`), from the counter
    /// this session has in hand.
    fn finish_recording(
        &mut self,
        client: &mut Client,
        catalog: &mut Catalog,
        counters: &CountersReader<'_, ReadWrite>,
        pending: PendingRecording,
        now_ms: i64,
    ) -> RecordingSession {
        let PendingRecording {
            session_id,
            correlation_id,
            recording_id,
            start_position,
            session_start_position,
            segment_file_length,
            request,
            image_session_id,
            subscription_id,
            image_id,
            term_buffer_length,
            is_auto_stop,
            position,
        } = pending;

        // `position.setRelease(startPosition)` (`:2029-2030`): the counter says
        // where the recording is before it has read anything, which is what
        // makes `waitUntilCaughtUp` a question about the recording and not about
        // the writer.
        position.set_position(counters, start_position);

        let session = RecordingSession::new(
            session_id,
            correlation_id,
            recording_id,
            // Where the **recording** starts, which for an extend is where it
            // started the first time (`:2109`) and for a start is where the image
            // joined (`:2005`) — the caller decided which.
            session_start_position,
            // And where this **session** joins it, which is where the image is
            // (`:2005`, `:2109`: the same number on both paths).
            start_position,
            segment_file_length,
            subscription_id,
            image_id,
            term_buffer_length,
            self.recording.file_io_max_length,
            self.recording.file_sync_level,
            is_auto_stop,
            position,
            &self.recording.archive_dir,
            // `aeron.archive.record.checksum` is not read yet (P2-9c), and the
            // reference's own default is no checksum at all
            // (`Archive.java:3666-3674`: the property is unset unless a
            // deployment sets it).
            None,
        );

        // What the two requests differ in from here: an extend writes itself
        // into the row (`:2120`) and tells the client `EXTEND` (`:2121-2122`)
        // where a start writes nothing more and tells it `START` (`:2046-2051`).
        let signal = match request {
            RecordingRequest::Start => RecordingSignal::START,
            RecordingRequest::Extend { .. } => {
                if let Err(error) = catalog.extend_recording(
                    recording_id,
                    session_id,
                    correlation_id,
                    image_session_id,
                ) {
                    self.warnings.push(format!(
                        "could not write the extension of {recording_id}: {error}"
                    ));
                }

                RecordingSignal::EXTEND
            }
        };

        if let Some(entry) = self.sessions.get_mut(&session_id) {
            entry.control.send_signal(
                correlation_id,
                recording_id,
                subscription_id,
                start_position,
                signal,
                now_ms,
                client,
            );
        }

        // `subscriptionRefCountMap.incrementAndGet`, `recordingSessionByIdMap
        // .put` and `ctx.recordingSessionCounter().incrementRelease()`
        // (`:2053-2056`) — and **not** `numActiveRecordings`, which the
        // reference moves at the subscription add (`:569`) and nowhere else.
        // Moving it here as well would count every recording twice and never
        // give the second one back: the count would climb to the configured
        // maximum and stay there, refusing every later recording and extension.
        *self
            .subscription_ref_counts
            .entry(subscription_id)
            .or_insert(0) += 1;

        if let Some(counter) = &self.recording_session_counter {
            counter.increment(counters);
        }

        // `recordingSessionByIdMap.put(recordingId, session)` (`:2054`), which
        // is what the two position questions read while this recording is live.
        self.recording_session_by_id.insert(
            recording_id,
            RecordingHandle {
                position,
                subscription_id,
            },
        );

        session
    }

    /// `ArchiveConductor.closeRecordingSession` (`:1329-1363`): everything the
    /// conductor does about a recording that has ended.
    ///
    /// The session has already closed its own writer — that is what `STOPPED`
    /// means — so what is left is what needs the catalog, the control session
    /// and the registry, which is why this is here and not on the recorder.
    pub fn close_recording_session(
        &mut self,
        client: &mut Client,
        catalog: &mut Catalog,
        counters: &CountersReader<'_, ReadWrite>,
        recorder: &mut Recorder,
        mut session: RecordingSession,
        now_ms: i64,
    ) {
        let recording_id = session.recording_id();
        let subscription_id = session.subscription_id();
        let position = session.recorded_position(counters).unwrap_or(NULL_POSITION);

        // `if (!isAbort)`: the reference writes nothing when the archive itself
        // is going down. This build has no such path yet — a session is only
        // ever collected while the archive is running — so the guard has
        // nothing to test, and the two writes below are unconditional.
        if let Err(error) = catalog.recording_stopped(recording_id, position, now_ms) {
            self.warnings.push(format!(
                "could not write the stop position of {recording_id}: {error}"
            ));
        }

        // `session.sendPendingError()` (`:1346`), so a client whose recording
        // never started hears why before it hears that it stopped.
        if let Some(message) = session.error_message() {
            let message = message.to_owned();
            self.send_error(
                client,
                session.session_id(),
                session.correlation_id(),
                0,
                &message,
                now_ms,
            );
        }

        if let Some(entry) = self.sessions.get_mut(&session.session_id()) {
            entry.control.send_signal(
                session.correlation_id(),
                recording_id,
                subscription_id,
                position,
                RecordingSignal::STOP,
                now_ms,
                client,
            );
        }

        // `if (subscriptionRefCountMap.decrementAndGet(subscriptionId) <= 0 ||
        // session.isAutoStop())` (`:1357-1360`): `<= 0` and not `== 0`, because
        // the two paths that move this count both move it.
        let remaining = self
            .subscription_ref_counts
            .get_mut(&subscription_id)
            .map(|count| {
                *count -= 1;
                *count
            })
            .unwrap_or(0);

        if remaining <= 0 || session.is_auto_stop() {
            self.close_and_remove_recording_subscription(
                client,
                recorder,
                subscription_id,
                "close recording session",
            );
        }

        session.close(client);

        // `recordingSessionByIdMap.remove(recordingId)` (`:1360`): from here the
        // two position questions fall back to the catalog again.
        self.recording_session_by_id.remove(&recording_id);

        self.num_active_recordings = self.num_active_recordings.saturating_sub(1);

        if let Some(counter) = &self.recording_session_counter {
            counter.decrement(counters);
        }
    }

    /// `ArchiveConductor.closeAndRemoveRecordingSubscription` (`:2580-2595`):
    /// the subscription is given up altogether, along with every session
    /// reading it.
    fn close_and_remove_recording_subscription(
        &mut self,
        client: &mut Client,
        recorder: &mut Recorder,
        subscription_id: i64,
        reason: &str,
    ) {
        self.subscription_ref_counts.remove(&subscription_id);
        recorder.abort_sessions_for(subscription_id, reason);
        self.remove_recording_subscription(subscription_id);
        self.release_recording_subscription(client, subscription_id);
    }

    /// Carry out what the callbacks could not (`ArchiveConductor.java:562`,
    /// `:1766-1780`).
    /// `ArchiveConductor.startReplay` (`:764-929`), everything up to the answer.
    ///
    /// The answer **waits**: a replay's OK carries the `replaySessionId`, and
    /// that cannot be minted until a publication exists to take its low half
    /// from. So what this does is refuse what it can, build the channel, and
    /// hand the rest to a [`CreateReplayPublicationSession`].
    fn start_replay(
        &mut self,
        counters: &CountersReader<'_, ReadWrite>,
        catalog: &Catalog,
        request: &StartReplayRequest,
    ) {
        let session_id = request.session_id;
        let correlation_id = request.correlation_id;

        // `:775-780`. `==` and not `>=`, which is the reference's own test — it
        // is only the same thing while nothing can push the count past the
        // bound, and two movements keep it from doing that.
        if self.num_active_replays == self.replay.max_concurrent_replays {
            self.pending.push(Deferred::Error {
                session_id,
                correlation_id,
                relevant_id: MAX_REPLAYS,
                message: format!(
                    "max concurrent replays reached {}",
                    self.replay.max_concurrent_replays
                ),
            });
            return;
        }

        // `startBoundedReplay` (`:996-1035`), which runs **before** the replay
        // proper and hands it a counter rather than a number.
        //
        // The refusal's **prefix** is the reference's own (`:1018-1023`); its
        // reason is not. The reference wraps `new Counter(countersReader, id)`
        // in a `try` and appends `ex.getMessage()`, which for a missing slot is
        // whatever the counter's own lookup threw; this build's lookup answers
        // an `Option` and the reason is spelled out instead.
        let limit = match request.limit_counter_id {
            Some(limit_counter_id) => match resolve_limit_counter(counters, limit_counter_id) {
                Some(limit) => Some(limit),
                None => {
                    self.refuse_replay(
                        session_id,
                        correlation_id,
                        0,
                        format!(
                            "unable to create replay limit counter id= {limit_counter_id} \
                             because of: no such counter is allocated"
                        ),
                    );
                    return;
                }
            },
            None => None,
        };

        let Ok(recording) = catalog.recording(request.recording_id) else {
            self.refuse_replay(
                session_id,
                correlation_id,
                UNKNOWN_RECORDING,
                unknown_recording_message(request.recording_id),
            );
            return;
        };

        let start_position = recording.start_position;

        // A null position is the recording's own beginning — and, more to the
        // point here, the checks below are **skipped** for it (`:805-814`):
        // `isInvalidReplayPosition` is called only when the client named a
        // position. That is not a formality. An empty recording that has stopped
        // has `startPosition == stopPosition`, so a null position resolved to the
        // start and then checked would be refused as past the stop, and
        // `shouldExitOnEmptyRecording` would be told `INVALID_POSITION` where the
        // reference tells it `EMPTY_RECORDING`.
        let replay_position = if request.position == NULL_VALUE {
            start_position
        } else {
            if let Some((relevant_id, message)) =
                invalid_replay_position(request.recording_id, request.position, &recording)
            {
                self.refuse_replay(session_id, correlation_id, relevant_id, message);
                return;
            }

            request.position
        };

        // `:816-819`: the block a replay reads at a time has to be able to hold
        // one of this recording's frames.
        if request.file_io_max_length > 0 && request.file_io_max_length < recording.mtu_length {
            self.refuse_replay(
                session_id,
                correlation_id,
                0,
                format!(
                    "fileIoMaxLength={} < mtuLength={}",
                    request.file_io_max_length, recording.mtu_length
                ),
            );
            return;
        }

        // `:821-829`: and so has the archive's **own** replay buffer, which is
        // what a request that named no length is read into
        // (`aeron.archive.file.io.max.length`, 1 MiB by default). An archive
        // configured below its recordings' MTUs refuses a replay rather than
        // reading blocks that cannot hold a frame.
        let archive_buffer = self.recording.file_io_max_length;
        let mtu_length = usize::try_from(recording.mtu_length).unwrap_or(0);

        if archive_buffer < mtu_length {
            self.refuse_replay(
                session_id,
                correlation_id,
                0,
                format!(
                    "replayBufferCapacity={archive_buffer} < mtuLength={}",
                    recording.mtu_length
                ),
            );
            return;
        }

        // `:789-802`: with no limit counter named, the limit is the **live
        // recording's own position**.
        //
        // This is the step that makes a replay of a recording that has not
        // stopped work at all, and it does not look like one. The reference
        // reaches for `recordingSessionByIdMap.get(recordingId).recordingPosition()`
        // in six lines that read as an optimisation and are load-bearing: with
        // them, a live recording's `stopPosition` is where it has got to and
        // `maxLength` is unbounded, so `-1` follows it and `-2` stops at it.
        // Without them the catalog's `NULL_VALUE` makes both lengths negative.
        // A recording that has **stopped** has no session, and the catalog's own
        // stop position is the limit — which is the branch a stopped recording
        // has always taken.
        //
        // [`Sessions::live_recording_position`] is where the two states this
        // build has and the reference does not are dealt with.
        let limit_position = match limit {
            Some(limit) => read_limit_counter(counters, limit),
            None => self.live_recording_position(counters, request.recording_id, start_position),
        };

        let (stop_position, max_length) = match limit_position {
            Some(limit) => {
                if replay_position > limit {
                    self.refuse_replay(
                        session_id,
                        correlation_id,
                        INVALID_POSITION,
                        exceeds_limit_message(replay_position, limit, request.recording_id),
                    );
                    return;
                }

                (limit, i64::MAX - replay_position)
            }
            None => {
                let stop = recording.stop_position;

                (stop, stop - replay_position)
            }
        };

        // The three branches (`:851-869`), and both sentinels are the client's
        // own. A recording that has not stopped has `NULL_VALUE` for its stop,
        // which makes `maxLength` negative and lands on the refusal below — the
        // reference does the same, because a replay that means to follow a live
        // recording says so with a **limit counter**
        // (`BoundedReplayRequest`, template 18).
        let replay_length = if request.length == REPLAY_ALL_AND_FOLLOW {
            max_length
        } else if request.length == REPLAY_ALL_AND_STOP {
            let to_stop = stop_position - replay_position;

            if to_stop == 0 {
                // **The deviation, recorded.** The reference sends this error
                // and then **falls through** (`:855-867` has no `return`), so it
                // goes on to create the publication with `replayLength == 0` and
                // that session's OK carrying a `replaySessionId` is offered for
                // the same correlation id the error just answered. One request,
                // two responses. Neither client can tell — `pollForResponse`
                // takes the first match, and the error is first — and no C case
                // asserts the count, so this is a missing `return` upstream
                // rather than a behaviour. This build refuses and stops.
                self.refuse_replay(
                    session_id,
                    correlation_id,
                    EMPTY_RECORDING,
                    format!(
                        "when replaying and stopping the replay length must be non-zero, \
                         recordingId={}",
                        request.recording_id
                    ),
                );
                return;
            }

            to_stop
        } else {
            request.length.min(max_length)
        };

        if replay_length < 0 {
            self.refuse_replay(
                session_id,
                correlation_id,
                0,
                format!(
                    "replay length must be positive: replayLength={replay_length}, length={}, \
                     stopPosition={stop_position}, replayPosition={replay_position} \
                     for recording {}",
                    request.length, request.recording_id
                ),
            );
            return;
        }

        let channel = match replay_channel(
            &request.replay_channel,
            replay_position,
            recording.initial_term_id,
            recording.term_buffer_length,
            recording.mtu_length,
            request.response_correlation_id,
        ) {
            Ok(channel) => channel,
            Err(error) => {
                self.refuse_replay(
                    session_id,
                    correlation_id,
                    0,
                    format!("replay channel could not be built: {error}"),
                );
                return;
            }
        };

        // `onReplayStart` (`:931-934`), which happens **before** there is a
        // publication — and a creation that fails counts it back out
        // (`CreateReplayPublicationSession.java:152-153`).
        self.num_active_replays += 1;
        self.creating_replays
            .push(CreateReplayPublicationSession::new(Replay {
                control_session_id: session_id,
                correlation_id,
                recording_id: request.recording_id,
                replay_position,
                replay_length,
                start_position,
                stop_position,
                segment_file_length: recording.segment_file_length,
                term_buffer_length: recording.term_buffer_length,
                stream_id: recording.stream_id,
                replay_channel: channel,
                replay_stream_id: request.replay_stream_id,
                file_io_max_length: request.file_io_max_length,
                limit,
            }));
    }

    /// Where a recording the archive is **still writing** has got to, as
    /// `startReplay` needs it (`:790-796`).
    ///
    /// `None` for a recording that has stopped, or that this archive is not
    /// recording — which is the branch that reads the catalog's own stop
    /// position, and the only branch a stopped recording has ever taken.
    ///
    /// # The two ways a live recording has no readable counter
    ///
    /// The reference reaches a `Counter` object and reads it, because its
    /// `RecordingPos.allocate` waits for the driver inside the turn
    /// (`RecordingPos.java:130-131`). This build asks for the counter on one
    /// turn and reads its slot on a later one, so there are two states the
    /// reference does not have:
    ///
    /// * a [`PendingRecording`] — asked for, not yet answered. The recording is
    ///   live and the counter is on its way;
    /// * a handle whose `counter_id` is not set yet — the same window, one turn
    ///   further along.
    ///
    /// Both answer with the **join position**, which is what a recording
    /// position counter is set to before anything is read (`:2029-2030`), and
    /// both are windows a request has to arrive inside to see at all. A live
    /// recording that has been read from cannot be in either.
    fn live_recording_position<Access>(
        &self,
        counters: &CountersReader<'_, Access>,
        recording_id: i64,
        start_position: i64,
    ) -> Option<i64> {
        if let Some(handle) = self.recording_session_by_id.get(&recording_id) {
            return Some(handle.position.value(counters).unwrap_or(start_position));
        }

        self.pending_recordings
            .iter()
            .any(|pending| pending.recording_id == recording_id)
            .then_some(start_position)
    }

    /// `ArchiveConductor.stopReplay` (`:1037-1054`).
    ///
    /// **Always an OK, even when there is no such replay** — the reference sends
    /// one unconditionally and lets the abort be asynchronous, so a client that
    /// stops a replay twice is told OK twice.
    fn stop_replay(&mut self, replay_session_id: i64, session_id: i64, correlation_id: i64) {
        if let Some(entry) = self.replay_sessions.get_mut(&replay_session_id) {
            entry.session.abort();
        }

        self.pending.push(Deferred::Ok {
            session_id,
            correlation_id,
            relevant_id: 0,
        });
    }

    /// `ArchiveConductor.stopAllReplays` (`:1056-1064`): every replay, or every
    /// replay **of one recording** — `NULL_VALUE` for the id means all of them.
    fn stop_all_replays(&mut self, recording_id: i64, session_id: i64, correlation_id: i64) {
        for entry in self.replay_sessions.values_mut() {
            if recording_id == NULL_VALUE || entry.session.recording_id() == recording_id {
                entry.session.abort();
            }
        }

        self.pending.push(Deferred::Ok {
            session_id,
            correlation_id,
            relevant_id: 0,
        });
    }

    /// `ArchiveConductor.onReplayEnd` (`:936-939`).
    fn on_replay_end(&mut self) {
        self.num_active_replays = self.num_active_replays.saturating_sub(1);
    }

    /// `ArchiveConductor.generateReplayToken` (`:2639-2652`): a random `i64`
    /// that names a session and a recording for as long as a connect timeout.
    ///
    /// **Drawn until it is free**, which is the reference's `while` and not
    /// decoration: a token that collides with a live one would hand its session
    /// to a client that never asked for it. Drawing again is the only way to
    /// answer, because the map's key *is* the token.
    ///
    /// The deadline is the reference's — `nanoClock.nanoTime()` plus
    /// `connectTimeoutMs` (`:2648`) — measured in the clock the conductor
    /// already keeps.
    fn generate_replay_token(&mut self, session_id: i64, recording_id: i64, now_ms: i64) -> i64 {
        let mut replay_token = NULL_VALUE;

        while replay_token == NULL_VALUE || self.replay_tokens.contains_key(&replay_token) {
            replay_token = random_i64();
        }

        self.replay_tokens.insert(
            replay_token,
            ReplayToken {
                recording_id,
                session_id,
                deadline_ms: now_ms.saturating_add(self.connect_timeout_ms),
            },
        );

        replay_token
    }

    /// `ArchiveConductor.getReplaySession` (`:2654-2667`).
    ///
    /// Three things must hold and all three are asked here: the token is one
    /// this archive issued, it was issued for **this** recording, and it has
    /// not expired. A token is not spent by being used (`:2654-2667` never
    /// removes one) — it stops working when `check_replay_tokens` sweeps it or
    /// when the session it names goes away.
    fn replay_session_by_token(
        &self,
        replay_token: i64,
        recording_id: i64,
        now_ms: i64,
    ) -> Option<i64> {
        let token = self.replay_tokens.get(&replay_token)?;

        (recording_id == token.recording_id && now_ms < token.deadline_ms)
            .then_some(token.session_id)
    }

    /// `ArchiveConductor.checkReplayTokens` (`:2683-2697`), which the reference
    /// runs once per `doWork` (`:391`).
    ///
    /// Returns how many it removed, which is what the reference adds into its
    /// `workCount`. Nothing here counts work, so the caller drops it — it is
    /// the number the sweep's own test reads, and the alternative is a sweep
    /// whose two halves are asserted apart.
    fn check_replay_tokens(&mut self, now_ms: i64) -> usize {
        let before = self.replay_tokens.len();
        self.replay_tokens
            .retain(|_, token| token.deadline_ms > now_ms);

        before - self.replay_tokens.len()
    }

    /// `removeReplayTokensForSession` (`:2669-2681`), called where a control
    /// session is removed (`ControlSessionAdapter.java:1157`).
    ///
    /// Without it a token outlives the session it names, and a session id is a
    /// number this archive hands out again (`nextSessionId` is a counter) — so
    /// a later session could be answered on by a token issued to an earlier
    /// one.
    fn remove_replay_tokens_for_session(&mut self, session_id: i64) {
        self.replay_tokens
            .retain(|_, token| token.session_id != session_id);
    }

    /// `replaySessionId` (`:956`): the replay's own counter in the high half,
    /// the publication's Aeron session id in the low one.
    ///
    /// `next_replay_id` **starts at 1**, so a `replaySessionId` is never
    /// mistaken for a bare session id, and it is an `i32` that wraps.
    fn mint_replay_session_id(&mut self, publication_session_id: i32) -> i64 {
        let replay_session_id = (i64::from(self.next_replay_id) << 32)
            | (i64::from(publication_session_id) & 0xFFFF_FFFF);
        self.next_replay_id = self.next_replay_id.wrapping_add(1);

        replay_session_id
    }

    /// One turn of the replays: publications still being made, then the ones
    /// that are running.
    fn drive_replays(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        catalog: &Catalog,
        now_ms: i64,
    ) {
        self.claim_creating_replays(client, counters, catalog, now_ms);
        self.drive_replay_sessions(client, counters, now_ms);
    }

    /// Take up the publications that have appeared, and give back the slots of
    /// the ones that will not.
    fn claim_creating_replays(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        catalog: &Catalog,
        now_ms: i64,
    ) {
        for mut creating in std::mem::take(&mut self.creating_replays) {
            match creating.do_work(client) {
                CreationProgress::Idle => self.creating_replays.push(creating),
                CreationProgress::Created { registration_id } => {
                    let replay = creating.replay().clone();
                    let control_session_id = replay.control_session_id;

                    match self.open_replay(client, catalog, &replay, registration_id, now_ms) {
                        Some(entry) => {
                            // **After** the handover succeeds, not before it:
                            // see [`CreateReplayPublicationSession::hand_over`]
                            // for what clearing it any earlier strands.
                            creating.hand_over();
                            // `newReplaySession`'s
                            // `ctx.replaySessionCounter().incrementRelease()`
                            // (`:993`).
                            self.replayer.session_opened(counters);
                            self.replay_sessions.insert(entry.session_id(), entry);
                        }
                        None => {
                            // The recording went away between the check and the
                            // publication: give it back to the driver and say so.
                            creating.close(client);
                            self.on_replay_end();
                            self.refuse_replay(
                                control_session_id,
                                replay.correlation_id,
                                UNKNOWN_RECORDING,
                                unknown_recording_message(replay.recording_id),
                            );
                        }
                    }
                }
                CreationProgress::Failed { code, message } => {
                    // `:604`: the `numActiveReplays++` that `startReplay` did is
                    // taken back here, and **only** here — the session never
                    // existed, so nothing else will count it down.
                    creating.close(client);
                    self.on_replay_end();
                    self.refuse_replay(
                        creating.replay().control_session_id,
                        creating.replay().correlation_id,
                        i64::from(code),
                        message,
                    );
                }
            }
        }
    }

    /// `ArchiveConductor.newReplaySession` (`:941-994`), as far as this build
    /// can go without the client: the session is built against the publication
    /// and put in the map.
    fn open_replay(
        &mut self,
        client: &mut Client,
        catalog: &Catalog,
        replay: &Replay,
        registration_id: i64,
        now_ms: i64,
    ) -> Option<ReplayEntry> {
        let recording = catalog.recording(replay.recording_id).ok()?;
        let facts = ReplayPublications::facts(client, registration_id)?;
        let replay_session_id = self.mint_replay_session_id(facts.session_id);

        // `ArchiveConductor.newReplaySession` (`:956-966`): the request's block
        // size when it named one **and it is smaller than the archive's**, and
        // the archive's own otherwise.
        //
        // The archive's is `ctx.replayBuffer().capacity()`, and that buffer is
        // allocated once, lazily, at `ctx.fileIoMaxLength()`
        // (`Archive.java:3717-3720`) — `aeron.archive.file.io.max.length`,
        // 1 MiB by default. **A first reading of this got it wrong**: the term
        // buffer of the recording is not what bounds a replay's read here, and
        // taking it for that made every session allocate up to 16 MiB where the
        // reference allocates nothing past its one buffer.
        //
        // **The deviation, recorded**: the reference has exactly **one** buffer
        // for the whole archive and every session slices it
        // (`new UnsafeBuffer(ctx.replayBuffer(), 0, fileIoMaxLength)`), because
        // a buffer lives only inside one `replay()` call. This build gives each
        // session its own, which is the same size and the same behaviour and
        // twenty times the memory at the default bound. Sharing one would mean
        // a session borrowing the archive's, and the sessions are owned values
        // in a map.
        let archive_buffer = self.recording.file_io_max_length;
        let requested = usize::try_from(replay.file_io_max_length).unwrap_or(0);
        let buffer_capacity = if requested > 0 && requested < archive_buffer {
            requested
        } else {
            archive_buffer
        };

        // `connectDeadlineMs = epochClock.time() + connectTimeoutMs`
        // (`ReplaySession.java:166`): the publication has this long to connect,
        // and the segment file this long to appear.
        let deadline_ns = now_ms
            .saturating_add(self.connect_timeout_ms)
            .saturating_mul(1_000_000);

        let session = ReplaySession::new(
            replay.recording_id,
            replay_session_id,
            SegmentSummary::from(&recording),
            replay.replay_position,
            replay.replay_length,
            replay.start_position,
            replay.stop_position,
            &self.recording.archive_dir,
            buffer_capacity,
            None,
            u64::try_from(deadline_ns).unwrap_or(u64::MAX),
        )
        .ok()?;

        Some(ReplayEntry {
            control_session_id: replay.control_session_id,
            correlation_id: replay.correlation_id,
            registration_id,
            limit: replay.limit,
            start_position: replay.start_position,
            started: false,
            session,
        })
    }

    /// One turn of every replay in flight.
    fn drive_replay_sessions(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        now_ms: i64,
    ) {
        let now_ns = u64::try_from(now_ms.saturating_mul(1_000_000)).unwrap_or(u64::MAX);
        let mut owed: Vec<Deferred> = Vec::new();
        let mut finished: Vec<i64> = Vec::new();
        // `Replayer.doWork`'s `workCount` (`ArchiveConductor.java:2779-2790`):
        // what decides whether the three read statistics are published this
        // turn. A session that did something and a session that was closed both
        // count, which is what the reference's `SessionWorker` counts too.
        let mut work = 0;

        // `notExtended` (`ReplaySession.java:544-578`), read out before the
        // sessions are walked because it is the conductor that can see a
        // recording's position and a session cannot.
        let limits: Vec<(i64, LimitUpdate)> = self
            .replay_sessions
            .iter()
            .filter_map(|(replay_session_id, entry)| {
                let update = match entry.limit {
                    Some(limit) => match read_limit_counter(counters, limit) {
                        Some(position) => LimitUpdate::ExtendTo(position),
                        // Closed, or its slot taken by another counter — this
                        // build reads both as absent, and the reference answers
                        // both by freezing (see [`read_limit_counter`]).
                        None => LimitUpdate::Gone,
                    },
                    None => LimitUpdate::ExtendTo(self.live_recording_position(
                        counters,
                        entry.session.recording_id(),
                        entry.start_position,
                    )?),
                };

                Some((*replay_session_id, update))
            })
            .collect();

        for (replay_session_id, entry) in &mut self.replay_sessions {
            if let Some((_, update)) = limits.iter().find(|(id, _)| id == replay_session_id) {
                match update {
                    LimitUpdate::ExtendTo(position) => {
                        entry.session.extend_stop_position(*position)
                    }
                    LimitUpdate::Gone => entry.session.limit_counter_gone(),
                }
            }

            let Some(mut published) = Published::new(client, entry.registration_id) else {
                // The publication is gone; there is nowhere to write.
                finished.push(*replay_session_id);
                continue;
            };

            let progress = entry.session.do_work(&mut published, now_ns);

            // `replayer.bytesRead(bytesRead)` and `replayer.readTimeNs(…)`
            // (`:431-434`), which the reference does inside the session because
            // the session holds the replayer. Here the session has worked the
            // two numbers out for this turn and the replayer is the
            // conductor's, so they are handed over.
            self.replayer.read(
                u64::try_from(entry.session.read_bytes()).unwrap_or(u64::MAX),
                entry.session.read_time_ns(),
            );

            if !matches!(progress, ReplayProgress::Idle) {
                work += 1;
            }

            match progress {
                ReplayProgress::Started => {
                    if !entry.started {
                        entry.started = true;

                        // The OK carries the `replaySessionId`
                        // (`ReplaySession.java:338`), which is what a later
                        // `stopReplay` names.
                        owed.push(Deferred::Ok {
                            session_id: entry.control_session_id,
                            correlation_id: entry.correlation_id,
                            relevant_id: *replay_session_id,
                        });
                    }
                }
                ReplayProgress::Finished => finished.push(*replay_session_id),
                ReplayProgress::Failed { code, message } => {
                    owed.push(Deferred::Error {
                        session_id: entry.control_session_id,
                        correlation_id: entry.correlation_id,
                        relevant_id: i64::from(code),
                        message,
                    });
                    finished.push(*replay_session_id);
                }
                ReplayProgress::Idle | ReplayProgress::Worked => {}
            }
        }

        for replay_session_id in finished {
            let Some(entry) = self.replay_sessions.remove(&replay_session_id) else {
                continue;
            };

            // A replay that ran to its end is **closed**, so the driver's linger
            // can retransmit its tail; one that failed or was cancelled is
            // **revoked** (`ReplaySession.java:175-194`).
            //
            // Through [`Published`], which is where that pair lives and where
            // its test is: the session's own `close`/`revoke` cannot be the
            // caller, because the client a teardown needs is the conductor's.
            // A publication the client no longer holds is one the driver has
            // already taken back, so there is nothing to give it.
            if let Some(mut published) = Published::new(client, entry.registration_id) {
                if entry.session.is_revoking() {
                    published.revoke();
                } else {
                    published.close();
                }
            }

            // `closeReplaySession`'s `ctx.replaySessionCounter().decrementRelease()`
            // (`:1382`), which is what makes 112 the count of replays that are
            // **open** rather than the count that have ever run.
            self.replayer.session_closed(counters);
            work += 1;

            self.on_replay_end();
        }

        // `Replayer.doWork`'s tail (`:2779-2790`): the three statistics go out
        // on a turn that did work, and not otherwise.
        self.replayer.publish_if(counters, work);

        self.pending.extend(owed);
    }

    /// One `ERROR` a replay owes, pushed rather than sent for the reason every
    /// other callback answer is.
    fn refuse_replay(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        relevant_id: i64,
        message: String,
    ) {
        self.pending.push(Deferred::Error {
            session_id,
            correlation_id,
            relevant_id,
            message,
        });
    }

    #[allow(clippy::too_many_arguments)] // one per collaborator the actions use
    fn run_action(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        catalog: &Catalog,
        action: Action,
        recorder: &mut Recorder,
        now_ms: i64,
    ) {
        match action {
            Action::StartRecording(request) => self.start_recording(client, &request, now_ms),
            Action::StopRecordingSubscription {
                session_id,
                correlation_id,
                subscription_id,
            } => self.stop_recording_subscription(
                client,
                recorder,
                session_id,
                correlation_id,
                subscription_id,
                now_ms,
            ),
            Action::ExtendRecording(request) => {
                self.extend_recording(client, catalog, &request, now_ms);
            }
            Action::StopRecording {
                session_id,
                correlation_id,
                stream_id,
                original_channel,
            } => self.stop_recording(
                client,
                recorder,
                session_id,
                correlation_id,
                stream_id,
                &original_channel,
                now_ms,
            ),
            Action::StartReplay(request) => self.start_replay(counters, catalog, &request),
            Action::StopReplay {
                session_id,
                correlation_id,
                replay_session_id,
            } => self.stop_replay(replay_session_id, session_id, correlation_id),
            Action::StopAllReplays {
                session_id,
                correlation_id,
                recording_id,
            } => self.stop_all_replays(recording_id, session_id, correlation_id),
            Action::StopRecordingByIdentity {
                session_id,
                correlation_id,
                recording_id,
            } => self.stop_recording_by_identity(
                client,
                catalog,
                recorder,
                session_id,
                correlation_id,
                recording_id,
                now_ms,
            ),
        }
    }

    /// `ArchiveConductor.startRecording` (`ArchiveConductor.java:530-583`), with
    /// the reference's nine steps in its order and two of them split across
    /// turns: the add, and the answer it draws ([`PendingStartRecording`]).
    ///
    /// The two checks that refuse before anything is registered are the
    /// reference's, in its order: the concurrent-recording bound (`:538-543`)
    /// and the filesystem's free space (`:545-548`).
    ///
    /// The reference wraps the rest in a `try` whose `catch` answers the client
    /// with the exception's message (`:578-582`). The one failure this build can
    /// meet there is a channel that will not parse, and it is answered the same
    /// way — with this build's words for it rather than the JDK's, which is the
    /// one thing about that refusal that is not the reference's.
    fn start_recording(
        &mut self,
        client: &mut Client,
        request: &StartRecordingRequest,
        now_ms: i64,
    ) {
        let (key, channel, stripped_channel) = match self.decide_start(
            request.stream_id,
            request.source_location,
            &request.original_channel,
        ) {
            StartDecision::Add {
                key,
                channel,
                stripped_channel,
            } => (key, channel, stripped_channel),
            StartDecision::Refuse {
                relevant_id,
                message,
            } => {
                self.send_error(
                    client,
                    request.session_id,
                    request.correlation_id,
                    relevant_id,
                    &message,
                    now_ms,
                );
                return;
            }
        };

        match client.async_add_subscription(&channel, request.stream_id, self.command_timeout) {
            Ok(add) => self.pending_starts.push(PendingStartRecording {
                session_id: request.session_id,
                correlation_id: request.correlation_id,
                key,
                registration_id: add.registration_id(),
                stripped_channel,
                channel,
                stream_id: request.stream_id,
                original_channel: request.original_channel.clone(),
                is_auto_stop: request.auto_stop,
                request: RecordingRequest::Start,
            }),
            Err(error) => self.send_error(
                client,
                request.session_id,
                request.correlation_id,
                0,
                &format!("subscription could not be added: {error}"),
                now_ms,
            ),
        }
    }

    /// Everything `startRecording` decides before the driver is asked
    /// (`ArchiveConductor.java:538-577`).
    ///
    /// Split out for the same reason [`Sessions::start_listing`] is: the
    /// decisions are what can be wrong, and they are four refusals and one
    /// channel — none of which needs the client that the add does.
    fn decide_start(
        &self,
        stream_id: i32,
        source_location: SourceLocation,
        original_channel: &str,
    ) -> StartDecision {
        let maximum = self.recording.max_concurrent_recordings;
        if self.num_active_recordings >= maximum {
            return StartDecision::Refuse {
                relevant_id: MAX_RECORDINGS,
                message: format!("max concurrent recordings reached {maximum}"),
            };
        }

        if let Some(message) = self.is_low_storage_space() {
            return StartDecision::Refuse {
                relevant_id: STORAGE_SPACE,
                message,
            };
        }

        // The reference's `catch` around the whole of the rest answers the
        // client with the exception's message (`:578-582`). The one failure
        // this build can meet there is a channel that will not parse, and its
        // relevant id is the zero the one-argument `sendErrorResponse` writes
        // (`ControlSession.java:692-695`).
        //
        // The message is this build's parser's where the reference's is the
        // JDK's — `ChannelUri.parse` throwing is the whole of that answer, and
        // no test in the C suite reaches it, because a start with a channel
        // that will not read is a client that could not have published on it
        // either.
        let Ok(uri) = ChannelUri::parse(original_channel) else {
            return StartDecision::Refuse {
                relevant_id: 0,
                message: format!("{original_channel} is not a channel"),
            };
        };

        let key = make_key(stream_id, &uri);
        if self.recording_subscriptions.contains_key(&key) {
            return StartDecision::Refuse {
                relevant_id: ACTIVE_SUBSCRIPTION,
                message: format!(
                    "recording exists for streamId={stream_id} channel={original_channel}"
                ),
            };
        }

        // `strippedChannelBuilder(uri).build()` — the same builder the control
        // response channel goes through, with none of the three control
        // settings written back over it (`:558` against `:471-474`).
        let stripped_channel = stripped_channel_builder(&uri).build();

        // A local publication in the same driver is not a network one: what
        // carries it to a subscriber is a spy link (`:559-560`).
        let channel = if source_location == SourceLocation::LOCAL && uri.media() == UDP_MEDIA {
            format!("{SPY_PREFIX}{stripped_channel}")
        } else {
            stripped_channel.clone()
        };

        StartDecision::Add {
            key,
            channel,
            stripped_channel,
        }
    }

    /// The answer to the adds that have gone out
    /// (`ArchiveConductor.java:567-570`, which is the same three writes one turn
    /// earlier in the reference).
    fn claim_pending_starts(&mut self, client: &mut Client, now_ms: i64) {
        let mut index = 0;
        while index < self.pending_starts.len() {
            let start = &self.pending_starts[index];

            match client.async_add_poll(AsyncAdd::subscription(start.registration_id)) {
                AsyncAddPoll::Ready => {
                    let start = self.pending_starts.swap_remove(index);

                    self.recording_subscriptions.insert(
                        start.key,
                        RecordingSubscription {
                            registration_id: start.registration_id,
                            stripped_channel: start.stripped_channel,
                            original_channel: start.original_channel,
                            is_auto_stop: start.is_auto_stop,
                            channel: start.channel,
                            stream_id: start.stream_id,
                            session_id: start.session_id,
                            correlation_id: start.correlation_id,
                            request: start.request,
                        },
                    );
                    *self
                        .subscription_ref_counts
                        .entry(start.registration_id)
                        .or_insert(0) += 1;
                    self.num_active_recordings += 1;

                    if let Some(entry) = self.sessions.get_mut(&start.session_id) {
                        entry.control.send_ok_response(
                            start.correlation_id,
                            start.registration_id,
                            now_ms,
                            client,
                        );
                    }
                }
                AsyncAddPoll::Awaiting => index += 1,
                // A refusal and a handle this client knows nothing about are the
                // same answer to the client: the recording did not start, and
                // the reference's `catch` says so in the driver's words.
                AsyncAddPoll::Failed(error) => {
                    let start = self.pending_starts.swap_remove(index);
                    self.send_error(
                        client,
                        start.session_id,
                        start.correlation_id,
                        0,
                        &format!("subscription could not be added: {error}"),
                        now_ms,
                    );
                }
                AsyncAddPoll::Unknown => {
                    let start = self.pending_starts.swap_remove(index);
                    self.send_error(
                        client,
                        start.session_id,
                        start.correlation_id,
                        0,
                        "subscription add is no longer known",
                        now_ms,
                    );
                }
            }
        }
    }

    /// Collect the answers to the removals that have gone out, which is what
    /// takes the subscription out of the client's list.
    ///
    /// A removal that failed leaves the subscription in that list, which is why
    /// the reference's own `close` cannot fail either: it ignores the answer.
    /// Nothing here does anything about it — the driver's resource is what the
    /// id names, and a client-side entry for a subscription the archive has
    /// forgotten is not read by anything.
    fn collect_pending_removals(&mut self, client: &mut Client) {
        self.pending_removals.retain_mut(|remove| {
            matches!(
                client.remove_subscription_poll(*remove),
                RemovePoll::Awaiting
            )
        });
    }

    /// `ArchiveConductor.stopRecordingSubscription` (`:612-624`).
    #[allow(clippy::too_many_arguments)] // the request's four fields, the recorder, the clock
    fn stop_recording_subscription(
        &mut self,
        client: &mut Client,
        recorder: &mut Recorder,
        session_id: i64,
        correlation_id: i64,
        subscription_id: i64,
        now_ms: i64,
    ) {
        if self
            .remove_recording_subscription(subscription_id)
            .is_none()
        {
            self.send_error(
                client,
                session_id,
                correlation_id,
                UNKNOWN_SUBSCRIPTION,
                &format!("no recording subscription found for subscriptionId={subscription_id}"),
                now_ms,
            );
            return;
        }

        self.abort_recording_session_and_close_subscription(client, recorder, subscription_id);

        // `sendOkResponse(correlationId)` — the one-argument form, whose
        // relevant id is `GENERIC`, zero (`ControlSession.java:682-690`).
        if let Some(entry) = self.sessions.get_mut(&session_id) {
            entry
                .control
                .send_ok_response(correlation_id, 0, now_ms, client);
        }
    }

    /// The subscription an image belongs to, found the way the reference's
    /// `AvailableImageHandler` knows it without looking: it **is** a closure
    /// over the subscription it was made for (`:562-565`).
    ///
    /// This build's image events carry the subscription's registration id and
    /// nothing else, so the registry — which is keyed by `makeKey` — is searched
    /// by id. The clone is what lets the caller go on to mutate the catalog
    /// while holding the facts: one small copy per image, on the path where an
    /// image arrives.
    fn subscription_by_registration_id(
        &self,
        registration_id: i64,
    ) -> Option<RecordingSubscription> {
        self.recording_subscriptions
            .values()
            .find(|held| held.registration_id == registration_id)
            .cloned()
    }

    /// `ArchiveConductor.stopRecording` (`:585-610`): the stop a client names
    /// by channel and stream.
    ///
    /// It resolves to the same three steps as a stop by registration id, so it
    /// delegates once it has the id — what differs is what it says when there
    /// is nothing there, in the reference's own words for this arm
    /// (`:603-604`) rather than the other arm's (`:621-622`).
    #[allow(clippy::too_many_arguments)] // the request's fields, the recorder, the clock
    fn stop_recording(
        &mut self,
        client: &mut Client,
        recorder: &mut Recorder,
        session_id: i64,
        correlation_id: i64,
        stream_id: i32,
        original_channel: &str,
        now_ms: i64,
    ) {
        match self.decide_stop(stream_id, original_channel) {
            StopDecision::Subscription(subscription_id) => self.stop_recording_subscription(
                client,
                recorder,
                session_id,
                correlation_id,
                subscription_id,
                now_ms,
            ),
            StopDecision::Refuse {
                relevant_id,
                message,
            } => self.send_error(
                client,
                session_id,
                correlation_id,
                relevant_id,
                &message,
                now_ms,
            ),
        }
    }

    /// `ArchiveConductor.extendRecording` (`:1067-1157`), up to the point the
    /// driver is asked — which is where it joins [`Sessions::start_recording`],
    /// because from there the two are the same request.
    ///
    /// The gates are the reference's six, in its order, and two of them are
    /// worth reading twice because they are *nearly* the start's:
    ///
    /// * the concurrent-recording refusal says "reached **at** N" here
    ///   (`:1078-1082`) and "reached N" there (`:540`) — one word, and it is the
    ///   reference's;
    /// * and the spy test is `originalChannel.contains("udp")` (`:1130`), a
    ///   **substring** test, where the start asks the parsed channel
    ///   (`:559-560`). The two agree on every channel either is likely to meet
    ///   and are not the same test, which is the kind of thing to copy rather
    ///   than to tidy.
    ///
    /// The gate this build has nothing for is the outstanding-delete one
    /// (`:1100-1108`): delete-segments sessions are a later slice, and
    /// `deleteSegmentsSessionByIdMap` has no counterpart here.
    fn extend_recording(
        &mut self,
        client: &mut Client,
        catalog: &Catalog,
        request: &ExtendRecordingRequest,
        now_ms: i64,
    ) {
        let ExtendRecordingRequest {
            session_id,
            correlation_id,
            recording_id,
            stream_id,
            source_location,
            auto_stop,
            original_channel,
        } = request;

        let (key, channel, stripped_channel) = match self.decide_extend(
            catalog,
            *recording_id,
            *stream_id,
            *source_location,
            original_channel,
        ) {
            StartDecision::Add {
                key,
                channel,
                stripped_channel,
            } => (key, channel, stripped_channel),
            StartDecision::Refuse {
                relevant_id,
                message,
            } => {
                self.send_error(
                    client,
                    *session_id,
                    *correlation_id,
                    relevant_id,
                    &message,
                    now_ms,
                );
                return;
            }
        };

        match client.async_add_subscription(&channel, *stream_id, self.command_timeout) {
            Ok(add) => self.pending_starts.push(PendingStartRecording {
                session_id: *session_id,
                correlation_id: *correlation_id,
                key,
                registration_id: add.registration_id(),
                stripped_channel,
                channel,
                stream_id: *stream_id,
                original_channel: original_channel.clone(),
                is_auto_stop: *auto_stop,
                request: RecordingRequest::Extend {
                    recording_id: *recording_id,
                },
            }),
            Err(error) => self.send_error(
                client,
                *session_id,
                *correlation_id,
                0,
                &format!("subscription could not be added: {error}"),
                now_ms,
            ),
        }
    }

    /// The gates in front of an extend (`ArchiveConductor.java:1076-1118`).
    ///
    /// The answer is the same shape a start's is — a channel to add a
    /// subscription on, or a refusal — because from the driver's side the two
    /// requests **are** the same request.
    fn decide_extend(
        &self,
        catalog: &Catalog,
        recording_id: i64,
        stream_id: i32,
        source_location: SourceLocation,
        original_channel: &str,
    ) -> StartDecision {
        let maximum = self.recording.max_concurrent_recordings;
        if self.num_active_recordings >= maximum {
            return StartDecision::Refuse {
                relevant_id: MAX_RECORDINGS,
                // `reached **at**` — the start's says `reached` (`:1079` against
                // `:540`), and the reference means it.
                message: format!("max concurrent recordings reached at {maximum}"),
            };
        }

        if !catalog.has_recording(recording_id) {
            return StartDecision::Refuse {
                relevant_id: UNKNOWN_RECORDING,
                message: unknown_recording_message(recording_id),
            };
        }

        let summary = match catalog.recording(recording_id) {
            Ok(summary) => summary,
            Err(error) => {
                return StartDecision::Refuse {
                    relevant_id: 0,
                    message: format!("catalog could not read recording {recording_id}: {error}"),
                };
            }
        };

        if stream_id != summary.stream_id {
            return StartDecision::Refuse {
                relevant_id: UNKNOWN_RECORDING,
                message: format!(
                    "cannot extend recording {recording_id} with streamId={stream_id} for existing streamId={}",
                    summary.stream_id
                ),
            };
        }

        if self.recording_session_by_id.contains_key(&recording_id) {
            return StartDecision::Refuse {
                relevant_id: ACTIVE_RECORDING,
                message: format!("cannot extend active recording {recording_id}"),
            };
        }

        if let Some(message) = self.is_low_storage_space() {
            return StartDecision::Refuse {
                relevant_id: STORAGE_SPACE,
                message,
            };
        }

        let Ok(uri) = ChannelUri::parse(original_channel) else {
            return StartDecision::Refuse {
                relevant_id: 0,
                message: format!("{original_channel} is not a channel"),
            };
        };

        let key = make_key(stream_id, &uri);
        if self.recording_subscriptions.contains_key(&key) {
            return StartDecision::Refuse {
                relevant_id: ACTIVE_SUBSCRIPTION,
                message: format!(
                    "recording exists for streamId={stream_id} channel={original_channel}"
                ),
            };
        }

        let stripped_channel = stripped_channel_builder(&uri).build();

        // `originalChannel.contains("udp")` (`:1130`) — the **string**, not the
        // parsed media. See this method's caller for why it is copied as it is.
        let channel =
            if original_channel.contains(UDP_MEDIA) && source_location == SourceLocation::LOCAL {
                format!("{SPY_PREFIX}{stripped_channel}")
            } else {
                stripped_channel.clone()
            };

        StartDecision::Add {
            key,
            channel,
            stripped_channel,
        }
    }

    /// `validateImageForExtendRecording` (`ArchiveConductor.java:2155-2194`):
    /// four questions, and an image that fails any of them is not a
    /// continuation of the recording.
    ///
    /// The four are the four things the *file* was laid out with — where the
    /// recording stopped, which term it began in, how long its terms are and how
    /// big its frames are — so an image that differs in any of them would be
    /// appended at a position the segments do not describe (`:2161-2192`).
    ///
    /// **Answers with where the recording started**, or `None` when it refused:
    /// the caller needs the first for the writer of a continuation, and the
    /// second is what a `continue` in the caller's loop means.
    #[allow(clippy::too_many_arguments)] // the request, the image, and the clock
    fn validate_extension(
        &mut self,
        client: &mut Client,
        catalog: &Catalog,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        subscription: &RecordingSubscription,
        facts: &ImageFacts,
        now_ms: i64,
    ) -> Option<i64> {
        // The reference re-checks the active-recording gate here as well as at
        // the request (`:2066-2072`): the image arrives a turn or more later,
        // and a second session on the same recording may have started in
        // between.
        if self.recording_session_by_id.contains_key(&recording_id) {
            self.send_error(
                client,
                session_id,
                correlation_id,
                ACTIVE_RECORDING,
                &format!(
                    "cannot extend active recording {recording_id} streamId={} channel={}",
                    facts.stream_id, subscription.original_channel
                ),
                now_ms,
            );

            return None;
        }

        let Ok(summary) = catalog.recording(recording_id) else {
            self.warnings.push(format!(
                "could not read recording {recording_id} to extend it"
            ));

            return None;
        };

        let refusal = if facts.join_position != summary.stop_position {
            Some(format!(
                "cannot extend recording {recording_id} image.joinPosition={} != rec.stopPosition={}",
                facts.join_position, summary.stop_position
            ))
        } else if facts.initial_term_id != summary.initial_term_id {
            Some(format!(
                "cannot extend recording {recording_id} image.initialTermId={} != rec.initialTermId={}",
                facts.initial_term_id, summary.initial_term_id
            ))
        } else if facts.term_buffer_length != summary.term_buffer_length {
            Some(format!(
                "cannot extend recording {recording_id} image.termBufferLength={} != rec.termBufferLength={}",
                facts.term_buffer_length, summary.term_buffer_length
            ))
        } else if facts.mtu_length != summary.mtu_length {
            Some(format!(
                "cannot extend recording {recording_id} image.mtuLength={} != rec.mtuLength={}",
                facts.mtu_length, summary.mtu_length
            ))
        } else {
            None
        };

        let Some(message) = refusal else {
            return Some(summary.start_position);
        };

        self.send_error(
            client,
            session_id,
            correlation_id,
            INVALID_EXTENSION,
            &message,
            now_ms,
        );

        // `if (autoStop) closeAndRemoveRecordingSubscription(…)` (`:2130-2136`):
        // a subscription whose extension was refused has nothing left to read,
        // and an archive told to stop with the client does that now rather than
        // leaving a subscription that will never have a session.
        if subscription.is_auto_stop {
            // The reference's `closeAndRemoveRecordingSubscription`
            // (`:2580-2595`) less its abort loop: the gate above refused
            // *because* the recording is not in flight, so no session is reading
            // this subscription and the loop would have had nothing to walk.
            self.subscription_ref_counts
                .remove(&subscription.registration_id);
            self.remove_recording_subscription(subscription.registration_id);
            self.release_recording_subscription(client, subscription.registration_id);
        }

        None
    }

    /// What a stop by channel and stream finds
    /// (`ArchiveConductor.stopRecording`, `:589-600`).
    fn decide_stop(&self, stream_id: i32, original_channel: &str) -> StopDecision {
        // The reference wraps the lookup in a `try` whose `catch` answers with
        // the exception's message (`:606-609`); the one thing that can throw
        // there is the parse, and the words are this build's.
        let Ok(uri) = ChannelUri::parse(original_channel) else {
            return StopDecision::Refuse {
                relevant_id: 0,
                message: format!("{original_channel} is not a channel"),
            };
        };

        let key = make_key(stream_id, &uri);

        match self.recording_subscriptions.get(&key) {
            Some(held) => StopDecision::Subscription(held.registration_id),
            None => StopDecision::Refuse {
                relevant_id: UNKNOWN_SUBSCRIPTION,
                message: format!(
                    "no recording found for streamId={stream_id} channel={original_channel}"
                ),
            },
        }
    }

    /// `ArchiveConductor.stopRecordingByIdentity` (`:1301-1327`).
    ///
    /// Two things make this arm different from the other stops. It is guarded by
    /// `hasRecording` — a recording the catalog does not hold is refused with
    /// the standard message, not with "no subscription found" (`:1303-1305`) —
    /// and its answer carries **whether** a recording was stopped, `0` or `1`,
    /// where the others answer with nothing (`:1323`). A recording that is in
    /// the catalog but not in flight is therefore an `OK` with a zero in it.
    #[allow(clippy::too_many_arguments)] // the request's fields, the catalog, the recorder, the clock
    fn stop_recording_by_identity(
        &mut self,
        client: &mut Client,
        catalog: &Catalog,
        recorder: &mut Recorder,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        now_ms: i64,
    ) {
        if !catalog.has_recording(recording_id) {
            self.send_error(
                client,
                session_id,
                correlation_id,
                UNKNOWN_RECORDING,
                &unknown_recording_message(recording_id),
                now_ms,
            );
            return;
        }

        let mut found = 0;

        if let Some(handle) = self.recording_session_by_id.get(&recording_id).copied() {
            // **This** recording's session, not every session reading the same
            // subscription: the reference aborts the one it looked up
            // (`:1306-1310`), and a subscription can carry more than one image
            // over its life.
            recorder.abort_session(recording_id, "stop recording by identity");

            if self
                .remove_recording_subscription(handle.subscription_id)
                .is_some()
            {
                found = 1;

                let remaining = self
                    .subscription_ref_counts
                    .get_mut(&handle.subscription_id)
                    .map(|count| {
                        *count -= 1;
                        *count
                    })
                    .unwrap_or(0);

                if 0 == remaining {
                    self.release_recording_subscription(client, handle.subscription_id);
                }
            }
        }

        if let Some(entry) = self.sessions.get_mut(&session_id) {
            entry
                .control
                .send_ok_response(correlation_id, found, now_ms, client);
        }
    }

    /// `ArchiveConductor.removeRecordingSubscription` (`:2139-2153`): the
    /// subscription is found by its **id** and taken out of the map by its key,
    /// which is the only thing the map is keyed by.
    ///
    /// **`numActiveRecordings` does not move here.** The reference's field counts
    /// live *recording sessions* — it goes up when a start is accepted (`:569`)
    /// and down when a session closes (`:1361`), and a subscription stopped
    /// while its recording is still running is a recording that has not ended
    /// yet. So this slice, which has no recording sessions, has a count that can
    /// only rise, and the movement down arrives with the recorder.
    fn remove_recording_subscription(&mut self, subscription_id: i64) -> Option<String> {
        let key = self
            .recording_subscriptions
            .iter()
            .find(|(_, held)| held.registration_id == subscription_id)
            .map(|(key, _)| key.clone())?;

        self.recording_subscriptions.remove(&key);

        Some(key)
    }

    /// `ArchiveConductor.abortRecordingSessionAndCloseSubscription`
    /// (`:1766-1780`): a client asked for a recording to stop.
    ///
    /// Three things, in the reference's order: every session reading this
    /// subscription is aborted — they notice on their next turn and end, which
    /// is where the catalog's stop position and the `STOP` signal come from —
    /// one comes off the refcount, and the subscription is given back when the
    /// count reaches zero.
    ///
    /// `0 ==` here and `<= 0` in [`Sessions::close_recording_session`], which is
    /// the reference's own pair of comparisons (`:1778` against `:1357`) and not
    /// a slip: this path says "the last holder has let go", that one tolerates a
    /// count that has already gone past zero.
    fn abort_recording_session_and_close_subscription(
        &mut self,
        client: &mut Client,
        recorder: &mut Recorder,
        subscription_id: i64,
    ) {
        recorder.abort_sessions_for(subscription_id, "stop recording");

        let count = self
            .subscription_ref_counts
            .get_mut(&subscription_id)
            .map(|count| {
                *count -= 1;
                *count
            })
            .unwrap_or(0);

        if 0 != count {
            return;
        }

        self.release_recording_subscription(client, subscription_id);
    }

    /// `subscription.close()` (`:1780`), which the reference's client does on a
    /// thread of its own.
    ///
    /// This one cannot: the command is sent here and the answer collected on a
    /// later turn (`Client::async_remove_subscription` is where that argument
    /// is written down).
    fn release_recording_subscription(&mut self, client: &mut Client, subscription_id: i64) {
        match client.async_remove_subscription(subscription_id, self.command_timeout) {
            Ok(remove) => self.pending_removals.push(remove),
            Err(error) => self.warnings.push(format!(
                "recording subscription {subscription_id} could not be given back: {error}"
            )),
        }
    }

    /// `ArchiveConductor.isLowStorageSpace` (`:2597-2617`), which answers with
    /// the message the refusal carries or `None` for "there is room".
    ///
    /// The check is against the **archive directory's** filesystem, and the
    /// reference's `FileStore.getUsableSpace` failing is a rethrow that takes
    /// the archive with it (`:2610-2613`). This build's port of the same
    /// `statvfs` answers zero when it cannot ask, which is below any positive
    /// threshold — so a filesystem that cannot be asked about refuses starts
    /// rather than accepting them blind.
    fn is_low_storage_space(&self) -> Option<String> {
        let threshold = self.recording.low_storage_space_threshold;
        let usable = usable_space(&self.recording.archive_dir);

        if usable <= threshold {
            return Some(format!(
                "low storage threshold={threshold} <= usableSpace={usable}"
            ));
        }

        None
    }

    /// Push an `ERROR` at a session, in the two answers that carry no client
    /// words of their own.
    fn send_error(
        &mut self,
        client: &mut Client,
        session_id: i64,
        correlation_id: i64,
        relevant_id: i64,
        message: &str,
        now_ms: i64,
    ) {
        if let Some(entry) = self.sessions.get_mut(&session_id) {
            entry
                .control
                .send_error_response(correlation_id, relevant_id, message, now_ms, client);
        }
    }

    /// Whether this session already has a listing in flight
    /// (`ControlSession.hasActiveListing`, `ControlSession.java:298-301`).
    ///
    /// The reference asks the session's own `activeListing` slot; the listing
    /// lives in [`Sessions::listings`] here, so the question is asked of the
    /// list — same question, one owner further out.
    #[must_use]
    fn has_active_listing(&self, control_session_id: i64) -> bool {
        self.listings
            .iter()
            .any(|listing| listing.control_session_id() == control_session_id)
    }

    /// Start the listing a request asked for, or say why it will not be started
    /// (`ArchiveConductor.listRecording`, `ArchiveConductor.java:688-706`).
    ///
    /// `None` is the listing itself, which is in [`Sessions::listings`] by the
    /// time this returns: the two refusals are the caller's to send, and the
    /// descriptor is [`Sessions::drive_listings`]'s.
    #[must_use]
    fn start_listing(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        catalog: &Catalog,
    ) -> Option<ListingRefusal> {
        if self.has_active_listing(session_id) {
            return Some(ListingRefusal::ActiveListing);
        }

        if !catalog.has_recording(recording_id) {
            return Some(ListingRefusal::UnknownRecording);
        }

        self.listings.push(Listing::Recording(RecordingListing {
            control_session_id: session_id,
            correlation_id,
            recording_id,
            is_done: false,
        }));

        None
    }

    /// Start a page-at-a-time listing of the recordings
    /// (`ArchiveConductor.newListRecordingsSession`, `:638-657`, and `:659-687`
    /// for the one with a filter).
    ///
    /// **One gate**, and it is the asking session's own (`:640-645`, `:661-666`):
    /// where [`Sessions::start_listing`] also asks the catalog whether the
    /// recording is there, there is nothing here for the catalog to refuse — a
    /// page about recordings that are not there is a page of nothing, and the
    /// walk answers it with the empty answer rather than a refusal. So this
    /// decides in the callback, the way the subscriptions listing does, and no
    /// part of it is deferred.
    fn start_recordings_listing(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        from_recording_id: i64,
        count: i32,
        filter: RecordingsFilter,
    ) {
        if self.has_active_listing(session_id) {
            self.pending.push(Deferred::Error {
                session_id,
                correlation_id,
                relevant_id: ACTIVE_LISTING,
                message: ACTIVE_LISTING_MSG.to_owned(),
            });
            return;
        }

        self.listings.push(Listing::Recordings(RecordingsListing {
            control_session_id: session_id,
            correlation_id,
            from_recording_id,
            count,
            sent: 0,
            filter,
            is_done: false,
        }));
    }

    /// Serve the listings, one attempt each (`SessionWorker.doWork` over
    /// `ListRecordingByIdSession.java:60-80`).
    ///
    /// The reference's session asks the catalog to wrap the descriptor and hands
    /// the wrapped bytes to `sendDescriptor`, which answers whether they went
    /// out. `false` is not a failure to report but a turn to try again — a full
    /// window or a log turning over — so the listing stays until the send takes,
    /// which is the whole of its pagination.
    ///
    /// A listing whose recording can no longer be read is answered with
    /// `RECORDING_UNKNOWN` and is done: that is what the reference's `false` from
    /// `wrapDescriptor` means (`:63-73`), and a client that asked about a
    /// recording that has since been retired hears the same thing as one that
    /// asked about a recording that was never there.
    ///
    /// A listing whose session is done is finished without an attempt: the
    /// reference's `SessionWorker` would have removed the listing as soon as its
    /// session aborted (`ControlSession.abort`, `ControlSession.java:145-157`,
    /// which aborts the listing it holds). Sessions are driven after listings
    /// here, so a session that ended last turn is caught here first.
    ///
    /// Runs before [`Sessions::drive_sessions`] because the reference's worker
    /// walks its sessions newest-first and a listing is always added after the
    /// session that asked for it.
    fn drive_listings(&mut self, client: &mut Client, catalog: &Catalog, now_ms: i64) {
        for listing in &mut self.listings {
            if listing.is_done() {
                continue;
            }

            let control_session_id = listing.control_session_id();
            let Some(entry) = self.sessions.get_mut(&control_session_id) else {
                mark_done(listing);
                continue;
            };

            if entry.control.is_done() {
                mark_done(listing);
                continue;
            }

            match listing {
                Listing::Recording(listing) => {
                    let Ok(Some(descriptor)) = catalog.descriptor_body(listing.recording_id) else {
                        entry.control.send_recording_unknown(
                            listing.correlation_id,
                            listing.recording_id,
                            now_ms,
                            client,
                        );
                        listing.is_done = true;
                        continue;
                    };

                    if entry.control.send_descriptor(
                        listing.correlation_id,
                        descriptor,
                        now_ms,
                        client,
                    ) {
                        listing.is_done = true;
                    }
                }
                // `ListRecordingSubscriptionsSession.doWork` (`:96-127`), walked
                // the way the reference walks it: an index that skips the entries
                // already passed, a filter, and two ways to stop — the client
                // having taken what it asked for, or the map running out.
                //
                // The order is this map's, which — like the reference's
                // `Object2ObjectHashMap` — is unspecified. What matters is that
                // it does not change under one listing, which it cannot: a
                // `HashMap` is only reordered by insertion, and the walk never
                // inserts.
                Listing::Subscriptions(listing) => {
                    let size =
                        i32::try_from(self.recording_subscriptions.len()).unwrap_or(i32::MAX);
                    let mut index = 0;

                    for subscription in self.recording_subscriptions.values() {
                        let examined = index;
                        index += 1;

                        if examined < listing.pseudo_index {
                            continue;
                        }

                        if matches_listing(
                            subscription,
                            listing.stream_id,
                            listing.apply_stream_id,
                            &listing.channel_fragment,
                        ) {
                            if !entry.control.send_subscription_descriptor(
                                listing.correlation_id,
                                subscription.registration_id,
                                subscription.stream_id,
                                &subscription.channel,
                                now_ms,
                                client,
                            ) {
                                // `isDone = controlSession.isDone()` (`:110`): a
                                // send that did not take leaves the walk where it
                                // is, and a session that ended under the listing
                                // takes the listing with it. A **live** session
                                // whose last offer failed still reaches the
                                // terminal answer below, which is the reference's
                                // own behaviour: the listing ends and the client
                                // is told there is nothing more (`:120-125`), even
                                // though one descriptor did not go out.
                                listing.is_done = entry.control.is_done();
                                break;
                            }

                            listing.sent += 1;

                            if listing.sent >= listing.subscription_count {
                                listing.is_done = true;
                                break;
                            }
                        }

                        // `pseudoIndex = index - 1` (`:117`): every entry the
                        // walk passes moves it on, whether it answered with a
                        // descriptor or not, which is what keeps a listing that
                        // sent nothing this turn from starting over.
                        listing.pseudo_index = index - 1;
                    }

                    if !listing.is_done && index >= size {
                        // The walk ran off the end without filling the page: the
                        // client is told there is nothing more (`:120-125`),
                        // which its poller reads as "this listing is complete".
                        entry.control.send_subscription_unknown(
                            listing.correlation_id,
                            now_ms,
                            client,
                        );
                        listing.is_done = true;
                    }
                }
                // `AbstractListRecordingsSession.doWork` (`:79-142`), whose
                // walk is [`walk_recordings`]. What is left here is the part
                // that needs the client: offering each descriptor until one
                // does not go out, and the terminal answer a walk that ran out
                // of recordings owes.
                Listing::Recordings(listing) => {
                    let correlation_id = listing.correlation_id;

                    match walk_recordings(listing, catalog, |descriptor| {
                        entry
                            .control
                            .send_descriptor(correlation_id, descriptor, now_ms, client)
                    }) {
                        RecordingsWalk::Unknown(recording_id) => {
                            entry.control.send_recording_unknown(
                                correlation_id,
                                recording_id,
                                now_ms,
                                client,
                            );
                            listing.is_done = true;
                        }
                        // `isDone = controlSession.isDone()` (`:119`): the
                        // cursor stands still and the session is what says
                        // whether the listing is over anyway.
                        RecordingsWalk::SendFailed => listing.is_done = entry.control.is_done(),
                        RecordingsWalk::Idle => {}
                    }
                }
            }
        }

        // The worker's `fastUnorderedRemove` is what takes a finished listing
        // out where it stands (the last one fills its place) — and the order
        // listings are driven in is not a contract, so a `retain` says the same
        // thing about the result.
        self.listings.retain(|listing| !listing.is_done());
    }

    /// `ctx.controlSessionsCounter()`, asked for once and taken up when the
    /// driver answers (`Archive.java:1560-1561`).
    ///
    /// **Not** through `ControlSessionsCounter::allocate`, which blocks. This
    /// runs inside `drive`, which the launcher calls inside the turn that
    /// drives the driver, so a blocking command here waits for a driver nobody
    /// is driving — the same seam the adapter's callbacks run into, one layer
    /// down. The command goes out on one turn and the answer is read on a
    /// later one, which is how the 113 already works.
    fn allocate_session_counter(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
    ) {
        if self.control_sessions.is_some() {
            return;
        }

        let Some(registration_id) = self.session_counter_registration_id else {
            match request_control_sessions_counter(client, self.archive_id, self.command_timeout) {
                Ok(registration_id) => {
                    self.session_counter_registration_id = Some(registration_id);
                }
                Err(error) => self.warnings.push(format!(
                    "could not ask for the archive control sessions counter: {error}"
                )),
            }
            return;
        };

        match claim_control_sessions_counter(client, counters, registration_id) {
            Ok(Some(counter)) => {
                self.control_sessions = Some(counter);
                self.session_counter_registration_id = None;
            }
            Ok(None) => {}
            Err(error) => {
                // The driver refused it, which can never become a counter; the
                // archive runs on without one rather than asking forever.
                self.session_counter_registration_id = None;
                self.warnings.push(format!(
                    "could not allocate the archive control sessions counter: {error}"
                ));
            }
        }
    }

    /// Move the aggregate counter by the turn's net (`:520`,
    /// `ControlSessionAdapter.java:1158-1161`).
    fn apply_session_count_delta(&mut self, counters: &CountersReader<'_, ReadWrite>) {
        let Some(counter) = self.control_sessions else {
            // Nothing to move it with yet; the delta is owed, not lost.
            return;
        };

        let delta = std::mem::take(&mut self.session_count_delta);
        for _ in 0..delta.abs() {
            if delta > 0 {
                counter.increment(counters);
            } else {
                counter.decrement(counters);
            }
        }
    }

    /// Make the sessions the connects that arrived this turn asked for
    /// (`ArchiveConductor.java:448-520`).
    ///
    /// The order is the reference's: the session counter is asked for before
    /// the session is made, the authenticator is offered the session after,
    /// and the aggregate counter moves last.
    fn run_pending_connects(
        &mut self,
        client: &mut Client,
        authenticator: &mut dyn Authenticator,
        now_ms: i64,
    ) {
        for connect in std::mem::take(&mut self.pending_connects) {
            let image_info = self.image_info(client, connect.image);
            let counted_by = if connect.client_info.is_empty() {
                image_info
            } else {
                format!("{} {}", connect.client_info, image_info)
            };

            let counter = match ControlSessionCounter::allocate(
                client,
                self.archive_id,
                connect.session_id,
                &counted_by,
                self.command_timeout,
            ) {
                Ok(counter) => Some(counter),
                // The reference lets an async add fail on its own time; here
                // the command itself could not be sent. The session is still
                // made — one that never gets a counter simply never leaves
                // `INIT` (`ControlSession.java:895`), which is what the
                // reference's `null != sessionCounter` guard says.
                Err(error) => {
                    self.warnings.push(format!(
                        "could not allocate the counter for control session {}: {error}",
                        connect.session_id
                    ));
                    None
                }
            };

            let mut control = ControlSession::new(
                connect.session_id,
                connect.correlation_id,
                connect.response_channel,
                connect.response_stream_id,
                connect.invalid_version_message,
                self.connect_timeout_ms,
                self.liveness_check_interval_ms,
                now_ms,
                ControlResponseProxy::new(self.command_timeout),
            );

            if let Some(counter) = counter {
                control.set_counter(counter);
            }

            authenticator.on_connect_request(
                connect.session_id,
                &connect.encoded_credentials,
                now_ms,
            );

            // A close that arrived in the same poll as the connect is applied
            // here, because the session it names did not exist when it
            // arrived. Everything else the adapter can say about a session is
            // either deferred or needs the session to be live, and both are
            // handled by the time this returns.
            if let Some(reason) = connect.abort_reason {
                control.abort(&reason);
            }

            self.sessions.insert(
                connect.session_id,
                SessionEntry {
                    control,
                    image: connect.image,
                },
            );
            self.session_count_delta += 1;
        }
    }

    /// Send what the callbacks owed (`ControlSession.java:682-815`).
    #[allow(clippy::too_many_arguments)] // one per thing a deferred intent may need
    fn run_deferred(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        authenticator: &mut dyn Authenticator,
        catalog: &Catalog,
        recorder: &mut Recorder,
        now_ms: i64,
    ) {
        for intent in std::mem::take(&mut self.pending) {
            match intent {
                Deferred::Ok {
                    session_id,
                    correlation_id,
                    relevant_id,
                } => {
                    if let Some(entry) = self.sessions.get_mut(&session_id) {
                        entry
                            .control
                            .send_ok_response(correlation_id, relevant_id, now_ms, client);
                    }
                }
                Deferred::Error {
                    session_id,
                    correlation_id,
                    relevant_id,
                    message,
                } => {
                    if let Some(entry) = self.sessions.get_mut(&session_id) {
                        entry.control.send_error_response(
                            correlation_id,
                            relevant_id,
                            &message,
                            now_ms,
                            client,
                        );
                    }
                }
                Deferred::ChallengeResponse {
                    session_id,
                    correlation_id,
                    encoded_credentials,
                } => {
                    if let Some(entry) = self.sessions.get_mut(&session_id) {
                        entry.control.on_challenge_response(
                            correlation_id,
                            &encoded_credentials,
                            now_ms,
                            client,
                            authenticator,
                        );
                    }
                }
                Deferred::Query {
                    session_id,
                    correlation_id,
                    query,
                } => {
                    let Some(entry) = self.sessions.get_mut(&session_id) else {
                        continue;
                    };

                    match answer_query(catalog, &self.recording_session_by_id, counters, &query) {
                        Ok(value) => {
                            entry
                                .control
                                .send_ok_response(correlation_id, value, now_ms, client)
                        }
                        Err(message) => entry.control.send_error_response(
                            correlation_id,
                            UNKNOWN_RECORDING,
                            &message,
                            now_ms,
                            client,
                        ),
                    }
                }
                // `ArchiveConductor.listRecording` (`:688-706`): the two
                // refusals, and then the listing itself — which is not sent from
                // here but handed to `drive_listings`, in this same turn
                // (`ArchiveConductor.addSession` at `:703`, driven by
                // `super.doWork` at `:395` in the reference too).
                Deferred::ListRecording {
                    session_id,
                    correlation_id,
                    recording_id,
                } => {
                    let Some(refusal) =
                        self.start_listing(session_id, correlation_id, recording_id, catalog)
                    else {
                        continue;
                    };

                    let Some(entry) = self.sessions.get_mut(&session_id) else {
                        continue;
                    };

                    match refusal {
                        ListingRefusal::ActiveListing => entry.control.send_error_response(
                            correlation_id,
                            ACTIVE_LISTING,
                            ACTIVE_LISTING_MSG,
                            now_ms,
                            client,
                        ),
                        ListingRefusal::UnknownRecording => {
                            entry.control.send_recording_unknown(
                                correlation_id,
                                recording_id,
                                now_ms,
                                client,
                            );
                        }
                    }
                }
                // A request that moves a resource rather than answering about
                // one — and the only kind that needs the client for something
                // other than talking to the client (`ArchiveConductor.java:562`,
                // `:1766-1780`).
                Deferred::SubscriptionUnknown {
                    session_id,
                    correlation_id,
                } => {
                    if let Some(entry) = self.sessions.get_mut(&session_id) {
                        entry
                            .control
                            .send_subscription_unknown(correlation_id, now_ms, client);
                    }
                }
                Deferred::Action(action) => {
                    self.run_action(client, counters, catalog, action, recorder, now_ms);
                }
            }
        }
    }

    /// Drive the sessions and collect the finished ones
    /// (`SessionWorker.java:56-81`).
    fn drive_sessions(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        authenticator: &mut dyn Authenticator,
        now_ms: i64,
    ) {
        for entry in self.sessions.values_mut() {
            entry
                .control
                .do_work(now_ms, client, counters, authenticator);
        }

        let finished: Vec<i64> = self
            .sessions
            .iter()
            .filter(|(_, entry)| entry.control.is_done())
            .map(|(session_id, _)| *session_id)
            .collect();

        for session_id in finished {
            let Some(mut entry) = self.sessions.remove(&session_id) else {
                continue;
            };

            // A token outlives nothing (`ControlSessionAdapter.java:1157`): its
            // session is gone, and session ids are handed out again.
            self.remove_replay_tokens_for_session(session_id);

            entry.control.close(client);

            let abort_reason = entry.control.abort_reason().map(str::to_owned);
            let aborted = EndedSession::reason_rejects_image(abort_reason.as_deref());

            self.ended.push(EndedSession {
                session_id,
                image: entry.image,
                abort_reason,
                aborted,
            });
            self.session_count_delta -= 1;
        }
    }

    /// `"sourceIdentity=" + image.sourceIdentity() + " sessionId=" + image.sessionId()`
    /// (`ArchiveConductor.java:492`).
    ///
    /// The image is named by the id the adapter had it by, and found through
    /// the subscriptions the archive holds. An image the client no longer has
    /// is only reachable this way for as long as the subscription does; the
    /// session id is what is left when it is gone.
    fn image_info(&self, client: &Client, image: ImageId) -> String {
        match self.find_image(client, image) {
            Some(found) => format!(
                "sourceIdentity={} sessionId={}",
                found.source_identity(),
                found.session_id()
            ),
            None => format!("sourceIdentity= sessionId={}", image.session_id()),
        }
    }

    /// The image an [`ImageId`] names, if a subscription still holds it.
    fn find_image<'c>(&self, client: &'c Client, image: ImageId) -> Option<&'c Image> {
        self.subscription_ids.iter().find_map(|subscription_id| {
            client
                .subscription(*subscription_id)?
                .image(image.correlation_id())
        })
    }
}

impl ControlPlane for Sessions {
    fn new_session(&mut self, image: ImageId, request: ConnectRequest<'_>, _now_ms: i64) -> i64 {
        let session_id = self.next_session_id;
        self.next_session_id += 1;

        // The channel the client asked to be answered on is rebuilt from the
        // archive's own settings. A channel that will not read is the one case
        // the reference does not survive — `ChannelUri.parse` throws and takes
        // the archive with it — so this keeps what was asked for and says so.
        // The session's own publication then fails to be added, which ends it
        // the way any unusable response channel does.
        let derived = response_channel(
            request.response_channel,
            image.correlation_id(),
            &self.response_channel_defaults,
        );
        let response_channel = match derived {
            Ok(channel) => channel,
            Err(error) => {
                self.warnings.push(format!(
                    "could not read the response channel {}: {error}",
                    request.response_channel
                ));
                request.response_channel.to_owned()
            }
        };

        let invalid_version_message =
            if semantic_version_major(request.version) != PROTOCOL_MAJOR_VERSION {
                Some(format!(
                    "invalid client version {}, archive is {}",
                    format_version(request.version),
                    format_version(PROTOCOL_SEMANTIC_VERSION)
                ))
            } else {
                None
            };

        self.pending_connects.push(PendingConnect {
            session_id,
            image,
            correlation_id: request.correlation_id,
            response_stream_id: request.response_stream_id,
            response_channel,
            invalid_version_message,
            encoded_credentials: request.encoded_credentials.to_vec(),
            client_info: request.client_info.to_owned(),
            abort_reason: None,
        });

        session_id
    }

    fn session_principal(&self, session_id: i64) -> Option<&[u8]> {
        self.sessions
            .get(&session_id)
            .and_then(|entry| entry.control.encoded_principal())
    }

    /// Aborts a session (`ControlSession.abort`).
    ///
    /// A session made this turn is not in `sessions` yet — it is still a
    /// pending connect — but the adapter already knows about it, because it
    /// records the session as soon as `new_session` answers. So a close that
    /// arrives in the same poll as its connect lands here, and the reason is
    /// carried on the pending connect rather than dropped.
    fn abort_session(&mut self, session_id: i64, reason: &str) {
        if let Some(entry) = self.sessions.get_mut(&session_id) {
            entry.control.abort(reason);
            return;
        }

        if let Some(connect) = self
            .pending_connects
            .iter_mut()
            .find(|connect| connect.session_id == session_id)
        {
            connect.abort_reason = Some(reason.to_owned());
        }
    }

    fn on_challenge_response(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        encoded_credentials: &[u8],
        _now_ms: i64,
    ) {
        self.pending.push(Deferred::ChallengeResponse {
            session_id,
            correlation_id,
            encoded_credentials: encoded_credentials.to_vec(),
        });
    }

    fn on_keep_alive(&mut self, session_id: i64) {
        if let Some(entry) = self.sessions.get_mut(&session_id) {
            entry.control.attempt_to_activate();
        }
    }

    fn on_archive_id(&mut self, session_id: i64, correlation_id: i64, _now_ms: i64) {
        self.pending.push(Deferred::Ok {
            session_id,
            correlation_id,
            relevant_id: self.archive_id,
        });
    }

    fn on_query(&mut self, session_id: i64, correlation_id: i64, query: Query, _now_ms: i64) {
        self.pending.push(Deferred::Query {
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
        self.pending.push(Deferred::ListRecording {
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
        self.start_recordings_listing(
            session_id,
            correlation_id,
            from_recording_id,
            count,
            RecordingsFilter::All,
        );
    }

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
        self.start_recordings_listing(
            session_id,
            correlation_id,
            from_recording_id,
            count,
            RecordingsFilter::Uri {
                stream_id,
                channel_fragment: channel_fragment.to_vec(),
            },
        );
    }

    fn on_start_recording(&mut self, request: StartRecordingRequest, _now_ms: i64) {
        self.pending
            .push(Deferred::Action(Action::StartRecording(request)));
    }

    fn replay_session_for_token(
        &self,
        replay_token: i64,
        recording_id: i64,
        now_ms: i64,
    ) -> Option<i64> {
        self.replay_session_by_token(replay_token, recording_id, now_ms)
    }

    fn on_replay_token(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        now_ms: i64,
    ) {
        // Generated here and answered next turn, rather than handed to the
        // conductor as an intent: nothing about it needs the client or the
        // catalog, and the reference generates it the moment it reads the
        // request (`ControlSessionAdapter.java:1100`).
        let replay_token = self.generate_replay_token(session_id, recording_id, now_ms);

        // The token **is** the relevant id (`:1101`), which is what the client
        // reads it back out of — not a message body, and not a response code of
        // its own.
        self.pending.push(Deferred::Ok {
            session_id,
            correlation_id,
            relevant_id: replay_token,
        });
    }

    fn on_start_replay(&mut self, request: StartReplayRequest, _now_ms: i64) {
        self.pending
            .push(Deferred::Action(Action::StartReplay(request)));
    }

    fn on_stop_replay(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        replay_session_id: i64,
        _now_ms: i64,
    ) {
        self.pending.push(Deferred::Action(Action::StopReplay {
            session_id,
            correlation_id,
            replay_session_id,
        }));
    }

    fn on_stop_all_replays(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        _now_ms: i64,
    ) {
        self.pending.push(Deferred::Action(Action::StopAllReplays {
            session_id,
            correlation_id,
            recording_id,
        }));
    }

    fn on_stop_recording_subscription(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        subscription_id: i64,
        _now_ms: i64,
    ) {
        self.pending
            .push(Deferred::Action(Action::StopRecordingSubscription {
                session_id,
                correlation_id,
                subscription_id,
            }));
    }

    fn on_extend_recording(&mut self, request: ExtendRecordingRequest, _now_ms: i64) {
        self.pending
            .push(Deferred::Action(Action::ExtendRecording(request)));
    }

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
        _now_ms: i64,
    ) {
        // `ArchiveConductor.listRecordingSubscriptions` (`:1266-1300`) is
        // decided **here**, unlike the other listings: both of its gates are the
        // session's own state — whether it is already being served, and whether
        // the page it asked for can exist — and neither needs the client or the
        // catalog a callback runs without.
        if self.has_active_listing(session_id) {
            self.pending.push(Deferred::Error {
                session_id,
                correlation_id,
                relevant_id: ACTIVE_LISTING,
                message: ACTIVE_LISTING_MSG.to_owned(),
            });
        } else if pseudo_index < 0
            || pseudo_index >= i32::try_from(self.recording_subscriptions.len()).unwrap_or(i32::MAX)
            || subscription_count <= 0
        {
            self.pending.push(Deferred::SubscriptionUnknown {
                session_id,
                correlation_id,
            });
        } else {
            self.listings
                .push(Listing::Subscriptions(SubscriptionListing {
                    control_session_id: session_id,
                    correlation_id,
                    pseudo_index,
                    subscription_count,
                    sent: 0,
                    stream_id,
                    apply_stream_id,
                    channel_fragment: channel_fragment.to_owned(),
                    is_done: false,
                }));
        }
    }

    fn on_stop_recording(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        stream_id: i32,
        original_channel: &str,
        _now_ms: i64,
    ) {
        self.pending.push(Deferred::Action(Action::StopRecording {
            session_id,
            correlation_id,
            stream_id,
            original_channel: original_channel.to_owned(),
        }));
    }

    fn on_stop_recording_by_identity(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        recording_id: i64,
        _now_ms: i64,
    ) {
        self.pending
            .push(Deferred::Action(Action::StopRecordingByIdentity {
                session_id,
                correlation_id,
                recording_id,
            }));
    }

    fn send_error_response(
        &mut self,
        session_id: i64,
        correlation_id: i64,
        relevant_id: i64,
        message: &str,
        _now_ms: i64,
    ) {
        self.pending.push(Deferred::Error {
            session_id,
            correlation_id,
            relevant_id,
            message: message.to_owned(),
        });
    }

    fn log_warning(&mut self, message: &str) {
        self.warnings.push(message.to_owned());
    }
}

/// The archive's conductor: the adapter, the sessions, and the per-turn loop.
///
/// The reference's `ArchiveConductor` is an `Agent` that owns far more —
/// recorder, replayer, catalog, replay tokens (`ArchiveConductor.java:125-127`,
/// `:364-396`). What is here is the control plane and the pieces it needs to
/// run: the adapter it dispatches through, the authenticator it lends to the
/// sessions, and the mark file it keeps alive.
///
/// # The driver is not driven from here
///
/// The reference's conductor drives the driver's shared agent
/// (`invokeDriverConductor`, `:426`), because in the reference the archive and
/// the driver are one process with one loop. Here they are two objects and the
/// launcher owns the loop, calling `Driver::do_work` and this in turn — which
/// is the same order, with the seam moved out one level.
pub struct ArchiveConductor {
    adapter: ControlAdapter<Box<dyn AuthorisationService>>,
    authenticator: Box<dyn Authenticator>,
    sessions: Sessions,
    mark_file: ArchiveMarkFile,
    /// `markFileUpdateDeadlineMs` (`:137`), one write per
    /// `MARK_FILE_UPDATE_INTERVAL_MS`.
    mark_file_update_deadline_ms: i64,
    /// `cachedEpochClock` (`:133`): the conductor does its per-turn work only
    /// when the millisecond clock has moved.
    cached_epoch_ms: i64,
    /// Where `logWarning` and this build's own complaints go
    /// (`ArchiveConductor.java:443-446`, via the error handler).
    error_counter: ErrorCounter,
    cnc: CncFile,
    /// The recordings the archive holds (`ArchiveConductor.java:196` and its
    /// `ctx.catalog()`).
    ///
    /// The conductor rather than the sessions, which is the reference's shape
    /// too — the catalog is the archive's, outlives every session, and is what
    /// the answers to the questions about a recording are read out of. It
    /// reaches [`Sessions::drive`] as an argument for the same reason the
    /// counters do.
    catalog: Catalog,
    /// The recordings in flight (`ArchiveConductor.java:180`, `:250`).
    ///
    /// Held here rather than by the sessions because that is where the reference
    /// holds it: its conductor owns the recorder and drives it on its own turn,
    /// **after** its own sessions (`SharedModeArchiveConductor.java:56-63`). The
    /// cost of the split is one parameter: two of the things a control session
    /// can ask for are things the recorder has to be told about
    /// (`:1766-1772`, `:1356-1359`), so [`Sessions::drive`] is lent it for the
    /// length of a turn.
    recorder: Recorder,
    command_timeout: Duration,
}

impl ArchiveConductor {
    /// Assemble a conductor over a driver that is already running.
    ///
    /// The subscriptions are passed in rather than made here: the reference
    /// makes them in its constructor (`:227-240`), but making one needs a
    /// client and an add that has to complete, and the launcher is where a
    /// client and its commands already live.
    ///
    /// `error_counter_id` is the driver's `ERRORS` counter
    /// (`ArchivingMediaDriver.java:87-89`), which the archive writes its
    /// errors into rather than keeping one of its own.
    pub fn new(
        config: &ArchiveConfig,
        cnc: CncFile,
        mark_file: ArchiveMarkFile,
        catalog: Catalog,
        error_counter_id: i32,
        remote_subscription_id: Option<i64>,
        local_subscription_id: i64,
    ) -> Result<Self, AuthError> {
        let authenticator = authenticator(&config.authenticator_supplier)?;
        let authorisation = authorisation_service(&config.authorisation_service_supplier)?;

        let mut subscription_ids = vec![local_subscription_id];
        if let Some(remote) = remote_subscription_id {
            subscription_ids.push(remote);
        }

        let sessions = Sessions::new(
            config.archive_id.unwrap_or(ARCHIVE_ID_DEFAULT),
            config.connect_timeout_ns / 1_000_000,
            config.session_liveness_check_interval_ns / 1_000_000,
            DEFAULT_TIMEOUT,
            ResponseChannelDefaults {
                term_buffer_length: config.control_term_buffer_length as i32,
                term_buffer_sparse: config.control_term_buffer_sparse,
                mtu_length: config.control_mtu_length.map(|mtu| mtu as i32),
            },
            RecordingSettings {
                // A negative setting is a bound nothing can be below, which is
                // what the reference's own `>=` against a negative `int` says
                // (`ArchiveConductor.java:538-543`).
                max_concurrent_recordings: usize::try_from(config.max_concurrent_recordings)
                    .unwrap_or(0),
                low_storage_space_threshold: config.low_storage_space_threshold,
                segment_file_length: config.segment_file_length,
                file_io_max_length: config.file_io_max_length,
                file_sync_level: config.file_sync_level,
                archive_dir: config.archive_dir.clone(),
            },
            ReplaySettings {
                max_concurrent_replays: usize::try_from(config.max_concurrent_replays).unwrap_or(0),
            },
            subscription_ids,
        );

        Ok(Self {
            adapter: ControlAdapter::new(
                remote_subscription_id,
                local_subscription_id,
                authorisation,
            ),
            authenticator,
            sessions,
            mark_file,
            mark_file_update_deadline_ms: 0,
            cached_epoch_ms: i64::MIN,
            error_counter: ErrorCounter::new(error_counter_id),
            cnc,
            catalog,
            recorder: Recorder::new(config.archive_id.unwrap_or(ARCHIVE_ID_DEFAULT)),
            command_timeout: DEFAULT_TIMEOUT,
        })
    }

    /// How many control sessions are live.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Mark the archive as stopped in its mark file
    /// (`ArchiveMarkFile.signalTerminated`, `:344-347`).
    ///
    /// That is the ready byte written back to `NULL_VALUE`, which is what tells
    /// a reader that the process is gone rather than merely quiet.
    pub fn signal_terminated(&self) {
        let _ = self.mark_file.signal_terminated();
    }

    /// One turn (`ArchiveConductor.java:364-396`).
    ///
    /// The driver's own turn is the launcher's to take; this is everything the
    /// archive does in the same pass, in the reference's order: the client is
    /// polled and the mark file kept alive when the millisecond clock moves,
    /// then the adapter reads the control subscriptions, then the sessions are
    /// driven.
    pub fn do_work(&mut self, client: &mut Client, now_ms: i64) -> Result<usize, ControlError> {
        let mut work = 0;

        if self.cached_epoch_ms != now_ms {
            self.cached_epoch_ms = now_ms;
            work += usize::from(client.poll());

            if now_ms >= self.mark_file_update_deadline_ms {
                self.mark_file_update_deadline_ms = now_ms + MARK_FILE_UPDATE_INTERVAL_MS;
                let _ = self.mark_file.update_activity_timestamp(now_ms);
            }
        }

        let Self {
            adapter,
            authenticator,
            sessions,
            cnc,
            catalog,
            recorder,
            error_counter,
            command_timeout,
            ..
        } = self;

        let Some(counters) = cnc.counters_writable() else {
            // Read-only CnC: there is nowhere to keep a counter, and the
            // reference's archive does not start without one
            // (`Archive.java:1375`). Nothing is driven, and the caller sees a
            // turn that did no work rather than a session that half-ran.
            return Ok(work);
        };

        work += adapter.poll(client, sessions, now_ms)?;

        sessions.drive(
            client,
            &counters,
            authenticator.as_mut(),
            catalog,
            recorder,
            now_ms,
        );

        // The recorder gets its own turn, and it is the turn **after** the
        // sessions (`SharedModeArchiveConductor.java:56-63`). What a session
        // that ended this turn owes is collected here, where the catalog and
        // the control sessions are.
        //
        // Its three counters are asked for here too, before it is driven, so
        // that the first turn which writes anything has somewhere to publish to
        // — the reference allocates them in `conclude` (`Archive.java:1587-1626`)
        // and this build spends a turn per add doing the same, by the same two
        // steps every counter here takes.
        recorder.allocate(client, &counters, *command_timeout);

        work += recorder.drive(client, &counters);

        for session in recorder.take_finished() {
            sessions.close_recording_session(client, catalog, &counters, recorder, session, now_ms);
        }

        // The adapter is told about every session that ended, and the image is
        // rejected only for the ones that ended badly. The image comes from
        // the adapter's own record rather than the session's, which is the one
        // the reference hands to `Image.reject`
        // (`ControlSessionAdapter.java:1152-1156`).
        for ended in sessions.take_ended() {
            let image = adapter.remove_session(ended.session_id);

            if !ended.aborted {
                continue;
            }

            let Some(image) = image else {
                continue;
            };

            let Some(position) = sessions.image_position(client, image) else {
                // The image is gone, so there is nothing left to reject.
                continue;
            };

            let reason = ended.abort_reason.as_deref().unwrap_or(SESSION_CLOSED_MSG);
            let _ = client.reject_image(image.correlation_id(), position, reason, *command_timeout);
        }

        for warning in sessions.take_warnings() {
            eprintln!("archive: {warning}");
            error_counter.increment(&counters);
        }

        Ok(work)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::catalog::Recording;
    use crate::mark::tests::TempDir;

    use deepmsg_cnc::layout;
    use deepmsg_core::buffer::AtomicBuffer;

    /// A counters region pair, sized to reach `counter_id`.
    #[repr(align(64))]
    struct Region(Vec<u8>);

    impl Region {
        fn zeroed(len: usize) -> Self {
            Self(vec![0u8; len])
        }

        fn buffer_mut(&mut self) -> AtomicBuffer<'_, ReadWrite> {
            AtomicBuffer::from_slice_mut(&mut self.0).expect("aligned region")
        }
    }

    /// One **allocated** counter at `counter_id` with this type and
    /// registration, and a value of `value`.
    fn limit_counters(
        counter_id: i32,
        type_id: i32,
        registration_id: i64,
        value: i64,
    ) -> (Region, Region) {
        let slots = counter_id as usize + 1;
        let mut meta = Region::zeroed(slots * layout::COUNTER_METADATA_LENGTH);
        let mut vals = Region::zeroed(slots * layout::COUNTER_VALUE_LENGTH);
        let base = counter_id as usize * layout::COUNTER_METADATA_LENGTH;

        let put = |bytes: &mut [u8], offset: usize, value: &[u8]| {
            bytes[base + offset..base + offset + value.len()].copy_from_slice(value);
        };

        // The slots in front of it are `RECLAIMED` rather than `UNUSED`, and
        // that is not decoration: `CountersReader::for_each` **stops** at the
        // first unused slot, so a region with one zeroed slot in front of the
        // counter would never reach it.
        for slot in 0..counter_id as usize {
            meta.0[slot * layout::COUNTER_METADATA_LENGTH + layout::COUNTER_STATE_OFFSET
                ..slot * layout::COUNTER_METADATA_LENGTH + layout::COUNTER_STATE_OFFSET + 4]
                .copy_from_slice(&layout::COUNTER_STATE_RECLAIMED.to_le_bytes());
        }

        put(
            &mut meta.0,
            layout::COUNTER_TYPE_ID_OFFSET,
            &type_id.to_le_bytes(),
        );
        // Published last, as a writer does.
        put(
            &mut meta.0,
            layout::COUNTER_STATE_OFFSET,
            &layout::COUNTER_STATE_ALLOCATED.to_le_bytes(),
        );

        // The value **and the registration id** are in the values region, not
        // the metadata one — `CountersReader::describe` reads both from there,
        // which is what `aeron_counters_manager` does too.
        let base = counter_id as usize * layout::COUNTER_VALUE_LENGTH;
        let put_value = |bytes: &mut [u8], offset: usize, value: i64| {
            bytes[base + offset..base + offset + 8].copy_from_slice(&value.to_le_bytes());
        };

        put_value(&mut vals.0, layout::COUNTER_VALUE_OFFSET, value);
        put_value(
            &mut vals.0,
            layout::COUNTER_REGISTRATION_ID_OFFSET,
            registration_id,
        );

        (meta, vals)
    }

    /// A bounded replay's limit counter is resolved from the slot the request
    /// names, and **refused** when the slot is not there
    /// (`ArchiveConductor.startBoundedReplay`, `:996-1035`).
    #[test]
    fn a_limit_counter_is_resolved_from_its_slot() {
        const TYPE_ID: i32 = 10001;
        const REGISTRATION_ID: i64 = 777;

        let (mut meta, mut vals) = limit_counters(3, TYPE_ID, REGISTRATION_ID, 640);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        assert_eq!(
            Some(LimitCounter {
                counter_id: 3,
                type_id: TYPE_ID,
                registration_id: REGISTRATION_ID,
            }),
            resolve_limit_counter(&counters, 3)
        );

        // A slot nothing was ever allocated in, and an id no `i32` can hold.
        assert_eq!(None, resolve_limit_counter(&counters, 2));
        assert_eq!(
            None,
            resolve_limit_counter(&counters, i64::from(i32::MAX) + 1)
        );
    }

    /// The limit moves while the slot is still ours, and **freezes** when it
    /// stops being ours — a reused slot is somebody else's counter
    /// (`ReplaySession.notExtended`, `:544-578`).
    #[test]
    fn a_reused_limit_counter_slot_stops_moving() {
        const TYPE_ID: i32 = 10001;
        const REGISTRATION_ID: i64 = 777;

        let (mut meta, mut vals) = limit_counters(3, TYPE_ID, REGISTRATION_ID, 640);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());
        let limit = LimitCounter {
            counter_id: 3,
            type_id: TYPE_ID,
            registration_id: REGISTRATION_ID,
        };

        assert_eq!(Some(640), read_limit_counter(&counters, limit));

        // Another kind of counter in the same slot: id reuse, which is the case
        // the registration id exists for.
        let (mut meta, mut vals) = limit_counters(3, TYPE_ID + 1, REGISTRATION_ID, 640);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        assert_eq!(None, read_limit_counter(&counters, limit));

        // And a different registration of the same type.
        let (mut meta, mut vals) = limit_counters(3, TYPE_ID, REGISTRATION_ID + 1, 640);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        assert_eq!(None, read_limit_counter(&counters, limit));
    }

    /// A catalog holding one recording, so that a question about it has
    /// something to read. Its fields are the ones the questions read, and the
    /// values are distinct so that a question answered with the wrong one
    /// shows.
    fn catalog_with_a_recording() -> (TempDir, Catalog) {
        let dir = TempDir::new();
        let mut catalog =
            Catalog::create(dir.path(), crate::catalog::DEFAULT_CAPACITY, 0).expect("a catalog");

        catalog
            .add_recording(&Recording {
                recording_id: 0,
                start_timestamp: 1_000,
                stop_timestamp: 2_000,
                start_position: 4096,
                stop_position: 8192,
                initial_term_id: 3,
                segment_file_length: 128 * 1024,
                term_buffer_length: 64 * 1024,
                mtu_length: 1408,
                session_id: 1001,
                stream_id: 33,
                stripped_channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
                original_channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
                source_identity: "aeron:ipc".to_owned(),
            })
            .expect("added");

        (dir, catalog)
    }

    /// A catalog holding one recording per `(stream_id, original_channel)` pair,
    /// added in the order given — so the ids it hands out are 0, 1, 2, … in
    /// that order, which is what a page of them is walked in.
    fn catalog_with_recordings(recordings: &[(i32, &str)]) -> (TempDir, Catalog) {
        let dir = TempDir::new();
        let mut catalog =
            Catalog::create(dir.path(), crate::catalog::DEFAULT_CAPACITY, 0).expect("a catalog");

        for (index, (stream_id, channel)) in recordings.iter().enumerate() {
            let index = i64::try_from(index).expect("small");

            catalog
                .add_recording(&Recording {
                    recording_id: 0,
                    start_timestamp: 1_000 + index,
                    stop_timestamp: 2_000 + index,
                    start_position: (index + 1) * 4096,
                    stop_position: (index + 1) * 8192,
                    initial_term_id: 3,
                    segment_file_length: 128 * 1024,
                    term_buffer_length: 64 * 1024,
                    mtu_length: 1408,
                    session_id: 1001,
                    stream_id: *stream_id,
                    stripped_channel: (*channel).to_owned(),
                    original_channel: (*channel).to_owned(),
                    source_identity: "aeron:ipc".to_owned(),
                })
                .expect("added");
        }

        (dir, catalog)
    }

    /// The recording a descriptor body is about, read back the way the client
    /// reads it — the body is the descriptor as the catalog holds it, so this
    /// is what a page is checked by.
    fn descriptor_recording_id(body: &[u8]) -> i64 {
        use deepmsg_codec::archive::recording_descriptor_codec::{
            self, RecordingDescriptorDecoder,
        };
        use deepmsg_codec::archive::{ReadBuf, SBE_SCHEMA_VERSION};

        RecordingDescriptorDecoder::default()
            .wrap(
                ReadBuf::new(body),
                0,
                recording_descriptor_codec::SBE_BLOCK_LENGTH,
                SBE_SCHEMA_VERSION,
            )
            .recording_id()
    }

    /// A page of every recording there is, asked for from the lowest id there
    /// is — which is what the C client sends for template 8
    /// (`aeron_archive_proxy.c:505-528`).
    fn recordings_listing(count: i32) -> RecordingsListing {
        RecordingsListing {
            control_session_id: 3,
            correlation_id: 7,
            from_recording_id: i64::MIN,
            count,
            sent: 0,
            filter: RecordingsFilter::All,
            is_done: false,
        }
    }

    /// One turn of `walk_recordings` with a client that takes everything, and
    /// the recordings that went out — read back out of the descriptors rather
    /// than assumed, so a page that answers about the wrong record shows.
    fn walk_taking(
        listing: &mut RecordingsListing,
        catalog: &Catalog,
    ) -> (RecordingsWalk, Vec<i64>) {
        let mut sent = Vec::new();
        let walk = walk_recordings(listing, catalog, |body| {
            sent.push(descriptor_recording_id(&body));
            true
        });

        (walk, sent)
    }

    /// Each question reads the field the reference reads, and the two that fall
    /// back do so with the reference's own values
    /// (`ArchiveConductor.java:1159-1195`).
    #[test]
    fn a_question_about_a_recording_is_answered_from_the_catalog() {
        let (_dir, catalog) = catalog_with_a_recording();

        assert_eq!(
            Ok(4096),
            answer_query(
                &catalog,
                &HashMap::new(),
                &counters_region(),
                &Query::StartPosition { recording_id: 0 }
            )
        );
        assert_eq!(
            Ok(8192),
            answer_query(
                &catalog,
                &HashMap::new(),
                &counters_region(),
                &Query::StopPosition { recording_id: 0 }
            )
        );
        assert_eq!(
            Ok(NULL_POSITION),
            answer_query(
                &catalog,
                &HashMap::new(),
                &counters_region(),
                &Query::RecordingPosition { recording_id: 0 }
            ),
            "a recording that is not in flight has no position to report"
        );
        assert_eq!(
            Ok(8192),
            answer_query(
                &catalog,
                &HashMap::new(),
                &counters_region(),
                &Query::MaxRecordedPosition { recording_id: 0 }
            ),
            "which for one that is not active is where it stopped"
        );
    }

    /// The refusal is the one the C client prints — `errorCode=5, error:
    /// unknown recording id: <id>` — and it is asserted here **word for word**,
    /// because the acceptance test asserts the same line
    /// (`aeron_archive_test.cpp:1125`).
    #[test]
    fn a_question_about_a_recording_the_catalog_does_not_hold_is_refused() {
        let (_dir, catalog) = catalog_with_a_recording();

        assert_eq!(5, UNKNOWN_RECORDING);
        assert_eq!(
            Err("unknown recording id: 12345".to_owned()),
            answer_query(
                &catalog,
                &HashMap::new(),
                &counters_region(),
                &Query::StartPosition {
                    recording_id: 12345
                }
            )
        );

        // A negative id is refused by the guard rather than by the lookup
        // (`Catalog.hasRecording`, `Catalog.java:495-498`).
        assert_eq!(
            Err(format!("unknown recording id: {}", i64::MIN)),
            answer_query(
                &catalog,
                &HashMap::new(),
                &counters_region(),
                &Query::MaxRecordedPosition {
                    recording_id: i64::MIN
                }
            )
        );
    }

    /// A match is answered with the id the catalog found, and with **`-1`** when
    /// nothing matched — an `OK` carrying a value, not the refusal the four
    /// questions about a named recording get (`ArchiveConductor.java:754-761`).
    ///
    /// The fixture catalog holds one recording: session 1001, stream 33, on
    /// `aeron:udp?endpoint=localhost:3333`.
    #[test]
    fn a_match_is_answered_with_an_id_or_with_minus_one() {
        let (_dir, catalog) = catalog_with_a_recording();

        assert_eq!(
            Ok(0),
            answer_query(
                &catalog,
                &HashMap::new(),
                &counters_region(),
                &Query::FindLastMatching {
                    min_recording_id: 0,
                    session_id: 1001,
                    stream_id: 33,
                    channel_fragment: b"endpoint=localhost:3333".to_vec(),
                }
            )
        );

        assert_eq!(
            Ok(0),
            answer_query(
                &catalog,
                &HashMap::new(),
                &counters_region(),
                &Query::FindLastMatching {
                    min_recording_id: 0,
                    session_id: 1001,
                    stream_id: 33,
                    channel_fragment: Vec::new(),
                }
            ),
            "an empty fragment is in every channel"
        );

        assert_eq!(
            Ok(NULL_RECORD_ID),
            answer_query(
                &catalog,
                &HashMap::new(),
                &counters_region(),
                &Query::FindLastMatching {
                    min_recording_id: 0,
                    session_id: 1001,
                    stream_id: 33,
                    channel_fragment: b"endpoint=localhost:4444".to_vec(),
                }
            ),
            "a match that is not there is an answer, and -1 is it"
        );

        for (session_id, stream_id) in [(1002, 33), (1001, 34)] {
            assert_eq!(
                Ok(NULL_RECORD_ID),
                answer_query(
                    &catalog,
                    &HashMap::new(),
                    &counters_region(),
                    &Query::FindLastMatching {
                        min_recording_id: 0,
                        session_id,
                        stream_id,
                        channel_fragment: Vec::new(),
                    }
                ),
                "session {session_id}, stream {stream_id}"
            );
        }

        assert_eq!(
            Ok(NULL_RECORD_ID),
            answer_query(
                &catalog,
                &HashMap::new(),
                &counters_region(),
                &Query::FindLastMatching {
                    min_recording_id: 1,
                    session_id: 1001,
                    stream_id: 33,
                    channel_fragment: Vec::new(),
                }
            ),
            "the floor excludes the one recording there is"
        );
    }

    /// A negative floor is refused before the catalog is asked, in the
    /// reference's own words (`ArchiveConductor.java:749-753`).
    #[test]
    fn a_negative_floor_is_refused_with_the_references_line() {
        let (_dir, catalog) = catalog_with_a_recording();

        assert_eq!(
            Err("minRecordingId=-1 < 0".to_owned()),
            answer_query(
                &catalog,
                &HashMap::new(),
                &counters_region(),
                &Query::FindLastMatching {
                    min_recording_id: -1,
                    session_id: 1,
                    stream_id: 2,
                    channel_fragment: Vec::new(),
                }
            )
        );
    }

    /// A segment is never shorter than a term, whatever the archive was
    /// configured with (`ArchiveConductor.java:2006`).
    #[test]
    fn a_segment_holds_at_least_one_term() {
        assert_eq!(64 * 1024, segment_file_length(64 * 1024, 64 * 1024));
        assert_eq!(
            64 * 1024,
            segment_file_length(16 * 1024, 64 * 1024),
            "a configured length below the driver's term does not shrink the segment"
        );
        assert_eq!(
            128 * 1024,
            segment_file_length(128 * 1024, 64 * 1024),
            "and one above it is left alone"
        );
    }

    /// `makeKey` is the reference's own string, character for character —
    /// including the last line, which strikes off the `?` when the channel
    /// carries none of the five parameters (`ArchiveConductor.java:1945`).
    #[test]
    fn a_recording_key_is_the_references_string() {
        let parsed = |channel: &str| ChannelUri::parse(channel).expect("a channel");

        assert_eq!(
            "33:ipc",
            make_key(33, &parsed("aeron:ipc")),
            "the last symbol comes off whether it was a separator or the `?`"
        );

        assert_eq!(
            "33:udp?endpoint=localhost:3333",
            make_key(33, &parsed("aeron:udp?endpoint=localhost:3333"))
        );

        // The five, in the reference's order, whatever order the channel wrote
        // them in — and nothing else survives.
        assert_eq!(
            "33:udp?endpoint=localhost:3333|interface=eth0|control=localhost:4040\
             |session-id=7|tags=a,b",
            make_key(
                33,
                &parsed(
                    "aeron:udp?tags=a,b|endpoint=localhost:3333|session-id=7\
                     |control=localhost:4040|interface=eth0|mtu=1408|term-length=64k"
                )
            ),
            "the parameters the key does not carry are what a key is not about"
        );

        // Two channels that differ only in a parameter the key carries no trace
        // of are one recording, which is the whole of what the key decides.
        assert_eq!(
            make_key(33, &parsed("aeron:udp?endpoint=localhost:3333")),
            make_key(33, &parsed("aeron:udp?endpoint=localhost:3333|mtu=1408"))
        );
    }

    /// The five refusals a start can answer with, and the channel a `LOCAL` UDP
    /// publication is recorded through
    /// (`ArchiveConductor.java:538-577`).
    #[test]
    fn a_start_is_decided_by_four_checks_and_a_spy() {
        // Built before the one below shadows the helper: a start that has hit
        // the bound counts recordings, not what has been asked for
        // (`:538-543`).
        let mut full = sessions();
        full.num_active_recordings = 20;

        let mut sessions = sessions();

        // Nothing registered: a local UDP channel is subscribed to through a
        // spy link, and the channel is the *stripped* one.
        assert_eq!(
            StartDecision::Add {
                key: "33:udp?endpoint=localhost:3333".to_owned(),
                channel: "aeron-spy:aeron:udp?endpoint=localhost:3333".to_owned(),
                stripped_channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
            },
            sessions.decide_start(
                33,
                SourceLocation::LOCAL,
                "aeron:udp?endpoint=localhost:3333"
            )
        );

        // An IPC channel is not a network one either way: it is subscribed to
        // as it is (`:559-560` tests the media, not the location alone).
        assert_eq!(
            StartDecision::Add {
                key: "33:ipc".to_owned(),
                channel: "aeron:ipc".to_owned(),
                stripped_channel: "aeron:ipc".to_owned(),
            },
            sessions.decide_start(33, SourceLocation::LOCAL, "aeron:ipc")
        );

        // A remote archive records the network channel itself: there is no
        // driver here to spy on.
        assert_eq!(
            StartDecision::Add {
                key: "33:udp?endpoint=localhost:3333".to_owned(),
                channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
                stripped_channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
            },
            sessions.decide_start(
                33,
                SourceLocation::REMOTE,
                "aeron:udp?endpoint=localhost:3333"
            )
        );

        // A channel that will not parse is the reference's `catch` (`:578-582`),
        // whose message here is this build's.
        assert_eq!(
            StartDecision::Refuse {
                relevant_id: 0,
                message: "not-a-channel is not a channel".to_owned(),
            },
            sessions.decide_start(33, SourceLocation::LOCAL, "not-a-channel")
        );

        // The same channel and stream twice is one recording (`:574-577`).
        sessions
            .recording_subscriptions
            .insert("33:udp?endpoint=localhost:3333".to_owned(), held(11));
        assert_eq!(
            StartDecision::Refuse {
                relevant_id: ACTIVE_SUBSCRIPTION,
                message:
                    "recording exists for streamId=33 channel=aeron:udp?endpoint=localhost:3333"
                        .to_owned(),
            },
            sessions.decide_start(
                33,
                SourceLocation::LOCAL,
                "aeron:udp?endpoint=localhost:3333"
            )
        );

        assert_eq!(
            StartDecision::Refuse {
                relevant_id: MAX_RECORDINGS,
                message: "max concurrent recordings reached 20".to_owned(),
            },
            full.decide_start(
                33,
                SourceLocation::LOCAL,
                "aeron:udp?endpoint=localhost:3333"
            )
        );
    }

    /// A stop named by channel and stream resolves to the registration id the
    /// **key** names, and refuses in this arm's own words when there is none
    /// (`ArchiveConductor.java:589-604` — which are not the other arm's,
    /// `:621-622`).
    #[test]
    fn a_stop_by_channel_finds_the_subscription_its_key_names() {
        let mut sessions = sessions();

        assert_eq!(
            StopDecision::Refuse {
                relevant_id: UNKNOWN_SUBSCRIPTION,
                message:
                    "no recording found for streamId=33 channel=aeron:udp?endpoint=localhost:3333"
                        .to_owned(),
            },
            sessions.decide_stop(33, "aeron:udp?endpoint=localhost:3333"),
            "nothing registered under that key"
        );

        sessions
            .recording_subscriptions
            .insert("33:udp?endpoint=localhost:3333".to_owned(), held(11));

        assert_eq!(
            StopDecision::Subscription(11),
            sessions.decide_stop(33, "aeron:udp?endpoint=localhost:3333")
        );

        // The same channel on another stream is another key, and the parameters
        // the key does not carry are not part of it — `makeKey` decides both,
        // which is why this arm asks the same question `start` did.
        assert_eq!(
            StopDecision::Subscription(11),
            sessions.decide_stop(33, "aeron:udp?endpoint=localhost:3333|mtu=1408")
        );
        assert!(matches!(
            sessions.decide_stop(34, "aeron:udp?endpoint=localhost:3333"),
            StopDecision::Refuse { .. }
        ));

        // A channel that will not parse is the reference's `catch` (`:606-609`).
        assert_eq!(
            StopDecision::Refuse {
                relevant_id: 0,
                message: "not-a-channel is not a channel".to_owned(),
            },
            sessions.decide_stop(33, "not-a-channel")
        );
    }

    /// The six gates in front of an extend (`ArchiveConductor.java:1076-1118`),
    /// and the two that are *nearly* the start's:
    ///
    /// * the concurrent-recording refusal says "reached **at** N" here and
    ///   "reached N" there (`:1079` against `:540`) — one word;
    /// * and the spy test is over the **channel string**
    ///   (`originalChannel.contains("udp")`, `:1130`) where the start asks the
    ///   parsed channel (`:559-560`).
    #[test]
    fn an_extend_is_gated_by_six_checks_of_its_own() {
        let (_dir, catalog) = catalog_with_a_recording();

        // Built before the first binding shadows the helper.
        let mut live = sessions();
        live.recording_session_by_id.insert(
            0,
            RecordingHandle {
                position: RecordingPos::for_test(1),
                subscription_id: 11,
            },
        );

        let mut full = sessions();
        full.num_active_recordings = 20;

        let sessions = sessions();

        // The recording the fixture holds is stream 33 on a UDP endpoint, so
        // this is an extend that gets all the way through.
        assert_eq!(
            StartDecision::Add {
                key: "33:udp?endpoint=localhost:3333".to_owned(),
                channel: "aeron-spy:aeron:udp?endpoint=localhost:3333".to_owned(),
                stripped_channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
            },
            sessions.decide_extend(
                &catalog,
                0,
                33,
                SourceLocation::LOCAL,
                "aeron:udp?endpoint=localhost:3333"
            )
        );

        // A channel that carries "udp" anywhere is a spy's, which is the
        // reference's substring test and not the parsed media.
        assert!(matches!(
            sessions.decide_extend(
                &catalog,
                0,
                33,
                SourceLocation::LOCAL,
                "aeron:ipc?alias=udp"
            ),
            StartDecision::Add { .. }
        ));

        // A remote archive records the network channel itself.
        assert_eq!(
            StartDecision::Add {
                key: "33:udp?endpoint=localhost:3333".to_owned(),
                channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
                stripped_channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
            },
            sessions.decide_extend(
                &catalog,
                0,
                33,
                SourceLocation::REMOTE,
                "aeron:udp?endpoint=localhost:3333"
            )
        );

        // An id the catalog does not hold (`:1084-1090`).
        assert_eq!(
            StartDecision::Refuse {
                relevant_id: UNKNOWN_RECORDING,
                message: "unknown recording id: 7".to_owned(),
            },
            sessions.decide_extend(
                &catalog,
                7,
                33,
                SourceLocation::LOCAL,
                "aeron:udp?endpoint=localhost:3333"
            )
        );

        // Another stream than the recording's (`:1091-1098`), which is the same
        // refusal id and a different message.
        assert_eq!(
            StartDecision::Refuse {
                relevant_id: UNKNOWN_RECORDING,
                message: "cannot extend recording 0 with streamId=34 for existing streamId=33"
                    .to_owned(),
            },
            sessions.decide_extend(
                &catalog,
                0,
                34,
                SourceLocation::LOCAL,
                "aeron:udp?endpoint=localhost:3333"
            )
        );

        // A recording in flight cannot be extended (`:1099-1104`).
        assert_eq!(
            StartDecision::Refuse {
                relevant_id: ACTIVE_RECORDING,
                message: "cannot extend active recording 0".to_owned(),
            },
            live.decide_extend(
                &catalog,
                0,
                33,
                SourceLocation::LOCAL,
                "aeron:udp?endpoint=localhost:3333"
            )
        );

        // And the bound, whose words are this arm's own (`:1079`).
        assert_eq!(
            StartDecision::Refuse {
                relevant_id: MAX_RECORDINGS,
                message: "max concurrent recordings reached at 20".to_owned(),
            },
            full.decide_extend(
                &catalog,
                0,
                33,
                SourceLocation::LOCAL,
                "aeron:udp?endpoint=localhost:3333"
            )
        );
    }

    /// The free-space refusal is the reference's line, word for word
    /// (`ArchiveConductor.java:2606`), and it is asked of the **archive
    /// directory's** filesystem.
    ///
    /// A directory that cannot be asked about answers zero — so an archive whose
    /// directory is gone refuses starts rather than accepting them blind, which
    /// is the reference's own answer to a failed `statvfs` one step further in.
    #[test]
    fn a_start_below_the_storage_threshold_is_refused() {
        let mut sessions = sessions();

        assert_eq!(None, sessions.is_low_storage_space(), "the temp directory");

        let usable = usable_space(&sessions.recording.archive_dir);
        sessions.recording.low_storage_space_threshold = usable;

        assert_eq!(
            Some(format!(
                "low storage threshold={usable} <= usableSpace={usable}"
            )),
            sessions.is_low_storage_space(),
            "the reference refuses at `<=`, not at `<`"
        );

        sessions.recording.archive_dir = PathBuf::from("/no/such/archive/directory");
        assert_eq!(
            Some(format!("low storage threshold={usable} <= usableSpace=0")),
            sessions.is_low_storage_space(),
            "a filesystem that cannot be asked about answers zero, which is below any threshold"
        );
    }

    /// The registry is **keyed by `makeKey` and searched by registration id**,
    /// which is the one thing about it that is not the reference's shape: its
    /// handler closure simply *is* the subscription it was made for (`:562-565`),
    /// and this build's image events carry the id instead.
    #[test]
    fn a_subscription_is_found_by_the_id_an_image_arrives_for() {
        let mut sessions = sessions();

        sessions
            .recording_subscriptions
            .insert("33:ipc".to_owned(), held(11));
        sessions
            .recording_subscriptions
            .insert("34:udp?endpoint=localhost:3333".to_owned(), held(22));

        assert_eq!(
            Some(11),
            sessions
                .subscription_by_registration_id(11)
                .map(|held| held.registration_id)
        );
        assert_eq!(
            Some(22),
            sessions
                .subscription_by_registration_id(22)
                .map(|held| held.registration_id)
        );
        assert_eq!(
            None,
            sessions.subscription_by_registration_id(33),
            "an image for something this archive does not record is not an error"
        );
    }

    /// A stop takes the subscription out of the registry by **id**, which is
    /// the only thing a client holds (`ArchiveConductor.java:2139-2153`).
    ///
    /// The count of active recordings is deliberately **not** touched: it counts
    /// live recording sessions, not registrations (`:569` and `:1361` are its
    /// two movements and this is neither).
    #[test]
    fn a_stop_finds_the_subscription_by_its_id() {
        let mut sessions = sessions();
        sessions
            .recording_subscriptions
            .insert("33:udp?endpoint=localhost:3333".to_owned(), held(11));
        sessions
            .recording_subscriptions
            .insert("34:ipc".to_owned(), held(22));
        sessions.num_active_recordings = 2;

        assert_eq!(
            Some("34:ipc".to_owned()),
            sessions.remove_recording_subscription(22)
        );
        assert_eq!(1, sessions.recording_subscriptions.len());

        assert_eq!(None, sessions.remove_recording_subscription(22));
        assert_eq!(1, sessions.recording_subscriptions.len(), "nothing moved");

        assert_eq!(
            Some("33:udp?endpoint=localhost:3333".to_owned()),
            sessions.remove_recording_subscription(11)
        );
        assert!(sessions.recording_subscriptions.is_empty());
        assert_eq!(
            2, sessions.num_active_recordings,
            "a stopped subscription is a recording session that has not ended"
        );
    }

    /// The two requests that move something are recorded, not carried out: what
    /// they need is the client, and a callback runs without it.
    #[test]
    fn a_recording_request_is_recorded_rather_than_started() {
        let mut sessions = sessions();

        sessions.on_start_recording(
            StartRecordingRequest {
                session_id: 3,
                correlation_id: 7,
                stream_id: 33,
                source_location: SourceLocation::LOCAL,
                auto_stop: true,
                original_channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
            },
            0,
        );
        sessions.on_stop_recording_subscription(3, 8, 11, 0);

        assert_eq!(
            vec![
                Deferred::Action(Action::StartRecording(StartRecordingRequest {
                    session_id: 3,
                    correlation_id: 7,
                    stream_id: 33,
                    source_location: SourceLocation::LOCAL,
                    auto_stop: true,
                    original_channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
                })),
                Deferred::Action(Action::StopRecordingSubscription {
                    session_id: 3,
                    correlation_id: 8,
                    subscription_id: 11,
                }),
            ],
            sessions.pending
        );
        assert!(
            sessions.recording_subscriptions.is_empty(),
            "nothing is registered until the driver answers the add"
        );
    }

    /// The filter a subscription listing walks by
    /// (`ListRecordingSubscriptionsSession.doWork`, `:104-105`): a **substring**
    /// of the channel the subscription was added on, and the stream only when
    /// the client says the stream is part of the question.
    #[test]
    fn a_subscription_listing_filters_by_a_piece_of_channel_and_maybe_a_stream() {
        let mut ipc = held(11);
        ipc.channel = "aeron:ipc".to_owned();
        ipc.stream_id = 33;

        let mut udp = held(22);
        udp.channel = "aeron-spy:aeron:udp?endpoint=localhost:5678".to_owned();
        udp.stream_id = 34;

        // The channel fragment, with the stream not part of the question.
        assert!(matches_listing(&ipc, 33, false, ""));
        assert!(
            matches_listing(&udp, 33, false, ""),
            "an empty fragment is in every channel, which is what the C suite's second query passes"
        );
        assert!(matches_listing(&ipc, 33, false, "ipc"));
        assert!(
            !matches_listing(&udp, 33, false, "ipc"),
            "a substring **of the channel it was added on**"
        );
        assert!(
            matches_listing(&udp, 33, false, "spy:aeron:udp"),
            "so the spy prefix a local recording carries is part of what a client matches on"
        );

        // And the stream, when the client says so.
        assert!(matches_listing(&ipc, 33, true, ""));
        assert!(
            !matches_listing(&udp, 33, true, ""),
            "another stream is not this one"
        );
        assert!(matches_listing(&udp, 34, true, ""));
        assert_eq!(
            2,
            [&ipc, &udp]
                .iter()
                .filter(|subscription| matches_listing(subscription, 33, false, ""))
                .count(),
            "which is the whole of what `applyStreamId` decides"
        );
    }

    /// A listing request is recorded, not started: what it needs first is the
    /// catalog and the session, and a callback holds neither.
    #[test]
    fn a_listing_request_is_recorded_rather_than_started() {
        let mut sessions = sessions();
        sessions.on_list_recording(3, 7, 100, 0);

        assert_eq!(
            vec![Deferred::ListRecording {
                session_id: 3,
                correlation_id: 7,
                recording_id: 100,
            }],
            sessions.pending
        );
        assert!(sessions.listings.is_empty(), "nothing is served yet");
    }

    /// The three answers `ArchiveConductor.listRecording` can give
    /// (`ArchiveConductor.java:688-706`): the listing itself, a refusal because
    /// the asking session is already being served, and `RECORDING_UNKNOWN` for a
    /// recording the catalog does not hold.
    ///
    /// What is asserted is the **decision**, which is what can be wrong; the two
    /// refusal messages and the descriptor are asserted where they are built
    /// (`control_session`'s tests and `response_proxy`'s).
    #[test]
    fn a_listing_is_started_once_per_session_and_only_for_a_recording_there_is() {
        let (_dir, catalog) = catalog_with_a_recording();
        let mut sessions = sessions();

        assert_eq!(
            None,
            sessions.start_listing(3, 7, 0, &catalog),
            "a recording the catalog holds is served"
        );
        assert_eq!(
            vec![Listing::Recording(RecordingListing {
                control_session_id: 3,
                correlation_id: 7,
                recording_id: 0,
                is_done: false,
            })],
            sessions.listings
        );
        assert!(sessions.has_active_listing(3));

        // The same session asking again: one listing per session, which is what
        // `hasActiveListing` is for (`:690-694`). A *different* session may
        // ask, and gets its own.
        assert_eq!(
            Some(ListingRefusal::ActiveListing),
            sessions.start_listing(3, 8, 0, &catalog)
        );
        assert_eq!(None, sessions.start_listing(4, 9, 0, &catalog));
        assert_eq!(2, sessions.listings.len());
        let second = match &sessions.listings[1] {
            Listing::Recording(listing) => listing.correlation_id,
            Listing::Subscriptions(listing) => listing.correlation_id,
            Listing::Recordings(listing) => listing.correlation_id,
        };
        assert_eq!(9, second);
        assert!(!sessions.has_active_listing(5));

        // A recording that is not there is not a listing, wherever the id came
        // from: `hasRecording` refuses it (`:695-698`).
        assert_eq!(
            Some(ListingRefusal::UnknownRecording),
            sessions.start_listing(5, 10, 1, &catalog),
            "the id next to the one the catalog holds"
        );
        assert_eq!(
            Some(ListingRefusal::UnknownRecording),
            sessions.start_listing(5, 11, -1, &catalog)
        );
        assert_eq!(2, sessions.listings.len());
    }

    /// One recording per page, and the page the client asked for is what ends
    /// the walk — either the catalog running out, which the client is told
    /// about, or the page filling up, which it is not
    /// (`AbstractListRecordingsSession.java:107-113`, `:136-139`).
    #[test]
    fn a_page_of_recordings_ends_with_the_catalog_or_with_the_page_being_full() {
        let (_dir, catalog) = catalog_with_recordings(&[
            (33, "aeron:udp?endpoint=localhost:3333"),
            (33, "aeron:udp?endpoint=localhost:3334"),
        ]);

        // A page bigger than the catalog: both recordings go out, and the
        // client is then told there is nothing more. The name in that answer is
        // the cursor — one past the last recording there was (`:129-132`) —
        // and the listing is left for the caller to finish, because sending the
        // answer is the caller's.
        let mut listing = recordings_listing(10);
        let (walk, sent) = walk_taking(&mut listing, &catalog);
        assert_eq!(RecordingsWalk::Unknown(2), walk);
        assert_eq!(vec![0, 1], sent);
        assert_eq!(2, listing.sent);
        assert!(!listing.is_done);

        // A page of one: one descriptor and **no** terminal answer, because the
        // client reads `count` descriptors as the whole of its page. This is
        // the second call in `shouldFindMultipleRecordingDescriptors`
        // (`aeron_archive_test.cpp:2450-2457`), and getting it wrong is what
        // makes the next request look like the previous page is unfinished.
        let mut listing = recordings_listing(1);
        let (walk, sent) = walk_taking(&mut listing, &catalog);
        assert_eq!(RecordingsWalk::Idle, walk);
        assert_eq!(vec![0], sent);
        assert!(listing.is_done);
    }

    /// The filter is the whole of what separates the reference's two listing
    /// sessions, and it takes a recording only when **both** halves hold —
    /// which is what the three calls of
    /// `shouldFindRecordingDescriptorForUri` (`aeron_archive_test.cpp:2468-2561`)
    /// tell apart.
    #[test]
    fn a_page_for_a_uri_is_narrowed_by_stream_and_by_channel_fragment() {
        let (_dir, catalog) = catalog_with_recordings(&[
            (33, "aeron:udp?endpoint=localhost:3333"),
            (33, "aeron:udp?endpoint=localhost:3334"),
        ]);

        let for_uri = |stream_id: i32, fragment: &str, count: i32| RecordingsListing {
            filter: RecordingsFilter::Uri {
                stream_id,
                channel_fragment: fragment.as_bytes().to_vec(),
            },
            ..recordings_listing(count)
        };

        // One channel, and a page of two: the second recording is the only one
        // taken, and the page is not full, so the walk ends by telling the
        // client there is nothing more.
        let mut listing = for_uri(33, "3334", 2);
        let (walk, sent) = walk_taking(&mut listing, &catalog);
        assert_eq!(RecordingsWalk::Unknown(2), walk);
        assert_eq!(vec![1], sent);

        // A fragment of both: still one filter, two recordings, and now the
        // page fills up — which is a different ending again.
        let mut listing = for_uri(33, "333", 10);
        let (walk, sent) = walk_taking(&mut listing, &catalog);
        assert_eq!(RecordingsWalk::Unknown(2), walk);
        assert_eq!(vec![0, 1], sent);

        let mut listing = for_uri(33, "333", 2);
        let (walk, sent) = walk_taking(&mut listing, &catalog);
        assert_eq!(RecordingsWalk::Idle, walk);
        assert_eq!(vec![0, 1], sent);

        // Nothing matches on the channel, and the walk still runs off the end
        // of the catalog — the empty page is an answer the client waits for,
        // not a refusal.
        let mut listing = for_uri(33, "no-match", 10);
        let (walk, sent) = walk_taking(&mut listing, &catalog);
        assert_eq!(RecordingsWalk::Unknown(2), walk);
        assert!(sent.is_empty());

        // And nothing matches on the stream, with a fragment every channel has:
        // the half that only the intersection catches.
        let mut listing = for_uri(99, "", 10);
        let (walk, sent) = walk_taking(&mut listing, &catalog);
        assert_eq!(RecordingsWalk::Unknown(2), walk);
        assert!(sent.is_empty());
    }

    /// One turn looks at at most `MAX_SCANS_PER_WORK_CYCLE` entries however few
    /// of them it answers with, and what it does not get to it picks up from on
    /// the next turn (`AbstractListRecordingsSession.java:105`).
    ///
    /// This is the branch the C suite cannot reach — its catalog is two
    /// recordings — and the one that would make a for-uri request over a large
    /// catalog hold a turn of its own.
    #[test]
    fn a_turn_scans_a_bounded_number_of_entries_and_goes_on_from_the_cursor() {
        let channels: Vec<(i32, String)> = (0..100)
            .map(|index| (33, format!("aeron:udp?endpoint=localhost:{}", 4000 + index)))
            .collect();
        let recordings: Vec<(i32, &str)> = channels
            .iter()
            .map(|(stream_id, channel)| (*stream_id, channel.as_str()))
            .collect();

        let (_dir, catalog) = catalog_with_recordings(&recordings);

        // A filter nothing matches, so that every entry is looked at and none
        // is answered with.
        let mut listing = RecordingsListing {
            filter: RecordingsFilter::Uri {
                stream_id: 99,
                channel_fragment: Vec::new(),
            },
            ..recordings_listing(100)
        };

        assert_eq!(RecordingsWalk::Idle, walk_taking(&mut listing, &catalog).0);
        assert_eq!(
            i64::from(MAX_SCANS_PER_WORK_CYCLE),
            listing.from_recording_id,
            "the turn stopped at the scan cap and the cursor is where it got to"
        );
        assert_eq!(0, listing.sent);
        assert!(!listing.is_done, "there is still the rest of the catalog");

        // The next turn starts where that one stopped, and finishes the walk.
        let (walk, sent) = walk_taking(&mut listing, &catalog);
        assert_eq!(RecordingsWalk::Unknown(100), walk);
        assert!(sent.is_empty());
    }

    /// A descriptor that did not go out is not a failure to report but a turn
    /// to try again: the cursor stands still, and the record is offered again
    /// (`AbstractListRecordingsSession.java:117-121`).
    #[test]
    fn a_send_that_does_not_take_leaves_the_cursor_where_it_was() {
        let (_dir, catalog) = catalog_with_recordings(&[
            (33, "aeron:udp?endpoint=localhost:3333"),
            (33, "aeron:udp?endpoint=localhost:3334"),
        ]);

        let mut listing = recordings_listing(10);
        let mut offered = 0;
        let walk = walk_recordings(&mut listing, &catalog, |_| {
            offered += 1;
            false
        });

        assert_eq!(RecordingsWalk::SendFailed, walk);
        assert_eq!(1, offered, "the turn ended at the offer that did not take");
        assert_eq!(
            i64::MIN,
            listing.from_recording_id,
            "the cursor stands still"
        );
        assert_eq!(0, listing.sent);
        assert!(
            !listing.is_done,
            "whether it is over is the session's answer"
        );

        // The next turn offers the same recording again, and the page goes out.
        let (walk, sent) = walk_taking(&mut listing, &catalog);
        assert_eq!(RecordingsWalk::Unknown(2), walk);
        assert_eq!(vec![0, 1], sent);
    }

    /// A id no recording carries is not a special case: the walk starts at the
    /// next recording there is, and a cursor past the end of the catalog is a
    /// page of nothing rather than a refusal (`:90-102`, `:107-113`).
    ///
    /// The gap is a retired recording, which is what leaves one — and it is
    /// what tells the two arms of the cursor rule apart, because the answer a
    /// walk that fell in the gap ends with names `cursor + 1` and not
    /// `last id + 1`.
    #[test]
    fn a_cursor_lands_on_the_next_recording_there_is() {
        use deepmsg_codec::archive::recording_state::RecordingState;

        let (_dir, mut catalog) = catalog_with_recordings(&[
            (33, "aeron:udp?endpoint=localhost:3333"),
            (33, "aeron:udp?endpoint=localhost:3334"),
            (33, "aeron:udp?endpoint=localhost:3335"),
        ]);

        assert!(
            catalog
                .change_state(1, RecordingState::INVALID)
                .expect("retired")
        );

        // From the lowest id: 0 and 2, the retired one stepped over, and the
        // cursor one past the last whatever it was is 3.
        let mut listing = recordings_listing(10);
        let (walk, sent) = walk_taking(&mut listing, &catalog);
        assert_eq!(vec![0, 2], sent);
        assert_eq!(RecordingsWalk::Unknown(3), walk);

        // From inside the gap: the walk starts at 2, and the cursor it ends on
        // is one past **where it was**, which is 2 and not 3. That is the
        // reference's `recordingId++` and not `index[position] + 1`
        // (`:129-132`).
        let mut listing = RecordingsListing {
            from_recording_id: 1,
            ..recordings_listing(10)
        };
        let (walk, sent) = walk_taking(&mut listing, &catalog);
        assert_eq!(vec![2], sent);
        assert_eq!(RecordingsWalk::Unknown(2), walk);

        // Past the end of the catalog: nothing to answer with, and the name is
        // the cursor the client asked from.
        let mut listing = RecordingsListing {
            from_recording_id: 1_000,
            ..recordings_listing(10)
        };
        let (walk, sent) = walk_taking(&mut listing, &catalog);
        assert!(sent.is_empty());
        assert_eq!(RecordingsWalk::Unknown(1_000), walk);
    }

    /// A page of nothing is answered by running out of catalog rather than by a
    /// count of zero, and a **negative** count is the same quiet end.
    ///
    /// The reference's own test is `sent == count` (`:136-139`), which for a
    /// negative count is never true: the listing never ends and the session's
    /// `activeListing` slot is held for good. This build takes `>=`, which is
    /// the same test wherever the count is positive — see the plan's Q6.
    #[test]
    fn a_count_that_cannot_be_filled_is_a_quiet_end() {
        let (_dir, catalog) = catalog_with_recordings(&[
            (33, "aeron:udp?endpoint=localhost:3333"),
            (33, "aeron:udp?endpoint=localhost:3334"),
        ]);

        for count in [0, -5] {
            let mut listing = recordings_listing(count);
            let (walk, sent) = walk_taking(&mut listing, &catalog);

            assert!(sent.is_empty(), "count {count} asks for no descriptors");
            assert_eq!(RecordingsWalk::Idle, walk, "and nothing is sent about it");
            assert!(listing.is_done, "count {count} leaves no listing behind");
        }
    }

    /// A page of recordings has one gate and it is the asking session's own
    /// (`ArchiveConductor.newListRecordingsSession`, `:638-657`): a second
    /// request on the same session is refused with the reference's own words,
    /// and one from another session is served.
    #[test]
    fn a_page_of_recordings_is_started_once_per_session() {
        let mut sessions = sessions();

        sessions.on_list_recordings(3, 7, i64::MIN, 10, 0);
        assert_eq!(
            vec![Listing::Recordings(RecordingsListing {
                control_session_id: 3,
                correlation_id: 7,
                from_recording_id: i64::MIN,
                count: 10,
                sent: 0,
                filter: RecordingsFilter::All,
                is_done: false,
            })],
            sessions.listings
        );
        assert!(
            sessions.pending.is_empty(),
            "there is nothing to defer: the one gate is the session's own state"
        );

        sessions.on_list_recordings(3, 8, i64::MIN, 10, 0);
        assert_eq!(1, sessions.listings.len(), "one listing per session");
        assert_eq!(
            vec![Deferred::Error {
                session_id: 3,
                correlation_id: 8,
                relevant_id: ACTIVE_LISTING,
                message: ACTIVE_LISTING_MSG.to_owned(),
            }],
            sessions.pending
        );

        // The uri form is the same page with a filter, and a different session
        // gets its own.
        sessions.on_list_recordings_for_uri(4, 9, 5, 2, 33, b"3333", 0);
        assert_eq!(
            Some(&Listing::Recordings(RecordingsListing {
                control_session_id: 4,
                correlation_id: 9,
                from_recording_id: 5,
                count: 2,
                sent: 0,
                filter: RecordingsFilter::Uri {
                    stream_id: 33,
                    channel_fragment: b"3333".to_vec(),
                },
                is_done: false,
            })),
            sessions.listings.get(1)
        );
    }

    /// What the archive's own control settings are for these tests, and the
    /// correlation id of the image a request arrived on.
    const DEFAULTS: ResponseChannelDefaults = ResponseChannelDefaults {
        term_buffer_length: 65536,
        term_buffer_sparse: false,
        mtu_length: Some(1408),
    };
    const IMAGE_CORRELATION_ID: i64 = 7;

    fn derive(requested: &str) -> String {
        response_channel(requested, IMAGE_CORRELATION_ID, &DEFAULTS).expect("a readable channel")
    }

    /// The strip list, read off the reference's own `strippedChannelBuilder` by
    /// reflection rather than reasoned about — every expected value below is
    /// what `aeron-archive-1.53.2.jar` printed.
    ///
    /// `term-length`, `mtu` and `sparse` are the interesting ones: a client
    /// that asked for them does **not** get them back, because the archive sets
    /// those from its own control settings and the strip list does not carry
    /// them. `nak-delay` and any parameter the archive has never heard of go
    /// the same way.
    #[test]
    fn the_strip_list_is_the_references() {
        assert_eq!(
            "aeron:udp?endpoint=localhost:8010",
            stripped_channel_builder(
                &ChannelUri::parse("aeron:udp?endpoint=localhost:8010").unwrap()
            )
            .build()
        );

        assert_eq!(
            "aeron:udp?endpoint=localhost:8010",
            stripped_channel_builder(
                &ChannelUri::parse(
                    "aeron:udp?endpoint=localhost:8010|term-length=64k|mtu=1408|sparse=true"
                )
                .unwrap()
            )
            .build(),
            "the three the archive sets itself are not kept"
        );

        assert_eq!(
            "aeron:udp?tags=1,2|endpoint=localhost:1|ttl=7|tether=true",
            stripped_channel_builder(
                &ChannelUri::parse("aeron:udp?endpoint=localhost:1|tags=1,2|tether=true|ttl=007")
                    .unwrap()
            )
            .build()
        );

        assert_eq!(
            "aeron:udp?endpoint=localhost:1|so-sndbuf=1m",
            stripped_channel_builder(
                &ChannelUri::parse(
                    "aeron:udp?endpoint=localhost:1|so-sndbuf=1m|nak-delay=5ms|unknown-param=x"
                )
                .unwrap()
            )
            .build()
        );
    }

    /// A plain control channel is answered on the channel it asked for. It
    /// named none of the three the strip list drops, so all three come from
    /// the archive.
    #[test]
    fn a_client_that_names_none_of_the_three_gets_the_archives() {
        assert_eq!(
            "aeron:udp?endpoint=localhost:8010|mtu=1408|term-length=64k|sparse=false",
            derive("aeron:udp?endpoint=localhost:8010")
        );
    }

    /// ...and a client that names them keeps them. This is the direction the
    /// strip list makes easy to read backwards: the three are dropped and
    /// written back, but what is written back is the client's own
    /// (`ArchiveConductor.java:460-474`), which is what the `null == ... ?`
    /// in each line is choosing between.
    #[test]
    fn a_client_that_names_the_three_keeps_them() {
        assert_eq!(
            "aeron:udp?endpoint=localhost:8010|mtu=1500|term-length=128k|sparse=true",
            derive("aeron:udp?endpoint=localhost:8010|term-length=128k|mtu=1500|sparse=true")
        );
    }

    /// Each of the three falls back on its own, so naming one does not drag
    /// the other two with it.
    #[test]
    fn each_of_the_three_falls_back_on_its_own() {
        assert_eq!(
            "aeron:udp?endpoint=localhost:8010|mtu=1408|term-length=256k|sparse=false",
            derive("aeron:udp?endpoint=localhost:8010|term-length=256k"),
            "the term length is the client's, the other two are not"
        );

        assert_eq!(
            "aeron:udp?endpoint=localhost:8010|mtu=1500|term-length=64k|sparse=false",
            derive("aeron:udp?endpoint=localhost:8010|mtu=1500"),
            "the MTU is the client's, the other two are not"
        );

        assert_eq!(
            "aeron:udp?endpoint=localhost:8010|mtu=1408|term-length=64k|sparse=true",
            derive("aeron:udp?endpoint=localhost:8010|sparse=true"),
            "sparse is the client's, the other two are not"
        );
    }

    /// A response channel carries the image's correlation id, and nothing else
    /// changes: that is the whole difference between the two.
    #[test]
    fn a_response_channel_carries_the_images_correlation_id() {
        assert_eq!(
            "aeron:udp?control=localhost:9090|control-mode=response|mtu=1408|term-length=64k\
             |sparse=false|response-correlation-id=7",
            derive("aeron:udp?control=localhost:9090|control-mode=response")
        );
    }

    /// The correlation id is a property of the *image*, not of the request:
    /// two clients on the same control channel get different response
    /// channels, which is what keeps their answers apart.
    #[test]
    fn two_images_on_one_channel_get_two_response_channels() {
        let first = response_channel(
            "aeron:udp?control=localhost:9090|control-mode=response",
            7,
            &DEFAULTS,
        )
        .unwrap();
        let second = response_channel(
            "aeron:udp?control=localhost:9090|control-mode=response",
            8,
            &DEFAULTS,
        )
        .unwrap();

        assert_ne!(first, second);
    }

    #[test]
    fn ipc_is_a_channel_too() {
        assert_eq!(
            "aeron:ipc?mtu=1408|term-length=64k|sparse=false",
            derive("aeron:ipc")
        );
    }

    /// An archive that has set no MTU does not write one, rather than writing
    /// the zero its `Option` would be.
    #[test]
    fn an_unset_mtu_is_left_out() {
        let defaults = ResponseChannelDefaults {
            mtu_length: None,
            ..DEFAULTS
        };

        assert_eq!(
            "aeron:ipc?term-length=64k|sparse=false",
            response_channel("aeron:ipc", IMAGE_CORRELATION_ID, &defaults).unwrap()
        );
    }

    #[test]
    fn a_channel_that_will_not_read_is_an_error() {
        assert_eq!(
            Err(UriError::InvalidScheme),
            response_channel("udp?endpoint=localhost:1", IMAGE_CORRELATION_ID, &DEFAULTS)
        );
    }

    // ---- the conductor -------------------------------------------------

    const ARCHIVE_ID: i64 = 42;
    /// The image a control request arrived on, named the way the adapter
    /// names it: the publication's registration id and its session id.
    const IMAGE: ImageId = ImageId::new(11, 22);

    /// A values region, empty, for the questions whose answer does not read one.
    ///
    /// The same ten lines `control_session`'s tests and `recorder`'s keep their
    /// own copy of: a region has to be aligned, and the metadata half describes
    /// no counters — which is why a slot reads zero rather than "absent".
    fn counters_region() -> CountersReader<'static, ReadWrite> {
        #[repr(align(64))]
        struct Region([u8; 64 * 64]);

        let metadata: &'static mut Region = Box::leak(Box::new(Region([0; 64 * 64])));
        let values: &'static mut Region = Box::leak(Box::new(Region([0; 64 * 64])));

        CountersReader::new(
            deepmsg_core::buffer::AtomicBuffer::from_slice_mut(&mut metadata.0)
                .expect("an aligned region"),
            deepmsg_core::buffer::AtomicBuffer::from_slice_mut(&mut values.0)
                .expect("an aligned region"),
        )
    }

    /// The recording settings a test archive runs with: the reference's
    /// defaults, and a **threshold of zero** so that the free-space check reads
    /// the temp directory's real filesystem and finds it above the line.
    ///
    /// The directory is the temp directory rather than a temporary one of the
    /// test's own: the check is a `statvfs` of it, and a directory that a
    /// `TempDir` had already removed would answer zero and refuse every start.
    fn recording_settings() -> RecordingSettings {
        RecordingSettings {
            max_concurrent_recordings: 20,
            low_storage_space_threshold: 0,
            segment_file_length: 128 * 1024,
            file_io_max_length: 1024 * 1024,
            file_sync_level: 0,
            archive_dir: std::env::temp_dir(),
        }
    }

    fn sessions() -> Sessions {
        Sessions::new(
            ARCHIVE_ID,
            5_000,
            1_000,
            Duration::from_secs(1),
            DEFAULTS,
            recording_settings(),
            ReplaySettings {
                max_concurrent_replays: 20,
            },
            Vec::new(),
        )
    }

    /// One registry entry, for tests that are about what a stop or an image
    /// does with one rather than about how it is made.
    fn held(registration_id: i64) -> RecordingSubscription {
        RecordingSubscription {
            registration_id,
            stripped_channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
            channel: "aeron-spy:aeron:udp?endpoint=localhost:3333".to_owned(),
            stream_id: 33,
            original_channel: "aeron:udp?endpoint=localhost:3333".to_owned(),
            is_auto_stop: false,
            session_id: 3,
            correlation_id: 7,
            request: RecordingRequest::Start,
        }
    }

    fn connect_request(channel: &str, version: i32) -> ConnectRequest<'_> {
        ConnectRequest {
            correlation_id: 7,
            response_stream_id: 20,
            version,
            response_channel: channel,
            encoded_credentials: b"admin:admin",
            client_info: "a client",
        }
    }

    /// The connect answer is the id the client is answered on, and it moves on
    /// (`ArchiveConductor.java:490`).
    #[test]
    fn a_connect_is_answered_with_the_next_session_id() {
        let mut sessions = sessions();
        let version = PROTOCOL_SEMANTIC_VERSION;

        let first = sessions.new_session(IMAGE, connect_request("aeron:ipc", version), 0);
        let second = sessions.new_session(IMAGE, connect_request("aeron:ipc", version), 0);

        assert_eq!(FIRST_SESSION_ID, first);
        assert_eq!(FIRST_SESSION_ID + 1, second);
    }

    /// The callback records a connect rather than making a session, because it
    /// runs with the client lent out. What it will be made from is everything
    /// the request carried.
    #[test]
    fn a_connect_is_recorded_rather_than_made() {
        let mut sessions = sessions();
        sessions.new_session(
            IMAGE,
            connect_request("aeron:ipc", PROTOCOL_SEMANTIC_VERSION),
            0,
        );

        assert_eq!(0, sessions.len());
        assert_eq!(1, sessions.pending_connects.len());

        let connect = &sessions.pending_connects[0];
        assert_eq!(7, connect.correlation_id);
        assert_eq!(20, connect.response_stream_id);
        assert_eq!(b"admin:admin", connect.encoded_credentials.as_slice());
        assert_eq!("a client", connect.client_info);
        assert_eq!(None, connect.abort_reason);
    }

    /// The channel the client is answered on is derived from the one it asked
    /// for, in the same turn (`ArchiveConductor.java:458-481`).
    #[test]
    fn the_answered_channel_is_the_archives() {
        let mut sessions = sessions();
        sessions.new_session(
            IMAGE,
            connect_request(
                "aeron:udp?control=localhost:9090|control-mode=response",
                PROTOCOL_SEMANTIC_VERSION,
            ),
            0,
        );

        assert_eq!(
            "aeron:udp?control=localhost:9090|control-mode=response|mtu=1408|term-length=64k\
             |sparse=false|response-correlation-id=11",
            sessions.pending_connects[0].response_channel
        );
    }

    /// A client on another protocol major is answered with an `ERROR` instead
    /// of a connection, in the reference's words (`:483-488`).
    #[test]
    fn another_major_is_a_version_error() {
        let mut sessions = sessions();
        sessions.new_session(
            IMAGE,
            connect_request(
                "aeron:ipc",
                deepmsg_core::version::semantic_version_compose(2, 0, 0),
            ),
            0,
        );

        assert_eq!(
            Some("invalid client version 2.0.0, archive is 1.12.0".to_owned()),
            sessions.pending_connects[0].invalid_version_message
        );
    }

    #[test]
    fn our_own_major_is_not_a_version_error() {
        let mut sessions = sessions();
        sessions.new_session(
            IMAGE,
            connect_request("aeron:ipc", PROTOCOL_SEMANTIC_VERSION),
            0,
        );

        assert_eq!(None, sessions.pending_connects[0].invalid_version_message);
    }

    /// What a callback cannot send is recorded, not dropped — see the module
    /// note for why it cannot send it.
    #[test]
    fn a_response_a_callback_owes_is_recorded() {
        let mut sessions = sessions();

        sessions.on_archive_id(3, 7, 0);
        sessions.send_error_response(3, 8, 13, "unauthorised action", 0);
        sessions.on_challenge_response(3, 9, b"admin:CSadmin", 0);

        assert_eq!(
            vec![
                Deferred::Ok {
                    session_id: 3,
                    correlation_id: 7,
                    relevant_id: ARCHIVE_ID,
                },
                Deferred::Error {
                    session_id: 3,
                    correlation_id: 8,
                    relevant_id: 13,
                    message: "unauthorised action".to_owned(),
                },
                Deferred::ChallengeResponse {
                    session_id: 3,
                    correlation_id: 9,
                    encoded_credentials: b"admin:CSadmin".to_vec(),
                },
            ],
            sessions.pending
        );
    }

    /// A close that lands in the same poll as its connect is not lost: the
    /// session does not exist yet, so the reason rides on the connect until it
    /// does. An id that is neither live nor pending is ignored.
    #[test]
    fn a_close_in_the_same_poll_as_its_connect_is_kept() {
        let mut sessions = sessions();
        let session_id = sessions.new_session(
            IMAGE,
            connect_request("aeron:ipc", PROTOCOL_SEMANTIC_VERSION),
            0,
        );

        sessions.abort_session(session_id, SESSION_CLOSED_MSG);
        sessions.abort_session(session_id + 99, SESSION_CLOSED_MSG);

        assert_eq!(1, sessions.pending_connects.len());
        assert_eq!(
            Some(SESSION_CLOSED_MSG.to_owned()),
            sessions.pending_connects[0].abort_reason
        );
    }

    /// A keep-alive for a session that has not been made yet asks it to
    /// activate, and a session that is not there does nothing at all.
    #[test]
    fn a_keep_alive_for_an_absent_session_is_nothing() {
        let mut sessions = sessions();
        sessions.on_keep_alive(3);
        assert_eq!(0, sessions.len());
    }

    #[test]
    fn a_warning_is_kept_for_the_conductor() {
        let mut sessions = sessions();
        sessions.log_warning("something went wrong");

        assert_eq!(
            vec!["something went wrong".to_owned()],
            sessions.take_warnings()
        );
        assert!(sessions.take_warnings().is_empty());
    }

    /// A session ending is not the same as a session ending badly: three ways
    /// of ending leave the client's image alone, and the rest reject it
    /// (`ControlSession.java:186-190`).
    #[test]
    fn three_kinds_of_ending_do_not_reject_the_image() {
        assert!(!EndedSession::reason_rejects_image(None));
        assert!(!EndedSession::reason_rejects_image(Some(
            SESSION_CLOSED_MSG
        )));
        assert!(!EndedSession::reason_rejects_image(Some(
            RESPONSE_NOT_CONNECTED_MSG
        )));
        assert!(!EndedSession::reason_rejects_image(Some(
            REQUEST_IMAGE_NOT_AVAILABLE_MSG
        )));
        // The reference tests this one with `startsWith`, because the session
        // appends which image it was.
        assert!(!EndedSession::reason_rejects_image(Some(
            "control request publication image unavailable: correlationId=11 sessionId=22"
        )));

        assert!(EndedSession::reason_rejects_image(Some(
            "failed to establish initial connection: state=INIT"
        )));
        assert!(EndedSession::reason_rejects_image(Some(
            "authentication rejected"
        )));
    }

    /// The three refusals of a replay position carry the reference's own words
    /// and its two codes (`ArchiveConductor.java:2256-2288` over
    /// `client/ArchiveException.java:244-268`).
    ///
    /// They are client-visible protocol text — the archive's error messages are
    /// the one part of the wire a reference test asserts verbatim — and the
    /// alignment refusal's code is **zero**, because the reference's
    /// `sendErrorResponse(correlationId, msg)` there is the overload that
    /// carries no code.
    #[test]
    fn a_replay_position_is_refused_in_the_references_words() {
        let (dir, catalog) = catalog_with_a_recording();
        let recording = catalog.recording(0).expect("the recording");
        let _dir = dir;

        assert_eq!(
            Some((
                0,
                "requested replay start position=1000 is not a multiple of FRAME_ALIGNMENT (32) for \
                 recording 0"
                    .to_owned()
            )),
            invalid_replay_position(0, 1000, &recording)
        );

        assert_eq!(
            Some((
                INVALID_POSITION,
                "requested replay start position=2048 is less than recording start position=4096 \
                 for recording 0"
                    .to_owned()
            )),
            invalid_replay_position(0, 2048, &recording)
        );

        assert_eq!(
            Some((
                INVALID_POSITION,
                "requested replay start position=8192 must be less than the limit position=8192 \
                 for recording 0"
                    .to_owned()
            )),
            invalid_replay_position(0, 8192, &recording)
        );

        assert_eq!(None, invalid_replay_position(0, 4096, &recording));
    }

    /// The protocol major is derived from the version stamped on every
    /// response rather than written down twice.
    #[test]
    fn the_protocol_major_comes_from_the_protocol_version() {
        assert_eq!(1, PROTOCOL_MAJOR_VERSION);
        assert_eq!(1, semantic_version_major(PROTOCOL_SEMANTIC_VERSION));
    }

    /// A replay's channel carries the recording's geometry, and — when the
    /// client asked for the answer on a channel of its own — the id of the
    /// image its request arrived on (`ArchiveConductor.java:891-901`,
    /// `ControlSessionAdapter.java:1183`).
    #[test]
    fn a_response_channel_replay_carries_the_image_it_answers() {
        const RESPONSE_CHANNEL: &str = "aeron:udp?control-mode=response|control=localhost:10002";

        let channel = replay_channel(RESPONSE_CHANNEL, 4096, 3, 64 * 1024, 1408, Some(11))
            .expect("a channel");

        assert!(
            channel.contains("control-mode=response"),
            "the client's own parameters survive the strip: {channel}"
        );
        assert!(
            channel.contains("response-correlation-id=11"),
            "and the image is written onto them: {channel}"
        );

        // The ordinary replay, which names no image and gets no parameter.
        let channel =
            replay_channel(RESPONSE_CHANNEL, 4096, 3, 64 * 1024, 1408, None).expect("a channel");

        assert!(
            !channel.contains("response-correlation-id"),
            "no image to answer: {channel}"
        );
    }

    /// A token names a session and a recording for as long as a connect
    /// timeout, and is drawn until it is free
    /// (`ArchiveConductor.generateReplayToken`, `:2639-2652`).
    #[test]
    fn a_replay_token_names_a_session_until_it_expires() {
        let mut sessions = sessions();
        let recording_id = 7;

        // `NULL_VALUE` is what the reference's loop rejects, so it is what this
        // one must never hand out — a client cannot tell it from no token.
        let first = sessions.generate_replay_token(1, recording_id, 1_000);
        assert_ne!(NULL_VALUE, first);
        assert_eq!(
            Some(1),
            sessions.replay_session_by_token(first, recording_id, 1_000)
        );

        // Another session's token, which is a different number.
        let second = sessions.generate_replay_token(2, recording_id, 1_000);
        assert_ne!(first, second);
        assert_eq!(
            Some(2),
            sessions.replay_session_by_token(second, recording_id, 1_000)
        );

        // The recording is half of what a token stands for (`:2659`).
        assert_eq!(
            None,
            sessions.replay_session_by_token(first, recording_id + 1, 1_000)
        );

        // And the clock is the other half: the timeout is the archive's
        // `connectTimeoutNs`, which `sessions()` sets to 5 s.
        assert_eq!(
            Some(1),
            sessions.replay_session_by_token(first, recording_id, 5_999)
        );
        assert_eq!(
            None,
            sessions.replay_session_by_token(first, recording_id, 6_000)
        );
    }

    /// A token dies with the session it names
    /// (`removeReplayTokensForSession`, `:2669-2681`), because session ids are
    /// handed out again.
    #[test]
    fn a_replay_token_dies_with_its_session() {
        let mut sessions = sessions();

        let kept = sessions.generate_replay_token(1, 7, 0);
        let removed = sessions.generate_replay_token(2, 7, 0);

        sessions.remove_replay_tokens_for_session(2);

        assert_eq!(Some(1), sessions.replay_session_by_token(kept, 7, 0));
        assert_eq!(None, sessions.replay_session_by_token(removed, 7, 0));
    }

    /// The sweep runs once per turn and takes only what has expired
    /// (`checkReplayTokens`, `:2683-2697`).
    #[test]
    fn the_token_sweep_takes_only_what_has_expired() {
        let mut sessions = sessions();
        sessions.connect_timeout_ms = 100;

        let old = sessions.generate_replay_token(1, 7, 0);
        let new = sessions.generate_replay_token(2, 7, 50);

        assert_eq!(1, sessions.check_replay_tokens(100), "the first is due");
        assert_eq!(None, sessions.replay_session_by_token(old, 7, 100));
        assert_eq!(Some(2), sessions.replay_session_by_token(new, 7, 100));

        assert_eq!(0, sessions.check_replay_tokens(100), "and nothing else is");
    }
}
