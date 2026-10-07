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
//! Three more things the callbacks cannot do for the same reason, and which
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
//!
//! `Sessions` consequently does not hold a `CncFile`: the region arrives each
//! turn as an argument. That also makes it testable without one.
//!
//! # What this slice does not have
//!
//! The reference's `doWork` also runs the recorder, the replayer and the
//! replay-token sweep (`:389-395`). Those belong to the sessions that use
//! them, and none of them exist yet — the recording and replay sessions are
//! their own slices. What is here is the control plane: connect, authenticate,
//! answer, and count.

use std::collections::HashMap;
use std::time::Duration;

use deepmsg_client::client::{Client, DEFAULT_TIMEOUT};
use deepmsg_client::image::Image;
use deepmsg_cnc::counters::CountersReader;
use deepmsg_cnc::file::CncFile;
use deepmsg_core::buffer::ReadWrite;
use deepmsg_core::uri::{ChannelUri, ChannelUriStringBuilder, UriError, parse_size};
use deepmsg_core::version::{format_version, semantic_version_major};

use crate::mark_file::{ArchiveMarkFile, MARK_FILE_UPDATE_INTERVAL_MS};
use crate::server::auth::{
    AuthError, Authenticator, AuthorisationService, authenticator, authorisation_service,
};
use crate::server::config::ArchiveConfig;
use crate::server::control_adapter::{
    ConnectRequest, ControlAdapter, ControlError, ControlPlane, ImageId,
};
use crate::server::control_session::{
    ControlSession, REQUEST_IMAGE_NOT_AVAILABLE_MSG, RESPONSE_NOT_CONNECTED_MSG, SESSION_CLOSED_MSG,
};
use crate::server::counters::{
    ControlSessionCounter, ControlSessionsCounter, ErrorCounter, claim_control_sessions_counter,
    request_control_sessions_counter,
};
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

impl Sessions {
    /// A session store for one archive.
    #[allow(clippy::too_many_arguments)] // one per setting the archive was configured with
    pub fn new(
        archive_id: i64,
        connect_timeout_ms: i64,
        liveness_check_interval_ms: i64,
        command_timeout: Duration,
        response_channel_defaults: ResponseChannelDefaults,
        subscription_ids: Vec<i64>,
    ) -> Self {
        Self {
            archive_id,
            next_session_id: FIRST_SESSION_ID,
            connect_timeout_ms,
            liveness_check_interval_ms,
            command_timeout,
            response_channel_defaults,
            subscription_ids,
            sessions: HashMap::new(),
            pending_connects: Vec::new(),
            pending: Vec::new(),
            ended: Vec::new(),
            session_count_delta: 0,
            session_counter_registration_id: None,
            control_sessions: None,
            warnings: Vec::new(),
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
    pub fn drive(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        authenticator: &mut dyn Authenticator,
        now_ms: i64,
    ) {
        self.allocate_session_counter(client, counters);
        self.run_pending_connects(client, authenticator, now_ms);
        self.run_deferred(client, authenticator, now_ms);
        self.drive_sessions(client, counters, authenticator, now_ms);
        self.apply_session_count_delta(counters);
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
    fn run_deferred(
        &mut self,
        client: &mut Client,
        authenticator: &mut dyn Authenticator,
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

        sessions.drive(client, &counters, authenticator.as_mut(), now_ms);

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

    fn sessions() -> Sessions {
        Sessions::new(
            ARCHIVE_ID,
            5_000,
            1_000,
            Duration::from_secs(1),
            DEFAULTS,
            Vec::new(),
        )
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

    /// The protocol major is derived from the version stamped on every
    /// response rather than written down twice.
    #[test]
    fn the_protocol_major_comes_from_the_protocol_version() {
        assert_eq!(1, PROTOCOL_MAJOR_VERSION);
        assert_eq!(1, semantic_version_major(PROTOCOL_SEMANTIC_VERSION));
    }
}
