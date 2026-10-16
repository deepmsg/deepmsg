//! One archive-to-archive replication (M13), as a state machine the conductor
//! ticks.
//!
//! The reference's `ReplicationSession` (`ReplicationSession.java`, 1043 lines)
//! is the whole of what it takes to copy a recording between two archives: it
//! **is** an archive client — `asyncConnect` (`:407`) and every request it
//! makes go through its own `srcArchive` (`:451`, `:492`, `:583`, `:719`,
//! `:935`) — and it drives itself a step at a time from the conductor's session
//! worker. Two consequences reach this build and neither is a detail:
//!
//! * **The client here cannot block — and it is not only the connect.**
//!   The reference's `asyncConnect` returns a pollable object and its session
//!   polls it once per turn; this build's synchronous
//!   [`Archive::connect`](crate::client::archive::Archive::connect) loops until
//!   it is done, which inside the conductor's turn is a deadlock (S1's rule: a
//!   blocking command in the launcher never returns). The client grew the door
//!   for this — `Archive::from_connected` and `AsyncConnect` are both public —
//!   and `CONNECT` opens it.
//!
//!   **The same rule reaches the context**, and there it needed a deviation:
//!   [`crate::client::context::ArchiveContext::conclude_with`] draws a session
//!   id — a driver round trip — for any response channel that is not
//!   `control-mode=response`. The reference can afford that because its archive
//!   agent and driver agent are separate threads; this build's archiving media
//!   driver runs both in one loop, so the draw would wedge the archive for its
//!   whole message timeout. The context a replication is built with therefore
//!   asks for a response channel — see
//!   [`crate::server::conductor::ReplicationSettings::client_context`] for the
//!   measurement and the reasoning.
//! * **There is more than one at a time.** `replicationSessionByIdMap`
//!   (`ArchiveConductor.java:148`) is keyed by the replication's id and holds
//!   every session in flight, so a consumer of a descriptor cannot be a `fn`
//!   pointer reaching for a `static` — see the client's descriptor poller,
//!   which hands the descriptor to a `&mut dyn FnMut` for exactly this caller.
//!
//! # What is here, and what is not
//!
//! **Nine of the twelve states walk, and every route through the machine ends
//! at the tenth.** `CONNECT` was first and the rest arrived one commit at a
//! time, in the order they are listed in [`State`]; what each one does, and
//! what it is observable through, is that state's own note.
//!
//! The two that do not walk are `CATCHUP` and `ATTEMPT_LIVE_JOIN`, the
//! live-merge pair. They are named and documented rather than stubbed, and that
//! is deliberate: a state method with a plausible-looking body is worse than
//! one that does nothing, because the next reader cannot tell the two apart.
//! What leads to them is a `live_destination` (`:380-399`), which no criterion
//! this build is held to names — and the branch a **tagged** request reaches
//! instead is a different one, which [`State`] has the note on.

use std::time::Duration;

use deepmsg_client::client::{
    AsyncDestination, AsyncDestinationPoll, AsyncNextSessionId, AsyncNextSessionIdPoll, Client,
};
use deepmsg_client::image::ImageView;
use deepmsg_codec::archive::recording_signal::RecordingSignal;
use deepmsg_codec::archive::source_location::SourceLocation;

use crate::catalog::{Catalog, Recording};
use crate::client::archive::{Archive, Handlers};
use crate::client::async_connect::{
    AsyncConnect, Credentials, NoCredentials, Polled, ReplicationCredentials,
};
use crate::client::context::ArchiveContext;
use crate::client::descriptor_poller::RecordingDescriptor;
use crate::client::proxy::ReplayParams;
use crate::client::replay_merge::LIVE_ADD_MAX_WINDOW;
use crate::mark::NULL_VALUE;
use crate::server::conductor::{
    GENERIC, NULL_POSITION, REPLICATION_CONNECTION_FAILURE, UNKNOWN_RECORDING,
};
use crate::server::control_adapter::{ExtendRecordingRequest, ReplicationRequest};
use deepmsg_core::uri::ChannelUri;

/// `CommonContext.REJOIN_PARAM_NAME` (`CommonContext.java:229`).
const REJOIN_KEY: &str = "rejoin";
/// `CommonContext.SESSION_ID_PARAM_NAME` (`CommonContext.java:135`).
const SESSION_ID_KEY: &str = "session-id";
/// `CommonContext.MDC_CONTROL_MODE_PARAM_NAME`'s key.
const CONTROL_MODE_KEY: &str = "control-mode";
/// `ChannelUri.hasControlModeResponse`'s value (`ChannelUri.java:653-656`).
const CONTROL_MODE_RESPONSE: &str = "response";
/// `CommonContext.ENDPOINT_PARAM_NAME` (`CommonContext.java:239`).
const ENDPOINT_KEY: &str = "endpoint";
/// `CommonContext.LINGER_PARAM_NAME` (`CommonContext.java:243`).
const LINGER_KEY: &str = "linger";
/// `CommonContext.EOS_PARAM_NAME` (`CommonContext.java:231`).
const EOS_KEY: &str = "eos";
/// `CommonContext.TAGS_PARAM_NAME` (`CommonContext.java:334`).
const TAGS_KEY: &str = "tags";
/// `CommonContext.MDC_CONTROL_MODE_MANUAL` (`CommonContext.java:303`).
const CONTROL_MODE_MANUAL: &str = "manual";
/// `CommonContext.RESPONSE_CORRELATION_ID_PARAM_NAME`.
const RESPONSE_CORRELATION_ID_KEY: &str = "response-correlation-id";
/// `AeronArchive.NULL_LENGTH` (`AeronArchive.java:106`), which is `NULL_VALUE`:
/// a replay with no length follows the source (`:115-117`).
const NULL_LENGTH: i64 = NULL_VALUE;
/// `ReplicationSession.REPLAY_REMOVE_THRESHOLD` (`:55`): how far behind the
/// source the copy may be when its replay is taken off. Nought, which is to say
/// it may not be behind at all.
const REPLAY_REMOVE_THRESHOLD: i64 = 0;
/// `ReplicationSession.RETRY_ATTEMPTS` (`:56`): how many times a source that
/// does not answer its position request is asked again before the join gives
/// up. Armed again by every answer (`:859`).
const RETRY_ATTEMPTS: i32 = 3;

/// What a session offers the **source** when it connects.
///
/// Two cases, and the reference has both (`ArchiveConductor.java:1443-1446`):
/// a request that carried credentials gets
/// [`ReplicationCredentials`] — offered as they are, and **no challenge
/// answered** — and a request that carried none leaves the context's own
/// supplier alone, which is the null one: it offers nothing and answers a
/// challenge with nothing.
///
/// The difference is only visible when the far end challenges a request that
/// brought no credentials, and then it is the difference between the server
/// refusing an empty answer and this build refusing to answer at all. Both end
/// the connect; the words differ, and the words are the reference's.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
enum SessionCredentials {
    /// The request carried none.
    #[default]
    Nothing,
    /// The request carried these.
    Request(ReplicationCredentials),
}

impl SessionCredentials {
    /// What a request with these `encoded_credentials` connects with.
    fn for_request(encoded_credentials: &[u8]) -> Self {
        if encoded_credentials.is_empty() {
            Self::Nothing
        } else {
            Self::Request(ReplicationCredentials::new(encoded_credentials.to_vec()))
        }
    }
}

impl Credentials for SessionCredentials {
    fn encoded(&mut self) -> Vec<u8> {
        match self {
            Self::Nothing => NoCredentials.encoded(),
            Self::Request(credentials) => credentials.encoded(),
        }
    }

    fn on_challenge(&mut self, challenge: &[u8]) -> Result<Vec<u8>, String> {
        match self {
            Self::Nothing => NoCredentials.on_challenge(challenge),
            Self::Request(credentials) => credentials.on_challenge(challenge),
        }
    }
}

/// The state a session is in (`ReplicationSession.java:61-75`), in the order it
/// walks them.
///
/// Each variant names the reference's own method for that state, which is one
/// method per state and no state without one. `DO_WORK` is what calls them
/// (`:217-293`, whose `switch` is the whole of the machine).
///
/// `CATCHUP` and `ATTEMPT_LIVE_JOIN` are the live-merge pair, and the criteria
/// this build is held to never reach them (`live_destination` is empty in every
/// one of them, the tagged replication included); they are named here so the
/// pair is not mistaken for a gap.
///
/// **The pair is not the same thing as the multi-destination branch.**
/// `EXTEND`'s `isMds` half walks for a **tagged** request, and
/// `a_tagged_replication_subscribes_by_tag_and_still_receives_the_replay` (in
/// `deepmsg-tests`) is the criterion that reaches it; the pair is where a
/// **live destination** leads, and nothing walks it yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Connect to the source archive (`connect`, `:401-443`).
    Connect,
    /// Ask the source for the recording's descriptor (`replicateDescriptor`,
    /// `:444-483`).
    ReplicateDescriptor,
    /// Ask the source where the recording has got to, for a live source
    /// (`srcRecordingPosition`, `:484-524`).
    SrcRecordingPosition,
    /// Make the destination recording — the catalog row and the subscription
    /// under it (`extend`, `:525-568`).
    Extend,
    /// Ask the source for a replay token, when the replication runs over a
    /// response channel (`replayToken`, `:569-605`).
    ReplayToken,
    /// Make the publication that token lets a *different* image drive
    /// (`getArchiveProxy`, `:606-665`).
    GetArchiveProxy,
    /// Ask the source to replay the recording (`replay`, `:666-752`).
    Replay,
    /// Wait for the source's replay to appear at the destination (`awaitImage`,
    /// `:753-776`).
    AwaitImage,
    /// Follow the image to the end of what was asked for (`replicate`,
    /// `:777-815`).
    Replicate,
    /// The live pair's first half (`catchup`, `:816-833`).
    Catchup,
    /// The live pair's second half (`attemptLiveJoin`, `:834-900`) — the one
    /// state that runs long, because it is where the copy joins a stream that
    /// is still being written.
    AttemptLiveJoin,
    /// Finished, and waiting for [`ReplicationProgress::Done`] to be collected
    /// (`isDone`, `:179-187`). Every path out of the machine ends here, `abort`
    /// included.
    Done,
}

/// What one turn of a session did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationProgress {
    /// Nothing this turn, or nothing the conductor has to act on.
    Idle,
    /// The session is over and its slot can be given back.
    Done,
}

/// Something a session **cannot do itself**, and what it does instead.
///
/// Most of these are owed to the destination's client; the last is an ask of the
/// archive the session runs inside.
///
/// The reference's session holds the asking `ControlSession` and calls it
/// directly — `sendSignal` and `sendErrorResponse` are methods on it
/// (`ReplicationSession.java:925`, `:932`). Here those sessions live in a map
/// the conductor owns, and a session is an owned value inside it, so what
/// travels out is the message and not the caller.
///
/// **A list, and not one field.** A turn can owe two things: `onRecordingDescriptor`
/// sends `SYNC` **and** ends the replication in the same breath when the two
/// archives are already in step (`:393-398`), and a lifecycle-shaped answer
/// cannot carry both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationNotice {
    /// `ControlSession.sendSignal` (`ControlSession.java:735-750`).
    Signal {
        /// The recording the signal is about.
        recording_id: i64,
        /// The subscription it belongs to, `NULL_VALUE` before `EXTEND` has
        /// made one (`ReplicationSession.java:930`).
        subscription_id: i64,
        /// Where the recording has got to.
        position: i64,
        /// Which signal.
        signal: RecordingSignal,
    },
    /// **Ask the conductor to stop the destination's recording**, quietly
    /// (`ArchiveConductor.stopRecordingSubscription(subscriptionId)`, `:626-636`
    /// — the overload that answers nobody; the client-facing one answers).
    ///
    /// The reference's session calls it directly from its `close()` (`:950`).
    StopRecording {
        /// The subscription the extend made.
        registration_id: i64,
    },
    /// **Ask the conductor to run one of the archive's own operations.**
    ///
    /// The reference's session calls the conductor **directly** and reads what
    /// it returns — `conductor.extendRecording(...)` (`:549`) is the one this
    /// build needs. Here the conductor owns its sessions in a map and drives
    /// them, so the request travels out and the answer is written back onto the
    /// session ([`Sessions::record_extend_outcome`]).
    ///
    /// Unlike its two siblings this is not a message to the client: what the
    /// client hears about an extend is the archive's own answer, sent under the
    /// **replication's** correlation id, exactly as it is for a client that
    /// asked for the extend itself.
    Extend {
        /// The request, with the replication's own ids in it.
        request: ExtendRecordingRequest,
    },
    /// `ControlSession.sendErrorResponse`.
    ///
    /// The correlation id is the **replication's**, not the request's: the
    /// reference answers `sendErrorResponse(replicationId, errorCode, msg)`
    /// (`:925`), and `replicationId` is the first argument — which is the
    /// correlation id. So a client sees an error about a request it made long
    /// ago, under an id it was *answered* with rather than one it sent.
    Error {
        /// What the error is about — the replication's id.
        correlation_id: i64,
        /// `ArchiveException`'s code, which is the `relevantId` on the wire.
        code: i64,
        /// The archive's own words.
        message: String,
    },
}

/// One replication in flight.
///
/// Built by `Sessions::replicate` (`super::conductor`) from the request that
/// asked for it, and nothing else: everything the reference's constructor takes
/// beyond that — the catalog's summary of the destination, the epoch clock, the
/// conductor itself — arrives with the states that need it.
pub struct ReplicationSession {
    /// Where the machine has got to.
    state: State,
    /// The control session that asked for this, which is who every answer goes
    /// to. The reference holds the `ControlSession` object itself
    /// (`ReplicationSession.java:101`); this build holds the id, because the
    /// session that owns it is in a map the conductor holds.
    control_session_id: i64,
    /// **The id a stop finds this session by, or `None` once one has.**
    ///
    /// The reference holds the id twice — here, and as
    /// `replicationSessionByIdMap`'s key — and reads the field in one place
    /// only: the source archive's client name (`ArchiveConductor.java:1456`).
    /// A stop takes the session out of the **map** (`:1482`), not out of the
    /// worker's array, so the two are not the same fact: after a stop the
    /// session is still driven, and it is its own `close()` (`:196-213`) that
    /// ends it and sends `REPLICATE_END`. `None` here is the map having let go
    /// — see [`ReplicationSession::unregister`].
    replication_id: Option<i64>,
    /// The request, field for field, which is what every state reads.
    request: ReplicationRequest,
    /// The channel the copy really runs over: the request's own, or the
    /// archive's configured one when the request named none
    /// (`ArchiveConductor.java:1394-1395`).
    #[allow(dead_code)] // read when EXTEND builds the destination's channel
    replication_channel: String,
    /// The context the **source** archive is reached with
    /// (`ArchiveConductor.java:1430-1437`): the archive's own client context,
    /// with the request's control channel and stream written over it.
    context: ArchiveContext,
    /// The session id the connect needs, asked for and not yet answered.
    ///
    /// **Not the reference's field.** The reference asks for this id inside
    /// `AeronArchive.context().conclude()` and blocks for the answer; a session
    /// runs inside the archive's turn and cannot (the module note, and
    /// [`crate::server::conductor::Sessions::drive_replication_sessions`]). So
    /// one turn asks ([`Client::async_next_session_id`]) and a later one reads,
    /// and what is read goes onto [`ArchiveContext::prepared_session_id`] —
    /// which is where `conclude_with` finds it instead of asking.
    session_id_request: Option<AsyncNextSessionId>,
    /// The connect, while one is going on (`ReplicationSession.java:106`), and
    /// `None` once it has finished **or** before it starts.
    ///
    /// It is an [`AsyncConnect`] and not [`Archive::connect`] because of where
    /// it runs: the conductor's turn. See the module note.
    async_connect: Option<AsyncConnect>,
    /// The source archive, once connected (`:107`). Every state past `CONNECT`
    /// reads a request through it.
    src_archive: Option<Archive>,

    /// Messages the destination's client is owed — see [`ReplicationNotice`].
    notices: Vec<ReplicationNotice>,

    /// The registration id of the destination's recording subscription, once
    /// [`State::Extend`] has made one — the reference's `recordingSubscription`,
    /// of which `REPLAY` reads the **resolved endpoint** and
    /// `GET_ARCHIVE_PROXY` the registration id.
    recording_subscription: Option<i64>,
    /// `fileIoMaxLength` (`:103`): how big a block one turn may read, which the
    /// request carries and `REPLAY` passes on to the source.
    file_io_max_length: i32,
    /// `replayToken` (`:111`): the token a replay over a **response channel**
    /// needs, or `NULL_VALUE`. Nothing in this build asks for one — see
    /// [`State::ReplayToken`].
    replay_token: i64,
    /// `srcReplaySessionId` (`:78`): the session the **source's replay** runs
    /// under, which is what `AWAIT_IMAGE` looks for at the destination.
    src_replay_session_id: i64,
    /// What this session offers the **source** when it connects, which is what
    /// the replication request carried (`ArchiveConductor.java:1443-1446`).
    ///
    /// **The reference keeps this on the context** the session connects with
    /// (`remoteArchiveContext.credentialsSupplier(new ReplicationCredentialsSupplier(...))`)
    /// and only when the request carried something; here it is a field of the
    /// session, which reaches the same place — `AsyncConnect::new` takes the
    /// supplier as an argument — without putting a trait object on a context
    /// that is `Clone`, `Debug` and `PartialEq`.
    ///
    /// **Measured**: without it every replication against the C suite's archives
    /// died at `CONNECT` with `authentication rejected` — the suite configures
    /// the sample authenticator (`TestArchive.h:50`) and sends `admin:admin`
    /// with every replication request (`aeron_archive_test.cpp:78`).
    credentials: SessionCredentials,
    /// The clock this turn is running on, which [`Self::go_to`] reads — the
    /// reference's `epochClock`, which its own `state` asks for the time
    /// (`:975-983`).
    now_ms: i64,
    /// Whether the extend has been handed to the conductor already, which is
    /// what keeps [`State::Extend`] from asking again every turn.
    extend_asked: bool,

    /// Every destination command this session has sent and not yet had
    /// answered — `EXTEND`'s add (`:553`) and `ATTEMPT_LIVE_JOIN`'s add and
    /// remove (`:870`, `:875`), none of which the reference waits for or reads
    /// the answer of.
    ///
    /// Kept for one reason: the handle is what retires the client's pending
    /// entry. The reference's `Subscription` polls its own destinations, so a
    /// caller that forgets one is a caller that leaks nothing; this client
    /// retires an entry when it is polled, and the session is the only thing
    /// that would ever poll these. See [`ReplicationSession::do_work`].
    pending_destinations: Vec<AsyncDestination>,

    /// What the conductor answered it with, once it has run — the reference's
    /// return value from `conductor.extendRecording`, delivered a turn late
    /// because that is where this build's conductor is.
    extend_answer: Option<Result<i64, (i64, String)>>,

    /// `activeCorrelationId` (`:77`): the request whose answer a state is
    /// waiting for, or `NULL_VALUE` when nothing is in flight.
    ///
    /// **One slot, and the states share it.** Each state either sends its
    /// request and parks here, or reads the answer and clears it, which is why
    /// every state is two halves and why none of them can be waiting for two
    /// things at once.
    active_correlation_id: i64,
    /// `timeOfLastActionMs` (`:84`): when `trackAction` last ran, which the
    /// action deadline is measured from.
    time_of_last_action_ms: i64,
    /// `actionTimeoutMs` (`:85`): the client context's `messageTimeoutNs`, in
    /// milliseconds — how long one action has before the state gives up on it.
    action_timeout_ms: i64,

    /// `dstRecordingId` (`:90`): **the destination recording**, which starts as
    /// the request's and becomes the id of the row `onRecordingDescriptor`
    /// makes when the request asked for a new one.
    dst_recording_id: i64,
    /// `dstStopPosition` (`:82`): where the request asked the copy to stop,
    /// which `REPLICATE` compares the image's position against to end a bounded
    /// copy (`:788`).
    #[allow(dead_code)] // read by REPLICATE, two commits away
    dst_stop_position: i64,
    /// `liveDestination` (`:97`), which is `None` unless the request named one
    /// (`:133`: an empty string is no destination, not an empty one).
    live_destination: Option<String>,
    /// `srcRecordingPosition` (`:81`): where the **source** says its recording
    /// has got to, asked for by [`State::SrcRecordingPosition`]. A different
    /// number from `src_stop_position` and asked a different way — that one
    /// comes off the descriptor, this one off a `getRecordingPosition` — and
    /// the reference keeps them apart.
    src_recording_position: i64,
    /// `srcStopPosition` (`:80`): where the source's recording stopped, which
    /// `onRecordingDescriptor` reads off the descriptor and `REPLICATE` compares
    /// the image against.
    src_stop_position: i64,
    /// `replayPosition` (`:79`): where the replay asks the source to start.
    replay_position: i64,
    /// `replayStreamId` (`:91`): the stream the source's replay goes out on,
    /// which the descriptor carries.
    replay_stream_id: i32,
    /// `replaySessionId` (`:92`): the Aeron session id the destination's
    /// subscription expects the replay to arrive under, which `onRecordingDescriptor`
    /// settles (`:308-315`) and `EXTEND` writes onto the channel.
    #[allow(dead_code)] // read when EXTEND builds the destination's channel
    replay_session_id: i32,
    /// `isDestinationRecordingEmpty` (`:83`): whether a destination that exists
    /// has nothing in it, which is the one case its row is **replaced** rather
    /// than left alone (`:361-370`).
    is_destination_recording_empty: bool,

    /// `replayDestination` (`:98`): the destination `EXTEND` added, kept so that
    /// `ATTEMPT_LIVE_JOIN` can take it off again once the live stream has
    /// arrived — which is the whole of what a merge *is* (`:873-880`).
    ///
    /// `None` on the branches that never add one, which is every replication
    /// that is not multi-destination.
    replay_destination: Option<String>,
    /// `isLiveAdded` (`:94`): whether the **live** destination has been added.
    ///
    /// The two predicates `ATTEMPT_LIVE_JOIN` branches on are exclusive
    /// through it: not yet added and close enough → add; added and caught up
    /// and the stream is arriving from two places → take the replay off and
    /// call it merged.
    is_live_added: bool,
    /// `retryAttempts` (`:93`): how many more times a source that has not
    /// answered its position request may be asked before the join gives up
    /// (`:890-895`). Armed to [`RETRY_ATTEMPTS`] by every answer (`:859`), so
    /// it counts **consecutive** silences rather than the lifetime's.
    retry_attempts: i32,
}

/// The channel an `EXTEND` is made on (`ReplicationSession.extend`,
/// `ReplicationSession.java:525-541`).
///
/// A free function over the six things that decide it, and not a method, so
/// that the channel can be judged **where it is built**. The criteria in
/// `deepmsg-tests` cannot: a replication's recording subscription is
/// `autoStop` and goes the moment the copy ends
/// (`ArchiveConductor.java:1355`), which leaves a window shorter than one
/// client round trip — so what those criteria assert is the `EXTEND` signal,
/// and the channel it names is asserted here
/// (`analysis/archive-client-api-flake.md`, section B).
///
/// Three parameters are the channel's own, three are the session's:
///
/// * `replication_channel` is the channel the request named;
/// * `replay_session_id` is what the source's replay will arrive under, which
///   `on_recording_descriptor` settles from the descriptor and `EXTEND` writes
///   down — **unless the channel is a response channel**, where the reference
///   leaves it out (`:532-536` asks `hasControlModeResponse`);
/// * `is_tagged` and the two ids are the multi-destination rewrite, and a
///   **live destination** joins it because a copy that has to follow a stream
///   which comes and goes cannot take its address from a channel (`isMds`,
///   `:527`).
fn extend_channel_uri(
    replication_channel: &str,
    replay_session_id: i32,
    is_tagged: bool,
    channel_tag_id: i64,
    subscription_tag_id: i64,
    has_live_destination: bool,
) -> Result<String, String> {
    let mut uri = ChannelUri::parse(replication_channel)
        .map_err(|error| format!("{replication_channel} is not a channel: {error}"))?;

    uri.put(REJOIN_KEY, "false");

    if uri.get(CONTROL_MODE_KEY) != Some(CONTROL_MODE_RESPONSE) {
        uri.put(SESSION_ID_KEY, replay_session_id.to_string());
    }

    if is_tagged || has_live_destination {
        uri.remove(ENDPOINT_KEY);

        // **The tags go on only when there is a tag**, which is the one place
        // this build departs from the reference's channel text — and it is a
        // departure the reference's own client library makes for it. The
        // reference writes `channelTagId + "," + subscriptionTagId`
        // unconditionally (`:539`), so a replication that named a live
        // destination and no tags — the only way to reach this branch without
        // a tag — asks for a subscription on `tags=-1,-1`. That is
        // `Aeron.NULL_VALUE` written as a tag: the Java driver reads it as a
        // **real** tag of `-1` (`UdpChannel.java:216-218`, and `matchesTag`
        // matches on equal tags, `:786`), and the C driver refuses the channel
        // outright (`aeron_udp_channel.c:385-390`, because its "no tag"
        // sentinel is the same `-1` its parser returns for a value it cannot
        // read).
        //
        // `ChannelUriStringBuilder.tags(Long, Long)` — the way every other
        // caller writes this parameter — says "no tags" by leaving it out
        // (`ChannelUriStringBuilder.java:1250-1264`), and that is what this
        // does. See `docs/compat.md`, "The channel URIs this driver serves";
        // the criterion is
        // `a_live_replication_merges_into_the_stream_it_is_copying` (in
        // `deepmsg-tests`), which pins the channel the live pair produces.
        if is_tagged {
            uri.put(TAGS_KEY, format!("{channel_tag_id},{subscription_tag_id}"));
        }

        uri.put(CONTROL_MODE_KEY, CONTROL_MODE_MANUAL);
    }

    Ok(uri.build())
}

impl ReplicationSession {
    /// A session that has not started: `state = CONNECT`
    /// (`ReplicationSession.java:110`).
    ///
    /// `replication_id` is minted by the conductor from its session counter
    /// (`ArchiveConductor.java:1455`) and handed in: the request carries a
    /// **correlation** id, which is a different number.
    ///
    /// `destination` is the catalog's summary of the recording the request
    /// asked the copy to go into, or `None` when it asked for a new one — the
    /// reference's `hasRecording ? recordingSummary : null` (`:1465`). Two
    /// fields are read off it **here** rather than in `EXTEND`, because the
    /// reference reads them here: where the replay starts, and whether there is
    /// anything there to replace.
    #[must_use]
    pub fn new(
        replication_id: i64,
        request: ReplicationRequest,
        replication_channel: String,
        context: ArchiveContext,
        destination: Option<Recording>,
    ) -> Self {
        let action_timeout_ms = context.message_timeout_ns / 1_000_000;
        // Read out before the request moves into the session: `REPLAY` passes
        // it on to the source (`:712`).
        let file_io_max_length = request.file_io_max_length;
        let credentials = SessionCredentials::for_request(&request.encoded_credentials);
        let (replay_position, replay_stream_id, is_destination_recording_empty) = match destination
        {
            Some(recording) => (
                recording.stop_position,
                recording.stream_id,
                recording.start_position == recording.stop_position,
            ),
            None => (NULL_POSITION, 0, false),
        };

        Self {
            state: State::Connect,
            control_session_id: request.session_id,
            replication_id: Some(replication_id),
            live_destination: Some(request.live_destination.clone()).filter(|it| !it.is_empty()),
            dst_recording_id: request.dst_recording_id,
            dst_stop_position: request.stop_position,
            request,
            replication_channel,
            context,
            session_id_request: None,
            async_connect: None,
            src_archive: None,
            notices: Vec::new(),
            recording_subscription: None,
            file_io_max_length,
            replay_token: NULL_VALUE,
            src_replay_session_id: NULL_VALUE,
            credentials,
            now_ms: 0,
            extend_asked: false,
            pending_destinations: Vec::new(),
            extend_answer: None,
            active_correlation_id: NULL_VALUE,
            time_of_last_action_ms: 0,
            action_timeout_ms,
            src_stop_position: NULL_POSITION,
            src_recording_position: NULL_POSITION,
            replay_position,
            replay_stream_id,
            replay_session_id: 0,
            is_destination_recording_empty,
            replay_destination: None,
            is_live_added: false,
            retry_attempts: RETRY_ATTEMPTS,
        }
    }

    /// Take what this session owes the destination's client, which the conductor
    /// sends ([`ReplicationNotice`]).
    pub fn take_notices(&mut self) -> Vec<ReplicationNotice> {
        std::mem::take(&mut self.notices)
    }

    /// The id this session is registered under, which is what an OK carried and
    /// what a stop names — `None` once it has been stopped once.
    #[must_use]
    pub fn replication_id(&self) -> Option<i64> {
        self.replication_id
    }

    /// `replicationSessionByIdMap.remove(replicationId)`
    /// (`ArchiveConductor.java:1482`), which the **stop** does and the session
    /// does not: what is given up is the id, and the session goes on.
    pub fn unregister(&mut self) {
        self.replication_id = None;
    }

    /// Where the machine has got to.
    #[must_use]
    pub fn state(&self) -> State {
        self.state
    }

    /// `ReplicationSession.abort(reason)` (`:188-191`), whose whole body is
    /// `state(DONE, "abort")`.
    ///
    /// **The state change alone is not what ends a replication**: what ends it
    /// is the `close()` that follows (`:196-213`), which is where the teardown
    /// and `REPLICATE_END` live. So a session that has been aborted still owes
    /// one, and
    /// [`Sessions::drive_replication_sessions`](super::conductor::Sessions) is
    /// where it is paid.
    pub fn abort(&mut self, _reason: &str) {
        self.go_to(State::Done);
    }

    /// The session that asked for this, which every answer goes to
    /// (`ReplicationRequest::session_id`).
    #[must_use]
    pub fn control_session_id(&self) -> i64 {
        self.control_session_id
    }

    /// One turn (`ReplicationSession.doWork`, `:217-293` — the reference has no
    /// such method and instead *is* a `Session`, which the conductor's worker
    /// calls `doWork` on).
    ///
    /// **`CONNECT` is the only state that walks yet.** It is the one that ends
    /// by handing the session an archive client of its own, which is what every
    /// state after it asks the source through, and it is the one place a
    /// replication can be blocked outright.
    pub fn do_work(
        &mut self,
        client: &mut Client,
        catalog: &mut Catalog,
        now_ms: i64,
    ) -> ReplicationProgress {
        self.now_ms = now_ms;

        if self.state == State::Done {
            return ReplicationProgress::Done;
        }

        // The destinations this session has added and taken away, once the
        // driver has answered them.
        //
        // Nothing reads the answers — the reference does not either, its
        // `Subscription` polls its own destinations and no state asks — so what
        // this is for is the *retirement* of the pending entries: a handle that
        // is never polled leaves the client holding a record of a command
        // whose only remaining outcome is a timeout. One poll per turn until
        // they are answered, and then they are dropped.
        self.pending_destinations.retain(|add| {
            matches!(
                client.destination_poll(*add),
                AsyncDestinationPoll::Awaiting
            )
        });

        // `ReplicationSession.doWork`'s `try` (`:273-278`): a state that throws
        // has its failure answered **for it**, with `GENERIC` and the
        // exception's own message, and then the state is over. A state that
        // answers with a code of its own returns `Ok` and has done so.
        let outcome = match self.state {
            State::Connect => self.connect(client),
            State::ReplicateDescriptor => self.replicate_descriptor(client, catalog, now_ms),
            State::SrcRecordingPosition => self.src_recording_position(client, now_ms),
            State::Extend => self.extend(client),
            State::ReplayToken => self.replay_token(),
            State::GetArchiveProxy => self.get_archive_proxy(),
            State::Replay => self.replay(client, now_ms),
            State::AwaitImage => self.await_image(client, now_ms),
            State::Replicate => self.replicate(client, now_ms),
            State::Catchup => self.catchup(client),
            State::AttemptLiveJoin => self.attempt_live_join(client, now_ms),
            // Not reachable: a session in `DONE` is returned above, and every
            // walk into it is a state that has already said what it did.
            State::Done => Ok(()),
        };

        if let Err(reason) = outcome {
            self.fail(GENERIC, reason);
        }

        if self.state == State::Done {
            ReplicationProgress::Done
        } else {
            ReplicationProgress::Idle
        }
    }

    /// `ReplicationSession.replicateDescriptor` (`:444-483`): ask the source for
    /// the recording's descriptor, and let [`Self::on_recording_descriptor`]
    /// answer what it says.
    ///
    /// Two halves on one correlation id, like every state: the first sends the
    /// listing and arms the poller, the second reads it. The **descriptor is
    /// handed over through a local `Vec` and not through `self`** — the poller
    /// is borrowed from `src_archive`, which is a field of `self`, so a
    /// consumer that wrote to the session would be a second borrow of it. The
    /// reference has no such problem: its consumer *is* the session
    /// (`:454` passes `this`), and Java has no borrow checker to object with.
    fn replicate_descriptor(
        &mut self,
        client: &mut Client,
        catalog: &mut Catalog,
        now_ms: i64,
    ) -> Result<(), String> {
        if self.src_archive.is_none() {
            return Err("no source archive: the connect did not finish".to_owned());
        }

        if self.active_correlation_id == NULL_VALUE {
            // The borrow of the source is taken and given back around each
            // step, because `track_action` writes to `self` and the archive is
            // a field of it. The reference has one line here (`:451-455`).
            let correlation_id = {
                let Some(archive) = self.src_archive.as_mut() else {
                    return Ok(());
                };

                archive
                    .next_correlation_id(client)
                    .map_err(|error| error.to_string())?
            };

            let offered = {
                let Some(archive) = self.src_archive.as_mut() else {
                    return Ok(());
                };

                archive
                    .proxy_mut()
                    .list_recording(client, correlation_id, self.request.src_recording_id)
                    .is_ok()
            };

            if !offered {
                return self.expire_or_hold(now_ms, "failed to list remote recording descriptor");
            }

            self.track_action(correlation_id, now_ms);

            if let Some(archive) = self.src_archive.as_mut() {
                archive.descriptor_poller_mut().reset(correlation_id, 1);
            }

            return Ok(());
        }

        let mut descriptors: Vec<RecordingDescriptor> = Vec::new();

        // `poller.isDispatchComplete() && remainingRecordCount() > 0` (`:466`):
        // the listing ended without handing over the one descriptor it was asked
        // for, which is the source saying it has no such recording.
        let listing_over_with_one_owed = {
            let Some(archive) = self.src_archive.as_mut() else {
                return Ok(());
            };

            let poller = archive.descriptor_poller_mut();

            poller
                .poll(client, &mut |descriptor| {
                    descriptors.push(descriptor.clone())
                })
                .map_err(|error| error.to_string())?;

            poller.is_dispatch_complete() && poller.remaining_record_count() > 0
        };

        if listing_over_with_one_owed {
            self.fail(
                UNKNOWN_RECORDING,
                format!("unknown src recording id {}", self.request.src_recording_id),
            );

            return Ok(());
        }

        for descriptor in &descriptors {
            self.on_recording_descriptor(descriptor, catalog);
        }

        if descriptors.is_empty() {
            return self.expire_or_hold(now_ms, "failed to fetch remote recording descriptor");
        }

        Ok(())
    }

    /// `ReplicationSession.extend` (`:525-568`): make the **destination**
    /// recording — the subscription that will receive the replay.
    ///
    /// What the reference does in one call is two things here, because the call
    /// it makes cannot be made from where this runs:
    ///
    /// * it builds the channel (`:534-546`) — the replication channel with
    ///   `rejoin=false`, and with `session-id=<replaySessionId>` unless that
    ///   channel is a response one, which is what ties the incoming replay to
    ///   this copy;
    /// * and it calls `conductor.extendRecording(...)` (`:549`), which is
    ///   **synchronous** for the reference and returns either the subscription
    ///   or the reason there is none.
    ///
    /// So the second half goes out as a [`ReplicationNotice::Extend`] and the
    /// answer comes back on a later turn, written onto this session by
    /// [`Sessions::record_extend_outcome`]. A refusal is **already answered to
    /// the client** by the conductor — `extendRecording` sends that response
    /// itself (`ArchiveConductor.java:1085-1111`) — so ending the session here
    /// says nothing more, which is what the reference's `state(DONE, msg)`
    /// (`:566`) does too.
    ///
    /// **No deadline**, and that is not an oversight: `expire_or_hold` bounds
    /// the states the reference bounds, and this one it does not — there is no
    /// `TimeoutException` in `extend`, because there is nothing here to wait
    /// for once the call has been made. The conductor answers every request it
    /// is handed, this turn or the next.
    ///
    /// **The `isMds` half is here too.** `isTagged || null != liveDestination`
    /// (`:527`) rewrites the channel again — endpoint off, tags on,
    /// `control-mode=manual` — and then, once the extend has been answered,
    /// adds the endpoint back as a **destination** (`:550-555`). The two halves
    /// are one thing: a manual subscription has no endpoints of its own, so the
    /// destination is what binds the address the replay arrives on, and a
    /// subscription given the channel without it would never see an image.
    ///
    /// It takes the client for that reason, and only for that: the add is a
    /// client command, and the client here is the one that holds the recording
    /// subscription — this archive's own.
    fn extend(&mut self, client: &mut Client) -> Result<(), String> {
        if let Some(answer) = self.extend_answer.take() {
            match answer {
                Ok(registration_id) => {
                    self.recording_subscription = Some(registration_id);

                    if self.is_mds() {
                        let destination = self.replay_destination()?;

                        self.add_destination(client, registration_id, &destination)?;
                        self.replay_destination = Some(destination);
                    }

                    self.go_to(State::ReplayToken);
                }
                Err(_) => self.state = State::Done,
            }

            return Ok(());
        }

        if self.extend_asked {
            // The answer is the **registration**, and the registration is the
            // driver's to confirm ([`crate::server::conductor`]: the extend's
            // reply is sent once the subscription is in the registry, the way
            // the reference's `extendRecording` answers after its own
            // `addSubscription`). So this state waits like every other one, and
            // a driver that never answers ends the replication rather than
            // leaving it here for ever.
            return self.expire_or_hold(self.now_ms, "the extend was never answered");
        }

        let channel = self.extend_channel()?;

        self.extend_asked = true;
        self.notices.push(ReplicationNotice::Extend {
            request: ExtendRecordingRequest {
                session_id: self.control_session_id,
                correlation_id: self.replication_id.unwrap_or(NULL_VALUE),
                recording_id: self.dst_recording_id,
                stream_id: self.replay_stream_id,
                source_location: SourceLocation::REMOTE,
                auto_stop: true,
                original_channel: channel,
            },
        });

        Ok(())
    }

    /// The channel the destination records on (`:534-546`).
    ///
    /// Two parameters, and both are the copy's rather than the channel's:
    /// `rejoin=false`, so an image that goes away is not fought for, and the
    /// session id the replay will arrive under — which is the **source's**
    /// replay session, settled off the descriptor, not this session's own.
    ///
    /// The third is the multi-destination rewrite (`:536-541`), and it undoes
    /// the first two's subject: the channel stops naming where to bind at all,
    /// and names **tags** instead — `channelTagId,subscriptionTagId`, the
    /// pair the request carried — with `control-mode=manual` so the client
    /// decides where it reads from. The endpoint it takes off is not lost: it
    /// is what [`Self::replay_destination`] puts back, on the destination.
    fn extend_channel(&self) -> Result<String, String> {
        extend_channel_uri(
            &self.replication_channel,
            self.replay_session_id,
            self.is_tagged(),
            self.request.channel_tag_id,
            self.request.subscription_tag_id,
            self.live_destination.is_some(),
        )
    }

    /// `isMds` (`:527`): whether the destination subscribes **by tag or by
    /// hand**, which is the one thing `EXTEND` branches on.
    ///
    /// The reference's name for the branch is the condition, which reads
    /// backwards: `isTagged` is the field it keeps for the two tag ids
    /// (`:95`, `:150`), and a **live destination** joins it because a copy
    /// that has to follow a stream which comes and goes cannot take its
    /// address from a channel — it has to be a destination that is added and
    /// removed, and a destination needs the manual shape to be addable.
    fn is_mds(&self) -> bool {
        self.is_tagged() || self.live_destination.is_some()
    }

    /// `isTagged` (`:95`, `:150`): whether the request named a **tag** at all.
    ///
    /// The reference keeps this as a field; here it is a question asked of the
    /// request, because it is the request that carries the two ids and nothing
    /// ever changes them.
    fn is_tagged(&self) -> bool {
        self.request.channel_tag_id != NULL_VALUE || self.request.subscription_tag_id != NULL_VALUE
    }

    /// The destination `EXTEND` adds (`:552`), or the reason there cannot be
    /// one.
    ///
    /// **Built from the copy's own channel**, not from the subscription's: the
    /// reference passes `replicationChannel` (`:552`) and the endpoint it took
    /// off it (`:529`), so the destination ends up naming the very address the
    /// channel used to — which is the whole point of the swap. The
    /// session-id/`rejoin`/tags the subscription's channel carries are
    /// deliberately not on it; see [`ChannelUri::create_destination_uri`].
    ///
    /// The reference keeps the result in `replayDestination` for
    /// `ATTEMPT_LIVE_JOIN` to remove when the live stream ends (`:875`). That
    /// state does not walk here yet, so this is computed when it is needed and
    /// kept nowhere.
    fn replay_destination(&self) -> Result<String, String> {
        let endpoint = ChannelUri::parse(&self.replication_channel)
            .map_err(|error| format!("{} is not a channel: {error}", self.replication_channel))?
            .get(ENDPOINT_KEY)
            .map(str::to_owned);

        ChannelUri::create_destination_uri(&self.replication_channel, endpoint.as_deref())
            .map_err(|error| error.to_string())
    }

    /// The destination commands the session sends, which it never waits for
    /// (`:553`, `:870`, `:875`).
    ///
    /// One place for the timeout they are all given — the session's own action
    /// deadline, because that is what bounds everything else a session sends —
    /// and one place for the handle bookkeeping [`ReplicationSession::do_work`]
    /// drains.
    fn add_destination(
        &mut self,
        client: &mut Client,
        registration_id: i64,
        destination: &str,
    ) -> Result<(), String> {
        let timeout =
            Duration::from_millis(u64::try_from(self.action_timeout_ms).unwrap_or(u64::MAX));

        let add = client
            .async_add_rcv_destination(registration_id, destination, timeout)
            .map_err(|error| format!("destination could not be added: {error}"))?;

        self.pending_destinations.push(add);

        Ok(())
    }

    /// The other half: taking a destination off again, which is how a merge
    /// ends (`:875`).
    fn remove_destination(
        &mut self,
        client: &mut Client,
        registration_id: i64,
        destination: &str,
    ) -> Result<(), String> {
        let timeout =
            Duration::from_millis(u64::try_from(self.action_timeout_ms).unwrap_or(u64::MAX));

        let remove = client
            .async_remove_rcv_destination(registration_id, destination, timeout)
            .map_err(|error| format!("destination could not be removed: {error}"))?;

        self.pending_destinations.push(remove);

        Ok(())
    }

    /// The conductor's answer to the extend, delivered a turn late
    /// (`ReplicationSession.extend`'s return value, `:549-568`).
    pub fn on_extend_answered(&mut self, outcome: Result<i64, (i64, String)>) {
        self.extend_answer = Some(outcome);
    }

    /// `ReplicationSession.srcRecordingPosition` (`:484-524`): ask the source
    /// where its recording has got to.
    ///
    /// **A live-source state, and it runs anyway.** What it produces is
    /// `srcRecordingPosition`, which the live pair reads — and the one thing
    /// this build's criteria ever do with it is the refusal below: a source
    /// recording that is *not* live answers `NULL_POSITION`, which a replication
    /// with no live destination is perfectly happy with. The reference walks the
    /// state for every replication and branches on the value, and so does this.
    ///
    /// Two halves, like every state: send the request and park on its
    /// correlation id, then read the answer. What is different is **how** the
    /// answer is read — a single non-blocking poll of the source's control
    /// channel ([`Archive::poll_for_response`]) rather than a wait, because a
    /// turn that waits here is a turn the client never gets driven in.
    fn src_recording_position(&mut self, client: &mut Client, now_ms: i64) -> Result<(), String> {
        if self.src_archive.is_none() {
            return Err("no source archive: the connect did not finish".to_owned());
        }

        if self.active_correlation_id == NULL_VALUE {
            let correlation_id = {
                let Some(archive) = self.src_archive.as_mut() else {
                    return Ok(());
                };

                archive
                    .next_correlation_id(client)
                    .map_err(|error| error.to_string())?
            };

            let offered = {
                let Some(archive) = self.src_archive.as_mut() else {
                    return Ok(());
                };

                archive
                    .proxy_mut()
                    .get_recording_position(client, correlation_id, self.request.src_recording_id)
                    .is_ok()
            };

            // `else if (time >= …) throw` (`:498-501`): a request that could
            // not be published is retried until the action deadline, not
            // failed on the spot — the publication it goes out on is one the
            // source may not have connected yet.
            if !offered {
                return self.expire_or_hold(now_ms, "failed to send recording position request");
            }

            self.track_action(correlation_id, now_ms);

            return Ok(());
        }

        let answer = {
            let Some(archive) = self.src_archive.as_mut() else {
                return Ok(());
            };

            archive
                .poll_for_response(client, self.active_correlation_id)
                .map_err(|error| error.to_string())?
        };

        let Some(position) = answer else {
            return self.expire_or_hold(now_ms, "failed to get recording position");
        };

        self.src_recording_position = position;

        // `:509-512` — a live destination is the only caller that needs the
        // source to still be recording, and it is the one case where finding
        // out it is not is fatal. The message is the reference's, and it
        // travels as the state's failure, so it is answered `GENERIC`.
        if NULL_POSITION == position && self.live_destination.is_some() {
            return Err("cannot live merge without active source recording".to_owned());
        }

        self.go_to(State::Extend);

        Ok(())
    }

    /// `ReplicationSession.onRecordingDescriptor` (`:295-399`): what the
    /// **destination** does with what the source said.
    ///
    /// Three things, in the reference's order: the two ids the descriptor
    /// settles, the destination's catalog row, and the state the session goes
    /// to next.
    fn on_recording_descriptor(&mut self, descriptor: &RecordingDescriptor, catalog: &mut Catalog) {
        self.src_stop_position = descriptor.stop_position;
        self.replay_stream_id = descriptor.stream_id;

        // `replaySessionId` (`:308-315`), which the reference settles here and
        // `EXTEND` puts on the destination's channel. A request that named one
        // keeps it; otherwise the source's own session id is what the replay
        // will arrive under — **unless a live destination is in play**, which is
        // the branch `SRC_RECORDING_POSITION` is for and which nothing this
        // slice is held to reaches.
        self.replay_session_id = match self.live_destination {
            None if self.request.replication_session_id != NULL_VALUE as i32 => {
                self.request.replication_session_id
            }
            _ => descriptor.session_id,
        };

        // `:324-330`. A source that reads less per turn than the recording's
        // MTU could not keep up with it, and the reference refuses the
        // replication rather than replaying it badly — "than than" and all.
        if self.file_io_max_length != NULL_VALUE as i32
            && self.file_io_max_length < descriptor.mtu_length
        {
            self.fail(
                GENERIC,
                format!(
                    "Replication fileIoMaxLength ({}) is less than than the recording mtuLength ({})",
                    self.file_io_max_length, descriptor.mtu_length
                ),
            );

            return;
        }

        if let Some(destination) = self.destination_row(descriptor, catalog) {
            if self.request.dst_recording_id == NULL_VALUE {
                // `:336`: a copy into a **new** row starts where the source's
                // recording does.
                self.replay_position = descriptor.start_position;

                match catalog.add_recording(&destination) {
                    Ok(recording_id) => {
                        self.dst_recording_id = recording_id;

                        // `signal(startPosition, REPLICATE)` (`:351`): the
                        // destination's client is told a recording it asked for
                        // now has a row, before anything is copied into it.
                        self.notices.push(ReplicationNotice::Signal {
                            recording_id: self.dst_recording_id,
                            subscription_id: NULL_VALUE,
                            position: descriptor.start_position,
                            signal: RecordingSignal::REPLICATE,
                        });
                    }
                    Err(error) => {
                        self.fail(
                            GENERIC,
                            format!("catalog could not add the recording: {error}"),
                        );
                    }
                }
            } else if self.is_destination_recording_empty {
                // `:360`: and so does one into a row that exists but is empty.
                self.replay_position = descriptor.start_position;

                if let Err(error) = catalog.replace_recording(&destination) {
                    self.fail(
                        GENERIC,
                        format!("catalog could not replace the recording: {error}"),
                    );
                }
            }
        }

        if self.state == State::Done {
            return;
        }

        // `:380-399`. The live branch is out of reach for every criterion this
        // build is held to (`live_destination` is empty in every one of them)
        // and the state it names does not walk yet; the transition is written
        // down because the **refusal** in front of it is the reference's, and
        // it is the one a live request meets.
        if self.live_destination.is_some() {
            if self.request.stop_position != NULL_POSITION {
                self.fail(
                    GENERIC,
                    "cannot live merge without active source recording".to_owned(),
                );

                return;
            }

            self.go_to(State::SrcRecordingPosition);
            return;
        }

        // `:393-398`: the shortcut. A source with nothing to copy, or a
        // destination already at the source's stop, is **in sync** — the client
        // is told so with a `SYNC` and the replication is over before a byte
        // moves.
        let in_sync = descriptor.start_position == descriptor.stop_position
            || (self.dst_recording_id != NULL_VALUE
                && catalog
                    .recording(self.dst_recording_id)
                    .is_ok_and(|it| it.stop_position == descriptor.stop_position));

        if in_sync {
            self.notices.push(ReplicationNotice::Signal {
                recording_id: self.dst_recording_id,
                subscription_id: NULL_VALUE,
                position: descriptor.stop_position,
                signal: RecordingSignal::SYNC,
            });
            self.go_to(State::Done);

            return;
        }

        self.go_to(State::Extend);
    }

    /// The row [`Self::on_recording_descriptor`] writes: the source's facts
    /// under this archive's recording id (`:336-348`, `:356-370`).
    ///
    /// `None` when the descriptor cannot make one, which is the file-io guard
    /// the reference makes before either branch (`:323-330`): a request that
    /// asked for a smaller block than the recording's MTU is refused with
    /// `GENERIC` rather than copied badly.
    fn destination_row(
        &mut self,
        descriptor: &RecordingDescriptor,
        _catalog: &Catalog,
    ) -> Option<Recording> {
        let file_io_max_length = self.request.file_io_max_length;
        if file_io_max_length != NULL_VALUE as i32 && file_io_max_length < descriptor.mtu_length {
            self.fail(
                GENERIC,
                format!(
                    "Replication fileIoMaxLength ({}) is less than than the recording \
                     mtuLength ({})",
                    file_io_max_length, descriptor.mtu_length
                ),
            );

            return None;
        }

        self.replay_position = descriptor.start_position;

        Some(Recording {
            recording_id: self.dst_recording_id,
            start_timestamp: descriptor.start_timestamp,
            stop_timestamp: descriptor.start_timestamp,
            start_position: descriptor.start_position,
            stop_position: descriptor.start_position,
            initial_term_id: descriptor.initial_term_id,
            segment_file_length: descriptor.segment_file_length,
            term_buffer_length: descriptor.term_buffer_length,
            mtu_length: descriptor.mtu_length,
            session_id: descriptor.session_id,
            stream_id: descriptor.stream_id,
            stripped_channel: descriptor.stripped_channel.clone(),
            original_channel: descriptor.original_channel.clone(),
            source_identity: descriptor.source_identity.clone(),
        })
    }

    /// `trackAction` (`:960-965`): arm the deadline on the request just sent.
    fn track_action(&mut self, correlation_id: i64, now_ms: i64) {
        self.time_of_last_action_ms = now_ms;
        self.active_correlation_id = correlation_id;
    }

    /// `ReplicationSession.state` (`:975-983`): the state, **and the two writes
    /// that go with it**.
    ///
    /// Not a plain assignment, and both of the others are load-bearing:
    ///
    /// * `activeCorrelationId` is cleared, because the correlation id belonged
    ///   to the state that just ended. A state entered still holding one reads
    ///   **an answer that is not its own** — `hasResponse` compares against it
    ///   (`:912`), and so does every `NULL_VALUE == activeCorrelationId` guard
    ///   that decides whether to send a request at all;
    /// * `timeOfLastActionMs` is set to now, so that the state's deadline is
    ///   measured from its own start rather than from the previous state's
    ///   last action.
    ///
    /// **Measured, and this is what it cost**: with the two missing, `REPLAY`
    /// entered holding `SRC_RECORDING_POSITION`'s correlation id, took the
    /// "an answer is already in flight" branch, and waited out its deadline for
    /// a reply to a request it had never sent. Nothing upstream showed it —
    /// `EXTEND`, `REPLAY_TOKEN` and `GET_ARCHIVE_PROXY` do not read the field.
    fn go_to(&mut self, state: State) {
        self.state = state;
        self.active_correlation_id = NULL_VALUE;
        self.time_of_last_action_ms = self.now_ms;
    }

    /// A state that has not heard back and whose deadline has passed answers its
    /// own `TimeoutException` — or keeps waiting (`:478-481`).
    fn expire_or_hold(&self, now_ms: i64, reason: &str) -> Result<(), String> {
        if now_ms
            >= self
                .time_of_last_action_ms
                .saturating_add(self.action_timeout_ms)
        {
            Err(reason.to_owned())
        } else {
            Ok(())
        }
    }

    /// `ReplicationSession.replayToken` (`:569-605`).
    ///
    /// **A pass-through for every channel this build serves.** The state exists
    /// to ask the source for a token when the copy runs over a **response
    /// channel** (`:573-575` is the whole of its guard), and the token is what
    /// `GET_ARCHIVE_PROXY` turns into a second publication for an image that is
    /// not this session's.
    ///
    /// No criterion in this slice reaches a response-channel replication (plan
    /// §3.2) and the request behind it — `requestReplayToken`, template 105 — is
    /// not built, so a session that did take that branch **ends loudly** rather
    /// than walking on to build a publication out of a token it never asked
    /// for. A silent pass-through there would be a session that looks like it
    /// works and copies nothing.
    fn replay_token(&mut self) -> Result<(), String> {
        if self.replay_token != NULL_VALUE || !self.replication_channel_is_response() {
            self.go_to(State::GetArchiveProxy);

            return Ok(());
        }

        Err(
            "a replication over a response channel needs a replay token, which \
             this build does not ask for"
                .to_owned(),
        )
    }

    /// `ReplicationSession.getArchiveProxy` (`:606-665`).
    ///
    /// The reference's **first branch is this whole state** for a session that
    /// has no token (`:610-614`), and that is every session this build drives:
    /// [`State::ReplayToken`] is the only thing that sets one and it refuses to
    /// ask. The rest — a second publication on the source's control channel,
    /// waited for until it is connected and has a window — is therefore not
    /// built, and a session that somehow reached it says so rather than
    /// producing a proxy that is not there.
    fn get_archive_proxy(&mut self) -> Result<(), String> {
        if self.replay_token != NULL_VALUE {
            return Err("a replay token needs a second publication, which this \
                        build does not make"
                .to_owned());
        }

        self.go_to(State::Replay);

        Ok(())
    }

    /// `ReplicationSession.replay` (`:666-752`): ask the **source** to replay
    /// the recording onto the destination.
    ///
    /// Three things are the copy's rather than the replay's, and all three go
    /// onto the channel the source is asked for:
    ///
    /// * the destination subscription's **resolved endpoint**, when the copy's
    ///   channel named a wildcard port — the source cannot send to `:0`;
    /// * `linger=0` and `eos=false` for a live destination, so the replay
    ///   publication does not go away when this session does;
    /// * and either the destination subscription's registration id, for a
    ///   response channel, or `session-id=<replaySessionId>` — which is what
    ///   makes the destination's subscription and the source's replay
    ///   publication the same stream.
    ///
    /// The request carries where to start and how much: `replayPosition`, and
    /// `dstStopPosition - replayPosition` when the request named a stop —
    /// otherwise `NULL_LENGTH`, which follows the source (`AeronArchive.java:106-117`).
    ///
    /// The answer is the **source's** replay session id, which is what
    /// `AWAIT_IMAGE` looks for at the destination. It is read a turn at a time,
    /// like every other answer this session waits for.
    fn replay(&mut self, client: &mut Client, now_ms: i64) -> Result<(), String> {
        if self.src_archive.is_none() {
            return Err("no source archive: the connect did not finish".to_owned());
        }

        if self.active_correlation_id == NULL_VALUE {
            let Some(registration_id) = self.recording_subscription else {
                return Err("no destination subscription: EXTEND did not finish".to_owned());
            };

            // The reference's `recordingSubscription.resolvedEndpoint()`
            // (`:668-681`): "not yet" is a wait, not a failure — a subscription
            // that has just been added has no port until the driver says so.
            let resolved = client.counters_reader().and_then(|counters| {
                client
                    .subscription(registration_id)?
                    .resolved_endpoint(&counters)
            });

            let Some(resolved) = resolved else {
                let channel = client
                    .subscription(registration_id)
                    .map_or_else(String::new, |subscription| {
                        subscription.channel().to_owned()
                    });

                return self.expire_or_hold(
                    now_ms,
                    &format!("failed to resolve subscription endpoint: channel={channel}"),
                );
            };

            let channel = self.replay_channel(registration_id, &resolved)?;

            let params = ReplayParams {
                position: self.replay_position,
                length: if self.dst_stop_position == NULL_POSITION {
                    NULL_LENGTH
                } else {
                    self.dst_stop_position.saturating_sub(self.replay_position)
                },
                file_io_max_length: self.file_io_max_length,
                replay_token: self.replay_token,
                ..ReplayParams::default()
            };

            let correlation_id = {
                let Some(archive) = self.src_archive.as_mut() else {
                    return Ok(());
                };

                archive
                    .next_correlation_id(client)
                    .map_err(|error| error.to_string())?
            };

            let offered = {
                let Some(archive) = self.src_archive.as_mut() else {
                    return Ok(());
                };

                archive
                    .proxy_mut()
                    .replay(
                        client,
                        correlation_id,
                        self.request.src_recording_id,
                        &channel,
                        self.replay_stream_id,
                        &params,
                    )
                    .is_ok()
            };

            if !offered {
                return self.expire_or_hold(now_ms, "failed to send replay request");
            }

            self.track_action(correlation_id, now_ms);

            return Ok(());
        }

        let answer = {
            let Some(archive) = self.src_archive.as_mut() else {
                return Ok(());
            };

            archive
                .poll_for_response(client, self.active_correlation_id)
                .map_err(|error| error.to_string())?
        };

        let Some(session_id) = answer else {
            return self.expire_or_hold(
                now_ms,
                &format!(
                    "failed get acknowledgement of replay request to: {}",
                    self.replication_channel
                ),
            );
        };

        self.src_replay_session_id = session_id;
        self.go_to(State::AwaitImage);

        Ok(())
    }

    /// What `REPLAY` asks the source for (`:683-703`).
    fn replay_channel(&self, registration_id: i64, resolved: &str) -> Result<String, String> {
        let mut uri = ChannelUri::parse(&self.replication_channel)
            .map_err(|error| format!("{} is not a channel: {error}", self.replication_channel))?;

        // `replaceEndpointWildcardPort` (`ChannelUri.java:613-637`). Its two
        // refusals are out of reach from here — the caller has already checked
        // there is an endpoint, and a resolved one is bound — and the branch
        // that matters is the wildcard port, which a source cannot send to.
        if let Some(endpoint) = uri.get(ENDPOINT_KEY) {
            if let (true, Some((_, port))) = (endpoint.ends_with(":0"), resolved.rsplit_once(':')) {
                uri.put(
                    ENDPOINT_KEY,
                    format!("{}:{port}", &endpoint[..endpoint.len() - 2]),
                );
            }
        }

        if self.live_destination.is_some() {
            uri.put(LINGER_KEY, "0");
            uri.put(EOS_KEY, "false");
        }

        if self.replication_channel_is_response() {
            uri.put(RESPONSE_CORRELATION_ID_KEY, registration_id.to_string());
        } else {
            uri.put(SESSION_ID_KEY, self.replay_session_id.to_string());
        }

        Ok(uri.build())
    }

    /// `ChannelUri.hasControlModeResponse()` for the copy's own channel, which
    /// is the question `REPLAY_TOKEN`, `EXTEND` and `REPLAY` each ask it.
    fn replication_channel_is_response(&self) -> bool {
        ChannelUri::parse(&self.replication_channel)
            .is_ok_and(|uri| uri.get(CONTROL_MODE_KEY) == Some(CONTROL_MODE_RESPONSE))
    }

    /// `(int)srcReplaySessionId` (`:760`), which is what the image is looked up
    /// by — and the cast is not a formality.
    ///
    /// The id the source answers with is the archive's own replay counter in
    /// the **high** half and the replay publication's Aeron session in the low
    /// one (`ArchiveConductor.java:954`:
    /// `((long)(replayId++) << 32) | (replayPublication.sessionId() & 0xFFFF_FFFFL)`),
    /// and an image carries only the second. **Measured**: without the cast the
    /// session sat in `AWAIT_IMAGE` for its whole deadline while the copy itself
    /// ran — the bytes arrived and the session never noticed.
    fn replay_image_session_id(&self) -> i32 {
        self.src_replay_session_id as i32
    }

    /// The destination's image, if it still has one.
    ///
    /// The reference holds the `Image` object and asks *it* whether it is
    /// closed (`:781`, `:826`, `:884`); this looks the image up each turn, so
    /// **an image that is no longer there is the closed one**. The two readings
    /// agree on what every caller does about it: nothing arrives any more.
    ///
    /// ★ **The lookup answers for an image the recorder has taken as well as one
    /// the subscription still holds** (P2-9a). This session and the recording
    /// session read the *same* image — the reference's one object, referenced
    /// twice — and this build's two are the subscription's copy and the
    /// recorder's, which is why what is asked for here is a view rather than the
    /// image itself: the archive's recorder owns the image, publishes where it
    /// has read to, and this reads that.
    fn image_view(&self, client: &Client) -> Option<ImageView> {
        let registration_id = self.recording_subscription?;

        client
            .subscription(registration_id)?
            .image_view_by_session_id(self.replay_image_session_id())
    }

    /// `ReplicationSession.awaitImage` (`:753-776`): wait for the destination's
    /// subscription to be given the image the **source's replay** is.
    ///
    /// The image is looked for by session id — `srcReplaySessionId`, which the
    /// source named in its answer to `REPLAY` — and that is the whole of the
    /// test. Nothing here is the session's own doing: the image appears when the
    /// source's replay publication and the destination's subscription meet, and
    /// what the session does about it is notice.
    ///
    /// **A live destination goes to `CATCHUP` instead** (`:762-766`): the copy
    /// has a stream to join, and joining it before the replay has caught up
    /// would leave a gap in the recording.
    fn await_image(&mut self, client: &mut Client, now_ms: i64) -> Result<(), String> {
        let Some(registration_id) = self.recording_subscription else {
            return Err("no destination subscription: EXTEND did not finish".to_owned());
        };

        if self.image_view(client).is_some() {
            self.go_to(if self.live_destination.is_none() {
                State::Replicate
            } else {
                State::Catchup
            });

            return Ok(());
        }

        let channel = client
            .subscription(registration_id)
            .map_or_else(String::new, |subscription| {
                subscription.channel().to_owned()
            });

        self.expire_or_hold(
            now_ms,
            &format!(
                "failed get replay image for sessionId={} on channel={channel}",
                self.src_replay_session_id
            ),
        )
    }

    /// `ReplicationSession.replicate` (`:777-815`): follow the image until the
    /// copy is done, and say so.
    ///
    /// **Done** is any of four things (`:784-785`): the destination's reader has
    /// caught up with where the source's recording stopped, it has reached the
    /// stop the request asked for, the image is at end-of-stream, or it is
    /// closed. The first is the one a client is told about — a `SYNC` at the
    /// position reached (`:797-800`) — and it is the one every bounded copy this
    /// build serves ends on.
    ///
    /// The reference holds the image and asks *it* whether it is closed; this
    /// looks the image up each turn, so **an image that is no longer there is
    /// the closed one**. Both readings agree on what the session does about it:
    /// nothing is copied any more, so the session is over.
    ///
    /// The reference logs the ending through its `ArchiveLog`
    /// (`logReplicationSessionDone`, `:1017-1041`), which this build does not
    /// have (the same gap `recording_session.rs` records).
    fn replicate(&mut self, client: &mut Client, now_ms: i64) -> Result<(), String> {
        let _ = now_ms;

        let Some(registration_id) = self.recording_subscription else {
            return Err("no destination subscription: EXTEND did not finish".to_owned());
        };

        let (position, is_end_of_stream) = match self.image_view(client) {
            Some(image) => (image.position(), image.is_end_of_stream().unwrap_or(false)),
            None => (NULL_POSITION, true),
        };

        let is_synced =
            self.src_stop_position != NULL_POSITION && position >= self.src_stop_position;

        let done = is_synced
            || (self.dst_stop_position != NULL_POSITION && position >= self.dst_stop_position)
            || is_end_of_stream;

        if !done {
            return Ok(());
        }

        if is_synced {
            self.notices.push(ReplicationNotice::Signal {
                recording_id: self.dst_recording_id,
                subscription_id: registration_id,
                position,
                signal: RecordingSignal::SYNC,
            });
        }

        self.src_replay_session_id = NULL_VALUE;
        self.go_to(State::Done);

        Ok(())
    }

    /// `shouldAddLiveDestination` (`:955-959`): the live destination has not
    /// been added, and the copy is within `min(termBufferLength / 4,
    /// LIVE_ADD_MAX_WINDOW)` of where the source is **now**.
    ///
    /// The window is the point of the predicate. Adding the live destination
    /// makes the destination subscription accept a second stream for the same
    /// session, and a stream joined too far ahead of the replay is a hole in
    /// the recording; a term's quarter is what the driver can still be asked to
    /// bridge, and 32 MiB is the ceiling the client's own replay merge puts on
    /// it (`ReplayMerge.LIVE_ADD_MAX_WINDOW`, `ReplayMerge.java:54`).
    ///
    /// A **negative** difference counts as inside the window, and that is not an
    /// accident of the arithmetic: the copy may already be past the position the
    /// source last reported.
    fn should_add_live_destination(&self, position: i64, term_buffer_length: i32) -> bool {
        !self.is_live_added
            && (self.src_recording_position - position)
                <= i64::from(term_buffer_length >> 2).min(LIVE_ADD_MAX_WINDOW)
    }

    /// `shouldStopReplay` (`:961-966`): the live destination **has** been added,
    /// the copy is level with the source (`REPLAY_REMOVE_THRESHOLD` is nought),
    /// and the stream is arriving from two places at once.
    ///
    /// The third clause is the one that says the join worked. An image with one
    /// transport is still only the replay; the live stream is a second source
    /// for the same session, so `activeTransportCount() >= 2` is exactly "the
    /// live stream has arrived" — and taking the replay off before that would
    /// cut the recording where the replay ran out.
    fn should_stop_replay(&self, position: i64, active_transport_count: i32) -> bool {
        self.is_live_added
            && (self.src_recording_position - position) <= REPLAY_REMOVE_THRESHOLD
            && active_transport_count >= 2
    }

    /// `ReplicationSession.catchup` (`:816-833`): wait for the copy to reach the
    /// **source's live position** before joining it.
    ///
    /// One comparison and one refusal, and the comparison is the reason this
    /// state exists: a replay runs through history and a live stream is at the
    /// present, so joining live while the replay is still behind would drop
    /// everything in between. The refusal is the same one `REPLICATE` makes —
    /// an image that goes away mid-catch-up is a copy that cannot finish
    /// (`:826-829`) — and it is a **failure**, not the end of the copy.
    ///
    /// Nothing is asked of the source here: `srcRecordingPosition` was settled
    /// before `EXTEND` (`:509`), when the state that asks it ran for the
    /// reason this one needs it.
    fn catchup(&mut self, client: &mut Client) -> Result<(), String> {
        let Some(position) = self.image_view(client).map(|image| image.position()) else {
            return Err("replication image closed unexpectedly".to_owned());
        };

        if position >= self.src_recording_position {
            self.go_to(State::AttemptLiveJoin);
        }

        Ok(())
    }

    /// `ReplicationSession.attemptLiveJoin` (`:834-900`): join the copy to the
    /// live stream, and then take the replay off it.
    ///
    /// Three things happen here and they are not the same kind of thing:
    ///
    /// * the source is asked, repeatedly, where its recording has **now** got
    ///   to (`:841-865`). That is what makes this the one state that runs long:
    ///   each answer re-arms it, and the state is left by a predicate, not by an
    ///   answer;
    /// * while the copy is within a window of the live position and the live
    ///   destination has not been added, it is added (`:868-872`) — the stream
    ///   starts arriving at the destination under the **same session** as the
    ///   replay, so the two become one image with two transports;
    /// * once it has been added, the copy has caught up, and the stream really
    ///   is arriving from two places (`activeTransportCount() >= 2`), the
    ///   **replay** destination comes off and the session ends as a merge
    ///   (`:873-880`) — with a `MERGE` signal, and with the recording
    ///   subscription let go rather than stopped so that the destination's
    ///   recording keeps following the live stream.
    ///
    /// The two predicates are one mechanism read in two directions, and
    /// [`Self::should_add_live_destination`] and [`Self::should_stop_replay`]
    /// are where that is written down.
    ///
    /// **A source that stops answering is asked again, three times** (`:888-896`)
    /// — `retryAttempts` counts *consecutive* silences because every answer
    /// arms it again — and the image going away between answers fails the state
    /// outright (`:884-887`).
    fn attempt_live_join(&mut self, client: &mut Client, now_ms: i64) -> Result<(), String> {
        if self.src_archive.is_none() {
            return Err("no source archive: the connect did not finish".to_owned());
        }

        if self.active_correlation_id == NULL_VALUE {
            let correlation_id = {
                let Some(archive) = self.src_archive.as_mut() else {
                    return Ok(());
                };

                archive
                    .next_correlation_id(client)
                    .map_err(|error| error.to_string())?
            };

            let offered = {
                let Some(archive) = self.src_archive.as_mut() else {
                    return Ok(());
                };

                archive
                    .proxy_mut()
                    .get_recording_position(client, correlation_id, self.request.src_recording_id)
                    .is_ok()
            };

            if !offered {
                return self.expire_or_hold(now_ms, "failed to send recording position request");
            }

            self.track_action(correlation_id, now_ms);

            return Ok(());
        }

        let answer = {
            let Some(archive) = self.src_archive.as_mut() else {
                return Ok(());
            };

            archive
                .poll_for_response(client, self.active_correlation_id)
                .map_err(|error| error.to_string())?
        };

        let Some(position) = answer else {
            if self.image_view(client).is_none() {
                return Err("replication image closed unexpectedly".to_owned());
            }

            if now_ms >= self.time_of_last_action_ms + self.action_timeout_ms {
                self.retry_attempts -= 1;

                if self.retry_attempts == 0 {
                    return Err("failed to get recording position".to_owned());
                }

                self.track_action(NULL_VALUE, now_ms);
            }

            return Ok(());
        };

        self.track_action(NULL_VALUE, now_ms);
        self.retry_attempts = RETRY_ATTEMPTS;
        self.src_recording_position = position;

        if NULL_POSITION == position && self.live_destination.is_some() {
            return Err("cannot live merge without active source recording".to_owned());
        }

        let Some(image) = self.image_view(client) else {
            return Err("replication image closed unexpectedly".to_owned());
        };

        let (image_position, term_buffer_length, active_transport_count) = (
            image.position(),
            image.term_buffer_length(),
            image.active_transport_count().unwrap_or(0),
        );

        if self.should_add_live_destination(image_position, term_buffer_length) {
            let Some(destination) = self.live_destination.clone() else {
                return Ok(());
            };
            let Some(registration_id) = self.recording_subscription else {
                return Ok(());
            };

            self.add_destination(client, registration_id, &destination)?;
            self.is_live_added = true;

            return Ok(());
        }

        if !self.should_stop_replay(image_position, active_transport_count) {
            return Ok(());
        }

        if let (Some(registration_id), Some(replay_destination)) =
            (self.recording_subscription, self.replay_destination.take())
        {
            self.remove_destination(client, registration_id, &replay_destination)?;
        }

        // `:876-878`, in the reference's order: the subscription is let go
        // **before** the signal, which is why the `MERGE` carries no
        // subscription id, and it is let go rather than stopped so that the
        // destination's recording keeps following the live stream.
        self.recording_subscription = None;

        self.notices.push(ReplicationNotice::Signal {
            recording_id: self.dst_recording_id,
            subscription_id: NULL_VALUE,
            position: image_position,
            signal: RecordingSignal::MERGE,
        });

        self.go_to(State::Done);

        Ok(())
    }

    /// `ReplicationSession.close` (`:196-211`): the teardown, and the **one**
    /// `REPLICATE_END` — the signal the reference's own fixture waits for, and
    /// the only one of them that is sent exactly once.
    ///
    /// Four things, in the reference's order: the destination's recording is
    /// stopped, the source is told to stop the replay it was asked for, the
    /// source archive's client is closed, and the client is told the session is
    /// over. The reference also removes the session from the conductor's map
    /// here (`:209`); in this build that is the same act as the session leaving
    /// [`Sessions::drive_replication_sessions`]'s list, which is where `close`
    /// is called from.
    ///
    /// **The order is what the signal says.** `stopRecording` drops the
    /// recording subscription *first*, so the `REPLICATE_END` that follows
    /// carries no subscription id (`signal`, `:925`) — which is what a client
    /// that saw the `REPLICATE` signal, with one, will notice.
    pub fn close(&mut self, client: &mut Client) {
        if let Some(registration_id) = self.recording_subscription.take() {
            self.notices
                .push(ReplicationNotice::StopRecording { registration_id });
        }

        self.stop_replay_session(client);

        if let Some(mut archive) = self.src_archive.take() {
            archive.close(client);
        }

        self.notices.push(ReplicationNotice::Signal {
            recording_id: self.dst_recording_id,
            subscription_id: NULL_VALUE,
            position: NULL_POSITION,
            signal: RecordingSignal::REPLICATE_END,
        });
    }

    /// `stopReplaySession` (`:929-944`): tell the source to stop the replay, and
    /// do not wait for the answer — the reference catches and counts whatever
    /// goes wrong here, because the session is over either way.
    fn stop_replay_session(&mut self, client: &mut Client) {
        if self.src_replay_session_id == NULL_VALUE {
            return;
        }

        if let Some(archive) = self.src_archive.as_mut() {
            if let Ok(correlation_id) = archive.next_correlation_id(client) {
                let _ = archive.proxy_mut().stop_replay(
                    client,
                    correlation_id,
                    self.src_replay_session_id,
                );
            }
        }

        self.src_replay_session_id = NULL_VALUE;
    }

    /// `ReplicationSession.connect` (`:401-443`): make the connect, then poll it
    /// until it is done or has failed.
    ///
    /// **The whole reason this state exists as a state.** The reference's
    /// `AeronArchive.asyncConnect(context)` returns an object, and three of its
    /// lines are the machine: build it once, poll it each turn, and on a failure
    /// answer the client with `REPLICATION_CONNECTION_FAILURE`. A blocking
    /// connect here would be a deadlock rather than a slow start — the driver
    /// this connect needs to read is driven by the same thread that is waiting,
    /// outside the turn this runs in (`deepmsg-archiving-media-driver.rs`'s
    /// loop: `driver.do_work()` then `conductor.do_work`).
    fn connect(&mut self, client: &mut Client) -> Result<(), String> {
        if self.async_connect.is_none() {
            // The id first, and **without waiting for it**: the connect's own
            // context cannot ask the driver from here. A turn that is still
            // waiting answers `Ok(())` like any other waiting state, which is
            // what lets the turn finish and the driver be driven — and with it
            // the answer this session's `replicate` is owed.
            match self.acquire_session_id(client) {
                Ok(true) => {}
                Ok(false) => return Ok(()),
                Err(error) => {
                    self.fail(
                        REPLICATION_CONNECTION_FAILURE,
                        format!("Replication connection failed={error}"),
                    );

                    return Ok(());
                }
            }

            // `AeronArchive.asyncConnect(context)` (`:407`). Its failure modes
            // are the context's and the driver's, and both are the same answer
            // here as a late failure: the machine is over and the client is told
            // why.
            //
            // **This failure answers for itself** and does not travel as the
            // state's error: the reference's `catch` around the whole of
            // `connect` answers `REPLICATION_CONNECTION_FAILURE` and returns
            // (`:437-442`), where every other state's failure is the outer
            // `catch`'s and gets `GENERIC`.
            // The two fields are taken apart because one is read and the other
            // written for the length of the call.
            let Self {
                context,
                credentials,
                ..
            } = self;

            return match AsyncConnect::new(context, client, credentials) {
                Ok(connect) => {
                    self.async_connect = Some(connect);

                    Ok(())
                }
                Err(error) => {
                    self.fail(
                        REPLICATION_CONNECTION_FAILURE,
                        format!("Replication connection failed={error}"),
                    );

                    Ok(())
                }
            };
        }

        // The same supplier the connect was built with, for the reason every
        // poll takes one: the challenge can arrive on any turn.
        let Self {
            async_connect,
            credentials,
            ..
        } = self;

        let Some(connect) = async_connect.as_mut() else {
            return Ok(());
        };

        match connect.poll(client, credentials) {
            Ok(Polled::Awaiting) => Ok(()),
            Ok(Polled::Done(connected)) => {
                // `srcArchive = archive` (`:433`), built from what the connect
                // assembled rather than from a second one. The handlers are the
                // defaults: a replication session has no signal handler of its
                // own, and the recording signals it acts on are the ones **the
                // destination archive** sends its client, not the source's.
                self.src_archive = Some(Archive::from_connected(
                    self.context.clone(),
                    *connected,
                    Handlers::default(),
                ));
                self.async_connect = None;
                self.go_to(State::ReplicateDescriptor);

                Ok(())
            }
            Err(error) => {
                self.fail(
                    REPLICATION_CONNECTION_FAILURE,
                    format!("Replication connection failed={error}"),
                );

                Ok(())
            }
        }
    }

    /// One turn's worth of the wait for the session id the connect needs.
    /// Answers `true` when the context is ready to be concluded.
    ///
    /// Three answers, and the middle one is the point: a context whose response
    /// channel needs no id ([`ArchiveContext::needs_session_id`]) and one that
    /// already carries a prepared id are both ready at once; otherwise the
    /// request goes out on the first turn and is read on a later one.
    ///
    /// The reference does all of this inside `conclude` and blocks for it
    /// (`aeron_archive_context.c:373-420`: send, then a `do { invoke the client;
    /// poll } while`). The loop is the same shape; what it is missing here is the
    /// blocking, because there is nobody to hand the wait to.
    fn acquire_session_id(&mut self, client: &mut Client) -> Result<bool, String> {
        let needed = self
            .context
            .needs_session_id()
            .map_err(|error| error.to_string())?;

        if !needed || self.context.prepared_session_id.is_some() {
            return Ok(true);
        }

        let timeout = std::time::Duration::from_nanos(
            u64::try_from(self.context.message_timeout_ns).unwrap_or(0),
        );

        let Some(request) = self.session_id_request else {
            let request = client
                .async_next_session_id(self.context.control_request_stream_id, timeout)
                .map_err(|error| format!("Failed to fetch next session-id: {error}"))?;

            self.session_id_request = Some(request);

            return Ok(false);
        };

        match client.async_next_session_id_poll(request) {
            AsyncNextSessionIdPoll::Awaiting => Ok(false),
            AsyncNextSessionIdPoll::Ready(session_id) => {
                self.context.prepared_session_id = Some(session_id);
                self.session_id_request = None;

                Ok(true)
            }
            AsyncNextSessionIdPoll::Failed(error) => {
                Err(format!("Failed to fetch next session-id: {error}"))
            }
            // The pending entry is gone, and the only thing that takes one out
            // without answering it is the deadline in the client's own `poll`.
            AsyncNextSessionIdPoll::Unknown => {
                Err("Failed to fetch next session-id: timed out".to_owned())
            }
        }
    }

    /// `ReplicationSession.error` (`:923-926`) and the `state(DONE, …)` line
    /// every caller in front of it makes.
    ///
    /// The message is the caller's, because the reference's two errors are:
    /// `doWork`'s outer `catch` passes the exception's own words (`:275-277`)
    /// and `connect`'s prefixes `"Replication connection failed="` (`:441`).
    fn fail(&mut self, code: i64, message: String) {
        self.go_to(State::Done);
        self.notices.push(ReplicationNotice::Error {
            correlation_id: self.replication_id.unwrap_or(NULL_VALUE),
            code,
            message,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The channel a request names, in the shape every replication in this
    /// build's tests uses: a localhost endpoint on an ephemeral port.
    const PLAIN: &str = "aeron:udp?endpoint=localhost:0";

    /// **What the source's replay arrives under.** The session settles
    /// `replay_session_id` from the descriptor the source sent
    /// (`on_recording_descriptor`) and `EXTEND` writes it onto the channel —
    /// and onto the subscription made from it — because that is the session
    /// the copy is filed under.
    ///
    /// This is the claim the `deepmsg-tests` criteria used to read off the
    /// recording subscription, which they cannot see (`extend_channel_uri`'s
    /// note says why). Here it is a value, and it is checked as one.
    #[test]
    fn a_plain_extend_names_the_session_the_replay_arrives_under() {
        let channel = extend_channel_uri(PLAIN, -741727832, false, NULL_VALUE, NULL_VALUE, false)
            .expect("a channel");

        assert!(
            channel.contains("session-id=-741727832"),
            "the replay's session, not this session's own: {channel}"
        );
        assert!(
            channel.contains("rejoin=false"),
            "and an image that goes away is not fought for: {channel}"
        );
        assert!(
            channel.contains("endpoint=localhost:0"),
            "a plain extend keeps the address it was given: {channel}"
        );
        assert!(
            !channel.contains("tags=") && !channel.contains("control-mode=manual"),
            "and it is not the multi-destination shape: {channel}"
        );
    }

    /// **A response channel names no session**, and that is the reference's own
    /// exception: `ReplicationSession.java:532-536` asks
    /// `hasControlModeResponse()` first, because the response channel is the
    /// one the answers travel on and it already belongs to a session.
    #[test]
    fn a_response_channel_does_not_name_one() {
        let channel = extend_channel_uri(
            "aeron:udp?endpoint=localhost:0|control-mode=response",
            -741727832,
            false,
            NULL_VALUE,
            NULL_VALUE,
            false,
        )
        .expect("a channel");

        assert!(channel.contains("rejoin=false"), "still: {channel}");
        assert!(
            !channel.contains("session-id="),
            "and no session id, which is the one channel that may not carry \
             one: {channel}"
        );
    }

    /// **The multi-destination rewrite**: the address comes off and the tags
    /// and the manual control mode go on, because the copy has to be able to
    /// follow a stream that comes and goes (`isMds`, `:527`).
    #[test]
    fn a_tagged_extend_names_its_tags_and_reads_by_hand() {
        let channel = extend_channel_uri(PLAIN, 42, true, 7, 9, false).expect("a channel");

        assert!(
            channel.contains("tags=7,9"),
            "the request's two ids, channel tag first (`:539`): {channel}"
        );
        assert!(
            channel.contains("control-mode=manual"),
            "so the client decides where it reads from: {channel}"
        );
        assert!(
            channel.contains("session-id=42"),
            "and it is still a session the replay arrives under: {channel}"
        );
        assert!(
            !channel.contains("endpoint="),
            "with the endpoint taken off — it is what `replay_destination` \
             puts back, on the destination: {channel}"
        );
    }

    /// **A live destination with no tag names none**, which is this build's
    /// one departure from the reference's channel text and the reason
    /// `docs/compat.md` has a row about it: the reference writes
    /// `tags=-1,-1` there, and the C driver refuses that channel outright
    /// (`aeron_udp_channel.c:385-390`).
    #[test]
    fn a_live_replication_without_tags_names_none() {
        let channel =
            extend_channel_uri(PLAIN, 42, false, NULL_VALUE, NULL_VALUE, true).expect("a channel");

        assert!(
            channel.contains("control-mode=manual"),
            "a live destination still needs the manual shape to be addable: {channel}"
        );
        assert!(
            !channel.contains("tags="),
            "and it says \"no tags\" by leaving the parameter out, which is \
             what the reference's own client library does \
             (`ChannelUriStringBuilder.java:1250-1264`): {channel}"
        );
        assert!(!channel.contains("endpoint="), "{channel}");
    }

    /// A channel that is not a channel is refused where it is built, with the
    /// text that says which one it was.
    #[test]
    fn a_channel_that_is_not_one_is_refused() {
        let refused = extend_channel_uri("not a channel", 42, false, NULL_VALUE, NULL_VALUE, false)
            .expect_err("not a channel");

        assert!(refused.contains("not a channel"), "{refused}");
    }
}
