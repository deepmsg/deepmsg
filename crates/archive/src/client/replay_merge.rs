//! Merging a replay into a live stream: the reference's `ReplayMerge`.
//!
//! A client that wants "everything from the recording, then whatever else the
//! publisher writes" cannot subscribe to the live stream and replay at the same
//! time — the two would arrive in an order nobody controls. The merge instead
//! **replays onto the same subscription the live stream will use**, and moves
//! the subscription from one destination to the other at a position both can
//! agree on:
//!
//! 1. the archive is asked where the recording has got to;
//! 2. a replay is started onto that subscription's destination, up to that
//!    position;
//! 3. when the replay has caught up, the **live** destination is added to the
//!    same subscription, so the live stream joins at the position the replay
//!    reached;
//! 4. once the replay is behind the live stream and the live stream is carrying
//!    more than one transport, the replay destination is removed and the replay
//!    stopped — from there the subscription is live only.
//!
//! The reference is `aeron-archive/src/main/c/client/aeron_archive_replay_merge.c`
//! (845 lines) and its Java twin, `ReplayMerge.java` (643). Both carry the same
//! eight states in the same order, and the C is the authority for this build.
//!
//! # Two constraints, both the reference's own
//!
//! * **UDP only.** Step 3 and 4 are destinations on a subscription, and IPC has
//!   no such thing (`ReplayMerge.java:44-46`).
//! * **Not thread safe, and not to be used with a shared `AeronArchive`.** It
//!   polls the control-response poller itself, so an archive client shared with
//!   anything else would have its answers taken (`:47-49`).
//!
//! # What this build had to change
//!
//! Two things the reference has and this build does not, both of them about an
//! image being a **handle** there and a **snapshot** here:
//!
//! * The reference holds an `Image` handle and reads its position every turn.
//!   A handle here would borrow the subscription for the life of the merge, so
//!   the merge takes an [`ImageFacts`] snapshot per turn through
//!   [`MergePeer::image`] instead — the same reads, no borrow.
//! * The reference asks that handle `isClosed` (`:457`) and means a closed
//!   image the client still holds, whose last position can still be read. This
//!   client removes an image when the driver says it is unavailable and holds
//!   nothing closed, so the image **going** is that same event: [`MergePeer::image`]
//!   answers `None`, and after the replay has been found once that is the
//!   closed image. A merge here fails a turn earlier than the reference's would
//!   in the corner where an image closes having already reached the target —
//!   there are no facts left to decide on.

use deepmsg_client::client::{
    AsyncDestination, AsyncDestinationPoll, Client, CommandError, DEFAULT_TIMEOUT,
};
use deepmsg_client::image::{Fragment, Image};
use deepmsg_client::publication::ExclusivePublication;
use deepmsg_client::subscription::Subscription;
use deepmsg_core::uri::{
    ChannelUri, ENDPOINT_PARAM_NAME, EOS_PARAM_NAME, LINGER_PARAM_NAME, UriError,
};

use crate::client::Archive;
use crate::client::archive::ArchiveError;
use crate::client::proxy::ReplayParams;

/// `AeronArchive.NULL_VALUE`, which is `Aeron.NULL_VALUE`.
pub const NULL_VALUE: i64 = -1;

/// The position the archive answers with for a recording that has no position
/// to give (`AeronArchive.NULL_POSITION`).
pub const NULL_POSITION: i64 = -1;

/// The maximum window at which a live destination is added (`ReplayMerge.java:54`).
pub const LIVE_ADD_MAX_WINDOW: i64 = 32 * 1024 * 1024;

/// The gap at which the replay destination is removed (`:56`).
const REPLAY_REMOVE_THRESHOLD: i64 = 0;

/// How long the merge may make no progress before it fails (`:57`).
pub const MERGE_PROGRESS_TIMEOUT_DEFAULT_MS: i64 = 5_000;

/// The first backoff between `getMaxRecordedPosition` attempts (`:58`).
const INITIAL_GET_MAX_RECORDED_POSITION_BACKOFF_MS: i64 = 8;

/// The backoff's ceiling (`:59`).
const GET_MAX_RECORDED_POSITION_BACKOFF_MAX_MS: i64 = 500;

/// How often the archive's control channel is drained when nothing is being
/// waited on (`:60`).
const ARCHIVE_POLL_INTERVAL_MS: i64 = 100;

/// The merge's eight states, in the reference's order and with its names
/// (`aeron_archive_replay_merge.c:34-45`, `ReplayMerge.java:63-73`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Waiting for the subscription to say which port it was given.
    ResolveReplayPort,
    /// Asking the archive where the recording has got to.
    GetRecordingPosition,
    /// Waiting for the replay to start.
    Replay,
    /// Waiting for the replay to reach the position the archive named.
    Catchup,
    /// Asking again where the recording is, to decide whether to join live.
    AttemptLiveJoin,
    /// Live only: the replay destination is gone and the replay stopped.
    Merged,
    /// The merge gave up; [`ReplayMerge::failure_reason`] says why.
    Failed,
    /// Closed by the caller.
    Closed,
}

impl State {
    /// The state's name, for the failure a merge reports.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::ResolveReplayPort => "RESOLVE_REPLAY_PORT",
            Self::GetRecordingPosition => "GET_RECORDING_POSITION",
            Self::Replay => "REPLAY",
            Self::Catchup => "CATCHUP",
            Self::AttemptLiveJoin => "ATTEMPT_LIVE_JOIN",
            Self::Merged => "MERGED",
            Self::Failed => "FAILED",
            Self::Closed => "CLOSED",
        }
    }
}

/// What a merge needs to know about the image carrying its replay, read fresh
/// every turn.
///
/// A snapshot and not a handle, because a handle would borrow the subscription
/// for the life of the merge — see the module note. The one thing a handle
/// carries that a snapshot cannot is the reference's `Image.isClosed`
/// (`:457`), a closed image the client still holds and whose last position can
/// still be read; there is no such image here, and the absence is said where it
/// arrives instead: [`MergePeer::image`] answers `None`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageFacts {
    /// How far the image has been read (`Image.position`).
    pub position: i64,
    /// The image's term buffer length (`Image.termBufferLength`), which bounds
    /// how far ahead of the replay the live destination may be added.
    pub term_buffer_length: i32,
    /// How many transports the image has (`Image.activeTransportCount`): a
    /// replay with one transport is the archive's and one with two is the
    /// archive's plus the live stream's.
    pub active_transport_count: i32,
}

/// Why a merge failed, which is the reference's two exceptions
/// (`ReplayMerge.java:580-583`, `:598-608`) and the closed image of `:457`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeFailure {
    /// No progress within the merge's timeout (`TimeoutException`).
    NoProgress {
        /// The state it was in.
        state: State,
        /// The transports the image had, when there was one to ask.
        active_transport_count: i32,
    },
    /// The replay's image closed while the merge was catching up (`:457`).
    ImageClosed,
    /// The archive answered `ERROR` (`ArchiveException`, `:602-606`).
    Refused {
        /// The archive's words.
        message: String,
    },
    /// The driver would not give the subscription a destination
    /// (`Subscription.asyncAddDestination` refusing, `:490`, `:501`).
    ///
    /// Its own variant and not [`Self::Refused`]: these words come from the
    /// driver on the **command** channel, where `Refused`'s come from the
    /// archive on the control channel, and a merge that is going to be reported
    /// to somebody should say which of the two fell over.
    DestinationRefused {
        /// The driver's words.
        message: String,
    },
    /// The archive's control channel is not connected (`:611-613`).
    ArchiveNotConnected,
}

impl std::fmt::Display for MergeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoProgress {
                state,
                active_transport_count,
            } => write!(
                f,
                "ReplayMerge no progress: state={}, activeTransportCount={active_transport_count}",
                state.name()
            ),
            Self::ImageClosed => write!(f, "ReplayMerge Image closed unexpectedly."),
            Self::Refused { message } => write!(f, "{message}"),
            Self::DestinationRefused { message } => {
                write!(f, "ReplayMerge destination refused: {message}")
            }
            Self::ArchiveNotConnected => write!(f, "archive is not connected"),
        }
    }
}

impl std::error::Error for MergeFailure {}

/// Everything a merge asks of the world: the reference's `AeronArchive`, its
/// `Subscription` and the `Aeron` client behind both.
///
/// One face rather than three because our client's methods take `&mut Client`
/// and the three would fight over it; the three the reference talks to are all
/// behind the adapter this is implemented for.
pub trait MergePeer {
    /// A correlation id for the next request (`Aeron.nextCorrelationId`).
    fn next_correlation_id(&mut self) -> i64;

    /// The stream the subscription reads (`Subscription.streamId`), which is
    /// also the stream the replay is asked for.
    fn subscription_stream_id(&self) -> i32;

    /// The endpoint the subscription resolved (`Subscription.resolvedEndpoint`),
    /// or `None` while it has not resolved one yet.
    fn resolved_endpoint(&mut self) -> Option<String>;

    /// Whether the subscription is connected (`Subscription.isConnected`).
    fn subscription_connected(&self) -> bool;

    /// One turn of the client behind this peer
    /// ([`Client::poll`](deepmsg_client::client::Client::poll)).
    ///
    /// The reference has nothing to ask for here: its `Aeron` runs a conductor
    /// thread of its own, so an image that arrives is announced whether or not
    /// the merge is looking. This build's client is driven by whoever holds it —
    /// [`ReplayMerge::poll`] is the merge's own turn and drives it there — and a
    /// merge that never did would wait forever for an image the driver had
    /// already sent, which is exactly what the first end-to-end run did.
    fn drive_client(&mut self);

    /// The replay's image, if the subscription has one for that session.
    ///
    /// **`None` is two things and the merge tells them apart by having found
    /// one already** (`image_found`): before the replay's image has appeared it
    /// is "not yet", and after it has, `None` is the reference's closed image.
    /// The reference holds an `Image` handle and asks `isClosed` on it
    /// (`:457`); this build's client removes an image when the driver says it
    /// is unavailable and holds nothing closed, so an image that cannot be
    /// asked about any more is exactly the one the reference would call closed.
    fn image(&mut self, session_id: i32) -> Option<ImageFacts>;

    /// `AeronArchive.getMaxRecordedPosition`, answering whether the request
    /// **went out** — the reference's `bool` (`:531-534`), which is false when
    /// the publication is back-pressured and the caller should try again.
    ///
    /// # Errors
    ///
    /// [`MergeFailure`] if the request could not be sent.
    fn get_max_recorded_position(
        &mut self,
        recording_id: i64,
        correlation_id: i64,
    ) -> Result<bool, MergeFailure>;

    /// `AeronArchive.replay` (`ReplayMerge.java:393-400`, with the length the
    /// merge uses).
    ///
    /// # Errors
    ///
    /// [`MergeFailure`] if the request could not be sent.
    fn replay(
        &mut self,
        recording_id: i64,
        start_position: i64,
        length: i64,
        channel: &str,
        stream_id: i32,
        correlation_id: i64,
    ) -> Result<bool, MergeFailure>;

    /// `AeronArchive.stopReplay`.
    ///
    /// # Errors
    ///
    /// [`MergeFailure`] if the request could not be sent.
    fn stop_replay(
        &mut self,
        replay_session_id: i64,
        correlation_id: i64,
    ) -> Result<bool, MergeFailure>;

    /// Poll the control channel once and answer the relevant id of the response
    /// to `correlation_id`, `None` while none has arrived
    /// (`ReplayMerge.pollForResponse`, `:593-619`). `NULL_VALUE` asks for
    /// whichever response is there.
    ///
    /// # Errors
    ///
    /// [`MergeFailure::Refused`] for an `ERROR` answer, and
    /// [`MergeFailure::ArchiveNotConnected`] for a control channel that is not
    /// connected.
    fn poll_for_response(&mut self, correlation_id: i64) -> Result<Option<i64>, MergeFailure>;

    /// Start putting a destination on the subscription, answering with the
    /// handle the driver will answer under
    /// (`Subscription.asyncAddDestination`, `:490`, `:501`, `:666-674`).
    ///
    /// **Started and not waited on**: the merge is a turn, and the driver's
    /// answer is read in a later one ([`MergePeer::destination_answered`]).
    /// The reference's merge is the reason the asynchronous form exists at all
    /// — its `async_destination` handle is polled at the top of every turn
    /// (`:717-731`) and holds the live join off while it is outstanding
    /// (`:635-643`).
    ///
    /// # Errors
    ///
    /// [`MergeFailure`] if the command could not be sent.
    fn start_add_destination(&mut self, destination: &str) -> Result<i64, MergeFailure>;

    /// Start taking one off (`Subscription.asyncRemoveDestination`, `:508`).
    ///
    /// # Errors
    ///
    /// [`MergeFailure`] if the command could not be sent.
    fn start_remove_destination(&mut self, destination: &str) -> Result<i64, MergeFailure>;

    /// Whether the driver has answered a destination command, polling what is
    /// there (`aeron_archive_replay_merge_handle_async_destination`,
    /// `:717-731`).
    ///
    /// `false` is "not yet", and the merge goes on waiting for it — which for
    /// the live destination means it does not ask the archive anything until
    /// the subscription is actually reading the live stream.
    ///
    /// # Errors
    ///
    /// [`MergeFailure`] if the driver refused the command.
    fn destination_answered(&mut self, correlation_id: i64) -> Result<bool, MergeFailure>;

    /// Whether the `Aeron` client behind this is closed (`Aeron.isClosed`),
    /// which `close` asks before it touches anything (`:206`).
    fn client_is_closed(&self) -> bool;

    /// Whether the archive's control publication is still connected
    /// (`archive.archiveProxy().publication().isConnected()`), which `close`
    /// asks before stopping a replay (`:215`).
    fn archive_publication_connected(&self) -> bool;

    /// Poll the fragments of the replay's image
    /// (`Image.poll(fragmentHandler, fragmentLimit)`).
    fn poll_image<F: FnMut(&Fragment<'_>)>(
        &mut self,
        session_id: i32,
        fragment_limit: usize,
        handler: F,
    ) -> usize;
}

/// The three objects the reference's merge holds, behind the one face
/// [`MergePeer`] is.
///
/// The reference's `ReplayMerge` is handed an `AeronArchive` and reaches the
/// subscription and the client through it (`ReplayMerge.java:112-133`). Here the
/// client and the archive client are two borrows and the subscription is an id,
/// because **our client's methods take the client** — `Subscription` has no
/// connection to the driver of its own — so a peer that held a `&mut
/// Subscription` could not also poll the archive.
///
/// # Two things that are the reference's shape and are worth naming
///
/// * **Destinations are asynchronous.** `Subscription.asyncAddDestination`
///   answers at once and is polled in a later turn, and that is what
///   [`MergePeer::start_add_destination`] and
///   [`MergePeer::destination_answered`] are: the merge starts a command and
///   carries its correlation id across turns. A blocking add would stop a
///   merge's turn for as long as the driver took.
/// * **A command the driver refuses is a merge that fails**, with the driver's
///   words (`:490-501`).
///
/// # One contract difference that decides how this is used
///
/// [`MergePeer::resolved_endpoint`] is
/// [`Subscription::resolved_endpoint`], which reads the address out of the
/// **counters** — and a subscription that named no endpoint at all, which is
/// what `control-mode=manual` on its own is, has no address there to read and
/// answers `None`. A replay destination with a **wildcard** port
/// (`aeron:udp?endpoint=localhost:0`, the reference's own test's shape) is
/// exactly the case that needs it, because the port only the subscription can
/// fill in is the one that has to be resolved.
///
/// So the wildcard path is not reachable in this build, and the reference's
/// other one is: a replay destination that named a port is a port the caller
/// meant, the merge starts at [`State::GetRecordingPosition`], and no resolution
/// is needed. That is the shape the end-to-end test takes, and the difference
/// belongs in documentation rather than in a silently different merge.
pub struct ClientMergePeer<'a> {
    client: &'a mut Client,
    archive: &'a mut Archive,
    /// The subscription this merge was built for. Held as an id rather than as
    /// a reference, because every read of it also needs the client.
    subscription_registration_id: i64,
    /// The stream the subscription reads, which is also the one the replay is
    /// asked for. Read once, at construction: a subscription's stream is what
    /// it was asked for and does not change.
    subscription_stream_id: i32,
}

impl<'a> ClientMergePeer<'a> {
    /// A peer over a client, an archive client, and one of the client's
    /// subscriptions.
    ///
    /// # Errors
    ///
    /// [`MergeFailure::DestinationRefused`] if the client does not hold that
    /// subscription, which is a merge that could not do anything at all.
    pub fn new(
        client: &'a mut Client,
        archive: &'a mut Archive,
        subscription_registration_id: i64,
    ) -> Result<Self, MergeFailure> {
        let subscription_stream_id = client
            .subscription(subscription_registration_id)
            .map(Subscription::stream_id)
            .ok_or_else(|| MergeFailure::DestinationRefused {
                message: format!(
                    "this client holds no subscription {subscription_registration_id}"
                ),
            })?;

        Ok(Self {
            client,
            archive,
            subscription_registration_id,
            subscription_stream_id,
        })
    }

    /// The subscription this merge is over, if the client still holds it.
    fn subscription(&self) -> Option<&Subscription> {
        self.client.subscription(self.subscription_registration_id)
    }

    /// The image carrying the replay, if the subscription has one for that
    /// session.
    ///
    /// Not `image`, which is what [`MergePeer::image`] is called and which the
    /// trait's own `&mut self` receiver would win over an inherent `&self` one.
    fn image_by_session(&self, session_id: i32) -> Option<&Image> {
        self.subscription()?.image_by_session_id(session_id)
    }
}

/// The driver's words for a destination it would not add or remove.
///
/// Spelled out rather than inlined because it is the only place this crate
/// turns a command error into a merge failure, and the two halves of it — which
/// variant, and whose words go in it — are what the criterion for
/// [`MergeFailure::DestinationRefused`] is about.
fn refused_destination(error: CommandError) -> MergeFailure {
    MergeFailure::DestinationRefused {
        message: error.to_string(),
    }
}

impl MergePeer for ClientMergePeer<'_> {
    fn drive_client(&mut self) {
        self.client.poll();
    }

    fn next_correlation_id(&mut self) -> i64 {
        // The reference's cannot fail (`aeron_archive_next_correlation_id`), and
        // this one can: a command ring that cannot be read is a client that is
        // on its way out. `NULL_VALUE` is the merge's "no correlation id", so
        // the request that follows is the one that fails, with the client's own
        // words, rather than a merge that fails here with a number.
        self.archive
            .next_correlation_id(self.client)
            .unwrap_or(NULL_VALUE)
    }

    fn subscription_stream_id(&self) -> i32 {
        self.subscription_stream_id
    }

    fn resolved_endpoint(&mut self) -> Option<String> {
        let counters = self.client.counters_reader()?;

        self.subscription()?.resolved_endpoint(&counters)
    }

    fn subscription_connected(&self) -> bool {
        self.subscription().is_some_and(Subscription::is_connected)
    }

    fn image(&mut self, session_id: i32) -> Option<ImageFacts> {
        let image = self.image_by_session(session_id)?;

        Some(ImageFacts {
            position: image.position(),
            term_buffer_length: image.term_buffer_length(),
            // `None` is an image whose transport count cannot be read — a
            // counter that has gone, which is the client closing it. Zero is
            // the answer that keeps a merge from treating it as two, which is
            // the direction a wrong answer must fall.
            active_transport_count: image.active_transport_count().unwrap_or(0),
        })
    }

    fn get_max_recorded_position(
        &mut self,
        recording_id: i64,
        correlation_id: i64,
    ) -> Result<bool, MergeFailure> {
        // The proxy retries an offer itself and answers `Err` when it never
        // landed — which the reference spells `false`, and whose consequence is
        // the backoff rather than a failed merge (`:761-765`).
        Ok(self
            .archive
            .proxy_mut()
            .get_max_recorded_position(self.client, correlation_id, recording_id)
            .is_ok())
    }

    fn replay(
        &mut self,
        recording_id: i64,
        start_position: i64,
        length: i64,
        channel: &str,
        stream_id: i32,
        correlation_id: i64,
    ) -> Result<bool, MergeFailure> {
        let params = ReplayParams {
            position: start_position,
            length,
            ..ReplayParams::default()
        };

        Ok(self
            .archive
            .proxy_mut()
            .replay(
                self.client,
                correlation_id,
                recording_id,
                channel,
                stream_id,
                &params,
            )
            .is_ok())
    }

    fn stop_replay(
        &mut self,
        replay_session_id: i64,
        correlation_id: i64,
    ) -> Result<bool, MergeFailure> {
        Ok(self
            .archive
            .proxy_mut()
            .stop_replay(self.client, correlation_id, replay_session_id)
            .is_ok())
    }

    fn poll_for_response(&mut self, correlation_id: i64) -> Result<Option<i64>, MergeFailure> {
        Ok(self
            .archive
            .poll_for_response(self.client, correlation_id)?)
    }

    fn start_add_destination(&mut self, destination: &str) -> Result<i64, MergeFailure> {
        self.client
            .async_add_rcv_destination(
                self.subscription_registration_id,
                destination,
                DEFAULT_TIMEOUT,
            )
            .map(|destination| destination.correlation_id())
            .map_err(refused_destination)
    }

    fn start_remove_destination(&mut self, destination: &str) -> Result<i64, MergeFailure> {
        self.client
            .async_remove_rcv_destination(
                self.subscription_registration_id,
                destination,
                DEFAULT_TIMEOUT,
            )
            .map(|destination| destination.correlation_id())
            .map_err(refused_destination)
    }

    fn destination_answered(&mut self, correlation_id: i64) -> Result<bool, MergeFailure> {
        match self
            .client
            .destination_poll(AsyncDestination::from_correlation_id(correlation_id))
        {
            AsyncDestinationPoll::Awaiting => Ok(false),
            AsyncDestinationPoll::Ready => Ok(true),
            AsyncDestinationPoll::Failed(error) => Err(refused_destination(error)),
            // A handle this client is not waiting on: the entry has already
            // been retired — by a deadline, or by this merge polling twice —
            // and there is nothing left to wait for.
            AsyncDestinationPoll::Unknown => Ok(true),
        }
    }

    fn client_is_closed(&self) -> bool {
        self.client.is_closed()
    }

    fn archive_publication_connected(&self) -> bool {
        // `close`'s own gate, and the same expression `Archive::close` uses
        // (`:215-220`): a session whose request publication is not connected
        // cannot be told anything.
        self.client
            .exclusive_publication(self.archive.proxy().request_publication())
            .and_then(ExclusivePublication::is_connected)
            .unwrap_or(false)
    }

    fn poll_image<F: FnMut(&Fragment<'_>)>(
        &mut self,
        session_id: i32,
        fragment_limit: usize,
        handler: F,
    ) -> usize {
        // The image's **publication** registration id, which is what the
        // client's own poll takes; the merge names it by session.
        let Some(registration_id) = self
            .image_by_session(session_id)
            .map(Image::registration_id)
        else {
            return 0;
        };

        self.client
            .poll_image(
                self.subscription_registration_id,
                registration_id,
                fragment_limit,
                handler,
            )
            .unwrap_or(0)
    }
}

/// The state machine.
///
/// Held by the caller for the life of the merge; every turn is
/// [`ReplayMerge::do_work`], and [`ReplayMerge::poll`] is that plus a poll of
/// the image it is carrying.
#[derive(Debug)]
pub struct ReplayMerge {
    recording_id: i64,
    start_position: i64,
    merge_progress_timeout_ms: i64,
    replay_channel: ChannelUri,
    replay_destination: String,
    live_destination: String,
    replay_session_id: i64,
    active_correlation_id: i64,
    next_target_position: i64,
    position_of_last_progress: i64,
    time_of_last_progress_ms: i64,
    time_of_next_get_max_recorded_position_ms: i64,
    get_max_recorded_position_backoff_ms: i64,
    time_of_last_scheduled_archive_poll_ms: i64,
    is_live_added: bool,
    is_replay_active: bool,
    image_found: bool,
    /// Whether the replay destination has gone onto the subscription yet.
    ///
    /// The reference does this in `init` (`:490`), which has the subscription to
    /// hand; here the first turn does it, so that no caller can construct a
    /// merge and forget to start it. See [`ReplayMerge::do_work`].
    replay_destination_added: bool,
    /// The destination command the driver has not answered yet, or
    /// [`NULL_VALUE`] — the reference's `async_destination` handle (`:717-731`).
    ///
    /// Only one at a time, which is the reference's own rule: a merge that
    /// started a second before the first was answered would not know which
    /// arrival it was looking at.
    destination_correlation_id: i64,
    state: State,
    failure: Option<MergeFailure>,
}

impl ReplayMerge {
    /// A merge of `recording_id` from `start_position`, onto `replay_channel`.
    ///
    /// `replay_destination` and `live_destination` are what the subscription is
    /// given and then has taken away: the first is where the archive's replay
    /// arrives, the second where the live stream does
    /// (`ReplayMerge.java:112-120`).
    ///
    /// The replay channel is a **template**, and this is where it is prepared
    /// (`ReplayMerge.java:139-152`): `linger=0` and `eos=false` go onto it, and
    /// then the replay destination's endpoint decides the state the merge starts
    /// in. A destination with a port of its own is a port the caller meant, so
    /// it goes straight onto the replay channel and the merge starts at
    /// [`State::GetRecordingPosition`]; a **wildcard** port (`:0`) is one only
    /// the subscription can fill in, so the merge waits for it in
    /// [`State::ResolveReplayPort`] (`:144-152`). A destination naming no
    /// endpoint at all resolves too, which is an extension: the reference reads
    /// the parameter without checking and has no answer for its absence.
    ///
    /// # Errors
    ///
    /// [`UriError`] if `replay_channel` or `replay_destination` is not a channel
    /// URI.
    pub fn new(
        replay_channel: &str,
        replay_destination: &str,
        live_destination: &str,
        recording_id: i64,
        start_position: i64,
        merge_progress_timeout_ms: i64,
    ) -> Result<Self, UriError> {
        let mut replay_channel = ChannelUri::parse(replay_channel)?;

        // `AERON_URI_LINGER_TIMEOUT_KEY` / `AERON_URI_EOS_KEY`
        // (`aeron-client/src/main/c/uri/aeron_uri.h:59`, `:62`). Linger zero
        // because the subscription is about to be handed a stream that stops on
        // purpose, and an end-of-stream is not the replay ending.
        replay_channel.put(LINGER_PARAM_NAME, "0");
        replay_channel.put(EOS_PARAM_NAME, "false");

        let replay_destination_uri = ChannelUri::parse(replay_destination)?;
        let state = match replay_destination_uri.get(ENDPOINT_PARAM_NAME) {
            Some(endpoint) if !endpoint.ends_with(":0") => {
                replay_channel.put(ENDPOINT_PARAM_NAME, endpoint);

                State::GetRecordingPosition
            }
            _ => State::ResolveReplayPort,
        };

        Ok(Self {
            recording_id,
            start_position,
            merge_progress_timeout_ms,
            replay_channel,
            replay_destination: replay_destination.to_owned(),
            live_destination: live_destination.to_owned(),
            replay_session_id: NULL_VALUE,
            active_correlation_id: NULL_VALUE,
            next_target_position: NULL_VALUE,
            position_of_last_progress: NULL_VALUE,
            time_of_last_progress_ms: 0,
            time_of_next_get_max_recorded_position_ms: 0,
            get_max_recorded_position_backoff_ms: INITIAL_GET_MAX_RECORDED_POSITION_BACKOFF_MS,
            time_of_last_scheduled_archive_poll_ms: 0,
            is_live_added: false,
            is_replay_active: false,
            image_found: false,
            replay_destination_added: false,
            destination_correlation_id: NULL_VALUE,
            state,
            failure: None,
        })
    }

    /// One turn (`ReplayMerge.doWork`, `:240-294`).
    ///
    /// Returns the reference's work count. A turn that fails leaves the merge in
    /// [`State::Failed`] — the C's `-1` and `FAILED` (`aeron_archive_replay_merge.c:301`),
    /// which is what the Java's thrown exception becomes here — and
    /// [`ReplayMerge::failure_reason`] carries what the Java would have thrown.
    pub fn do_work<P: MergePeer>(&mut self, peer: &mut P, now_ms: i64) -> usize {
        if matches!(self.state, State::Merged | State::Closed | State::Failed) {
            return 0;
        }

        // The reference puts the replay destination on the subscription in
        // `init`, which is handed the subscription and the client (`:490-501`);
        // this type is built without either, so the first turn does it. It is
        // the same turn order — nothing has been asked of the archive yet — and
        // it cannot be left out by a caller that forgets to start the merge.
        if !self.replay_destination_added {
            let destination = self.replay_destination.clone();

            match peer.start_add_destination(&destination) {
                Ok(correlation_id) => {
                    self.replay_destination_added = true;
                    self.destination_correlation_id = correlation_id;
                }
                Err(failure) => {
                    self.fail(failure);

                    return 0;
                }
            }
        }

        // The driver's answer to a destination, read before the state runs — so
        // a state that is held off by an outstanding one is held off by a *live*
        // question rather than by one this turn could have settled.
        if self.destination_correlation_id != NULL_VALUE {
            match peer.destination_answered(self.destination_correlation_id) {
                Ok(true) => self.destination_correlation_id = NULL_VALUE,
                Ok(false) => {}
                Err(failure) => {
                    self.fail(failure);

                    return 0;
                }
            }
        }

        let outcome = match self.state {
            State::ResolveReplayPort => self.resolve_replay_port(peer, now_ms),
            State::GetRecordingPosition => self.get_recording_position(peer, now_ms),
            State::Replay => self.replay(peer, now_ms),
            State::Catchup => self.catchup(peer, now_ms),
            State::AttemptLiveJoin => self.attempt_live_join(peer, now_ms),
            State::Merged | State::Closed | State::Failed => Ok(0),
        };

        match outcome {
            Ok(work) => match self.check_progress(peer, now_ms) {
                Ok(()) => work,
                Err(failure) => {
                    self.fail(failure);

                    0
                }
            },
            Err(failure) => {
                self.fail(failure);

                0
            }
        }
    }

    /// Poll the image the merge is carrying (`:297-301`), which is the merge's
    /// turn followed by the image's own poll — in that order, because the turn
    /// is what decides whether the image is still the right one.
    ///
    /// It is also where the **client** is driven, once a turn
    /// ([`MergePeer::drive_client`]). That is not the reference's shape — its
    /// `Aeron` has a conductor thread and this build's client has its caller —
    /// and it is here rather than in [`ReplayMerge::do_work`] so that a merge
    /// which is over still takes its client's turns: the subscription is live
    /// from [`State::Merged`] onward and its messages are read through this same
    /// call.
    pub fn poll<P, F>(
        &mut self,
        peer: &mut P,
        now_ms: i64,
        fragment_limit: usize,
        handler: F,
    ) -> usize
    where
        P: MergePeer,
        F: FnMut(&Fragment<'_>),
    {
        peer.drive_client();
        self.do_work(peer, now_ms);

        if !self.image_found {
            return 0;
        }

        peer.poll_image(self.replay_session(), fragment_limit, handler)
    }

    /// Whether the replay has been left behind and the subscription is live
    /// only (`ReplayMerge.isMerged`, `:308`).
    #[must_use]
    pub const fn is_merged(&self) -> bool {
        matches!(self.state, State::Merged)
    }

    /// Whether the merge gave up (`:318`).
    #[must_use]
    pub const fn has_failed(&self) -> bool {
        matches!(self.state, State::Failed)
    }

    /// What it gave up on, for a caller that wants to say so.
    #[must_use]
    pub const fn failure_reason(&self) -> Option<&MergeFailure> {
        self.failure.as_ref()
    }

    /// Whether the live destination has been added (`:338`).
    #[must_use]
    pub const fn is_live_added(&self) -> bool {
        self.is_live_added
    }

    /// The state, for a caller that is watching the merge rather than waiting
    /// for it.
    #[must_use]
    pub const fn state(&self) -> State {
        self.state
    }

    /// The replay's session id, once the archive has answered with one.
    #[must_use]
    pub const fn replay_session_id(&self) -> i64 {
        self.replay_session_id
    }

    /// Close the merge and stop any replay (`:202-223`).
    ///
    /// The **live destination is left in place** — the reference says why: so
    /// that the subscription can go on being consumed live.
    pub fn close<P: MergePeer>(&mut self, peer: &mut P) {
        if self.state == State::Closed {
            return;
        }

        if !peer.client_is_closed() {
            if self.state != State::Merged {
                let replay_destination = self.replay_destination.clone();

                // Started **and drained**: the reference will not go on with the
                // command outstanding (`:266-282`), because a removal nobody
                // reads leaves the client holding a pending entry and the
                // subscription still reading from a destination the caller
                // thinks is gone. A refusal ends the wait rather than hanging
                // on it, which is what the C's own `-1` does.
                if let Ok(correlation_id) = peer.start_remove_destination(&replay_destination) {
                    while !peer.destination_answered(correlation_id).unwrap_or(true) {
                        peer.drive_client();
                    }
                }
            }

            if self.is_replay_active && peer.archive_publication_connected() {
                self.stop_replay(peer);
            }
        }

        self.transition(State::Closed);
    }

    fn resolve_replay_port<P: MergePeer>(&mut self, peer: &mut P, now_ms: i64) -> Outcome {
        let Some(resolved_endpoint) = peer.resolved_endpoint() else {
            return Ok(0);
        };

        self.replay_channel
            .replace_endpoint_wildcard_port(&resolved_endpoint)
            .map_err(|error| MergeFailure::Refused {
                message: error.to_string(),
            })?;

        self.time_of_last_progress_ms = now_ms;
        self.transition(State::GetRecordingPosition);

        Ok(1)
    }

    fn get_recording_position<P: MergePeer>(&mut self, peer: &mut P, now_ms: i64) -> Outcome {
        if self.active_correlation_id == NULL_VALUE {
            if self.call_get_max_recorded_position(peer, now_ms)? {
                self.time_of_last_progress_ms = now_ms;

                return Ok(1);
            }

            return Ok(0);
        }

        let Some(relevant_id) = peer.poll_for_response(self.active_correlation_id)? else {
            return Ok(0);
        };

        self.next_target_position = relevant_id;
        self.active_correlation_id = NULL_VALUE;

        if relevant_id != NULL_POSITION {
            self.time_of_last_progress_ms = now_ms;
            self.transition(State::Replay);
        }

        Ok(1)
    }

    fn replay<P: MergePeer>(&mut self, peer: &mut P, now_ms: i64) -> Outcome {
        if self.active_correlation_id == NULL_VALUE {
            let correlation_id = peer.next_correlation_id();
            let channel = self.replay_channel.build();
            let stream_id = peer.subscription_stream_id();

            if peer.replay(
                self.recording_id,
                self.start_position,
                i64::MAX,
                &channel,
                stream_id,
                correlation_id,
            )? {
                self.active_correlation_id = correlation_id;
                self.time_of_last_progress_ms = now_ms;

                return Ok(1);
            }

            return Ok(0);
        }

        let Some(relevant_id) = peer.poll_for_response(self.active_correlation_id)? else {
            return Ok(0);
        };

        self.is_replay_active = true;
        self.replay_session_id = relevant_id;
        self.time_of_last_progress_ms = now_ms;
        self.active_correlation_id = NULL_VALUE;

        // `:409-411`: the backoff is reset on the way into `CATCHUP`, which is
        // the only place a merge starts making progress on its own.
        self.get_max_recorded_position_backoff_ms = INITIAL_GET_MAX_RECORDED_POSITION_BACKOFF_MS;
        self.time_of_next_get_max_recorded_position_ms = now_ms;

        self.transition(State::Catchup);

        Ok(1)
    }

    fn catchup<P: MergePeer>(&mut self, peer: &mut P, now_ms: i64) -> Outcome {
        if !self.image_found && peer.subscription_connected() {
            self.time_of_last_progress_ms = now_ms;

            match peer.image(self.replay_session()) {
                Some(facts) => {
                    self.image_found = true;
                    self.position_of_last_progress = facts.position;
                }
                None => self.position_of_last_progress = NULL_VALUE,
            }
        }

        if !self.image_found {
            return Ok(0);
        }

        let facts = self.image(peer)?;

        if facts.position >= self.next_target_position {
            self.time_of_last_progress_ms = now_ms;
            self.position_of_last_progress = facts.position;
            self.transition(State::AttemptLiveJoin);

            Ok(1)
        } else if facts.position > self.position_of_last_progress {
            self.time_of_last_progress_ms = now_ms;
            self.position_of_last_progress = facts.position;

            Ok(0)
        } else {
            Ok(0)
        }
    }

    fn attempt_live_join<P: MergePeer>(&mut self, peer: &mut P, now_ms: i64) -> Outcome {
        // `:633-639`: a destination the driver has not answered is one whose
        // answer this turn would be reading as its own — the reference bails
        // out of the state entirely rather than fall through to the decisions.
        if self.destination_correlation_id != NULL_VALUE {
            return Ok(0);
        }

        if self.active_correlation_id == NULL_VALUE {
            if self.call_get_max_recorded_position(peer, now_ms)? {
                self.time_of_last_progress_ms = now_ms;

                return Ok(1);
            }

            return Ok(0);
        }

        let Some(relevant_id) = peer.poll_for_response(self.active_correlation_id)? else {
            return Ok(0);
        };

        self.next_target_position = relevant_id;
        self.active_correlation_id = NULL_VALUE;

        if relevant_id != NULL_POSITION {
            let mut next = State::Catchup;

            if self.image_found {
                let facts = self.image(peer)?;

                if self.should_add_live_destination(facts) {
                    let live_destination = self.live_destination.clone();
                    self.destination_correlation_id =
                        peer.start_add_destination(&live_destination)?;
                    self.time_of_last_progress_ms = now_ms;
                    self.position_of_last_progress = facts.position;
                    self.is_live_added = true;
                } else if self.should_stop_and_remove_replay(facts) {
                    let replay_destination = self.replay_destination.clone();
                    self.destination_correlation_id =
                        peer.start_remove_destination(&replay_destination)?;
                    self.stop_replay(peer);
                    self.time_of_last_progress_ms = now_ms;
                    self.position_of_last_progress = facts.position;
                    next = State::Merged;
                }
            }

            self.transition(next);
        }

        Ok(1)
    }

    fn call_get_max_recorded_position<P: MergePeer>(
        &mut self,
        peer: &mut P,
        now_ms: i64,
    ) -> Result<bool, MergeFailure> {
        if now_ms < self.time_of_next_get_max_recorded_position_ms {
            return Ok(false);
        }

        let correlation_id = peer.next_correlation_id();
        let sent = peer.get_max_recorded_position(self.recording_id, correlation_id)?;

        if sent {
            self.active_correlation_id = correlation_id;
        }

        // `:536-540`: the backoff doubles whether the request went out or not,
        // and whether the answer has arrived or not. A merge that only backed
        // off on failure would ask once a turn forever.
        self.get_max_recorded_position_backoff_ms = self
            .get_max_recorded_position_backoff_ms
            .saturating_mul(2)
            .min(GET_MAX_RECORDED_POSITION_BACKOFF_MAX_MS);
        self.time_of_next_get_max_recorded_position_ms =
            now_ms + self.get_max_recorded_position_backoff_ms;

        Ok(sent)
    }

    fn stop_replay<P: MergePeer>(&mut self, peer: &mut P) {
        let correlation_id = peer.next_correlation_id();

        if peer
            .stop_replay(self.replay_session_id, correlation_id)
            .unwrap_or(false)
        {
            self.is_replay_active = false;
        }
    }

    fn check_progress<P: MergePeer>(
        &mut self,
        peer: &mut P,
        now_ms: i64,
    ) -> Result<(), MergeFailure> {
        if now_ms > self.time_of_last_progress_ms + self.merge_progress_timeout_ms {
            let active_transport_count = if self.image_found {
                self.image(peer)?.active_transport_count
            } else {
                0
            };

            return Err(MergeFailure::NoProgress {
                state: self.state,
                active_transport_count,
            });
        }

        if self.active_correlation_id == NULL_VALUE
            && now_ms > self.time_of_last_scheduled_archive_poll_ms + ARCHIVE_POLL_INTERVAL_MS
        {
            self.time_of_last_scheduled_archive_poll_ms = now_ms;
            let _ = peer.poll_for_response(NULL_VALUE)?;
        }

        Ok(())
    }

    /// `:563-567`: the live destination goes on when the recording is within a
    /// quarter of a term (or 32 MiB, whichever is smaller) of where the replay
    /// has got to — close enough that the join will not be a hole.
    fn should_add_live_destination(&self, facts: ImageFacts) -> bool {
        !self.is_live_added
            && (self.next_target_position - facts.position)
                <= i64::from(facts.term_buffer_length >> 2).min(LIVE_ADD_MAX_WINDOW)
    }

    /// `:569-574`: the replay comes off once the live stream has caught up with
    /// it **and** is carrying two transports — one live transport would mean the
    /// replay's destination was still the only one, and removing it there is a
    /// gap rather than a merge.
    fn should_stop_and_remove_replay(&self, facts: ImageFacts) -> bool {
        self.is_live_added
            && (self.next_target_position - facts.position) <= REPLAY_REMOVE_THRESHOLD
            && facts.active_transport_count >= 2
    }

    fn image<P: MergePeer>(&mut self, peer: &mut P) -> Result<ImageFacts, MergeFailure> {
        peer.image(self.replay_session())
            .ok_or(MergeFailure::ImageClosed)
    }

    /// The replay session as an **image** is named by, which is the low half of
    /// the id the archive answers with.
    ///
    /// The archive's `relevantId` for a replay is not a session id but a replay
    /// id over one: `(replayId << 32) | sessionId` (`ReplaySession.java:338`),
    /// and the reference reaches the image by **truncating** it —
    /// `subscription.imageBySessionId((int)replaySessionId)`
    /// (`ReplayMerge.java:436`), `(int32_t)replay_merge->replay_session_id` in
    /// the C (`:591`). A narrowing that refused an id it did not fit would
    /// answer with a session no image has and leave the merge waiting in
    /// `CATCHUP` for a replay that had arrived.
    fn replay_session(&self) -> i32 {
        self.replay_session_id as i32
    }

    /// `:556-561`: a state change drops whatever correlation the previous state
    /// was waiting on, so a response that arrives late is not read as this
    /// state's.
    fn transition(&mut self, state: State) {
        self.state = state;
        self.active_correlation_id = NULL_VALUE;
    }

    fn fail(&mut self, failure: MergeFailure) {
        self.failure = Some(failure);
        self.transition(State::Failed);
    }
}

/// One state's turn: how much work it did, or why the merge is over.
type Outcome = Result<usize, MergeFailure>;

/// The error type an [`ArchiveError`] becomes, for the adapter that has to
/// spell it out.
impl From<ArchiveError> for MergeFailure {
    fn from(error: ArchiveError) -> Self {
        Self::Refused {
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Everything the merge asks for, answered by hand: a subscription that has
    /// resolved an endpoint, an image whose facts the test moves, and an archive
    /// that answers `getMaxRecordedPosition` with whatever the test set.
    struct FakePeer {
        correlation_ids: i64,
        resolved_endpoint: Option<String>,
        subscription_connected: bool,
        image: Option<ImageFacts>,
        max_recorded_position: i64,
        /// Whether `getMaxRecordedPosition` goes out at all.
        sends: bool,
        /// Whether the archive answers anything.
        answers: bool,
        /// Answers waiting to be read, by the correlation id that will be
        /// waited on — what a real archive's control channel holds.
        pending: Vec<(i64, i64)>,
        asked: Vec<i64>,
        /// Which recording each `getMaxRecordedPosition` was about, so that
        /// "the id the merge was built with" is assertable.
        asked_about: Vec<i64>,
        /// Every destination command started, in order, as (added, uri).
        started: Vec<(bool, String)>,
        /// Whether the driver answers destination commands on the turn they are
        /// started. `false` is a driver that takes its time, which is what holds
        /// the live join off.
        answers_destinations: bool,
        /// Answers waiting to be read, by correlation id.
        destination_answers: Vec<i64>,
        /// The id of the last destination command started, so a test that held
        /// the answer back can give it later.
        last_destination_id: i64,
        /// The channel each `replay` request was sent on, which is the template
        /// after the merge has finished it.
        replayed: Vec<String>,
        /// The `relevantId` a replay is answered with, which for a real archive
        /// is a replay id over a session id.
        replay_relevant_id: i64,
        /// Which session each image was asked for.
        asked_sessions: Vec<i32>,
        /// How many turns of the client `poll` took.
        driven: usize,
        /// Set to make every destination command fail, the way a driver that
        /// will not take one answers.
        refuse_destinations: Option<String>,
        stopped: Vec<i64>,
        client_closed: bool,
        archive_connected: bool,
    }

    impl FakePeer {
        /// A destination command the merge started, answered on the next turn
        /// unless the peer is set to make the driver take its time.
        fn start_destination(
            &mut self,
            added: bool,
            destination: &str,
        ) -> Result<i64, MergeFailure> {
            if let Some(message) = &self.refuse_destinations {
                return Err(MergeFailure::DestinationRefused {
                    message: message.clone(),
                });
            }

            let correlation_id = self.next_correlation_id();
            self.started.push((added, destination.to_owned()));
            self.last_destination_id = correlation_id;

            if self.answers_destinations {
                self.destination_answers.push(correlation_id);
            }

            Ok(correlation_id)
        }

        /// The driver answers the destination it was last given, which is how a
        /// test that made it take its time releases it.
        fn answer_last_destination(&mut self) {
            self.destination_answers.push(self.last_destination_id);
        }

        fn new(max_recorded_position: i64) -> Self {
            Self {
                correlation_ids: 0,
                resolved_endpoint: Some("localhost:40123".to_owned()),
                subscription_connected: true,
                image: None,
                max_recorded_position,
                sends: true,
                answers: true,
                pending: Vec::new(),
                asked: Vec::new(),
                asked_about: Vec::new(),
                started: Vec::new(),
                answers_destinations: true,
                destination_answers: Vec::new(),
                last_destination_id: NULL_VALUE,
                replayed: Vec::new(),
                replay_relevant_id: 99,
                asked_sessions: Vec::new(),
                driven: 0,
                refuse_destinations: None,
                stopped: Vec::new(),
                client_closed: false,
                archive_connected: true,
            }
        }
    }

    impl MergePeer for FakePeer {
        fn next_correlation_id(&mut self) -> i64 {
            self.correlation_ids += 1;

            self.correlation_ids
        }

        fn subscription_stream_id(&self) -> i32 {
            1001
        }

        fn resolved_endpoint(&mut self) -> Option<String> {
            self.resolved_endpoint.clone()
        }

        fn subscription_connected(&self) -> bool {
            self.subscription_connected
        }

        fn drive_client(&mut self) {
            self.driven += 1;
        }

        fn image(&mut self, session_id: i32) -> Option<ImageFacts> {
            self.asked_sessions.push(session_id);

            self.image
        }

        fn get_max_recorded_position(
            &mut self,
            recording_id: i64,
            correlation_id: i64,
        ) -> Result<bool, MergeFailure> {
            self.asked.push(correlation_id);
            self.asked_about.push(recording_id);

            if self.sends && self.answers {
                self.pending
                    .push((correlation_id, self.max_recorded_position));
            }

            Ok(self.sends)
        }

        fn replay(
            &mut self,
            _recording_id: i64,
            _start_position: i64,
            _length: i64,
            channel: &str,
            _stream_id: i32,
            correlation_id: i64,
        ) -> Result<bool, MergeFailure> {
            self.replayed.push(channel.to_owned());

            if self.answers {
                self.pending.push((correlation_id, self.replay_relevant_id));
            }

            Ok(true)
        }

        fn stop_replay(
            &mut self,
            replay_session_id: i64,
            _correlation_id: i64,
        ) -> Result<bool, MergeFailure> {
            self.stopped.push(replay_session_id);

            Ok(true)
        }

        fn poll_for_response(&mut self, correlation_id: i64) -> Result<Option<i64>, MergeFailure> {
            let waiting = if correlation_id == NULL_VALUE {
                // The drain `checkProgress` does before anything is waited on
                // (`:588-592`): whatever is there, or nothing.
                (!self.pending.is_empty()).then_some(0)
            } else {
                self.pending
                    .iter()
                    .position(|(correlation, _)| *correlation == correlation_id)
            };

            match waiting {
                Some(index) => Ok(Some(self.pending.remove(index).1)),
                None => Ok(None),
            }
        }

        fn start_add_destination(&mut self, destination: &str) -> Result<i64, MergeFailure> {
            self.start_destination(true, destination)
        }

        fn start_remove_destination(&mut self, destination: &str) -> Result<i64, MergeFailure> {
            self.start_destination(false, destination)
        }

        fn destination_answered(&mut self, correlation_id: i64) -> Result<bool, MergeFailure> {
            let Some(index) = self
                .destination_answers
                .iter()
                .position(|answer| *answer == correlation_id)
            else {
                return Ok(false);
            };

            self.destination_answers.remove(index);

            Ok(true)
        }

        fn client_is_closed(&self) -> bool {
            self.client_closed
        }

        fn archive_publication_connected(&self) -> bool {
            self.archive_connected
        }

        fn poll_image<F: FnMut(&Fragment<'_>)>(
            &mut self,
            _session_id: i32,
            _fragment_limit: usize,
            _handler: F,
        ) -> usize {
            0
        }
    }

    /// A merge whose destinations are the shapes an archive's replay has: a
    /// replay **channel** that names no endpoint (the archive is told where to
    /// send), a replay **destination** with a wildcard port (the subscription
    /// fills in the one it resolved), and a live destination with a port of its
    /// own.
    fn merge(timeout_ms: i64) -> ReplayMerge {
        ReplayMerge::new(
            "aeron:udp",
            "aeron:udp?endpoint=localhost:0",
            "aeron:udp?endpoint=localhost:40223",
            7,
            0,
            timeout_ms,
        )
        .expect("a channel")
    }

    fn catchup_facts(position: i64, transports: i32) -> ImageFacts {
        ImageFacts {
            position,
            term_buffer_length: 64 * 1024,
            active_transport_count: transports,
        }
    }

    /// Turn until the merge reaches a named state. A turn that sends a request
    /// and a turn that reads its answer are two turns — that is the reference's
    /// shape, not a property of the fake.
    fn turn_until_state(
        merge: &mut ReplayMerge,
        peer: &mut FakePeer,
        now_ms: &mut i64,
        expected: State,
    ) {
        // 50 ms a turn, not 1: the merge's own backoff can hold it off for up
        // to 500 ms between attempts, and a budget in single milliseconds would
        // spend all sixteen turns inside one of those windows and call it stuck.
        for _ in 0..24 {
            merge.do_work(peer, *now_ms);
            *now_ms += 50;

            if merge.state() == expected {
                return;
            }
        }

        panic!(
            "the merge did not reach {}; it is in {}",
            expected.name(),
            merge.state().name()
        );
    }

    /// The whole walk: the states in order, the live destination added once the
    /// replay is within a quarter term, and the replay taken off once the live
    /// stream has caught up on two transports
    /// (`ReplayMerge.java:240-294`, `:472-521`).
    #[test]
    fn a_merge_walks_its_states_to_merged() {
        let mut merge = merge(MERGE_PROGRESS_TIMEOUT_DEFAULT_MS);
        let mut peer = FakePeer::new(1024);
        let mut now = 0;
        peer.image = Some(catchup_facts(0, 1));

        assert_eq!(State::ResolveReplayPort, merge.state());
        turn_until_state(&mut merge, &mut peer, &mut now, State::GetRecordingPosition);
        turn_until_state(&mut merge, &mut peer, &mut now, State::Replay);
        turn_until_state(&mut merge, &mut peer, &mut now, State::Catchup);
        assert_eq!(99, merge.replay_session_id());

        // The replay has not reached the target yet, so the merge waits.
        for _ in 0..8 {
            merge.do_work(&mut peer, now);
            now += 1;
        }
        assert_eq!(State::Catchup, merge.state());
        assert_eq!(
            vec![(true, "aeron:udp?endpoint=localhost:0".to_owned())],
            peer.started,
            "the replay destination and nothing else, before the target"
        );

        // It has, so the merge asks where the recording is now.
        peer.image = Some(catchup_facts(1024, 1));
        turn_until_state(&mut merge, &mut peer, &mut now, State::AttemptLiveJoin);

        // The live destination goes on — the target is within a quarter term —
        // and the merge goes back to catching up, now with two destinations.
        turn_until_state(&mut merge, &mut peer, &mut now, State::Catchup);
        assert_eq!(
            vec![
                (true, "aeron:udp?endpoint=localhost:0".to_owned()),
                (true, "aeron:udp?endpoint=localhost:40223".to_owned()),
            ],
            peer.started
        );
        assert!(merge.is_live_added());

        // The live destination is on and the recording has not moved, but the
        // live stream has only **one** transport — the archive's replay
        // transport. Taking the replay destination off here is a gap rather
        // than a merge (`:569-574`).
        peer.max_recorded_position = 1024;
        peer.image = Some(catchup_facts(1024, 1));

        // Driven, not waited on: the merge goes round `CATCHUP` and
        // `ATTEMPT_LIVE_JOIN` here, and every time it reaches the decision the
        // answer has to be no.
        for _ in 0..24 {
            merge.do_work(&mut peer, now);
            now += 50;

            assert!(
                !merge.is_merged(),
                "one transport is not a merge: {:?}",
                peer.started
            );
        }
        assert_eq!(2, peer.started.len(), "and the replay is still on");
        assert!(merge.is_live_added(), "with live already added");

        // The live stream catches up and carries two transports: the replay
        // comes off, and the merge is done.
        peer.max_recorded_position = 2048;
        peer.image = Some(catchup_facts(2048, 2));
        turn_until_state(&mut merge, &mut peer, &mut now, State::AttemptLiveJoin);
        turn_until_state(&mut merge, &mut peer, &mut now, State::Merged);

        assert!(merge.is_merged());
        assert!(!merge.has_failed());

        // What went on the subscription first came off it here, and it is the
        // **destination** — wildcard port and all — and not the replay channel's
        // endpoint, which is the port the subscription resolved.
        assert_eq!(
            vec![
                (true, "aeron:udp?endpoint=localhost:0".to_owned()),
                (true, "aeron:udp?endpoint=localhost:40223".to_owned()),
                (false, "aeron:udp?endpoint=localhost:0".to_owned()),
            ],
            peer.started
        );
        assert_eq!(vec![99], peer.stopped);

        // Closing a merged merge leaves the live destination alone — that is
        // what makes the subscription still usable (`:198-200`).
        merge.close(&mut peer);
        assert_eq!(State::Closed, merge.state());
        assert_eq!(3, peer.started.len(), "and nothing more was removed");
    }

    /// The replay channel is a **template** the merge finishes, and the replay
    /// destination's endpoint decides where it is finished.
    ///
    /// `ReplayMerge.java:139-152` in one test, because the two halves are one
    /// decision: `linger=0` and `eos=false` go on whichever way, and a
    /// destination naming a port of its own is a port the caller meant — it goes
    /// onto the channel and the merge starts asking the archive on the first
    /// turn — while a wildcard port is one only the subscription can fill in,
    /// so the merge waits for it and the resolved one goes on instead.
    ///
    /// The built string is asserted whole rather than parameter by parameter:
    /// what travels to the archive is the text, and a parameter in the wrong
    /// place is a different channel.
    #[test]
    fn the_replay_channel_is_finished_from_the_template_and_the_destination() {
        // A destination with its own port: no resolve step, and the port is the
        // channel's.
        let mut named = ReplayMerge::new(
            "aeron:udp?session-id=7",
            "aeron:udp?endpoint=localhost:40123",
            "aeron:udp?endpoint=localhost:40223",
            7,
            0,
            MERGE_PROGRESS_TIMEOUT_DEFAULT_MS,
        )
        .expect("a channel");

        assert_eq!(
            State::GetRecordingPosition,
            named.state(),
            "a destination that named a port is one the merge can use at once"
        );

        let mut peer = FakePeer::new(1024);
        let mut now = 0;
        turn_until_state(&mut named, &mut peer, &mut now, State::Catchup);

        assert_eq!(
            vec!["aeron:udp?session-id=7|linger=0|eos=false|endpoint=localhost:40123"],
            peer.replayed
        );

        // And a wildcard one: the merge waits for the subscription, and what
        // goes on the channel is the port the subscription resolved.
        let mut merge = merge(MERGE_PROGRESS_TIMEOUT_DEFAULT_MS);
        let mut peer = FakePeer::new(1024);
        let mut now = 0;

        assert_eq!(
            State::ResolveReplayPort,
            merge.state(),
            "a destination with a wildcard port is one only the subscription can fill in"
        );

        turn_until_state(&mut merge, &mut peer, &mut now, State::Catchup);

        assert_eq!(
            vec!["aeron:udp?linger=0|eos=false|endpoint=localhost:40123"],
            peer.replayed,
            "the template had no endpoint, so the resolved one is taken whole"
        );
    }

    /// The image is looked up by the **low half** of what the archive answered
    /// with, because a replay's `relevantId` is a replay id over a session id.
    ///
    /// The two halves are why the reference casts rather than converts
    /// (`ReplayMerge.java:436`): the id a replay is answered with is
    /// `(replayId << 32) | sessionId` (`ReplaySession.java:338`), and it is the
    /// session an image is named by. A merge that refused an id it could not fit
    /// in an `i32` would look for a session no subscription has and wait in
    /// `CATCHUP` for a replay that was already arriving.
    #[test]
    fn the_image_is_named_by_the_low_half_of_the_replay_id() {
        let mut merge = merge(MERGE_PROGRESS_TIMEOUT_DEFAULT_MS);
        let mut peer = FakePeer::new(1024);
        peer.replay_relevant_id = (1i64 << 32) | 0x1234;
        let mut now = 0;

        turn_until_state(&mut merge, &mut peer, &mut now, State::Catchup);
        merge.do_work(&mut peer, now);

        assert_eq!((1i64 << 32) | 0x1234, merge.replay_session_id());
        assert_eq!(
            vec![0x1234],
            peer.asked_sessions,
            "the image is asked for by the session, not by the replay id"
        );
    }

    /// A destination the driver has not answered **holds the live join off**,
    /// and the answer lets it go again.
    ///
    /// The reference's rule and the reason its merge keeps a handle at all
    /// (`:633-639`): a live destination that has been asked for is not a live
    /// destination yet, and a merge that went on would be reading the archive's
    /// answer into a state that has already moved.
    #[test]
    fn a_destination_in_flight_holds_the_live_join_off() {
        let mut merge = merge(i64::MAX / 2);
        let mut peer = FakePeer::new(1024);
        let mut now = 0;
        peer.image = Some(catchup_facts(1024, 1));

        turn_until_state(&mut merge, &mut peer, &mut now, State::AttemptLiveJoin);

        // From here the driver takes its time, so the live destination this turn
        // asks for is one nobody has answered.
        peer.answers_destinations = false;
        turn_until_state(&mut merge, &mut peer, &mut now, State::Catchup);
        assert!(merge.is_live_added(), "the live destination was asked for");

        let asked = peer.asked.len();

        for _ in 0..16 {
            merge.do_work(&mut peer, now);
            now += 1;
        }

        assert_eq!(
            asked,
            peer.asked.len(),
            "the merge asks the archive nothing while a destination is in flight"
        );
        assert!(
            !merge.is_merged(),
            "and it is not merged on an answer it has not had"
        );

        // The driver answers, and the merge picks up where it left off.
        peer.answer_last_destination();
        peer.max_recorded_position = 2048;
        peer.image = Some(catchup_facts(2048, 2));
        peer.answers_destinations = true;

        turn_until_state(&mut merge, &mut peer, &mut now, State::Merged);
        assert!(merge.is_merged());
    }

    /// The replay destination goes on the subscription **before** anything is
    /// asked of the archive, and to the recording the merge was built with.
    ///
    /// The first half is the reference's `init` (`:490`) done a turn later,
    /// which is the only place this build could do it; the second is why
    /// `getMaxRecordedPosition` is handed the id rather than remembering one of
    /// its own.
    #[test]
    fn the_replay_destination_goes_on_before_the_first_question() {
        let mut merge = merge(MERGE_PROGRESS_TIMEOUT_DEFAULT_MS);
        let mut peer = FakePeer::new(1024);
        let mut now = 0;

        merge.do_work(&mut peer, now);
        assert_eq!(
            vec![(true, "aeron:udp?endpoint=localhost:0".to_owned())],
            peer.started,
            "the destination went on in the first turn"
        );

        turn_until_state(&mut merge, &mut peer, &mut now, State::Replay);

        // Once, however many turns went by — a second add would be a second
        // destination on a subscription that has one.
        assert_eq!(1, peer.started.len());
        assert!(!peer.asked.is_empty(), "and the archive was asked");
        assert!(
            peer.asked_about
                .iter()
                .all(|recording_id| *recording_id == 7),
            "every question was about this merge's recording: {:?}",
            peer.asked_about
        );
    }

    /// A destination the driver will not give the subscription is a failure with
    /// its own words, and it happens before the merge has asked the archive
    /// anything.
    #[test]
    fn a_destination_the_driver_refuses_fails_the_merge() {
        let mut merge = merge(MERGE_PROGRESS_TIMEOUT_DEFAULT_MS);
        let mut peer = FakePeer::new(1024);
        peer.refuse_destinations = Some("subscription is not a multi-destination one".to_owned());

        merge.do_work(&mut peer, 0);

        assert!(merge.has_failed());
        assert_eq!(
            Some(&MergeFailure::DestinationRefused {
                message: "subscription is not a multi-destination one".to_owned(),
            }),
            merge.failure_reason()
        );
        assert_eq!(
            "ReplayMerge destination refused: subscription is not a multi-destination one",
            merge.failure_reason().expect("a reason").to_string()
        );
        assert!(peer.asked.is_empty(), "the archive was never asked");
    }

    /// A merge that hears nothing fails with the state it was waiting in, after
    /// the progress timeout (`:576-583`).
    #[test]
    fn a_merge_that_makes_no_progress_fails_with_its_state() {
        let mut merge = merge(MERGE_PROGRESS_TIMEOUT_DEFAULT_MS);
        let mut peer = FakePeer::new(1024);
        peer.resolved_endpoint = None;

        merge.do_work(&mut peer, 0);
        assert_eq!(State::ResolveReplayPort, merge.state());

        merge.do_work(&mut peer, MERGE_PROGRESS_TIMEOUT_DEFAULT_MS + 1);

        assert!(merge.has_failed());
        assert_eq!(
            Some(&MergeFailure::NoProgress {
                state: State::ResolveReplayPort,
                active_transport_count: 0,
            }),
            merge.failure_reason()
        );
        assert_eq!(
            "ReplayMerge no progress: state=RESOLVE_REPLAY_PORT, activeTransportCount=0",
            merge.failure_reason().expect("a reason").to_string()
        );
    }

    /// An image that closes while the merge is catching up is the reference's
    /// `IllegalStateException` (`:457`), and it is what a merge whose replay was
    /// stopped underneath it sees.
    ///
    /// The image **going** is how this peer says closed, which is what the
    /// client behind the real one does — see [`MergePeer::image`]. Nothing
    /// catches this before the replay has been found, which is the other half
    /// of the same rule and is why the merge walks to CATCHUP first.
    #[test]
    fn an_image_that_closes_while_catching_up_fails_the_merge() {
        let mut merge = merge(MERGE_PROGRESS_TIMEOUT_DEFAULT_MS);
        let mut peer = FakePeer::new(1024);
        let mut now = 0;
        peer.image = Some(catchup_facts(0, 1));

        turn_until_state(&mut merge, &mut peer, &mut now, State::GetRecordingPosition);
        turn_until_state(&mut merge, &mut peer, &mut now, State::Replay);
        turn_until_state(&mut merge, &mut peer, &mut now, State::Catchup);

        // One turn with the image there, which is the turn that finds it —
        // arriving in CATCHUP and being *in* it are two turns, so the image has
        // to be found before it can go.
        merge.do_work(&mut peer, now);
        assert_eq!(
            State::Catchup,
            merge.state(),
            "the merge is still catching up"
        );

        peer.image = None;
        merge.do_work(&mut peer, now + 1);

        assert!(merge.has_failed());
        assert_eq!(Some(&MergeFailure::ImageClosed), merge.failure_reason());
    }

    /// And the other half of "no image": one that has not **appeared** yet is
    /// not a closed one.
    ///
    /// The merge goes on waiting rather than failing — and on waiting without a
    /// deadline, because that is the reference's own gate: the turn that looks
    /// for the image also records progress (`:591-596`), so as long as the
    /// subscription is connected the progress timeout is pushed back every
    /// turn. A merge waiting for an image that never comes waits forever, there
    /// and here, and what would end it is the caller's `close`.
    #[test]
    fn an_image_that_has_not_appeared_yet_is_not_a_closed_one() {
        let mut merge = merge(MERGE_PROGRESS_TIMEOUT_DEFAULT_MS);
        let mut peer = FakePeer::new(1024);
        let mut now = 0;
        assert_eq!(None, peer.image, "this peer never has an image");

        turn_until_state(&mut merge, &mut peer, &mut now, State::Catchup);

        // Well past the progress timeout, in both directions of the clock a
        // merge could be reading: neither is a closed image.
        merge.do_work(&mut peer, now);
        merge.do_work(&mut peer, now + MERGE_PROGRESS_TIMEOUT_DEFAULT_MS + 1);

        assert_eq!(State::Catchup, merge.state());
        assert_eq!(
            None,
            merge.failure_reason(),
            "a replay whose image has not arrived is not a failure"
        );
    }

    /// The backoff between `getMaxRecordedPosition` attempts doubles to a
    /// ceiling — **whether or not the request went out** (`:536-540`): a merge
    /// that only backed off on failure would ask once a turn forever.
    ///
    /// The gaps are 16, 32, … and not 8: the initial backoff is doubled before
    /// it is ever used as a delay, which is what the reference's field order
    /// does too.
    #[test]
    fn the_get_position_backoff_doubles_to_its_ceiling() {
        let mut merge = merge(i64::MAX / 2);
        // A recording that has nothing to give yet: the archive answers
        // `NULL_POSITION`, which is what keeps a merge in this state rather
        // than moving it on to the replay.
        let mut peer = FakePeer::new(NULL_POSITION);

        let mut attempted_at = Vec::new();

        for now in 0..1_100 {
            let before = peer.asked.len();
            merge.do_work(&mut peer, now);

            if peer.asked.len() > before {
                attempted_at.push(now);
            }
        }

        assert_eq!(State::GetRecordingPosition, merge.state());

        let gaps: Vec<i64> = attempted_at
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .collect();

        assert_eq!(vec![16, 32, 64, 128, 256, 500], gaps);
    }

    /// A request that could not be sent does not stop the backoff either — the
    /// same rule, from the other side.
    #[test]
    fn a_request_that_could_not_be_sent_still_backs_off() {
        let mut merge = merge(i64::MAX / 2);
        let mut peer = FakePeer::new(NULL_POSITION);
        peer.sends = false;

        let mut attempted_at = Vec::new();

        for now in 0..1_100 {
            let before = peer.asked.len();
            merge.do_work(&mut peer, now);

            if peer.asked.len() > before {
                attempted_at.push(now);
            }
        }

        // Not one request went out — and the merge went on asking at the same
        // cadence, because the backoff does not depend on that either.
        let gaps: Vec<i64> = attempted_at
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .collect();

        assert_eq!(vec![16, 32, 64, 128, 256, 500], gaps);
        assert_eq!(State::GetRecordingPosition, merge.state());
    }
}
