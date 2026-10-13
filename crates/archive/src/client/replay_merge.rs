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
//! The reference holds an `Image` handle and reads its position every turn. A
//! handle here would borrow the subscription for the life of the merge, so the
//! merge holds an [`ImageFacts`] **snapshot per turn** instead, taken through
//! [`MergePeer::image`] — same reads, no borrow. An image that can no longer be
//! read at all is an image the client has closed ([`ImageFacts::is_closed`] is
//! what the reference asks), and is reported as one.

use crate::client::archive::ArchiveError;
use deepmsg_client::image::Fragment;
use deepmsg_core::uri::{ChannelUri, UriError};

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
    /// Whether the client has closed the image (`Image.isClosed`).
    pub is_closed: bool,
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

    /// The replay's image, if the subscription has one for that session.
    fn image(&mut self, session_id: i32) -> Option<ImageFacts>;

    /// `AeronArchive.getMaxRecordedPosition`.
    ///
    /// # Errors
    ///
    /// [`MergeFailure`] if the request could not be sent.
    fn get_max_recorded_position(&mut self, correlation_id: i64) -> Result<bool, MergeFailure>;

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

    /// Add the live destination to the subscription
    /// (`Subscription.asyncAddDestination`).
    ///
    /// # Errors
    ///
    /// [`MergeFailure`] if the command could not be sent.
    fn add_live_destination(&mut self, destination: &str) -> Result<(), MergeFailure>;

    /// Remove the replay destination from the subscription.
    ///
    /// # Errors
    ///
    /// [`MergeFailure`] if the command could not be sent.
    fn remove_replay_destination(&mut self, destination: &str) -> Result<(), MergeFailure>;

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
    /// # Errors
    ///
    /// [`UriError`] if `replay_channel` is not a channel URI.
    pub fn new(
        replay_channel: &str,
        replay_destination: &str,
        live_destination: &str,
        recording_id: i64,
        start_position: i64,
        merge_progress_timeout_ms: i64,
    ) -> Result<Self, UriError> {
        let replay_channel = ChannelUri::parse(replay_channel)?;

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
            state: State::ResolveReplayPort,
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
                let _ = peer.remove_replay_destination(&replay_destination);
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
        } else if facts.is_closed {
            Err(MergeFailure::ImageClosed)
        } else {
            Ok(0)
        }
    }

    fn attempt_live_join<P: MergePeer>(&mut self, peer: &mut P, now_ms: i64) -> Outcome {
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
                    peer.add_live_destination(&live_destination)?;
                    self.time_of_last_progress_ms = now_ms;
                    self.position_of_last_progress = facts.position;
                    self.is_live_added = true;
                } else if self.should_stop_and_remove_replay(facts) {
                    let replay_destination = self.replay_destination.clone();
                    peer.remove_replay_destination(&replay_destination)?;
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
        let sent = peer.get_max_recorded_position(correlation_id)?;

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

    /// The replay session as the wire carries it: the archive's id is an `i64`
    /// on the control channel and an `i32` in an image (`:436` does the same
    /// narrowing).
    fn replay_session(&self) -> i32 {
        i32::try_from(self.replay_session_id).unwrap_or(i32::MAX)
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
        live_added: Vec<String>,
        replay_removed: Vec<String>,
        stopped: Vec<i64>,
        client_closed: bool,
        archive_connected: bool,
    }

    impl FakePeer {
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
                live_added: Vec::new(),
                replay_removed: Vec::new(),
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

        fn image(&mut self, _session_id: i32) -> Option<ImageFacts> {
            self.image
        }

        fn get_max_recorded_position(&mut self, correlation_id: i64) -> Result<bool, MergeFailure> {
            self.asked.push(correlation_id);

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
            _channel: &str,
            _stream_id: i32,
            correlation_id: i64,
        ) -> Result<bool, MergeFailure> {
            if self.answers {
                self.pending.push((correlation_id, 99));
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

        fn add_live_destination(&mut self, destination: &str) -> Result<(), MergeFailure> {
            self.live_added.push(destination.to_owned());

            Ok(())
        }

        fn remove_replay_destination(&mut self, destination: &str) -> Result<(), MergeFailure> {
            self.replay_removed.push(destination.to_owned());

            Ok(())
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

    fn merge(timeout_ms: i64) -> ReplayMerge {
        ReplayMerge::new(
            "aeron:udp?endpoint=localhost:0",
            "aeron:udp?endpoint=localhost:40123",
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
            is_closed: false,
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
        assert!(
            peer.live_added.is_empty(),
            "nothing is added before the target"
        );

        // It has, so the merge asks where the recording is now.
        peer.image = Some(catchup_facts(1024, 1));
        turn_until_state(&mut merge, &mut peer, &mut now, State::AttemptLiveJoin);

        // The live destination goes on — the target is within a quarter term —
        // and the merge goes back to catching up, now with two destinations.
        turn_until_state(&mut merge, &mut peer, &mut now, State::Catchup);
        assert_eq!(vec!["aeron:udp?endpoint=localhost:40223"], peer.live_added);
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
                "one transport is not a merge: {}",
                peer.replay_removed.len()
            );
        }
        assert!(peer.replay_removed.is_empty(), "and the replay is still on");
        assert!(merge.is_live_added(), "with live already added");

        // The live stream catches up and carries two transports: the replay
        // comes off, and the merge is done.
        peer.max_recorded_position = 2048;
        peer.image = Some(catchup_facts(2048, 2));
        turn_until_state(&mut merge, &mut peer, &mut now, State::AttemptLiveJoin);
        turn_until_state(&mut merge, &mut peer, &mut now, State::Merged);

        assert!(merge.is_merged());
        assert!(!merge.has_failed());
        assert_eq!(
            vec!["aeron:udp?endpoint=localhost:40123"],
            peer.replay_removed
        );
        assert_eq!(vec![99], peer.stopped);

        // Closing a merged merge leaves the live destination alone — that is
        // what makes the subscription still usable (`:198-200`).
        merge.close(&mut peer);
        assert_eq!(State::Closed, merge.state());
        assert_eq!(1, peer.replay_removed.len(), "and nothing more was removed");
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
    #[test]
    fn an_image_that_closes_while_catching_up_fails_the_merge() {
        let mut merge = merge(MERGE_PROGRESS_TIMEOUT_DEFAULT_MS);
        let mut peer = FakePeer::new(1024);
        let mut now = 0;
        peer.image = Some(catchup_facts(0, 1));

        turn_until_state(&mut merge, &mut peer, &mut now, State::GetRecordingPosition);
        turn_until_state(&mut merge, &mut peer, &mut now, State::Replay);
        turn_until_state(&mut merge, &mut peer, &mut now, State::Catchup);

        peer.image = Some(ImageFacts {
            is_closed: true,
            ..catchup_facts(0, 1)
        });
        merge.do_work(&mut peer, now);
        merge.do_work(&mut peer, now + 1);

        assert!(merge.has_failed());
        assert_eq!(Some(&MergeFailure::ImageClosed), merge.failure_reason());
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
