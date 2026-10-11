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
//! **One state of twelve walks: `CONNECT`.** It is the one that ends with the
//! session holding an archive client of its own, which is what every state after
//! it asks the source through, and the one place a replication can fail outright.
//!
//! The other eleven are named, documented and **do nothing**, and that is
//! deliberate: a state method with a plausible-looking stub is worse than one
//! with no body at all, because the next reader cannot tell the two apart. They
//! arrive one commit at a time, in the order they are listed in [`State`].

use deepmsg_client::client::{AsyncNextSessionId, AsyncNextSessionIdPoll, Client};
use deepmsg_codec::archive::recording_signal::RecordingSignal;

use crate::catalog::{Catalog, Recording};
use crate::client::archive::{Archive, Handlers};
use crate::client::async_connect::{AsyncConnect, NoCredentials, Polled};
use crate::client::context::ArchiveContext;
use crate::client::descriptor_poller::RecordingDescriptor;
use crate::mark::NULL_VALUE;
use crate::server::conductor::{
    GENERIC, NULL_POSITION, REPLICATION_CONNECTION_FAILURE, UNKNOWN_RECORDING,
};
use crate::server::control_adapter::ReplicationRequest;

/// The state a session is in (`ReplicationSession.java:61-75`), in the order it
/// walks them.
///
/// Each variant names the reference's own method for that state, which is one
/// method per state and no state without one. `DO_WORK` is what calls them
/// (`:217-293`, whose `switch` is the whole of the machine).
///
/// `CATCHUP` and `ATTEMPT_LIVE_JOIN` are the live-merge pair, and the four
/// criteria this build is held to never reach them (`live_destination` is empty
/// in all four); they are named here so the pair is not mistaken for a gap.
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

/// Something a session owes the destination's client and **cannot send itself**.
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
            active_correlation_id: NULL_VALUE,
            time_of_last_action_ms: 0,
            action_timeout_ms,
            src_stop_position: NULL_POSITION,
            replay_position,
            replay_stream_id,
            replay_session_id: 0,
            is_destination_recording_empty,
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
        self.state = State::Done;
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
        if self.state == State::Done {
            return ReplicationProgress::Done;
        }

        // `ReplicationSession.doWork`'s `try` (`:273-278`): a state that throws
        // has its failure answered **for it**, with `GENERIC` and the
        // exception's own message, and then the state is over. A state that
        // answers with a code of its own returns `Ok` and has done so.
        let outcome = match self.state {
            State::Connect => self.connect(client),
            State::ReplicateDescriptor => self.replicate_descriptor(client, catalog, now_ms),
            // The other ten, one commit at a time. A state that returns a
            // plausible-looking step here would be worse than one that does
            // nothing, because the next reader could not tell them apart.
            _ => Ok(()),
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

        if let Some(destination) = self.destination_row(descriptor, catalog) {
            if self.request.dst_recording_id == NULL_VALUE {
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
        // slice is held to (`live_destination` is empty in all four) and the
        // state it names does not walk yet; the transition is written down
        // because the **refusal** in front of it is the reference's, and it is
        // the one a live request meets.
        if self.live_destination.is_some() {
            if self.request.stop_position != NULL_POSITION {
                self.fail(
                    GENERIC,
                    "cannot live merge without active source recording".to_owned(),
                );

                return;
            }

            self.state = State::SrcRecordingPosition;
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
            self.state = State::Done;

            return;
        }

        self.state = State::Extend;
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
            return match AsyncConnect::new(&self.context, client, &mut NoCredentials) {
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

        let Some(connect) = self.async_connect.as_mut() else {
            return Ok(());
        };

        match connect.poll(client, &mut NoCredentials) {
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
                self.state = State::ReplicateDescriptor;

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
        self.state = State::Done;
        self.notices.push(ReplicationNotice::Error {
            correlation_id: self.replication_id.unwrap_or(NULL_VALUE),
            code,
            message,
        });
    }
}
