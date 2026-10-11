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

use deepmsg_client::client::Client;

use crate::client::archive::{Archive, Handlers};
use crate::client::async_connect::{AsyncConnect, NoCredentials, Polled};
use crate::client::context::ArchiveContext;
use crate::mark::NULL_VALUE;
use crate::server::conductor::REPLICATION_CONNECTION_FAILURE;
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
    /// The session failed, and the client is owed an `ERROR`
    /// (`ReplicationSession.error`, `:923-926`).
    ///
    /// The correlation id is the **replication's**, not the request's: the
    /// reference answers `sendErrorResponse(replicationId, errorCode, msg)`
    /// (`:925`), and `replicationId` is the first argument, which is the
    /// correlation id. So a client sees an error about a request it made long
    /// ago, under an id it was answered with rather than one it sent.
    Failed {
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
    #[allow(dead_code)] // read by the twelve states, most of which do not walk yet
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
    /// The connect, while one is going on (`ReplicationSession.java:106`), and
    /// `None` once it has finished **or** before it starts.
    ///
    /// It is an [`AsyncConnect`] and not [`Archive::connect`] because of where
    /// it runs: the conductor's turn. See the module note.
    async_connect: Option<AsyncConnect>,
    /// The source archive, once connected (`:107`). Every state past `CONNECT`
    /// reads a request through it.
    ///
    /// **Not read yet** — `REPLICATE_DESCRIPTOR` is the next commit's, and it is
    /// the first state that asks the source anything.
    #[allow(dead_code)]
    src_archive: Option<Archive>,
}

impl ReplicationSession {
    /// A session that has not started: `state = CONNECT`
    /// (`ReplicationSession.java:110`).
    ///
    /// `replication_id` is minted by the conductor from its session counter
    /// (`ArchiveConductor.java:1455`) and handed in: the request carries a
    /// **correlation** id, which is a different number.
    #[must_use]
    pub fn new(
        replication_id: i64,
        request: ReplicationRequest,
        replication_channel: String,
        context: ArchiveContext,
    ) -> Self {
        Self {
            state: State::Connect,
            control_session_id: request.session_id,
            replication_id: Some(replication_id),
            request,
            replication_channel,
            context,
            async_connect: None,
            src_archive: None,
        }
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
    pub fn do_work(&mut self, client: &mut Client, _now_ms: i64) -> ReplicationProgress {
        match self.state {
            State::Connect => self.connect(client),
            State::Done => ReplicationProgress::Done,
            // The other ten, one commit at a time. A state that returns a
            // plausible-looking step here would be worse than one that does
            // nothing, because the next reader could not tell them apart.
            _ => ReplicationProgress::Idle,
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
    fn connect(&mut self, client: &mut Client) -> ReplicationProgress {
        if self.async_connect.is_none() {
            // `AeronArchive.asyncConnect(context)` (`:407`). Its failure modes
            // are the context's and the driver's, and both are the same answer
            // here as a late failure: the machine is over and the client is told
            // why.
            return match AsyncConnect::new(&self.context, client, &mut NoCredentials) {
                Ok(connect) => {
                    self.async_connect = Some(connect);
                    ReplicationProgress::Idle
                }
                Err(error) => self.fail(&error.to_string()),
            };
        }

        let Some(connect) = self.async_connect.as_mut() else {
            return ReplicationProgress::Idle;
        };

        match connect.poll(client, &mut NoCredentials) {
            Ok(Polled::Awaiting) => ReplicationProgress::Idle,
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
                ReplicationProgress::Idle
            }
            Err(error) => self.fail(&error.to_string()),
        }
    }

    /// `ReplicationSession.error` (`:923-926`), and the `state(DONE, …)` line
    /// every caller in front of it has already done.
    ///
    /// The reference's words are `"Replication connection failed=" + message`
    /// (`:441`), assembled at one call site and passed in here; the state change
    /// is made **here** rather than at each of them, because every caller's is
    /// the same line.
    fn fail(&mut self, reason: &str) -> ReplicationProgress {
        self.state = State::Done;

        ReplicationProgress::Failed {
            correlation_id: self.replication_id.unwrap_or(NULL_VALUE),
            code: REPLICATION_CONNECTION_FAILURE,
            message: format!("Replication connection failed={reason}"),
        }
    }
}
