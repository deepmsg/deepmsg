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
//! * **The client here cannot block.** The reference's `asyncConnect` returns a
//!   pollable object and its session polls it once per turn; this build's
//!   synchronous [`Archive::connect`](crate::client::archive::Archive::connect)
//!   loops until it is done, which inside the conductor's turn is a deadlock
//!   (S1's rule: a blocking command in the launcher never returns). The client
//!   grew the door for this — `Archive::from_connected` and `AsyncConnect` are
//!   both public — and it is opened by the `CONNECT` state below.
//! * **There is more than one at a time.** `replicationSessionByIdMap`
//!   (`ArchiveConductor.java:148`) is keyed by the replication's id and holds
//!   every session in flight, so a consumer of a descriptor cannot be a `fn`
//!   pointer reaching for a `static` — see the client's descriptor poller,
//!   which hands the descriptor to a `&mut dyn FnMut` for exactly this caller.
//!
//! # What is here, and what is not
//!
//! **This is the skeleton**: the twelve states, the facts a session is built
//! from, and a turn that does nothing. It is what [`super::conductor`] registers
//! when it answers a replicate with an id, and what a later commit makes walk.
//! Every state method is therefore a `TODO`-shaped hole rather than a stub with
//! a plausible body: an empty body that *looks* like a step is worse than no
//! body at all, because the next reader cannot tell the two apart.

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationProgress {
    /// Nothing this turn, or nothing the conductor has to act on.
    Idle,
    /// The session is over and its slot can be given back.
    Done,
}

/// One replication in flight.
///
/// Built by `Sessions::replicate` (`super::conductor`) from the request that
/// asked for it, and nothing else: everything the reference's constructor takes
/// beyond that — the catalog's summary of the destination, the epoch clock, the
/// conductor itself — arrives with the states that need it.
#[derive(Debug, PartialEq, Eq)]
pub struct ReplicationSession {
    /// Where the machine has got to.
    state: State,
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
    #[allow(dead_code)] // read by the twelve states, none of which walk yet
    request: ReplicationRequest,
    /// The channel the copy really runs over: the request's own, or the
    /// archive's configured one when the request named none
    /// (`ArchiveConductor.java:1394-1395`).
    #[allow(dead_code)] // read when EXTEND builds the destination's channel
    replication_channel: String,
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
    ) -> Self {
        Self {
            state: State::Connect,
            replication_id: Some(replication_id),
            request,
            replication_channel,
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

    /// One turn (`ReplicationSession.doWork` — the reference has no such method
    /// and instead *is* a `Session`, which the conductor's worker calls
    /// `doWork` on).
    ///
    /// **Nothing walks yet**: the twelve states are the next two commits, and
    /// until one of them exists there is no step to take. What this does have
    /// is the one transition the conductor needs to give a slot back.
    pub fn do_work(&mut self, _now_ms: i64) -> ReplicationProgress {
        if self.state == State::Done {
            ReplicationProgress::Done
        } else {
            ReplicationProgress::Idle
        }
    }
}
