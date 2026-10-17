//! What the election asks of the consensus module.
//!
//! **This trait is frozen.** `P3-5a-α` closed it; after that, methods may be
//! *added* (a later slice discovers a gap) but never changed or removed —
//! lane M compiles against it from `P3-4a` onward.
//!
//! # Where the method set comes from
//!
//! It is not a design choice: it is exactly what `Election.java` calls on its
//! `ConsensusModuleAgent`, captured by this command over the reference at
//! `664f58e705`:
//!
//! ```text
//! grep -o 'consensusModuleAgent\.[a-zA-Z]*(' Election.java | sort -u | wc -l
//! ```
//!
//! That answers **26 distinct names**, which is where the plan's "26 个方法"
//! comes from. But four of those names are Java overload pairs — a getter and a
//! setter — so the call surface is **30 distinct signatures** across **44
//! calls**. Java can write both halves as one name; Rust cannot, so this trait
//! has 30 methods. `tests/election_surface.rs` pins the set against
//! `tests/fixtures/election-surface.tsv`.
//!
//! # Departures from the reference, all deliberate
//!
//! 1. **A member is named by its id, not handed over as an object.** Java passes
//!    the `ClusterMember` (`Election.java:552`, `:304`). The member table lives
//!    in the host, and the host is already `&mut` for the duration of the call,
//!    so it looks its own member up. Handing out `&mut ClusterMember` across a
//!    `&mut dyn` boundary would freeze a borrow this trait does not need.
//! 2. **Void means void.** The reference's `void` methods signal failure by
//!    throwing; there is nothing to return. Here they return
//!    `Result<(), ClusterError>` because the *throwing* is the part that must
//!    survive the translation (plan §4.4, rule 5) — not the return type.
//! 3. **Getters take `&self`.** `role()`, `state()`, `log_recording_id()`,
//!    `time_of_last_leader_update_ns()` and the two destination getters only
//!    read; everything else takes `&mut self` because the reference's
//!    counterpart can mutate host state even when it looks like a query.
//!
//! # The three methods that block
//!
//! `join_log_as_leader`, `try_join_log_as_follower` and `await_local_sockets_closed`
//! spin in the reference (`ConsensusModuleAgent.java:1688`, `:1771`, `:2988`).
//! In the host they are written as `loop { if done { break } idle()? }`, where
//! `idle()` drives this agent's own client and polls archive events — and
//! **nothing else** (plan §4.4). The election never writes such a loop itself;
//! neither does the reference's.
//!
//! # What is deliberately not here
//!
//! The reference also reaches the consensus module's `Context` from the
//! election — `nodeStateFile()`, `recordingLog()`, the clocks, four timeouts,
//! the commit-position counter, the error handler, the election counter
//! (`Election.java:148`, `:264`, `:265`, `:357`, `:376`, `:456`, `:681`,
//! `:687`, `:704`, `:715`, `:736`, `:1509`, `:1520`, `:1598-1608`). Those are
//! **`P3-5a-β`'s input**, not α's: `NodeStateFile` and `RecordingLog` arrive
//! with `P3-1d`/`P3-1f`, and α exists precisely so lane M does not wait for
//! them. Adding them later is allowed by the freeze rule — the trait may grow,
//! it may not change.
//!
//! The one `Context` call the election makes that is *not* deferred is
//! `random()` (`:723`): it is replaced by the injected `ElectionRng` field, so
//! that the canvass backoff is reproducible (see the `election` module docs).

use deepmsg_client::image::Image;

use crate::persist::node_state::NodeStateFile;
use crate::persist::recording_log::RecordingLog;

use crate::election::state::ElectionState;
use crate::error::ClusterError;
use crate::handles::{LogReplay, RecordingReplication};
use crate::state::{ModuleState, Role};

/// What the election needs from the consensus module.
///
/// Every method names its reference counterpart. The line numbers are
/// `ConsensusModuleAgent.java` unless stated otherwise.
pub trait ElectionHost {
    // ---- the three counter-backed axes ---------------------------------

    /// Write counter 201. `role(Cluster.Role)` at `:1337`.
    ///
    /// Only the election calls this (plan §2: role is election-driven).
    fn set_role(&mut self, new_role: Role);

    /// Read counter 201. `role()` at `:1355`.
    fn role(&self) -> Role;

    /// Read counter 200. `state()` at `:1323`.
    ///
    /// Note the reference only *reads* this from the election (`:1104`); the
    /// plan's trait table listed the setter by mistake, which the surface test
    /// caught.
    fn state(&self) -> ModuleState;

    // ---- log recording identity ----------------------------------------

    /// `logRecordingId()` at `:1869`.
    fn log_recording_id(&self) -> i64;

    /// `logRecordingId(long)` at `:1874`, called once from
    /// `Election.java:280` when a replication produces a recording.
    fn set_log_recording_id(&mut self, recording_id: i64);

    /// `timeOfLastLeaderUpdateNs()` at `:2908`.
    fn time_of_last_leader_update_ns(&self) -> i64;

    // ---- positions -----------------------------------------------------

    /// The leader-side quorum position, bounded by the leader's own append
    /// position. `quorumPositionBoundedByLeaderLog(long, long)` at `:2897`.
    fn quorum_position_bounded_by_leader_log(
        &mut self,
        leader_append_position: i64,
        now_ns: i64,
    ) -> i64;

    /// Record what a member told us its position is.
    /// `updateMemberLogPosition(ClusterMember, long, long)` at `:1100`.
    fn update_member_log_position(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        log_position: i64,
    );

    /// Advance the leader's own position and publish it.
    /// `updateLeaderPosition(long, long, long)` at `:2913` — the three-argument
    /// overload; the one-argument form at `:2885` is private and not on this
    /// seam.
    fn update_leader_position(
        &mut self,
        now_ns: i64,
        append_position: i64,
        quorum_position: i64,
    ) -> Result<usize, ClusterError>;

    /// `publishCommitPosition(long, long)` at `:2940`.
    fn publish_commit_position(
        &mut self,
        commit_position: i64,
        leadership_term_id: i64,
    ) -> Result<(), ClusterError>;

    // ---- joining the log ----------------------------------------------

    /// Become leader for a term. `joinLogAsLeader(long, long, int, boolean)`
    /// at `:1688`. **Blocks** (creates the recording and waits for the append
    /// position counter and for the services).
    fn join_log_as_leader(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
        log_session_id: i32,
        is_startup: bool,
    ) -> Result<(), ClusterError>;

    /// Become a follower on an existing log image.
    /// `tryJoinLogAsFollower(Image, boolean, long)` at `:1771`. **Blocks**.
    fn try_join_log_as_follower(
        &mut self,
        image: Image,
        is_leader_startup: bool,
        now_ns: i64,
    ) -> Result<bool, ClusterError>;

    /// Create the log publication and add a destination per member.
    /// `addLogPublication(long)` at `:1639`. Returns the publication's work
    /// count.
    fn add_log_publication(&mut self, append_position: i64) -> Result<usize, ClusterError>;

    /// Start replaying our own log tail. `newLogReplay(long, long)` at `:1840`.
    /// The replay is polled through [`Self::poll_archive_events`], not here.
    fn new_log_replay(
        &mut self,
        log_position: i64,
        append_position: i64,
    ) -> Result<LogReplay, ClusterError>;

    /// Start replicating a recording from another archive.
    /// `newLogReplication(String, String, long, long, long)` at `:2951`.
    ///
    /// The second endpoint is the member's `archiveResponseEndpoint` — it is
    /// `Option` here because the reference null-checks it to choose between a
    /// plain and a response-mode replication channel (`:2964-2972`). It is the
    /// only consumer of that member attribute (`P3-0e`).
    fn new_log_replication(
        &mut self,
        leader_archive_endpoint: &str,
        response_archive_endpoint: Option<&str>,
        leader_recording_id: i64,
        stop_position: i64,
        now_ns: i64,
    ) -> Result<RecordingReplication, ClusterError>;

    /// Cut the log back to a term boundary. `truncateLogEntry(long, long)` at
    /// `:1882`. The reference's caller then always raises a `ClusterEvent`
    /// (`Election.java:634-645`) — that is the *caller's* job, not this one's.
    fn truncate_log_entry(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
    ) -> Result<(), ClusterError>;

    /// Append the `NewLeadershipTermEvent` that marks the term's start.
    /// `appendNewLeadershipTermEvent(long)` at `:1893`.
    fn append_new_leadership_term_event(&mut self, now_ns: i64) -> Result<bool, ClusterError>;

    /// Stop recording, close the log, roll back uncommitted state, and report
    /// the position to start from. `prepareForNewLeadership(long, long)` at
    /// `:1360`.
    fn prepare_for_new_leadership(
        &mut self,
        log_position: i64,
        now_ns: i64,
    ) -> Result<i64, ClusterError>;

    // ---- catchup -------------------------------------------------------

    /// A catchup has begun. `catchupInitiated(long)` at `:1946`-`2059`.
    fn catchup_initiated(&mut self, now_ns: i64) -> Result<(), ClusterError>;

    /// Poll the catchup up to `limit_position`. `catchupPoll(long, long)` at
    /// `:1946`-`2059`.
    fn catchup_poll(&mut self, limit_position: i64, now_ns: i64) -> Result<usize, ClusterError>;

    /// Is the catchup close enough to switch to live?
    /// `isCatchupNearLive(long)` at `:1946`-`2059`.
    fn is_catchup_near_live(&mut self, position: i64) -> bool;

    /// Note a member's catchup progress.
    /// `trackCatchupCompletion(ClusterMember, long, short)` at `:1946`.
    fn track_catchup_completion(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        append_position_flags: i16,
    ) -> Result<(), ClusterError>;

    /// `stopAllCatchups()` at `:1946`-`2059`.
    fn stop_all_catchups(&mut self) -> Result<(), ClusterError>;

    /// The live log destination, or `""` when unset.
    /// `liveLogDestination()` at `:1746`.
    fn live_log_destination(&self) -> &str;

    /// `liveLogDestination(String)` at `:1741`.
    fn set_live_log_destination(&mut self, destination: &str) -> Result<(), ClusterError>;

    /// The catchup log destination, or `""` when unset.
    /// `catchupLogDestination()` at `:1756`.
    fn catchup_log_destination(&self) -> &str;

    /// `catchupLogDestination(String)` at `:1751`.
    fn set_catchup_log_destination(&mut self, destination: &str) -> Result<(), ClusterError>;

    // ---- lifecycle -----------------------------------------------------

    /// Wait until the local sockets a closed registration held are really
    /// free. `awaitLocalSocketsClosed(long)` at `:2988`. **Blocks**; the wait
    /// is why a follower can rebind a UDP port without racing the old image.
    fn await_local_sockets_closed(&mut self, registration_id: i64) -> Result<(), ClusterError>;

    /// Hand control back to the steady state. `electionComplete(long)` at
    /// `:1905`. The reference nulls `election` from inside this call; here the
    /// outer take-and-put-back drops the `Election` instead (plan §4.5).
    fn election_complete(&mut self, now_ns: i64) -> Result<(), ClusterError>;

    /// Poll archive events (recording signals). `pollArchiveEvents()` at
    /// `:2061`. Called from `idle()` and from the slow tick; returns the work
    /// count.
    fn poll_archive_events(&mut self) -> usize;

    // ---- added by P3-5a-beta: the `Context` accessors the election reads ----
    //
    // `P3-5a-alpha` deliberately froze the trait without these, because
    // `NodeStateFile` and `RecordingLog` did not exist yet and alpha existed so
    // lane M would not wait for them. Both exist now, so they arrive here — by
    // the add-only rule, not by changing anything above.

    /// `ctx.nodeStateFile()` (`Election.java:681`, `:687`, `:357`, `:376`).
    fn node_state(&mut self) -> &mut NodeStateFile;

    /// `ctx.recordingLog()` (`:1509`, inside `updateRecordingLog`).
    fn recording_log(&mut self) -> &mut RecordingLog;

    /// `ctx.clusterClock()` converted to nanoseconds (`:1598-1608`).
    fn now_ns(&mut self) -> i64;

    /// `ctx.epochClock().time()` in epoch milliseconds (`:688`).
    fn epoch_ms(&self) -> i64;

    /// `nanosToTimestamp(ctx, nowNs)` (`:1601-1604`): nanosecond time in the
    /// cluster clock's own time unit, which is what `RecordingLog` stores.
    fn timestamp_from_nanos(&self, now_ns: i64) -> i64;

    /// `timestampToNanos(ctx, timestamp)` (`:1606-1609`): the inverse, used by
    /// `onReplayNewLeadershipTermEvent` (`:603`).
    fn nanos_from_timestamp(&self, timestamp: i64) -> i64;

    /// `ctx.appointedLeaderId()` (`:715`).
    fn appointed_leader_id(&self) -> i32;

    /// `ctx.startupCanvassTimeoutNs()` (`:704`).
    fn startup_canvass_timeout_ns(&self) -> i64;

    /// `ctx.leaderHeartbeatTimeoutNs()`.
    fn leader_heartbeat_timeout_ns(&self) -> i64;

    /// `ctx.leaderHeartbeatIntervalNs()`.
    fn leader_heartbeat_interval_ns(&self) -> i64;

    /// `ctx.electionTimeoutNs()`.
    fn election_timeout_ns(&self) -> i64;

    /// `ctx.electionStatusIntervalNs()`.
    fn election_status_interval_ns(&self) -> i64;

    /// The session id of the log publication `add_log_publication` created.
    ///
    /// `Election.java:1401` assigns the **return value** of
    /// `consensusModuleAgent.addLogPublication(..)` to `logSessionId`; the
    /// frozen [`Self::add_log_publication`] returns a work count instead (it was
    /// transcribed from the election's other calls), so the election reads the
    /// same number here. Added after the freeze, by the add-only rule.
    fn log_session_id(&self) -> i32;

    /// `ClusterMember.hasQuorumAtPosition(..)` over the host's member table
    /// (`Election.java:885-887`). A query on the host because the members live
    /// there.
    fn has_quorum_at_position(
        &mut self,
        leadership_term_id: i64,
        position: i64,
        now_ns: i64,
        timeout_ns: i64,
    ) -> bool;

    // ---- added by P3-5b: the member table and the log subscription ----
    //
    // The election keeps member ids, not members (module docs). Every question
    // and mutation it made on the array in `Election.java` is one method here.
    // The log subscription is the client's, so the states that add one ask the
    // host for a registration id instead of holding a `Subscription`.

    /// `resetMembers()` (`Election.java:1446-1451`): every member back to
    /// nothing, then this member at the term and append position the election
    /// holds.
    fn reset_members(&mut self, leadership_term_id: i64, append_position: i64);

    /// `ClusterMember.becomeCandidate` (`ClusterMember.java:947`).
    fn become_candidate(&mut self, candidate_term_id: i64, candidate_member_id: i32);

    /// `member.isBallotSent()` (`Election.java:784`).
    fn member_ballot_sent(&self, member_id: i32) -> bool;

    /// `member.isBallotSent(boolean)` (`Election.java:787`).
    fn set_member_ballot_sent(&mut self, member_id: i32, sent: bool);

    /// `follower.candidateTermId(..).vote(..)` (`Election.java:403-405`).
    fn set_member_vote(&mut self, member_id: i32, candidate_term_id: i64, vote: Option<bool>);

    /// `ClusterMember.isUnanimousCandidate` (`ClusterMember.java:1117`).
    fn unanimous_candidate(&self, candidate_member_id: i32, graceful_closed_leader_id: i32)
    -> bool;

    /// `ClusterMember.isQuorumCandidate` (`ClusterMember.java:1147`).
    fn quorum_candidate(&self, candidate_member_id: i32) -> bool;

    /// `ClusterMember.isUnanimousLeader` (`ClusterMember.java:981`).
    fn unanimous_leader(&self, candidate_term_id: i64, graceful_closed_leader_id: i32) -> bool;

    /// `ClusterMember.isQuorumLeader` (`ClusterMember.java:1015`).
    fn quorum_leader(&self, candidate_term_id: i64) -> bool;

    /// `null == member.publication()` (`Election.java:1238`).
    fn member_has_publication(&self, member_id: i32) -> bool;

    /// `ClusterMember.tryAddPublication` (`ClusterMember.java:1073-1098`),
    /// which `publishCanvassPosition` calls for a member with no publication
    /// (`Election.java:1238-1245`).
    ///
    /// # Errors
    ///
    /// The client's, when the publication cannot be made.
    fn try_add_member_publication(&mut self, member_id: i32) -> Result<(), ClusterError>;

    /// `ctx.consensusStreamId()` (`Election.java:1242`).
    fn consensus_stream_id(&self) -> i32;

    /// `ctx.appVersion()` (`Election.java:1295`).
    fn app_version(&self) -> i32;

    /// `ctx.fileSyncLevel()` (`Election.java:1520`).
    fn file_sync_level(&self) -> i32;

    /// `ctx.logChannel()` (`Election.java:1346`, `:1360`).
    fn log_channel(&self) -> &str;

    /// `ctx.logStreamId()` (`Election.java:1382`).
    fn log_stream_id(&self) -> i32;

    /// `ctx.isLogMdc()` (`Election.java:1354`).
    fn log_is_mdc(&self) -> bool;

    /// `thisMember.logEndpoint()` (`Election.java:1357`).
    fn member_log_endpoint(&self, member_id: i32) -> Option<String>;

    /// `thisMember.catchupEndpoint()` (`Election.java:1029`).
    fn member_catchup_endpoint(&self, member_id: i32) -> Option<String>;

    /// `leaderMember.archiveEndpoint()` (`Election.java:906`).
    fn member_archive_endpoint(&self, member_id: i32) -> Option<String>;

    /// `leaderMember.archiveResponseEndpoint()` (`Election.java:907`).
    fn member_archive_response_endpoint(&self, member_id: i32) -> Option<String>;

    /// `addFollowerSubscription()` (`Election.java:1366-1383`): create the
    /// manual-MDC subscription a follower joins the log through. Answers its
    /// registration id.
    ///
    /// # Errors
    ///
    /// The client's, when the subscription cannot be made.
    fn add_log_subscription(&mut self, log_session_id: i32) -> Result<i64, ClusterError>;

    /// `CloseHelper.close(logSubscription)` (`Election.java:674`).
    ///
    /// # Errors
    ///
    /// The client's.
    fn remove_log_subscription(&mut self, registration_id: i64) -> Result<(), ClusterError>;

    /// `logSubscription.addDestination(destination)` (`Election.java:1347`,
    /// `:1362`).
    ///
    /// # Errors
    ///
    /// The client's.
    fn add_log_destination(
        &mut self,
        registration_id: i64,
        destination: &str,
    ) -> Result<(), ClusterError>;

    /// `logSubscription.resolvedEndpoint()` (`Election.java:1032`).
    fn log_subscription_resolved_endpoint(&self, registration_id: i64) -> Option<String>;

    /// `ChannelEndpointStatus.ERRORED == logSubscription.channelStatus()`
    /// (`Election.java:1071`, `:1153`).
    fn log_subscription_channel_errored(&self, registration_id: i64) -> bool;

    /// `logSubscription.imageBySessionId(logSessionId)` (`Election.java:1062`,
    /// `:1138`).
    ///
    /// `&mut self` because the image is **handed over**
    /// (`Client::take_image`): the adapter that `try_join_log_as_follower`
    /// builds owns it, and the reference's second call in the same election is
    /// the same object.
    fn log_image_by_session(&mut self, registration_id: i64, session_id: i32) -> Option<Image>;

    /// `ctx.commitPositionCounter().getPlain()` (`Election.java:265`,
    /// `:456`, `:1100`).
    fn commit_position_value(&self) -> i64;

    /// `ctx.electionStateCounter().setRelease(newState.code())`
    /// (`Election.java:1433`): counter 207.
    fn set_election_state(&mut self, state: ElectionState);
}

/// Every method on [`ElectionHost`], in declaration order.
///
/// Exists only so `tests/election_surface.rs` can compare the trait against
/// the reference's call surface. **A new method must be added here too** —
/// that is the point: the test fails until the fixture and this list agree.
pub const HOST_METHODS: &[&str] = &[
    "set_role",
    "role",
    "state",
    "log_recording_id",
    "set_log_recording_id",
    "time_of_last_leader_update_ns",
    "quorum_position_bounded_by_leader_log",
    "update_member_log_position",
    "update_leader_position",
    "publish_commit_position",
    "join_log_as_leader",
    "try_join_log_as_follower",
    "add_log_publication",
    "new_log_replay",
    "new_log_replication",
    "truncate_log_entry",
    "append_new_leadership_term_event",
    "prepare_for_new_leadership",
    "catchup_initiated",
    "catchup_poll",
    "is_catchup_near_live",
    "track_catchup_completion",
    "stop_all_catchups",
    "live_log_destination",
    "set_live_log_destination",
    "catchup_log_destination",
    "set_catchup_log_destination",
    "await_local_sockets_closed",
    "election_complete",
    "poll_archive_events",
];

/// The methods `P3-5a-beta` added on top of the frozen 30. Kept separate so the
/// surface test can still assert that the **frozen** set is exactly the
/// reference's `consensusModuleAgent.*` calls, while these are the `ctx`
/// accessors the reference reaches through a different object.
/// The methods `P3-5b` added: the member table and the log subscription.
/// `tests/election_surface.rs` pins the frozen 30 separately; this list exists so
/// a later slice knows what is 5b's rather than the reference call surface's.
pub const HOST_5B_METHODS: &[&str] = &[
    "reset_members",
    "become_candidate",
    "member_ballot_sent",
    "set_member_ballot_sent",
    "set_member_vote",
    "unanimous_candidate",
    "quorum_candidate",
    "unanimous_leader",
    "quorum_leader",
    "member_has_publication",
    "try_add_member_publication",
    "consensus_stream_id",
    "app_version",
    "file_sync_level",
    "log_channel",
    "log_stream_id",
    "log_is_mdc",
    "member_log_endpoint",
    "member_catchup_endpoint",
    "member_archive_endpoint",
    "member_archive_response_endpoint",
    "add_log_subscription",
    "remove_log_subscription",
    "add_log_destination",
    "log_subscription_resolved_endpoint",
    "log_subscription_channel_errored",
    "log_image_by_session",
    "commit_position_value",
    "set_election_state",
];

/// The methods `P3-5a-beta` added on top of the frozen 30. Kept separate so the
/// surface test can still assert that the **frozen** set is exactly the
/// reference's `consensusModuleAgent.*` calls, while these are the `ctx`
/// accessors the reference reaches through a different object.
pub const HOST_CTX_METHODS: &[&str] = &[
    "node_state",
    "recording_log",
    "now_ns",
    "epoch_ms",
    "timestamp_from_nanos",
    "nanos_from_timestamp",
    "appointed_leader_id",
    "startup_canvass_timeout_ns",
    "leader_heartbeat_timeout_ns",
    "leader_heartbeat_interval_ns",
    "election_timeout_ns",
    "election_status_interval_ns",
    "log_session_id",
    "has_quorum_at_position",
];
