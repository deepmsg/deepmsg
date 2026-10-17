//! The election state machine (18 states, 8 inbound handlers) and its seams.
//!
//! `P3-5a-α` froze the seams and the state space; `P3-5a-β` fills in `INIT`,
//! the single-member short circuit and `LEADER_LOG_REPLICATION`..`LEADER_READY`;
//! `P3-5b` does the other thirteen states and the eight handlers.
//!
//! # Shape, and the four things it does differently from the reference
//!
//! The reference's `Election` holds the consensus module, its `Context`, the
//! member table and the publisher as fields (`Election.java:75-84`). Here it
//! holds none of them, because `host` and `sender` arrive per call:
//!
//! 1. **The member table lives in the host**, so the election keeps member
//!    *ids* and asks the host about positions. The reference stores
//!    `ClusterMember[]`, the by-id map and `thisMember`; two of its mutations
//!    (`updateMemberLogPosition`, `trackCatchupCompletion`) are already host
//!    methods.
//! 2. **The subscription stays in the client.** The reference keeps a
//!    `Subscription` field (`:107`); here it is a registration id, because
//!    deepmsg's client owns its subscriptions and hands out images on request.
//! 3. **The publisher is a per-call `sender`**, not a field, so a test can
//!    record sends without a `ConsensusModuleAgent`.
//! 4. **The RNG is a field.** The reference calls `ctx.random()`
//!    (`Election.java:723`); here a seed is injected, so the backoff in
//!    `CANVASS -> NOMINATE` is reproducible — the reference's tests cannot do
//!    that and have to steer around it.
//!
//! Everything else keeps the reference's names and grouping.

// α ships the shape before any of it is used; β is the first reader.
#![allow(dead_code)]

pub mod host;
pub mod sender;
pub mod state;

pub use host::{ElectionHost, HOST_METHODS};
pub use sender::{ConsensusSender, SENDER_METHODS};
pub use state::ElectionState;

use crate::handles::{LogReplay, RecordingReplication};

/// A deterministic source for the canvass backoff.
///
/// The reference uses `ctx.random()` for exactly one thing — picking the
/// nomination deadline, `(long)(random() * (electionTimeoutNs / 2.0))` at
/// `Election.java:723-724`. A fixed seed here makes every election test
/// reproducible; the production seed comes from the cluster clock.
#[derive(Clone, Copy, Debug)]
pub struct ElectionRng {
    state: u64,
}

impl ElectionRng {
    /// Seed the generator. Production passes the epoch clock; tests pass a
    /// constant.
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// The next draw in `[0, 1)`, mirroring `java.util.Random.nextDouble()`'s
    /// *contract shape* — not its bit sequence, which no test may depend on.
    pub fn next_f64(&mut self) -> f64 {
        // xorshift64*, chosen for being checkable by eye rather than good.
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        (self.state >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// One election, between `enterElection` and `electionComplete`.
///
/// Field grouping is frozen at `P3-5a-α`; β and 5b fill in the behaviour.
/// The groups, top to bottom: immutable-for-one-election, the five-tuple being
/// reconciled, progress, in-flight work, deadlines.
pub struct Election {
    // ---- immutable for the life of one election (ctor, `:110-150`) ----
    /// Whether the node is starting up, not re-electing (`:112`).
    is_node_startup: bool,
    /// `isExtendedCanvass`, seeded from `isNodeStartup` (`:124`).
    is_extended_canvass: bool,
    /// The leader that shut down cleanly, or a null id (`:113`).
    graceful_closed_leader_id: i32,
    /// `:131` — also the initial `logLeadershipTermId` and `candidateTermId`.
    initial_log_leadership_term_id: i64,
    /// `:133`.
    initial_term_base_log_position: i64,
    /// `:140` — one day in the past, so the first heartbeat check fires.
    initial_time_of_last_update_ns: i64,
    /// `thisMember.id()` (`:135`).
    this_member_id: i32,
    /// The eligible members, in the reference's array order (`:134`).
    ///
    /// Ids, not members: see the module note. Order matters — `CANVASS`
    /// publishes in it and the ballots are indexed by it.
    member_ids: Vec<i32>,
    rng: ElectionRng,

    // ---- the five-tuple being reconciled (spec §1.2) ----
    /// The term this member is in (`:136`).
    leadership_term_id: i64,
    /// The term of the last entry in the log (`:132`).
    log_leadership_term_id: i64,
    /// Applied position (`:127`).
    log_position: i64,
    /// Written position (`:128`).
    append_position: i64,
    /// What the leader last told us is committed (`:130`).
    notified_commit_position: i64,

    // ---- progress ----
    state: ElectionState,
    /// `:144` — bumped before every vote we cast or request.
    candidate_term_id: i64,
    /// `:146` — `INIT` uses it to tell "first INIT" from "retry".
    is_first_init: bool,
    /// `:145`.
    is_leader_startup: bool,
    /// `leaderMember`, as an id; `-1`/null while none (`:144`).
    leader_member_id: i32,
    /// `:143` — determined when the log publication is created.
    log_session_id: i32,
    /// `:142`.
    leader_recording_id: i64,
    /// `:129` — where a follower must join; `-1` when not catching up.
    catchup_join_position: i64,
    /// `:138`.
    replication_leadership_term_id: i64,
    /// `:139`.
    replication_stop_position: i64,
    /// `:141`.
    replication_term_base_log_position: i64,
    /// `:143`-adjacent: the last commit position we published (`:149`).
    last_published_commit_position: i64,
    /// The last append position we published (`:150`).
    last_published_append_position: i64,

    // ---- in-flight work, owned here, advanced through the host ----
    /// The log subscription's registration id, or none (`:107`).
    log_subscription_id: Option<i64>,
    /// Our own tail replay, when replaying (`:108`).
    log_replay: Option<LogReplay>,
    /// An archive-to-archive replication, when catching up by copying (`:109`).
    log_replication: Option<RecordingReplication>,

    // ---- deadlines (`:85-92`, `:140`) ----
    /// `:86`.
    time_of_last_state_change_ns: i64,
    /// `:87`.
    time_of_last_update_ns: i64,
    /// `:88`.
    time_of_last_commit_position_update_ns: i64,
    /// `:89` — when `NOMINATE` gives up waiting and asks for votes.
    nomination_deadline_ns: i64,
    /// `:137` — how long a replication may stall before it is a `Timeout`.
    replication_deadline_ns: i64,
}
