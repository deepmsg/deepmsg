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

// ---------------------------------------------------------------------------
// P3-5a-beta: INIT, and the leader states 5..8.
//
// Only these five states exist so far. Everything else — CANVASS, the two
// ballots, and every follower state — is P3-5b, and calling `do_work` in
// `CANVASS` returns an error rather than pretending.
// ---------------------------------------------------------------------------

use crate::error::ClusterError;

impl Election {
    /// Start an election. `Election(..)` (`Election.java:110-150`).
    ///
    /// The constructor's side effects are kept: the initial deadline is one day
    /// in the past so the first heartbeat check fires, and `candidate_term_id`
    /// starts at the term the log is at.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        this_member_id: i32,
        member_ids: Vec<i32>,
        is_node_startup: bool,
        graceful_closed_leader_id: i32,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        append_position: i64,
        now_ns: i64,
        rng_seed: u64,
    ) -> Self {
        let initial_time_of_last_update_ns = now_ns - 86_400_000_000_000;
        Self {
            is_node_startup,
            is_extended_canvass: is_node_startup,
            graceful_closed_leader_id,
            initial_log_leadership_term_id: leadership_term_id,
            initial_term_base_log_position: term_base_log_position,
            initial_time_of_last_update_ns,
            this_member_id,
            member_ids,
            rng: ElectionRng::new(rng_seed),
            leadership_term_id,
            log_leadership_term_id: leadership_term_id,
            log_position,
            append_position,
            notified_commit_position: 0,
            state: ElectionState::Init,
            candidate_term_id: leadership_term_id,
            is_first_init: true,
            is_leader_startup: false,
            leader_member_id: -1,
            log_session_id: -1,
            leader_recording_id: -1,
            catchup_join_position: -1,
            replication_leadership_term_id: -1,
            replication_stop_position: -1,
            replication_term_base_log_position: 0,
            last_published_commit_position: 0,
            last_published_append_position: 0,
            log_subscription_id: None,
            log_replay: None,
            log_replication: None,
            time_of_last_state_change_ns: initial_time_of_last_update_ns,
            time_of_last_update_ns: initial_time_of_last_update_ns,
            time_of_last_commit_position_update_ns: initial_time_of_last_update_ns,
            nomination_deadline_ns: 0,
            replication_deadline_ns: 0,
        }
    }

    /// The state the machine is in.
    pub fn state(&self) -> ElectionState {
        self.state
    }

    /// The term this member is in.
    pub fn leadership_term_id(&self) -> i64 {
        self.leadership_term_id
    }

    /// The leader this election settled on, or `-1`.
    pub fn leader_member_id(&self) -> i32 {
        self.leader_member_id
    }

    /// One duty cycle. `Election.doWork(nowNs)` (`:152-260`).
    ///
    /// # Errors
    ///
    /// [`ClusterError::Cluster`] for a state this slice does not implement —
    /// returning something plausible would be worse than saying so.
    pub fn do_work(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
    ) -> Result<usize, ClusterError> {
        let now_ns = host.now_ns();
        match self.state {
            ElectionState::Init => self.init(host, now_ns),
            ElectionState::LeaderLogReplication => {
                self.leader_log_replication(host, sender, now_ns)
            }
            ElectionState::LeaderReplay => self.leader_replay(host, sender, now_ns),
            ElectionState::LeaderInit => self.leader_init(host, now_ns),
            ElectionState::LeaderReady => self.leader_ready(host, sender, now_ns),
            other => Err(ClusterError::Cluster(format!(
                "state {other:?} is P3-5b, not implemented yet"
            ))),
        }
    }

    /// `init` (`:653-694`).
    fn init(&mut self, host: &mut dyn ElectionHost, now_ns: i64) -> Result<usize, ClusterError> {
        if self.is_first_init {
            self.is_first_init = false;
            if !self.is_node_startup {
                self.prepare_for_new_leadership(host, now_ns)?;
            }
        } else {
            self.prepare_for_new_leadership(host, now_ns)?;
            self.log_session_id = -1;
        }

        self.notified_commit_position = 0;
        let persisted = host.node_state().candidate_term().candidate_term_id;
        self.candidate_term_id = persisted.max(self.leadership_term_id);

        if self.member_ids.len() == 1 && self.this_member_id == self.member_ids[0] {
            // The reference's short circuit (`:683-691`): nominate self, win
            // the ballot, and go straight to LEADER_LOG_REPLICATION.
            self.candidate_term_id += 1;
            let term = self.candidate_term_id;
            let position = self.log_position;
            let epoch_ms = host.epoch_ms();
            host.node_state()
                .update_candidate_term_id(term, position, epoch_ms)
                .map_err(|error| ClusterError::Cluster(error.to_string()))?;
            self.leader_member_id = self.this_member_id;
            self.leadership_term_id = term;
            self.transition(ElectionState::LeaderLogReplication, now_ns, host)?;
        } else {
            self.transition(ElectionState::Canvass, now_ns, host)?;
        }
        Ok(1)
    }

    /// `prepareForNewLeadership` (`Election.java:1360`, on the agent).
    fn prepare_for_new_leadership(
        &mut self,
        host: &mut dyn ElectionHost,
        now_ns: i64,
    ) -> Result<(), ClusterError> {
        self.log_position = host.prepare_for_new_leadership(self.log_position, now_ns)?;
        Ok(())
    }

    /// `leaderLogReplication` (`:811-828`).
    fn leader_log_replication(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        let mut work = 0;
        self.time_of_last_update_ns = now_ns;

        // `thisMember.logPosition(appendPosition).timeOfLastAppendPositionNs(nowNs)`
        // (`Election.java:813`). Without this the member's own position stays at
        // zero, and a node restarting onto a non-empty log never reaches a
        // quorum at its append position.
        host.update_member_log_position(
            self.this_member_id,
            self.leadership_term_id,
            self.append_position,
        );

        let quorum_position =
            host.quorum_position_bounded_by_leader_log(self.append_position, now_ns);
        work += self.publish_new_leadership_term_on_interval(host, sender, quorum_position, now_ns);
        work += self.publish_commit_position_on_interval(host, quorum_position, now_ns);

        if quorum_position >= self.append_position {
            work += 1;
            self.transition(ElectionState::LeaderReplay, now_ns, host)?;
        }
        Ok(work)
    }

    /// `leaderReplay` (`:830-866`).
    fn leader_replay(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        let mut work = 0;
        match self.log_replay {
            None => {
                if self.log_position < self.append_position {
                    self.log_replay =
                        Some(host.new_log_replay(self.log_position, self.append_position)?);
                } else {
                    self.transition(ElectionState::LeaderInit, now_ns, host)?;
                }
                work += 1;
                self.is_leader_startup = self.is_node_startup;
                self.log_position = self.append_position;
                // The same member update as `leaderLogReplication`
                // (`Election.java:844-849`).
                host.update_member_log_position(
                    self.this_member_id,
                    self.leadership_term_id,
                    self.append_position,
                );
            }
            Some(_) => {
                // The replay is advanced by the host's own poll (the archive
                // replay poll, `ConsensusModuleAgent.java:1845`); the election
                // only watches for it being done. P3-5b's `LogReplay` will carry
                // `is_done`; until then a replay is treated as complete.
                self.log_replay = None;
                self.log_position = self.append_position;
                work += 1;
                self.transition(ElectionState::LeaderInit, now_ns, host)?;
            }
        }

        let quorum_position =
            host.quorum_position_bounded_by_leader_log(self.append_position, now_ns);
        work += self.publish_new_leadership_term_on_interval(host, sender, quorum_position, now_ns);
        work += self.publish_commit_position_on_interval(host, quorum_position, now_ns);
        Ok(work)
    }

    /// `leaderInit` (`:868-875`).
    fn leader_init(
        &mut self,
        host: &mut dyn ElectionHost,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        host.join_log_as_leader(
            self.leadership_term_id,
            self.log_position,
            self.log_session_id,
            self.is_leader_startup,
        )?;
        self.update_recording_log(host, now_ns)?;
        self.transition(ElectionState::LeaderReady, now_ns, host)?;
        Ok(1)
    }

    /// `leaderReady` (`:877-895`).
    fn leader_ready(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        let quorum_position =
            host.quorum_position_bounded_by_leader_log(self.append_position, now_ns);
        let mut work =
            host.update_leader_position(now_ns, self.append_position, quorum_position)?;
        work += self.publish_new_leadership_term_on_interval(host, sender, quorum_position, now_ns);

        let heartbeat_timeout = host.leader_heartbeat_timeout_ns();
        let log_position = self.log_position;
        let term = self.leadership_term_id;
        // The member table lives in the host, so the quorum question is asked
        // there rather than recomputed here from a partial copy.
        let has_quorum = host.has_quorum_at_position(term, log_position, now_ns, heartbeat_timeout);
        if has_quorum && host.append_new_leadership_term_event(now_ns)? {
            host.election_complete(now_ns)?;
            self.transition(ElectionState::Closed, now_ns, host)?;
            work += 1;
        }
        Ok(work)
    }

    /// `updateRecordingLog` (`:1523-1528`).
    fn update_recording_log(
        &mut self,
        host: &mut dyn ElectionHost,
        now_ns: i64,
    ) -> Result<(), ClusterError> {
        let recording_id = host.log_recording_id();
        if recording_id != -1 {
            let service_id = if self.leader_member_id == -1 {
                -1
            } else {
                self.leader_member_id
            };
            let (term, position) = (self.leadership_term_id, self.log_position);
            let timestamp = host.timestamp_from_nanos(now_ns);
            host.recording_log()
                .append_term(
                    term,
                    position,
                    position,
                    timestamp,
                    recording_id,
                    service_id,
                )
                .map_err(|error| ClusterError::Cluster(error.to_string()))?;
        }
        Ok(())
    }

    /// `publishNewLeadershipTermOnInterval` (`:1198`).
    fn publish_new_leadership_term_on_interval(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        quorum_position: i64,
        now_ns: i64,
    ) -> usize {
        let interval = host.leader_heartbeat_interval_ns();
        if now_ns < self.time_of_last_update_ns + interval {
            return 0;
        }
        self.time_of_last_update_ns = now_ns;
        let leadership_term_id = self.leadership_term_id;
        let log_leadership_term_id = self.log_leadership_term_id;
        let log_position = self.log_position;
        let leader_id = self.this_member_id;
        let this_id = self.this_member_id;
        for member_id in self.member_ids.iter().copied() {
            if member_id == this_id {
                continue;
            }
            sender.new_leadership_term(
                member_id,
                log_leadership_term_id,
                leadership_term_id,
                self.initial_term_base_log_position,
                log_position,
                leadership_term_id,
                self.initial_term_base_log_position,
                log_position,
                quorum_position,
                self.leader_recording_id,
                now_ns,
                leader_id,
                self.log_session_id,
                0,
                self.is_node_startup,
            );
        }
        1
    }

    /// `publishCommitPositionOnInterval` (`:1212`).
    fn publish_commit_position_on_interval(
        &mut self,
        host: &mut dyn ElectionHost,
        quorum_position: i64,
        now_ns: i64,
    ) -> usize {
        let interval = host.leader_heartbeat_interval_ns();
        if now_ns < self.time_of_last_commit_position_update_ns + interval
            && quorum_position == self.last_published_commit_position
        {
            return 0;
        }
        let term = self.leadership_term_id;
        if host.publish_commit_position(quorum_position, term).is_err() {
            return 0;
        }
        self.last_published_commit_position = quorum_position;
        self.time_of_last_commit_position_update_ns = now_ns;
        1
    }

    /// `state(newState, nowNs, reason)` (`:1385-1438`): the transition **and**
    /// its side effects. The role axis is driven from here and nowhere else.
    fn transition(
        &mut self,
        next: ElectionState,
        now_ns: i64,
        host: &mut dyn ElectionHost,
    ) -> Result<(), ClusterError> {
        self.state = next;
        self.time_of_last_state_change_ns = now_ns;
        match next {
            ElectionState::Canvass => host.set_role(crate::state::Role::Follower),
            ElectionState::CandidateBallot => host.set_role(crate::state::Role::Candidate),
            ElectionState::LeaderLogReplication => {
                host.set_role(crate::state::Role::Leader);
                // The log publication is created here, which is what fixes the
                // log session id for the term (`Election.java:1398-1400`).
                // The reference takes the session id from the call's return
                // value; the frozen seam returns a work count, so it is read
                // back through the host accessor.
                host.add_log_publication(self.append_position)?;
                self.log_session_id = host.log_session_id();
            }
            ElectionState::Closed => {}
            _ => {}
        }
        Ok(())
    }
}

/// How long the quorum scratch array must be for `member_count`.
pub const fn quorum_scratch_len(member_count: usize) -> usize {
    (member_count >> 1) + 1
}
