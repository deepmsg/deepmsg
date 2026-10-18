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

use deepmsg_cluster_common::member;

use crate::handles::{LogReplay, RecordingReplication, RecordingSignal};

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
    /// The replication position the last turn saw, for the progress clock
    /// (`RecordingReplication.poll`'s `progressDeadlineNs`).
    replication_progress_position: i64,
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
            replication_progress_position: -1,
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

    /// What the leader last told this member is committed (`:648-651`).
    pub fn notified_commit_position(&self) -> i64 {
        self.notified_commit_position
    }

    /// The log position this election started from or has reached
    /// (`Election.logPosition()`), which a stalled catchup reports.
    pub fn log_position(&self) -> i64 {
        self.log_position
    }

    /// The archive-to-archive replication this election is waiting on, if any.
    ///
    /// The reference reads `logReplication` directly from its own test
    /// (`ElectionTest.java`'s replication cases); this is the same door, and
    /// the agent's 6b half will use it to drive the replication.
    pub fn log_replication(&self) -> Option<&RecordingReplication> {
        self.log_replication.as_ref()
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
            ElectionState::Canvass => self.canvass(host, sender, now_ns),
            ElectionState::Nominate => self.nominate(host, sender, now_ns),
            ElectionState::CandidateBallot => self.candidate_ballot(host, sender, now_ns),
            ElectionState::FollowerBallot => self.follower_ballot(host, now_ns),
            ElectionState::LeaderLogReplication => {
                self.leader_log_replication(host, sender, now_ns)
            }
            ElectionState::LeaderReplay => self.leader_replay(host, sender, now_ns),
            ElectionState::LeaderInit => self.leader_init(host, now_ns),
            ElectionState::LeaderReady => self.leader_ready(host, sender, now_ns),
            ElectionState::FollowerLogReplication => {
                self.follower_log_replication(host, sender, now_ns)
            }
            ElectionState::FollowerReplay => self.follower_replay(host, sender, now_ns),
            ElectionState::FollowerCatchupInit => self.follower_catchup_init(host, sender, now_ns),
            ElectionState::FollowerCatchupAwait => {
                self.follower_catchup_await(host, sender, now_ns)
            }
            ElectionState::FollowerCatchup => self.follower_catchup(host, sender, now_ns),
            ElectionState::FollowerLogInit => self.follower_log_init(host, sender, now_ns),
            ElectionState::FollowerLogAwait => self.follower_log_await(host, sender, now_ns),
            ElectionState::FollowerReady => self.follower_ready(host, sender, now_ns),
            ElectionState::Closed => Ok(0),
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
            self.stop_log_replication();
            self.stop_catchup(host)?;

            self.prepare_for_new_leadership(host, now_ns)?;
            self.log_session_id = -1;
            self.stop_replay();

            if let Some(registration_id) = self.log_subscription_id.take() {
                host.remove_log_subscription(registration_id)?;
                host.await_local_sockets_closed(registration_id)?;
            }
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
        match self.log_replay.take() {
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
            Some(replay) => {
                // The replay is advanced by the host's own poll (the agent's
                // `poll_log_replay`, `ConsensusModuleAgent.java:1845`); the
                // election watches `isDone` (`Election.java:855-866`).
                if replay.is_done() {
                    self.log_position = self.append_position;
                    work += 1;
                    self.transition(ElectionState::LeaderInit, now_ns, host)?;
                } else {
                    self.log_replay = Some(replay);
                    work += 1;
                }
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
            host.set_leadership_term_id(self.leadership_term_id);
            host.set_leader_member_id(self.this_member_id);
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
        let term = self.leadership_term_id;
        let position = self.log_position;
        // `Election.java:1525`: `ensureRecordingLogCoherent(leadershipTermId,
        // logPosition, NULL_VALUE, nowNs)` — the log position is the **new
        // term's base**, and the commit position is deliberately not moved.
        self.ensure_recording_log_coherent(host, term, Some(position), None, now_ns)?;
        self.log_leadership_term_id = term;
        Ok(())
    }

    /// `updateRecordingLogForReplication` (`:1529-1537`).
    fn update_recording_log_for_replication(
        &mut self,
        host: &mut dyn ElectionHost,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        now_ns: i64,
    ) -> Result<(), ClusterError> {
        self.ensure_recording_log_coherent(
            host,
            leadership_term_id,
            Some(term_base_log_position),
            Some(log_position),
            now_ns,
        )?;
        self.log_leadership_term_id = leadership_term_id;
        Ok(())
    }

    /// `ensureRecordingLogCoherent` (`:1474-1521`).
    fn ensure_recording_log_coherent(
        &mut self,
        host: &mut dyn ElectionHost,
        leadership_term_id: i64,
        term_base_log_position: Option<i64>,
        log_position: Option<i64>,
        now_ns: i64,
    ) -> Result<(), ClusterError> {
        let recording_id = host.log_recording_id();
        if recording_id == -1 {
            // The reference's early return: a replication can have an initial
            // append position but no recording of its own yet.
            return Ok(());
        }

        let timestamp = host.timestamp_from_nanos(now_ns);
        let file_sync_level = host.file_sync_level();
        let initial_term = self.initial_log_leadership_term_id;
        let initial_base = self.initial_term_base_log_position;
        let base = term_base_log_position.unwrap_or(initial_base);
        host.recording_log()
            .ensure_coherent(
                recording_id,
                initial_term,
                initial_base,
                leadership_term_id,
                Some(base),
                log_position,
                timestamp,
                file_sync_level,
            )
            .map_err(|error| ClusterError::Cluster(error.to_string()))?;
        Ok(())
    }

    /// `canvass` (`:700-730`).
    fn canvass(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        let mut work = 0;
        let deadline_ns = if self.is_extended_canvass {
            self.time_of_last_state_change_ns + host.startup_canvass_timeout_ns()
        } else {
            host.time_of_last_leader_update_ns() + host.leader_heartbeat_timeout_ns()
        };

        if now_ns - self.time_of_last_update_ns >= host.election_status_interval_ns() {
            self.time_of_last_update_ns = now_ns;
            self.publish_canvass_position(host, sender)?;
            work += 1;
        }

        let appointed = host.appointed_leader_id();
        if appointed != -1 && appointed != self.this_member_id {
            return Ok(work);
        }

        if host.unanimous_candidate(self.this_member_id, self.graceful_closed_leader_id)
            || (now_ns >= deadline_ns && host.quorum_candidate(self.this_member_id))
        {
            let half = host.election_timeout_ns() >> 1;
            #[allow(clippy::cast_possible_truncation)]
            let delay_ns = (self.rng.next_f64() * half as f64) as i64;
            self.nomination_deadline_ns = now_ns + delay_ns;
            self.transition(ElectionState::Nominate, now_ns, host)?;
            work += 1;
        }

        Ok(work)
    }

    /// `nominate` (`:732-752`).
    fn nominate(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        if now_ns >= self.nomination_deadline_ns {
            let next = self.candidate_term_id + 1;
            let position = self.log_position;
            let epoch_ms = host.epoch_ms();
            host.node_state()
                .propose_max_candidate_term_id(next, position, epoch_ms)
                .map_err(|error| ClusterError::Cluster(error.to_string()))?;
            self.candidate_term_id = host.node_state().candidate_term().candidate_term_id;
            host.become_candidate(self.candidate_term_id, self.this_member_id);
            self.transition(ElectionState::CandidateBallot, now_ns, host)?;
            Ok(1)
        } else if now_ns - self.time_of_last_update_ns >= host.election_status_interval_ns() {
            self.time_of_last_update_ns = now_ns;
            self.publish_canvass_position(host, sender)?;
            Ok(1)
        } else {
            Ok(0)
        }
    }

    /// `candidateBallot` (`:754-794`).
    fn candidate_ballot(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        let mut work = 0;
        let candidate_term_id = self.candidate_term_id;

        if host.unanimous_leader(candidate_term_id, self.graceful_closed_leader_id) {
            self.leader_member_id = self.this_member_id;
            self.leadership_term_id = candidate_term_id;
            self.transition(ElectionState::LeaderLogReplication, now_ns, host)?;
            work += 1;
        } else if now_ns >= self.time_of_last_state_change_ns + host.election_timeout_ns() {
            if host.quorum_leader(candidate_term_id) {
                self.leader_member_id = self.this_member_id;
                self.leadership_term_id = candidate_term_id;
                self.transition(ElectionState::LeaderLogReplication, now_ns, host)?;
            } else {
                self.transition(ElectionState::Canvass, now_ns, host)?;
            }
            work += 1;
        } else {
            for index in 0..self.member_ids.len() {
                let member_id = self.member_ids[index];
                if !host.member_ballot_sent(member_id) {
                    work += 1;
                    let sent = sender.request_vote(
                        member_id,
                        self.log_leadership_term_id,
                        self.append_position,
                        candidate_term_id,
                        self.this_member_id,
                    );
                    host.set_member_ballot_sent(member_id, sent);
                }
            }
        }

        Ok(work)
    }

    /// `followerBallot` (`:796-807`).
    fn follower_ballot(
        &mut self,
        host: &mut dyn ElectionHost,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        if now_ns >= self.time_of_last_state_change_ns + host.election_timeout_ns() {
            self.transition(ElectionState::Canvass, now_ns, host)?;
            return Ok(1);
        }
        Ok(0)
    }

    /// `followerLogReplication` (`:897-958`).
    fn follower_log_replication(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        let mut work = 0;

        if self.log_replication.is_none() {
            if self.append_position < self.replication_stop_position {
                let leader = self.leader_member_id;
                let archive = host.member_archive_endpoint(leader).unwrap_or_default();
                let response = host.member_archive_response_endpoint(leader);
                self.log_replication = Some(host.new_log_replication(
                    &archive,
                    response.as_deref(),
                    self.leader_recording_id,
                    self.replication_stop_position,
                    now_ns,
                )?);
                self.replication_deadline_ns = now_ns + host.leader_heartbeat_timeout_ns();
                self.replication_progress_position = -1;
                work += 1;
            } else {
                let term = self.replication_leadership_term_id;
                let base = self.replication_term_base_log_position;
                let stop = self.replication_stop_position;
                self.update_recording_log_for_replication(host, term, base, stop, now_ns)?;
                self.transition(ElectionState::Canvass, now_ns, host)?;
            }
            return Ok(work);
        }

        work += host.poll_archive_events();
        let (replication_done, position) = {
            let replication = self.log_replication.as_ref().expect("just checked");
            replication.poll(now_ns);
            (
                replication.has_replication_ended() && replication.has_stopped(),
                replication.position(),
            )
        };
        // `RecordingReplication.poll`'s progress clock (`:185-201`): a position
        // that moved buys another heartbeat timeout, and one that has not
        // moved is the replication failing — whether or not it ever ended.
        // This is where the check lives in this build (plan Q1: the handle's
        // `poll` cannot read the agent, so the election that drives it keeps
        // the deadline).
        if position > self.replication_progress_position {
            self.replication_progress_position = position;
            self.replication_deadline_ns = now_ns + host.leader_heartbeat_timeout_ns();
        }

        // The last position of the leadership is published after the completion
        // check (`Election.java:925-928`), so a replication that ends this turn
        // still sends it.
        work += self.publish_follower_replication_position(host, sender, now_ns);

        if replication_done {
            if self.notified_commit_position >= self.append_position {
                self.append_position = position;
                self.stop_log_replication();
                let term = self.replication_leadership_term_id;
                let base = self.replication_term_base_log_position;
                let stop = self.replication_stop_position;
                self.update_recording_log_for_replication(host, term, base, stop, now_ns)?;
                self.transition(ElectionState::Canvass, now_ns, host)?;
                work += 1;
            } else if now_ns >= self.replication_deadline_ns {
                return Err(ClusterError::Timeout(
                    "timeout awaiting commit position".to_owned(),
                ));
            }
        } else if now_ns >= self.replication_deadline_ns {
            return Err(ClusterError::Timeout(
                "log replication has not progressed".to_owned(),
            ));
        }

        Ok(work)
    }

    /// `followerReplay` (`:960-1018`).
    fn follower_replay(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        let mut work = 0;

        if self.log_replay.is_none() {
            if self.log_position < self.append_position {
                if self.notified_commit_position == 0 {
                    return Ok(self.publish_follower_append_position(host, sender, now_ns));
                }
                if self.log_position >= self.notified_commit_position {
                    self.transition(ElectionState::Canvass, now_ns, host)?;
                    return Ok(work);
                }
                let start = self.log_position;
                let stop = self.append_position.min(self.notified_commit_position);
                self.log_replay = Some(host.new_log_replay(start, stop)?);
                work += 1;
            } else {
                let next = if self.catchup_join_position != -1 {
                    ElectionState::FollowerCatchupInit
                } else {
                    ElectionState::FollowerLogInit
                };
                self.transition(next, now_ns, host)?;
                work += 1;
            }
            return Ok(work);
        }

        let (done, position) = {
            let replay = self.log_replay.as_ref().expect("just checked");
            (replay.is_done(), replay.position())
        };
        if done {
            self.log_position = position;
            self.stop_replay();

            if self.log_position == self.append_position {
                let next = if self.catchup_join_position != -1 {
                    ElectionState::FollowerCatchupInit
                } else {
                    ElectionState::FollowerLogInit
                };
                self.transition(next, now_ns, host)?;
            } else {
                self.transition(ElectionState::Canvass, now_ns, host)?;
            }
        }

        Ok(work)
    }

    /// `followerCatchupInit` (`:1020-1056`).
    fn follower_catchup_init(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        if self.log_subscription_id.is_none() {
            let registration_id = host.add_log_subscription(self.log_session_id)?;
            self.log_subscription_id = Some(registration_id);
            self.add_catchup_log_destination(host)?;
        }

        let endpoint = host
            .member_catchup_endpoint(self.this_member_id)
            .unwrap_or_default();
        let catchup_endpoint = if endpoint.ends_with(":0") {
            match self.log_subscription_id.and_then(|registration_id| {
                host.log_subscription_resolved_endpoint(registration_id)
            }) {
                Some(resolved) => resolved.rfind(':').map(|index| {
                    format!("{}{}", &endpoint[..endpoint.len() - 2], &resolved[index..])
                }),
                None => None,
            }
        } else {
            Some(endpoint)
        };

        if let Some(endpoint) = catchup_endpoint {
            if sender.catchup_position(
                self.leader_member_id,
                self.leadership_term_id,
                self.log_position,
                self.this_member_id,
                &endpoint,
            ) {
                self.time_of_last_update_ns = now_ns;
                host.catchup_initiated(now_ns)?;
                self.transition(ElectionState::FollowerCatchupAwait, now_ns, host)?;
                return Ok(1);
            }
        }

        if now_ns >= self.time_of_last_state_change_ns + host.leader_heartbeat_timeout_ns() {
            return Err(ClusterError::Timeout(
                "failed to send catchup position".to_owned(),
            ));
        }

        Ok(1)
    }

    /// `followerCatchupAwait` (`:1058-1087`).
    fn follower_catchup_await(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        let _ = sender;
        let mut work = 0;
        let Some(registration_id) = self.log_subscription_id else {
            return Ok(0);
        };

        if let Some(image) = host.log_image_by_session(registration_id, self.log_session_id) {
            self.verify_log_join_position("followerCatchupAwait", image.join_position())?;
            let startup = self.is_leader_startup;
            if host.try_join_log_as_follower(image, startup, now_ns)? {
                self.transition(ElectionState::FollowerCatchup, now_ns, host)?;
                work += 1;
            } else if host.log_subscription_channel_errored(registration_id) {
                return Err(ClusterError::Cluster(
                    "failed to add catchup log as follower".to_owned(),
                ));
            } else if now_ns
                >= self.time_of_last_state_change_ns + host.leader_heartbeat_timeout_ns()
            {
                return Err(ClusterError::Timeout(
                    "failed to join catchup log as follower".to_owned(),
                ));
            }
        } else if now_ns >= self.time_of_last_state_change_ns + host.leader_heartbeat_timeout_ns() {
            return Err(ClusterError::Timeout(
                "failed to join catchup log".to_owned(),
            ));
        }

        Ok(work)
    }

    /// `followerCatchup` (`:1089-1113`).
    fn follower_catchup(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        let _ = sender;
        let mut work = host.catchup_poll(self.notified_commit_position, now_ns)?;

        let near_live_position = self
            .catchup_join_position
            .max(self.notified_commit_position);
        if host.live_log_destination().is_empty() && host.is_catchup_near_live(near_live_position) {
            self.add_live_log_destination(host)?;
            work += 1;
        }

        let position = host.commit_position_value();
        if position >= self.catchup_join_position
            && position >= self.notified_commit_position
            && host.catchup_log_destination().is_empty()
            && host.state() != crate::state::ModuleState::Snapshot
        {
            self.append_position = position;
            self.log_position = position;
            self.transition(ElectionState::FollowerLogInit, now_ns, host)?;
            work += 1;
        }

        Ok(work)
    }

    /// `followerLogInit` (`:1115-1132`).
    fn follower_log_init(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        let _ = sender;
        if self.log_subscription_id.is_none() {
            if self.log_session_id != -1 {
                let registration_id = host.add_log_subscription(self.log_session_id)?;
                self.log_subscription_id = Some(registration_id);
                self.add_live_log_destination(host)?;
                self.transition(ElectionState::FollowerLogAwait, now_ns, host)?;
            }
        } else {
            self.transition(ElectionState::FollowerReady, now_ns, host)?;
        }

        Ok(1)
    }

    /// `followerLogAwait` (`:1134-1164`).
    fn follower_log_await(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        let _ = sender;
        let mut work = 0;
        let Some(registration_id) = self.log_subscription_id else {
            return Ok(0);
        };

        if let Some(image) = host.log_image_by_session(registration_id, self.log_session_id) {
            self.verify_log_join_position("followerLogAwait", image.join_position())?;
            let startup = self.is_leader_startup;
            if host.try_join_log_as_follower(image, startup, now_ns)? {
                self.update_recording_log(host, now_ns)?;
                self.transition(ElectionState::FollowerReady, now_ns, host)?;
                work += 1;
            } else if now_ns
                >= self.time_of_last_state_change_ns + host.leader_heartbeat_timeout_ns()
            {
                return Err(ClusterError::Timeout(
                    "failed to join live log as follower".to_owned(),
                ));
            }
        } else if host.log_subscription_channel_errored(registration_id) {
            return Err(ClusterError::Cluster(
                "failed to add live log as follower".to_owned(),
            ));
        } else if now_ns >= self.time_of_last_state_change_ns + host.leader_heartbeat_timeout_ns() {
            return Err(ClusterError::Timeout("failed to join live log".to_owned()));
        }

        Ok(work)
    }

    /// `followerReady` (`:1166-1180`).
    fn follower_ready(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        if sender.append_position(
            self.leader_member_id,
            self.leadership_term_id,
            self.log_position,
            self.this_member_id,
            APPEND_POSITION_FLAG_NONE,
        ) {
            host.set_leadership_term_id(self.leadership_term_id);
            host.set_leader_member_id(self.leader_member_id);
            host.election_complete(now_ns)?;
            self.transition(ElectionState::Closed, now_ns, host)?;
        } else if now_ns >= self.time_of_last_state_change_ns + host.leader_heartbeat_timeout_ns() {
            return Err(ClusterError::Timeout(
                "ready follower failed to notify leader".to_owned(),
            ));
        }

        Ok(1)
    }

    /// `publishNewLeadershipTermOnInterval` (`:1198-1210`).
    fn publish_new_leadership_term_on_interval(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        quorum_position: i64,
        now_ns: i64,
    ) -> usize {
        if now_ns - self.time_of_last_update_ns < host.leader_heartbeat_interval_ns() {
            return 0;
        }
        self.time_of_last_update_ns = now_ns;
        let timestamp = host.timestamp_from_nanos(now_ns);
        self.publish_new_leadership_term(host, sender, quorum_position, timestamp);
        1
    }

    /// `publishNewLeadershipTerm(quorumPosition, timestamp)` (`:1253-1259`).
    fn publish_new_leadership_term(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        quorum_position: i64,
        timestamp: i64,
    ) {
        for index in 0..self.member_ids.len() {
            let member_id = self.member_ids[index];
            self.publish_new_leadership_term_to(
                host,
                sender,
                member_id,
                self.log_leadership_term_id,
                quorum_position,
                timestamp,
            );
        }
    }

    /// `publishNewLeadershipTerm(member, ..)` (`:1261-1298`).
    ///
    /// The `next` triple comes from the recording log's entry for
    /// `logLeadershipTermId + 1`, when there is one: a follower that is behind
    /// needs to know where the term after this one starts, or where to stop
    /// replicating.
    fn publish_new_leadership_term_to(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        member_id: i32,
        log_leadership_term_id: i64,
        quorum_position: i64,
        timestamp: i64,
    ) {
        if member_id == self.this_member_id || self.log_session_id == -1 {
            return;
        }

        let next = host
            .recording_log()
            .find_term_entry(log_leadership_term_id + 1)
            .map(|entry| {
                (
                    entry.leadership_term_id,
                    entry.term_base_log_position,
                    entry.log_position,
                )
            });

        let (next_leadership_term_id, next_term_base_log_position, next_log_position) = match next {
            Some((term, base, position)) => (
                term,
                base,
                if position != -1 {
                    position
                } else {
                    self.append_position
                },
            ),
            None => (self.leadership_term_id, self.append_position, -1),
        };

        sender.new_leadership_term(
            member_id,
            log_leadership_term_id,
            next_leadership_term_id,
            next_term_base_log_position,
            next_log_position,
            self.leadership_term_id,
            self.append_position,
            self.append_position,
            quorum_position,
            host.log_recording_id(),
            timestamp,
            self.this_member_id,
            self.log_session_id,
            host.app_version(),
            self.is_leader_startup,
        );
    }

    /// `publishCanvassPosition` (`:1232-1251`).
    fn publish_canvass_position(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
    ) -> Result<(), ClusterError> {
        for index in 0..self.member_ids.len() {
            let member_id = self.member_ids[index];
            if member_id == self.this_member_id {
                continue;
            }

            if !host.member_has_publication(member_id) {
                host.try_add_member_publication(member_id)?;
            }

            sender.canvass_position(
                member_id,
                self.log_leadership_term_id,
                self.append_position,
                self.leadership_term_id,
                self.this_member_id,
            );
        }
        Ok(())
    }

    /// `publishFollowerReplicationPosition` (`:1300-1316`).
    fn publish_follower_replication_position(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> usize {
        let Some(position) = self
            .log_replication
            .as_ref()
            .map(RecordingReplication::position)
        else {
            return 0;
        };

        let due = position > self.append_position
            || (position == self.append_position
                && now_ns - self.time_of_last_update_ns >= host.leader_heartbeat_interval_ns());
        if due
            && sender.append_position(
                self.leader_member_id,
                self.leadership_term_id,
                position,
                self.this_member_id,
                APPEND_POSITION_FLAG_NONE,
            )
        {
            self.append_position = position;
            self.time_of_last_update_ns = now_ns;
            return 1;
        }
        0
    }

    /// `publishFollowerAppendPosition` (`:1318-1336`).
    fn publish_follower_append_position(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        now_ns: i64,
    ) -> usize {
        let due = self.last_published_append_position != self.append_position
            || now_ns - self.time_of_last_update_ns >= host.leader_heartbeat_interval_ns();
        if due
            && sender.append_position(
                self.leader_member_id,
                self.leadership_term_id,
                self.append_position,
                self.this_member_id,
                APPEND_POSITION_FLAG_NONE,
            )
        {
            self.last_published_append_position = self.append_position;
            self.time_of_last_update_ns = now_ns;
            return 1;
        }
        0
    }

    /// `addCatchupLogDestination` (`:1344-1349`).
    fn add_catchup_log_destination(
        &mut self,
        host: &mut dyn ElectionHost,
    ) -> Result<(), ClusterError> {
        let Some(endpoint) = host.member_catchup_endpoint(self.this_member_id) else {
            return Ok(());
        };
        let destination = destination_uri(host.log_channel(), &endpoint);
        if let Some(registration_id) = self.log_subscription_id {
            host.add_log_destination(registration_id, &destination)?;
        }
        host.set_catchup_log_destination(&destination)
    }

    /// `addLiveLogDestination` (`:1351-1364`).
    fn add_live_log_destination(
        &mut self,
        host: &mut dyn ElectionHost,
    ) -> Result<(), ClusterError> {
        let destination = if host.log_is_mdc() {
            let Some(endpoint) = host.member_log_endpoint(self.this_member_id) else {
                return Ok(());
            };
            destination_uri(host.log_channel(), &endpoint)
        } else {
            host.log_channel().to_owned()
        };
        if let Some(registration_id) = self.log_subscription_id {
            host.add_log_destination(registration_id, &destination)?;
        }
        host.set_live_log_destination(&destination)
    }

    /// `placeVote` (`:1182-1196`).
    fn place_vote(
        &mut self,
        sender: &mut dyn ConsensusSender,
        candidate_term_id: i64,
        candidate_id: i32,
        vote: bool,
    ) {
        sender.place_vote(
            candidate_id,
            candidate_term_id,
            self.log_leadership_term_id,
            self.append_position,
            candidate_id,
            self.this_member_id,
            vote,
        );
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
        if next != self.state {
            if self.state == ElectionState::Canvass {
                self.is_extended_canvass = false;
            }

            match next {
                ElectionState::Canvass => {
                    self.reset_members(host);
                    host.set_role(crate::state::Role::Follower);
                }
                ElectionState::CandidateBallot => {
                    host.set_role(crate::state::Role::Candidate);
                }
                ElectionState::LeaderLogReplication => {
                    host.set_role(crate::state::Role::Leader);
                    // The log publication is created here, which is what fixes
                    // the log session id for the term (`Election.java:1398-1400`).
                    // The reference takes the session id from the call's return
                    // value; the frozen seam returns a work count, so it is read
                    // back through the host accessor.
                    host.add_log_publication(self.append_position)?;
                    self.log_session_id = host.log_session_id();
                }
                ElectionState::FollowerLogReplication | ElectionState::FollowerReplay => {
                    host.set_role(crate::state::Role::Follower);
                }
                _ => {}
            }

            self.state = next;
            host.set_election_state(next);
            self.time_of_last_state_change_ns = now_ns;
            self.time_of_last_update_ns = self.initial_time_of_last_update_ns;
            self.time_of_last_commit_position_update_ns = self.initial_time_of_last_update_ns;
        }
        Ok(())
    }

    /// `handleError` (`:262-267`): count (the caller does), put the applied
    /// position back to the commit counter, and start again.
    ///
    /// # Errors
    ///
    /// The transition's: only `LEADER_LOG_REPLICATION` can fail, and `INIT`
    /// never goes there.
    pub fn handle_error(
        &mut self,
        host: &mut dyn ElectionHost,
        now_ns: i64,
        _error: ClusterError,
    ) -> Result<(), ClusterError> {
        self.log_position = host.commit_position_value();
        self.transition(ElectionState::Init, now_ns, host)
    }

    /// `onRecordingSignal` (`:269-282`): replication progress.
    ///
    /// # Errors
    ///
    /// Never; the signature is `Result` for the caller's uniform dispatch.
    pub fn on_recording_signal(
        &mut self,
        host: &mut dyn ElectionHost,
        correlation_id: i64,
        position: i64,
        signal: RecordingSignal,
    ) -> Result<(), ClusterError> {
        if self.state == ElectionState::Init {
            return Ok(());
        }

        if let Some(replication) = &self.log_replication {
            replication.on_signal(correlation_id, position, signal);
            let recording_id = replication.recording_id();
            host.set_log_recording_id(recording_id);
        }

        Ok(())
    }

    /// `onCanvassPosition` (`:284-332`).
    ///
    /// # Errors
    ///
    /// [`ClusterError::ClusterEvent`] when a leader hears of a newer term.
    #[allow(clippy::too_many_arguments)]
    pub fn on_canvass_position(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        log_leadership_term_id: i64,
        log_position: i64,
        _leadership_term_id: i64,
        follower_member_id: i32,
        _protocol_version: i32,
    ) -> Result<(), ClusterError> {
        if self.state == ElectionState::Init {
            return Ok(());
        }

        if follower_member_id == self.graceful_closed_leader_id {
            self.graceful_closed_leader_id = -1;
        }

        if follower_member_id == self.this_member_id
            || !self.member_ids.contains(&follower_member_id)
        {
            return Ok(());
        }

        host.update_member_log_position(follower_member_id, log_leadership_term_id, log_position);

        if log_leadership_term_id < self.leadership_term_id {
            if host.role() == crate::state::Role::Leader {
                let now_ns = host.now_ns();
                let quorum =
                    host.quorum_position_bounded_by_leader_log(self.append_position, now_ns);
                let timestamp = host.timestamp_from_nanos(now_ns);
                self.publish_new_leadership_term_to(
                    host,
                    sender,
                    follower_member_id,
                    log_leadership_term_id,
                    quorum,
                    timestamp,
                );
            }
        } else if log_leadership_term_id > self.leadership_term_id
            && matches!(
                self.state,
                ElectionState::LeaderLogReplication | ElectionState::LeaderReady
            )
        {
            return Err(ClusterError::ClusterEvent(
                "potential new election in progress".to_owned(),
            ));
        }

        Ok(())
    }

    /// `onRequestVote` (`:334-381`).
    ///
    /// # Errors
    ///
    /// The node-state file's; the reference throws out of its
    /// `proposeMaxCandidateTermId`.
    #[allow(clippy::too_many_arguments)]
    pub fn on_request_vote(
        &mut self,
        host: &mut dyn ElectionHost,
        sender: &mut dyn ConsensusSender,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_term_id: i64,
        candidate_id: i32,
        _protocol_version: i32,
    ) -> Result<(), ClusterError> {
        if self.state == ElectionState::Init || candidate_id == self.this_member_id {
            return Ok(());
        }

        if candidate_term_id <= self.candidate_term_id {
            self.place_vote(sender, candidate_term_id, candidate_id, false);
        } else if member::compare_log_values(
            self.log_leadership_term_id,
            self.append_position,
            log_leadership_term_id,
            log_position,
        ) > 0
        {
            let epoch_ms = host.epoch_ms();
            host.node_state()
                .propose_max_candidate_term_id(candidate_term_id, log_position, epoch_ms)
                .map_err(|error| ClusterError::Cluster(error.to_string()))?;
            self.candidate_term_id = host.node_state().candidate_term().candidate_term_id;

            self.place_vote(sender, candidate_term_id, candidate_id, false);

            if host.role() == crate::state::Role::Leader && self.member_ids.contains(&candidate_id)
            {
                let now_ns = host.now_ns();
                let quorum =
                    host.quorum_position_bounded_by_leader_log(self.append_position, now_ns);
                let timestamp = host.timestamp_from_nanos(now_ns);
                self.publish_new_leadership_term_to(
                    host,
                    sender,
                    candidate_id,
                    log_leadership_term_id,
                    quorum,
                    timestamp,
                );
            }
        } else if matches!(
            self.state,
            ElectionState::Canvass
                | ElectionState::Nominate
                | ElectionState::CandidateBallot
                | ElectionState::FollowerBallot
        ) {
            let now_ns = host.now_ns();
            let epoch_ms = host.epoch_ms();
            host.node_state()
                .propose_max_candidate_term_id(candidate_term_id, log_position, epoch_ms)
                .map_err(|error| ClusterError::Cluster(error.to_string()))?;
            self.candidate_term_id = host.node_state().candidate_term().candidate_term_id;

            self.place_vote(sender, candidate_term_id, candidate_id, true);
            self.transition(ElectionState::FollowerBallot, now_ns, host)?;
        }

        Ok(())
    }

    /// `onVote` (`:383-410`).
    ///
    /// # Errors
    ///
    /// Never; `Result` for the uniform dispatch.
    #[allow(clippy::too_many_arguments)]
    pub fn on_vote(
        &mut self,
        host: &mut dyn ElectionHost,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_term_id: i64,
        candidate_id: i32,
        voter_id: i32,
        vote: bool,
    ) -> Result<(), ClusterError> {
        if self.state == ElectionState::Init {
            return Ok(());
        }

        if self.state == ElectionState::CandidateBallot
            && candidate_term_id == self.candidate_term_id
            && candidate_id == self.this_member_id
            && self.member_ids.contains(&voter_id)
        {
            host.set_member_vote(voter_id, candidate_term_id, Some(vote));
            host.update_member_log_position(voter_id, log_leadership_term_id, log_position);
        }

        Ok(())
    }

    /// `onNewLeadershipTerm` (`:412-533`).
    ///
    /// # Errors
    ///
    /// The truncation's [`ClusterError::ClusterEvent`] (the reference throws
    /// out of `onTruncateLogEntry`), or the invariant failure the reference
    /// words as `invalid newLeadershipTerm`.
    #[allow(clippy::too_many_arguments)]
    pub fn on_new_leadership_term(
        &mut self,
        host: &mut dyn ElectionHost,
        log_leadership_term_id: i64,
        next_leadership_term_id: i64,
        next_term_base_log_position: i64,
        next_log_position: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        commit_position: i64,
        leader_recording_id: i64,
        _timestamp: i64,
        leader_member_id: i32,
        log_session_id: i32,
        is_startup: bool,
    ) -> Result<(), ClusterError> {
        if self.state == ElectionState::Init {
            return Ok(());
        }

        if !self.member_ids.contains(&leader_member_id)
            || (leader_member_id == self.this_member_id
                && leadership_term_id == self.leadership_term_id)
        {
            return Ok(());
        }

        if leader_member_id == self.graceful_closed_leader_id {
            self.graceful_closed_leader_id = -1;
        }

        let in_ballot = matches!(
            self.state,
            ElectionState::FollowerBallot | ElectionState::CandidateBallot
        ) && leadership_term_id == self.candidate_term_id;

        if in_ballot || self.state == ElectionState::Canvass {
            if log_leadership_term_id == self.log_leadership_term_id {
                if next_term_base_log_position != -1
                    && next_term_base_log_position < self.append_position
                {
                    return self.on_truncate_log_entry(
                        host,
                        self.this_member_id,
                        log_leadership_term_id,
                        self.leadership_term_id,
                        self.candidate_term_id,
                        self.log_position,
                        self.append_position,
                        next_term_base_log_position,
                    );
                }

                self.leader_member_id = leader_member_id;
                // The member table learns the leader **now**, not at
                // `electionComplete`: a follower in `FOLLOWER_CATCHUP` sends its
                // append-position heartbeats from a host method
                // (`ConsensusModuleAgent.catchupPoll` → `updateFollowerPosition`,
                // `:2753-2785`), and those address the leader. The reference
                // reads the election's own `leaderMember` there; this build's
                // host only has the member table, so the flag has to be set when
                // the term is accepted.
                host.set_leader_member_id(leader_member_id);
                self.is_leader_startup = is_startup;
                self.leadership_term_id = leadership_term_id;
                self.candidate_term_id = self.candidate_term_id.max(leadership_term_id);
                self.log_session_id = log_session_id;
                self.leader_recording_id = leader_recording_id;
                self.catchup_join_position = if self.append_position < log_position {
                    log_position
                } else {
                    -1
                };
                self.notified_commit_position = self.notified_commit_position.max(commit_position);

                if self.append_position < term_base_log_position {
                    if next_leadership_term_id != -1 {
                        if self.append_position < next_term_base_log_position {
                            self.replication_leadership_term_id = log_leadership_term_id;
                            self.replication_stop_position = next_term_base_log_position;
                            self.replication_term_base_log_position = -1;
                            let now_ns = host.now_ns();
                            self.transition(ElectionState::FollowerLogReplication, now_ns, host)?;
                        } else if self.append_position == next_term_base_log_position
                            && next_log_position != -1
                        {
                            self.replication_leadership_term_id = next_leadership_term_id;
                            self.replication_stop_position = next_log_position;
                            self.replication_term_base_log_position = next_term_base_log_position;
                            let now_ns = host.now_ns();
                            self.transition(ElectionState::FollowerLogReplication, now_ns, host)?;
                        }
                    } else {
                        return Err(ClusterError::Cluster(format!(
                            "invalid newLeadershipTerm - this.appendPosition={} < termBaseLogPosition={term_base_log_position} and nextLeadershipTermId={next_leadership_term_id}",
                            self.append_position
                        )));
                    }
                } else {
                    let now_ns = host.now_ns();
                    self.transition(ElectionState::FollowerReplay, now_ns, host)?;
                }
            } else {
                let now_ns = host.now_ns();
                self.transition(ElectionState::Canvass, now_ns, host)?;
            }
        }

        if self.state == ElectionState::FollowerLogReplication
            && log_leadership_term_id == self.log_leadership_term_id
            && self.leader_member_id == leader_member_id
        {
            let now_ns = host.now_ns();
            self.replication_deadline_ns = now_ns + host.leader_heartbeat_timeout_ns();
        }

        Ok(())
    }

    /// `onAppendPosition` (`:535-555`).
    ///
    /// # Errors
    ///
    /// The host's, from tracking a catchup completion.
    pub fn on_append_position(
        &mut self,
        host: &mut dyn ElectionHost,
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        flags: i16,
    ) -> Result<(), ClusterError> {
        if self.state == ElectionState::Init {
            return Ok(());
        }

        if leadership_term_id <= self.leadership_term_id
            && self.member_ids.contains(&follower_member_id)
        {
            host.update_member_log_position(follower_member_id, leadership_term_id, log_position);
            host.track_catchup_completion(follower_member_id, leadership_term_id, flags)?;
        }

        Ok(())
    }

    /// `onCommitPosition` (`:557-588`).
    ///
    /// # Errors
    ///
    /// [`ClusterError::ClusterEvent`] when a leader sees a newer term in a
    /// commit.
    pub fn on_commit_position(
        &mut self,
        host: &mut dyn ElectionHost,
        leadership_term_id: i64,
        log_position: i64,
        leader_member_id: i32,
    ) -> Result<(), ClusterError> {
        let state = self.state;
        if state == ElectionState::Init {
            return Ok(());
        }

        if self.leader_member_id != -1 && self.leader_member_id == leader_member_id {
            self.notified_commit_position = self.notified_commit_position.max(log_position);
            if state == ElectionState::FollowerLogReplication {
                let now_ns = host.now_ns();
                self.replication_deadline_ns = now_ns + host.leader_heartbeat_timeout_ns();
            }
        } else if leadership_term_id > self.leadership_term_id
            && state == ElectionState::LeaderReady
        {
            return Err(ClusterError::ClusterEvent(format!(
                "new leader detected due to commit position - memberId={} this.leadershipTermId={}                  newLeadershipTermId={leadership_term_id} newLeaderMemberId={leader_member_id}                  newCommitPosition={log_position}",
                self.this_member_id, self.leadership_term_id
            )));
        }

        Ok(())
    }

    /// `onReplayNewLeadershipTermEvent` (`:590-608`).
    ///
    /// # Errors
    ///
    /// The recording log's, from the coherence check.
    pub fn on_replay_new_leadership_term_event(
        &mut self,
        host: &mut dyn ElectionHost,
        leadership_term_id: i64,
        log_position: i64,
        timestamp: i64,
        term_base_log_position: i64,
    ) -> Result<(), ClusterError> {
        if self.state == ElectionState::Init {
            return Ok(());
        }

        if matches!(
            self.state,
            ElectionState::FollowerCatchup | ElectionState::FollowerReplay
        ) {
            let now_ns = host.nanos_from_timestamp(timestamp);
            self.ensure_recording_log_coherent(
                host,
                leadership_term_id,
                Some(term_base_log_position),
                None,
                now_ns,
            )?;
            self.log_position = log_position;
            self.log_leadership_term_id = leadership_term_id;
        }

        Ok(())
    }

    /// `onTruncateLogEntry` (`:610-646`): truncate, then **always** raise a
    /// `ClusterEvent` — the reference's own comment says the exception is how
    /// the cleanup happens.
    ///
    /// # Errors
    ///
    /// Always [`ClusterError::ClusterEvent`] after a successful truncation; the
    /// host's error if the truncation itself fails.
    #[allow(clippy::too_many_arguments)]
    pub fn on_truncate_log_entry(
        &mut self,
        host: &mut dyn ElectionHost,
        member_id: i32,
        log_leadership_term_id: i64,
        leadership_term_id: i64,
        candidate_term_id: i64,
        log_position: i64,
        old_position: i64,
        new_position: i64,
    ) -> Result<(), ClusterError> {
        host.truncate_log_entry(log_leadership_term_id, new_position)?;
        self.append_position = new_position;

        Err(ClusterError::ClusterEvent(format!(
            "Truncating Cluster Log - memberId={member_id} state={:?}              this.logLeadershipTermId={log_leadership_term_id}              this.leadershipTermId={leadership_term_id}              this.candidateTermId={candidate_term_id}              this.logPosition={log_position}              this.appendPosition={} oldPosition={old_position} newPosition={new_position}",
            self.state, self.append_position
        )))
    }

    /// `resetMembers()` (`:1446-1451`).
    fn reset_members(&mut self, host: &mut dyn ElectionHost) {
        host.reset_members(self.leadership_term_id, self.append_position);
        self.leader_member_id = -1;
    }

    /// `stopCatchup()` (`:1440-1444`).
    fn stop_catchup(&mut self, host: &mut dyn ElectionHost) -> Result<(), ClusterError> {
        host.stop_all_catchups()?;
        self.catchup_join_position = -1;
        Ok(())
    }

    /// `stopReplay()` (`:1453-1461`). The agent's own replay poll notices the
    /// end of its replay; dropping the handle is this side's half.
    fn stop_replay(&mut self) {
        self.log_replay = None;
        self.last_published_append_position = 0;
    }

    /// `stopLogReplication()` (`:1463-1472`).
    fn stop_log_replication(&mut self) {
        self.log_replication = None;
        self.replication_deadline_ns = 0;
        self.replication_progress_position = -1;
        self.last_published_commit_position = 0;
    }

    /// `verifyLogJoinPosition` (`:1539-1547`).
    fn verify_log_join_position(
        &self,
        state: &str,
        join_position: i64,
    ) -> Result<(), ClusterError> {
        if join_position != self.log_position {
            let inequality = if join_position < self.log_position {
                " less "
            } else {
                " greater "
            };
            return Err(ClusterError::ClusterEvent(format!(
                "{state} - joinPosition={join_position}{inequality}than logPosition={}",
                self.log_position
            )));
        }
        Ok(())
    }
}

/// How long the quorum scratch array must be for `member_count`.
pub const fn quorum_scratch_len(member_count: usize) -> usize {
    (member_count >> 1) + 1
}

/// `APPEND_POSITION_FLAG_NONE` (`ConsensusModuleAgent.java:125`).
pub const APPEND_POSITION_FLAG_NONE: i16 = 0;
/// `APPEND_POSITION_FLAG_CATCHUP` (`ConsensusModuleAgent.java:126`).
pub const APPEND_POSITION_FLAG_CATCHUP: i16 = 1;

/// `ChannelUri.createDestinationUri(channel, endpoint)` (`ChannelUri.java:589-601`):
/// `aeron:<media>?endpoint=<endpoint>`, plus the channel's interface when it
/// names one.
fn destination_uri(channel: &str, endpoint: &str) -> String {
    match deepmsg_core::uri::ChannelUri::parse(channel) {
        Ok(uri) => {
            let media = uri.media();
            match uri.get("interface") {
                Some(interface) => {
                    format!("aeron:{media}?endpoint={endpoint}|interface={interface}")
                }
                None => format!("aeron:{media}?endpoint={endpoint}"),
            }
        }
        Err(_) => format!("aeron:udp?endpoint={endpoint}"),
    }
}
