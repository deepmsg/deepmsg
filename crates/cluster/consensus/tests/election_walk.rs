//! A single-member election, walked end to end on a mock host.
//!
//! This is `P3-5a-beta`'s DoD: `INIT -> LEADER_LOG_REPLICATION -> LEADER_REPLAY
//! -> LEADER_INIT -> LEADER_READY -> CLOSED` (0 -> 5 -> 6 -> 7 -> 8 -> 17) with
//! nothing but the trait under it. The two real collaborators are the files
//! `P3-1f` and `P3-1e` built, because the walk cannot be honest without them:
//! `INIT` reads and writes `node-state.dat`, and `LEADER_INIT` writes
//! `recording.log`.
//!
//! The mock answers, it does not simulate. Where a value would have to come
//! from a cluster (the quorum position, whether the quorum reached a position)
//! it returns the answer this one-member cluster would produce.

use deepmsg_client::image::ImageView;
use deepmsg_cluster::election::state::ElectionState;
use deepmsg_cluster::election::{ConsensusSender, Election, ElectionHost, ElectionRng};
use deepmsg_cluster::error::ClusterError;
use deepmsg_cluster::handles::{LogReplay, RecordingReplication};
use deepmsg_cluster::persist::node_state::NodeStateFile;
use deepmsg_cluster::persist::recording_log::RecordingLog;
use deepmsg_cluster::state::{ModuleState, Role};

struct Mock {
    node_state: NodeStateFile,
    recording_log: RecordingLog,
    now_ns: i64,
    role: Role,
    log_recording_id: i64,
    append_position: i64,
    quorum_reached: bool,
    calls: Vec<&'static str>,
}

impl Mock {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "deepmsg-election-{tag}-{}-{}",
            std::process::id(),
            1_700_000_000_000_i64
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self {
            node_state: NodeStateFile::open_or_create(&dir.join("node-state.dat")).unwrap(),
            recording_log: RecordingLog::open(&dir.join("recording.log")).unwrap(),
            now_ns: 1_000,
            role: Role::Follower,
            log_recording_id: 5,
            append_position: 0,
            quorum_reached: true,
            calls: Vec::new(),
        }
    }
}

impl ElectionHost for Mock {
    fn set_leader_member_id(&mut self, _leader_member_id: i32) {}

    fn set_leadership_term_id(&mut self, _leadership_term_id: i64) {}

    fn set_role(&mut self, new_role: Role) {
        self.role = new_role;
        self.calls.push("set_role");
    }
    fn role(&self) -> Role {
        self.role
    }
    fn state(&self) -> ModuleState {
        ModuleState::Active
    }
    fn log_recording_id(&self) -> i64 {
        self.log_recording_id
    }
    fn set_log_recording_id(&mut self, recording_id: i64) {
        self.log_recording_id = recording_id;
    }
    fn time_of_last_leader_update_ns(&self) -> i64 {
        self.now_ns
    }
    fn quorum_position_bounded_by_leader_log(&mut self, _a: i64, _n: i64) -> i64 {
        self.append_position
    }
    fn update_member_log_position(&mut self, _m: i32, _t: i64, _p: i64) {}
    fn update_leader_position(&mut self, _n: i64, _a: i64, _q: i64) -> Result<usize, ClusterError> {
        self.calls.push("update_leader_position");
        Ok(1)
    }
    fn publish_commit_position(&mut self, _c: i64, _t: i64) -> Result<(), ClusterError> {
        self.calls.push("publish_commit_position");
        Ok(())
    }
    fn join_log_as_leader(
        &mut self,
        _t: i64,
        _p: i64,
        s: i32,
        _u: bool,
    ) -> Result<(), ClusterError> {
        // The session id is the one `add_log_publication` produced; the election
        // carries it from the LEADER_LOG_REPLICATION transition into the join.
        assert_eq!(s, 77, "the session id comes from the log publication");
        self.calls.push("join_log_as_leader");
        Ok(())
    }
    fn try_join_log_as_follower(
        &mut self,
        _i: ImageView,
        _s: bool,
        _n: i64,
    ) -> Result<bool, ClusterError> {
        Ok(false)
    }
    fn add_log_publication(&mut self, _a: i64) -> Result<usize, ClusterError> {
        self.calls.push("add_log_publication");
        Ok(1)
    }
    fn log_session_id(&self) -> i32 {
        // What `addLogPublication` answers in the reference; the election reads
        // it back through this accessor (the frozen seam returns a work count).
        77
    }
    fn new_log_replay(&mut self, _l: i64, _a: i64) -> Result<LogReplay, ClusterError> {
        self.calls.push("new_log_replay");
        Ok(LogReplay::completed())
    }
    fn new_log_replication(
        &mut self,
        _l: &str,
        _r: Option<&str>,
        _i: i64,
        _s: i64,
        _n: i64,
    ) -> Result<RecordingReplication, ClusterError> {
        Ok(RecordingReplication::default())
    }
    fn truncate_log_entry(&mut self, _t: i64, _p: i64) -> Result<(), ClusterError> {
        Ok(())
    }
    fn append_new_leadership_term_event(&mut self, _n: i64) -> Result<bool, ClusterError> {
        self.calls.push("append_new_leadership_term_event");
        Ok(true)
    }
    fn prepare_for_new_leadership(&mut self, p: i64, _n: i64) -> Result<i64, ClusterError> {
        Ok(p)
    }
    fn catchup_initiated(&mut self, _n: i64) -> Result<(), ClusterError> {
        Ok(())
    }
    fn catchup_poll(&mut self, _l: i64, _n: i64) -> Result<usize, ClusterError> {
        Ok(0)
    }
    fn is_catchup_near_live(&mut self, _p: i64) -> bool {
        true
    }
    fn track_catchup_completion(&mut self, _m: i32, _t: i64, _f: i16) -> Result<(), ClusterError> {
        Ok(())
    }
    fn stop_all_catchups(&mut self) -> Result<(), ClusterError> {
        Ok(())
    }
    fn live_log_destination(&self) -> &str {
        ""
    }
    fn set_live_log_destination(&mut self, _d: &str) -> Result<(), ClusterError> {
        Ok(())
    }
    fn catchup_log_destination(&self) -> &str {
        ""
    }
    fn set_catchup_log_destination(&mut self, _d: &str) -> Result<(), ClusterError> {
        Ok(())
    }
    fn await_local_sockets_closed(&mut self, _r: i64) -> Result<(), ClusterError> {
        Ok(())
    }
    fn election_complete(&mut self, _n: i64) -> Result<(), ClusterError> {
        self.calls.push("election_complete");
        Ok(())
    }
    fn poll_archive_events(&mut self) -> usize {
        0
    }
    fn node_state(&mut self) -> &mut NodeStateFile {
        &mut self.node_state
    }
    fn recording_log(&mut self) -> &mut RecordingLog {
        &mut self.recording_log
    }
    fn now_ns(&mut self) -> i64 {
        self.now_ns
    }
    fn timestamp_from_nanos(&self, now_ns: i64) -> i64 {
        now_ns / 1_000_000
    }
    fn nanos_from_timestamp(&self, timestamp: i64) -> i64 {
        timestamp * 1_000_000
    }
    fn epoch_ms(&self) -> i64 {
        self.now_ns / 1_000_000
    }
    fn appointed_leader_id(&self) -> i32 {
        -1
    }
    fn startup_canvass_timeout_ns(&self) -> i64 {
        60_000_000_000
    }
    fn leader_heartbeat_timeout_ns(&self) -> i64 {
        10_000_000_000
    }
    fn leader_heartbeat_interval_ns(&self) -> i64 {
        200_000_000
    }
    fn election_timeout_ns(&self) -> i64 {
        1_000_000_000
    }
    fn election_status_interval_ns(&self) -> i64 {
        100_000_000
    }
    fn has_quorum_at_position(&mut self, _t: i64, _p: i64, _n: i64, _o: i64) -> bool {
        self.quorum_reached
    }

    // P3-5b's additions: this walk never leaves the leader path, so the member
    // table answers vacuously and the log subscription is never made.
    fn reset_members(&mut self, _t: i64, _p: i64) {}
    fn become_candidate(&mut self, _t: i64, _m: i32) {}
    fn member_ballot_sent(&self, _m: i32) -> bool {
        true
    }
    fn set_member_ballot_sent(&mut self, _m: i32, _s: bool) {}
    fn set_member_vote(&mut self, _m: i32, _t: i64, _v: Option<bool>) {}
    fn unanimous_candidate(&self, _c: i32, _g: i32) -> bool {
        true
    }
    fn quorum_candidate(&self, _c: i32) -> bool {
        true
    }
    fn unanimous_leader(&self, _t: i64, _g: i32) -> bool {
        true
    }
    fn quorum_leader(&self, _t: i64) -> bool {
        true
    }
    fn member_has_publication(&self, _m: i32) -> bool {
        true
    }
    fn try_add_member_publication(&mut self, _m: i32) -> Result<(), ClusterError> {
        Ok(())
    }
    fn consensus_stream_id(&self) -> i32 {
        5
    }
    fn app_version(&self) -> i32 {
        0
    }
    fn file_sync_level(&self) -> i32 {
        0
    }
    fn log_channel(&self) -> &str {
        "aeron:ipc"
    }
    fn log_stream_id(&self) -> i32 {
        6
    }
    fn log_is_mdc(&self) -> bool {
        false
    }
    fn member_log_endpoint(&self, _m: i32) -> Option<String> {
        None
    }
    fn member_catchup_endpoint(&self, _m: i32) -> Option<String> {
        None
    }
    fn member_archive_endpoint(&self, _m: i32) -> Option<String> {
        None
    }
    fn member_archive_response_endpoint(&self, _m: i32) -> Option<String> {
        None
    }
    fn add_log_subscription(&mut self, _s: i32) -> Result<i64, ClusterError> {
        Ok(11)
    }
    fn remove_log_subscription(&mut self, _r: i64) -> Result<(), ClusterError> {
        Ok(())
    }
    fn add_log_destination(&mut self, _r: i64, _d: &str) -> Result<(), ClusterError> {
        Ok(())
    }
    fn log_subscription_resolved_endpoint(&self, _r: i64) -> Option<String> {
        None
    }
    fn log_subscription_channel_errored(&self, _r: i64) -> bool {
        false
    }
    fn log_image_by_session(
        &mut self,
        _r: i64,
        _s: i32,
    ) -> Option<deepmsg_client::image::ImageView> {
        None
    }
    fn commit_position_value(&self) -> i64 {
        0
    }
    fn set_election_state(&mut self, state: deepmsg_cluster::election::state::ElectionState) {
        self.calls.push(match state {
            deepmsg_cluster::election::state::ElectionState::Closed => "state:closed",
            _ => "state",
        });
    }
}

struct MockSender {
    sent: Vec<&'static str>,
}

impl ConsensusSender for MockSender {
    fn canvass_position(&mut self, _m: i32, _a: i64, _b: i64, _c: i64, _d: i32) {
        self.sent.push("canvass_position");
    }
    fn request_vote(&mut self, _m: i32, _a: i64, _b: i64, _c: i64, _d: i32) -> bool {
        self.sent.push("request_vote");
        true
    }
    fn place_vote(&mut self, _m: i32, _a: i64, _b: i64, _c: i64, _d: i32, _e: i32, _f: bool) {
        self.sent.push("place_vote");
    }
    fn new_leadership_term(
        &mut self,
        _m: i32,
        _a: i64,
        _b: i64,
        _c: i64,
        _d: i64,
        _e: i64,
        _f: i64,
        _g: i64,
        _h: i64,
        _i: i64,
        _j: i64,
        _k: i32,
        _l: i32,
        _n: i32,
        _o: bool,
    ) {
        self.sent.push("new_leadership_term");
    }
    fn append_position(&mut self, _m: i32, _a: i64, _b: i64, _c: i32, _d: i16) -> bool {
        self.sent.push("append_position");
        true
    }
    fn catchup_position(&mut self, _m: i32, _a: i64, _b: i64, _c: i32, _d: &str) -> bool {
        self.sent.push("catchup_position");
        true
    }
}

#[test]
fn a_single_member_election_walks_to_closed() {
    let mut host = Mock::new("walk");
    let mut sender = MockSender { sent: Vec::new() };
    let mut election = Election::new(0, vec![0], true, -1, 0, 0, 0, 0, 1_000, 42);

    // 0 -> 5: INIT persists a term and short-circuits the ballot, and the
    // transition to LEADER_LOG_REPLICATION is what makes this member a leader
    // and creates its log publication.
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::LeaderLogReplication);
    assert_eq!(host.role(), Role::Leader);
    assert!(host.calls.contains(&"add_log_publication"));
    assert_eq!(host.node_state.candidate_term().candidate_term_id, 1);
    assert_eq!(election.leader_member_id(), 0);

    // 5 -> 6: the quorum position is already at the append position.
    host.now_ns += 1_000_000_000;
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::LeaderReplay);

    // 6 -> 7: log position equals append position, so there is nothing to
    // replay.
    host.now_ns += 1_000_000_000;
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::LeaderInit);

    // 7 -> 8: join the log and write the term into recording.log.
    host.now_ns += 1_000_000_000;
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::LeaderReady);
    assert!(host.calls.contains(&"join_log_as_leader"));
    // `ensureCoherent` on an empty log writes every term from the initial one:
    // term 0, then term 1 (the election's), the second committed at the applied
    // position (`RecordingLog.java:1382-1393`).
    assert_eq!(host.recording_log.entries().len(), 2, "terms 0 and 1");
    assert_eq!(host.recording_log.entries()[0].leadership_term_id, 0);
    assert_eq!(host.recording_log.entries()[1].leadership_term_id, 1);
    assert_eq!(host.recording_log.entries()[1].log_position, 0);

    // 8 -> 17: the quorum has reached the position, the event is appended, and
    // the election hands control back.
    host.now_ns += 1_000_000_000;
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Closed);
    assert!(host.calls.contains(&"election_complete"));
}

#[test]
fn a_two_member_election_canvasses_nominates_and_wins_the_ballot() {
    let mut host = Mock::new("canvass");
    let mut sender = MockSender { sent: Vec::new() };
    let mut election = Election::new(0, vec![0, 1], true, -1, 0, 0, 0, 0, 1_000, 42);

    // 0 -> 1.
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Canvass);
    assert_eq!(host.role(), Role::Follower);

    // 1 -> 2: the mock is a unanimous candidate, so canvass nominates at once.
    host.now_ns += 1_000_000_000;
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Nominate);
    assert!(sender.sent.contains(&"canvass_position"));

    // 2 -> 3: the nomination deadline passes and the term is persisted.
    host.now_ns += 1_000_000_000;
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::CandidateBallot);
    assert_eq!(host.role(), Role::Candidate);
    assert_eq!(host.node_state.candidate_term().candidate_term_id, 1);

    // 3 -> 5: every member voted yes at the candidate's term.
    host.now_ns += 1_000_000_000;
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::LeaderLogReplication);
    assert_eq!(host.role(), Role::Leader);
}

#[test]
fn the_initial_candidate_term_comes_from_the_file_not_from_zero() {
    let mut host = Mock::new("persisted-term");
    host.node_state.update_candidate_term_id(7, 0, 0).unwrap();
    let mut sender = MockSender { sent: Vec::new() };
    let mut election = Election::new(0, vec![0], true, -1, 0, 0, 0, 0, 1_000, 42);

    election.do_work(&mut host, &mut sender).unwrap();
    // max(7, 0) + 1 for the single-member short circuit.
    assert_eq!(election.leadership_term_id(), 8);
    assert_eq!(host.node_state.candidate_term().candidate_term_id, 8);
}

#[test]
fn the_rng_is_injectable_so_a_backoff_is_reproducible() {
    let mut a = ElectionRng::new(7);
    let mut b = ElectionRng::new(7);
    assert_eq!(a.next_f64(), b.next_f64());
    assert!((0.0..1.0).contains(&a.next_f64()));
}
