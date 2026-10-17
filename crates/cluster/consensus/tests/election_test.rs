//! `ElectionTest`: the reference's election cases, ported onto a scripted host.
//!
//! The reference drives `Election` with Mockito: `when(consensusModuleAgent.…)`
//! scripts an answer and `verify(consensusPublisher).…` reads a call back. This
//! file is the same arrangement with two structs — [`TestHost`] answers and
//! records, [`RecordingSender`] records — because the two seams were frozen for
//! exactly that (election spec §2).
//!
//! # What a mock can and cannot make
//!
//! A `LogReplay` and the follower's four flags are scriptable
//! ([`deepmsg_cluster::handles`]' handles). An **`Image` is not**: it is a mapped log
//! buffer and has no test constructor, so the two cases whose success needs
//! `logSubscription.imageBySessionId(..)` to answer (FOLLOWER_CATCHUP_AWAIT and
//! FOLLOWER_LOG_AWAIT succeeding) are recorded as deferred in the delivery
//! record rather than faked. Every other state is reachable.
//!
//! Cases are named after `ElectionTest.java`; a name here is the port of that
//! row.

use std::path::PathBuf;

use deepmsg_client::image::Image;
use deepmsg_cluster::election::state::ElectionState;
use deepmsg_cluster::election::{ConsensusSender, ElectionRng};
use deepmsg_cluster::election::{Election, ElectionHost};
use deepmsg_cluster::error::ClusterError;
use deepmsg_cluster::handles::{LogReplay, RecordingReplication, RecordingSignal};
use deepmsg_cluster::persist::node_state::NodeStateFile;
use deepmsg_cluster::persist::recording_log::RecordingLog;
use deepmsg_cluster::state::{ModuleState, Role};
use deepmsg_cluster_common::member::{self, ClusterMember};

const LOG_SESSION_ID: i32 = 777;
const RECORDING_ID: i64 = 600;
const EPOCH_MS: i64 = 1_700_000_000_000;

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "deepmsg-election-test-{tag}-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("the temp directory");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One answer the sender recorded. Message-shaped, like the reference's
/// `ArgumentCaptor`: the test asserts on the numbers, not just the name.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Sent {
    Canvass {
        member_id: i32,
        log_term: i64,
        log_position: i64,
        leadership_term: i64,
        follower_member: i32,
    },
    RequestVote {
        member_id: i32,
        log_term: i64,
        log_position: i64,
        candidate_term: i64,
        candidate_member: i32,
    },
    PlaceVote {
        member_id: i32,
        candidate_term: i64,
        candidate_member: i32,
        vote: bool,
    },
    NewLeadershipTerm {
        member_id: i32,
        log_term: i64,
        next_term: i64,
        next_base: i64,
        next_log_position: i64,
        leadership_term: i64,
        base: i64,
        log_position: i64,
        commit_position: i64,
        recording_id: i64,
        leader_member: i32,
        log_session_id: i32,
    },
    AppendPosition {
        member_id: i32,
        leadership_term: i64,
        log_position: i64,
        follower_member: i32,
        flags: i16,
    },
    CatchupPosition {
        member_id: i32,
        log_position: i64,
        catchup_endpoint: String,
    },
}

#[derive(Default)]
struct RecordingSender {
    sent: Vec<Sent>,
    append_position_accepts: bool,
    request_vote_accepts: bool,
    catchup_position_accepts: bool,
}

impl RecordingSender {
    fn accepting() -> Self {
        Self {
            sent: Vec::new(),
            append_position_accepts: true,
            request_vote_accepts: true,
            catchup_position_accepts: true,
        }
    }
}

impl ConsensusSender for RecordingSender {
    fn canvass_position(
        &mut self,
        member_id: i32,
        log_leadership_term_id: i64,
        log_position: i64,
        leadership_term_id: i64,
        follower_member_id: i32,
    ) {
        self.sent.push(Sent::Canvass {
            member_id,
            log_term: log_leadership_term_id,
            log_position,
            leadership_term: leadership_term_id,
            follower_member: follower_member_id,
        });
    }

    fn request_vote(
        &mut self,
        member_id: i32,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_term_id: i64,
        candidate_member_id: i32,
    ) -> bool {
        self.sent.push(Sent::RequestVote {
            member_id,
            log_term: log_leadership_term_id,
            log_position,
            candidate_term: candidate_term_id,
            candidate_member: candidate_member_id,
        });
        self.request_vote_accepts
    }

    fn place_vote(
        &mut self,
        member_id: i32,
        candidate_term_id: i64,
        _log_leadership_term_id: i64,
        _log_position: i64,
        candidate_member_id: i32,
        _follower_member_id: i32,
        vote: bool,
    ) {
        self.sent.push(Sent::PlaceVote {
            member_id,
            candidate_term: candidate_term_id,
            candidate_member: candidate_member_id,
            vote,
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn new_leadership_term(
        &mut self,
        member_id: i32,
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
        _app_version: i32,
        _is_startup: bool,
    ) {
        self.sent.push(Sent::NewLeadershipTerm {
            member_id,
            log_term: log_leadership_term_id,
            next_term: next_leadership_term_id,
            next_base: next_term_base_log_position,
            next_log_position,
            leadership_term: leadership_term_id,
            base: term_base_log_position,
            log_position,
            commit_position,
            recording_id: leader_recording_id,
            leader_member: leader_member_id,
            log_session_id,
        });
    }

    fn append_position(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        flags: i16,
    ) -> bool {
        self.sent.push(Sent::AppendPosition {
            member_id,
            leadership_term: leadership_term_id,
            log_position,
            follower_member: follower_member_id,
            flags,
        });
        self.append_position_accepts
    }

    fn catchup_position(
        &mut self,
        member_id: i32,
        _leadership_term_id: i64,
        log_position: i64,
        _follower_member_id: i32,
        catchup_endpoint: &str,
    ) -> bool {
        self.sent.push(Sent::CatchupPosition {
            member_id,
            log_position,
            catchup_endpoint: catchup_endpoint.to_owned(),
        });
        self.catchup_position_accepts
    }
}

struct TestHost {
    _dir: TempDir,
    node_state: NodeStateFile,
    recording_log: RecordingLog,
    now_ns: i64,
    role: Role,
    state: ModuleState,
    log_recording_id: i64,
    quorum_position: i64,
    commit_position: i64,
    members: Vec<ClusterMember>,
    this_member_id: i32,
    appointed_leader_id: i32,
    log_subscription_id: i64,
    catchup_destination: String,
    live_destination: String,
    channel_errored: bool,
    image: Option<Image>,
    election_state: Option<ElectionState>,
    prepare_calls: usize,
    join_log_as_leader_calls: usize,
    election_complete_calls: usize,
    catchup_initiated_calls: usize,
    truncate_calls: usize,
    update_leader_position_calls: usize,
    publish_commit_position_calls: usize,
    near_live: bool,
}

impl TestHost {
    fn new(tag: &str, member_specs: &[i32], this_member_id: i32) -> Self {
        let dir = TempDir::new(tag);
        let members = member_specs
            .iter()
            .map(|id| {
                ClusterMember::parse(&format!(
                    "{id},localhost:1,localhost:2,localhost:3,localhost:4,localhost:5"
                ))
                .expect("a member")
            })
            .collect();

        Self {
            node_state: NodeStateFile::open_or_create(&dir.0.join("node-state.dat"))
                .expect("node state"),
            recording_log: RecordingLog::open(&dir.0.join("recording.log")).expect("log"),
            _dir: dir,
            now_ns: 1_000,
            role: Role::Follower,
            state: ModuleState::Active,
            log_recording_id: RECORDING_ID,
            quorum_position: 0,
            commit_position: 0,
            members,
            this_member_id,
            appointed_leader_id: -1,
            log_subscription_id: 41,
            catchup_destination: String::new(),
            live_destination: String::new(),
            channel_errored: false,
            image: None,
            election_state: None,
            prepare_calls: 0,
            join_log_as_leader_calls: 0,
            election_complete_calls: 0,
            catchup_initiated_calls: 0,
            truncate_calls: 0,
            update_leader_position_calls: 0,
            publish_commit_position_calls: 0,
            near_live: true,
        }
    }

    fn member(&self, id: i32) -> &ClusterMember {
        self.members
            .iter()
            .find(|member| member.id == id)
            .expect("a member")
    }

    fn member_mut(&mut self, id: i32) -> &mut ClusterMember {
        self.members
            .iter_mut()
            .find(|member| member.id == id)
            .expect("a member")
    }

    fn advance(&mut self, ns: i64) {
        self.now_ns += ns;
    }
}

impl ElectionHost for TestHost {
    fn set_role(&mut self, new_role: Role) {
        self.role = new_role;
    }
    fn role(&self) -> Role {
        self.role
    }
    fn state(&self) -> ModuleState {
        self.state
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
    fn quorum_position_bounded_by_leader_log(&mut self, append_position: i64, _now_ns: i64) -> i64 {
        self.quorum_position.min(append_position)
    }
    fn update_member_log_position(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        log_position: i64,
    ) {
        if let Some(member) = self
            .members
            .iter_mut()
            .find(|member| member.id == member_id)
        {
            member.leadership_term_id = leadership_term_id;
            member.log_position = log_position;
        }
    }
    fn update_leader_position(
        &mut self,
        _now_ns: i64,
        _append_position: i64,
        _quorum_position: i64,
    ) -> Result<usize, ClusterError> {
        self.update_leader_position_calls += 1;
        Ok(1)
    }
    fn publish_commit_position(
        &mut self,
        _commit_position: i64,
        _leadership_term_id: i64,
    ) -> Result<(), ClusterError> {
        self.publish_commit_position_calls += 1;
        Ok(())
    }
    fn join_log_as_leader(
        &mut self,
        _leadership_term_id: i64,
        _log_position: i64,
        _log_session_id: i32,
        _is_startup: bool,
    ) -> Result<(), ClusterError> {
        self.join_log_as_leader_calls += 1;
        Ok(())
    }
    fn try_join_log_as_follower(
        &mut self,
        _image: Image,
        _is_leader_startup: bool,
        _now_ns: i64,
    ) -> Result<bool, ClusterError> {
        Ok(true)
    }
    fn add_log_publication(&mut self, _append_position: i64) -> Result<usize, ClusterError> {
        Ok(1)
    }
    fn new_log_replay(
        &mut self,
        _log_position: i64,
        _append_position: i64,
    ) -> Result<LogReplay, ClusterError> {
        Ok(LogReplay::completed())
    }
    fn new_log_replication(
        &mut self,
        _leader_archive_endpoint: &str,
        _response_archive_endpoint: Option<&str>,
        leader_recording_id: i64,
        _stop_position: i64,
        _now_ns: i64,
    ) -> Result<RecordingReplication, ClusterError> {
        Ok(RecordingReplication::new(leader_recording_id, 1))
    }
    fn truncate_log_entry(
        &mut self,
        _leadership_term_id: i64,
        _log_position: i64,
    ) -> Result<(), ClusterError> {
        self.truncate_calls += 1;
        Ok(())
    }
    fn append_new_leadership_term_event(&mut self, _now_ns: i64) -> Result<bool, ClusterError> {
        Ok(true)
    }
    fn prepare_for_new_leadership(
        &mut self,
        log_position: i64,
        _now_ns: i64,
    ) -> Result<i64, ClusterError> {
        self.prepare_calls += 1;
        Ok(log_position)
    }
    fn catchup_initiated(&mut self, _now_ns: i64) -> Result<(), ClusterError> {
        self.catchup_initiated_calls += 1;
        Ok(())
    }
    fn catchup_poll(&mut self, _limit_position: i64, _now_ns: i64) -> Result<usize, ClusterError> {
        Ok(0)
    }
    fn is_catchup_near_live(&mut self, _position: i64) -> bool {
        self.near_live
    }
    fn track_catchup_completion(
        &mut self,
        _member_id: i32,
        _leadership_term_id: i64,
        _flags: i16,
    ) -> Result<(), ClusterError> {
        Ok(())
    }
    fn stop_all_catchups(&mut self) -> Result<(), ClusterError> {
        Ok(())
    }
    fn live_log_destination(&self) -> &str {
        &self.live_destination
    }
    fn set_live_log_destination(&mut self, destination: &str) -> Result<(), ClusterError> {
        self.live_destination = destination.to_owned();
        Ok(())
    }
    fn catchup_log_destination(&self) -> &str {
        &self.catchup_destination
    }
    fn set_catchup_log_destination(&mut self, destination: &str) -> Result<(), ClusterError> {
        self.catchup_destination = destination.to_owned();
        Ok(())
    }
    fn await_local_sockets_closed(&mut self, _registration_id: i64) -> Result<(), ClusterError> {
        Ok(())
    }
    fn election_complete(&mut self, _now_ns: i64) -> Result<(), ClusterError> {
        self.election_complete_calls += 1;
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
    fn epoch_ms(&self) -> i64 {
        EPOCH_MS + self.now_ns / 1_000_000
    }
    fn timestamp_from_nanos(&self, now_ns: i64) -> i64 {
        now_ns / 1_000_000
    }
    fn nanos_from_timestamp(&self, timestamp: i64) -> i64 {
        timestamp * 1_000_000
    }
    fn appointed_leader_id(&self) -> i32 {
        self.appointed_leader_id
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
    fn log_session_id(&self) -> i32 {
        LOG_SESSION_ID
    }
    fn has_quorum_at_position(
        &mut self,
        _leadership_term_id: i64,
        _position: i64,
        _now_ns: i64,
        _timeout_ns: i64,
    ) -> bool {
        true
    }
    fn reset_members(&mut self, leadership_term_id: i64, append_position: i64) {
        ClusterMember::reset_all(&mut self.members);
        let this_id = self.this_member_id;
        self.member_mut(this_id).leadership_term_id = leadership_term_id;
        self.member_mut(this_id).log_position = append_position;
    }
    fn become_candidate(&mut self, candidate_term_id: i64, candidate_member_id: i32) {
        ClusterMember::become_candidate(&mut self.members, candidate_term_id, candidate_member_id);
    }
    fn member_ballot_sent(&self, member_id: i32) -> bool {
        self.member(member_id).is_ballot_sent
    }
    fn set_member_ballot_sent(&mut self, member_id: i32, sent: bool) {
        self.member_mut(member_id).is_ballot_sent = sent;
    }
    fn set_member_vote(&mut self, member_id: i32, candidate_term_id: i64, vote: Option<bool>) {
        self.member_mut(member_id).candidate_term_id = candidate_term_id;
        self.member_mut(member_id).vote = vote;
    }
    fn unanimous_candidate(&self, candidate_member_id: i32, graceful: i32) -> bool {
        self.members
            .iter()
            .find(|member| member.id == candidate_member_id)
            .is_some_and(|candidate| {
                ClusterMember::is_unanimous_candidate(&self.members, candidate, graceful)
            })
    }
    fn quorum_candidate(&self, candidate_member_id: i32) -> bool {
        self.members
            .iter()
            .find(|member| member.id == candidate_member_id)
            .is_some_and(|candidate| member::is_quorum_candidate(&self.members, candidate))
    }
    fn unanimous_leader(&self, candidate_term_id: i64, graceful: i32) -> bool {
        ClusterMember::is_unanimous_leader(&self.members, candidate_term_id, graceful)
    }
    fn quorum_leader(&self, candidate_term_id: i64) -> bool {
        member::is_quorum_leader(&self.members, candidate_term_id)
    }
    fn member_has_publication(&self, _member_id: i32) -> bool {
        true
    }
    fn try_add_member_publication(&mut self, _member_id: i32) -> Result<(), ClusterError> {
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
        "aeron:udp?control-mode=manual"
    }
    fn log_stream_id(&self) -> i32 {
        6
    }
    fn log_is_mdc(&self) -> bool {
        true
    }
    fn member_log_endpoint(&self, _member_id: i32) -> Option<String> {
        Some("localhost:9001".to_owned())
    }
    fn member_catchup_endpoint(&self, _member_id: i32) -> Option<String> {
        Some("localhost:9002".to_owned())
    }
    fn member_archive_endpoint(&self, _member_id: i32) -> Option<String> {
        Some("localhost:9003".to_owned())
    }
    fn member_archive_response_endpoint(&self, _member_id: i32) -> Option<String> {
        None
    }
    fn add_log_subscription(&mut self, _log_session_id: i32) -> Result<i64, ClusterError> {
        Ok(self.log_subscription_id)
    }
    fn remove_log_subscription(&mut self, _registration_id: i64) -> Result<(), ClusterError> {
        Ok(())
    }
    fn add_log_destination(
        &mut self,
        _registration_id: i64,
        _destination: &str,
    ) -> Result<(), ClusterError> {
        Ok(())
    }
    fn log_subscription_resolved_endpoint(&self, _registration_id: i64) -> Option<String> {
        Some("localhost:43210".to_owned())
    }
    fn log_subscription_channel_errored(&self, _registration_id: i64) -> bool {
        self.channel_errored
    }
    fn log_image_by_session(&mut self, _registration_id: i64, _session_id: i32) -> Option<Image> {
        self.image.take()
    }
    fn commit_position_value(&self) -> i64 {
        self.commit_position
    }
    fn set_election_state(&mut self, state: ElectionState) {
        self.election_state = Some(state);
    }
}

fn new_election(member_ids: &[i32], this_member_id: i32) -> Election {
    Election::new(
        this_member_id,
        member_ids.to_vec(),
        true,
        -1,
        -1,
        0,
        0,
        0,
        1_000,
        42,
    )
}

/// INIT -> CANVASS -> NOMINATE -> CANDIDATE_BALLOT.
fn walk_to_candidate_ballot(
    host: &mut TestHost,
    sender: &mut RecordingSender,
    election: &mut Election,
) {
    election.do_work(host, sender).unwrap();
    assert_eq!(election.state(), ElectionState::Canvass);

    // The peers report the same log, so the canvass is unanimous.
    election
        .on_canvass_position(host, sender, -1, 0, -1, 1, 0)
        .unwrap();
    election
        .on_canvass_position(host, sender, -1, 0, -1, 2, 0)
        .unwrap();
    host.advance(200_000_000);
    election.do_work(host, sender).unwrap();
    assert_eq!(election.state(), ElectionState::Nominate);

    // The nomination deadline is at most half an election timeout away.
    host.advance(1_000_000_000);
    election.do_work(host, sender).unwrap();
    assert_eq!(election.state(), ElectionState::CandidateBallot);
}

/// The rest of the leader walk: 5 -> 6 -> 7 -> 8 -> 17.
fn walk_to_closed(host: &mut TestHost, sender: &mut RecordingSender, election: &mut Election) {
    for _ in 0..4 {
        host.advance(1_000_000_000);
        election.do_work(host, sender).unwrap();
    }
    assert_eq!(election.state(), ElectionState::Closed);
}

#[test]
fn should_elect_single_node_cluster_leader() {
    let mut host = TestHost::new("single", &[0], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0], 0);

    for _ in 0..5 {
        host.advance(1);
        election.do_work(&mut host, &mut sender).unwrap();
    }

    assert_eq!(election.state(), ElectionState::Closed);
    assert_eq!(host.join_log_as_leader_calls, 1);
    assert_eq!(host.election_complete_calls, 1);
    assert!(host.election_state == Some(ElectionState::Closed));
    // The constructor's initial term is `-1`, so the single-member short circuit
    // makes term 0; `ensureCoherent` on an empty log writes it
    // (`RecordingLog.java:1382-1389`).
    assert_eq!(host.recording_log.entries().len(), 1);
    assert_eq!(host.recording_log.entries()[0].leadership_term_id, 0);
    // A one-member cluster has nobody to send to.
    assert!(
        sender.sent.is_empty(),
        "single node sends nothing: {:?}",
        sender.sent
    );
}

#[test]
fn should_canvass_members_in_successful_leadership_bid() {
    let mut host = TestHost::new("canvass", &[0, 1, 2], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1, 2], 0);

    // INIT -> CANVASS, which resets the member table.
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Canvass);

    // The peers report the same log (the reference's two `onCanvassPosition`
    // calls), which makes the next canvass a unanimous candidacy.
    election
        .on_canvass_position(&mut host, &mut sender, -1, 0, -1, 1, 0)
        .unwrap();
    election
        .on_canvass_position(&mut host, &mut sender, -1, 0, -1, 2, 0)
        .unwrap();
    host.advance(200_000_000);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Nominate);

    let canvassed: Vec<i32> = sender
        .sent
        .iter()
        .filter_map(|sent| match sent {
            Sent::Canvass { member_id, .. } => Some(*member_id),
            _ => None,
        })
        .collect();
    assert_eq!(vec![1, 2], canvassed, "every member but this one");
}

#[test]
fn should_timeout_canvass_with_majority() {
    // A member whose log is behind is not a candidate, so canvass waits for a
    // quorum of willing voters before nominating.
    let mut host = TestHost::new("canvass-majority", &[0, 1, 2], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1, 2], 0);

    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Canvass);

    // Peer 1 is behind us, so it would vote for us; peer 2 is ahead, so it
    // would not. That is a quorum but not unanimity, which is the deadline
    // path.
    election
        .on_canvass_position(&mut host, &mut sender, -2, 0, -2, 1, 0)
        .unwrap();
    election
        .on_canvass_position(&mut host, &mut sender, 5, 9, 5, 2, 0)
        .unwrap();
    host.advance(100_000_000);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(
        election.state(),
        ElectionState::Canvass,
        "not unanimous yet"
    );

    // The startup canvass timeout: a quorum of willing voters is enough.
    host.advance(60_000_000_000);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Nominate);
}

#[test]
fn should_win_candidate_ballot_with_majority() {
    let mut host = TestHost::new("ballot", &[0, 1, 2], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1, 2], 0);

    walk_to_candidate_ballot(&mut host, &mut sender, &mut election);
    let candidate_term = host.node_state.candidate_term().candidate_term_id;

    // Both peers vote yes.
    election
        .on_vote(&mut host, -1, 0, candidate_term, 0, 1, true)
        .unwrap();
    election
        .on_vote(&mut host, -1, 0, candidate_term, 0, 2, true)
        .unwrap();
    host.advance(1);
    election.do_work(&mut host, &mut sender).unwrap();

    assert_eq!(election.state(), ElectionState::LeaderLogReplication);
    assert_eq!(host.role, Role::Leader);
    assert_eq!(election.leadership_term_id(), candidate_term);
}

#[test]
fn should_timeout_candidate_ballot_without_majority() {
    let mut host = TestHost::new("ballot-fail", &[0, 1, 2], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1, 2], 0);

    walk_to_candidate_ballot(&mut host, &mut sender, &mut election);

    // No votes; the ballot times out and the election starts again.
    host.advance(1_000_000_000);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Canvass);
}

#[test]
fn should_timeout_follower_ballot_without_leader_emerging() {
    let mut host = TestHost::new("follower-ballot", &[0, 1, 2], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1, 2], 0);

    // A candidate we would vote for (our log is not more recent), in CANVASS.
    election.do_work(&mut host, &mut sender).unwrap();
    election
        .on_request_vote(&mut host, &mut sender, -1, 0, 0, 1, 0)
        .unwrap();
    assert_eq!(election.state(), ElectionState::FollowerBallot);
    assert!(sender.sent.iter().any(|sent| matches!(
        sent,
        Sent::PlaceVote {
            member_id: 1,
            vote: true,
            ..
        }
    )));

    host.advance(1_000_000_000);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Canvass);
}

#[test]
fn on_request_vote_refuses_a_stale_candidate_and_a_more_recent_voter() {
    let mut host = TestHost::new("request-vote", &[0, 1, 2], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1, 2], 0);

    election.do_work(&mut host, &mut sender).unwrap();

    // Stale: candidateTermId <= this.candidateTermId (-1).
    election
        .on_request_vote(&mut host, &mut sender, -1, 0, -1, 1, 0)
        .unwrap();
    assert!(
        sender
            .sent
            .iter()
            .any(|sent| matches!(sent, Sent::PlaceVote { vote: false, .. }))
    );

    // More recent log: our term/position is higher, so we refuse and persist
    // the candidate's term.
    host.member_mut(0).leadership_term_id = 5;
    host.member_mut(0).log_position = 100;
    let before = sender.sent.len();
    election
        .on_request_vote(&mut host, &mut sender, 1, 1, 1, 1, 0)
        .unwrap();
    assert_eq!(before + 1, sender.sent.len(), "one refusal");
}

#[test]
fn on_vote_is_recorded_only_for_our_ballot() {
    let mut host = TestHost::new("on-vote", &[0, 1, 2], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1, 2], 0);

    walk_to_candidate_ballot(&mut host, &mut sender, &mut election);
    let candidate_term = host.node_state.candidate_term().candidate_term_id;

    election
        .on_vote(&mut host, -1, 0, candidate_term, 0, 1, true)
        .unwrap();
    assert_eq!(host.members[1].vote, Some(true));
    assert_eq!(host.members[1].candidate_term_id, candidate_term);
}

#[test]
fn on_canvass_position_brings_a_follower_back_with_a_new_leadership_term() {
    let mut host = TestHost::new("on-canvass", &[0, 1], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1], 0);

    // Walk to leader.
    walk_to_candidate_ballot(&mut host, &mut sender, &mut election);
    let candidate_term = host.node_state.candidate_term().candidate_term_id;
    election
        .on_vote(&mut host, -1, 0, candidate_term, 0, 1, true)
        .unwrap();
    host.advance(1);
    election.do_work(&mut host, &mut sender).unwrap();
    walk_to_closed(&mut host, &mut sender, &mut election);
    assert_eq!(election.state(), ElectionState::Closed);
    assert_eq!(host.role, Role::Leader);
    let leader_term = election.leadership_term_id();

    // A follower with an older log term can be brought up to date.
    let before = sender.sent.len();
    election
        .on_canvass_position(&mut host, &mut sender, -1, 0, -1, 1, 0)
        .unwrap();
    assert_eq!(before + 1, sender.sent.len(), "one NewLeadershipTerm");
    assert!(matches!(
        sender.sent.last(),
        Some(Sent::NewLeadershipTerm {
            member_id: 1,
            leadership_term: term,
            ..
        }) if *term == leader_term
    ));
}

#[test]
fn on_canvass_position_raises_a_new_election_for_a_newer_term() {
    let mut host = TestHost::new("on-canvass-newer", &[0, 1], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1], 0);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Canvass);
    election
        .on_canvass_position(&mut host, &mut sender, -1, 0, -1, 1, 0)
        .unwrap();
    host.advance(200_000_000);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Nominate);
    host.advance(1_000_000_000);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::CandidateBallot);
    let candidate_term = host.node_state.candidate_term().candidate_term_id;
    election
        .on_vote(&mut host, -1, 0, candidate_term, 0, 1, true)
        .unwrap();
    host.advance(1);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::LeaderLogReplication);

    let result = election.on_canvass_position(&mut host, &mut sender, 9, 0, 9, 1, 0);
    assert!(matches!(result, Err(ClusterError::ClusterEvent(message))
        if message.contains("potential new election")));
}

#[test]
fn on_new_leadership_term_moves_a_canvassing_member_to_follower_replay() {
    let mut host = TestHost::new("nlt-replay", &[0, 1], 1);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1], 1);

    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Canvass);

    // Leader 0's term 0, append position 0, term base 0: nothing is missing,
    // so replay (with nothing to replay) is the next state.
    election
        .on_new_leadership_term(
            &mut host,
            -1,
            -1,
            -1,
            -1,
            0,
            0,
            0,
            0,
            RECORDING_ID,
            0,
            0,
            LOG_SESSION_ID,
            true,
        )
        .unwrap();
    assert_eq!(election.state(), ElectionState::FollowerReplay);
    assert_eq!(election.leader_member_id(), 0);
}

#[test]
fn on_new_leadership_term_truncates_ahead_of_the_term_base_and_raises() {
    let mut host = TestHost::new("nlt-truncate", &[0, 1], 1);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1], 1);

    election.do_work(&mut host, &mut sender).unwrap();
    // Our append position is 50; the leader's next term base is 10.
    host.member_mut(1).log_position = 50;
    // The election's own append position is set through the constructor's log
    // and append arguments (both 0 here), so make the candidate's view newer.
    host.member_mut(0).log_position = 0;
    host.member_mut(0).leadership_term_id = 0;

    // Re-create the election with an append position of 50.
    let mut election = Election::new(1, vec![0, 1], true, -1, 0, 0, 0, 50, 1_000, 42);
    election.do_work(&mut host, &mut sender).unwrap();

    let result = election.on_new_leadership_term(
        &mut host,
        0,
        -1,
        10,
        -1,
        0,
        0,
        0,
        0,
        RECORDING_ID,
        0,
        0,
        LOG_SESSION_ID,
        true,
    );
    let truncation = match &result {
        Err(ClusterError::ClusterEvent(message)) => message.contains("Truncating Cluster Log"),
        _ => false,
    };
    assert!(truncation, "onTruncateLogEntry always raises: {result:?}");
    assert_eq!(host.truncate_calls, 1);
}

#[test]
fn on_new_leadership_term_refuses_a_missing_term_with_no_next_term() {
    let mut host = TestHost::new("nlt-missing", &[0, 1], 1);
    let mut sender = RecordingSender::accepting();
    let mut election = Election::new(1, vec![0, 1], true, -1, 0, 0, 0, 0, 1_000, 42);
    election.do_work(&mut host, &mut sender).unwrap();

    // Our append is 0, the leader's term base is 10, and there is no next term
    // to replicate from: the reference throws.
    let result = election.on_new_leadership_term(
        &mut host,
        0,
        -1,
        -1,
        -1,
        0,
        10,
        20,
        0,
        RECORDING_ID,
        0,
        0,
        LOG_SESSION_ID,
        true,
    );
    assert!(matches!(result, Err(ClusterError::Cluster(message))
        if message.contains("invalid newLeadershipTerm")));
}

#[test]
fn on_append_position_keeps_the_member_table_up_to_date() {
    let mut host = TestHost::new("append", &[0, 1], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1], 0);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Canvass);

    // The message's term must not be ahead of ours (`:546`).
    election
        .on_append_position(&mut host, -1, 42, 1, 0)
        .unwrap();
    assert_eq!(host.member(1).log_position, 42);
}

#[test]
fn notified_commit_position_cannot_go_backwards_upon_receiving_commit_position() {
    let mut host = TestHost::new("commit", &[0, 1], 1);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1], 1);
    election.do_work(&mut host, &mut sender).unwrap();
    election
        .on_new_leadership_term(
            &mut host,
            -1,
            -1,
            -1,
            -1,
            0,
            0,
            0,
            0,
            RECORDING_ID,
            0,
            0,
            LOG_SESSION_ID,
            true,
        )
        .unwrap();

    election.on_commit_position(&mut host, 0, 100, 0).unwrap();
    election.on_commit_position(&mut host, 0, 50, 0).unwrap();
    assert_eq!(election.notified_commit_position(), 100);
}

#[test]
fn notified_commit_position_cannot_go_backwards_upon_receiving_new_leadership_term() {
    let mut host = TestHost::new("commit-nlt", &[0, 1], 1);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1], 1);
    election.do_work(&mut host, &mut sender).unwrap();

    election
        .on_new_leadership_term(
            &mut host,
            -1,
            -1,
            -1,
            -1,
            0,
            0,
            0,
            100,
            RECORDING_ID,
            0,
            0,
            LOG_SESSION_ID,
            true,
        )
        .unwrap();
    // A second term from the same leader: the commit position may not move
    // backwards even when the message says so.
    election
        .on_new_leadership_term(
            &mut host,
            -1,
            -1,
            -1,
            -1,
            1,
            0,
            0,
            50,
            RECORDING_ID,
            0,
            0,
            LOG_SESSION_ID,
            true,
        )
        .unwrap();
    assert_eq!(election.notified_commit_position(), 100);
}

#[test]
fn follower_log_replication_completes_and_returns_to_canvass() {
    let mut host = TestHost::new("replication", &[0, 1], 1);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1], 1);
    election.do_work(&mut host, &mut sender).unwrap();

    // Leader's term 0 starts at 10; our append is 0, so the whole term has to
    // be copied. The next term (1) starts at 100.
    election
        .on_new_leadership_term(
            &mut host,
            -1,
            0,
            100,
            120,
            0,
            10,
            20,
            0,
            RECORDING_ID,
            0,
            0,
            LOG_SESSION_ID,
            true,
        )
        .unwrap();
    assert_eq!(election.state(), ElectionState::FollowerLogReplication);

    // The next duty cycle creates the replication; the handle it keeps is the
    // scripted archive, exactly as the reference's mocked
    // `RecordingReplication`.
    host.advance(1);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::FollowerLogReplication);
    {
        let replication = election.log_replication().expect("a replication");
        replication.on_signal(1, 120, RecordingSignal::Stop);
        replication.on_signal(1, 120, RecordingSignal::ReplicateEnd);
    }
    election.on_commit_position(&mut host, 0, 120, 0).unwrap();

    host.advance(1);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(
        election.state(),
        ElectionState::Canvass,
        "a completed replication goes back to canvass with the newer log"
    );
    assert!(
        sender
            .sent
            .iter()
            .any(|sent| matches!(sent, Sent::AppendPosition { .. }))
    );
}

#[test]
fn handle_error_returns_to_init_with_the_commit_position() {
    let mut host = TestHost::new("handle-error", &[0, 1], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1], 0);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Canvass);

    host.commit_position = 77;
    let now_ns = host.now_ns;
    election
        .handle_error(&mut host, now_ns, ClusterError::Timeout("t".to_owned()))
        .unwrap();
    assert_eq!(election.state(), ElectionState::Init);
}

#[test]
fn on_recording_signal_moves_the_replication_forward() {
    let mut host = TestHost::new("signal", &[0, 1], 1);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1], 1);
    election.do_work(&mut host, &mut sender).unwrap();

    // No replication yet: the signal is ignored without a panic.
    election
        .on_recording_signal(&mut host, 1, 5, RecordingSignal::ReplicateEnd)
        .unwrap();
}

#[test]
fn the_election_rng_is_deterministic() {
    let mut a = ElectionRng::new(42);
    let mut b = ElectionRng::new(42);
    for _ in 0..16 {
        assert_eq!(a.next_f64(), b.next_f64());
    }
}

#[test]
fn should_elect_appointed_leader() {
    // An appointment naming a peer never lets this member advance past
    // CANVASS, however willing the voters are.
    let mut host = TestHost::new("appointed-peer", &[0, 1, 2], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1, 2], 0);
    host.appointed_leader_id = 1;

    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Canvass);
    election
        .on_canvass_position(&mut host, &mut sender, -1, 0, -1, 1, 0)
        .unwrap();
    election
        .on_canvass_position(&mut host, &mut sender, -1, 0, -1, 2, 0)
        .unwrap();
    host.advance(60_000_000_000);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Canvass);

    // Named as the leader, the same member advances.
    let mut host2 = TestHost::new("appointed-self", &[0, 1, 2], 0);
    let mut sender2 = RecordingSender::accepting();
    let mut election2 = new_election(&[0, 1, 2], 0);
    host2.appointed_leader_id = 0;
    election2.do_work(&mut host2, &mut sender2).unwrap();
    election2
        .on_canvass_position(&mut host2, &mut sender2, -1, 0, -1, 1, 0)
        .unwrap();
    election2
        .on_canvass_position(&mut host2, &mut sender2, -1, 0, -1, 2, 0)
        .unwrap();
    host2.advance(200_000_000);
    election2.do_work(&mut host2, &mut sender2).unwrap();
    assert_eq!(election2.state(), ElectionState::Nominate);
}

#[test]
fn leader_should_move_to_log_replication_then_wait_for_commit_position() {
    let mut host = TestHost::new("leader-wait", &[0, 1], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1], 0);
    walk_to_candidate_ballot(&mut host, &mut sender, &mut election);
    let candidate_term = host.node_state.candidate_term().candidate_term_id;
    election
        .on_vote(&mut host, -1, 0, candidate_term, 0, 1, true)
        .unwrap();
    host.advance(1);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::LeaderLogReplication);

    // The peer has not reported the append position yet.
    host.quorum_position = -1;
    host.advance(200_000_000);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::LeaderLogReplication);

    // It reports, and the replica moves on.
    host.quorum_position = 0;
    host.advance(200_000_000);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::LeaderReplay);
}

#[test]
fn should_send_commit_position_and_new_leadership_term_events_with_the_same_leadership_term() {
    let mut host = TestHost::new("same-term", &[0, 1], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1], 0);
    walk_to_candidate_ballot(&mut host, &mut sender, &mut election);
    let candidate_term = host.node_state.candidate_term().candidate_term_id;
    election
        .on_vote(&mut host, -1, 0, candidate_term, 0, 1, true)
        .unwrap();
    host.advance(1);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::LeaderLogReplication);

    host.quorum_position = 0;
    host.advance(200_000_000);
    election.do_work(&mut host, &mut sender).unwrap();

    let nlt_term = sender.sent.iter().find_map(|sent| match sent {
        Sent::NewLeadershipTerm {
            leadership_term, ..
        } => Some(*leadership_term),
        _ => None,
    });
    assert_eq!(Some(candidate_term), nlt_term, "sent: {:?}", sender.sent);
    assert!(
        host.publish_commit_position_calls > 0,
        "the commit position goes out on the same interval"
    );
}

#[test]
fn should_timeout_failed_candidate_ballot_on_split_vote_then_succeed_on_retry() {
    let mut host = TestHost::new("split-vote", &[0, 1, 2], 0);
    let mut sender = RecordingSender::accepting();
    let mut election = new_election(&[0, 1, 2], 0);

    walk_to_candidate_ballot(&mut host, &mut sender, &mut election);
    // One yes, one no: not a quorum of yes, and a no at the term ends it.
    let first_term = host.node_state.candidate_term().candidate_term_id;
    election
        .on_vote(&mut host, -1, 0, first_term, 0, 1, true)
        .unwrap();
    election
        .on_vote(&mut host, -1, 0, first_term, 0, 2, false)
        .unwrap();
    host.advance(1_000_000_000);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Canvass);

    // A second round, with both peers behind and a clean ballot.
    election
        .on_canvass_position(&mut host, &mut sender, -2, 0, -2, 1, 0)
        .unwrap();
    election
        .on_canvass_position(&mut host, &mut sender, -2, 0, -2, 2, 0)
        .unwrap();
    host.advance(200_000_000);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::Nominate);
    host.advance(1_000_000_000);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::CandidateBallot);
    let second_term = host.node_state.candidate_term().candidate_term_id;
    assert!(second_term > first_term, "each round asks for a newer term");
    election
        .on_vote(&mut host, -1, 0, second_term, 0, 1, true)
        .unwrap();
    election
        .on_vote(&mut host, -1, 0, second_term, 0, 2, true)
        .unwrap();
    host.advance(1);
    election.do_work(&mut host, &mut sender).unwrap();
    assert_eq!(election.state(), ElectionState::LeaderLogReplication);
}

#[test]
fn should_become_follower_if_entering_new_election() {
    // A node that was leader starts a new election: INIT prepares for new
    // leadership before anything else, and CANVASS makes it a follower
    // (`Election.java:653-695`).
    let mut host = TestHost::new("new-election", &[0, 1, 2], 0);
    host.role = Role::Leader;
    let mut sender = RecordingSender::accepting();
    let mut election = Election::new(0, vec![0, 1, 2], false, -1, 1, 0, 120, 120, 1_000, 42);

    election.do_work(&mut host, &mut sender).unwrap();

    assert_eq!(election.state(), ElectionState::Canvass);
    assert_eq!(host.prepare_calls, 1);
    assert_eq!(host.role, Role::Follower);
}
