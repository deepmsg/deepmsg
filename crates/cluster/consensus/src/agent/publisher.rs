//! What the election sends to other members, for a one-member cluster.
//!
//! `P3-4a` refuses a configuration with more than one member (`launch` says so
//! rather than starting a node whose election would stall in `CANVASS`), so
//! there is no other member to send to and every method here is a no-op. The
//! type exists now because the frozen [`ConsensusSender`] seam returns it and
//! the duty cycle takes it out of the agent — the shape P3-6a fills in with
//! per-member publications.

use crate::election::ConsensusSender;

/// The per-member consensus publications.
///
/// The reference keeps one publication per other member on `ClusterMember`
/// (`ClusterMember.addConsensusPublications`) and this struct is where that
/// table will live. It holds nothing today: `P3-4a` has no other members.
#[derive(Debug, Default)]
pub struct ConsensusPublisher {
    /// Whether any publication exists. Always false until P3-6a.
    publications: usize,
}

impl ConsensusPublisher {
    /// A publisher for a cluster with `other_members` members besides this one.
    pub const fn new(other_members: usize) -> Self {
        Self {
            publications: other_members,
        }
    }
}

impl ConsensusSender for ConsensusPublisher {
    fn canvass_position(
        &mut self,
        _member_id: i32,
        _log_leadership_term_id: i64,
        _log_position: i64,
        _leadership_term_id: i64,
        _follower_member_id: i32,
    ) {
        debug_assert_eq!(0, self.publications, "P3-6a has not created publications");
    }

    fn request_vote(
        &mut self,
        _member_id: i32,
        _log_leadership_term_id: i64,
        _log_position: i64,
        _candidate_term_id: i64,
        _candidate_member_id: i32,
    ) -> bool {
        debug_assert_eq!(0, self.publications, "P3-6a has not created publications");
        false
    }

    fn place_vote(
        &mut self,
        _member_id: i32,
        _candidate_term_id: i64,
        _log_leadership_term_id: i64,
        _log_position: i64,
        _candidate_member_id: i32,
        _follower_member_id: i32,
        _vote: bool,
    ) {
        debug_assert_eq!(0, self.publications, "P3-6a has not created publications");
    }

    #[allow(clippy::too_many_arguments)]
    fn new_leadership_term(
        &mut self,
        _member_id: i32,
        _log_leadership_term_id: i64,
        _next_leadership_term_id: i64,
        _next_term_base_log_position: i64,
        _next_log_position: i64,
        _leadership_term_id: i64,
        _term_base_log_position: i64,
        _log_position: i64,
        _commit_position: i64,
        _leader_recording_id: i64,
        _timestamp: i64,
        _leader_member_id: i32,
        _log_session_id: i32,
        _app_version: i32,
        _is_startup: bool,
    ) {
        debug_assert_eq!(0, self.publications, "P3-6a has not created publications");
    }

    fn append_position(
        &mut self,
        _member_id: i32,
        _leadership_term_id: i64,
        _log_position: i64,
        _follower_member_id: i32,
        _flags: i16,
    ) -> bool {
        debug_assert_eq!(0, self.publications, "P3-6a has not created publications");
        false
    }

    fn catchup_position(
        &mut self,
        _member_id: i32,
        _leadership_term_id: i64,
        _log_position: i64,
        _follower_member_id: i32,
        _catchup_endpoint: &str,
    ) -> bool {
        debug_assert_eq!(0, self.publications, "P3-6a has not created publications");
        false
    }
}
