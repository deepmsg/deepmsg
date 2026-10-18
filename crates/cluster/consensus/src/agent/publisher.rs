//! `ConsensusPublisher`: the fixed-shape messages the election and the
//! position engine send to other members.
//!
//! The reference keeps one `ExclusivePublication` per other member on
//! `ClusterMember` (`ClusterMember.addConsensusPublications` `:927-953`) and
//! every message is `tryClaim` + an SBE flyweight + `commit`
//! (`ConsensusPublisher.java:53-575`). This keeps the same table, one entry per
//! member id, and the same claim discipline.
//!
//! # Where the position limit comes from
//!
//! The reference's `tryClaim` reads the publication's term limit itself. The
//! client here asks for it (`ExclusivePublication::try_claim(position_limit,
//! length)`), and the publisher has no client — the election takes it out of
//! the agent for the duration of a turn. So the limits are refreshed into the
//! table once per turn by [`ConsensusPublisher::refresh_position_limits`], from
//! the same counters the reference's publication would read.
//!
//! # Errors
//!
//! The reference throws out of `checkResult` on a closed publication or an
//! exhausted position. The frozen [`ConsensusSender`] trait cannot carry a
//! `Result`, so the first such error is kept and handed back through
//! [`ConsensusPublisher::take_error`], which the agent checks after every turn
//! that used the sender. A send that could not be made for want of a
//! publication or back-pressure is not an error: it is the next interval's
//! work, exactly as in the reference.

use std::cell::RefCell;
use std::rc::Rc;

use deepmsg_client::client::Client;
use deepmsg_client::publication::ExclusivePublication;
use deepmsg_codec_cluster::boolean_type::BooleanType;
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::{
    WriteBuf, append_position_codec::AppendPositionEncoder,
    canvass_position_codec::CanvassPositionEncoder, catchup_position_codec::CatchupPositionEncoder,
    commit_position_codec::CommitPositionEncoder,
    new_leadership_term_codec::NewLeadershipTermEncoder, request_vote_codec::RequestVoteEncoder,
    stop_catchup_codec::StopCatchupEncoder, termination_ack_codec::TerminationAckEncoder,
    termination_position_codec::TerminationPositionEncoder, vote_codec::VoteEncoder,
};
use deepmsg_core::logbuffer::append::Appended;
use deepmsg_core::logbuffer::frame::DATA_HEADER_LENGTH;

use crate::election::ConsensusSender;
use crate::error::ClusterError;

/// `ConsensusModule.Configuration.PROTOCOL_SEMANTIC_VERSION` — schema 111,
/// version 17 (`MemoryOrder` 5.4). Sent in the two messages that carry it.
pub const PROTOCOL_SEMANTIC_VERSION: i32 = 17;

/// `ConsensusPublisher.SEND_ATTEMPTS` (`:29`).
const SEND_ATTEMPTS: usize = 3;

/// Room the shared encode buffer is given before a message is written into it.
/// The reference wraps an `ExpandableArrayBuffer` that grows; this one is
/// fixed because a steady-state send may not allocate (performance spec R8).
/// The largest fixed consensus message is far below this (`NewLeadershipTerm`
/// is the widest at a few dozen bytes).
const SCRATCH_LENGTH: usize = 512;

struct MemberPublication {
    member_id: i32,
    publication: ExclusivePublication,
    /// The value `try_claim` needs, refreshed once per turn.
    position_limit: i64,
}

/// What the table actually holds. Shared through [`ConsensusPublisher`].
#[derive(Default)]
struct PublisherState {
    publications: Vec<MemberPublication>,
    buffer: Vec<u8>,
    error: Option<ClusterError>,
}

/// The per-member consensus publications.
///
/// **Shared, not moved.** An election turn takes the sender out of the agent
/// (`election.do_work(self, &mut publisher)`), and the *host* methods the
/// election calls back into (`catchup_poll` → `updateFollowerPosition`, a
/// leader's `publishCommitPosition`) send through the same table. When the
/// table was a value, `mem::take` left the agent holding an empty one for the
/// length of that turn: every send a host method made found no publication and
/// quietly did nothing — the follower's catch-up heartbeats never reached the
/// leader, so the leader never sent `StopCatchup` and the follower purged back
/// into `Init` every heartbeat timeout. The `Rc<RefCell<_>>` is what lets the
/// taken-out sender and the agent's own field be the **same** table, which is
/// what the reference's two object references are.
#[derive(Clone, Default)]
pub struct ConsensusPublisher {
    state: Rc<RefCell<PublisherState>>,
}

impl ConsensusPublisher {
    /// An empty table. The agent fills it with
    /// [`ConsensusPublisher::add_publication`] for every other member.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The `ClusterMember.publication` slot (`ClusterMember.java:44`).
    pub fn add_publication(&mut self, member_id: i32, publication: ExclusivePublication) {
        let mut state = self.state.borrow_mut();
        state.publications.push(MemberPublication {
            member_id,
            publication,
            position_limit: 0,
        });
        state.publications.sort_by_key(|entry| entry.member_id);
    }

    /// Whether `publishCanvassPosition` would find a publication
    /// (`Election.java:1238`).
    #[must_use]
    pub fn has_publication(&self, member_id: i32) -> bool {
        self.state
            .borrow()
            .publications
            .iter()
            .any(|entry| entry.member_id == member_id)
    }

    /// The member ids this table can reach.
    #[must_use]
    pub fn member_ids(&self) -> Vec<i32> {
        self.state
            .borrow()
            .publications
            .iter()
            .map(|entry| entry.member_id)
            .collect()
    }

    /// Read every publication's position limit. Called once per turn before the
    /// sender is used; the election has no client.
    pub fn refresh_position_limits(&mut self, client: &Client) {
        let Some(counters) = client.counters_reader() else {
            return;
        };
        for entry in &mut self.state.borrow_mut().publications {
            entry.position_limit = counters
                .value(entry.publication.position_limit_counter_id())
                .unwrap_or(0);
        }
    }

    /// The first fatal publication error a send met, if any.
    pub fn take_error(&mut self) -> Option<ClusterError> {
        self.state.borrow_mut().error.take()
    }
}

impl PublisherState {
    fn publication(&self, member_id: i32) -> Option<&MemberPublication> {
        self.publications
            .iter()
            .find(|entry| entry.member_id == member_id)
    }

    fn set_error(&mut self, error: ClusterError) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }

    /// Claim `length` bytes on the member's publication, write `encoded` into
    /// it and publish. The shape every fixed message shares
    /// (`ConsensusPublisher.java:53-71`).
    fn claim_and_publish(&mut self, member_id: i32, encoded: &[u8]) -> bool {
        let Some(entry) = self.publication(member_id) else {
            return false;
        };
        let position_limit = entry.position_limit;
        let publication = &entry.publication;
        let length = encoded.len();

        for _ in 0..SEND_ATTEMPTS {
            match publication.try_claim(position_limit, length) {
                Ok(claim) => {
                    if claim.frame().write_payload(encoded).is_none() {
                        self.set_error(ClusterError::Cluster(
                            "failed to write a consensus message".to_owned(),
                        ));
                        return false;
                    }
                    // The unaligned frame length is what `publish` takes, so the
                    // padding is not payload (`log_publisher.rs`'s claim path).
                    #[allow(clippy::cast_possible_truncation)] // bounded by a term length
                    let unaligned = (DATA_HEADER_LENGTH + length) as i32;
                    if claim.frame().publish(unaligned).is_none() {
                        self.set_error(ClusterError::Cluster(
                            "failed to publish a consensus message".to_owned(),
                        ));
                        return false;
                    }
                    return true;
                }
                Err(Appended::EndOfLog)
                | Err(Appended::BackPressured)
                | Err(Appended::MidRotation) => {}
                Err(Appended::NotConnected) => return false,
                Err(Appended::MaxPositionExceeded) => {
                    self.set_error(ClusterError::Cluster(
                        "publication at max position".to_owned(),
                    ));
                    return false;
                }
                Err(_) => {
                    self.set_error(ClusterError::Cluster(
                        "the consensus publication refused the message".to_owned(),
                    ));
                    return false;
                }
            }
        }
        false
    }
}

impl ConsensusPublisher {
    /// Encode into the shared buffer, then claim and publish.
    fn send<F>(&mut self, member_id: i32, encode: F) -> bool
    where
        F: FnOnce(&mut Vec<u8>) -> usize,
    {
        let mut state = self.state.borrow_mut();
        if state.publication(member_id).is_none() {
            return false;
        }
        // The buffer is moved out so the claim can borrow the rest of the table
        // while the encoder writes into it. It is sized before the encoder sees
        // it: `WriteBuf` indexes, it does not grow.
        let mut buffer = std::mem::take(&mut state.buffer);
        buffer.clear();
        buffer.resize(SCRATCH_LENGTH, 0);
        let length = encode(&mut buffer);
        debug_assert!(
            length <= SCRATCH_LENGTH,
            "a consensus message outgrew {SCRATCH_LENGTH}"
        );
        let published = state.claim_and_publish(member_id, &buffer[..length]);
        state.buffer = buffer;
        published
    }

    // ---- the fixed messages ------------------------------------------------

    /// `canvassPosition` (`:53-89`).
    pub fn canvass_position(
        &mut self,
        member_id: i32,
        log_leadership_term_id: i64,
        log_position: i64,
        leadership_term_id: i64,
        follower_member_id: i32,
        protocol_version: i32,
    ) {
        self.send(member_id, |buffer| {
            let encoder = CanvassPositionEncoder::default()
                .wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .log_leadership_term_id(log_leadership_term_id)
                .log_position(log_position)
                .leadership_term_id(leadership_term_id)
                .follower_member_id(follower_member_id)
                .protocol_version(protocol_version);
            MESSAGE_HEADER_LENGTH + message.encoded_length()
        });
    }

    /// `requestVote` (`:91-129`).
    pub fn request_vote(
        &mut self,
        member_id: i32,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_term_id: i64,
        candidate_member_id: i32,
        protocol_version: i32,
    ) -> bool {
        self.send(member_id, |buffer| {
            let encoder =
                RequestVoteEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .log_leadership_term_id(log_leadership_term_id)
                .log_position(log_position)
                .candidate_term_id(candidate_term_id)
                .candidate_member_id(candidate_member_id)
                .protocol_version(protocol_version);
            MESSAGE_HEADER_LENGTH + message.encoded_length()
        })
    }

    /// `placeVote` (`:131-170`).
    #[allow(clippy::too_many_arguments)]
    pub fn place_vote(
        &mut self,
        member_id: i32,
        candidate_term_id: i64,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_member_id: i32,
        follower_member_id: i32,
        vote: bool,
    ) {
        self.send(member_id, |buffer| {
            let encoder = VoteEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .candidate_term_id(candidate_term_id)
                .log_leadership_term_id(log_leadership_term_id)
                .log_position(log_position)
                .candidate_member_id(candidate_member_id)
                .follower_member_id(follower_member_id)
                .vote(boolean(vote));
            MESSAGE_HEADER_LENGTH + message.encoded_length()
        });
    }

    /// `newLeadershipTerm` (`:172-232`).
    #[allow(clippy::too_many_arguments)]
    pub fn new_leadership_term(
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
        timestamp: i64,
        leader_member_id: i32,
        log_session_id: i32,
        app_version: i32,
        is_startup: bool,
    ) {
        self.send(member_id, |buffer| {
            let encoder = NewLeadershipTermEncoder::default()
                .wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .log_leadership_term_id(log_leadership_term_id)
                .next_leadership_term_id(next_leadership_term_id)
                .next_term_base_log_position(next_term_base_log_position)
                .next_log_position(next_log_position)
                .leadership_term_id(leadership_term_id)
                .term_base_log_position(term_base_log_position)
                .log_position(log_position)
                .commit_position(commit_position)
                .leader_recording_id(leader_recording_id)
                .timestamp(timestamp)
                .leader_member_id(leader_member_id)
                .log_session_id(log_session_id)
                .app_version(app_version)
                .is_startup(boolean(is_startup));
            MESSAGE_HEADER_LENGTH + message.encoded_length()
        });
    }

    /// `appendPosition` (`:234-271`).
    pub fn append_position(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        flags: i16,
    ) -> bool {
        self.send(member_id, |buffer| {
            let encoder =
                AppendPositionEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .leadership_term_id(leadership_term_id)
                .log_position(log_position)
                .follower_member_id(follower_member_id)
                .flags(u8::try_from(flags).unwrap_or(0));
            MESSAGE_HEADER_LENGTH + message.encoded_length()
        })
    }

    /// `commitPosition` (`:273-306`).
    pub fn commit_position(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        log_position: i64,
        leader_member_id: i32,
    ) {
        self.send(member_id, |buffer| {
            let encoder =
                CommitPositionEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .leadership_term_id(leadership_term_id)
                .log_position(log_position)
                .leader_member_id(leader_member_id);
            MESSAGE_HEADER_LENGTH + message.encoded_length()
        });
    }

    /// `catchupPosition` (`:308-349`).
    pub fn catchup_position(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        catchup_endpoint: &str,
    ) -> bool {
        self.send(member_id, |buffer| {
            let encoder = CatchupPositionEncoder::default()
                .wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .leadership_term_id(leadership_term_id)
                .log_position(log_position)
                .follower_member_id(follower_member_id)
                .catchup_endpoint(catchup_endpoint.as_bytes());
            MESSAGE_HEADER_LENGTH + message.encoded_length()
        })
    }

    /// `stopCatchup` (`:351-381`). Answers whether the message was sent: the
    /// leader clears a follower's catchup session id only when it was.
    pub fn stop_catchup(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        follower_member_id: i32,
    ) -> bool {
        self.send(member_id, |buffer| {
            let encoder =
                StopCatchupEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .leadership_term_id(leadership_term_id)
                .follower_member_id(follower_member_id);
            MESSAGE_HEADER_LENGTH + message.encoded_length()
        })
    }

    /// `terminationPosition` (`:383-416`).
    pub fn termination_position(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        log_position: i64,
    ) -> bool {
        self.send(member_id, |buffer| {
            let encoder = TerminationPositionEncoder::default()
                .wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .leadership_term_id(leadership_term_id)
                .log_position(log_position);
            MESSAGE_HEADER_LENGTH + message.encoded_length()
        })
    }

    /// `terminationAck` (`:417-450`).
    pub fn termination_ack(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
    ) -> bool {
        self.send(member_id, |buffer| {
            let encoder =
                TerminationAckEncoder::default().wrap(WriteBuf::new(buffer), MESSAGE_HEADER_LENGTH);
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .leadership_term_id(leadership_term_id)
                .log_position(log_position)
                .member_id(follower_member_id);
            MESSAGE_HEADER_LENGTH + message.encoded_length()
        })
    }
}

fn boolean(value: bool) -> BooleanType {
    if value {
        BooleanType::TRUE
    } else {
        BooleanType::FALSE
    }
}

impl ConsensusSender for ConsensusPublisher {
    fn canvass_position(
        &mut self,
        member_id: i32,
        log_leadership_term_id: i64,
        log_position: i64,
        leadership_term_id: i64,
        follower_member_id: i32,
    ) {
        Self::canvass_position(
            self,
            member_id,
            log_leadership_term_id,
            log_position,
            leadership_term_id,
            follower_member_id,
            PROTOCOL_SEMANTIC_VERSION,
        );
    }

    fn request_vote(
        &mut self,
        member_id: i32,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_term_id: i64,
        candidate_member_id: i32,
    ) -> bool {
        Self::request_vote(
            self,
            member_id,
            log_leadership_term_id,
            log_position,
            candidate_term_id,
            candidate_member_id,
            PROTOCOL_SEMANTIC_VERSION,
        )
    }

    fn place_vote(
        &mut self,
        member_id: i32,
        candidate_term_id: i64,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_member_id: i32,
        follower_member_id: i32,
        vote: bool,
    ) {
        Self::place_vote(
            self,
            member_id,
            candidate_term_id,
            log_leadership_term_id,
            log_position,
            candidate_member_id,
            follower_member_id,
            vote,
        );
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
        timestamp: i64,
        leader_member_id: i32,
        log_session_id: i32,
        app_version: i32,
        is_startup: bool,
    ) {
        Self::new_leadership_term(
            self,
            member_id,
            log_leadership_term_id,
            next_leadership_term_id,
            next_term_base_log_position,
            next_log_position,
            leadership_term_id,
            term_base_log_position,
            log_position,
            commit_position,
            leader_recording_id,
            timestamp,
            leader_member_id,
            log_session_id,
            app_version,
            is_startup,
        );
    }

    fn append_position(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        flags: i16,
    ) -> bool {
        Self::append_position(
            self,
            member_id,
            leadership_term_id,
            log_position,
            follower_member_id,
            flags,
        )
    }

    fn catchup_position(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        catchup_endpoint: &str,
    ) -> bool {
        Self::catchup_position(
            self,
            member_id,
            leadership_term_id,
            log_position,
            follower_member_id,
            catchup_endpoint,
        )
    }
}
