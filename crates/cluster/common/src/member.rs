//! Cluster members: the `aeron.cluster.members` string, and the quorum
//! arithmetic that decides who leads.
//!
//! Counterpart of `ClusterMember.java:540-1210`. Two halves:
//!
//! * **The string.** `id,ingress,consensus,log,catchup,archive[,archiveResponse[,egressResponse]]`
//!   per member, members separated by `|`. Six attributes are required, eight
//!   at most (`:561-564`). Both optional endpoints are kept — the seventh is
//!   read by the log replication (`P3-0e`), the eighth by nothing in 1.53.2.
//! * **The arithmetic.** `quorumThreshold`, `quorumPosition`,
//!   `hasQuorumAtPosition`, `compareLog` and the vote predicates. These are
//!   pure functions over members and a clock, and they are where an election
//!   is decided — so each one names its reference line and each has a test.
//!
//! # Why `quorumPosition` takes a scratch array
//!
//! It ranks the live members' positions by **insertion into a preallocated
//! top-k list** and answers the k-th, where k is the quorum threshold. The
//! reference passes the array in for the same reason this does: it is called
//! every duty cycle and must not allocate (performance spec R8).

use std::fmt;

/// `ClusterMember.NULL_POSITION`.
pub const NULL_POSITION: i64 = -1;

/// Why a member string could not be parsed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemberError {
    /// Fewer than six or more than eight comma-separated attributes.
    AttributeCount {
        /// The offending member text.
        member: String,
        /// How many attributes it had.
        found: usize,
    },
    /// The id is not an integer.
    Id {
        /// The text that was supposed to be an id.
        value: String,
    },
}

impl fmt::Display for MemberError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AttributeCount { member, found } => {
                write!(f, "invalid member value, {found} attributes: {member}")
            }
            Self::Id { value } => write!(f, "invalid cluster member id: {value:?}"),
        }
    }
}

impl std::error::Error for MemberError {}

/// One cluster member: its endpoints, and the election state the quorum
/// functions read.
///
/// The endpoints are immutable for the life of a cluster; everything from
/// `leadership_term_id` down is per-election state, exactly as the reference
/// keeps it on the same object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterMember {
    /// The member id, first field of the string.
    pub id: i32,
    /// `ingress` endpoint.
    pub ingress_endpoint: String,
    /// `consensus` endpoint.
    pub consensus_endpoint: String,
    /// `log` endpoint.
    pub log_endpoint: String,
    /// `catchup` endpoint.
    pub catchup_endpoint: String,
    /// `archive` endpoint.
    pub archive_endpoint: String,
    /// The seventh attribute, or `None`. Read by log replication (`P3-0e`).
    pub archive_response_endpoint: Option<String>,
    /// The eighth attribute, or `None`. Read by nothing in 1.53.2.
    pub egress_response_endpoint: Option<String>,
    /// The attributes joined back together, as the reference keeps them.
    pub endpoints: String,

    /// The log's leadership term, as this member last reported it.
    pub leadership_term_id: i64,
    /// The log position, as this member last reported it.
    pub log_position: i64,
    /// The term this member is contesting or has voted in.
    pub candidate_term_id: i64,
    /// When this member last reported an append position, for liveness.
    pub time_of_last_append_position_ns: i64,
    /// This member's vote in the current ballot, if it has cast one.
    pub vote: Option<bool>,
    /// Whether a vote request has already gone to this member.
    pub is_ballot_sent: bool,
    /// The catchup replay session this member is serving, or `-1`.
    pub catchup_replay_session_id: i64,
    /// Whether this member is the leader of the current term
    /// (`ClusterMember.isLeader`, `:46`). Set when a term is established
    /// (`ConsensusModuleAgent.updateMemberDetails` `:2370-2381`).
    pub is_leader: bool,
    /// Whether this member has acknowledged a `TerminationPosition`
    /// (`ClusterMember.hasTerminated`, `:47`). Cleared by the leader before a
    /// new round of termination positions (`ClusterTermination.java:73`).
    pub has_terminated: bool,
}

impl ClusterMember {
    /// Parse an `aeron.cluster.members` string.
    ///
    /// An empty or absent value is an empty list, as in the reference.
    ///
    /// # Errors
    ///
    /// [`MemberError`] for a member with the wrong number of attributes or a
    /// non-integer id.
    pub fn parse_list(value: &str) -> Result<Vec<Self>, MemberError> {
        if value.is_empty() {
            return Ok(Vec::new());
        }
        value.split('|').map(Self::parse).collect()
    }

    /// Parse one member: `id,ingress,consensus,log,catchup,archive[,resp[,egress]]`.
    ///
    /// # Errors
    ///
    /// [`MemberError`] as above.
    pub fn parse(value: &str) -> Result<Self, MemberError> {
        let attributes: Vec<&str> = value.split(',').collect();
        if attributes.len() < 6 || attributes.len() > 8 {
            return Err(MemberError::AttributeCount {
                member: value.to_owned(),
                found: attributes.len(),
            });
        }
        let id = attributes[0].parse::<i32>().map_err(|_| MemberError::Id {
            value: attributes[0].to_owned(),
        })?;
        let archive_response_endpoint = attributes.get(6).map(|s| (*s).to_owned());
        let egress_response_endpoint = attributes.get(7).map(|s| (*s).to_owned());
        let endpoints = attributes[1..6].join(",");

        Ok(Self {
            id,
            ingress_endpoint: attributes[1].to_owned(),
            consensus_endpoint: attributes[2].to_owned(),
            log_endpoint: attributes[3].to_owned(),
            catchup_endpoint: attributes[4].to_owned(),
            archive_endpoint: attributes[5].to_owned(),
            archive_response_endpoint,
            egress_response_endpoint,
            endpoints,
            leadership_term_id: -1,
            log_position: NULL_POSITION,
            candidate_term_id: -1,
            time_of_last_append_position_ns: 0,
            vote: None,
            is_ballot_sent: false,
            catchup_replay_session_id: -1,
            is_leader: false,
            has_terminated: false,
        })
    }

    /// `ClusterMember.setIsLeader` (`:1263-1270`): mark every member whose id
    /// is the leader's.
    pub fn set_is_leader(members: &mut [Self], leader_member_id: i32) {
        for member in members {
            member.is_leader = member.id == leader_member_id;
        }
    }

    /// `ClusterMember.reset()` (`:140-149`): forget everything that belongs to
    /// a term. The endpoints stay — they name the member, not the term.
    pub fn reset(&mut self) {
        self.is_ballot_sent = false;
        self.is_leader = false;
        self.has_terminated = false;
        self.vote = None;
        self.candidate_term_id = -1;
        self.leadership_term_id = -1;
        self.log_position = NULL_POSITION;
    }

    /// `ClusterMember.reset(members)` (`:932-938`).
    pub fn reset_all(members: &mut [Self]) {
        for member in members {
            member.reset();
        }
    }

    /// `ClusterMember.becomeCandidate` (`:947-968`): the candidate votes for
    /// itself and everyone else's ballot starts from nothing.
    pub fn become_candidate(
        members: &mut [Self],
        candidate_term_id: i64,
        candidate_member_id: i32,
    ) {
        for member in members {
            if member.id == candidate_member_id {
                member.vote = Some(true);
                member.candidate_term_id = candidate_term_id;
                member.is_ballot_sent = true;
            } else {
                member.vote = None;
                member.candidate_term_id = -1;
                member.is_ballot_sent = false;
            }
        }
    }

    /// `ClusterMember.isUnanimousLeader` (`:981-1006`): every member that is
    /// not the gracefully closed leader voted yes at `candidate_term_id`, and
    /// there are at least a quorum of them.
    pub fn is_unanimous_leader(
        members: &[Self],
        candidate_term_id: i64,
        graceful_closed_leader_id: i32,
    ) -> bool {
        let mut votes = 0;
        for member in members {
            if member.id == graceful_closed_leader_id {
                continue;
            }
            if candidate_term_id != member.candidate_term_id || member.vote != Some(true) {
                return false;
            }
            votes += 1;
        }
        votes >= quorum_threshold(members.len())
    }

    /// `ClusterMember.isUnanimousCandidate` (`:1117-1140`): every member that
    /// is not the gracefully closed leader would vote for `candidate`.
    pub fn is_unanimous_candidate(
        members: &[Self],
        candidate: &Self,
        graceful_closed_leader_id: i32,
    ) -> bool {
        let mut possible_votes = 0;
        for member in members {
            if member.id == graceful_closed_leader_id {
                continue;
            }
            if !member.will_vote_for(candidate) {
                return false;
            }
            possible_votes += 1;
        }
        possible_votes >= quorum_threshold(members.len())
    }

    /// Whether this member reported an append position within `timeout_ns`.
    /// `isActive` (`:1287`).
    pub fn is_active(&self, now_ns: i64, timeout_ns: i64) -> bool {
        self.time_of_last_append_position_ns + timeout_ns > now_ns
    }

    /// Whether this member has reached `position` in `leadership_term_id`, and
    /// is still alive. `hasReachedPosition` (`:1296`).
    pub fn has_reached_position(
        &self,
        leadership_term_id: i64,
        position: i64,
        now_ns: i64,
        timeout_ns: i64,
    ) -> bool {
        self.is_active(now_ns, timeout_ns)
            && leadership_term_id == self.leadership_term_id
            && self.log_position >= position
    }

    /// Whether this member would vote for `candidate`. `willVoteFor` (`:1292`).
    ///
    /// **`is_active` is deliberately not checked** — the reference says why:
    /// checking it would stop a node proposing itself when other nodes' append
    /// positions stop advancing, which is exactly the crash and partition case
    /// the election exists for.
    pub fn will_vote_for(&self, candidate: &Self) -> bool {
        self.log_position != NULL_POSITION && compare_log(self, candidate) <= 0
    }

    /// The k-th highest log position among **active** members, where k is
    /// `quorum_threshold(members.len())`. `quorumPosition` (`:867`).
    ///
    /// `ranked` is the caller's scratch array; it must be
    /// `quorum_threshold(members.len())` long and is overwritten.
    ///
    /// # Panics
    ///
    /// If `ranked` is not the quorum threshold for `members`.
    pub fn quorum_position(
        members: &[Self],
        ranked: &mut [i64],
        now_ns: i64,
        timeout_ns: i64,
    ) -> i64 {
        assert_eq!(
            ranked.len(),
            quorum_threshold(members.len()),
            "ranked must be quorumThreshold long"
        );
        ranked.fill(0);
        for member in members {
            if member.is_active(now_ns, timeout_ns) {
                let mut new_position = member.log_position;
                for slot in ranked.iter_mut() {
                    if new_position > *slot {
                        std::mem::swap(slot, &mut new_position);
                    }
                }
            }
        }
        ranked[ranked.len() - 1]
    }
}

/// `(memberCount >> 1) + 1` (`:853`).
pub const fn quorum_threshold(member_count: usize) -> usize {
    (member_count >> 1) + 1
}

/// Has a quorum of members of `members` reached `position` in
/// `leadership_term_id`? `hasQuorumAtPosition` (`:907`).
pub fn has_quorum_at_position(
    members: &[ClusterMember],
    leadership_term_id: i64,
    position: i64,
    now_ns: i64,
    timeout_ns: i64,
) -> bool {
    let votes = members
        .iter()
        .filter(|member| {
            member.has_reached_position(leadership_term_id, position, now_ns, timeout_ns)
        })
        .count();
    votes >= quorum_threshold(members.len())
}

/// Is a quorum of members alive? `hasActiveQuorum` (`:900-918`).
pub fn has_active_quorum(members: &[ClusterMember], now_ns: i64, timeout_ns: i64) -> bool {
    let mut threshold = quorum_threshold(members.len()) as i32;
    for member in members {
        if member.is_active(now_ns, timeout_ns) {
            threshold -= 1;
            if threshold <= 0 {
                return true;
            }
        }
    }
    false
}

/// The 4-argument form (`:1173`): positive when the left log is more recent.
pub fn compare_log_values(
    lhs_term: i64,
    lhs_position: i64,
    rhs_term: i64,
    rhs_position: i64,
) -> i32 {
    match lhs_term.cmp(&rhs_term) {
        std::cmp::Ordering::Greater => 1,
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => match lhs_position.cmp(&rhs_position) {
            std::cmp::Ordering::Greater => 1,
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
        },
    }
}

/// The member form (`:1208`).
pub fn compare_log(lhs: &ClusterMember, rhs: &ClusterMember) -> i32 {
    compare_log_values(
        lhs.leadership_term_id,
        lhs.log_position,
        rhs.leadership_term_id,
        rhs.log_position,
    )
}

/// Is `candidate_term_id` the election's winner? `isQuorumLeader` (`:1015`).
///
/// A `false` vote anywhere ends it immediately: a quorum of `true` is not
/// enough if a single member said no at that term.
pub fn is_quorum_leader(members: &[ClusterMember], candidate_term_id: i64) -> bool {
    let mut votes = 0;
    for member in members {
        if candidate_term_id == member.candidate_term_id {
            match member.vote {
                Some(false) => return false,
                Some(true) => votes += 1,
                None => {}
            }
        }
    }
    votes >= quorum_threshold(members.len())
}

/// Could `candidate` win? `isQuorumCandidate` (`:1147`).
pub fn is_quorum_candidate(members: &[ClusterMember], candidate: &ClusterMember) -> bool {
    let possible = members
        .iter()
        .filter(|member| member.will_vote_for(candidate))
        .count();
    possible >= quorum_threshold(members.len())
}

/// Find a member by id (`:1225`).
/// `ClusterMember.encodeAsString(members)` (`ClusterMember.java:644-669`):
/// `id,endpoints` per member, joined by `|` — the string a
/// `ClusterMembersResponse` carries.
pub fn encode_as_string(members: &[ClusterMember]) -> String {
    let mut out = String::new();
    for (index, member) in members.iter().enumerate() {
        if index > 0 {
            out.push('|');
        }
        out.push_str(&member.id.to_string());
        out.push(',');
        out.push_str(&member.endpoints);
    }
    out
}

pub fn find_member(members: &[ClusterMember], member_id: i32) -> Option<&ClusterMember> {
    members.iter().find(|member| member.id == member_id)
}
