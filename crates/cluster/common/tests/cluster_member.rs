//! The member string and the quorum arithmetic, ported from
//! `ClusterMemberTest` (32 cases) plus a randomised check of `quorum_position`
//! against the obvious implementation.
//!
//! The randomised check is the one that matters: `quorumPosition` ranks by
//! insertion into a preallocated top-k list, which is easy to get subtly wrong
//! for repeated or interleaved positions, and the reference's own tests mostly
//! use one member.

use deepmsg_cluster_common::member::{
    self, ClusterMember, NULL_POSITION, compare_log, has_quorum_at_position, is_quorum_candidate,
    is_quorum_leader, quorum_threshold,
};

const MEMBER_6: &str = "0,ingress:1,consensus:2,log:3,catchup:4,archive:5";
const MEMBER_7: &str = "1,ingress:1,consensus:2,log:3,catchup:4,archive:5,resp:6";
const MEMBER_8: &str = "2,ingress:1,consensus:2,log:3,catchup:4,archive:5,resp:6,egress:7";

fn member(id: i32, position: i64, last_append_ns: i64) -> ClusterMember {
    let mut m =
        ClusterMember::parse(&format!("{id},i{id}:1,c{id}:2,l{id}:3,u{id}:4,a{id}:5")).unwrap();
    m.log_position = position;
    m.time_of_last_append_position_ns = last_append_ns;
    m
}

#[test]
fn an_empty_or_absent_string_is_an_empty_list() {
    assert!(ClusterMember::parse_list("").unwrap().is_empty());
}

#[test]
fn six_attributes_parse_and_the_optional_ones_are_none() {
    let m = ClusterMember::parse(MEMBER_6).unwrap();
    assert_eq!(m.id, 0);
    assert_eq!(
        m.endpoints,
        "ingress:1,consensus:2,log:3,catchup:4,archive:5"
    );
    assert_eq!(m.archive_response_endpoint, None);
    assert_eq!(m.egress_response_endpoint, None);
    assert_eq!(m.log_position, NULL_POSITION);
}

#[test]
fn seven_and_eight_attributes_are_kept() {
    let seven = ClusterMember::parse(MEMBER_7).unwrap();
    assert_eq!(seven.archive_response_endpoint.as_deref(), Some("resp:6"));
    assert_eq!(seven.egress_response_endpoint, None);

    let eight = ClusterMember::parse(MEMBER_8).unwrap();
    assert_eq!(eight.archive_response_endpoint.as_deref(), Some("resp:6"));
    assert_eq!(eight.egress_response_endpoint.as_deref(), Some("egress:7"));
}

#[test]
fn five_and_nine_attributes_are_refused() {
    for bad in ["0,a:1,b:2,c:3,d:4", "0,a:1,b:2,c:3,d:4,e:5,f:6,g:7,h:8"] {
        assert!(
            ClusterMember::parse(bad).is_err(),
            "{bad} should be refused"
        );
    }
}

#[test]
fn a_non_integer_id_is_refused() {
    let bad = "x,a:1,b:2,c:3,d:4,e:5";
    assert!(matches!(
        ClusterMember::parse(bad),
        Err(member::MemberError::Id { .. })
    ));
}

#[test]
fn members_are_separated_by_a_pipe() {
    let members = ClusterMember::parse_list(&format!("{MEMBER_6}|{MEMBER_7}")).unwrap();
    assert_eq!(members.len(), 2);
    assert_eq!(members[0].id, 0);
    assert_eq!(members[1].id, 1);
}

#[test]
fn the_quorum_threshold_is_half_plus_one() {
    assert_eq!(quorum_threshold(1), 1);
    assert_eq!(quorum_threshold(2), 2);
    assert_eq!(quorum_threshold(3), 2);
    assert_eq!(quorum_threshold(4), 3);
    assert_eq!(quorum_threshold(5), 3);
    assert_eq!(quorum_threshold(7), 4);
}

#[test]
fn a_member_is_active_until_its_timeout_expires() {
    let m = member(0, 10, 1_000);
    assert!(m.is_active(1_500, 1_000));
    assert!(!m.is_active(2_000, 1_000));
    assert!(!m.is_active(2_001, 1_000));
}

#[test]
fn the_quorum_position_is_the_threshold_highest_active_position() {
    let members = vec![
        member(0, 10, 1_000),
        member(1, 20, 1_000),
        member(2, 30, 1_000),
    ];
    let mut ranked = [0_i64; 2];
    assert_eq!(
        ClusterMember::quorum_position(&members, &mut ranked, 1_000, 1_000),
        20
    );
}

#[test]
fn an_inactive_member_does_not_contribute_a_position() {
    let members = vec![member(0, 10, 1_000), member(1, 20, 0), member(2, 30, 0)];
    let mut ranked = [0_i64; 2];
    // Only member 0 is active, so the second-highest slot stays 0.
    assert_eq!(
        ClusterMember::quorum_position(&members, &mut ranked, 5_000, 1_000),
        0
    );
}

#[test]
fn the_quorum_position_agrees_with_sorting() {
    // Deterministic pseudo-random positions, including repeats and equal
    // timestamps, which is where an insertion ranking is easiest to get wrong.
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    let mut next = |bound: i64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state % bound as u64) as i64
    };
    for member_count in 1..=7_usize {
        for _ in 0..50 {
            let members: Vec<_> = (0..member_count)
                .map(|i| member(i as i32, next(50), 1_000))
                .collect();
            let mut ranked = vec![0_i64; quorum_threshold(member_count)];
            let got = ClusterMember::quorum_position(&members, &mut ranked, 1_000, 1_000);

            let mut naive: Vec<i64> = members.iter().map(|m| m.log_position).collect();
            naive.sort_unstable_by(|a, b| b.cmp(a));
            assert_eq!(got, naive[quorum_threshold(member_count) - 1]);
        }
    }
}

#[test]
fn has_quorum_at_position_counts_term_and_position_and_liveness() {
    let mut members = vec![
        member(0, 10, 1_000),
        member(1, 10, 1_000),
        member(2, 0, 1_000),
    ];
    for m in &mut members {
        m.leadership_term_id = 4;
    }
    assert!(has_quorum_at_position(&members, 4, 10, 1_000, 1_000));
    assert!(!has_quorum_at_position(&members, 4, 11, 1_000, 1_000));
    assert!(!has_quorum_at_position(&members, 5, 10, 1_000, 1_000));
    assert!(!has_quorum_at_position(&members, 4, 10, 5_000, 1_000));
}

#[test]
fn compare_log_prefers_the_higher_term_then_the_higher_position() {
    assert_eq!(member::compare_log_values(1, 100, 2, 0), -1);
    assert_eq!(member::compare_log_values(2, 0, 1, 100), 1);
    assert_eq!(member::compare_log_values(1, 100, 1, 200), -1);
    assert_eq!(member::compare_log_values(1, 200, 1, 100), 1);
    assert_eq!(member::compare_log_values(1, 100, 1, 100), 0);

    let mut a = member(0, 10, 0);
    let mut b = member(1, 20, 0);
    a.leadership_term_id = 3;
    b.leadership_term_id = 3;
    assert_eq!(compare_log(&a, &b), -1);
}

#[test]
fn a_member_will_vote_for_a_log_at_least_as_recent() {
    let mut voter = member(0, 10, 0);
    voter.leadership_term_id = 3;
    let mut fresh = member(1, 20, 0);
    fresh.leadership_term_id = 3;

    assert!(voter.will_vote_for(&fresh));
    assert!(!fresh.will_vote_for(&voter));

    // A member that has never reported a position votes for nobody.
    let mut silent = member(2, 0, 0);
    silent.log_position = NULL_POSITION;
    assert!(!silent.will_vote_for(&fresh));
}

#[test]
fn a_leader_needs_a_quorum_of_yes_and_no_no() {
    let mut members = vec![member(0, 0, 0), member(1, 0, 0), member(2, 0, 0)];
    for m in &mut members {
        m.candidate_term_id = 5;
        m.vote = Some(true);
    }
    assert!(is_quorum_leader(&members, 5));

    members[2].vote = Some(false);
    assert!(!is_quorum_leader(&members, 5), "one no is enough to lose");

    // An abstention is not a yes, but it is not a no either: with two yeses
    // and one abstention the quorum of two still holds.
    members[2].vote = None;
    assert!(is_quorum_leader(&members, 5));
    members[1].vote = None;
    assert!(!is_quorum_leader(&members, 5), "one yes is not a quorum");
    assert!(
        !is_quorum_leader(&members, 6),
        "the wrong term wins nothing"
    );
}

#[test]
fn a_candidate_needs_a_quorum_of_willing_voters() {
    let candidate = member(0, 10, 0);
    let willing = vec![member(0, 5, 0), member(1, 10, 0), member(2, 0, 0)];
    assert!(is_quorum_candidate(&willing, &candidate));

    let unwilling = vec![member(0, 50, 0), member(1, 60, 0), member(2, 0, 0)];
    assert!(!is_quorum_candidate(&unwilling, &candidate));
}

#[test]
fn find_member_answers_by_id() {
    let members = ClusterMember::parse_list(&format!("{MEMBER_6}|{MEMBER_7}")).unwrap();
    assert_eq!(member::find_member(&members, 1).map(|m| m.id), Some(1));
    assert!(member::find_member(&members, 9).is_none());
}

#[test]
fn a_member_reset_forgets_the_term_but_not_its_endpoints() {
    let mut member = member(0, 42, 7);
    member.vote = Some(true);
    member.candidate_term_id = 5;
    member.leadership_term_id = 3;
    member.is_ballot_sent = true;
    member.is_leader = true;

    member.reset();

    assert_eq!(Some(false), Some(member.is_ballot_sent));
    assert!(!member.is_leader);
    assert_eq!(None, member.vote);
    assert_eq!(-1, member.candidate_term_id);
    assert_eq!(-1, member.leadership_term_id);
    assert_eq!(-1, member.log_position);
    assert_eq!("i0:1", member.ingress_endpoint);
}

#[test]
fn a_candidate_votes_for_itself_and_clears_everyone_else() {
    let mut members = vec![member(0, 0, 0), member(1, 0, 0), member(2, 0, 0)];
    members[1].vote = Some(true);
    members[2].vote = Some(false);

    ClusterMember::become_candidate(&mut members, 7, 0);

    assert_eq!(Some(true), members[0].vote);
    assert_eq!(7, members[0].candidate_term_id);
    assert!(members[0].is_ballot_sent);
    for other in &members[1..] {
        assert_eq!(None, other.vote);
        assert_eq!(-1, other.candidate_term_id);
        assert!(!other.is_ballot_sent);
    }
}

#[test]
fn a_unanimous_leader_needs_everyone_but_the_closed_leader_to_say_yes() {
    let mut members = vec![member(0, 0, 0), member(1, 0, 0), member(2, 0, 0)];
    ClusterMember::become_candidate(&mut members, 3, 0);
    members[1].vote = Some(true);
    members[1].candidate_term_id = 3;

    // Member 2 has not voted yet.
    assert!(!ClusterMember::is_unanimous_leader(&members, 3, -1));

    members[2].vote = Some(true);
    members[2].candidate_term_id = 3;
    assert!(ClusterMember::is_unanimous_leader(&members, 3, -1));

    // A gracefully closed leader is not asked.
    members[2].vote = None;
    assert!(ClusterMember::is_unanimous_leader(&members, 3, 2));
}

#[test]
fn a_unanimous_candidate_needs_everyone_but_the_closed_leader_to_be_willing() {
    let members = vec![member(0, 10, 0), member(1, 10, 0), member(2, 20, 0)];
    let candidate = &members[0];

    // Member 2 is ahead of the candidate and would not vote for it.
    assert!(!ClusterMember::is_unanimous_candidate(
        &members, candidate, -1
    ));

    // Unless it is the gracefully closed leader, which is not asked.
    assert!(ClusterMember::is_unanimous_candidate(
        &members, candidate, 2
    ));
}
