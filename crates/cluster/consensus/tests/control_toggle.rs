//! `ClusterControl`, `NodeControl` and `ClusterTermination`: the operator
//! side of P3-4e.
//!
//! Counterparts: `ClusterControl.java:43-227`, `NodeControl.java:40-176`,
//! `ClusterTermination.java:22-85`. The agent's reading half
//! (`checkClusterControlToggle`, `checkNodeControlToggle`, the termination
//! paths) is driven in `tests/integration/cluster_control.rs`.

use deepmsg_cluster::ModuleState;
use deepmsg_cluster::agent::publisher::ConsensusPublisher;
use deepmsg_cluster::control::cluster_control::ToggleState as ClusterToggle;
use deepmsg_cluster::control::node_control::ToggleState as NodeToggle;
use deepmsg_cluster::control::termination::ClusterTermination;
use deepmsg_cluster_common::member::ClusterMember;
use deepmsg_cnc::counters::CountersReader;
use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};

/// A writable region for a `CountersReader`, as the CnC file's two regions
/// are. The alignment is what `AtomicBuffer` requires, not the size.
#[repr(align(64))]
struct Region<const N: usize>([u8; N]);

impl<const N: usize> Region<N> {
    fn new() -> Self {
        Self([0; N])
    }

    fn buffer_mut(&mut self) -> AtomicBuffer<'_, ReadWrite> {
        AtomicBuffer::from_slice_mut(&mut self.0).expect("an aligned region")
    }
}

/// One counter's slot; `CountersReader` only needs the value region to read and
/// compare-and-set (the metadata is for `get`/`find`).
fn counters() -> (Region<1024>, Region<256>) {
    (Region::new(), Region::new())
}

fn control_codes() -> Vec<(ClusterToggle, i32)> {
    vec![
        (ClusterToggle::Inactive, 0),
        (ClusterToggle::Neutral, 1),
        (ClusterToggle::Suspend, 2),
        (ClusterToggle::Resume, 3),
        (ClusterToggle::Snapshot, 4),
        (ClusterToggle::Shutdown, 5),
        (ClusterToggle::Abort, 6),
        (ClusterToggle::StandbySnapshot, 7),
    ]
}

#[test]
fn the_toggle_codes_are_the_ones_the_reference_counter_carries() {
    for (state, code) in control_codes() {
        assert_eq!(code, state.code());
        assert_eq!(Ok(state), ClusterToggle::from_code(i64::from(code)));
    }

    for (state, code) in [
        (NodeToggle::Inactive, 0),
        (NodeToggle::Neutral, 1),
        (NodeToggle::ReplicateStandbySnapshot, 2),
        (NodeToggle::ValidateRecordingLog, 3),
    ] {
        assert_eq!(code, state.code());
        assert_eq!(Ok(state), NodeToggle::from_code(i64::from(code)));
    }
}

#[test]
fn an_invalid_toggle_value_is_refused_by_name() {
    for value in [-1_i64, 8, 99] {
        let error = ClusterToggle::from_code(value).expect_err("outside the table");
        assert_eq!(
            format!("cluster: invalid toggle value: {value}"),
            error.to_string()
        );
    }

    for value in [-1_i64, 4] {
        let error = NodeToggle::from_code(value).expect_err("outside the table");
        assert_eq!(
            format!("cluster: invalid toggle value: {value}"),
            error.to_string()
        );
    }
}

#[test]
fn a_cluster_switch_is_a_compare_and_set_out_of_neutral() {
    let (mut metadata, mut values) = counters();
    let counters = CountersReader::new(metadata.buffer_mut(), values.buffer_mut());

    counters.set_value(0, 1).expect("a neutral counter");
    assert!(ClusterToggle::Suspend.toggle(&counters, 0));
    assert_eq!(Some(2), counters.value(0));

    // The module has not reset the counter yet: a second operator loses.
    assert!(!ClusterToggle::Snapshot.toggle(&counters, 0));
    assert_eq!(Some(2), counters.value(0));
}

#[test]
fn resume_is_the_one_switch_that_moves_a_suspended_counter() {
    let (mut metadata, mut values) = counters();
    let counters = CountersReader::new(metadata.buffer_mut(), values.buffer_mut());

    counters.set_value(0, 2).expect("a suspended counter");
    assert!(ClusterToggle::Resume.toggle(&counters, 0));
    assert_eq!(Some(3), counters.value(0));

    // From NEUTRAL it also succeeds: the reference only adds the SUSPEND case
    // to the ordinary compare-and-set, it does not remove NEUTRAL
    // (`ClusterControl.java:118-126`).
    counters.set_value(0, 1).expect("a neutral counter");
    assert!(ClusterToggle::Resume.toggle(&counters, 0));
    assert_eq!(Some(3), counters.value(0));

    // Nothing else leaves SUSPEND.
    counters.set_value(0, 2).expect("a suspended counter");
    assert!(!ClusterToggle::Abort.toggle(&counters, 0));
    assert_eq!(Some(2), counters.value(0));
}

#[test]
fn a_node_switch_only_leaves_neutral() {
    let (mut metadata, mut values) = counters();
    let counters = CountersReader::new(metadata.buffer_mut(), values.buffer_mut());

    counters.set_value(0, 1).expect("a neutral counter");
    assert!(NodeToggle::ReplicateStandbySnapshot.toggle(&counters, 0));
    assert_eq!(Some(2), counters.value(0));

    // There is no SUSPEND/RESUME exception on this toggle.
    counters.set_value(0, 1).expect("a neutral counter");
    assert!(NodeToggle::ValidateRecordingLog.toggle(&counters, 0));
    assert_eq!(Some(3), counters.value(0));
    assert!(!NodeToggle::ReplicateStandbySnapshot.toggle(&counters, 0));
    assert_eq!(Some(3), counters.value(0));
}

#[test]
fn a_closed_counter_slot_is_refused() {
    let (mut metadata, mut values) = counters();
    let counters = CountersReader::new(metadata.buffer_mut(), values.buffer_mut());

    // Counter 8 is outside a 256-byte value region (two slots).
    assert!(!ClusterToggle::Suspend.toggle(&counters, 8));
    assert_eq!(None, counters.value(8));
}

fn members() -> Vec<ClusterMember> {
    vec![
        ClusterMember::parse("0,localhost:1,localhost:2,localhost:3,localhost:4,localhost:5")
            .expect("member 0"),
        ClusterMember::parse("1,localhost:1,localhost:2,localhost:3,localhost:4,localhost:5")
            .expect("member 1"),
        ClusterMember::parse("2,localhost:1,localhost:2,localhost:3,localhost:4,localhost:5")
            .expect("member 2"),
    ]
}

#[test]
fn a_terminating_leader_never_finishes_while_its_services_are_awaited() {
    let mut members = members();
    for member in &mut members {
        member.has_terminated = true;
    }

    let mut termination = ClusterTermination::new(i64::MAX);
    // Every follower answered and the deadline has not come — services still
    // win (`ClusterTermination.java:39-42`).
    assert!(!termination.can_terminate(&members, i64::MAX - 1));

    termination.on_services_terminated();
    assert!(termination.can_terminate(&members, i64::MAX - 1));
}

#[test]
fn a_terminating_leader_waits_for_every_follower_and_then_the_deadline() {
    let mut members = members();
    ClusterMember::set_is_leader(&mut members, 0);

    let mut termination = ClusterTermination::new(1_000);
    termination.on_services_terminated();

    // Two followers, neither acknowledged.
    assert!(!termination.can_terminate(&members, 999));

    // One of them answers; the leader does not count itself.
    members[1].has_terminated = true;
    assert!(!termination.can_terminate(&members, 999));

    members[2].has_terminated = true;
    assert!(termination.can_terminate(&members, 999));

    // The deadline ends the wait for a follower that never answers.
    members[2].has_terminated = false;
    assert!(termination.can_terminate(&members, 1_000));
}

#[test]
fn a_new_termination_round_clears_the_previous_answers() {
    let mut members = members();
    ClusterMember::set_is_leader(&mut members, 0);
    for member in &mut members {
        member.has_terminated = true;
    }

    // A publisher with no per-member publications answers `false` for every
    // send, which is the reference's null-publication answer
    // (`ConsensusPublisher.java:387-390`); the callback is what records it.
    let mut publisher = ConsensusPublisher::new();
    let mut failed = Vec::new();
    let mut termination = ClusterTermination::new(i64::MAX);
    termination.termination_position(
        &mut members,
        0,
        &mut publisher,
        &mut |member_id| failed.push(member_id),
        -1,
        42,
    );

    // Every flag is cleared first (`ClusterTermination.java:73`), and only the
    // other members are sent to.
    assert!(members.iter().all(|member| !member.has_terminated));
    assert_eq!(vec![1, 2], failed);
}

#[test]
fn module_state_codes_round_trip() {
    for code in 0..=6 {
        let state = ModuleState::from_code(code).expect("a state code");
        assert_eq!(code, state.code());
    }
    assert!(ModuleState::from_code(7).is_none());
}
