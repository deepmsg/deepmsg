//! `ClusterTermination` (`ClusterTermination.java:22-85`): the leader's wait
//! before it closes.
//!
//! When the leader is told to `SHUTDOWN` or `ABORT` it sends every other member
//! a `TerminationPosition` and then waits for two things: its own services to
//! acknowledge their termination, and every follower to answer
//! `TerminationAck`. The wait ends early at `terminationTimeout`, which is why
//! a follower that has died cannot hold the whole cluster open.
//!
//! Blocking order is the point of the class: `canTerminate` says **no** while
//! services are still being awaited, no matter how much time has passed
//! (`:39-42`). A snapshot or termination ack that arrives after the deadline
//! therefore cannot cut a service's state machine short.

use deepmsg_cluster_common::member::ClusterMember;

use crate::agent::publisher::ConsensusPublisher;

/// `ClusterTermination` (`:22-85`).
#[derive(Clone, Copy, Debug)]
pub struct ClusterTermination {
    deadline_ns: i64,
    is_awaiting_services: bool,
}

impl ClusterTermination {
    /// `new ClusterTermination(deadlineNs)` (`:27-30`).
    pub const fn new(deadline_ns: i64) -> Self {
        Self {
            deadline_ns,
            is_awaiting_services: true,
        }
    }

    /// The deadline the leader set (`deadlineNs`).
    pub const fn deadline_ns(&self) -> i64 {
        self.deadline_ns
    }

    /// `deadlineNs(deadlineNs)` (`:32-35`): the snapshot half of a `SHUTDOWN`
    /// restarts the clock once its services have answered.
    pub fn set_deadline_ns(&mut self, deadline_ns: i64) {
        self.deadline_ns = deadline_ns;
    }

    /// `canTerminate(members, nowNs)` (`:37-56`).
    ///
    /// A leader itself is never counted — it is the caller — so a one-member
    /// cluster is ready as soon as its services are.
    pub fn can_terminate(&self, members: &[ClusterMember], now_ns: i64) -> bool {
        if self.is_awaiting_services {
            return false;
        }

        let all_followers_terminated = members
            .iter()
            .all(|member| member.is_leader || member.has_terminated);

        all_followers_terminated || now_ns >= self.deadline_ns
    }

    /// `onServicesTerminated()` (`:58-61`).
    pub fn on_services_terminated(&mut self) {
        self.is_awaiting_services = false;
    }

    /// `terminationPosition(errorHandler, publisher, members, thisMember,
    /// leadershipTermId, position)` (`:63-84`).
    ///
    /// Every member's termination flag is cleared first — a `TerminationAck`
    /// from an earlier round must not count for this one. Sending to a member
    /// that refuses is reported through `on_send_failure`, the reference's
    /// counted `WARN` `ClusterEvent`, and does not stop the loop: one deaf
    /// follower cannot keep the rest from hearing the position.
    pub fn termination_position(
        &mut self,
        members: &mut [ClusterMember],
        this_member_id: i32,
        publisher: &mut ConsensusPublisher,
        on_send_failure: &mut dyn FnMut(i32),
        leadership_term_id: i64,
        position: i64,
    ) {
        for member in members {
            member.has_terminated = false;

            if member.id != this_member_id
                && !publisher.termination_position(member.id, leadership_term_id, position)
            {
                on_send_failure(member.id);
            }
        }
    }
}
