//! Log replication, from the agent's side (`RecordingReplication.java`).
//!
//! A follower whose log is older than the leader's term cannot be caught up by
//! a replay: its own recording does not reach into the leader's term at all.
//! The gap is filled by an archive-to-archive replication — the leader's
//! archive copies the log recording into the follower's archive, which extends
//! the follower's own recording
//! (`ConsensusModuleAgent.newLogReplication`, `:2951-2986`).
//!
//! # Why the work is split in two
//!
//! The election holds a [`RecordingReplication`](crate::handles::RecordingReplication)
//! — four shared atomics — and polls it on the turn it runs in. The session
//! itself (the archive client's replication, and the recording signals that
//! say how far it has got) belongs to the agent, because polling it needs the
//! client and the archive mutably, which the election's turn cannot borrow.
//! Plan Q1: same duty-cycle thread, once per turn, so the cost is the atomics
//! and not a lock.

use crate::handles::RecordingReplication;

/// The agent's half of one log replication.
pub struct AgentLogReplication {
    /// The handle the election polls, shared with it.
    handle: RecordingReplication,
    /// The **source** recording — the leader's — being copied in.
    src_recording_id: i64,
}

impl AgentLogReplication {
    /// Bind a started replication to the handle the election was given.
    pub const fn new(handle: RecordingReplication, src_recording_id: i64) -> Self {
        Self {
            handle,
            src_recording_id,
        }
    }

    /// The handle, for the election.
    pub fn handle(&self) -> RecordingReplication {
        self.handle.clone()
    }

    /// The replication's id — the correlation id the archive answers and sends
    /// signals under.
    pub const fn replication_id(&self) -> i64 {
        self.handle.correlation_id()
    }

    /// The source recording being replicated.
    pub const fn src_recording_id(&self) -> i64 {
        self.src_recording_id
    }

    /// Whether the replication has finished: the archive sent both
    /// `REPLICATE_END` and the destination recording's `STOP`
    /// (`RecordingReplication.hasReplicationEnded`/`hasStopped`).
    pub fn is_complete(&self) -> bool {
        self.handle.has_replication_ended() && self.handle.has_stopped()
    }
}
