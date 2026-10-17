//! Opaque handles the election stores but never inspects.
//!
//! `LogReplay` is a real type from P3-4d: the election holds it across duty
//! cycles and asks whether it is done, while the **agent** does the polling
//! (its `do_work` owns the client and the archive). The two share the flag and
//! the position through atomics — the reference has one object doing both, and
//! Rust cannot hand the election a borrow of the agent.
//!
//! `RecordingReplication` is still a placeholder: it is P3-6b.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

/// A log replay in progress (reference `LogReplay.java`).
#[derive(Clone, Debug, Default)]
pub struct LogReplay {
    done: Arc<AtomicBool>,
    position: Arc<AtomicI64>,
}

impl LogReplay {
    /// A fresh handle pair: the agent keeps one clone, the election the other.
    pub fn new() -> Self {
        Self::default()
    }

    /// The handle the election gets from a replay the agent has started.
    pub fn shared(done: Arc<AtomicBool>, position: Arc<AtomicI64>) -> Self {
        Self { done, position }
    }

    /// A handle that is already finished — what a mock host answers so an
    /// election walk does not wait for an agent that does not exist.
    pub fn completed() -> Self {
        let replay = Self::default();
        replay
            .done
            .store(true, std::sync::atomic::Ordering::Release);
        replay
    }

    /// `isDone()` (`LogReplay.java:114-118`).
    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    /// `position()` (`:120-123`).
    pub fn position(&self) -> i64 {
        self.position.load(Ordering::Acquire)
    }

    /// The two atomics, for the agent's own copy.
    pub fn parts(&self) -> (Arc<AtomicBool>, Arc<AtomicI64>) {
        (Arc::clone(&self.done), Arc::clone(&self.position))
    }
}

/// An archive-to-archive replication of a log (reference
/// `RecordingReplication.java`). Created by `ElectionHost::new_log_replication`
/// (`ConsensusModuleAgent.java:2951`), advanced by `Election::on_recording_signal`
/// (`Election.java:269-282`). P3-6b.
#[derive(Clone, Copy, Debug, Default)]
pub struct RecordingReplication;
