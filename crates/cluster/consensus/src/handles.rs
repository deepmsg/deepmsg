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

/// `RecordingSignal` (`aeron-archive/.../RecordingSignal.java`): the signals a
/// replication consumes. Only the four `RecordingReplication.onSignal` reads
/// are named here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum RecordingSignal {
    /// `START(0)`.
    Start = 0,
    /// `STOP(1)`.
    Stop = 1,
    /// `EXTEND(2)`: the recording is being extended; its position counter is
    /// looked up again.
    Extend = 2,
    /// `SYNC(5)`.
    Sync = 5,
    /// `REPLICATE_END(7)`.
    ReplicateEnd = 7,
    /// `NULL_VAL`.
    NullVal = -2_147_483_648,
}

/// An archive-to-archive replication of a log (reference
/// `RecordingReplication.java`). Created by `ElectionHost::new_log_replication`
/// (`ConsensusModuleAgent.java:2951`), advanced by
/// [`Election::on_recording_signal`](crate::election::Election::on_recording_signal).
///
/// **The state machine is here; the work is not.** The reference's `poll`
/// drives an `AeronArchive` session, and that belongs to `P3-6b`; the handle a
/// follower election holds is the same four facts either way — the recording
/// id, its position, and the two flags the state machine waits on.
#[derive(Clone, Debug)]
pub struct RecordingReplication {
    correlation_id: i64,
    recording_id: Arc<AtomicI64>,
    position: Arc<AtomicI64>,
    replication_ended: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    synced: Arc<AtomicBool>,
}

impl Default for RecordingReplication {
    fn default() -> Self {
        Self::new(-1, -1)
    }
}

impl RecordingReplication {
    /// A replication of `recording_id` started under `correlation_id`, not yet
    /// ended or stopped.
    pub fn new(recording_id: i64, correlation_id: i64) -> Self {
        Self {
            correlation_id,
            recording_id: Arc::new(AtomicI64::new(recording_id)),
            position: Arc::new(AtomicI64::new(0)),
            replication_ended: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(AtomicBool::new(false)),
            synced: Arc::new(AtomicBool::new(false)),
        }
    }

    /// `recordingId()` (`:136-139`).
    pub fn recording_id(&self) -> i64 {
        self.recording_id.load(Ordering::Acquire)
    }

    /// `position()` (`:131-134`).
    pub fn position(&self) -> i64 {
        self.position.load(Ordering::Acquire)
    }

    /// `hasReplicationEnded()` (`:141-144`).
    pub fn has_replication_ended(&self) -> bool {
        self.replication_ended.load(Ordering::Acquire)
    }

    /// `hasStopped()` (`:151-154`).
    pub fn has_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// `hasSynced()` (`:146-149`).
    pub fn has_synced(&self) -> bool {
        self.synced.load(Ordering::Acquire)
    }

    /// `poll(nowNs)` (`:176`): the reference advances the archive session here.
    /// The session is `P3-6b`'s; until then this is where it will be called
    /// from, and a test moves the handle through [`Self::on_signal`].
    pub fn poll(&self, _now_ns: i64) {}

    /// `onSignal(correlationId, recordingId, position, signal)` (`:176-203`).
    ///
    /// `replication_id` is the correlation id the replication was started
    /// under; the reference matches on it and ignores everyone else's signals.
    pub fn on_signal(&self, correlation_id: i64, position: i64, signal: RecordingSignal) {
        if correlation_id != self.correlation_id {
            return;
        }

        match signal {
            // `EXTEND` re-reads the recording position counter; that counter
            // is the archive client's, so it is `P3-6b`'s half.
            RecordingSignal::Extend => {}
            RecordingSignal::Sync => self.synced.store(true, Ordering::Release),
            RecordingSignal::ReplicateEnd => {
                self.replication_ended.store(true, Ordering::Release);
            }
            RecordingSignal::Stop => {
                if position != -1 {
                    self.position.store(position, Ordering::Release);
                }
                self.stopped.store(true, Ordering::Release);
            }
            _ => {}
        }
    }
}
