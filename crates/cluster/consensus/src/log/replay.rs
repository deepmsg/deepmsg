//! The agent's half of a log replay (`LogReplay.java`).
//!
//! The reference's `LogReplay` owns the archive call, the subscription and the
//! adapter, and the election calls its `doWork`. Here the election holds only an
//! atomics-backed handle ([`crate::handles::LogReplay`]) and the **agent** polls
//! this state from its own duty cycle, because the client and the archive live
//! there.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64};

use crate::log::adapter::LogAdapter;

/// The agent's half of a log replay: the subscription the archive's replay is
/// read from, and the adapter that applies its records. The election holds only
/// a [`LogReplay`] handle over the same atomics.
pub struct AgentLogReplay {
    /// The subscription the replay is read from.
    pub subscription_id: i64,
    /// The channel that subscription named — what a `JoinLog` to the services
    /// hands them, so they read the same replay
    /// (`LogReplay.doWork`'s `logSubscription.channel()`, `LogReplay.java:96`).
    pub channel: String,
    /// The archive's replay session id, for `stop_replay`.
    pub replay_session_id: i64,
    /// The replay publication's session id (low 32 bits of the above).
    pub log_session_id: i32,
    /// Where the replay starts, which the image's join position must equal.
    pub start_position: i64,
    /// Where it stops.
    pub stop_position: i64,
    /// The reader.
    pub adapter: LogAdapter,
    /// Shared with the election's handle.
    pub done: Arc<AtomicBool>,
    /// Shared with the election's handle.
    pub position: Arc<AtomicI64>,
}
