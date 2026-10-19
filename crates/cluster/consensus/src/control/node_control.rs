//! `NodeControl` (`NodeControl.java:28-222`): the per-node operator toggle.
//!
//! Unlike [`super::cluster_control`], this toggle never leaves the node that
//! owns the counter and has no `SUSPEND -> RESUME` exception: every switch is a
//! compare-and-set out of `NEUTRAL` (`:92-95`).
//!
//! Both OSS switch values name features this build does not have — the standby
//! snapshot replicator and the recording-log validator — so the module refuses
//! them by name and resets the toggle rather than leaving it armed (see
//! [`crate::agent`]).

use deepmsg_client::client::Client;
use deepmsg_client::counter::Counter;
use deepmsg_cluster_common::counters::{NODE_CONTROL_TOGGLE_TYPE_ID, find_cluster_counter};
use deepmsg_cnc::counters::CountersReader;
use deepmsg_core::buffer::ReadWrite;

use crate::error::ClusterError;

/// `NodeControl.CONTROL_TOGGLE_TYPE_ID` (`:154`).
pub const CONTROL_TOGGLE_TYPE_ID: i32 = NODE_CONTROL_TOGGLE_TYPE_ID;

/// `NodeControl.ToggleState` (`:40-149`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum ToggleState {
    /// `INACTIVE(0)`: not accepting new actions.
    Inactive = 0,
    /// `NEUTRAL(1)`: ready to accept a new action.
    Neutral = 1,
    /// `REPLICATE_STANDBY_SNAPSHOT(2)`.
    ReplicateStandbySnapshot = 2,
    /// `VALIDATE_RECORDING_LOG(3)`.
    ValidateRecordingLog = 3,
}

impl ToggleState {
    /// The value the counter carries (`:81-84`).
    pub const fn code(self) -> i32 {
        self as i32
    }

    /// The name the reference's `ToggleState.toString()` prints.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Inactive => "INACTIVE",
            Self::Neutral => "NEUTRAL",
            Self::ReplicateStandbySnapshot => "REPLICATE_STANDBY_SNAPSHOT",
            Self::ValidateRecordingLog => "VALIDATE_RECORDING_LOG",
        }
    }

    /// `ToggleState.get` (`:134-148`) without the counter.
    ///
    /// # Errors
    ///
    /// [`ClusterError::Cluster`] for a value outside the table.
    pub fn from_code(value: i64) -> Result<Self, ClusterError> {
        match value {
            0 => Ok(Self::Inactive),
            1 => Ok(Self::Neutral),
            2 => Ok(Self::ReplicateStandbySnapshot),
            3 => Ok(Self::ValidateRecordingLog),
            _ => Err(ClusterError::Cluster(format!(
                "invalid toggle value: {value}"
            ))),
        }
    }

    /// `ToggleState.get(nodeControlToggle)` (`:134-148`) through this agent's
    /// client.
    ///
    /// # Errors
    ///
    /// `counter is closed` or `invalid toggle value`, as the reference's
    /// `ClusterException`.
    pub fn get(client: &Client, counter: Counter) -> Result<Self, ClusterError> {
        let value = client
            .counters_reader()
            .and_then(|counters| counters.value(counter.counter_id()))
            .ok_or_else(|| ClusterError::Cluster("counter is closed".to_owned()))?;
        Self::from_code(value)
    }

    /// `ToggleState.toggle(controlToggle)` (`:92-95`): compare-and-set out of
    /// `NEUTRAL`, with no exception.
    pub fn toggle(self, counters: &CountersReader<'_, ReadWrite>, counter_id: i32) -> bool {
        counters
            .compare_and_set_value(
                counter_id,
                i64::from(Self::Neutral.code()),
                i64::from(self.code()),
            )
            .unwrap_or(false)
    }

    /// `ToggleState.reset(controlToggle)` (`:102-105`).
    pub fn reset(client: &Client, counter: Counter) {
        write(client, counter, Self::Neutral);
    }

    /// `ToggleState.activate(controlToggle)` (`:112-115`).
    pub fn activate(client: &Client, counter: Counter) {
        write(client, counter, Self::Neutral);
    }

    /// `ToggleState.deactivate(controlToggle)` (`:122-125`).
    pub fn deactivate(client: &Client, counter: Counter) {
        write(client, counter, Self::Inactive);
    }
}

fn write(client: &Client, counter: Counter, state: ToggleState) {
    if let Some(counters) = client.counters_writable() {
        let _ = counter.set_value(&counters, i64::from(state.code()));
    }
}

/// `NodeControl.findControlToggle` (`:167-176`).
pub fn find_control_toggle(counters: &CountersReader<'_>, cluster_id: i32) -> Option<i32> {
    find_cluster_counter(counters, CONTROL_TOGGLE_TYPE_ID, cluster_id)
}
