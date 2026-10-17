//! `ClusterControl` (`ClusterControl.java:36-227`): the cluster-wide operator
//! toggle.
//!
//! An operator process maps the driver's CnC file, finds the counter whose
//! type is [`CONTROL_TOGGLE_TYPE_ID`] and whose key names the cluster, and
//! switches it out of `NEUTRAL`. The leader reads it in `slowTickWork`
//! (`ConsensusModuleAgent.java:2411`) and resets it when the action it names is
//! complete (`:2580`, `:2654`, `:3143`).
//!
//! Two rules are the whole of the protocol:
//!
//! * a switch is a **compare-and-set out of `NEUTRAL`** (`:118-126`) — the only
//!   exception is `SUSPEND -> RESUME`, which the module cannot perform for the
//!   operator because it is the module that is suspended;
//! * `activate` and `deactivate` (`:143-156`) are unconditional writes, made
//!   when this node becomes the leader (`ConsensusModuleAgent.java:1913`) and
//!   when it stops being one (`:1382`).

use deepmsg_client::client::Client;
use deepmsg_client::counter::Counter;
use deepmsg_cluster_common::counters::{CLUSTER_CONTROL_TOGGLE_TYPE_ID, find_cluster_counter};
use deepmsg_cnc::counters::CountersReader;
use deepmsg_core::buffer::ReadWrite;

use crate::error::ClusterError;

/// `ClusterControl.CONTROL_TOGGLE_TYPE_ID` (`:189`).
pub const CONTROL_TOGGLE_TYPE_ID: i32 = CLUSTER_CONTROL_TOGGLE_TYPE_ID;

/// `ClusterControl.ToggleState` (`:43-180`). The codes are the counter's
/// values, so they are the contract, not an implementation detail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum ToggleState {
    /// `INACTIVE(0)`: not accepting new actions (this node is not the leader).
    Inactive = 0,
    /// `NEUTRAL(1)`: ready to accept a new action.
    Neutral = 1,
    /// `SUSPEND(2)`: suspend processing of ingress and timers.
    Suspend = 2,
    /// `RESUME(3)`: resume processing of ingress and timers.
    Resume = 3,
    /// `SNAPSHOT(4)`: take a snapshot of cluster state.
    Snapshot = 4,
    /// `SHUTDOWN(5)`: snapshot, then terminate the cluster in an orderly way.
    Shutdown = 5,
    /// `ABORT(6)`: terminate without a snapshot.
    Abort = 6,
    /// `STANDBY_SNAPSHOT(7)`: a standby-only snapshot (premium; a consensus
    /// module is never a standby).
    StandbySnapshot = 7,
}

impl ToggleState {
    /// The value the counter carries (`:98-107`).
    pub const fn code(self) -> i32 {
        self as i32
    }

    /// `ToggleState.get` (`:165-179`) without the counter.
    ///
    /// # Errors
    ///
    /// [`ClusterError::Cluster`] for a value outside the table, the reference's
    /// `invalid toggle value`.
    pub fn from_code(value: i64) -> Result<Self, ClusterError> {
        match value {
            0 => Ok(Self::Inactive),
            1 => Ok(Self::Neutral),
            2 => Ok(Self::Suspend),
            3 => Ok(Self::Resume),
            4 => Ok(Self::Snapshot),
            5 => Ok(Self::Shutdown),
            6 => Ok(Self::Abort),
            7 => Ok(Self::StandbySnapshot),
            _ => Err(ClusterError::Cluster(format!(
                "invalid toggle value: {value}"
            ))),
        }
    }

    /// `ToggleState.get(controlToggle)` (`:165-179`) through this agent's
    /// client.
    ///
    /// # Errors
    ///
    /// The reference's two: `counter is closed` when the slot is gone, and
    /// `invalid toggle value` outside the table.
    pub fn get(client: &Client, counter: Counter) -> Result<Self, ClusterError> {
        let value = client
            .counters_reader()
            .and_then(|counters| counters.value(counter.counter_id()))
            .ok_or_else(|| ClusterError::Cluster("counter is closed".to_owned()))?;
        Self::from_code(value)
    }

    /// `ToggleState.toggle(controlToggle)` (`:118-126`).
    ///
    /// `false` is the reference's answer for "the counter was not in the state
    /// this switch leaves" — another operator won, or the module has not yet
    /// reset the previous action.
    pub fn toggle(self, counters: &CountersReader<'_, ReadWrite>, counter_id: i32) -> bool {
        if self == Self::Resume
            && counters.value(counter_id) == Some(i64::from(Self::Suspend.code()))
        {
            return counters
                .compare_and_set_value(
                    counter_id,
                    i64::from(Self::Suspend.code()),
                    i64::from(Self::Resume.code()),
                )
                .unwrap_or(false);
        }

        counters
            .compare_and_set_value(
                counter_id,
                i64::from(Self::Neutral.code()),
                i64::from(self.code()),
            )
            .unwrap_or(false)
    }

    /// `ToggleState.reset(controlToggle)` (`:133-136`): back to `NEUTRAL`.
    pub fn reset(client: &Client, counter: Counter) {
        write(client, counter, Self::Neutral);
    }

    /// `ToggleState.activate(controlToggle)` (`:143-146`): `NEUTRAL`,
    /// unconditionally.
    pub fn activate(client: &Client, counter: Counter) {
        write(client, counter, Self::Neutral);
    }

    /// `ToggleState.deactivate(controlToggle)` (`:153-156`): `INACTIVE`,
    /// unconditionally.
    pub fn deactivate(client: &Client, counter: Counter) {
        write(client, counter, Self::Inactive);
    }
}

fn write(client: &Client, counter: Counter, state: ToggleState) {
    if let Some(counters) = client.counters_writable() {
        let _ = counter.set_value(&counters, i64::from(state.code()));
    }
}

/// `ClusterControl.findControlToggle` (`:218-227`): the counter id an operator
/// process works from, or `None` when this node has not allocated it.
pub fn find_control_toggle(counters: &CountersReader<'_>, cluster_id: i32) -> Option<i32> {
    find_cluster_counter(counters, CONTROL_TOGGLE_TYPE_ID, cluster_id)
}
