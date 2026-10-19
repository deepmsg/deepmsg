//! `ClusterBackup` (`P3-9`): the OSS backup node.
//!
//! The reference splits this over four files, and so does this module:
//!
//! * `ClusterBackup.java` — a `Context` and an `AgentRunner` around the agent,
//!   plus the two enums ([`State`], [`SourceType`]);
//! * `ClusterBackupAgent.java` — the eight-state machine (in [`agent`]);
//! * `SnapshotReplication.java` — fetching a cluster's snapshots (in
//!   [`snapshot_replication`]);
//! * `LogSourceValidator.java` — which member may supply the log (in
//!   [`log_source_validator`]).
//!
//! A backup node is not a cluster member. It never joins, never votes and never
//! serves a client: it asks the cluster "where are you" over the consensus
//! channel, then copies the leader's archive into its own. The output is a
//! directory a **consensus module can be started from** — the mark file is
//! written as `ClusterComponentType::BACKUP`, and the takeover rule in
//! `common::mark` is what lets a `CONSENSUS_MODULE` inherit it.
//!
//! # The two halves of P3-9
//!
//! The cluster side of the protocol (answering `BackupQuery`, the `BACKUP`
//! session, its authentication) lives in `session` and `agent`; this module is
//! the backup side. Neither half is useful without the other, which is why the
//! card covers both.

pub mod log_source_validator;
pub mod snapshot_replication;

use crate::error::ClusterError;

/// `Aeron.NULL_VALUE` (`Aeron.java:82`): -1, used by the reference for
/// `long`-typed recording ids, positions, correlations and session ids alike.
pub const NULL_VALUE: i64 = -1;

/// `AeronArchive.NULL_POSITION` (`AeronArchive.java:96`), which is the same -1
/// but carries a different meaning at call sites that take a position.
pub const NULL_POSITION: i64 = -1;

/// `AeronArchive.NULL_LENGTH` (`AeronArchive.java:101`).
pub const NULL_LENGTH: i32 = -1;

/// `CountersReader.NULL_COUNTER_ID`.
pub const NULL_COUNTER_ID: i32 = -1;

/// `ClusterBackup.State` (`ClusterBackup.java:102-200`).
///
/// The code is the ordinal, and the constructor in the reference refuses a
/// code that is not (`:150-155`); [`State::from_code`] is where that lives here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum State {
    /// Query the leader for the cluster's current state.
    #[default]
    BackupQuery = 0,
    /// Retrieve the latest snapshots from the cluster.
    SnapshotRetrieve = 1,
    /// Set up recording of the live log.
    LiveLogRecord = 2,
    /// Replay the live log since the snapshot and join it.
    LiveLogReplay = 3,
    /// Update the local `recording.log` for recovery.
    UpdateRecordingLog = 4,
    /// Back up the live log until the next query deadline.
    BackingUp = 5,
    /// After an error or a stall: reset, cool down, start again.
    ResetBackup = 6,
    /// The backup is complete and closed.
    Closed = 7,
}

impl State {
    /// Every state, in code order, as the reference's `STATES` array
    /// (`ClusterBackup.java:144`).
    pub const ALL: [Self; 8] = [
        Self::BackupQuery,
        Self::SnapshotRetrieve,
        Self::LiveLogRecord,
        Self::LiveLogReplay,
        Self::UpdateRecordingLog,
        Self::BackingUp,
        Self::ResetBackup,
        Self::Closed,
    ];

    /// The code written into the state counter (`code()`, `:163-166`).
    #[must_use]
    pub const fn code(self) -> i64 {
        self as i64
    }

    /// `State.get(long code)` (`:191-199`).
    ///
    /// # Errors
    ///
    /// `ClusterException("invalid state counter code")` for a code outside
    /// `0..=7` — the reference's message names the value.
    pub fn from_code(code: i64) -> Result<Self, ClusterError> {
        usize::try_from(code)
            .ok()
            .and_then(|index| Self::ALL.get(index).copied())
            .ok_or_else(|| ClusterError::Cluster(format!("invalid state counter code: {code}")))
    }
}

/// `ClusterBackup.SourceType` (`ClusterBackup.java:205-219`): which member the
/// backup will take its log from.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SourceType {
    /// Any member.
    #[default]
    Any,
    /// Only the leader.
    Leader,
    /// Any member that is not the leader.
    Follower,
}

impl SourceType {
    /// The reference's enum names, which is what the property carries
    /// (`aeron.cluster.backup.source.type`).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Any => "ANY",
            Self::Leader => "LEADER",
            Self::Follower => "FOLLOWER",
        }
    }

    /// `SourceType.valueOf` (`ClusterBackup.java:905-914`), with the reference's
    /// message naming the value and the three legal ones.
    ///
    /// # Errors
    ///
    /// `"ClusterBackup.Context.sourceType=<value> is not valid. Must be one of:
    /// [ANY, LEADER, FOLLOWER]"`.
    pub fn parse(value: &str) -> Result<Self, ClusterError> {
        match value {
            "ANY" => Ok(Self::Any),
            "LEADER" => Ok(Self::Leader),
            "FOLLOWER" => Ok(Self::Follower),
            other => Err(ClusterError::Cluster(format!(
                "ClusterBackup.Context.sourceType={other} is not valid. \
                 Must be one of: [ANY, LEADER, FOLLOWER]"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_state_code_is_its_ordinal_and_the_range_is_checked() {
        for (index, state) in State::ALL.iter().enumerate() {
            assert_eq!(index as i64, state.code());
            assert_eq!(Ok(*state), State::from_code(index as i64));
        }

        assert!(matches!(
            State::from_code(8),
            Err(ClusterError::Cluster(message)) if message == "invalid state counter code: 8"
        ));
        assert!(State::from_code(-1).is_err());
    }

    #[test]
    fn every_source_type_round_trips_through_its_name() {
        for source_type in [SourceType::Any, SourceType::Leader, SourceType::Follower] {
            assert_eq!(Ok(source_type), SourceType::parse(source_type.name()));
        }

        assert!(SourceType::parse("any").is_err());
        assert!(SourceType::parse("STANDBY").is_err());
    }
}
