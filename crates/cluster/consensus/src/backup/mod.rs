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

pub mod agent;
pub mod consensus_publisher;
pub mod context;
pub mod log_source_validator;
pub mod publication_group;
pub mod snapshot_replication;

use deepmsg_client::client::Client;
use deepmsg_cluster_common::counters::{
    self, CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID, CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID,
    CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID, CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID,
    CLUSTER_BACKUP_STATE_TYPE_ID,
};

use crate::backup::agent::{BackupCounters, ClusterBackupAgent, ClusterBackupComponents};
use crate::backup::context::{ClusterBackupConfig, ClusterBackupContext};
use crate::backup::publication_group::COMMAND_TIMEOUT;
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

/// `ClusterBackup.Configuration.ReplayStart` (`ClusterBackup.java:538-549`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReplayStart {
    /// Start from the earliest available position in the cluster log.
    #[default]
    Beginning,
    /// Start from the log position of the most recent snapshot.
    LatestSnapshot,
}

impl ReplayStart {
    /// The property's spelling.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Beginning => "BEGINNING",
            Self::LatestSnapshot => "LATEST_SNAPSHOT",
        }
    }

    /// `ReplayStart.valueOf` (`ClusterBackup.java:569-578`).
    ///
    /// # Errors
    ///
    /// The value is neither `BEGINNING` nor `LATEST_SNAPSHOT`.
    pub fn parse(value: &str) -> Result<Self, ClusterError> {
        match value {
            "BEGINNING" => Ok(Self::Beginning),
            "LATEST_SNAPSHOT" => Ok(Self::LatestSnapshot),
            other => Err(ClusterError::Cluster(format!(
                "{name}={other} is not a ReplayStart",
                name = crate::backup::context::INITIAL_REPLAY_START_PROP_NAME
            ))),
        }
    }
}

/// `ClusterBackup` (`ClusterBackup.java:208-322`): a concluded context with an
/// agent on it.
///
/// The reference's class is a `Context` plus an `AgentRunner` — everything that
/// happens is in the agent. The same shape here: [`Self::launch`] concludes the
/// context, makes the Aeron client and the five counters, and starts the agent
/// (its `onStart` opens the recording log and the local archive).
pub struct ClusterBackup;

impl ClusterBackup {
    /// Launch with default components — no credentials, no listener, no hooks.
    ///
    /// # Errors
    ///
    /// As [`Self::launch_with_components`].
    pub fn launch(context: ClusterBackupContext) -> Result<ClusterBackupAgent, ClusterError> {
        Self::launch_with_components(context, ClusterBackupComponents::default())
    }

    /// The same, with the deployment's live objects supplied in process — the
    /// equivalent of the reference's configurable `Context` fields.
    ///
    /// # Errors
    ///
    /// The context's `conclude`, the driver's client, a counter the driver
    /// refuses, or the agent's `onStart`.
    pub fn launch_with_components(
        mut context: ClusterBackupContext,
        components: ClusterBackupComponents,
    ) -> Result<ClusterBackupAgent, ClusterError> {
        context.conclude()?;

        let config = context.config().clone();
        deepmsg_driver::sys::set_current_thread_name(&config.role_name()).map_err(|error| {
            ClusterError::Cluster(format!("could not name the agent's thread: {error}"))
        })?;

        let mut client = Client::connect(&config.aeron_dir).map_err(|error| {
            ClusterError::Cluster(format!(
                "the backup's Aeron client will not connect: {error}"
            ))
        })?;

        let counters = allocate_counters(&mut client, &config)?;
        let mut agent = ClusterBackupAgent::new(context, client, counters, components)?;
        agent.on_start()?;

        Ok(agent)
    }
}

/// The five counters `ClusterBackup.Context.conclude` allocates
/// (`ClusterBackup.java:778-819`).
///
/// # Errors
///
/// A label the counter's field will not hold, or a driver that refuses the add.
fn allocate_counters(
    client: &mut Client,
    config: &ClusterBackupConfig,
) -> Result<BackupCounters, ClusterError> {
    let cluster_id = config.cluster_id;
    let allocate = |client: &mut Client,
                    type_id: i32,
                    spec: counters::CounterSpec|
     -> Result<deepmsg_client::counter::Counter, ClusterError> {
        client
            .add_counter(type_id, spec.key(), spec.label(), COMMAND_TIMEOUT)
            .map_err(|error| ClusterError::Cluster(format!("the backup's counters: {error}")))
    };

    let errors = allocate(
        client,
        CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID,
        // The same two literals the consensus module's own versioned counter
        // carries (`agent/mod.rs::allocate_counters`): the reference's
        // `ClusterBackupVersion.VERSION` and `GIT_SHA`.
        counters::versioned_cluster_counter(
            "ClusterBackup Errors",
            cluster_id,
            "1.53.2",
            "deepmsg",
        )?,
    )?;
    let state = allocate(
        client,
        CLUSTER_BACKUP_STATE_TYPE_ID,
        counters::cluster_counter("ClusterBackup State", cluster_id)?,
    )?;
    let live_log_position = allocate(
        client,
        CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID,
        counters::cluster_counter("ClusterBackup live log position", cluster_id)?,
    )?;
    let next_query_deadline_ms = allocate(
        client,
        CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID,
        counters::cluster_counter("ClusterBackup next query deadline in ms", cluster_id)?,
    )?;
    let snapshots_retrieved = allocate(
        client,
        CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID,
        counters::cluster_counter("ClusterBackup snapshots retrieved", cluster_id)?,
    )?;

    Ok(BackupCounters {
        errors,
        state,
        live_log_position,
        next_query_deadline_ms,
        snapshots_retrieved,
    })
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
