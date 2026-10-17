//! The recovery-state counter: the one cluster counter whose key is a struct.
//!
//! Counterpart of `service/RecoveryState.java`. The counter exists only while
//! a consensus module is recovering: the module allocates it before the
//! container starts (`ConsensusModuleAgent.java:2865-2874`), the container
//! reads it to learn which snapshot recordings to load (`ClusteredServiceContainer`
//! `recoverState`, `CSA:765`), and it is removed when recovery finishes.
//!
//! The key layout is `RecoveryState.java:34-57`:
//!
//! ```text
//!   0  leadershipTermId  i64
//!   8  logPosition       i64
//!  16  timestamp         i64
//!  24  clusterId         i32
//!  28  serviceCount      i32
//!  32  snapshotRecordingId[0]  i64
//!  40  snapshotRecordingId[1]  i64
//!  …
//! ```
//!
//! The label is `Cluster recovery: leadershipTermId=<n> logPosition=<n>
//! clusterId=<n>` (`RecoveryState.java:69`, `:142-147`).

use super::{CLUSTER_RECOVERY_STATE_TYPE_ID, CounterError, CounterSpec};

/// `RecoveryState.NAME` (`RecoveryState.java:69`).
pub const RECOVERY_STATE_NAME: &str = "Cluster recovery: leadershipTermId=";

/// `RecoveryState.LEADERSHIP_TERM_ID_OFFSET`.
pub const LEADERSHIP_TERM_ID_OFFSET: usize = 0;
/// `RecoveryState.LOG_POSITION_OFFSET`.
pub const LOG_POSITION_OFFSET: usize = LEADERSHIP_TERM_ID_OFFSET + 8;
/// `RecoveryState.TIMESTAMP_OFFSET`.
pub const TIMESTAMP_OFFSET: usize = LOG_POSITION_OFFSET + 8;
/// `RecoveryState.CLUSTER_ID_OFFSET`.
pub const CLUSTER_ID_OFFSET: usize = TIMESTAMP_OFFSET + 8;
/// `RecoveryState.SERVICE_COUNT_OFFSET`.
pub const SERVICE_COUNT_OFFSET: usize = CLUSTER_ID_OFFSET + 4;
/// `RecoveryState.SNAPSHOT_RECORDING_IDS_OFFSET`.
pub const SNAPSHOT_RECORDING_IDS_OFFSET: usize = SERVICE_COUNT_OFFSET + 4;

/// The type id, re-exported here because a recovery-state reader should not
/// have to know that the constant lives in `AeronCounters`.
pub const TYPE_ID: i32 = CLUSTER_RECOVERY_STATE_TYPE_ID;

/// A recovery-state counter's key, parsed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryStateKey {
    /// The term the snapshot was taken in.
    pub leadership_term_id: i64,
    /// The log position the snapshot covers.
    pub log_position: i64,
    /// When recovery began, epoch milliseconds.
    pub timestamp: i64,
    /// Which cluster this counter belongs to.
    pub cluster_id: i32,
    /// One snapshot recording id per service, indexed by service id.
    pub snapshot_recording_ids: Vec<i64>,
}

impl RecoveryStateKey {
    /// Build a key from its fields.
    ///
    /// # Errors
    ///
    /// [`CounterError::KeyTooLong`] when there are more services than the
    /// 112-byte counter key can hold. The reference throws
    /// `ClusterException` at exactly the same point
    /// (`RecoveryState.java:129-133`).
    pub fn new(
        leadership_term_id: i64,
        log_position: i64,
        timestamp: i64,
        cluster_id: i32,
        snapshot_recording_ids: Vec<i64>,
    ) -> Result<Self, CounterError> {
        let key_length = SNAPSHOT_RECORDING_IDS_OFFSET + snapshot_recording_ids.len() * 8;
        if key_length > deepmsg_cnc::layout::COUNTER_KEY_LENGTH {
            return Err(CounterError::KeyTooLong { length: key_length });
        }

        Ok(Self {
            leadership_term_id,
            log_position,
            timestamp,
            cluster_id,
            snapshot_recording_ids,
        })
    }

    /// The key and label bytes for the counter.
    ///
    /// The key length is `32 + 8 * serviceCount`, not the full 112 bytes: a
    /// counter's key field is fixed-width, but the writer publishes only the
    /// bytes it means (`aeron_counters_manager.c:112-119`), and the reference's
    /// `addCounter` is told the same length here.
    ///
    /// # Errors
    ///
    /// [`CounterError::KeyTooLong`] from [`Self::new`] is impossible here
    /// because the constructor already checked it; [`CounterError::LabelTooLong`]
    /// if the label does not fit.
    pub fn encode(&self) -> Result<CounterSpec, CounterError> {
        let mut key = Vec::with_capacity(
            SNAPSHOT_RECORDING_IDS_OFFSET + self.snapshot_recording_ids.len() * 8,
        );
        key.extend_from_slice(&self.leadership_term_id.to_le_bytes());
        key.extend_from_slice(&self.log_position.to_le_bytes());
        key.extend_from_slice(&self.timestamp.to_le_bytes());
        key.extend_from_slice(&self.cluster_id.to_le_bytes());
        let service_count = self.snapshot_recording_ids.len() as i32;
        key.extend_from_slice(&service_count.to_le_bytes());
        for recording_id in &self.snapshot_recording_ids {
            key.extend_from_slice(&recording_id.to_le_bytes());
        }

        let label = format!(
            "{RECOVERY_STATE_NAME}{} logPosition={} clusterId={}",
            self.leadership_term_id, self.log_position, self.cluster_id
        );

        // `finish` is the single place that enforces both field widths, so this
        // path and the `ClusterCounters` ones cannot drift.
        super::finish(key, label)
    }

    /// Parse a key read back out of a counter.
    ///
    /// The input is the counter's key field — [`deepmsg_cnc::CountersReader::key`]
    /// hands back all 112 bytes, and the reader must believe `serviceCount`
    /// rather than the width.
    ///
    /// # Errors
    ///
    /// [`CounterError::KeyTooShort`] for a key without the 32-byte prefix, and
    /// [`CounterError::InvalidServiceCount`] for a negative count or one that
    /// runs past the supplied bytes. The reference does **not** validate the
    /// count on read (`RecoveryState.java:259-266`), which is a latent overread
    /// against a torn key; the check here changes no bytes.
    pub fn decode(key: &[u8]) -> Result<Self, CounterError> {
        if key.len() < SNAPSHOT_RECORDING_IDS_OFFSET {
            return Err(CounterError::KeyTooShort { length: key.len() });
        }

        let service_count = read_i32(key, SERVICE_COUNT_OFFSET);
        if service_count < 0 {
            return Err(CounterError::InvalidServiceCount {
                count: service_count,
            });
        }
        let count = service_count as usize;
        let needed = SNAPSHOT_RECORDING_IDS_OFFSET + count * 8;
        if key.len() < needed {
            return Err(CounterError::KeyTooShort { length: key.len() });
        }

        let mut snapshot_recording_ids = Vec::with_capacity(count);
        for i in 0..count {
            snapshot_recording_ids.push(read_i64(key, SNAPSHOT_RECORDING_IDS_OFFSET + i * 8));
        }

        Ok(Self {
            leadership_term_id: read_i64(key, LEADERSHIP_TERM_ID_OFFSET),
            log_position: read_i64(key, LOG_POSITION_OFFSET),
            timestamp: read_i64(key, TIMESTAMP_OFFSET),
            cluster_id: read_i32(key, CLUSTER_ID_OFFSET),
            snapshot_recording_ids,
        })
    }

    /// The snapshot recording id for one service.
    ///
    /// # Errors
    ///
    /// [`CounterError::ServiceIdOutOfRange`] for an id outside
    /// `0..serviceCount`, which the reference raises as a `ClusterException`
    /// (`RecoveryState.java:260-263`).
    pub fn snapshot_recording_id(&self, service_id: i32) -> Result<i64, CounterError> {
        let service_count = self.snapshot_recording_ids.len() as i32;
        if service_id < 0 || service_id >= service_count {
            return Err(CounterError::ServiceIdOutOfRange {
                service_id,
                service_count,
            });
        }
        Ok(self.snapshot_recording_ids[service_id as usize])
    }
}

fn read_i64(key: &[u8], offset: usize) -> i64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[offset..offset + 8]);
    i64::from_le_bytes(bytes)
}

fn read_i32(key: &[u8], offset: usize) -> i32 {
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&key[offset..offset + 4]);
    i32::from_le_bytes(bytes)
}
