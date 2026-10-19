//! The counters a cluster's agents use to talk to each other without messaging.
//!
//! Counterpart of `AeronCounters.java:860-1106` (the type ids),
//! `service/ClusterCounters.java` (the key and label encodings) and
//! `service/RecoveryState.java` (the one counter whose key carries a struct).
//!
//! Three things live here, and they are the three things the rest of P3 asks
//! for:
//!
//! * **The type ids.** Every counter the consensus module, the container and
//!   the backup allocate is in `200..=241`; [`TYPE_IDS`] is that table with the
//!   reference's own constant names, so a reader of a CnC file can say what a
//!   counter is.
//! * **The key and label bytes.** A counter's identity on the wire is its
//!   `(type_id, key)` pair; the label is the human-readable half. Both are
//!   written here and compared byte for byte against files the reference
//!   wrote (`crates/cluster/common/tests/fixtures/counters-metadata.bin`).
//! * **The finders.** `ClusterCounters.find` and `RecoveryState.findCounterId`
//!   search the CnC metadata region for `(type_id, clusterId)`; the difference
//!   between them is *where* the cluster id sits in the key, which is why each
//!   has its own function rather than a shared offset parameter.
//!
//! The encoding half does not allocate on a duty cycle: it is called when an
//! agent starts, once per counter. It does allocate, because the reference
//! allocates too (`ExpandableArrayBuffer` and string concatenation), and
//! because a `Vec`/`String` pair is what [`deepmsg_client::Client::add_counter`]
//! takes. Nothing here is on a hot path.

use deepmsg_cnc::{CountersReader, layout};

pub mod recovery;

pub use recovery::RecoveryStateKey;

/// Suffix the cluster appends to every counter label
/// (`ClusterCounters.CLUSTER_ID_LABEL_SUFFIX`, `ClusterCounters.java:37`).
pub const CLUSTER_ID_LABEL_SUFFIX: &str = " - clusterId=";

/// Suffix a service counter appends after the cluster id
/// (`ClusterCounters.SERVICE_ID_SUFFIX`, `ClusterCounters.java:38`).
pub const SERVICE_ID_SUFFIX: &str = " serviceId=";

/// Prefix of a cluster session counter's label
/// (`ClusterSession.addSessionCounter`, `ClusterSession.java:517-520`).
pub const SESSION_LABEL_PREFIX: &str = "cluster-session: ";

// ---------------------------------------------------------------------------
// Type ids — `AeronCounters.java:860-1106`.
//
// The range is 200..=241 but not every value in it is used: 206 was never
// assigned, 225 is commented out, and the transition-module and standby
// counters belong to components P3 does not build. They are listed anyway —
// a reader of a CnC file must be able to name what it finds, and "this build
// does not write it" is different from "this build does not know it".
// ---------------------------------------------------------------------------

/// Consensus module state, `AeronCounters.java:860`.
pub const CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID: i32 = 200;
/// Node role, `AeronCounters.java:866`.
pub const CLUSTER_NODE_ROLE_TYPE_ID: i32 = 201;
/// Cluster control toggle, `AeronCounters.java:872`.
pub const CLUSTER_CONTROL_TOGGLE_TYPE_ID: i32 = 202;
/// Commit position, `AeronCounters.java:878`.
pub const CLUSTER_COMMIT_POSITION_TYPE_ID: i32 = 203;
/// Recovery state, `AeronCounters.java:884`.
pub const CLUSTER_RECOVERY_STATE_TYPE_ID: i32 = 204;
/// Snapshot count, `AeronCounters.java:890`.
pub const CLUSTER_SNAPSHOT_COUNTER_TYPE_ID: i32 = 205;
/// Election state, `AeronCounters.java:896`.
pub const CLUSTER_ELECTION_STATE_TYPE_ID: i32 = 207;
/// Backup state, `AeronCounters.java:902`.
pub const CLUSTER_BACKUP_STATE_TYPE_ID: i32 = 208;
/// Backup live log position, `AeronCounters.java:908`.
pub const CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID: i32 = 209;
/// Backup query deadline, `AeronCounters.java:914`.
pub const CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID: i32 = 210;
/// Backup error count, `AeronCounters.java:920`.
pub const CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID: i32 = 211;
/// Consensus module error count, `AeronCounters.java:926`.
pub const CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID: i32 = 212;
/// Client timeout count, `AeronCounters.java:932`.
pub const CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID: i32 = 213;
/// Invalid request count, `AeronCounters.java:938`.
pub const CLUSTER_INVALID_REQUEST_COUNT_TYPE_ID: i32 = 214;
/// Clustered service error count, `AeronCounters.java:944`.
pub const CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID: i32 = 215;
/// Consensus module max cycle time, `AeronCounters.java:950`.
pub const CLUSTER_MAX_CYCLE_TIME_TYPE_ID: i32 = 216;
/// Consensus module cycle-time-threshold-exceeded count,
/// `AeronCounters.java:957`.
pub const CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID: i32 = 217;
/// Container max cycle time, `AeronCounters.java:963`.
pub const CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID: i32 = 218;
/// Container cycle-time-threshold-exceeded count, `AeronCounters.java:970`.
pub const CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID: i32 = 219;
/// Standby state (premium), `AeronCounters.java:976`.
pub const CLUSTER_STANDBY_STATE_TYPE_ID: i32 = 220;
/// Standby error count (premium), `AeronCounters.java:982`.
pub const CLUSTER_STANDBY_ERROR_COUNT_TYPE_ID: i32 = 221;
/// Standby heartbeat responses (premium), `AeronCounters.java:988`.
pub const CLUSTER_STANDBY_HEARTBEAT_RESPONSE_COUNT_TYPE_ID: i32 = 222;
/// Standby control toggle (premium), `AeronCounters.java:994`.
pub const CLUSTER_STANDBY_CONTROL_TOGGLE_TYPE_ID: i32 = 223;
/// Transition module state (premium), `AeronCounters.java:1000`.
pub const TRANSITION_MODULE_STATE_TYPE_ID: i32 = 224;
/// Transition module error count (premium), `AeronCounters.java:1008`.
pub const TRANSITION_MODULE_ERROR_COUNT_TYPE_ID: i32 = 226;
/// Standby max cycle time (premium), `AeronCounters.java:1014`.
pub const CLUSTER_STANDBY_MAX_CYCLE_TIME_TYPE_ID: i32 = 227;
/// Standby cycle-time-threshold-exceeded count (premium),
/// `AeronCounters.java:1021`.
pub const CLUSTER_STANDBY_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID: i32 = 228;
/// Transition module max cycle time (premium), `AeronCounters.java:1027`.
pub const TRANSITION_MODULE_MAX_CYCLE_TIME_TYPE_ID: i32 = 229;
/// Transition module cycle-time-threshold-exceeded count (premium),
/// `AeronCounters.java:1034`.
pub const TRANSITION_MODULE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID: i32 = 230;
/// Standby source member id (premium), `AeronCounters.java:1041`.
pub const CLUSTER_STANDBY_SOURCE_MEMBER_ID_TYPE_ID: i32 = 231;
/// Standby snapshot count (premium), `AeronCounters.java:1047`.
pub const CLUSTER_STANDBY_SNAPSHOT_COUNTER_TYPE_ID: i32 = 232;
/// Node control toggle, `AeronCounters.java:1053`.
pub const NODE_CONTROL_TOGGLE_TYPE_ID: i32 = 233;
/// Total max snapshot duration, `AeronCounters.java:1059`.
pub const CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID: i32 = 234;
/// Total snapshot-duration-threshold-exceeded count,
/// `AeronCounters.java:1066`.
pub const CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID: i32 = 235;
/// Per-service max snapshot duration, `AeronCounters.java:1073`.
pub const CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID: i32 = 236;
/// Per-service snapshot-duration-threshold-exceeded count,
/// `AeronCounters.java:1080`.
pub const CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID: i32 = 237;
/// Election count, `AeronCounters.java:1086`.
pub const CLUSTER_ELECTION_COUNT_TYPE_ID: i32 = 238;
/// Leadership term id, `AeronCounters.java:1092`.
pub const CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID: i32 = 239;
/// Backup snapshots retrieved, `AeronCounters.java:1098`.
pub const CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID: i32 = 240;
/// Cluster session, `AeronCounters.java:1106`.
pub const CLUSTER_SESSION_TYPE_ID: i32 = 241;

/// Every type id above, with the reference constant's name.
///
/// The names are the reference's, not this build's: a CnC dump that says
/// `CLUSTER_COMMIT_POSITION_TYPE_ID` is one an `AeronStat` reader recognises.
/// The table is checked against `tests/fixtures/counter-type-ids.tsv`, which is
/// a `grep` of `AeronCounters.java`, so a missing or renamed id is a red test
/// rather than a reader that silently says "unknown".
pub const TYPE_IDS: &[(i32, &str)] = &[
    (200, "CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID"),
    (201, "CLUSTER_NODE_ROLE_TYPE_ID"),
    (202, "CLUSTER_CONTROL_TOGGLE_TYPE_ID"),
    (203, "CLUSTER_COMMIT_POSITION_TYPE_ID"),
    (204, "CLUSTER_RECOVERY_STATE_TYPE_ID"),
    (205, "CLUSTER_SNAPSHOT_COUNTER_TYPE_ID"),
    (207, "CLUSTER_ELECTION_STATE_TYPE_ID"),
    (208, "CLUSTER_BACKUP_STATE_TYPE_ID"),
    (209, "CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID"),
    (210, "CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID"),
    (211, "CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID"),
    (212, "CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID"),
    (213, "CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID"),
    (214, "CLUSTER_INVALID_REQUEST_COUNT_TYPE_ID"),
    (215, "CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID"),
    (216, "CLUSTER_MAX_CYCLE_TIME_TYPE_ID"),
    (217, "CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID"),
    (218, "CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID"),
    (
        219,
        "CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID",
    ),
    (220, "CLUSTER_STANDBY_STATE_TYPE_ID"),
    (221, "CLUSTER_STANDBY_ERROR_COUNT_TYPE_ID"),
    (222, "CLUSTER_STANDBY_HEARTBEAT_RESPONSE_COUNT_TYPE_ID"),
    (223, "CLUSTER_STANDBY_CONTROL_TOGGLE_TYPE_ID"),
    (224, "TRANSITION_MODULE_STATE_TYPE_ID"),
    (226, "TRANSITION_MODULE_ERROR_COUNT_TYPE_ID"),
    (227, "CLUSTER_STANDBY_MAX_CYCLE_TIME_TYPE_ID"),
    (228, "CLUSTER_STANDBY_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID"),
    (229, "TRANSITION_MODULE_MAX_CYCLE_TIME_TYPE_ID"),
    (
        230,
        "TRANSITION_MODULE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID",
    ),
    (231, "CLUSTER_STANDBY_SOURCE_MEMBER_ID_TYPE_ID"),
    (232, "CLUSTER_STANDBY_SNAPSHOT_COUNTER_TYPE_ID"),
    (233, "NODE_CONTROL_TOGGLE_TYPE_ID"),
    (234, "CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID"),
    (
        235,
        "CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID",
    ),
    (236, "CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID"),
    (
        237,
        "CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID",
    ),
    (238, "CLUSTER_ELECTION_COUNT_TYPE_ID"),
    (239, "CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID"),
    (240, "CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID"),
    (241, "CLUSTER_SESSION_TYPE_ID"),
];

// ---------------------------------------------------------------------------
// Key and label bytes.
// ---------------------------------------------------------------------------

/// Why a counter key or label cannot be built, or a key cannot be read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CounterError {
    /// A recovery-state key longer than the 112-byte counter key field.
    KeyTooLong {
        /// The length the encoding asked for.
        length: usize,
    },
    /// A key shorter than the fixed prefix it must carry.
    KeyTooShort {
        /// The length that was supplied.
        length: usize,
    },
    /// A label longer than the 380-byte counter label field.
    LabelTooLong {
        /// The length the encoding asked for.
        length: usize,
    },
    /// A recovery-state key with a negative service count.
    InvalidServiceCount {
        /// The count that was read.
        count: i32,
    },
    /// A service id outside the range the recovery-state key covers.
    ServiceIdOutOfRange {
        /// The id that was asked for.
        service_id: i32,
        /// The count the key carries.
        service_count: i32,
    },
}

impl std::fmt::Display for CounterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyTooLong { length } => {
                write!(
                    f,
                    "{length} exceeds max key length {}",
                    layout::COUNTER_KEY_LENGTH
                )
            }
            Self::KeyTooShort { length } => write!(
                f,
                "recovery state key is {length} bytes, shorter than its 32-byte prefix"
            ),
            Self::LabelTooLong { length } => write!(
                f,
                "{length} exceeds max label length {}",
                layout::COUNTER_LABEL_LENGTH_MAX
            ),
            Self::InvalidServiceCount { count } => {
                write!(f, "invalid service count {count}")
            }
            Self::ServiceIdOutOfRange {
                service_id,
                service_count,
            } => write!(
                f,
                "invalid serviceId {service_id} for count of {service_count}"
            ),
        }
    }
}

impl std::error::Error for CounterError {}

/// A counter's key and label, ready for `Client::add_counter`.
///
/// The key is the bytes the counter's identity is compared on; the label is
/// the text. They are kept together because every allocation site in the
/// reference builds both from the same arguments, and splitting them makes it
/// possible to allocate a counter whose label names a different cluster than
/// its key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CounterSpec {
    key: Vec<u8>,
    label: String,
}

impl CounterSpec {
    /// The key bytes: `clusterId` alone, `clusterId` then `serviceId`, or the
    /// recovery-state struct.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// The label, without a NUL and without padding.
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }
}

/// `ClusterCounters.allocate` (`ClusterCounters.java:54-71`).
///
/// Key: `clusterId` as a 4-byte little-endian `int`. Label:
/// `name - clusterId=<clusterId>`.
///
/// # Errors
///
/// [`CounterError::LabelTooLong`] when the label does not fit the field; the
/// reference lets `Aeron.addCounter` refuse it one layer later.
pub fn cluster_counter(name: &str, cluster_id: i32) -> Result<CounterSpec, CounterError> {
    finish(
        cluster_id.to_le_bytes().to_vec(),
        format!("{name}{CLUSTER_ID_LABEL_SUFFIX}{cluster_id}"),
    )
}

/// `ClusterCounters.allocateVersioned` (`ClusterCounters.java:85-105`).
///
/// Same key as [`cluster_counter`]; the label gains
/// ` version=<fullVersion> commit=<commitHashCode>`
/// (`AeronCounters.appendVersionInfo`, `AeronCounters.java:1577-1584`).
///
/// # Errors
///
/// [`CounterError::LabelTooLong`] as above.
pub fn versioned_cluster_counter(
    name: &str,
    cluster_id: i32,
    version: &str,
    commit_hash: &str,
) -> Result<CounterSpec, CounterError> {
    finish(
        cluster_id.to_le_bytes().to_vec(),
        format!(
            "{name}{CLUSTER_ID_LABEL_SUFFIX}{cluster_id} version={version} commit={commit_hash}"
        ),
    )
}

/// `ClusterCounters.allocateServiceCounter` (`ClusterCounters.java:151-173`).
///
/// Key: `clusterId` then `serviceId`, each a 4-byte little-endian `int`.
/// Label: `name - clusterId=<clusterId> serviceId=<serviceId>`.
///
/// # Errors
///
/// [`CounterError::LabelTooLong`] as above.
pub fn service_counter(
    name: &str,
    cluster_id: i32,
    service_id: i32,
) -> Result<CounterSpec, CounterError> {
    let mut key = Vec::with_capacity(8);
    key.extend_from_slice(&cluster_id.to_le_bytes());
    key.extend_from_slice(&service_id.to_le_bytes());

    finish(
        key,
        format!("{name}{CLUSTER_ID_LABEL_SUFFIX}{cluster_id}{SERVICE_ID_SUFFIX}{service_id}"),
    )
}

/// `ClusterCounters.allocateServiceErrorCounter`
/// (`ClusterCounters.java:175-204`).
///
/// A service counter whose label is also versioned; the container allocates
/// exactly this shape for its error counter (`ClusteredServiceContainer.java:955`).
///
/// # Errors
///
/// [`CounterError::LabelTooLong`] as above.
pub fn service_error_counter(
    cluster_id: i32,
    service_id: i32,
    version: &str,
    commit_hash: &str,
) -> Result<CounterSpec, CounterError> {
    let mut key = Vec::with_capacity(8);
    key.extend_from_slice(&cluster_id.to_le_bytes());
    key.extend_from_slice(&service_id.to_le_bytes());

    finish(
        key,
        format!(
            "Cluster Container Errors{CLUSTER_ID_LABEL_SUFFIX}{cluster_id}{SERVICE_ID_SUFFIX}{service_id} \
             version={version} commit={commit_hash}"
        ),
    )
}

/// `ClusterSession.addSessionCounter` (`ClusterSession.java:502-525`).
///
/// Key: `clusterId` (4 bytes) then the session id (8 bytes), both
/// little-endian. Label: `cluster-session: <sessionInfo> - clusterId=<clusterId>`.
/// The reader that pins this shape is `ClusterTest.java:3247-3255`, which reads
/// the cluster id at 0 and the session id at 4.
///
/// # Errors
///
/// [`CounterError::LabelTooLong`] as above.
pub fn session_counter(
    cluster_id: i32,
    session_id: i64,
    session_info: &str,
) -> Result<CounterSpec, CounterError> {
    let mut key = Vec::with_capacity(12);
    key.extend_from_slice(&cluster_id.to_le_bytes());
    key.extend_from_slice(&session_id.to_le_bytes());

    finish(
        key,
        format!("{SESSION_LABEL_PREFIX}{session_info}{CLUSTER_ID_LABEL_SUFFIX}{cluster_id}"),
    )
}

fn finish(key: Vec<u8>, label: String) -> Result<CounterSpec, CounterError> {
    if key.len() > layout::COUNTER_KEY_LENGTH {
        return Err(CounterError::KeyTooLong { length: key.len() });
    }
    if label.len() > layout::COUNTER_LABEL_LENGTH_MAX {
        return Err(CounterError::LabelTooLong {
            length: label.len(),
        });
    }

    Ok(CounterSpec { key, label })
}

// ---------------------------------------------------------------------------
// Finders.
// ---------------------------------------------------------------------------

/// `ClusterCounters.find` (`ClusterCounters.java:115-138`).
///
/// The cluster id is the **first** four bytes of the key. Returns the id of the
/// first allocated counter with this type id whose key names this cluster,
/// skipping reclaimed records and stopping at the first unused one — the order
/// [`CountersReader::for_each`] already has.
///
/// `None` is the reference's `Aeron.NULL_VALUE`.
#[must_use]
pub fn find_cluster_counter<A>(
    counters: &CountersReader<'_, A>,
    type_id: i32,
    cluster_id: i32,
) -> Option<i32> {
    find_by_key(counters, type_id, 0, cluster_id as u32)
}

/// `RecoveryState.findCounterId` (`RecoveryState.java:159-180`).
///
/// Same search as [`find_cluster_counter`], but the cluster id is at
/// `CLUSTER_ID_OFFSET` = 24 in the key (`RecoveryState.java:86`), after the
/// three `long`s.
#[must_use]
pub fn find_recovery_state(counters: &CountersReader<'_>, cluster_id: i32) -> Option<i32> {
    find_by_key(
        counters,
        CLUSTER_RECOVERY_STATE_TYPE_ID,
        recovery::CLUSTER_ID_OFFSET,
        cluster_id as u32,
    )
}

fn find_by_key<A>(
    counters: &CountersReader<'_, A>,
    type_id: i32,
    offset: usize,
    expected: u32,
) -> Option<i32> {
    let mut found = None;
    counters.for_each(|counter| {
        if found.is_some() || counter.type_id != type_id {
            return;
        }
        let Some(key) = counters.key(counter.counter_id) else {
            return;
        };
        let Some(bytes) = key.get(offset..offset + 4) else {
            return;
        };
        if u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) == expected {
            found = Some(counter.counter_id);
        }
    });
    found
}
