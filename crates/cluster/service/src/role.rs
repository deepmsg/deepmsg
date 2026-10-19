//! The two small enums the business API exposes: a node's role and the unit of
//! the cluster's clock.

use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;

use crate::error::ContainerError;

/// `Cluster.Role` (`Cluster.java:51-136`): the code space is its ordinal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Role {
    /// The node is a follower in the current leadership term (`:56`).
    #[default]
    Follower,
    /// The node is contesting the election (`:64`).
    Candidate,
    /// The node is the leader for the current leadership term (`:70`).
    Leader,
}

impl Role {
    /// `Role.get(long)` (`Cluster.java:92-101`): anything outside the code
    /// space is not a role.
    ///
    /// # Errors
    ///
    /// [`ContainerError::Cluster`] for a code no `Role` has.
    pub fn get(code: i64) -> Result<Self, ContainerError> {
        match code {
            0 => Ok(Self::Follower),
            1 => Ok(Self::Candidate),
            2 => Ok(Self::Leader),
            _ => Err(ContainerError::Cluster(format!(
                "Invalid role counter code: {code}"
            ))),
        }
    }

    /// The code the counters and the wire carry.
    pub const fn code(self) -> i32 {
        match self {
            Self::Follower => 0,
            Self::Candidate => 1,
            Self::Leader => 2,
        }
    }
}

/// `java.util.concurrent.TimeUnit`, restricted to the three the cluster uses.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TimeUnit {
    /// Milliseconds: the default and the only clock this build has.
    #[default]
    Milliseconds,
    /// Microseconds.
    Microseconds,
    /// Nanoseconds.
    Nanoseconds,
}

impl TimeUnit {
    /// `ClusterClock.map(ClusterTimeUnit)` (`ClusterClock.java:96-119`):
    /// `NULL_VAL` means milliseconds.
    pub const fn from_cluster_time_unit(unit: ClusterTimeUnit) -> Self {
        match unit {
            ClusterTimeUnit::MICROS => Self::Microseconds,
            ClusterTimeUnit::NANOS => Self::Nanoseconds,
            ClusterTimeUnit::MILLIS | ClusterTimeUnit::NullVal => Self::Milliseconds,
        }
    }

    /// The wire code for the snapshot marker: `ClusterClock.map`'s inverse.
    pub const fn to_cluster_time_unit(self) -> ClusterTimeUnit {
        match self {
            Self::Milliseconds => ClusterTimeUnit::MILLIS,
            Self::Microseconds => ClusterTimeUnit::MICROS,
            Self::Nanoseconds => ClusterTimeUnit::NANOS,
        }
    }

    /// `TimeUnit.toMicros`.
    pub const fn to_micros(self, time: i64) -> i64 {
        match self {
            Self::Milliseconds => time.saturating_mul(1_000),
            Self::Microseconds => time,
            Self::Nanoseconds => time / 1_000,
        }
    }

    /// `TimeUnit.toNanos`.
    pub const fn to_nanos(self, time: i64) -> i64 {
        match self {
            Self::Milliseconds => time.saturating_mul(1_000_000),
            Self::Microseconds => time.saturating_mul(1_000),
            Self::Nanoseconds => time,
        }
    }
}
