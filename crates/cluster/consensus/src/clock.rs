//! The clock the consensus module timestamps with.
//!
//! Counterpart of `ClusterClock` (`aeron-cluster/src/main/java/io/aeron/cluster/service/ClusterClock.java`)
//! and its default `MillisecondClusterClock`. The reference lets a deployment
//! name its own implementation with `aeron.cluster.clock`; P3-4a reads the name
//! and **refuses** a class it cannot instantiate, rather than silently using
//! milliseconds (`common::config::names` records that disposition).
//!
//! The distinction that matters: a *cluster* timestamp is a count of the clock's
//! own unit since the epoch, and it is what goes into `TimerEvent`, `NewLeadershipTermEvent`
//! and every log message (`SessionMessageHeader.timestamp`). A *deadline* is
//! always nanoseconds. [`ClusterClock::convert_to_nanos`] is the one crossing.

use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;

/// A clock counted in [`ClusterTimeUnit`]s since 1 January 1970 UTC.
pub trait ClusterClock {
    /// The count of the clock's units since the epoch (`ClusterClock.time()`).
    fn time(&self) -> i64;

    /// The unit [`Self::time`] counts in (`ClusterClock.timeUnit()`).
    fn time_unit(&self) -> ClusterTimeUnit;

    /// `ClusterClock.convertToNanos(long)`.
    fn convert_to_nanos(&self, timestamp: i64) -> i64 {
        match self.time_unit() {
            // `timeUnit().toNanos(..)` saturates rather than wrapping; a cluster
            // timestamp is a deadline base here, so saturation is the safe end.
            ClusterTimeUnit::MILLIS => timestamp.saturating_mul(1_000_000),
            ClusterTimeUnit::MICROS => timestamp.saturating_mul(1_000),
            ClusterTimeUnit::NANOS => timestamp,
            ClusterTimeUnit::NullVal => timestamp.saturating_mul(1_000_000),
        }
    }

    /// `ClusterClock.timeMillis()`.
    fn time_millis(&self) -> i64 {
        match self.time_unit() {
            ClusterTimeUnit::MILLIS | ClusterTimeUnit::NullVal => self.time(),
            ClusterTimeUnit::MICROS => self.time() / 1_000,
            ClusterTimeUnit::NANOS => self.time() / 1_000_000,
        }
    }

    /// `ClusterClock.timeNanos()`.
    fn time_nanos(&self) -> i64 {
        self.convert_to_nanos(self.time())
    }

    /// The inverse of [`Self::convert_to_nanos`]: a nanosecond deadline turned
    /// back into a cluster timestamp. `Election.nanosToTimestamp`
    /// (`Election.java:1601-1604`) is `timeUnit().convert(ns, NANOSECONDS)`.
    fn timestamp_from_nanos(&self, now_ns: i64) -> i64 {
        match self.time_unit() {
            ClusterTimeUnit::MILLIS | ClusterTimeUnit::NullVal => now_ns / 1_000_000,
            ClusterTimeUnit::MICROS => now_ns / 1_000,
            ClusterTimeUnit::NANOS => now_ns,
        }
    }
}

/// The default: milliseconds since the epoch (`MillisecondClusterClock`).
///
/// A unit struct rather than a wrapper around a function pointer because it is
/// read once per duty cycle and once per `RecordingLog` entry, and there is only
/// one behaviour to select: the reference's property names a class, and the only
/// class this build implements is this one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MillisecondClusterClock;

impl ClusterClock for MillisecondClusterClock {
    fn time(&self) -> i64 {
        deepmsg_core::clock::epoch_millis()
    }

    fn time_unit(&self) -> ClusterTimeUnit {
        ClusterTimeUnit::MILLIS
    }
}
