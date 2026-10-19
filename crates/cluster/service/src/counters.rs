//! The counters the container owns, and how it reports an error.
//!
//! The reference's `Context.conclude` allocates five counters for a service
//! (`ClusteredServiceContainer.java:948-1010`): the error count, a max-cycle and
//! a threshold-exceeded pair, and a snapshot-duration pair. P3-2a allocated the
//! **first** one — it is the counter an error is counted in, and the mark
//! file's error log is the other half of the same report. P3-2b adds the
//! snapshot-duration pair (236/237), because `executeAction` is where they are
//! written and a tracker with no counters would be a half-wired contract. The
//! cycle-time pair (218/219) stays with `P3-8`, with the LC-6 name table.
//!
//! `Cluster Container Errors` is the reference's label
//! (`ClusterCounters.java:175-204`), and the key is `clusterId` then
//! `serviceId`, which `common::counters` already builds.

use std::time::Duration;

use deepmsg_client::client::Client;
use deepmsg_client::counter::Counter;
use deepmsg_cluster_common::counters::{
    self, CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID,
    CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID,
    CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID,
};
use deepmsg_cluster_common::mark::ClusterMarkFile;
use deepmsg_cnc::error_log::{DistinctErrorLog, ErrorLogRegion};

use crate::error::ContainerError;

/// How long a counter allocation may take before the driver is called silent.
pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

/// What this build calls itself in the versioned counter labels.
pub const VERSION: &str = "1.53.2";

/// The commit marker the consensus module's counters carry, for the same reason.
pub const COMMIT_HASH: &str = "deepmsg";

/// The error counter plus the distinct error log that shares the mark file.
pub struct ErrorReporter {
    log: DistinctErrorLog,
    counter: Counter,
}

/// `SnapshotDurationTracker` (`SnapshotDurationTracker.java:31-105`): the two
/// counters 236/237 and the start stamp between them.
pub struct SnapshotDuration {
    max_duration: Counter,
    threshold_exceeded: Counter,
    threshold_ns: i64,
    /// `snapshotStartTimeNs`; `None` is the reference's `Long.MIN_VALUE`.
    start_ns: Option<i64>,
}

impl SnapshotDuration {
    /// Allocate `Clustered service max snapshot duration in ns` (236) and its
    /// threshold-exceeded count (237) (`ClusteredServiceContainer.java:988-1008`).
    ///
    /// # Errors
    ///
    /// [`ContainerError`] when a label does not fit or the driver refuses a
    /// counter.
    pub fn allocate(
        client: &mut Client,
        cluster_id: i32,
        service_id: i32,
        threshold_ns: i64,
    ) -> Result<Self, ContainerError> {
        let max_spec = counters::service_counter(
            "Clustered service max snapshot duration in ns",
            cluster_id,
            service_id,
        )?;
        let max_duration = client.add_counter(
            CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID,
            max_spec.key(),
            max_spec.label(),
            COMMAND_TIMEOUT,
        )?;

        let name = format!(
            "Clustered service max snapshot duration exceeded count: threshold={}",
            format_duration_ns(threshold_ns)
        );
        let exceeded_spec = counters::service_counter(&name, cluster_id, service_id)?;
        let threshold_exceeded = client.add_counter(
            CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID,
            exceeded_spec.key(),
            exceeded_spec.label(),
            COMMAND_TIMEOUT,
        )?;

        Ok(Self {
            max_duration,
            threshold_exceeded,
            threshold_ns,
            start_ns: None,
        })
    }

    /// `onSnapshotBegin` (`:74-77`).
    pub const fn on_snapshot_begin(&mut self, now_ns: i64) {
        self.start_ns = Some(now_ns);
    }

    /// `onSnapshotEnd` (`:84-97`): propose the max, count a threshold breach.
    pub fn on_snapshot_end(&mut self, client: &Client, now_ns: i64) {
        let Some(start_ns) = self.start_ns else {
            return;
        };

        let duration_ns = now_ns.saturating_sub(start_ns);
        if duration_ns > self.threshold_ns {
            if let Some(counters) = client.counters_writable() {
                let value = counters
                    .value(self.threshold_exceeded.counter_id())
                    .unwrap_or(0);
                counters.set_value(self.threshold_exceeded.counter_id(), value + 1);
            }
        }

        let current = client
            .counters_reader()
            .and_then(|counters| counters.value(self.max_duration.counter_id()))
            .unwrap_or(0);
        if duration_ns > current {
            if let Some(counters) = client.counters_writable() {
                let _ = self.max_duration.propose_max(&counters, duration_ns);
            }
        }
    }
}

/// `SystemUtil.formatDuration` (`SystemUtil.java`): the exact-division ladder,
/// in this order, with the reference's suffixes.
fn format_duration_ns(duration_ns: i64) -> String {
    for (unit, scale) in [("s", 1_000_000_000_i64), ("ms", 1_000_000), ("us", 1_000)] {
        if duration_ns >= scale && duration_ns % scale == 0 {
            return format!("{}{unit}", duration_ns / scale);
        }
    }
    format!("{duration_ns}ns")
}

impl ErrorReporter {
    /// Allocate `Cluster Container Errors` for `(clusterId, serviceId)`
    /// (`ClusteredServiceContainer.java:948-957`).
    ///
    /// # Errors
    ///
    /// [`ContainerError`] when the label does not fit or the driver refuses the
    /// counter.
    pub fn allocate(
        client: &mut Client,
        cluster_id: i32,
        service_id: i32,
    ) -> Result<Self, ContainerError> {
        let spec = counters::service_error_counter(cluster_id, service_id, VERSION, COMMIT_HASH)?;
        let counter = client.add_counter(
            CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID,
            spec.key(),
            spec.label(),
            COMMAND_TIMEOUT,
        )?;

        Ok(Self {
            log: DistinctErrorLog::new(),
            counter,
        })
    }

    /// Count the error and record it in the mark file's distinct error log
    /// (`CountedErrorHandler.onError`, `:1160-1180`).
    ///
    /// A counter or log that cannot be written is reported on stderr: this is
    /// the path that exists because something else failed, and a second failure
    /// must not mask the first.
    pub fn report(
        &mut self,
        client: &Client,
        mark_file: &ClusterMarkFile,
        now_ms: i64,
        error: &ContainerError,
    ) {
        let value = client
            .counters_reader()
            .and_then(|counters| self.counter.value(&counters))
            .unwrap_or(0);
        if let Some(counters) = client.counters_writable() {
            self.counter.set_value(&counters, value + 1);
        }

        let description = error.to_string();
        let Some(buffer) = mark_file.error_buffer() else {
            eprintln!("{description}");
            return;
        };

        if let Err(error) = self
            .log
            .record(&ErrorLogRegion::new(buffer), now_ms, 0, &description)
        {
            eprintln!("{description} (and it did not fit the error log: {error:?})");
        }
    }
}
