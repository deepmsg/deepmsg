//! The counters the container owns, and how it reports an error.
//!
//! The reference's `Context.conclude` allocates five counters for a service
//! (`ClusteredServiceContainer.java:948-1010`): the error count, a max-cycle and
//! a threshold-exceeded pair, and a snapshot-duration pair. P3-2a allocates the
//! **first** one — it is the counter an error is counted in, and the mark
//! file's error log is the other half of the same report. The other four are
//! the LC-6 name table's business (`P3-8`); the plan says so and this module
//! says so rather than pretending they are absent by accident.
//!
//! `Cluster Container Errors` is the reference's label
//! (`ClusterCounters.java:175-204`), and the key is `clusterId` then
//! `serviceId`, which `common::counters` already builds.

use std::time::Duration;

use deepmsg_client::client::Client;
use deepmsg_client::counter::Counter;
use deepmsg_cluster_common::counters::{self, CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID};
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
