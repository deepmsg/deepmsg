//! The LC-4a smoke's business: the product echo, plus an observation.
//!
//! P3-2c moved the echo — the 4-byte echo, the 16-byte timer commands and the
//! reference's service-message echo — into the product crate
//! (`deepmsg_cluster_service::services::echo`), because the container binary
//! selects it by name. The one thing that did not move is the flag below:
//! `on_terminate` has no observable in the reference's business API, so a test
//! that wants to know it ran has to be the business that runs.
//!
//! Everything else forwards, method for method.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use deepmsg_cluster_service::cluster::Cluster;
use deepmsg_cluster_service::service::{LogHeader, SnapshotReader, SnapshotWriter};
use deepmsg_cluster_service::{ClusteredService, ContainerError, EchoService, Role, SessionId};
use deepmsg_codec_cluster::close_reason::CloseReason;

/// The product echo, recording that it was terminated.
pub struct EchoServiceWithTermination {
    echo: EchoService,
    terminated: Arc<AtomicBool>,
}

impl EchoServiceWithTermination {
    /// A fresh service and the flag its termination flips.
    pub fn new() -> (Self, Arc<AtomicBool>) {
        let terminated = Arc::new(AtomicBool::new(false));
        (
            Self {
                echo: EchoService::new(),
                terminated: Arc::clone(&terminated),
            },
            terminated,
        )
    }
}

impl ClusteredService for EchoServiceWithTermination {
    fn on_start(
        &mut self,
        cluster: &mut Cluster<'_>,
        snapshot: Option<&mut SnapshotReader<'_>>,
    ) -> Result<(), ContainerError> {
        self.echo.on_start(cluster, snapshot)
    }

    fn on_session_open(
        &mut self,
        cluster: &mut Cluster<'_>,
        session: SessionId,
        timestamp: i64,
    ) -> Result<(), ContainerError> {
        self.echo.on_session_open(cluster, session, timestamp)
    }

    fn on_session_close(
        &mut self,
        cluster: &mut Cluster<'_>,
        session: SessionId,
        timestamp: i64,
        reason: CloseReason,
    ) -> Result<(), ContainerError> {
        self.echo
            .on_session_close(cluster, session, timestamp, reason)
    }

    fn on_session_message(
        &mut self,
        cluster: &mut Cluster<'_>,
        session: Option<SessionId>,
        timestamp: i64,
        payload: &[u8],
        header: &LogHeader,
    ) -> Result<(), ContainerError> {
        self.echo
            .on_session_message(cluster, session, timestamp, payload, header)
    }

    fn on_timer_event(
        &mut self,
        cluster: &mut Cluster<'_>,
        correlation_id: i64,
        timestamp: i64,
    ) -> Result<(), ContainerError> {
        self.echo.on_timer_event(cluster, correlation_id, timestamp)
    }

    fn on_take_snapshot(&mut self, writer: &mut SnapshotWriter<'_>) -> Result<(), ContainerError> {
        self.echo.on_take_snapshot(writer)
    }

    fn on_role_change(&mut self, new_role: Role) -> Result<(), ContainerError> {
        self.echo.on_role_change(new_role)
    }

    fn on_terminate(&mut self, cluster: &mut Cluster<'_>) -> Result<(), ContainerError> {
        self.terminated.store(true, Ordering::Release);
        self.echo.on_terminate(cluster)
    }
}
