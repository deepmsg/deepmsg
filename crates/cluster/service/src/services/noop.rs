//! `noop` — a business that does nothing.
//!
//! Every callback succeeds and writes nothing, including the snapshot: an empty
//! snapshot is legal (BEGIN and END with no records between them), and a
//! container running this business is the fixture's way to put the container
//! itself under test without a business's state in the way.

use crate::cluster::Cluster;
use crate::error::ContainerError;
use crate::role::Role;
use crate::service::{ClusteredService, LogHeader, SessionId, SnapshotReader, SnapshotWriter};
use deepmsg_codec_cluster::close_reason::CloseReason;

/// The do-nothing business.
#[derive(Default)]
pub struct NoopService;

impl NoopService {
    /// A fresh instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl ClusteredService for NoopService {
    fn on_start(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _snapshot: Option<&mut SnapshotReader<'_>>,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_session_open(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _session: SessionId,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_session_close(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _session: SessionId,
        _timestamp: i64,
        _reason: CloseReason,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_session_message(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _session: Option<SessionId>,
        _timestamp: i64,
        _payload: &[u8],
        _header: &LogHeader,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_timer_event(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _correlation_id: i64,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_take_snapshot(&mut self, _writer: &mut SnapshotWriter<'_>) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_role_change(&mut self, _new_role: Role) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_terminate(&mut self, _cluster: &mut Cluster<'_>) -> Result<(), ContainerError> {
        Ok(())
    }
}
