//! The business the LC-4a smoke runs: an echo, plus the two timer commands.
//!
//! It is deliberately the smallest thing that makes the S2 seam observable from
//! outside: a 4-byte message is offered straight back to the session that sent
//! it, and a 16-byte command schedules or cancels a timer whose event is then
//! echoed as its 8-byte correlation id. Both halves are deterministic — the
//! only inputs are log events — because a clustered service's state must be.
//!
//! It lives in the test crate because P3-2a has no container binary to load a
//! business into; P3-2b's business twin (`basic_auction`) belongs beside the
//! container's own tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use deepmsg_cluster_service::Role;
use deepmsg_cluster_service::cluster::Cluster;
use deepmsg_cluster_service::service::{LogHeader, SnapshotReader, SnapshotWriter};
use deepmsg_cluster_service::{ClusteredService, ContainerError, SessionId};
use deepmsg_codec_cluster::close_reason::CloseReason;

/// Schedule a timer: `(i32 command, i64 correlationId, i32 delayMillis)`.
const SCHEDULE: i32 = 1;
/// Cancel the timer with that correlation id.
const CANCEL: i32 = 2;

/// The echo service, and the flag that says `on_terminate` ran.
pub struct EchoService {
    /// `(correlationId, the session that scheduled it)`.
    pending: Vec<(i64, Option<SessionId>)>,
    terminated: Arc<AtomicBool>,
}

impl EchoService {
    /// A fresh service and the flag its termination flips.
    pub fn new() -> (Self, Arc<AtomicBool>) {
        let terminated = Arc::new(AtomicBool::new(false));
        (
            Self {
                pending: Vec::new(),
                terminated: Arc::clone(&terminated),
            },
            terminated,
        )
    }

    fn offer_to_session(
        cluster: &mut Cluster<'_>,
        session: SessionId,
        payload: &[u8],
    ) -> Result<(), ContainerError> {
        // `SEND_ATTEMPTS = 3`, as the reference's own echo service has it.
        for _ in 0..3 {
            let offered = {
                let Some(mut view) = cluster.session(session) else {
                    return Ok(());
                };
                view.offer(payload)?
            };
            if offered > 0 {
                return Ok(());
            }
            cluster.idle(1)?;
        }
        Ok(())
    }
}

impl ClusteredService for EchoService {
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
        cluster: &mut Cluster<'_>,
        session: Option<SessionId>,
        _timestamp: i64,
        payload: &[u8],
        _header: &LogHeader,
    ) -> Result<(), ContainerError> {
        match payload.len() {
            4 => {
                if let Some(session) = session {
                    Self::offer_to_session(cluster, session, payload)?;
                }
            }
            16 => {
                let command = i32::from_le_bytes(payload[0..4].try_into().expect("four bytes"));
                let correlation_id =
                    i64::from_le_bytes(payload[4..12].try_into().expect("eight bytes"));
                let delay_millis =
                    i32::from_le_bytes(payload[12..16].try_into().expect("four bytes"));

                match command {
                    SCHEDULE => {
                        let deadline = cluster.time() + i64::from(delay_millis);
                        if cluster.schedule_timer(correlation_id, deadline)? {
                            self.pending.push((correlation_id, session));
                        }
                    }
                    CANCEL => {
                        cluster.cancel_timer(correlation_id)?;
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn on_timer_event(
        &mut self,
        cluster: &mut Cluster<'_>,
        correlation_id: i64,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
        if let Some(position) = self
            .pending
            .iter()
            .position(|(pending, _)| *pending == correlation_id)
        {
            let (_, session) = self.pending.remove(position);
            if let Some(session) = session {
                let payload = correlation_id.to_le_bytes();
                Self::offer_to_session(cluster, session, &payload)?;
            }
        }
        Ok(())
    }

    fn on_take_snapshot(&mut self, _writer: &mut SnapshotWriter<'_>) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_role_change(&mut self, _new_role: Role) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_terminate(&mut self, _cluster: &mut Cluster<'_>) -> Result<(), ContainerError> {
        self.terminated.store(true, Ordering::Release);
        Ok(())
    }
}
