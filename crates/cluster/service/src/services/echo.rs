//! `echo` — the business the LC-4a scenarios run.
//!
//! Two behaviours live here because the reference keeps them in two places:
//!
//! * **the echo** — whatever a session offers comes back on that session
//!   (`aeron-samples/.../cluster/EchoService.java:48-68`, three attempts
//!   replaced by the retry loop its sibling uses);
//! * **the service-message echo** — the reference's test business
//!   (`TestNode.java:669-690`, `StubClusteredService.java:82-104`): the
//!   message [`ECHO_SERVICE_IPC_INGRESS_MSG`] is offered back **into the
//!   cluster** as a service message (`Cluster.offer`), and the service message
//!   that comes back out of the log (`session == None`) is echoed to **every**
//!   client session. That round trip is what `ServiceIpcIngressMessageTest`
//!   asserts, and it cannot be produced by the sample's plain echo.
//!
//! The two halves must not be confused: offering to the cluster from inside the
//! service-message callback would append a second service message and echo
//! itself forever. The reference's `if/else` on `null != session`
//! (`TestNode.java:669-690`) is exactly that guard.
//!
//! # Timer commands
//!
//! A 16-byte message schedules or cancels a timer, so the LC-4a timer scenario
//! has a business to run through the container's `schedule_timer` /
//! `cancel_timer` and its `TimerEvent` path:
//!
//! | Bytes | Meaning |
//! |---|---|
//! | `0..4` | command: `1` = schedule, `2` = cancel |
//! | `4..12` | correlation id |
//! | `12..16` | delay in milliseconds (schedule only) |
//!
//! A fired timer echoes its 8-byte correlation id to the session that
//! scheduled it. The commands are this build's, not the reference's — the
//! reference's timer scenario runs through `TestNode`'s message predicates — and
//! they are recorded in `docs/compat.md`'s list of built-in services.

use crate::cluster::Cluster;
use crate::error::ContainerError;
use crate::proxy::{OFFER_ADMIN_ACTION, OFFER_BACK_PRESSURED};
use crate::role::Role;
use crate::service::{ClusteredService, LogHeader, SessionId, SnapshotReader, SnapshotWriter};
use deepmsg_codec_cluster::close_reason::CloseReason;

/// `ClusterTests.ECHO_SERVICE_IPC_INGRESS_MSG` (`ClusterTests.java:40`).
pub const ECHO_SERVICE_IPC_INGRESS_MSG: &[u8] = b"Echo as Service IPC ingress";

/// Schedule the timer in `(command, correlation id, delay millis)`.
pub const TIMER_SCHEDULE: i32 = 1;
/// Cancel the timer with that correlation id.
pub const TIMER_CANCEL: i32 = 2;
/// A timer command's length.
pub const TIMER_COMMAND_LENGTH: usize = 16;

/// The echo business.
#[derive(Default)]
pub struct EchoService {
    /// `(correlationId, the session that scheduled it)`.
    pending: Vec<(i64, Option<SessionId>)>,
}

impl EchoService {
    /// A fresh echo.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            pending: Vec::new(),
        }
    }

    /// `echoMessage` (`StubClusteredService.java:82-104`): offer until the
    /// publication takes it, idle on back pressure, retry on `ADMIN_ACTION`,
    /// and fail the container on anything else.
    fn echo_to_session(
        cluster: &mut Cluster<'_>,
        session: SessionId,
        payload: &[u8],
    ) -> Result<(), ContainerError> {
        loop {
            let offered = {
                let Some(mut view) = cluster.session(session) else {
                    return Ok(());
                };
                view.offer(payload)?
            };

            if offered > 0 {
                return Ok(());
            }
            if offered == OFFER_BACK_PRESSURED {
                cluster.idle(0)?;
            } else if offered != OFFER_ADMIN_ACTION {
                return Err(ContainerError::Cluster(format!(
                    "egress publication error: result={offered}, clusterSessionId={session}"
                )));
            }
        }
    }

    /// `sendServiceIpcMessage` (`TestNode.java:669-690`).
    fn send_service_ipc_message(
        cluster: &mut Cluster<'_>,
        session: Option<SessionId>,
        payload: &[u8],
    ) -> Result<(), ContainerError> {
        match session {
            // A client message: offer it back into the cluster. The session
            // that sent it is not used — the service message the log carries
            // names this service, not the client (`TestNode.java:675-683`).
            Some(_) => {
                // `while (cluster.offer(...) < 0) idleStrategy.idle();`
                loop {
                    if cluster.offer(payload)? > 0 {
                        return Ok(());
                    }
                    cluster.idle(0)?;
                }
            }
            None => {
                // A service message has no sender: echo it to every session.
                // The ids are collected first because `session()` borrows
                // `Cluster` mutably.
                let sessions: Vec<SessionId> = cluster.sessions().map(|info| info.id).collect();
                for session in sessions {
                    Self::echo_to_session(cluster, session, payload)?;
                }
                Ok(())
            }
        }
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
        if payload == ECHO_SERVICE_IPC_INGRESS_MSG {
            return Self::send_service_ipc_message(cluster, session, payload);
        }

        if payload.len() == TIMER_COMMAND_LENGTH {
            let command = i32::from_le_bytes(payload[0..4].try_into().expect("four bytes"));
            let correlation_id =
                i64::from_le_bytes(payload[4..12].try_into().expect("eight bytes"));
            let delay_millis = i32::from_le_bytes(payload[12..16].try_into().expect("four bytes"));

            match command {
                TIMER_SCHEDULE => {
                    let deadline = cluster.time() + i64::from(delay_millis);
                    if cluster.schedule_timer(correlation_id, deadline)? {
                        self.pending.push((correlation_id, session));
                    }
                }
                TIMER_CANCEL => {
                    cluster.cancel_timer(correlation_id)?;
                }
                _ => {}
            }

            return Ok(());
        }

        if let Some(session) = session {
            Self::echo_to_session(cluster, session, payload)?;
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
                Self::echo_to_session(cluster, session, &payload)?;
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
        Ok(())
    }
}
