//! Writing a consensus-module snapshot (`ConsensusModuleSnapshotTaker.java`).
//!
//! The record order is the contract (`ConsensusModuleAgent.java:3221-3253`):
//!
//! 1. `SnapshotMarker` BEGIN (100, mark 0)
//! 2. `ConsensusModule` (105) — **one** record
//! 3. `ClusterSession` (103) — one per committed session, in the order
//!    [`SessionManager::snapshot_sessions`](crate::session::manager::SessionManager::snapshot_sessions)
//!    yields them
//! 4. `Timer` (104) — one per scheduled timer, in the timer service's own
//!    snapshot order
//! 5. `PendingMessageTracker` (107) followed by each queued message verbatim,
//!    per service, ascending service id
//! 6. `SnapshotMarker` END (100, mark 2)
//!
//! # The iteration containers, one by one (plan §7.4, Q8)
//!
//! * **Consensus module state**: not an iteration — the reference reads
//!   `sessionManager.nextCommittedSessionId()` and **tracker zero's**
//!   `nextServiceSessionId` / `logServiceSessionId` / `size`, or zeroes when
//!   there are no services (`:3225-3235`).
//! * **Sessions**: `SessionManager.sessions` is an `ArrayList` kept ascending by
//!   id (`addSession`, `:1134-1157`), and only `OPEN` and `CLOSING` are
//!   written (`:1107-1119`). A session's `timeOfLastActivity` is written as
//!   `Aeron.NULL_VALUE` — the live value is deliberately not serialised — and
//!   `closedLogPosition` / `sessionInfo` are not serialised at all.
//! * **Timers**: implementation-dependent, and that is the reason
//!   `aeron.cluster.timer.service.supplier` is a byte contract: the wheel walks
//!   its `Long2LongHashMap` in entry-iterator order, the heap walks its array
//!   (`WheelTimerService.java:118-131`, `PriorityHeapTimerService.java:179-187`).
//! * **Trackers**: the array is indexed by service id, so ascending service id
//!   (`ConsensusModuleAgent.java:270-275, :3237-3250`).
//! * **Pending messages**: `ExpandableRingBuffer.forEach` from the head — FIFO,
//!   oldest first, which is ascending cluster session id (`:159`).
//! * **`ClusterMembers` (106)**: never written by the reference; its decoder
//!   ignores it. Left the same here.

use deepmsg_codec_cluster::WriteBuf;
use deepmsg_codec_cluster::close_reason::CloseReason;
use deepmsg_codec_cluster::cluster_session_codec::{
    ClusterSessionEncoder, SBE_BLOCK_LENGTH as CLUSTER_SESSION_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;
use deepmsg_codec_cluster::consensus_module_codec::{
    ConsensusModuleEncoder, SBE_BLOCK_LENGTH as CONSENSUS_MODULE_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::message_header_codec::ENCODED_LENGTH as MESSAGE_HEADER_LENGTH;
use deepmsg_codec_cluster::pending_message_tracker_codec::{
    PendingMessageTrackerEncoder, SBE_BLOCK_LENGTH as PENDING_MESSAGE_TRACKER_BLOCK_LENGTH,
};
use deepmsg_codec_cluster::snapshot_mark::SnapshotMark;
use deepmsg_codec_cluster::snapshot_marker_codec::{
    SBE_BLOCK_LENGTH as SNAPSHOT_MARKER_BLOCK_LENGTH, SnapshotMarkerEncoder,
};
use deepmsg_codec_cluster::timer_codec::{SBE_BLOCK_LENGTH as TIMER_BLOCK_LENGTH, TimerEncoder};

use crate::error::ClusterError;
use crate::service_control::tracker::PendingServiceMessageTracker;
use crate::session::ClusterSession;
use crate::timer::TimerSnapshotTaker;

/// `ConsensusModule.Configuration.SNAPSHOT_TYPE_ID` (`ConsensusModule.java:421`).
pub const SNAPSHOT_TYPE_ID: i64 = 1;

/// `Aeron.NULL_VALUE`, the `timeOfLastActivity` a snapshot always carries.
pub const NULL_VALUE: i64 = -1;

/// The answer a snapshot write gets (`Publication.tryClaim` / `offer`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotAppend {
    /// Written.
    Ok,
    /// `BACK_PRESSURED` or another retryable answer: idle and try again.
    Retry,
    /// A fatal answer (closed, not connected, at its maximum position): the
    /// reference throws from `SnapshotTaker.checkResultAndIdle`.
    Fatal(String),
}

/// What the taker needs from the publication, so
/// `ConsensusModuleSnapshotTakerTest` can mock it the way the reference mocks
/// `ExclusivePublication`.
pub trait SnapshotSend {
    /// Write a claimed record of `length` bytes.
    fn try_claim(&self, length: usize, bytes: &[u8]) -> SnapshotAppend;

    /// Offer an encoded record (the oversized-session path).
    fn offer(&self, bytes: &[u8]) -> SnapshotAppend;

    /// `Publication.maxPayloadLength()`, which picks the path for a session.
    fn max_payload_length(&self) -> usize;
}

/// The idle strategy the taker uses between attempts (`SnapshotTaker`'s
/// `idleStrategy` + agent invoker): a retry that does not drive the client
/// would never clear back-pressure.
pub trait SnapshotIdle {
    /// `IdleStrategy.reset()`.
    fn reset(&mut self);

    /// `IdleStrategy.idle()` and the agent invoker's poll.
    fn idle(&mut self);
}

/// A publication and the idle strategy together: the taker needs both in the
/// same retry loop, and they both need the client, so they travel as one
/// borrow.
pub trait SnapshotSink: SnapshotSend + SnapshotIdle {}

impl<T: SnapshotSend + SnapshotIdle> SnapshotSink for T {}

/// `ConsensusModuleSnapshotTaker` (`ConsensusModuleSnapshotTaker.java:31-193`).
pub struct ConsensusModuleSnapshotTaker<'a> {
    sink: &'a mut dyn SnapshotSink,
    buffer: Vec<u8>,
    time_unit: ClusterTimeUnit,
    app_version: i32,
    /// A record the publication refused. The timer hook cannot return an error,
    /// so the failure is kept here for the caller to check after
    /// `timer.snapshot(..)`.
    failed: bool,
}

impl<'a> ConsensusModuleSnapshotTaker<'a> {
    /// A taker over a publication, for one snapshot.
    pub fn new(
        sink: &'a mut dyn SnapshotSink,
        time_unit: ClusterTimeUnit,
        app_version: i32,
    ) -> Self {
        Self {
            sink,
            buffer: vec![0_u8; 1024],
            time_unit,
            app_version,
            failed: false,
        }
    }

    /// Whether any record was refused. Check after the timer snapshot too.
    pub const fn failed(&self) -> bool {
        self.failed
    }

    /// `SnapshotTaker.markBegin` (`SnapshotTaker.java:108-136`).
    ///
    /// # Errors
    ///
    /// [`ClusterError::Cluster`] when the record is refused.
    pub fn mark_begin(
        &mut self,
        log_position: i64,
        leadership_term_id: i64,
    ) -> Result<(), ClusterError> {
        self.mark(SnapshotMark::BEGIN, log_position, leadership_term_id)
    }

    /// `SnapshotTaker.markEnd` (`SnapshotTaker.java:127-136`).
    ///
    /// # Errors
    ///
    /// As [`Self::mark_begin`].
    pub fn mark_end(
        &mut self,
        log_position: i64,
        leadership_term_id: i64,
    ) -> Result<(), ClusterError> {
        self.mark(SnapshotMark::END, log_position, leadership_term_id)
    }

    /// `snapshotConsensusModuleState` (`:56-83`).
    ///
    /// # Errors
    ///
    /// As [`Self::mark_begin`].
    pub fn snapshot_consensus_module_state(
        &mut self,
        next_session_id: i64,
        next_service_session_id: i64,
        log_service_session_id: i64,
        pending_message_capacity: i32,
    ) -> Result<(), ClusterError> {
        let length = MESSAGE_HEADER_LENGTH + usize::from(CONSENSUS_MODULE_BLOCK_LENGTH);
        self.size_buffer(length);

        {
            let encoder = ConsensusModuleEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .next_session_id(next_session_id)
                .next_service_session_id(next_service_session_id)
                .log_service_session_id(log_service_session_id)
                .pending_message_capacity(pending_message_capacity);
        }

        self.claim(length)
    }

    /// `snapshotSession` (`:85-113`): claim when it fits the maximum payload,
    /// encode into the reusable buffer and offer when it does not.
    ///
    /// # Errors
    ///
    /// As [`Self::mark_begin`].
    pub fn snapshot_session(&mut self, session: &ClusterSession) -> Result<(), ClusterError> {
        let length = MESSAGE_HEADER_LENGTH
            + usize::from(CLUSTER_SESSION_BLOCK_LENGTH)
            + 4
            + session.response_channel().len();
        self.size_buffer(length);

        {
            let encoder = ClusterSessionEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .cluster_session_id(session.id())
                .correlation_id(session.correlation_id())
                .opened_log_position(session.opened_log_position())
                // The reference writes `Aeron.NULL_VALUE` here, never the live
                // activity stamp (`ConsensusModuleSnapshotTaker.java:168`).
                .time_of_last_activity(NULL_VALUE)
                .close_reason(close_reason(session))
                .response_stream_id(session.response_stream_id())
                .response_channel(session.response_channel().as_bytes());
        }

        if length <= self.sink.max_payload_length() {
            self.claim(length)
        } else {
            self.offer(length)
        }
    }

    /// `snapshot(tracker, errorHandler)` (`:135-160`): the tracker record, then
    /// every queued message verbatim.
    ///
    /// Answers the reference's error line for a follower that missed messages
    /// (`nextServiceSessionId` had to be corrected upwards), which the caller
    /// records; `None` when nothing was wrong.
    ///
    /// # Errors
    ///
    /// As [`Self::mark_begin`].
    pub fn snapshot_tracker(
        &mut self,
        tracker: &PendingServiceMessageTracker,
    ) -> Result<Option<String>, ClusterError> {
        // `correctNextServiceSessionId` (`:176-193`).
        let mut pending_count = 0;
        tracker.for_each_pending(
            &mut |_: &[u8], _: usize| {
                pending_count += 1;
                true
            },
            usize::MAX,
        );
        let next_service_session_id = if pending_count == 0 {
            tracker.log_service_session_id() + 1
        } else {
            tracker.next_service_session_id()
        };
        let correction = (next_service_session_id > tracker.next_service_session_id()).then(|| {
            format!(
                "service messages were missed by the follower: serviceId={} \
                 nextServiceSessionId={} corrected={}",
                tracker.service_id(),
                tracker.next_service_session_id(),
                next_service_session_id
            )
        });

        #[allow(clippy::cast_possible_truncation)] // a pending count, far below i32::MAX
        let pending_message_capacity = tracker.size() as i32;
        let length = MESSAGE_HEADER_LENGTH + usize::from(PENDING_MESSAGE_TRACKER_BLOCK_LENGTH);
        self.size_buffer(length);

        {
            let encoder = PendingMessageTrackerEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .next_service_session_id(next_service_session_id)
                .log_service_session_id(tracker.log_service_session_id())
                .pending_message_capacity(pending_message_capacity)
                .service_id(tracker.service_id());
        }

        self.claim(length)?;

        let mut error = None;
        tracker.for_each_pending(
            &mut |message: &[u8], _head_offset: usize| {
                self.sink.reset();
                loop {
                    match self.sink.offer(message) {
                        SnapshotAppend::Ok => return true,
                        SnapshotAppend::Retry => self.sink.idle(),
                        SnapshotAppend::Fatal(reason) => {
                            error = Some(ClusterError::Cluster(format!(
                                "failed to offer a pending service message into the snapshot: {reason}"
                            )));
                            return false;
                        }
                    }
                }
            },
            usize::MAX,
        );

        match error {
            Some(error) => Err(error),
            None => Ok(correction),
        }
    }

    fn mark(
        &mut self,
        mark: SnapshotMark,
        log_position: i64,
        leadership_term_id: i64,
    ) -> Result<(), ClusterError> {
        let length = MESSAGE_HEADER_LENGTH + usize::from(SNAPSHOT_MARKER_BLOCK_LENGTH);
        self.size_buffer(length);

        {
            let encoder = SnapshotMarkerEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message
                .type_id(SNAPSHOT_TYPE_ID)
                .log_position(log_position)
                .leadership_term_id(leadership_term_id)
                .index(0)
                .mark(mark)
                .time_unit(self.time_unit)
                .app_version(self.app_version);
        }

        self.claim(length)
    }

    fn claim(&mut self, length: usize) -> Result<(), ClusterError> {
        self.sink.reset();
        loop {
            match self.sink.try_claim(length, &self.buffer[..length]) {
                SnapshotAppend::Ok => return Ok(()),
                SnapshotAppend::Retry => self.sink.idle(),
                SnapshotAppend::Fatal(message) => {
                    self.failed = true;
                    return Err(ClusterError::Cluster(format!(
                        "failed to claim a snapshot record: {message}"
                    )));
                }
            }
        }
    }

    fn offer(&mut self, length: usize) -> Result<(), ClusterError> {
        self.sink.reset();
        loop {
            match self.sink.offer(&self.buffer[..length]) {
                SnapshotAppend::Ok => return Ok(()),
                SnapshotAppend::Retry => self.sink.idle(),
                SnapshotAppend::Fatal(message) => {
                    self.failed = true;
                    return Err(ClusterError::Cluster(format!(
                        "failed to offer a session snapshot record: {message}"
                    )));
                }
            }
        }
    }

    fn size_buffer(&mut self, length: usize) {
        if self.buffer.len() < length {
            self.buffer.resize(length, 0);
        }
    }
}

impl TimerSnapshotTaker for ConsensusModuleSnapshotTaker<'_> {
    /// `snapshotTimer` (`:115-133`).
    fn snapshot_timer(&mut self, correlation_id: i64, deadline: i64) {
        let length = MESSAGE_HEADER_LENGTH + usize::from(TIMER_BLOCK_LENGTH);
        self.size_buffer(length);

        {
            let encoder = TimerEncoder::default().wrap(
                WriteBuf::new(&mut self.buffer[..length]),
                MESSAGE_HEADER_LENGTH,
            );
            let mut header = encoder.header(0);
            let mut message = header.parent().expect("the encoder just wrapped");
            message.correlation_id(correlation_id).deadline(deadline);
        }

        // The trait cannot return an error; `failed()` reports it after the
        // fact instead of dropping the record silently.
        let _ = self.claim(length);
    }
}

/// A session's close reason, with `NULL_VAL` for an open one.
fn close_reason(session: &ClusterSession) -> CloseReason {
    session.close_reason()
}
