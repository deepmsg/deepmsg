//! `PendingServiceMessageTracker` (`PendingServiceMessageTracker.java`).
//!
//! Messages a service emits are not appended to the log by the service: they
//! are handed to the consensus module on the control channel, **renumbered**
//! here, queued in a ring buffer, and appended by the leader at its own pace
//! (`poll`, at most [`SERVICE_MESSAGE_LIMIT`] per pass). The leader then sweeps
//! the appended ones as the commit position passes them; a follower sweeps them
//! as the replayed log reaches them.
//!
//! # The two fields that are really one
//!
//! The `timestamp` field of a queued `SessionMessageHeader` is **not** a
//! timestamp: the tracker overwrites it with the log position the message was
//! appended at, or [`NOT_APPENDED_YET`] before it has one. `verify` and the two
//! sweepers read it back. That is a byte-level fact about the queue, so it is
//! written down here and asserted directly in the tests.
//!
//! # Session ids
//!
//! A service's log messages carry a cluster session id whose **top seven bits
//! are the service id** (`serviceIdFromLogMessage`); a message *from* a service
//! uses the service id itself as the cluster session id
//! (`serviceIdFromServiceMessage`). Both packings are reproduced here.

use crate::error::ClusterError;
use deepmsg_cluster_common::collections::ExpandableRingBuffer;

/// `PendingServiceMessageTracker.SERVICE_MESSAGE_LIMIT`: appends per poll.
pub const SERVICE_MESSAGE_LIMIT: usize = 20;

/// `AeronCluster.SESSION_HEADER_LENGTH`.
pub const SESSION_HEADER_LENGTH: usize = 32;

/// Where the cluster session id lives in a queued message.
const CLUSTER_SESSION_ID_OFFSET: usize = 16;

/// Where the append position is stashed (the header's timestamp field).
const APPEND_POSITION_OFFSET: usize = 24;

/// `Long.MAX_VALUE`: "not appended yet".
pub const NOT_APPENDED_YET: i64 = i64::MAX;

/// `Aeron.NULL_VALUE`, used by the reference's `leadershipTermId` default.
pub const NULL_VALUE: i64 = -1;

/// The one thing the tracker needs from the log publisher, so the tracker's
/// tests can mock the append (`PendingServiceMessageTrackerTest` mocks
/// `LogPublisher` with Mockito).
pub trait LogAppender {
    /// `LogPublisher.appendMessage`, with a caller-supplied window limit.
    fn append_message(
        &mut self,
        position_limit: i64,
        leadership_term_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        payload: &[u8],
    ) -> Result<i64, ClusterError>;
}

impl LogAppender for crate::log_publisher::LogPublisher {
    fn append_message(
        &mut self,
        position_limit: i64,
        leadership_term_id: i64,
        cluster_session_id: i64,
        timestamp: i64,
        payload: &[u8],
    ) -> Result<i64, ClusterError> {
        crate::log_publisher::LogPublisher::append_message(
            self,
            position_limit,
            leadership_term_id,
            cluster_session_id,
            timestamp,
            payload,
        )
    }
}

/// One service's pending messages (`PendingServiceMessageTracker.java:33-205`).
pub struct PendingServiceMessageTracker {
    service_id: i32,
    pending_message_head_offset: usize,
    uncommitted_messages: usize,
    next_service_session_id: i64,
    log_service_session_id: i64,
    leadership_term_id: i64,
    pending_messages: ExpandableRingBuffer,
}

impl PendingServiceMessageTracker {
    /// `new PendingServiceMessageTracker(serviceId, commitPosition,
    /// logPublisher, clusterClock)` (`:52-65`), minus the collaborators this
    /// build passes per call.
    pub fn new(service_id: i32) -> Self {
        let log_service_session_id = service_session_id(service_id, i64::MIN);
        Self {
            service_id,
            pending_message_head_offset: 0,
            uncommitted_messages: 0,
            next_service_session_id: log_service_session_id + 1,
            log_service_session_id,
            leadership_term_id: NULL_VALUE,
            pending_messages: ExpandableRingBuffer::new(),
        }
    }

    /// `serviceId()`.
    pub const fn service_id(&self) -> i32 {
        self.service_id
    }

    /// `leadershipTermId(long)`.
    pub fn set_leadership_term_id(&mut self, leadership_term_id: i64) {
        self.leadership_term_id = leadership_term_id;
    }

    /// `nextServiceSessionId()`.
    pub const fn next_service_session_id(&self) -> i64 {
        self.next_service_session_id
    }

    /// `logServiceSessionId()`.
    pub const fn log_service_session_id(&self) -> i64 {
        self.log_service_session_id
    }

    /// `size()`: the ring's used bytes, which is what the snapshot writes as
    /// `pendingMessageCapacity` (`ConsensusModuleSnapshotTaker.java:150`).
    pub fn size(&self) -> usize {
        self.pending_messages.size()
    }

    /// `enqueueMessage(buffer, offset, length)` (`:87-105`).
    ///
    /// `message` is the **whole** queued message: the SBE header, the
    /// `SessionMessageHeader` and the payload, as it arrived on the control
    /// channel. Its session id is replaced with the next service session id and
    /// its timestamp with [`NOT_APPENDED_YET`].
    ///
    /// # Errors
    ///
    /// [`ClusterError::Cluster`] with the reference's `pending service message
    /// buffer at capacity=` words when the ring is full.
    pub fn enqueue_message(&mut self, message: &[u8]) -> Result<(), ClusterError> {
        let cluster_session_id = self.next_service_session_id;
        self.next_service_session_id += 1;

        if cluster_session_id <= self.log_service_session_id {
            return Ok(());
        }

        let mut stored = message.to_vec();
        if stored.len() < SESSION_HEADER_LENGTH {
            return Err(ClusterError::Cluster(
                "a service message is shorter than its SessionMessageHeader".to_owned(),
            ));
        }
        stored[CLUSTER_SESSION_ID_OFFSET..CLUSTER_SESSION_ID_OFFSET + 8]
            .copy_from_slice(&cluster_session_id.to_le_bytes());
        stored[APPEND_POSITION_OFFSET..APPEND_POSITION_OFFSET + 8]
            .copy_from_slice(&NOT_APPENDED_YET.to_le_bytes());

        if !self.pending_messages.append(&stored) {
            return Err(ClusterError::Cluster(format!(
                "pending service message buffer at capacity={} for serviceId={}",
                self.pending_messages.size(),
                self.service_id
            )));
        }
        Ok(())
    }

    /// `appendMessage(buffer, offset, length)` (`:133-136`): the raw load path,
    /// with no header rewriting.
    pub fn append_message(&mut self, message: &[u8]) -> bool {
        self.pending_messages.append(message)
    }

    /// `loadState(nextServiceSessionId, logServiceSessionId,
    /// pendingMessageCapacity)` (`:138-143`).
    pub fn load_state(
        &mut self,
        next_service_session_id: i64,
        log_service_session_id: i64,
        pending_message_capacity: usize,
    ) {
        self.next_service_session_id = next_service_session_id;
        self.log_service_session_id = log_service_session_id;
        self.pending_messages.reset(pending_message_capacity);
    }

    /// `poll()` (`:145-148`): append up to [`SERVICE_MESSAGE_LIMIT`] queued
    /// messages the log has not taken yet.
    ///
    /// # Errors
    ///
    /// [`ClusterError`] from the log append.
    pub fn poll(
        &mut self,
        appender: &mut dyn LogAppender,
        position_limit: i64,
        timestamp: i64,
    ) -> Result<usize, ClusterError> {
        let leadership_term_id = self.leadership_term_id;
        let mut uncommitted_messages = self.uncommitted_messages;
        let mut pending_message_head_offset = self.pending_message_head_offset;
        let mut error = None;

        let appended = self.pending_messages.for_each_mut(
            pending_message_head_offset,
            &mut |message: &mut [u8], head_offset: usize| {
                let cluster_session_id = read_i64(message, CLUSTER_SESSION_ID_OFFSET);
                match appender.append_message(
                    position_limit,
                    leadership_term_id,
                    cluster_session_id,
                    timestamp,
                    &message[SESSION_HEADER_LENGTH..],
                ) {
                    Ok(position) if position > 0 => {
                        uncommitted_messages += 1;
                        pending_message_head_offset = head_offset;
                        write_i64(message, APPEND_POSITION_OFFSET, position);
                        true
                    }
                    Ok(_) => false,
                    Err(caught) => {
                        error = Some(caught);
                        false
                    }
                }
            },
            SERVICE_MESSAGE_LIMIT,
        );

        self.uncommitted_messages = uncommitted_messages;
        self.pending_message_head_offset = pending_message_head_offset;

        match error {
            Some(error) => Err(error),
            None => Ok(appended),
        }
    }

    /// `sweepLeaderMessages()` (`:113-120`): drop what the commit position
    /// covers.
    ///
    /// The reference reads the commit-position counter inside the tracker; the
    /// position is passed here, which is the same number the caller just wrote
    /// (`ConsensusModuleAgent.sweepUncommittedEntriesTo`).
    pub fn sweep_leader_messages(&mut self, commit_position: i64) -> usize {
        if self.uncommitted_messages == 0 {
            return 0;
        }

        let mut log_service_session_id = self.log_service_session_id;
        let mut uncommitted_messages = self.uncommitted_messages;

        let consumed = self.pending_messages.consume(
            &mut |message: &[u8], _head_offset: usize| {
                let append_position = read_i64(message, APPEND_POSITION_OFFSET);
                if commit_position >= append_position {
                    log_service_session_id = read_i64(message, CLUSTER_SESSION_ID_OFFSET);
                    uncommitted_messages -= 1;
                    true
                } else {
                    false
                }
            },
            usize::MAX,
        );

        self.log_service_session_id = log_service_session_id;
        self.uncommitted_messages = uncommitted_messages;
        self.pending_message_head_offset =
            self.pending_message_head_offset.saturating_sub(consumed);
        consumed
    }

    /// `sweepFollowerMessages(clusterSessionId)` (`:107-111`): a follower takes
    /// everything the replayed log has reached.
    pub fn sweep_follower_messages(&mut self, cluster_session_id: i64) {
        self.log_service_session_id = cluster_session_id;
        let log_service_session_id = self.log_service_session_id;
        let consumed = self.pending_messages.consume(
            &mut |message: &[u8], _head_offset: usize| {
                read_i64(message, CLUSTER_SESSION_ID_OFFSET) <= log_service_session_id
            },
            usize::MAX,
        );
        self.pending_message_head_offset =
            self.pending_message_head_offset.saturating_sub(consumed);
    }

    /// `restoreUncommittedMessages()` (`:122-131`): everything that was
    /// appended but not committed goes back to "not appended".
    pub fn restore_uncommitted_messages(&mut self, commit_position: i64) {
        if self.uncommitted_messages == 0 {
            return;
        }

        // The committed prefix goes for good, as the reference's leader sweeper
        // does; what is left is un-appended again.
        self.sweep_leader_messages(commit_position);
        self.pending_messages.for_each_mut(
            0,
            &mut |message: &mut [u8], _: usize| {
                write_i64(message, APPEND_POSITION_OFFSET, NOT_APPENDED_YET);
                true
            },
            usize::MAX,
        );
        self.uncommitted_messages = 0;
        self.pending_message_head_offset = 0;
    }

    /// `verify()` (`:155-195`): the snapshot's messages must be the
    /// consecutive session ids after `logServiceSessionId`.
    ///
    /// # Errors
    ///
    /// [`ClusterError::Cluster`] with the reference's `snapshot has incorrect
    /// pending message` words.
    pub fn verify(&self) -> Result<(), ClusterError> {
        let mut count = 0i64;
        let mut error = None;

        self.pending_messages.for_each(
            0,
            &mut |message: &[u8], _head_offset: usize| {
                // `messageCount` is one-based in the reference's check.
                count += 1;
                let cluster_session_id = read_i64(message, CLUSTER_SESSION_ID_OFFSET);
                if cluster_session_id != self.log_service_session_id + count {
                    error = Some(ClusterError::Cluster(format!(
                        "snapshot has incorrect pending message: \
                         expected clusterSessionId={} but was {}",
                        self.log_service_session_id + count,
                        cluster_session_id
                    )));
                    false
                } else {
                    true
                }
            },
            usize::MAX,
        );

        if let Some(error) = error {
            return Err(error);
        }

        if self.next_service_session_id != self.log_service_session_id + count + 1 {
            return Err(ClusterError::Cluster(format!(
                "snapshot has incorrect pending message state: \
                 nextServiceSessionId={} logServiceSessionId={} pendingMessageCount={count}",
                self.next_service_session_id, self.log_service_session_id
            )));
        }
        Ok(())
    }

    /// `reset()` (`:197-200`): clear the tracked positions of everything in the
    /// ring.
    pub fn reset(&mut self) {
        self.pending_messages.for_each_mut(
            0,
            &mut |message: &mut [u8], _: usize| {
                write_i64(message, APPEND_POSITION_OFFSET, NOT_APPENDED_YET);
                true
            },
            usize::MAX,
        );
    }

    /// `pendingMessages().forEach(...)`: the snapshot's view of every queued
    /// message, from the head.
    pub fn for_each_pending<F: FnMut(&[u8], usize) -> bool>(
        &self,
        consumer: &mut F,
        limit: usize,
    ) -> usize {
        self.pending_messages.for_each(0, consumer, limit)
    }
}

/// `serviceIdFromLogMessage(clusterSessionId)` (`:279-282`): the top 7 bits.
pub const fn service_id_from_log_message(cluster_session_id: i64) -> i32 {
    (((cluster_session_id >> 56) & 0x7F) as i32) & 0x7F
}

/// `serviceIdFromServiceMessage(clusterSessionId)` (`:291-294`): the low 32
/// bits.
pub const fn service_id_from_service_message(cluster_session_id: i64) -> i32 {
    cluster_session_id as i32
}

/// `serviceSessionId(serviceId, sessionId)` (`:296-299`).
pub const fn service_session_id(service_id: i32, session_id: i64) -> i64 {
    ((service_id as i64) << 56) | session_id
}

fn read_i64(message: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(
        message[offset..offset + 8]
            .try_into()
            .expect("eight bytes inside a queued message"),
    )
}

fn write_i64(message: &mut [u8], offset: usize, value: i64) {
    message[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_service_id_packings_are_the_references() {
        // Service 3, session 0x1234: top seven bits are the service id.
        let log_message = service_session_id(3, 0x1234);
        assert_eq!(3, service_id_from_log_message(log_message));
        // A message *from* a service carries the service id itself.
        assert_eq!(3, service_id_from_service_message(3));
    }

    #[test]
    fn a_queued_message_gets_the_next_session_id_and_no_append_position() {
        let mut tracker = PendingServiceMessageTracker::new(0);
        let mut message = vec![0_u8; SESSION_HEADER_LENGTH + 4];
        message[CLUSTER_SESSION_ID_OFFSET..CLUSTER_SESSION_ID_OFFSET + 8]
            .copy_from_slice(&99i64.to_le_bytes());
        tracker.enqueue_message(&message).expect("it fits");

        let mut stored = Vec::new();
        tracker.for_each_pending(
            &mut |message: &[u8], _| {
                stored.extend_from_slice(message);
                true
            },
            10,
        );
        assert_eq!(
            tracker.log_service_session_id() + 1,
            read_i64(&stored, CLUSTER_SESSION_ID_OFFSET)
        );
        assert_eq!(NOT_APPENDED_YET, read_i64(&stored, APPEND_POSITION_OFFSET));
    }
}
