//! `SnapshotReplication` (`SnapshotReplication.java`), with the two classes it
//! is built from: `MultipleRecordingReplication.java` and
//! `RecordingReplication.java`.
//!
//! A backup retrieves a cluster's snapshots by asking **its own** archive to
//! replicate each snapshot recording from the **cluster's** archive. That is
//! one bidirectional protocol run once per snapshot, and the reference splits it
//! over three classes:
//!
//! * [`SnapshotReplication`] — the public face: a list of snapshots to fetch,
//!   `add_snapshot`, `poll`, `on_signal`, `is_complete`, `snapshots_retrieved`.
//! * [`MultipleRecordingReplication`] — one replication at a time, in list
//!   order, remembering each completed source's destination recording id.
//! * [`RecordingReplication`] — one replication: the request, the destination's
//!   progress counter, and the four `RecordingSignal`s that end it.
//!
//! # Why the signals rather than the control responses
//!
//! A replication has no single "done" answer. The source archive publishes
//! `RecordingSignalEvent`s on the destination's control response channel as the
//! copy progresses — `EXTEND` when the destination recording starts, `SYNC`
//! when the two ends have met, `REPLICATE_END` when the copy is finished — and
//! the destination archive *also* answers the `Replicate` request immediately.
//! So the completion condition is the signal stream, not the response, and that
//! is why [`SnapshotReplication::on_signal`] exists and is called from the
//! backup's slow tick rather than from `poll`.
//!
//! # Progress stall
//!
//! `RecordingReplication.poll` is also the watchdog: every
//! `progress_interval_ns` it reads the destination's recording position, and if
//! the position has not moved within `progress_timeout_ns` the replication is
//! failed **at the deadline** rather than left to hang. The reference's two
//! messages are reproduced (`RecordingReplication.java:96-107`) because they say
//! different things: one is a stall, the other a copy that reached its stop
//! position and did not stop.
//!
//! # The seam
//!
//! The reference's test for this class is a Mockito mock of `AeronArchive`
//! (`SnapshotReplicationTest.java:35`), so the calls this class makes on its
//! archive are behind [`ReplicationArchive`] rather than taken from a concrete
//! [`deepmsg_archive::client::archive::Archive`]. The production side is
//! [`ArchiveClient`], which is a client and an archive borrowed together; the
//! test side is [`crate::backup::agent`]'s recording double in this crate's
//! tests. It is the same split `deepmsg-cluster-client` makes with `ClientOps`.

use deepmsg_cluster_common::collections::HashMap;

use deepmsg_archive::client::proxy::ReplicationParams;
use deepmsg_codec::archive::recording_signal::RecordingSignal;

use super::{NULL_POSITION, NULL_VALUE};
use crate::error::ClusterError;
use crate::persist::recording_log::Snapshot;

/// The counter id no counter has (`CountersReader.NULL_COUNTER_ID`).
pub const NULL_COUNTER_ID: i32 = -1;

/// The archive operations a snapshot replication performs.
///
/// Four of these are the reference's `AeronArchive` calls
/// (`replicate`, `tryStopReplication`, `nextCorrelationId`, `archiveId`) and two
/// are its `Aeron.countersReader()` reads (`RecordingPos.findCounterIdByRecording`
/// and `counters.getCounterValue`). The trait exists so that
/// `SnapshotReplicationTest`'s three cases can be expressed without a media
/// driver, as they are in the reference.
pub trait ReplicationArchive {
    /// `AeronArchive.nextCorrelationId()` — the replication's session id is a
    /// truncation of it (`MultipleRecordingReplication.java:203`).
    ///
    /// # Errors
    ///
    /// Whatever the client refused.
    fn next_correlation_id(&mut self) -> Result<i64, ClusterError>;

    /// `AeronArchive.archiveId()`.
    fn archive_id(&self) -> i64;

    /// `AeronArchive.replicate` (`:1611-1638`): answers the replication id.
    ///
    /// # Errors
    ///
    /// Whatever the archive refused.
    fn replicate(
        &mut self,
        src_recording_id: i64,
        src_control_stream_id: i32,
        src_control_channel: &str,
        params: &ReplicationParams,
    ) -> Result<i64, ClusterError>;

    /// `AeronArchive.tryStopReplication` (`:1816-1847`): `true` when one was
    /// stopped, `false` when there was nothing to stop.
    ///
    /// # Errors
    ///
    /// Any refusal that is not `UNKNOWN_REPLICATION`.
    fn try_stop_replication(&mut self, replication_id: i64) -> Result<bool, ClusterError>;

    /// `RecordingPos.findCounterIdByRecording` (`RecordingPos.java:157-188`) for
    /// this archive.
    fn recording_position_counter(&self, recording_id: i64) -> Option<i32>;

    /// A counter's value if it is still the one for `recording_id`
    /// (`RecordingPos.isActive` plus `getCounterValue`,
    /// `RecordingReplication.java:223-238`).
    fn recording_position(&self, counter_id: i32, recording_id: i64) -> Option<i64>;
}

/// The production side of [`ReplicationArchive`]: a client and the archive this
/// replication runs against, borrowed together.
pub struct ArchiveClient<'a> {
    /// The media driver client — where counters are read.
    pub client: &'a mut deepmsg_client::client::Client,
    /// The destination archive.
    pub archive: &'a mut deepmsg_archive::client::archive::Archive,
}

impl ReplicationArchive for ArchiveClient<'_> {
    fn next_correlation_id(&mut self) -> Result<i64, ClusterError> {
        Ok(self.archive.next_correlation_id(self.client)?)
    }

    fn archive_id(&self) -> i64 {
        self.archive.archive_id()
    }

    fn replicate(
        &mut self,
        src_recording_id: i64,
        src_control_stream_id: i32,
        src_control_channel: &str,
        params: &ReplicationParams,
    ) -> Result<i64, ClusterError> {
        Ok(self.archive.replicate(
            self.client,
            src_recording_id,
            src_control_stream_id,
            src_control_channel,
            params,
        )?)
    }

    fn try_stop_replication(&mut self, replication_id: i64) -> Result<bool, ClusterError> {
        Ok(self
            .archive
            .try_stop_replication(self.client, replication_id)?)
    }

    fn recording_position_counter(&self, recording_id: i64) -> Option<i32> {
        let counters = self.client.counters_reader()?;
        deepmsg_archive::server::recording_pos::find_counter_id_by_recording(
            &counters,
            recording_id,
            self.archive.archive_id(),
        )
    }

    fn recording_position(&self, counter_id: i32, recording_id: i64) -> Option<i64> {
        let counters = self.client.counters_reader()?;
        let value = counters.value(counter_id)?;

        counter_is_for_recording(&counters, counter_id, recording_id).then_some(value)
    }
}

/// `RecordingPos.isActive(counters, counterId, recordingId)`
/// (`RecordingPos.java:292-298`) for a counter id this code only holds rather
/// than a claimed [`deepmsg_archive::server::recording_pos::RecordingPos`].
fn counter_is_for_recording<Access>(
    counters: &deepmsg_cnc::CountersReader<'_, Access>,
    counter_id: i32,
    recording_id: i64,
) -> bool {
    if counters.get(counter_id).is_none_or(|descriptor| {
        descriptor.type_id != deepmsg_archive::server::counters::ARCHIVE_RECORDING_POSITION_TYPE_ID
    }) {
        return false;
    }

    counters
        .key(counter_id)
        .and_then(|key| deepmsg_archive::server::recording_pos::parse_key(&key))
        .is_some_and(|identity| identity.recording_id == recording_id)
}

/// `SnapshotReplication` (`SnapshotReplication.java:26-143`).
pub struct SnapshotReplication {
    /// The snapshots asked for, in the order they were added.
    snapshots_pending: Vec<Snapshot>,
    /// The engine.
    multiple: MultipleRecordingReplication,
}

impl SnapshotReplication {
    /// The four-argument form, which is the one the backup uses
    /// (`SnapshotReplication.java:32-45`): 10 s of progress timeout and 1 s
    /// between checks.
    #[must_use]
    pub fn new(
        src_control_stream_id: i32,
        src_control_channel: String,
        replication_channel: String,
    ) -> Self {
        Self::with_timeouts(
            src_control_stream_id,
            src_control_channel,
            replication_channel,
            10_000_000_000,
            1_000_000_000,
        )
    }

    /// The six-argument form (`SnapshotReplication.java:47-62`).
    #[must_use]
    pub fn with_timeouts(
        src_control_stream_id: i32,
        src_control_channel: String,
        replication_channel: String,
        replication_progress_timeout_ns: i64,
        replication_progress_interval_ns: i64,
    ) -> Self {
        Self {
            snapshots_pending: Vec::with_capacity(4),
            multiple: MultipleRecordingReplication::new(
                src_control_stream_id,
                src_control_channel,
                replication_channel,
                None,
                replication_progress_timeout_ns,
                replication_progress_interval_ns,
            ),
        }
    }

    /// `addSnapshot` (`SnapshotReplication.java:64-68`): queue one, and start a
    /// replication for its recording id with no destination id (make a new
    /// recording) and no stop position (copy to the end).
    pub fn add_snapshot(&mut self, snapshot: Snapshot) {
        self.snapshots_pending.push(snapshot);
        self.multiple
            .add_recording(snapshot.recording_id, NULL_VALUE, NULL_VALUE);
    }

    /// `poll` (`SnapshotReplication.java:70-73`).
    ///
    /// # Errors
    ///
    /// A replication that failed, or one that has stopped progressing.
    pub fn poll(
        &mut self,
        archive: &mut impl ReplicationArchive,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        self.multiple.poll(archive, now_ns)
    }

    /// `onSignal` (`SnapshotReplication.java:75-78`).
    ///
    /// # Errors
    ///
    /// A `DELETE` signal: the recording was removed while being copied, which
    /// the reference turns into a `ClusterException` (`:204-207`).
    pub fn on_signal(
        &mut self,
        archive: &impl ReplicationArchive,
        correlation_id: i64,
        recording_id: i64,
        position: i64,
        signal: RecordingSignal,
    ) -> Result<(), ClusterError> {
        self.multiple
            .on_signal(archive, correlation_id, recording_id, position, signal)
    }

    /// `currentReplicationId` (`SnapshotReplication.java:80-83`).
    #[must_use]
    pub fn current_replication_id(&self) -> i64 {
        self.multiple.current_replication_id()
    }

    /// `isComplete` (`SnapshotReplication.java:85-88`).
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.multiple.is_complete()
    }

    /// `snapshotsRetrieved` (`SnapshotReplication.java:90-105`): each pending
    /// snapshot with its **destination** recording id substituted.
    #[must_use]
    pub fn snapshots_retrieved(&self) -> Vec<Snapshot> {
        self.snapshots_pending
            .iter()
            .map(|pending| {
                retrieved_snapshot(
                    *pending,
                    self.multiple
                        .completed_dst_recording_id(pending.recording_id),
                )
            })
            .collect()
    }

    /// `currentSnapshot` (`SnapshotReplication.java:107-124`): the pending
    /// snapshot whose source recording is the one being replicated.
    #[must_use]
    pub fn current_snapshot(&self) -> Option<Snapshot> {
        let current_src_recording_id = self.multiple.current_src_recording_id();
        if NULL_VALUE == current_src_recording_id {
            return None;
        }

        self.snapshots_pending
            .iter()
            .find(|snapshot| snapshot.recording_id == current_src_recording_id)
            .copied()
    }

    /// `retrievedSnapshot` (`SnapshotReplication.java:126-137`).
    #[must_use]
    pub fn retrieved_snapshot(pending: Snapshot, recording_id: i64) -> Snapshot {
        retrieved_snapshot(pending, recording_id)
    }

    /// `close` (`SnapshotReplication.java:139-142`).
    pub fn close(&mut self, archive: &mut impl ReplicationArchive) {
        self.multiple.close(archive);
    }
}

/// `SnapshotReplication.retrievedSnapshot` (`:126-137`).
fn retrieved_snapshot(pending: Snapshot, recording_id: i64) -> Snapshot {
    Snapshot {
        recording_id,
        leadership_term_id: pending.leadership_term_id,
        term_base_log_position: pending.term_base_log_position,
        log_position: pending.log_position,
        timestamp: pending.timestamp,
        service_id: pending.service_id,
    }
}

/// One recording to copy (`MultipleRecordingReplication.RecordingInfo`,
/// `MultipleRecordingReplication.java:238-252`).
#[derive(Clone, Copy, Debug)]
struct RecordingInfo {
    src_recording_id: i64,
    dst_recording_id: i64,
    stop_position: i64,
}

/// `MultipleRecordingReplication` (`MultipleRecordingReplication.java:32-262`):
/// one replication at a time, in the order the recordings were added.
struct MultipleRecordingReplication {
    src_control_stream_id: i32,
    src_control_channel: String,
    replication_channel: String,
    /// The response channel for the source archive's control replies, when the
    /// caller drives the replication over response channels. `None` is the
    /// shape the backup uses (`MultipleRecordingReplication.java:199-207`).
    src_response_channel: Option<String>,
    recordings_pending: Vec<RecordingInfo>,
    /// Source recording id → destination recording id, for the ones that synced.
    recordings_completed: HashMap<i64, i64>,
    progress_timeout_ns: i64,
    progress_interval_ns: i64,
    recording_cursor: usize,
    recording_replication: Option<RecordingReplication>,
}

impl MultipleRecordingReplication {
    /// `newInstance` (`MultipleRecordingReplication.java:74-88`).
    fn new(
        src_control_stream_id: i32,
        src_control_channel: String,
        replication_channel: String,
        src_response_channel: Option<String>,
        progress_timeout_ns: i64,
        progress_interval_ns: i64,
    ) -> Self {
        Self {
            src_control_stream_id,
            src_control_channel,
            replication_channel,
            src_response_channel,
            recordings_pending: Vec::new(),
            recordings_completed: HashMap::default(),
            progress_timeout_ns,
            progress_interval_ns,
            recording_cursor: 0,
            recording_replication: None,
        }
    }

    /// `addRecording` (`MultipleRecordingReplication.java:120-123`).
    fn add_recording(&mut self, src_recording_id: i64, dst_recording_id: i64, stop_position: i64) {
        self.recordings_pending.push(RecordingInfo {
            src_recording_id,
            dst_recording_id,
            stop_position,
        });
    }

    /// `poll` (`MultipleRecordingReplication.java:125-170`).
    fn poll(
        &mut self,
        archive: &mut impl ReplicationArchive,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        if self.is_complete() {
            return Ok(0);
        }

        let mut work_done = 0;

        if self.recording_replication.is_none() {
            self.replicate_current_snapshot(archive, now_ns)?;
            work_done += 1;
        } else if let Some(replication) = self.recording_replication.as_mut() {
            replication.poll(archive, now_ns)?;
            if replication.has_replication_ended() {
                let pending = self.recordings_pending[self.recording_cursor];
                let recording_id = replication.recording_id();
                let has_synced = replication.has_synced();

                // The reference's `EventListener.onReplicationEnded` (`:186-193`)
                // is where a caller that wants progress would hook in; the
                // backup sets no listener, so there is none here.
                if has_synced {
                    self.recordings_completed
                        .insert(pending.src_recording_id, recording_id);
                    self.recording_cursor += 1;

                    if let Some(mut replication) = self.recording_replication.take() {
                        replication.close(archive);
                    }
                } else if let Some(mut replication) = self.recording_replication.take() {
                    replication.close(archive);
                    self.replicate_current_snapshot(archive, now_ns)?;
                }

                work_done += 1;
            }
        }

        Ok(work_done)
    }

    /// `completedDstRecordingId` (`MultipleRecordingReplication.java:172-175`).
    fn completed_dst_recording_id(&self, src_recording_id: i64) -> i64 {
        self.recordings_completed
            .get(&src_recording_id)
            .copied()
            .unwrap_or(NULL_VALUE)
    }

    /// `onSignal` (`MultipleRecordingReplication.java:177-183`).
    fn on_signal(
        &mut self,
        archive: &impl ReplicationArchive,
        correlation_id: i64,
        recording_id: i64,
        position: i64,
        signal: RecordingSignal,
    ) -> Result<(), ClusterError> {
        if let Some(replication) = self.recording_replication.as_mut() {
            replication.on_signal(archive, correlation_id, recording_id, position, signal)?;
        }

        Ok(())
    }

    /// `currentReplicationId` (`MultipleRecordingReplication.java:185-188`).
    fn current_replication_id(&self) -> i64 {
        self.recording_replication
            .as_ref()
            .map_or(NULL_VALUE, RecordingReplication::replication_id)
    }

    /// `currentSrcRecordingId` (`MultipleRecordingReplication.java:190-200`).
    fn current_src_recording_id(&self) -> i64 {
        if self.recording_cursor < self.recordings_pending.len() {
            self.recordings_pending[self.recording_cursor].src_recording_id
        } else {
            NULL_VALUE
        }
    }

    /// `isComplete` (`MultipleRecordingReplication.java:202-205`).
    fn is_complete(&self) -> bool {
        self.recording_cursor >= self.recordings_pending.len()
    }

    /// `close` (`MultipleRecordingReplication.java:210-214`).
    fn close(&mut self, archive: &mut impl ReplicationArchive) {
        if let Some(mut replication) = self.recording_replication.take() {
            replication.close(archive);
        }
    }

    /// `replicateCurrentSnapshot` (`MultipleRecordingReplication.java:216-236`).
    fn replicate_current_snapshot(
        &mut self,
        archive: &mut impl ReplicationArchive,
        now_ns: i64,
    ) -> Result<(), ClusterError> {
        let recording_info = self.recordings_pending[self.recording_cursor];
        // `(int)archive.context().aeron().nextCorrelationId()`: the reference's
        // cast **truncates**, it does not saturate, so this is a wrapping cast
        // and not a `try_from`.
        #[allow(clippy::cast_possible_truncation)]
        let replication_session_id = archive.next_correlation_id()? as i32;

        let mut params = ReplicationParams::new();
        params.dst_recording_id = recording_info.dst_recording_id;
        params.stop_position = recording_info.stop_position;
        params
            .replication_channel
            .clone_from(&self.replication_channel);
        if let Some(channel) = &self.src_response_channel {
            params.src_response_channel.clone_from(channel);
        }
        params.replication_session_id = replication_session_id;

        self.recording_replication = Some(RecordingReplication::new(
            archive,
            recording_info.src_recording_id,
            &self.src_control_channel,
            self.src_control_stream_id,
            &params,
            self.progress_timeout_ns,
            self.progress_interval_ns,
            now_ns,
        )?);

        Ok(())
    }
}

/// `RecordingReplication` (`RecordingReplication.java:26-262`): one copy, its
/// destination's progress counter, and the signals that end it.
struct RecordingReplication {
    replication_id: i64,
    stop_position: i64,
    progress_check_timeout_ns: i64,
    progress_check_interval_ns: i64,
    recording_position_counter_id: i32,
    recording_id: i64,
    position: i64,
    progress_deadline_ns: i64,
    progress_check_deadline_ns: i64,
    last_recording_signal: RecordingSignal,
    has_replication_ended: bool,
    has_synced: bool,
    has_stopped: bool,
}

impl RecordingReplication {
    /// The constructor (`RecordingReplication.java:53-73`): it sends the
    /// `Replicate` request, which the destination archive answers at once.
    #[allow(clippy::too_many_arguments)] // one per field the reference's ctor takes
    fn new(
        archive: &mut impl ReplicationArchive,
        src_recording_id: i64,
        src_archive_channel: &str,
        src_control_stream_id: i32,
        params: &ReplicationParams,
        progress_check_timeout_ns: i64,
        progress_check_interval_ns: i64,
        now_ns: i64,
    ) -> Result<Self, ClusterError> {
        let replication_id = archive.replicate(
            src_recording_id,
            src_control_stream_id,
            src_archive_channel,
            params,
        )?;

        Ok(Self {
            replication_id,
            stop_position: params.stop_position,
            progress_check_timeout_ns,
            progress_check_interval_ns,
            recording_position_counter_id: NULL_COUNTER_ID,
            recording_id: NULL_VALUE,
            position: NULL_POSITION,
            progress_deadline_ns: now_ns + progress_check_timeout_ns,
            progress_check_deadline_ns: now_ns + progress_check_interval_ns,
            last_recording_signal: RecordingSignal::NullVal,
            has_replication_ended: false,
            has_synced: false,
            has_stopped: false,
        })
    }

    /// `poll` (`RecordingReplication.java:75-120`), including its watchdog.
    fn poll(
        &mut self,
        archive: &mut impl ReplicationArchive,
        now_ns: i64,
    ) -> Result<usize, ClusterError> {
        if self.has_replication_ended {
            return Ok(0);
        }

        let mut work_count = 0;

        if now_ns >= self.progress_check_deadline_ns {
            self.progress_check_deadline_ns = now_ns + self.progress_check_interval_ns;
            if self.poll_dst_recording_position(archive) {
                self.progress_deadline_ns = now_ns + self.progress_check_timeout_ns;
            }
            work_count += 1;
        }

        if now_ns >= self.progress_deadline_ns {
            let error = if NULL_POSITION == self.stop_position || self.position < self.stop_position
            {
                // `RecordingReplication.java:100`: WARN, "has not progressed".
                ClusterError::Timeout("log replication has not progressed".to_owned())
            } else {
                ClusterError::Cluster("log replication failed to stop".to_owned())
            };

            self.close(archive);
            return Err(error);
        }

        Ok(work_count)
    }

    /// `replicationId` (`RecordingReplication.java:122-125`).
    fn replication_id(&self) -> i64 {
        self.replication_id
    }

    /// `recordingId` (`RecordingReplication.java:133-136`).
    fn recording_id(&self) -> i64 {
        self.recording_id
    }

    /// `hasReplicationEnded` (`RecordingReplication.java:138-141`).
    fn has_replication_ended(&self) -> bool {
        self.has_replication_ended
    }

    /// `hasSynced` (`RecordingReplication.java:143-146`).
    fn has_synced(&self) -> bool {
        self.has_synced
    }

    /// `close` (`RecordingReplication.java:155-171`): stop the replication on
    /// the destination archive, which expects to find nothing when the
    /// replication already ended.
    fn close(&mut self, archive: &mut impl ReplicationArchive) {
        if !self.has_replication_ended {
            self.has_replication_ended = true;
            // A failure here is a WARN in the reference (`:164-169`) and does
            // not escape a close; the same choice is made here.
            let _ = archive.try_stop_replication(self.replication_id);
        }
    }

    /// `onSignal` (`RecordingReplication.java:173-217`).
    fn on_signal(
        &mut self,
        archive: &impl ReplicationArchive,
        correlation_id: i64,
        recording_id: i64,
        position: i64,
        signal: RecordingSignal,
    ) -> Result<(), ClusterError> {
        if correlation_id != self.replication_id {
            return Ok(());
        }

        match signal {
            RecordingSignal::EXTEND => {
                // `:180-185`: the destination recording now exists, so its
                // position counter can be found — by the destination archive's
                // id, which is what the counter's key carries.
                self.recording_position_counter_id = archive
                    .recording_position_counter(recording_id)
                    .unwrap_or(NULL_COUNTER_ID);
            }
            RecordingSignal::SYNC => self.has_synced = true,
            RecordingSignal::REPLICATE_END => self.has_replication_ended = true,
            RecordingSignal::STOP => {
                if NULL_POSITION != position {
                    self.position = position;
                }
                self.has_stopped = true;
            }
            RecordingSignal::DELETE => {
                return Err(ClusterError::Cluster(format!(
                    "recording was deleted during replication: replicationId={} recordingId={}",
                    self.replication_id, self.recording_id
                )));
            }
            _ => {}
        }

        self.last_recording_signal = signal;

        if NULL_VALUE != recording_id {
            self.recording_id = recording_id;
        }

        if NULL_POSITION != position {
            self.position = position;
        }

        Ok(())
    }

    /// `pollDstRecordingPosition` (`RecordingReplication.java:219-240`): the
    /// destination's recording position, if the counter is still the one for
    /// this recording and it has advanced.
    fn poll_dst_recording_position(&mut self, archive: &impl ReplicationArchive) -> bool {
        if NULL_COUNTER_ID == self.recording_position_counter_id {
            return false;
        }

        let Some(recording_position) =
            archive.recording_position(self.recording_position_counter_id, self.recording_id)
        else {
            return false;
        };

        if recording_position > self.position {
            self.position = recording_position;
            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    //! `SnapshotReplicationTest` (`SnapshotReplicationTest.java:25-194`), all
    //! three cases, against a recording [`ReplicationArchive`] in place of the
    //! reference's Mockito mock.

    use std::cell::RefCell;
    use std::collections::VecDeque;

    use deepmsg_archive::client::proxy::ReplicationParams;
    use deepmsg_codec::archive::recording_signal::RecordingSignal;

    use super::*;

    /// One call the replication made, so a case can assert the sequence the
    /// reference's `verify`/`verifyNoMoreInteractions` assert.
    #[derive(Clone, Debug, PartialEq)]
    enum Call {
        NextCorrelationId,
        Replicate {
            src_recording_id: i64,
            src_control_stream_id: i32,
            src_control_channel: String,
            replication_channel: String,
            dst_recording_id: i64,
            stop_position: i64,
            replication_session_id: i32,
        },
        TryStopReplication(i64),
        RecordingPositionCounter(i64),
        RecordingPosition(i32, i64),
    }

    /// The mock: it answers from a script and records what it was asked.
    #[derive(Default)]
    struct RecordingArchive {
        correlation_id: i64,
        replication_ids: VecDeque<i64>,
        positions: Vec<(i32, i64)>,
        calls: RefCell<Vec<Call>>,
    }

    impl RecordingArchive {
        fn with_replications(ids: &[i64]) -> Self {
            Self {
                correlation_id: 987_237_452_342,
                replication_ids: ids.iter().copied().collect(),
                positions: Vec::new(),
                calls: RefCell::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<Call> {
            self.calls.borrow().clone()
        }
    }

    impl ReplicationArchive for RecordingArchive {
        fn next_correlation_id(&mut self) -> Result<i64, ClusterError> {
            self.calls.borrow_mut().push(Call::NextCorrelationId);
            Ok(self.correlation_id)
        }

        fn archive_id(&self) -> i64 {
            0
        }

        fn replicate(
            &mut self,
            src_recording_id: i64,
            src_control_stream_id: i32,
            src_control_channel: &str,
            params: &ReplicationParams,
        ) -> Result<i64, ClusterError> {
            self.calls.borrow_mut().push(Call::Replicate {
                src_recording_id,
                src_control_stream_id,
                src_control_channel: src_control_channel.to_owned(),
                replication_channel: params.replication_channel.clone(),
                dst_recording_id: params.dst_recording_id,
                stop_position: params.stop_position,
                replication_session_id: params.replication_session_id,
            });

            self.replication_ids
                .pop_front()
                .ok_or_else(|| ClusterError::Cluster("no replication id scripted".to_owned()))
        }

        fn try_stop_replication(&mut self, replication_id: i64) -> Result<bool, ClusterError> {
            self.calls
                .borrow_mut()
                .push(Call::TryStopReplication(replication_id));
            Ok(true)
        }

        fn recording_position_counter(&self, recording_id: i64) -> Option<i32> {
            self.calls
                .borrow_mut()
                .push(Call::RecordingPositionCounter(recording_id));
            // The reference's mock returns a counter for any recording whose
            // position the case scripted; the tests here do not script one, so
            // the counter stays `NULL_COUNTER_ID` exactly as the reference's
            // `RecordingPos.findCounterIdByRecording` on a mock `CountersReader`
            // would.
            self.positions
                .iter()
                .find(|(_, position)| *position == recording_id)
                .map(|(counter_id, _)| *counter_id)
        }

        fn recording_position(&self, counter_id: i32, recording_id: i64) -> Option<i64> {
            self.calls
                .borrow_mut()
                .push(Call::RecordingPosition(counter_id, recording_id));
            None
        }
    }

    const NOW_NS: i64 = 1_000_000_000;
    const SRC_STREAM_ID: i32 = 892_374;
    const CORRELATION_ID: i64 = 987_237_452_342;

    fn snapshot(recording_id: i64, service_id: i32) -> Snapshot {
        Snapshot {
            recording_id,
            leadership_term_id: 3,
            term_base_log_position: 5,
            log_position: 7,
            timestamp: 11,
            service_id,
        }
    }

    fn replication_call(src_recording_id: i64) -> Call {
        Call::Replicate {
            src_recording_id,
            src_control_stream_id: SRC_STREAM_ID,
            src_control_channel: "aeron:udp?endpoint=coming_from:8888".to_owned(),
            replication_channel: "aeron:udp?endpoint=going_to:8888".to_owned(),
            dst_recording_id: NULL_VALUE,
            stop_position: NULL_VALUE,
            replication_session_id: CORRELATION_ID as i32,
        }
    }

    fn replication() -> SnapshotReplication {
        SnapshotReplication::new(
            SRC_STREAM_ID,
            "aeron:udp?endpoint=coming_from:8888".to_owned(),
            "aeron:udp?endpoint=going_to:8888".to_owned(),
        )
    }

    /// `shouldReplicateTwoSnapshots` (`SnapshotReplicationTest.java:55-133`).
    #[test]
    fn should_replicate_two_snapshots() {
        let mut archive = RecordingArchive::with_replications(&[89_374, 89_375]);
        let new_recording_id_0: i64 = 9_823_754_293;
        let new_recording_id_1: i64 = 7_635_445_643;

        let mut replication = replication();
        replication.add_snapshot(snapshot(2, 0));
        replication.add_snapshot(snapshot(17, -1));

        assert_eq!(Ok(1), replication.poll(&mut archive, NOW_NS));
        assert_eq!(
            vec![Call::NextCorrelationId, replication_call(2)],
            archive.calls()
        );
        assert_eq!(89_374, replication.current_replication_id());
        assert_eq!(
            Some(2),
            replication.current_snapshot().map(|s| s.recording_id)
        );

        // A poll with nothing new asks the archive nothing.
        assert_eq!(Ok(0), replication.poll(&mut archive, NOW_NS));
        assert_eq!(2, archive.calls().len());

        replication
            .on_signal(
                &archive,
                89_374,
                new_recording_id_0,
                23_423,
                RecordingSignal::SYNC,
            )
            .expect("a SYNC is accepted");
        assert_eq!(Ok(0), replication.poll(&mut archive, NOW_NS));
        assert_eq!(2, archive.calls().len());

        replication
            .on_signal(
                &archive,
                89_374,
                new_recording_id_0,
                23_423,
                RecordingSignal::REPLICATE_END,
            )
            .expect("a REPLICATE_END is accepted");
        assert_eq!(Ok(1), replication.poll(&mut archive, NOW_NS));
        assert_eq!(2, archive.calls().len());

        // The completed one is retired and the second replication starts; the
        // first is not stopped because its own REPLICATE_END said it was over.
        assert_eq!(Ok(1), replication.poll(&mut archive, NOW_NS));
        assert_eq!(
            vec![
                Call::NextCorrelationId,
                replication_call(2),
                Call::NextCorrelationId,
                replication_call(17)
            ],
            archive.calls()
        );
        assert_eq!(89_375, replication.current_replication_id());
        assert_eq!(
            Some(17),
            replication.current_snapshot().map(|s| s.recording_id)
        );

        replication
            .on_signal(
                &archive,
                89_375,
                new_recording_id_1,
                23_423,
                RecordingSignal::SYNC,
            )
            .expect("a SYNC is accepted");
        replication
            .on_signal(
                &archive,
                89_375,
                new_recording_id_1,
                23_423,
                RecordingSignal::REPLICATE_END,
            )
            .expect("a REPLICATE_END is accepted");
        assert_eq!(Ok(1), replication.poll(&mut archive, NOW_NS));

        assert!(replication.is_complete());
        let retrieved = replication.snapshots_retrieved();
        assert_eq!(0, retrieved[0].service_id);
        assert_eq!(new_recording_id_0, retrieved[0].recording_id);
        assert_eq!(-1, retrieved[1].service_id);
        assert_eq!(new_recording_id_1, retrieved[1].recording_id);

        assert_eq!(Ok(0), replication.poll(&mut archive, NOW_NS));
        assert_eq!(4, archive.calls().len());
    }

    /// `shouldNotBeCompleteIfNotSynced` (`SnapshotReplicationTest.java:135-167`):
    /// a `REPLICATE_END` without a `SYNC` retires that attempt and starts the
    /// same snapshot again; the cursor does not advance.
    #[test]
    fn should_not_be_complete_if_not_synced() {
        let mut archive = RecordingArchive::with_replications(&[89_374, 89_375]);
        let mut replication = replication();
        replication.add_snapshot(snapshot(2, 13));
        replication.add_snapshot(snapshot(17, 37));

        assert_eq!(Ok(1), replication.poll(&mut archive, NOW_NS));
        assert_eq!(
            vec![Call::NextCorrelationId, replication_call(2)],
            archive.calls()
        );

        assert_eq!(Ok(0), replication.poll(&mut archive, NOW_NS));
        replication
            .on_signal(&archive, 89_374, 2, 23_423, RecordingSignal::REPLICATE_END)
            .expect("a REPLICATE_END is accepted");

        assert!(!replication.is_complete());
        // The attempt ended without a SYNC: the cursor is where it was, and the
        // next poll starts the same snapshot again rather than the next one.
        assert_eq!(0, replication.multiple.recording_cursor);
        assert!(replication.multiple.recordings_completed.is_empty());
    }

    /// `closeWillCloseUnderlyingSnapshotReplication`
    /// (`SnapshotReplicationTest.java:169-191`): close stops the live
    /// replication once, and a second close does nothing.
    #[test]
    fn close_will_close_underlying_snapshot_replication() {
        let mut archive = RecordingArchive::with_replications(&[1, 2]);
        let mut replication = replication();
        replication.add_snapshot(snapshot(2, 0));
        replication.add_snapshot(snapshot(17, -1));

        assert_eq!(Ok(1), replication.poll(&mut archive, NOW_NS));

        replication.close(&mut archive);
        assert_eq!(
            vec![
                Call::NextCorrelationId,
                replication_call(2),
                Call::TryStopReplication(1)
            ],
            archive.calls()
        );

        replication.close(&mut archive);
        assert_eq!(3, archive.calls().len());
    }

    /// The `EXTEND` signal is what finds the destination's position counter
    /// (`RecordingReplication.java:180-185`); without it the watchdog has
    /// nothing to read.
    #[test]
    fn an_extend_signal_looks_up_the_destination_position_counter() {
        let mut archive = RecordingArchive::with_replications(&[7]);
        let mut replication = replication();
        replication.add_snapshot(snapshot(2, 0));

        assert_eq!(Ok(1), replication.poll(&mut archive, NOW_NS));

        replication
            .on_signal(&archive, 7, 99, -1, RecordingSignal::EXTEND)
            .expect("an EXTEND is accepted");

        assert_eq!(
            vec![
                Call::NextCorrelationId,
                replication_call(2),
                Call::RecordingPositionCounter(99)
            ],
            archive.calls()
        );
    }

    /// A `DELETE` mid-replication is a failure, not a state change
    /// (`RecordingReplication.java:204-207`).
    #[test]
    fn a_delete_signal_fails_the_replication() {
        let mut archive = RecordingArchive::with_replications(&[7]);
        let mut replication = replication();
        replication.add_snapshot(snapshot(2, 0));
        assert_eq!(Ok(1), replication.poll(&mut archive, NOW_NS));

        let error = replication
            .on_signal(&archive, 7, 99, -1, RecordingSignal::DELETE)
            .expect_err("a DELETE is refused");
        assert!(
            matches!(error, ClusterError::Cluster(message) if message.contains("deleted during replication"))
        );
    }
}
