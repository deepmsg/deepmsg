//! The recorder: every recording session in flight, and the numbers they add
//! up to.
//!
//! `ArchiveConductor.Recorder` is a `SessionWorker` of its own
//! (`ArchiveConductor.java:2699-2744`), driven on its own turn by the
//! conductor that owns it (`SharedModeArchiveConductor.java:56-63` — the
//! recorder runs *after* the conductor's own sessions). What it holds is the
//! recording sessions and three running totals; what it does not hold is
//! anything the *conductor* does when a session ends, which in the reference is
//! the `closeSession` hook a `SharedModeRecorder` overrides to reach back into
//! the conductor (`:88-96`).
//!
//! That split is kept here, and one step further: a session that has finished is
//! **handed back** by [`Recorder::take_finished`] rather than closed by the
//! recorder. The work of closing one — the catalog's stop position, the `STOP`
//! signal, the subscription's refcount — belongs to whoever holds the catalog,
//! the control sessions and the registry, and here that is one object that the
//! recorder does not own.
//!
//! # The totals, and when they are published
//!
//! Three numbers, accumulated from what the writers report:
//!
//! | counter | what it holds |
//! |---|---|
//! | 106 | the bytes every recording has written |
//! | 107 | the nanoseconds those writes took |
//! | 105 | the longest one write ever took |
//!
//! The reference accumulates all three from `RecordingWriter` calls
//! (`:2717-2730`, hung off the writer's own clock around each block,
//! `RecordingWriter.java:117`, `:143-145`) and publishes them **only on a turn
//! that did work** (`:2732-2743`). The condition is not an optimisation: what it
//! changes is *when* a reader of those counters sees them, and the reference's
//! line is the one to copy.

use deepmsg_client::client::Client;
use deepmsg_cnc::counters::CountersReader;
use deepmsg_core::buffer::ReadWrite;

use crate::recording_writer::WriteStats;
use crate::server::counters::{
    ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID, ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID,
    ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID, ArchiveIdCounter, RECORDER_MAX_WRITE_TIME_NAME,
    RECORDER_TOTAL_WRITE_BYTES_NAME, RECORDER_TOTAL_WRITE_TIME_NAME, claim_archive_id_counter,
    request_archive_id_counter,
};
use crate::server::recording_session::RecordingSession;

/// What every recording this archive has made adds up to
/// (`ArchiveConductor.java:2701-2703`).
///
/// `i64` and not `u64`, because the reference's three are `long`s and are
/// published into counters that are read as `long`s — the writer hands over a
/// byte count that cannot be negative, and the accumulator is the reference's
/// type.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecorderTotals {
    total_write_bytes: i64,
    total_write_time_ns: i64,
    max_write_time_ns: i64,
}

impl WriteStats for RecorderTotals {
    fn bytes_written(&mut self, bytes: u64) {
        self.total_write_bytes += i64::try_from(bytes).unwrap_or(i64::MAX);
    }

    fn write_time_ns(&mut self, nanos: u64) {
        let nanos = i64::try_from(nanos).unwrap_or(i64::MAX);

        self.total_write_time_ns += nanos;

        // `if (nanos > maxWriteTimeNs)` (`:2726-2729`): the largest single
        // write, not the total.
        if nanos > self.max_write_time_ns {
            self.max_write_time_ns = nanos;
        }
    }
}

impl RecorderTotals {
    /// The bytes written, the nanoseconds spent, and the longest single write
    /// (`:2701-2703`).
    #[must_use]
    pub const fn totals(&self) -> (i64, i64, i64) {
        (
            self.total_write_bytes,
            self.total_write_time_ns,
            self.max_write_time_ns,
        )
    }
}

/// The three counters those totals are published to.
///
/// 105, 106 and 107, allocated lazily — which is the conductor's to do, since
/// it is the object with the client and the counters region.
#[derive(Debug)]
pub struct RecorderCounters {
    /// 105: the longest single write, whose allocation the reference refuses if
    /// one for this archive id is already there (`Archive.java:1589-1595`).
    pub max_write_time: ArchiveIdCounter,
    /// 106.
    pub total_write_bytes: ArchiveIdCounter,
    /// 107.
    pub total_write_time: ArchiveIdCounter,
}

/// The three write counters, in the order the reference allocates them
/// (`Archive.java:1587-1626`).
///
/// The order is not decoration: the reference asks for the max-write-time one
/// first and **refuses to start** if a 105 for this archive id is already there
/// (`:1589-1595`), and this build does that check where its archive starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecorderCounter {
    /// 105: the longest single write, whose duplicate is refused.
    MaxWriteTime,
    /// 106: the bytes every recording has written.
    TotalWriteBytes,
    /// 107: the nanoseconds those writes took.
    TotalWriteTime,
}

impl RecorderCounter {
    /// All three, in the order the reference allocates them.
    const ORDER: [Self; 3] = [
        Self::MaxWriteTime,
        Self::TotalWriteBytes,
        Self::TotalWriteTime,
    ];

    /// The type id `ArchiveCounters.allocate` is called with (`:1598`, `:1610`,
    /// `:1622` over `AeronCounters`).
    #[must_use]
    const fn type_id(self) -> i32 {
        match self {
            Self::MaxWriteTime => ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID,
            Self::TotalWriteBytes => ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID,
            Self::TotalWriteTime => ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID,
        }
    }

    /// The name its label is built from (`:1600`, `:1612`, `:1624`).
    #[must_use]
    const fn name(self) -> &'static str {
        match self {
            Self::MaxWriteTime => RECORDER_MAX_WRITE_TIME_NAME,
            Self::TotalWriteBytes => RECORDER_TOTAL_WRITE_BYTES_NAME,
            Self::TotalWriteTime => RECORDER_TOTAL_WRITE_TIME_NAME,
        }
    }
}

/// Every recording session, and the numbers they add up to.
#[derive(Debug)]
pub struct Recorder {
    /// The sessions still in flight, which is the reference's
    /// `SessionWorker.sessions` (`SessionWorker.java:23`).
    sessions: Vec<RecordingSession>,
    /// The ones that finished and have not been collected
    /// ([`Recorder::take_finished`]).
    finished: Vec<RecordingSession>,
    totals: RecorderTotals,
    /// `None` until the three counters have been allocated — and an archive that
    /// never allocated them still records, it just does not count.
    counters: Option<RecorderCounters>,
    /// The counters that are in hand while the rest are still on their way.
    ///
    /// The reference allocates all three at once, in `Archive.Context.conclude`
    /// (`:1587-1626`), because its archive drives the driver. Here each is a
    /// command the driver answers on a later turn, so they are asked for **one
    /// at a time** and this holds the ones already claimed.
    held: Vec<(RecorderCounter, ArchiveIdCounter)>,
    /// The counter being asked for, and the registration id the add drew —
    /// `None` between one counter arriving and the next being asked for.
    pending: Option<(RecorderCounter, i64)>,
    /// The archive id every one of the three is keyed by
    /// (`ArchiveCounters.allocate`, `ArchiveCounters.java:52-69`).
    archive_id: i64,
}

impl Recorder {
    /// A recorder with nothing in it, for an archive with this id.
    ///
    /// The id is what the three counters are keyed by — the reference's
    /// `ArchiveCounters.allocate` writes it into the key and into the label's
    /// suffix — so a recorder has to know it before it can count.
    #[must_use]
    pub const fn new(archive_id: i64) -> Self {
        Self {
            sessions: Vec::new(),
            finished: Vec::new(),
            totals: RecorderTotals {
                total_write_bytes: 0,
                total_write_time_ns: 0,
                max_write_time_ns: 0,
            },
            counters: None,
            held: Vec::new(),
            pending: None,
            archive_id,
        }
    }

    /// Ask for the three counters, one at a time, and take each up when the
    /// driver answers (`Archive.java:1587-1626`).
    ///
    /// The reference does all three in `Archive.Context.conclude` — at startup,
    /// synchronously, because its archive drives the driver. This build cannot:
    /// it runs inside the turn that drives the driver, so each add goes out on
    /// one turn and its answer is read on a later one (the same two steps the
    /// control-session counters take, `Sessions::allocate_session_counter`).
    ///
    /// One at a time rather than three at once, because that keeps the state one
    /// registration id instead of three and the order is the reference's anyway.
    /// A refusal is not retried: a counter the driver will not make can never
    /// become one, and a recorder with no counters still records.
    pub fn allocate(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        timeout: std::time::Duration,
    ) {
        if self.counters.is_some() {
            return;
        }

        let Some((counter, registration_id)) = self.pending else {
            let Some(counter) = RecorderCounter::ORDER
                .into_iter()
                .find(|counter| !self.holds(*counter))
            else {
                return;
            };

            match request_archive_id_counter(
                client,
                counter.type_id(),
                counter.name(),
                self.archive_id,
                timeout,
            ) {
                Ok(registration_id) => self.pending = Some((counter, registration_id)),
                Err(_) => {
                    // A command that would not go out is one this build has
                    // already given up on: the next turn asks again for the
                    // first counter it does not hold, and the archive records
                    // without counting until one takes.
                    self.pending = None;
                }
            }

            return;
        };

        match claim_archive_id_counter(client, counters, counter.type_id(), registration_id) {
            Ok(Some(counter_id)) => {
                self.held.push((counter, counter_id));

                if self.held.len() == RecorderCounter::ORDER.len() {
                    let held = std::mem::take(&mut self.held);
                    let take = |want: RecorderCounter| {
                        held.iter()
                            .find(|(counter, _)| *counter == want)
                            .map(|(_, counter)| *counter)
                            .expect("all three are in hand")
                    };

                    self.counters = Some(RecorderCounters {
                        max_write_time: take(RecorderCounter::MaxWriteTime),
                        total_write_bytes: take(RecorderCounter::TotalWriteBytes),
                        total_write_time: take(RecorderCounter::TotalWriteTime),
                    });
                }

                self.pending = None;
            }
            Ok(None) => {}
            Err(_) => self.pending = None,
        }
    }

    /// Whether one of the three is in hand.
    fn holds(&self, counter: RecorderCounter) -> bool {
        self.held.iter().any(|(held, _)| *held == counter)
    }

    /// `recorder.addSession` (`ArchiveConductor.java:2055`).
    pub fn add_session(&mut self, session: RecordingSession) {
        self.sessions.push(session);
    }

    /// The counters, once the conductor has allocated them.
    pub fn set_counters(&mut self, counters: RecorderCounters) {
        self.counters = counters.into();
    }

    /// How many recordings are in flight.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// What every recording has added up to, for a reader that wants the
    /// numbers before a counter has been published to.
    #[must_use]
    pub const fn totals(&self) -> RecorderTotals {
        self.totals
    }

    /// Abort every session reading this subscription
    /// (`ArchiveConductor.abortRecordingSessionAndCloseSubscription`,
    /// `:1766-1772`).
    ///
    /// The reference walks **the map of sessions**, not the subscription's own
    /// list, and aborts the ones that are reading it. A session that has
    /// finished is not one of them: its recording has already stopped, and what
    /// is left for it is the closing, which the conductor does when it collects
    /// it.
    pub fn abort_sessions_for(&mut self, subscription_id: i64, reason: &str) {
        for session in &mut self.sessions {
            if session.subscription_id() == subscription_id {
                session.abort(reason);
            }
        }
    }

    /// Abort **one** session, by the recording it is making
    /// (`ArchiveConductor.stopRecordingByIdentity`, `:1306-1310`).
    ///
    /// Not [`Recorder::abort_sessions_for`]: a subscription can carry more than
    /// one image over its life, so "the session for this recording" and "every
    /// session reading this subscription" are different sets, and a stop named
    /// by identity is about one recording.
    pub fn abort_session(&mut self, recording_id: i64, reason: &str) {
        for session in &mut self.sessions {
            if session.recording_id() == recording_id {
                session.abort(reason);
            }
        }
    }

    /// One turn of every session (`SessionWorker.doWork` over
    /// `RecordingSession.doWork`).
    ///
    /// The sessions are driven first and removed afterwards, which is the
    /// worker's own order (`SessionWorker.java:56-81`) — and the reason a
    /// session that finished this turn still gets its turn.
    pub fn drive(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
    ) -> usize {
        let mut work = 0;
        let totals = &mut self.totals;

        for session in &mut self.sessions {
            work += session.do_work(client, counters, totals);
        }

        let mut index = 0;
        while index < self.sessions.len() {
            if self.sessions[index].is_done() {
                self.finished.push(self.sessions.swap_remove(index));
            } else {
                index += 1;
            }
        }

        self.publish_if(counters, work);

        work
    }

    /// The sessions that finished, for the conductor to close
    /// (`SessionWorker.closeSession`, `:73-76`, which a `SharedModeRecorder`
    /// sends to `closeRecordingSession`).
    pub fn take_finished(&mut self) -> Vec<RecordingSession> {
        self.finished.drain(..).collect()
    }

    /// Publish the three totals — but only on a turn that did work
    /// (`ArchiveConductor.java:2732-2743`).
    ///
    /// `workCount > 0` is the whole of the condition, and it is a parameter here
    /// rather than a test of the totals because that is what the reference
    /// tests. A turn that read nothing is a turn whose numbers have not moved.
    fn publish_if(&self, counters: &CountersReader<'_, ReadWrite>, work: usize) {
        if 0 == work {
            return;
        }

        let Some(published) = &self.counters else {
            return;
        };

        let (total_write_bytes, total_write_time_ns, max_write_time_ns) = self.totals.totals();

        published.total_write_bytes.set(counters, total_write_bytes);
        published
            .total_write_time
            .set(counters, total_write_time_ns);
        published.max_write_time.set(counters, max_write_time_ns);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::server::recording_pos::RecordingPos;

    use deepmsg_core::buffer::AtomicBuffer;

    /// A values region with room for the three counters, leaked so it can be
    /// handed to a call.
    ///
    /// The same helper `control_session`'s and `conductor`'s tests keep their
    /// own copy of, for the same reason: it is ten lines of alignment and the
    /// alternative is a shared test module for three callers.
    fn values_region() -> CountersReader<'static, ReadWrite> {
        #[repr(align(64))]
        struct Region([u8; 64 * 64]);

        let metadata: &'static mut Region = Box::leak(Box::new(Region([0; 64 * 64])));
        let values: &'static mut Region = Box::leak(Box::new(Region([0; 64 * 64])));

        CountersReader::new(
            AtomicBuffer::from_slice_mut(&mut metadata.0).expect("aligned region"),
            AtomicBuffer::from_slice_mut(&mut values.0).expect("aligned region"),
        )
    }

    /// A recorder with the three counters pointing at the first three slots of
    /// a region of its own.
    fn recorder_with_counters() -> (Recorder, CountersReader<'static, ReadWrite>) {
        let counters = values_region();
        let mut recorder = Recorder::new(42);

        recorder.set_counters(RecorderCounters {
            max_write_time: ArchiveIdCounter::for_test(0),
            total_write_bytes: ArchiveIdCounter::for_test(1),
            total_write_time: ArchiveIdCounter::for_test(2),
        });

        (recorder, counters)
    }

    /// The three totals, and the one of them that is a maximum: a shorter write
    /// after a longer one leaves the longest where it was
    /// (`ArchiveConductor.java:2717-2730`).
    #[test]
    fn the_totals_add_up_and_the_longest_write_is_kept() {
        let mut totals = RecorderTotals::default();

        totals.bytes_written(128);
        totals.write_time_ns(1_000);
        totals.bytes_written(64);
        totals.write_time_ns(500);

        assert_eq!((192, 1_500, 1_000), totals.totals());

        totals.write_time_ns(4_000);

        assert_eq!(
            (192, 5_500, 4_000),
            totals.totals(),
            "the max follows the longest, not the last"
        );
    }

    /// The three counters are published on a turn that did work and **not** on
    /// one that did not (`ArchiveConductor.java:2732-2743`).
    ///
    /// What the condition is about is when a reader sees the numbers, so the
    /// quiet turn is asserted *after* a busy one and against the numbers that
    /// turn wrote: a slot that still reads what it read before is a slot the
    /// quiet turn left alone. (An unwritten slot reads zero here rather than
    /// "absent", because the metadata half of this region describes no
    /// counters — see the helper.)
    #[test]
    fn the_counters_are_published_only_on_a_turn_that_did_work() {
        let (mut recorder, counters) = recorder_with_counters();

        recorder.totals.bytes_written(4_096);
        recorder.totals.write_time_ns(7_000);
        recorder.totals.write_time_ns(9_000);

        recorder.publish_if(&counters, 1);

        assert_eq!(Some(9_000), counters.value(0), "105 is the longest write");
        assert_eq!(Some(4_096), counters.value(1));
        assert_eq!(Some(16_000), counters.value(2));

        recorder.totals.bytes_written(1_024);
        recorder.publish_if(&counters, 0);

        assert_eq!(Some(9_000), counters.value(0));
        assert_eq!(
            Some(4_096),
            counters.value(1),
            "a quiet turn writes nothing"
        );
        assert_eq!(Some(16_000), counters.value(2));

        recorder.publish_if(&counters, 2);

        assert_eq!(
            Some(5_120),
            counters.value(1),
            "and the next one catches up"
        );
    }

    /// A stop **by identity** aborts the recording it names and leaves the
    /// other sessions alone, even the ones reading the same subscription
    /// (`ArchiveConductor.stopRecordingByIdentity`, `:1306-1310`).
    ///
    /// `abort_sessions_for` is the subscription's question
    /// (`abortRecordingSessionAndCloseSubscription`, `:1766-1774`); this is the
    /// recording's, and a subscription can carry more than one image over its
    /// life, so the two sets are not the same.
    #[test]
    fn a_stop_by_identity_aborts_one_recording_and_not_its_subscription() {
        let dir = crate::mark::tests::TempDir::new();
        let mut recorder = Recorder::new(42);

        // Two recordings on one subscription — the shape a recording whose
        // publisher restarted leaves behind for a turn.
        recorder.add_session(a_session(dir.path(), 0, 9));
        recorder.add_session(a_session(dir.path(), 1, 9));
        assert_eq!(2, recorder.session_count());

        recorder.abort_session(0, "stop recording by identity");

        assert!(
            recorder.sessions[0].abort_reason().is_some(),
            "the recording that was named is aborted"
        );
        assert!(
            recorder.sessions[1].abort_reason().is_none(),
            "and the other recording on the same subscription is not"
        );

        recorder.abort_sessions_for(9, "stop recording");

        assert!(
            recorder.sessions[1].abort_reason().is_some(),
            "while the subscription's own question aborts what is left on it"
        );
    }

    /// One session, made without a client: enough of one for the two aborts
    /// above, and nothing that opens a file.
    fn a_session(
        directory: &std::path::Path,
        recording_id: i64,
        subscription_id: i64,
    ) -> RecordingSession {
        RecordingSession::new(
            3,
            7,
            recording_id,
            recording_id * 1024,
            recording_id * 1024,
            128 * 1024,
            subscription_id,
            11,
            64 * 1024,
            1024 * 1024,
            0,
            false,
            RecordingPos::for_test(1),
            directory,
            None,
        )
    }

    /// A recorder with no counters still records: the numbers are accumulated
    /// and simply never published, which is what an archive that never
    /// allocated them does (`Archive.java:1586-1626` allocates on demand).
    #[test]
    fn a_recorder_with_no_counters_publishes_nothing() {
        let counters = values_region();
        let mut recorder = Recorder::new(42);

        recorder.totals.bytes_written(8);

        recorder.publish_if(&counters, 1);

        assert_eq!(Some(0), counters.value(0), "nothing was written here");
        assert_eq!(Some(0), counters.value(1));
        assert_eq!(Some(0), counters.value(2));
        assert_eq!((8, 0, 0), recorder.totals().totals(), "but it was counted");
    }

    /// The three counters are the reference's three, by **type id and label**:
    /// what `AeronStat` reads to tell a 105 from a 106, and what a comparison
    /// against the reference's own reader turns on (`Archive.java:1587-1626`).
    #[test]
    fn the_three_counters_are_the_references_three() {
        assert_eq!(
            105,
            RecorderCounter::MaxWriteTime.type_id(),
            "AeronCounters.ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID"
        );
        assert_eq!(106, RecorderCounter::TotalWriteBytes.type_id());
        assert_eq!(107, RecorderCounter::TotalWriteTime.type_id());

        assert_eq!(
            "archive-recorder max write time in ns",
            RecorderCounter::MaxWriteTime.name()
        );
        assert_eq!(
            "archive-recorder total write bytes",
            RecorderCounter::TotalWriteBytes.name()
        );
        assert_eq!(
            "archive-recorder total write time in ns",
            RecorderCounter::TotalWriteTime.name()
        );

        // And the label the reader sees is that name with the archive id's
        // suffix, which is the one thing about these counters that a reader has
        // to agree with us about (`ArchiveCounters.java:35`, `:52-69`).
        assert_eq!(
            "archive-recorder total write bytes - archiveId=42",
            crate::server::counters::archive_id_label(RecorderCounter::TotalWriteBytes.name(), 42)
        );

        assert_eq!(
            [
                RecorderCounter::MaxWriteTime,
                RecorderCounter::TotalWriteBytes,
                RecorderCounter::TotalWriteTime
            ],
            RecorderCounter::ORDER,
            "and they are asked for in the reference's order (`:1587-1626`)"
        );
    }

    /// A recorder is empty until a session is added, and a session that has
    /// finished is handed back rather than closed
    /// (`SessionWorker.java:56-81`).
    #[test]
    fn an_empty_recorder_hands_back_nothing() {
        let mut recorder = Recorder::new(42);

        assert!(recorder.is_empty());
        assert_eq!(0, recorder.session_count());
        assert!(recorder.take_finished().is_empty());
        assert_eq!(RecorderTotals::default(), recorder.totals());
    }
}
