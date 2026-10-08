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
use crate::server::counters::ArchiveIdCounter;
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

/// Every recording session, and the numbers they add up to.
#[derive(Debug, Default)]
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
}

impl Recorder {
    /// A recorder with nothing in it.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            sessions: Vec::new(),
            finished: Vec::new(),
            totals: RecorderTotals {
                total_write_bytes: 0,
                total_write_time_ns: 0,
                max_write_time_ns: 0,
            },
            counters: None,
        }
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
        let mut recorder = Recorder::new();

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

    /// A recorder with no counters still records: the numbers are accumulated
    /// and simply never published, which is what an archive that never
    /// allocated them does (`Archive.java:1586-1626` allocates on demand).
    #[test]
    fn a_recorder_with_no_counters_publishes_nothing() {
        let counters = values_region();
        let mut recorder = Recorder::new();

        recorder.totals.bytes_written(8);

        recorder.publish_if(&counters, 1);

        assert_eq!(Some(0), counters.value(0), "nothing was written here");
        assert_eq!(Some(0), counters.value(1));
        assert_eq!(Some(0), counters.value(2));
        assert_eq!((8, 0, 0), recorder.totals().totals(), "but it was counted");
    }

    /// A recorder is empty until a session is added, and a session that has
    /// finished is handed back rather than closed
    /// (`SessionWorker.java:56-81`).
    #[test]
    fn an_empty_recorder_hands_back_nothing() {
        let mut recorder = Recorder::new();

        assert!(recorder.is_empty());
        assert_eq!(0, recorder.session_count());
        assert!(recorder.take_finished().is_empty());
        assert_eq!(RecorderTotals::default(), recorder.totals());
    }
}
