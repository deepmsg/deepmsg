//! The replayer's counters: how many replays are open, and what the reads cost.
//!
//! Four of the archive's, and the reference allocates them together
//! (`Archive.java:1578-1659`): **112** `Archive Replay Sessions`, which the
//! conductor moves as sessions open and close (`:993`, `:1382`), and the
//! replayer's three statistics — **108** the longest single read, **109** the
//! bytes read, **110** the nanoseconds spent reading — which the `Replayer`
//! worker publishes once per turn that did work (`:2745-2791`).
//!
//! # The two halves live in different places, on purpose
//!
//! In the reference all four belong to `ArchiveConductor.Replayer`, because
//! that worker is what drives the replay sessions. In this build the sessions
//! are [`Sessions`](crate::server::conductor::Sessions)' — they are driven in
//! the conductor's own turn rather than by a worker of their own (P2-9a is
//! where a replayer thread would arrive) — so what is here is the part that is
//! the replayer's whatever drives it: the four counters, the running totals, and
//! the rule that a turn which read nothing publishes nothing.

use std::time::Duration;

use deepmsg_client::client::Client;
use deepmsg_cnc::counters::CountersReader;
use deepmsg_core::buffer::ReadWrite;

use crate::server::counters::{
    ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID, ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID,
    ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID, ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID,
    ArchiveIdCounter, CounterAllocator, CounterKind, REPLAY_SESSIONS_NAME,
    REPLAYER_MAX_READ_TIME_NAME, REPLAYER_TOTAL_READ_BYTES_NAME, REPLAYER_TOTAL_READ_TIME_NAME,
};

/// One of the replayer's four counters.
///
/// The order is `Archive.Context.conclude`'s (`Archive.java:1578-1659`), which
/// allocates 112, then the recorder's three, then these — the recorder's own
/// block is the other [`CounterAllocator`], and the two blocks are independent
/// of each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayerCounter {
    /// 112: how many replay sessions are open right now.
    ReplaySessions,
    /// 108: the longest single read, in nanoseconds.
    MaxReadTime,
    /// 109: every byte every replay has read.
    TotalReadBytes,
    /// 110: every nanosecond those reads took.
    TotalReadTime,
}

impl CounterKind for ReplayerCounter {
    const ORDER: &'static [Self] = &[
        Self::ReplaySessions,
        Self::MaxReadTime,
        Self::TotalReadBytes,
        Self::TotalReadTime,
    ];

    /// `AeronCounters`' own ids (`:792`, `:799`, `:806`, `:818`).
    fn type_id(self) -> i32 {
        match self {
            Self::ReplaySessions => ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID,
            Self::MaxReadTime => ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID,
            Self::TotalReadBytes => ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID,
            Self::TotalReadTime => ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID,
        }
    }

    /// The names their labels are built from (`Archive.java:1581`, `:1635`,
    /// `:1646`, `:1657`).
    fn name(self) -> &'static str {
        match self {
            Self::ReplaySessions => REPLAY_SESSIONS_NAME,
            Self::MaxReadTime => REPLAYER_MAX_READ_TIME_NAME,
            Self::TotalReadBytes => REPLAYER_TOTAL_READ_BYTES_NAME,
            Self::TotalReadTime => REPLAYER_TOTAL_READ_TIME_NAME,
        }
    }
}

/// The four, in hand.
///
/// `pub(crate)` fields inside a crate-private module: nothing outside the
/// archive's own loop moves them.
#[derive(Debug)]
pub struct ReplayerCounters {
    pub(crate) replay_sessions: ArchiveIdCounter,
    pub(crate) max_read_time: ArchiveIdCounter,
    pub(crate) total_read_bytes: ArchiveIdCounter,
    pub(crate) total_read_time: ArchiveIdCounter,
}

/// What the reads have added up to (`ArchiveConductor.java:2747-2749`).
///
/// `i64` and not `u64`, for the reason [`RecorderTotals`] gives: the
/// reference's three are `long`s, and they are published into counters read as
/// `long`s.
///
/// [`RecorderTotals`]: crate::server::recorder::RecorderTotals
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadTotals {
    total_read_bytes: i64,
    total_read_time_ns: i64,
    max_read_time_ns: i64,
}

impl ReadTotals {
    /// `Replayer.bytesRead` and `Replayer.readTimeNs` (`:2764-2777`), which a
    /// session calls once per read it did.
    ///
    /// The maximum is of a **single** read, so a short read after a long one
    /// leaves it where it was (`:2769-2772`).
    pub fn read(&mut self, bytes: u64, time_ns: u64) {
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        let time_ns = i64::try_from(time_ns).unwrap_or(i64::MAX);

        self.total_read_bytes = self.total_read_bytes.saturating_add(bytes);
        self.total_read_time_ns = self.total_read_time_ns.saturating_add(time_ns);

        if time_ns > self.max_read_time_ns {
            self.max_read_time_ns = time_ns;
        }
    }

    /// The bytes, the nanoseconds, and the longest single read (`:2747-2749`).
    #[must_use]
    pub const fn totals(&self) -> (i64, i64, i64) {
        (
            self.total_read_bytes,
            self.total_read_time_ns,
            self.max_read_time_ns,
        )
    }
}

/// `ArchiveConductor.Replayer` (`:2745-2791`), as far as its counters go: the
/// four, the totals, and the rule for publishing them.
#[derive(Debug)]
pub struct Replayer {
    /// `None` until the four have been allocated — and a replayer that never
    /// allocated them still replays, it just does not count.
    counters: Option<ReplayerCounters>,
    allocator: CounterAllocator<ReplayerCounter>,
    totals: ReadTotals,
}

impl Replayer {
    /// A replayer for an archive with this id, which is what the four are keyed
    /// by.
    #[must_use]
    pub const fn new(archive_id: i64) -> Self {
        Self {
            counters: None,
            allocator: CounterAllocator::new(archive_id),
            totals: ReadTotals {
                total_read_bytes: 0,
                total_read_time_ns: 0,
                max_read_time_ns: 0,
            },
        }
    }

    /// Ask for the four, one at a time, and take each up when the driver
    /// answers (`Archive.java:1578-1659`).
    pub fn allocate(
        &mut self,
        client: &mut Client,
        counters: &CountersReader<'_, ReadWrite>,
        timeout: Duration,
    ) {
        if self.counters.is_some() {
            return;
        }

        self.allocator.drive(client, counters, timeout);

        if !self.allocator.is_complete() {
            return;
        }

        let take = |kind| self.allocator.counter(kind).expect("all four are in hand");

        self.counters = Some(ReplayerCounters {
            replay_sessions: take(ReplayerCounter::ReplaySessions),
            max_read_time: take(ReplayerCounter::MaxReadTime),
            total_read_bytes: take(ReplayerCounter::TotalReadBytes),
            total_read_time: take(ReplayerCounter::TotalReadTime),
        });
    }

    /// `Replayer.bytesRead` + `readTimeNs`, for one session's read.
    pub fn read(&mut self, bytes: u64, time_ns: u64) {
        self.totals.read(bytes, time_ns);
    }

    /// Publish the three statistics — but only on a turn that did work
    /// (`:2779-2790`).
    ///
    /// `workCount > 0` is the whole of the condition, and it is a parameter here
    /// rather than a test of the totals because that is what the reference
    /// tests. A turn that read nothing is a turn whose numbers have not moved.
    pub fn publish_if(&self, counters: &CountersReader<'_, ReadWrite>, work: usize) {
        if 0 == work {
            return;
        }

        let Some(published) = &self.counters else {
            return;
        };

        let (total_read_bytes, total_read_time_ns, max_read_time_ns) = self.totals.totals();

        published.total_read_bytes.set(counters, total_read_bytes);
        published.total_read_time.set(counters, total_read_time_ns);
        published.max_read_time.set(counters, max_read_time_ns);
    }

    /// `ctx.replaySessionCounter().incrementRelease()` (`:993`), which the
    /// reference does where a replay session is put in the map.
    pub fn session_opened(&self, counters: &CountersReader<'_, ReadWrite>) {
        if let Some(published) = &self.counters {
            published.replay_sessions.increment(counters);
        }
    }

    /// `decrementRelease` (`:1382`), where one is closed.
    pub fn session_closed(&self, counters: &CountersReader<'_, ReadWrite>) {
        if let Some(published) = &self.counters {
            published.replay_sessions.decrement(counters);
        }
    }

    /// The totals, for a caller that wants to check them — the reference's own
    /// three fields, read back.
    #[must_use]
    pub const fn totals(&self) -> (i64, i64, i64) {
        self.totals.totals()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_core::buffer::AtomicBuffer;

    /// The four, over slots 0 to 3 of a region of its own.
    fn replayer_with_counters() -> (Replayer, Vec<u8>, Vec<u8>) {
        let mut meta = vec![0u8; 4 * deepmsg_cnc::layout::COUNTER_METADATA_LENGTH];
        let values = vec![0u8; 4 * deepmsg_cnc::layout::COUNTER_VALUE_LENGTH];

        // Every slot allocated, and the archive's own counting is a read of the
        // values region afterwards.
        for slot in 0..4 {
            let base = slot * deepmsg_cnc::layout::COUNTER_METADATA_LENGTH;
            meta[base + deepmsg_cnc::layout::COUNTER_STATE_OFFSET
                ..base + deepmsg_cnc::layout::COUNTER_STATE_OFFSET + 4]
                .copy_from_slice(&deepmsg_cnc::layout::COUNTER_STATE_ALLOCATED.to_le_bytes());
        }

        let mut replayer = Replayer::new(42);
        replayer.counters = Some(ReplayerCounters {
            replay_sessions: ArchiveIdCounter::for_test(0),
            max_read_time: ArchiveIdCounter::for_test(1),
            total_read_bytes: ArchiveIdCounter::for_test(2),
            total_read_time: ArchiveIdCounter::for_test(3),
        });

        (replayer, meta, values)
    }

    /// The totals add up, and the one that is a maximum stays at the longest —
    /// a short read after a long one leaves it where it was
    /// (`ArchiveConductor.java:2764-2777`).
    #[test]
    fn the_reads_add_up_and_the_longest_read_is_kept() {
        let mut totals = ReadTotals::default();

        totals.read(4_096, 1_000);
        totals.read(1_024, 500);

        assert_eq!((5_120, 1_500, 1_000), totals.totals());

        totals.read(2_048, 4_000);

        assert_eq!((7_168, 5_500, 4_000), totals.totals());
    }

    /// The three are published on a turn that did work, and not otherwise
    /// (`:2779-2790`).
    #[test]
    fn the_statistics_are_published_only_on_a_turn_that_did_work() {
        let (mut replayer, mut meta, mut values) = replayer_with_counters();
        let counters = CountersReader::new(
            AtomicBuffer::from_slice_mut(&mut meta).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut values).expect("aligned"),
        );

        replayer.read(4_096, 1_000);

        replayer.publish_if(&counters, 0);
        assert_eq!(Some(0), counters.value(1), "an idle turn publishes nothing");
        assert_eq!(Some(0), counters.value(2));

        replayer.publish_if(&counters, 1);
        assert_eq!(Some(1_000), counters.value(1));
        assert_eq!(Some(4_096), counters.value(2));
        assert_eq!(Some(1_000), counters.value(3));
    }

    /// A replayer with no counters replays without counting: the allocation is
    /// the driver's to answer, and an archive that never got an answer still
    /// serves.
    #[test]
    fn a_replayer_with_no_counters_publishes_nothing() {
        let mut meta = vec![0u8; deepmsg_cnc::layout::COUNTER_METADATA_LENGTH];
        let mut values = vec![0u8; deepmsg_cnc::layout::COUNTER_VALUE_LENGTH];
        let counters = CountersReader::new(
            AtomicBuffer::from_slice_mut(&mut meta).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut values).expect("aligned"),
        );

        let mut replayer = Replayer::new(42);
        replayer.read(4_096, 1_000);
        replayer.publish_if(&counters, 1);
        replayer.session_opened(&counters);
        replayer.session_closed(&counters);

        assert_eq!(Some(0), counters.value(0));
    }

    /// The session count goes up and down with the sessions that are open
    /// (`:993`, `:1382`).
    #[test]
    fn the_session_count_follows_the_sessions() {
        let (replayer, mut meta, mut values) = replayer_with_counters();
        let counters = CountersReader::new(
            AtomicBuffer::from_slice_mut(&mut meta).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut values).expect("aligned"),
        );

        replayer.session_opened(&counters);
        replayer.session_opened(&counters);
        assert_eq!(Some(2), counters.value(0));

        replayer.session_closed(&counters);
        assert_eq!(Some(1), counters.value(0));
    }

    /// The four kinds are the reference's four, with the reference's names.
    #[test]
    fn the_four_counters_are_the_references_four() {
        assert_eq!(
            vec![
                (112, "Archive Replay Sessions"),
                (108, "archive-replayer max read time in ns"),
                (109, "archive-replayer total read bytes"),
                (110, "archive-replayer total read time in ns"),
            ],
            ReplayerCounter::ORDER
                .iter()
                .map(|kind| (kind.type_id(), kind.name()))
                .collect::<Vec<_>>()
        );
    }
}
