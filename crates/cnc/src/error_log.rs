//! The distinct error log: writing it, and reading it back.
//!
//! The driver keeps a de-duplicated log of the errors it has seen, and the
//! reference client exposes it so a tool can show why a driver is unhappy
//! without reading its stderr. Entries are appended once and then only
//! counted, so the log is a fixed sequence in write order.
//!
//! Mirrors `aeron-client/src/main/c/concurrent/aeron_distinct_error_log.c`:
//! the writer is `:60-204`, the reader `:216-257`. The two agree on an
//! entry's shape — a twenty-four byte header
//! (`aeron_distinct_error_log.h:29-37`, packed to four) followed by the
//! description, with no NUL and **no error code**: the code belongs to the
//! process's de-duplication key and never reaches the region. And they agree
//! on the one ordering that matters: the header's `length` is stored last,
//! under a release, and that store is what publishes everything before it.

use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};

use crate::layout;

/// One entry of the error log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorLogEntry {
    /// How many times this error has been seen.
    pub observation_count: i32,
    /// When it was first seen, epoch milliseconds.
    pub first_observation_timestamp_ms: i64,
    /// When it was last seen, epoch milliseconds.
    pub last_observation_timestamp_ms: i64,
    /// The description, without a NUL terminator.
    pub text: String,
}

/// What a scan saw.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ErrorLogScan {
    /// Entries the filter accepted and handed to the caller.
    pub entries: u32,
    /// A structurally impossible entry — a length shorter than the header.
    /// The scan stops there, because every subsequent offset would be a guess.
    pub malformed: u32,
    /// The log ended with an entry whose length runs past the region. The
    /// writer appends under a release, so this means the region is smaller
    /// than the writer believes, not that a record is half-written.
    pub truncated: bool,
}

/// A read-only view over the error-log region.
pub struct ErrorLogReader<'a> {
    buffer: AtomicBuffer<'a>,
}

impl<'a> ErrorLogReader<'a> {
    /// Wrap the error-log region.
    pub const fn new(buffer: AtomicBuffer<'a>) -> Self {
        Self { buffer }
    }

    /// Whether the driver has recorded anything at all.
    ///
    /// Cheap: it reads the first entry's length and stops. The reference does
    /// the same (`aeron_distinct_error_log.c:206-214`).
    pub fn has_entries(&self) -> bool {
        self.buffer
            .load_i32_acquire(layout::ERROR_LOG_LENGTH_OFFSET)
            .is_some_and(|length| 0 != length)
    }

    /// Append every entry observed at or after `since_timestamp_ms`.
    ///
    /// Pass `i64::MIN` for everything. Entries are appended to `out`, which is
    /// reused rather than allocated per call.
    pub fn read(&self, since_timestamp_ms: i64, out: &mut Vec<ErrorLogEntry>) -> ErrorLogScan {
        let mut scan = ErrorLogScan::default();
        let mut offset: usize = 0;

        while offset + layout::ERROR_LOG_HEADER_LENGTH <= self.buffer.len() {
            let Some(length) = self
                .buffer
                .load_i32_acquire(offset + layout::ERROR_LOG_LENGTH_OFFSET)
            else {
                break;
            };

            // A zero length is the end of the log, not an error: entries are
            // contiguous from offset zero and the rest of the region is
            // untouched.
            if 0 == length {
                break;
            }

            #[allow(clippy::cast_sign_loss)] // rejected above
            let length = length as usize;
            if length < layout::ERROR_LOG_HEADER_LENGTH {
                scan.malformed += 1;
                break;
            }

            let Some(end) = offset.checked_add(length) else {
                scan.malformed += 1;
                break;
            };
            if end > self.buffer.len() {
                scan.truncated = true;
                break;
            }

            let Some(last) = self
                .buffer
                .load_i64_acquire(offset + layout::ERROR_LOG_LAST_TIMESTAMP_OFFSET)
            else {
                break;
            };

            if last >= since_timestamp_ms {
                let payload = length - layout::ERROR_LOG_HEADER_LENGTH;
                let mut text = vec![0u8; payload];

                if self
                    .buffer
                    .copy_out(offset + layout::ERROR_LOG_HEADER_LENGTH, &mut text)
                    .is_none()
                {
                    scan.truncated = true;
                    break;
                }

                scan.entries += 1;
                out.push(ErrorLogEntry {
                    // Read after the payload: the reference treats
                    // `observation_count` and the timestamps as independent
                    // snapshots, since each is published by its own release.
                    observation_count: self
                        .buffer
                        .load_i32_acquire(offset + layout::ERROR_LOG_OBSERVATION_COUNT_OFFSET)
                        .unwrap_or_default(),
                    first_observation_timestamp_ms: self
                        .buffer
                        .load_i64_relaxed(offset + layout::ERROR_LOG_FIRST_TIMESTAMP_OFFSET)
                        .unwrap_or_default(),
                    last_observation_timestamp_ms: last,
                    text: String::from_utf8_lossy(&text).into_owned(),
                });
            }

            offset += layout::align_up(length, layout::ERROR_LOG_RECORD_ALIGNMENT);
        }

        scan
    }
}

/// One error the process has seen, remembered so the next sighting counts
/// rather than writes (`aeron_distinct_observation_t`,
/// `aeron_distinct_error_log.c:41-46`).
///
/// The error code never reaches the region — see the module header — so this
/// is the only place it lives, which is also why a restart forgets it: the
/// same text seen by a new process is a new entry, not a counted one.
#[derive(Debug)]
struct Observation {
    error_code: i32,
    offset: usize,
    /// The text as it was first written, which the prefix rule compares
    /// against.
    description: String,
}

/// The write half of the distinct error log: the process's memory of what it
/// has written (`aeron_distinct_error_log_t`, `aeron_distinct_error_log.c:50-66`).
///
/// The region is passed to every call rather than held, for the same reason
/// the counters manager works that way: the conductor owns the CnC file, and
/// a Rust value cannot borrow a window out of a mapping it also holds.
#[derive(Debug, Default)]
pub struct DistinctErrorLog {
    /// Newest last. The reference keeps newest *first* (`:150` prepends into
    /// a fresh array), and the order is observable: a description that
    /// prefixes two remembered ones counts against the newer, so the scan
    /// runs backwards here to match.
    observations: Vec<Observation>,
    next_offset: usize,
}

/// A writable window over the error-log region.
pub struct ErrorLogRegion<'a> {
    buffer: AtomicBuffer<'a, ReadWrite>,
}

impl<'a> ErrorLogRegion<'a> {
    /// Wrap the error-log region, writable.
    pub const fn new(buffer: AtomicBuffer<'a, ReadWrite>) -> Self {
        Self { buffer }
    }

    /// A read-only view of the same bytes, for the reader that shares them.
    pub fn as_read_only(&self) -> ErrorLogReader<'a> {
        ErrorLogReader::new(self.buffer.as_read_only())
    }
}

/// A description the log could not record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordError {
    /// The entry would not fit the region. Nothing was written — not half an
    /// entry, and not a truncated description — and the caller is expected to
    /// say so on stderr, as the reference does
    /// (`aeron_distinct_error_log.c:185-191`).
    Unrecordable { description: String },
}

impl DistinctErrorLog {
    /// An empty log over a fresh region (`aeron_distinct_error_log_init`,
    /// `:30-56`).
    ///
    /// Nothing is read back: the initialiser stores a zero `next_offset` and
    /// walks away (`:52`), because the only region the reference ever hands
    /// it is a CnC file the driver itself just created and zeroed. A log over
    /// a region that already holds entries does not resume after them — it
    /// starts over at offset zero, exactly as the reference would.
    pub const fn new() -> Self {
        Self {
            observations: Vec::new(),
            next_offset: 0,
        }
    }

    /// Record one sighting of an error (`aeron_distinct_error_log_record`,
    /// `aeron_distinct_error_log.c:160-204`).
    ///
    /// A description this process has seen before — same error code, and the
    /// remembered text a prefix of this one — only counts: the observation
    /// count goes up by one and the last-seen time moves. A new description
    /// is written as a fresh entry, in the order that is the contract:
    ///
    /// * the text, the first-seen time and a zero count land first, with no
    ///   ordering of their own;
    /// * the entry's `length` is stored **last**, under a release — that
    ///   store is what publishes the entry, and the reader's acquire of the
    ///   length is what makes the unordered fields safe to read;
    /// * the count of one and the last-seen time of a *new* entry arrive
    ///   after the publish, which is the reference's order too (`:156` then
    ///   `:200-201`).
    ///
    /// # Errors
    ///
    /// [`RecordError::Unrecordable`] when the entry would not fit; nothing is
    /// written then. Counting the failure is the caller's business — the
    /// reference increments its errors counter even for an error it could not
    /// record (`aeron_driver_conductor.c:1203-1210`) — which is why the error
    /// carries the description back rather than asking the caller to re-derive
    /// it.
    pub fn record(
        &mut self,
        region: &ErrorLogRegion<'_>,
        now_ms: i64,
        error_code: i32,
        description: &str,
    ) -> Result<(), RecordError> {
        // The prefix rule (`:79-92`): the remembered text is a prefix of the
        // new one, not the other way round, so "failed to X" counts against
        // "failed to X" and against "failed to X for reason Y", while the
        // shorter sighting after the longer one writes.
        let offset = match self
            .observations
            .iter()
            .rev()
            .find(|observation| {
                observation.error_code == error_code
                    && description.starts_with(observation.description.as_str())
            })
            .map(|observation| observation.offset)
        {
            Some(offset) => offset,
            None => self.append(region, now_ms, error_code, description)?,
        };

        region
            .buffer
            .fetch_add_i32(offset + layout::ERROR_LOG_OBSERVATION_COUNT_OFFSET, 1)
            .expect("capacity was checked before the write");
        region
            .buffer
            .store_i64_release(offset + layout::ERROR_LOG_LAST_TIMESTAMP_OFFSET, now_ms)
            .expect("capacity was checked before the write");

        Ok(())
    }

    /// Write a new entry and remember it
    /// (`aeron_distinct_error_log_new_observation`, `:113-158`).
    fn append(
        &mut self,
        region: &ErrorLogRegion<'_>,
        now_ms: i64,
        error_code: i32,
        description: &str,
    ) -> Result<usize, RecordError> {
        let length = layout::ERROR_LOG_HEADER_LENGTH + description.len();
        let offset = self.next_offset;

        // The whole entry fits or none of it does (`:127`): publishing a
        // truncated description would promise text the entry does not carry.
        if offset.saturating_add(length) > region.buffer.len() {
            return Err(RecordError::Unrecordable {
                description: description.to_owned(),
            });
        }

        // `length` is bounded by the region, and a CnC layout's error-log
        // length is four megabytes.
        #[allow(clippy::cast_possible_truncation)]
        let length_i32 = length as i32;

        region
            .buffer
            .copy_in(
                offset + layout::ERROR_LOG_HEADER_LENGTH,
                description.as_bytes(),
            )
            .expect("capacity was checked before the write");
        region
            .buffer
            .store_i64_relaxed(offset + layout::ERROR_LOG_FIRST_TIMESTAMP_OFFSET, now_ms)
            .expect("capacity was checked before the write");
        region
            .buffer
            .store_i32_relaxed(offset + layout::ERROR_LOG_OBSERVATION_COUNT_OFFSET, 0)
            .expect("capacity was checked before the write");

        self.next_offset = layout::align_up(length, layout::ERROR_LOG_RECORD_ALIGNMENT);
        self.observations.push(Observation {
            error_code,
            offset,
            description: description.to_owned(),
        });

        // The publish (`:156`): everything above is deliberately unordered,
        // because this release is what a reader synchronises with.
        region
            .buffer
            .store_i32_release(offset + layout::ERROR_LOG_LENGTH_OFFSET, length_i32)
            .expect("capacity was checked before the write");

        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(align(64))]
    struct Region(Vec<u8>);

    impl Region {
        fn zeroed(len: usize) -> Self {
            Self(vec![0u8; len])
        }

        fn buffer(&self) -> AtomicBuffer<'_> {
            AtomicBuffer::from_slice(&self.0).expect("aligned region")
        }

        fn writable(&mut self) -> ErrorLogRegion<'_> {
            ErrorLogRegion::new(AtomicBuffer::from_slice_mut(&mut self.0).expect("aligned region"))
        }

        fn reader(&self) -> ErrorLogReader<'_> {
            ErrorLogReader::new(self.buffer())
        }

        fn i32_at(&self, offset: usize) -> i32 {
            i32::from_le_bytes(self.0[offset..offset + 4].try_into().unwrap())
        }

        fn put_i32(&mut self, offset: usize, v: i32) {
            self.0[offset..offset + 4].copy_from_slice(&v.to_le_bytes());
        }

        fn put_i64(&mut self, offset: usize, v: i64) {
            self.0[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
        }
    }

    /// Append one entry at `offset` and return the next free offset.
    fn append(region: &mut Region, offset: usize, text: &str, count: i32, last_ms: i64) -> usize {
        let length = layout::ERROR_LOG_HEADER_LENGTH + text.len();
        region.put_i32(offset + layout::ERROR_LOG_LENGTH_OFFSET, length as i32);
        region.put_i32(offset + layout::ERROR_LOG_OBSERVATION_COUNT_OFFSET, count);
        region.put_i64(offset + layout::ERROR_LOG_FIRST_TIMESTAMP_OFFSET, 1_000);
        region.put_i64(offset + layout::ERROR_LOG_LAST_TIMESTAMP_OFFSET, last_ms);
        region.0[offset + layout::ERROR_LOG_HEADER_LENGTH
            ..offset + layout::ERROR_LOG_HEADER_LENGTH + text.len()]
            .copy_from_slice(text.as_bytes());
        offset + layout::align_up(length, layout::ERROR_LOG_RECORD_ALIGNMENT)
    }

    #[test]
    fn an_untouched_log_has_nothing_in_it() {
        let region = Region::zeroed(4 * layout::ERROR_LOG_HEADER_LENGTH);
        let reader = ErrorLogReader::new(region.buffer());

        assert!(!reader.has_entries());
        let mut out = Vec::new();
        assert_eq!(ErrorLogScan::default(), reader.read(i64::MIN, &mut out));
        assert!(out.is_empty());
    }

    #[test]
    fn reads_entries_in_write_order() {
        let mut region = Region::zeroed(512);
        let next = append(&mut region, 0, "first problem", 3, 5_000);
        let _ = append(&mut region, next, "second problem", 1, 9_000);

        let reader = ErrorLogReader::new(region.buffer());
        assert!(reader.has_entries());

        let mut out = Vec::new();
        let scan = reader.read(i64::MIN, &mut out);

        assert_eq!(2, scan.entries);
        assert_eq!(0, scan.malformed);
        assert!(!scan.truncated);
        assert_eq!("first problem", out[0].text);
        assert_eq!(3, out[0].observation_count);
        assert_eq!(1_000, out[0].first_observation_timestamp_ms);
        assert_eq!(5_000, out[0].last_observation_timestamp_ms);
        assert_eq!("second problem", out[1].text);
    }

    #[test]
    fn filters_on_the_last_observation_time() {
        let mut region = Region::zeroed(512);
        let next = append(&mut region, 0, "old", 1, 5_000);
        let _ = append(&mut region, next, "new", 1, 9_000);

        let reader = ErrorLogReader::new(region.buffer());
        let mut out = Vec::new();
        let scan = reader.read(6_000, &mut out);

        assert_eq!(1, scan.entries);
        assert_eq!(
            "new", out[0].text,
            "the filter is inclusive on last >= since"
        );
    }

    #[test]
    fn stops_at_a_length_shorter_than_its_own_header() {
        let mut region = Region::zeroed(512);
        let next = append(&mut region, 0, "fine", 1, 1);
        region.put_i32(next + layout::ERROR_LOG_LENGTH_OFFSET, 12);

        let reader = ErrorLogReader::new(region.buffer());
        let mut out = Vec::new();
        let scan = reader.read(i64::MIN, &mut out);

        assert_eq!(
            1, scan.entries,
            "the good entry before it is still reported"
        );
        assert_eq!(1, scan.malformed);
        assert_eq!(
            1,
            out.len(),
            "the scan stops rather than guessing where the next entry starts"
        );
    }

    #[test]
    fn reports_an_entry_that_runs_past_the_region() {
        let mut region = Region::zeroed(128);
        region.put_i32(layout::ERROR_LOG_LENGTH_OFFSET, 4_096);

        let reader = ErrorLogReader::new(region.buffer());
        let mut out = Vec::new();
        let scan = reader.read(i64::MIN, &mut out);

        assert!(scan.truncated);
        assert_eq!(0, scan.entries);
    }

    #[test]
    fn a_new_description_is_written_where_the_reader_finds_it() {
        let mut region = Region::zeroed(512);
        let mut log = DistinctErrorLog::new();

        {
            let writable = region.writable();
            log.record(&writable, 1_000, 11, "first problem")
                .expect("fits");
        }

        let mut out = Vec::new();
        let scan = region.reader().read(i64::MIN, &mut out);

        assert_eq!(1, scan.entries);
        // The count of a first sighting is one: the entry is written with a
        // zero count (`:136`) and the sighting is then counted against it
        // (`:200`).
        assert_eq!(1, out[0].observation_count);
        assert_eq!(1_000, out[0].first_observation_timestamp_ms);
        assert_eq!(1_000, out[0].last_observation_timestamp_ms);
        assert_eq!("first problem", out[0].text);
    }

    #[test]
    fn a_repeat_sighting_counts_rather_than_writes() {
        let mut region = Region::zeroed(512);
        let mut log = DistinctErrorLog::new();

        {
            let writable = region.writable();
            log.record(&writable, 1_000, 11, "same problem")
                .expect("fits");
            log.record(&writable, 2_000, 11, "same problem")
                .expect("fits");
        }

        let mut out = Vec::new();
        let scan = region.reader().read(i64::MIN, &mut out);

        assert_eq!(1, scan.entries);
        assert_eq!(2, out[0].observation_count);
        assert_eq!(1_000, out[0].first_observation_timestamp_ms);
        assert_eq!(2_000, out[0].last_observation_timestamp_ms);
    }

    #[test]
    fn the_same_text_under_another_code_is_its_own_entry() {
        // The code is half the de-duplication key (`:84-85`), even though it
        // never reaches the region: a reader cannot tell the two entries
        // apart, and only the process can.
        let mut region = Region::zeroed(512);
        let mut log = DistinctErrorLog::new();

        {
            let writable = region.writable();
            log.record(&writable, 1_000, 11, "same problem")
                .expect("fits");
            log.record(&writable, 2_000, 3, "same problem")
                .expect("fits");
        }

        let mut out = Vec::new();
        assert_eq!(2, region.reader().read(i64::MIN, &mut out).entries);
        assert_eq!(1, out[0].observation_count);
        assert_eq!(1, out[1].observation_count);
    }

    #[test]
    fn a_longer_text_containing_a_remembered_one_counts_against_it() {
        // The remembered text is a prefix of the new one — `strncmp` with the
        // *stored* length (`:85`) — so a more specific failure of the same
        // thing still counts, and the region keeps the shorter text.
        let mut region = Region::zeroed(512);
        let mut log = DistinctErrorLog::new();

        {
            let writable = region.writable();
            log.record(&writable, 1_000, 11, "failed to X")
                .expect("fits");
            log.record(&writable, 2_000, 11, "failed to X for reason Y")
                .expect("fits");
        }

        let mut out = Vec::new();
        assert_eq!(1, region.reader().read(i64::MIN, &mut out).entries);
        assert_eq!(2, out[0].observation_count);
        assert_eq!("failed to X", out[0].text);
    }

    #[test]
    fn entries_are_eight_byte_aligned_so_a_reader_can_step() {
        // "12345" is a length-29 entry; the next one starts at 32, not 29, or
        // a reader walking by `length` would land mid-field
        // (`AERON_ERROR_LOG_RECORD_ALIGNMENT`,
        // `aeron_distinct_error_log.h:38-39`).
        let mut region = Region::zeroed(512);
        let mut log = DistinctErrorLog::new();

        {
            let writable = region.writable();
            log.record(&writable, 1_000, 11, "12345").expect("fits");
            log.record(&writable, 2_000, 11, "another problem")
                .expect("fits");
        }

        assert_eq!(29, region.i32_at(layout::ERROR_LOG_LENGTH_OFFSET));
        assert_eq!(
            layout::ERROR_LOG_HEADER_LENGTH as i32 + "another problem".len() as i32,
            region.i32_at(32 + layout::ERROR_LOG_LENGTH_OFFSET)
        );

        let mut out = Vec::new();
        assert_eq!(2, region.reader().read(i64::MIN, &mut out).entries);
    }

    #[test]
    fn an_entry_that_does_not_fit_is_refused_whole() {
        // Thirty-two bytes hold exactly an eight-character description — the
        // reference's test is `(offset + length) > capacity` (`:127`) — so a
        // ninth character is refused and writes nothing. A later, fitting
        // entry still lands, because the refusal leaves `next_offset` alone.
        let mut region = Region::zeroed(32);
        let mut log = DistinctErrorLog::new();

        {
            let writable = region.writable();
            assert_eq!(
                Err(RecordError::Unrecordable {
                    description: "nine char!".to_owned()
                }),
                log.record(&writable, 1_000, 11, "nine char!")
            );
        }

        assert_eq!(
            0,
            region.i32_at(layout::ERROR_LOG_LENGTH_OFFSET),
            "nothing was written"
        );

        {
            let writable = region.writable();
            log.record(&writable, 2_000, 11, "12345678")
                .expect("fits exactly");
        }

        let mut out = Vec::new();
        assert_eq!(1, region.reader().read(i64::MIN, &mut out).entries);
        assert_eq!("12345678", out[0].text);
    }

    #[test]
    fn a_fresh_log_does_not_resume_after_a_regions_existing_entries() {
        // The table is process state and the initialiser reads nothing back
        // (`:52` stores `next_offset = 0` unconditionally). A driver never
        // hands this log a dirty region — it owns the CnC file's creation —
        // so what this asserts is precisely that the write side has no
        // fallback scan: the entry goes to offset zero, over the old one.
        let mut region = Region::zeroed(512);
        let _ = append(&mut region, 0, "a previous life", 7, 5_000);

        let mut log = DistinctErrorLog::new();
        {
            let writable = region.writable();
            log.record(&writable, 9_000, 11, "a new problem")
                .expect("fits");
        }

        assert_eq!(
            layout::ERROR_LOG_HEADER_LENGTH as i32 + "a new problem".len() as i32,
            region.i32_at(layout::ERROR_LOG_LENGTH_OFFSET),
            "the new entry starts at offset zero, over the old one"
        );
    }
}
