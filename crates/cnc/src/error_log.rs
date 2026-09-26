//! Reading the distinct error log.
//!
//! The driver keeps a de-duplicated log of the errors it has seen, and the
//! reference client exposes it so a tool can show why a driver is unhappy
//! without reading its stderr. Entries are appended once and then only
//! counted, so the log is a fixed sequence in write order.
//!
//! Mirrors `aeron-client/src/main/c/concurrent/aeron_distinct_error_log.c:216-257`.

use deepmsg_core::buffer::AtomicBuffer;

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
}
