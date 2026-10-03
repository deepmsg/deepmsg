//! The loss report: a file in the aeron directory that a driver writes when an
//! image loses data, and that any client reads.
//!
//! It is a fixed-length file mapped by both ends — the driver creates it at
//! startup and appends one record per stream that lost something, and a reader
//! walks the records until one says there are no more. The name is the
//! reference's (`AERON_LOSS_REPORT_FILE`, `reports/aeron_loss_reporter.h:28`)
//! and so is every byte of the layout below, because what reads it is not this
//! build's code but the reference's own: `LossStat`, the C client's
//! `aeron_cnc_loss_reporter_read` and Java's `LossReportReader`.
//!
//! # The record
//!
//! `aeron_loss_reporter_entry_t` is `#pragma pack(4)` (`:30-42`), so a record
//! is a forty-byte header with no padding inside it, then the channel and the
//! source each behind a four-byte length, and the whole record is then rounded
//! up to `AERON_LOSS_REPORTER_ENTRY_ALIGNMENT` — one cache line (`:44`):
//!
//! ```text
//! offset  size  field
//!      0     8  observation_count      (written last: it is the record's gate)
//!      8     8  total_bytes_lost
//!     16     8  first_observation_timestamp   (epoch milliseconds)
//!     24     8  last_observation_timestamp
//!     32     4  session_id
//!     36     4  stream_id
//!     40     4  channel_length
//!     44     n  channel, padded to four bytes
//!   44+a     4  source_length
//!   48+a     m  source
//! ```
//!
//! # What ends the walk
//!
//! A record's `observation_count` is stored **after** everything else and with
//! a release (`aeron_loss_reporter.c:66-68`), and the buffer is zero to begin
//! with — so a reader that finds a count that is not positive has found the
//! end, which is what both readers do (`:120-126`, `LossReportReader.java:119-127`).
//! Nothing else marks the end: there is no count of records anywhere.
//!
//! # The two readers disagree, and the writer decides
//!
//! The stride a record occupies is `40 + align4(4 + channel) + 4 + source`,
//! rounded up to 64 — that is what the **writer** advances by
//! (`aeron_loss_reporter.c:44-46`) and what Java's reader uses
//! (`LossReportReader.java:143-147`). The **C reader** computes it as
//! `40 + 8 + channel + source` and rounds that up (`:157-159`), which is the
//! same number whenever the channel's four-byte padding does not push the
//! record across a 64-byte boundary and a different one when it does. This
//! build writes what the writer writes and reads what the writer's own
//! arithmetic says; the C reader's difference is the reference's, is recorded
//! in `docs/compat.md`, and does not show up for the lengths a real channel URI
//! and source identity have.

use std::io;
use std::path::{Path, PathBuf};

use deepmsg_core::buffer::{AtomicBuffer, ReadOnly, ReadWrite};
use deepmsg_core::pal::MappedFile;

/// The file's name in the aeron directory (`AERON_LOSS_REPORT_FILE`).
pub const LOSS_REPORT_FILE_NAME: &str = "loss-report.dat";

/// The stride a record is rounded up to (`AERON_LOSS_REPORTER_ENTRY_ALIGNMENT`
/// is `AERON_CACHE_LINE_LENGTH`).
pub const ENTRY_ALIGNMENT: usize = 64;

/// The fixed part of a record: `sizeof(aeron_loss_reporter_entry_t)`, which is
/// 40 under `#pragma pack(4)` and 40 again on any ABI the reference builds for.
pub const ENTRY_LENGTH: usize = 40;

/// The offset the channel's bytes start at — the header plus its length.
const CHANNEL_OFFSET: usize = ENTRY_LENGTH + 4;

/// What the driver reports about one stream's loss, and what a reader gets
/// back.
///
/// The timestamps are **epoch milliseconds** — the reference passes
/// `image->epoch_clock()` into `aeron_loss_reporter_create_entry` and
/// `LossStat` formats them as dates — which is why this is not the monotonic
/// clock the rest of the driver counts in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LossReportEntry {
    /// How many times loss was reported for this stream. It is the record's
    /// gate: a reader stops at the first one that is not positive.
    pub observation_count: i64,
    /// Every byte every observation accounted for.
    pub total_bytes_lost: i64,
    /// When the first one was reported.
    pub first_observation_timestamp: i64,
    /// When the last one was.
    pub last_observation_timestamp: i64,
    /// The session the loss was seen on.
    pub session_id: i32,
    /// The stream it was seen on.
    pub stream_id: i32,
    /// The channel the image was reading, as the client wrote it.
    pub channel: Vec<u8>,
    /// Who sent it, formatted the way a source identity is.
    pub source: Vec<u8>,
}

impl LossReportEntry {
    /// How many bytes this record takes in the file, padding included — the
    /// writer's own arithmetic (`aeron_loss_reporter.c:44-46`).
    pub fn record_length(&self) -> usize {
        align(
            CHANNEL_OFFSET + align(4 + self.channel.len(), 4) + 4 + self.source.len(),
            ENTRY_ALIGNMENT,
        )
    }

    /// Write the record at `offset`.
    ///
    /// The order is the contract: everything else first, and
    /// `observation_count` last with a release, so a reader that can see a
    /// positive count can see a whole record (`aeron_loss_reporter.c:44-68`).
    ///
    /// # Returns
    ///
    /// `None` when `offset` plus the record does not fit the buffer, which is
    /// the reference's `ENOMEM` arm (`:47-50`).
    pub fn encode(&self, buffer: &AtomicBuffer<'_, ReadWrite>, offset: usize) -> Option<()> {
        i32::try_from(self.channel.len()).ok()?;
        i32::try_from(self.source.len()).ok()?;

        buffer.store_i64_relaxed(offset + 8, self.total_bytes_lost)?;
        buffer.store_i64_relaxed(offset + 16, self.first_observation_timestamp)?;
        buffer.store_i64_relaxed(offset + 24, self.last_observation_timestamp)?;
        buffer.store_i32_relaxed(offset + 32, self.session_id)?;
        buffer.store_i32_relaxed(offset + 36, self.stream_id)?;

        buffer.store_i32_relaxed(offset + 40, i32::try_from(self.channel.len()).ok()?)?;
        buffer.copy_in(offset + CHANNEL_OFFSET, &self.channel)?;

        let source_length_offset = offset + CHANNEL_OFFSET + align(self.channel.len(), 4);
        buffer.store_i32_relaxed(source_length_offset, i32::try_from(self.source.len()).ok()?)?;
        buffer.copy_in(source_length_offset + 4, &self.source)?;

        // Last, and with a release: this is the byte a reader tests.
        buffer.store_i64_release(offset, self.observation_count)
    }

    /// Read the record at `offset`.
    ///
    /// # Returns
    ///
    /// `None` when the record does not fit the buffer, or when its lengths are
    /// not a record's — a torn or foreign file is a `None`, not a panic.
    pub fn decode(buffer: &AtomicBuffer<'_, ReadOnly>, offset: usize) -> Option<Self> {
        let channel_length = usize::try_from(buffer.load_i32(offset + 40)?).ok()?;
        let mut channel = vec![0_u8; channel_length];
        buffer.copy_out(offset + CHANNEL_OFFSET, &mut channel)?;

        let source_length_offset = offset + CHANNEL_OFFSET + align(channel_length, 4);
        let source_length = usize::try_from(buffer.load_i32(source_length_offset)?).ok()?;
        let mut source = vec![0_u8; source_length];
        buffer.copy_out(source_length_offset + 4, &mut source)?;

        Some(Self {
            observation_count: buffer.load_i64_acquire(offset)?,
            total_bytes_lost: buffer.load_i64_acquire(offset + 8)?,
            first_observation_timestamp: buffer.load_i64(offset + 16)?,
            last_observation_timestamp: buffer.load_i64_acquire(offset + 24)?,
            session_id: buffer.load_i32(offset + 32)?,
            stream_id: buffer.load_i32(offset + 36)?,
            channel,
            source,
        })
    }
}

/// Record one more observation on the entry at `offset`
/// (`aeron_loss_reporter_record_observation`, `:71-107`).
///
/// The bytes go in before the count, and the timestamp before both, so a
/// reader that sees the new count sees the new totals.
///
/// # Returns
///
/// `None` when the offsets are not in the buffer.
pub fn record_observation(
    buffer: &AtomicBuffer<'_, ReadWrite>,
    offset: usize,
    bytes_lost: i64,
    timestamp_ms: i64,
) -> Option<()> {
    buffer.store_i64_release(offset + 24, timestamp_ms)?;
    buffer.fetch_add_i64(offset + 8, bytes_lost)?;
    buffer.fetch_add_i64(offset, 1)?;

    Some(())
}

/// Every record the buffer holds, in the order they were written.
///
/// The walk stops at the first `observation_count` that is not positive, which
/// is the end of the records rather than an error — the file is zeroed when it
/// is created and the driver only ever appends.
pub fn read_all(buffer: &AtomicBuffer<'_, ReadOnly>) -> Vec<LossReportEntry> {
    let mut entries = Vec::new();
    let mut offset = 0;
    // The window is the whole file: `LossReportFile` maps it in one piece, and
    // a reader that was handed a slice of it would be reading a file with a
    // different length.
    let capacity = buffer.len();

    while offset + ENTRY_LENGTH <= capacity {
        let Some(count) = buffer.load_i64_acquire(offset) else {
            break;
        };

        if count <= 0 {
            break;
        }

        let Some(entry) = LossReportEntry::decode(buffer, offset) else {
            break;
        };

        offset += entry.record_length();
        entries.push(entry);
    }

    entries
}

/// The mapped loss report file: what the driver writes and what a client maps
/// to read.
///
/// The file is created **exclusively** and at a length fixed by the driver's
/// configuration, exactly as the reference creates it
/// (`aeron_map_new_file(..., fill_with_zeroes = true, page_size)`,
/// `aeron-driver/src/main/c/aeron_driver.c:324-345`, over an `O_CREAT|O_EXCL`
/// open at `util/aeron_fileutil.c:967`). A file already in the directory is a
/// driver already using it, and this says so rather than overwriting it.
pub struct LossReportFile {
    mapping: MappedFile,
    path: PathBuf,
}

impl LossReportFile {
    /// Create and map the file in `directory`.
    ///
    /// `length` is the caller's to round: the reference aligns the configured
    /// length up to the file page size before creating
    /// (`aeron_driver.c:329-330`).
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the file cannot be created — including
    /// [`io::ErrorKind::AlreadyExists`] for one that is already there — or
    /// mapped.
    pub fn create(directory: &Path, length: usize) -> io::Result<Self> {
        let path = directory.join(LOSS_REPORT_FILE_NAME);
        let mapping = MappedFile::create(&path, length)?;

        Ok(Self { mapping, path })
    }

    /// Map a file a driver already created.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if the file does not exist or cannot be mapped.
    pub fn open_readonly(directory: &Path) -> io::Result<Self> {
        let path = directory.join(LOSS_REPORT_FILE_NAME);
        let mapping = MappedFile::open_readonly(&path)?;

        Ok(Self { mapping, path })
    }

    /// Where the file is.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// How long it is — the length the driver configured, aligned.
    pub fn length(&self) -> usize {
        self.mapping.len()
    }

    /// The whole file, writable: the driver's side.
    pub fn writable(&self) -> Option<AtomicBuffer<'_, ReadWrite>> {
        self.mapping.region_mut(0, self.mapping.len())
    }

    /// The whole file, read-only: a reader's side.
    pub fn readonly(&self) -> Option<AtomicBuffer<'_, ReadOnly>> {
        self.mapping.region(0, self.mapping.len())
    }

    /// Every record in the file.
    pub fn entries(&self) -> Vec<LossReportEntry> {
        self.readonly()
            .map_or_else(Vec::new, |buffer| read_all(&buffer))
    }
}

/// `value` rounded up to the next multiple of `alignment`, which is a power of
/// two (`AERON_ALIGN`).
const fn align(value: usize, alignment: usize) -> usize {
    value.div_ceil(alignment) * alignment
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepmsg_core::buffer::AtomicBuffer;

    /// A directory of its own in the system temp directory, removed on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("deepmsg-loss-report-{}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("create temp dir");

            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[repr(align(64))]
    struct Region(Vec<u8>);

    impl Region {
        fn zeroed(length: usize) -> Self {
            Self(vec![0_u8; length])
        }

        fn writable(&mut self) -> AtomicBuffer<'_, ReadWrite> {
            AtomicBuffer::from_slice_mut(&mut self.0).expect("aligned")
        }

        fn readonly(&self) -> AtomicBuffer<'_, ReadOnly> {
            AtomicBuffer::from_slice(&self.0).expect("aligned")
        }
    }

    fn entry(index: i64) -> LossReportEntry {
        LossReportEntry {
            observation_count: 1,
            total_bytes_lost: 128 * index,
            first_observation_timestamp: 1_700_000_000_000 + index,
            last_observation_timestamp: 1_700_000_000_000 + index,
            session_id: -1_234_567,
            stream_id: 1001,
            channel: b"aeron:udp?endpoint=localhost:20111".to_vec(),
            source: b"localhost:20111".to_vec(),
        }
    }

    /// The layout is the reference's `#pragma pack(4)` struct, byte for byte:
    /// a forty-byte header, the channel behind a four-byte length, the source
    /// behind its own, and the record rounded up to a cache line.
    #[test]
    fn a_record_is_the_references_forty_bytes_and_a_stride() {
        let mut region = Region::zeroed(1024);
        let buffer = region.writable();
        let entry = entry(1);

        entry.encode(&buffer, 0).expect("it fits");

        assert_eq!(40, ENTRY_LENGTH);
        assert_eq!(Some(1), buffer.load_i64(0), "observation_count is first");
        assert_eq!(Some(128), buffer.load_i64(8), "total_bytes_lost");
        assert_eq!(
            Some(1_700_000_000_001),
            buffer.load_i64(16),
            "first timestamp"
        );
        assert_eq!(
            Some(1_700_000_000_001),
            buffer.load_i64(24),
            "last timestamp"
        );
        assert_eq!(Some(-1_234_567), buffer.load_i32(32), "session_id");
        assert_eq!(Some(1001), buffer.load_i32(36), "stream_id");
        assert_eq!(Some(34), buffer.load_i32(40), "channel_length");

        let mut channel = [0_u8; 34];
        buffer.copy_out(44, &mut channel).expect("the channel");
        assert_eq!(b"aeron:udp?endpoint=localhost:20111", &channel);

        // 44 + align4(34) = 80, so the source's length is there and its bytes
        // after it.
        assert_eq!(Some(15), buffer.load_i32(80), "source_length");
        let mut source = [0_u8; 15];
        buffer.copy_out(84, &mut source).expect("the source");
        assert_eq!(b"localhost:20111", &source);

        // And the record's stride is that, rounded to 64: 44 + 36 + 4 + 15 = 99.
        assert_eq!(128, entry.record_length());
    }

    /// A record can be written and read back, and the reader walks several in
    /// a row at the strides the writer made.
    #[test]
    fn records_round_trip_and_are_walked_in_order() {
        let mut region = Region::zeroed(4096);
        let buffer = region.writable();

        let mut offset = 0;
        for index in 1..=3 {
            let entry = entry(index);
            entry.encode(&buffer, offset).expect("it fits");
            offset += entry.record_length();
        }

        assert_eq!(384, offset, "three records of 128 bytes");

        let readonly = region.readonly();
        let entries = read_all(&readonly);
        assert_eq!(vec![entry(1), entry(2), entry(3)], entries);
    }

    /// `read_all` stops at the first count that is not positive: the file is
    /// zeroed and the driver only appends, so a zero is the end of the records
    /// and not a torn one.
    #[test]
    fn a_zeroed_record_ends_the_walk() {
        let mut region = Region::zeroed(4096);
        let buffer = region.writable();
        entry(1).encode(&buffer, 0).expect("it fits");

        let readonly = region.readonly();
        assert_eq!(vec![entry(1)], read_all(&readonly));

        // The second slot is zero, and stays the end however much room is left.
        assert_eq!(0, readonly.load_i64(128).expect("the second slot"));
        assert_eq!(1, read_all(&readonly).len());
    }

    /// An observation adds its bytes and its count, and moves the last
    /// timestamp — and leaves the first one alone.
    #[test]
    fn an_observation_adds_bytes_and_count() {
        let mut region = Region::zeroed(1024);
        let buffer = region.writable();
        entry(1).encode(&buffer, 0).expect("it fits");

        record_observation(&buffer, 0, 512, 1_700_000_009_999).expect("in the buffer");

        let readonly = region.readonly();
        let entries = read_all(&readonly);
        assert_eq!(2, entries[0].observation_count);
        assert_eq!(128 + 512, entries[0].total_bytes_lost);
        assert_eq!(1_700_000_000_001, entries[0].first_observation_timestamp);
        assert_eq!(1_700_000_009_999, entries[0].last_observation_timestamp);
    }

    /// A record whose bytes do not fit is not written at all — the reference's
    /// `ENOMEM` arm, which is what makes a full buffer a driver that stops
    /// reporting rather than one that writes half a record.
    #[test]
    fn a_record_that_does_not_fit_is_refused() {
        let mut region = Region::zeroed(64);
        let buffer = region.writable();

        assert!(entry(1).encode(&buffer, 0).is_none(), "128 does not fit 64");
        assert_eq!(0, buffer.load_i64(0).expect("nothing was written"));
    }

    /// The file is created at the length it is given, zero-filled, and a
    /// reader maps it back.
    #[test]
    fn the_file_is_created_zeroed_and_read_back() {
        let dir = TempDir::new();
        let file = LossReportFile::create(dir.path(), 4096).expect("created");

        assert_eq!(4096, file.length());
        assert!(file.path().ends_with(LOSS_REPORT_FILE_NAME));

        {
            let buffer = file.writable().expect("writable");
            entry(1).encode(&buffer, 0).expect("it fits");
        }

        let reopened = LossReportFile::open_readonly(dir.path()).expect("reopened");
        assert_eq!(vec![entry(1)], reopened.entries());

        // And it is exclusive, like the reference's `O_CREAT|O_EXCL`: a second
        // driver finds the file rather than taking it over.
        let second = LossReportFile::create(dir.path(), 4096);
        assert_eq!(
            Some(io::ErrorKind::AlreadyExists),
            second.err().map(|error| error.kind())
        );
    }
}
