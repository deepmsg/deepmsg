//! Where a recording's frames go, and how long it took to put them there.
//!
//! `RecordingWriter.java` is two things in one class, and this module is the
//! half that [`crate::segment`] is not. The segment writer owns the *file*: its
//! name, its length, the rollover, the three things a written block has to do
//! (a padding frame stays a header, a checksum lands in the session-id field, a
//! sync level decides whether it is forced). What is left is the **session's**
//! half, which is what this adds:
//!
//! * the recording's position, `segmentBasePosition + segmentOffset`
//!   (`RecordingWriter.java:183-186`) — the number the
//!   [`recording_pos`](crate::server::recording_pos) counter publishes;
//! * **timing** every block, and reporting the two numbers the recorder's
//!   counters are fed from (`:117`, `:143-145`);
//! * the split the reference has between *construction*, which cannot fail, and
//!   `init()`, which opens the file and can (`:188-196` over
//!   `RecordingSession.java:204-217`).
//!
//! # Why `init` is a separate step and not a constructor
//!
//! Because the reference's session catches its failure and tells the client
//! (`RecordingSession.java:206-216`): an `IOException` out of `init` becomes the
//! session's `errorMessage` and a `STOP` signal, not a panic in whichever turn
//! happened to be running. A constructor that can fail would have to be caught
//! by its caller anyway, and this way there is one place that decides what a
//! failed recording write means.

use std::fmt;
use std::path::{Path, PathBuf};

use deepmsg_core::clock::monotonic_nano_time;

use crate::checksum::Checksum;
use crate::segment::{SegmentError, SegmentSpec, SegmentWriter};

/// What a write tells the archive's recorder, so that the three counters it
/// publishes (105, 106, 107) get their numbers
/// (`ArchiveConductor.Recorder.bytesWritten` and `writeTimeNs`,
/// `ArchiveConductor.java:2717-2730`).
///
/// A trait rather than a counter handle because the reference separates the two
/// deliberately: `RecordingWriter` reports, and the `Recorder` is what keeps
/// the running totals and decides when they are published (`:2732-2743`). The
/// writer has no business knowing about a counter.
pub trait WriteStats {
    /// The bytes a block put in the file — the reference's `dataLength`, which
    /// for a padding frame is its header and nothing else
    /// (`RecordingWriter.java:114`, `:144`).
    fn bytes_written(&mut self, bytes: u64);

    /// How long that block's write took (`:145`).
    fn write_time_ns(&mut self, nanos: u64);
}

/// Nothing to report to, for a writer whose numbers nobody counts.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoStats;

impl WriteStats for NoStats {
    fn bytes_written(&mut self, _bytes: u64) {}

    fn write_time_ns(&mut self, _nanos: u64) {}
}

/// What can go wrong writing a recording.
#[derive(Debug)]
pub enum RecordingWriterError {
    /// The writer was asked to write before [`RecordingWriter::init`] or after
    /// [`RecordingWriter::close`], so there is no segment to write into.
    ///
    /// The reference cannot meet this — its `RecordingWriter` holds the channel
    /// and a closed one throws — and it is named here rather than panicked
    /// because the caller is a session that has its own idea of when it is
    /// recording, and a session whose two ideas disagree should say so.
    NotStarted,
    /// The segment: a file system error, or the next segment being in the way.
    Segment(SegmentError),
}

impl fmt::Display for RecordingWriterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotStarted => write!(f, "recording writer has no open segment"),
            Self::Segment(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for RecordingWriterError {}

impl From<SegmentError> for RecordingWriterError {
    fn from(error: SegmentError) -> Self {
        Self::Segment(error)
    }
}

/// One recording's writer: one segment at a time, rolled over when it fills.
pub struct RecordingWriter {
    directory: PathBuf,
    recording_id: i64,
    start_position: i64,
    segment_length: usize,
    term_buffer_length: i32,
    file_sync_level: i32,
    checksum: Option<Checksum>,
    /// The segment being written, from [`RecordingWriter::init`] until
    /// [`RecordingWriter::close`].
    segment: Option<SegmentWriter>,
    /// Where in the stream the writer has got to, which is the open segment's
    /// base plus its offset (`RecordingWriter.java:183-186`).
    ///
    /// Kept rather than asked of the segment every time, because the answer has
    /// to survive a rollover, which is a different segment.
    position: i64,
}

impl RecordingWriter {
    /// Describe a writer. Nothing is opened until [`RecordingWriter::init`].
    ///
    /// `start_position` is where the *recording* began and `join_position`
    /// where this writer joins it; they differ for a recording that started
    /// late in a term, and the difference is the first segment's offset
    /// (`RecordingWriter.java:98-101`).
    #[must_use]
    #[allow(clippy::too_many_arguments)] // one per thing the reference's constructor takes
    pub fn new(
        directory: &Path,
        recording_id: i64,
        start_position: i64,
        join_position: i64,
        term_buffer_length: i32,
        segment_length: usize,
        file_sync_level: i32,
        checksum: Option<Checksum>,
    ) -> Self {
        Self {
            directory: directory.to_path_buf(),
            recording_id,
            start_position,
            segment_length,
            term_buffer_length,
            file_sync_level,
            checksum,
            segment: None,
            // Before anything is written the recording is exactly where it
            // joined: the first segment's base plus its offset is the join
            // position by construction.
            position: join_position,
        }
    }

    /// Open the first segment (`RecordingWriter.init`, `:188-196`).
    ///
    /// # Errors
    ///
    /// [`SegmentError::Exists`] when a segment of this recording is already
    /// there — a recording does not overwrite one — and [`SegmentError::Io`]
    /// for the file system.
    pub fn init(&mut self) -> Result<(), SegmentError> {
        let segment = SegmentWriter::create(
            &self.directory,
            SegmentSpec {
                recording_id: self.recording_id,
                start_position: self.start_position,
                join_position: self.position,
                term_buffer_length: self.term_buffer_length,
                segment_length: self.segment_length,
            },
            self.file_sync_level,
            self.checksum,
        )?;

        self.segment = Some(segment);

        Ok(())
    }

    /// Where in the stream the recording has got to
    /// (`RecordingWriter.position`, `:183-186`).
    ///
    /// This is the number a recording position counter publishes
    /// (`RecordingSession.java:240`).
    #[must_use]
    pub const fn position(&self) -> i64 {
        self.position
    }

    /// The segment being written, if one is open.
    #[must_use]
    pub fn path(&self) -> Option<PathBuf> {
        self.segment.as_ref().map(SegmentWriter::path)
    }

    /// Write one block of frames, and report what it cost.
    ///
    /// The clock runs around the write **and its sync**, which is what the
    /// reference measures (`:117` before, `:143` after the `force`): the number
    /// is what the file system cost, not what the syscall did.
    ///
    /// # Errors
    ///
    /// [`RecordingWriterError::NotStarted`] with no segment open, and
    /// [`RecordingWriterError::Segment`] for whatever the segment writer
    /// refuses — a malformed block, or the next segment being in the way.
    pub fn write_block<S: WriteStats>(
        &mut self,
        block: &[u8],
        stats: &mut S,
    ) -> Result<(), RecordingWriterError> {
        let Some(segment) = self.segment.as_mut() else {
            return Err(RecordingWriterError::NotStarted);
        };

        let start_ns = monotonic_nano_time();
        let written = segment.write_block(block)?;
        let elapsed = monotonic_nano_time() - start_ns;

        self.position = segment.segment_base_position() + segment.offset() as i64;

        stats.bytes_written(written);
        // A clock that went backwards would make this negative, and the
        // counter it is about to feed takes an unsigned — so it counts as
        // having taken no time, which is also what the reference's own
        // `NanoClock` cannot produce.
        stats.write_time_ns(u64::try_from(elapsed).unwrap_or(0));

        Ok(())
    }

    /// Close the segment being written (`RecordingWriter.close`, `:173-181`).
    ///
    /// Idempotent, as the reference's is: a session that has ended may be
    /// closed again by the conductor that collected it.
    pub fn close(&mut self) {
        self.segment = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    use deepmsg_core::logbuffer::frame::{
        DATA_HEADER_LENGTH, FRAME_LENGTH_OFFSET, TYPE_DATA, TYPE_OFFSET, TYPE_PAD,
    };

    const RECORDING_ID: i64 = 7;
    const TERM_LENGTH: i32 = 64 * 1024;
    const SEGMENT_LENGTH: usize = 128 * 1024;

    /// What the recorder would have been told.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct Tally {
        bytes: u64,
        writes: u64,
        nanos: u64,
    }

    impl WriteStats for Tally {
        fn bytes_written(&mut self, bytes: u64) {
            self.bytes += bytes;
            self.writes += 1;
        }

        fn write_time_ns(&mut self, nanos: u64) {
            self.nanos += nanos;
        }
    }

    /// One data frame of `length` bytes, as `Image::block_poll` hands a block
    /// over — the same shape `segment`'s own tests build.
    fn frame(term_offset: i32, term_id: i32, length: usize) -> Vec<u8> {
        let mut bytes = vec![0_u8; length];

        bytes[FRAME_LENGTH_OFFSET..FRAME_LENGTH_OFFSET + 4]
            .copy_from_slice(&i32::try_from(length).expect("small").to_le_bytes());
        bytes[TYPE_OFFSET..TYPE_OFFSET + 2].copy_from_slice(&TYPE_DATA.to_le_bytes());
        bytes[8..12].copy_from_slice(&term_offset.to_le_bytes());
        bytes[12..16].copy_from_slice(&7_i32.to_le_bytes());
        bytes[16..20].copy_from_slice(&1001_i32.to_le_bytes());
        bytes[20..24].copy_from_slice(&term_id.to_le_bytes());

        bytes
    }

    /// A padding frame, which is what a term's short tail is written as.
    ///
    /// Its body is deliberately **not** zeroes, as `segment`'s own fixture puts
    /// it: the file was preallocated with zeroes, so a zeroed body could not
    /// tell "the header alone was written" from "the whole frame was".
    fn padding(length: usize) -> Vec<u8> {
        let mut bytes = vec![0xAB_u8; length];

        bytes[FRAME_LENGTH_OFFSET..FRAME_LENGTH_OFFSET + 4]
            .copy_from_slice(&i32::try_from(length).expect("small").to_le_bytes());
        bytes[TYPE_OFFSET..TYPE_OFFSET + 2].copy_from_slice(&TYPE_PAD.to_le_bytes());

        bytes
    }

    fn directory(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("deepmsg-recording-writer-{name}"));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("a directory");

        path
    }

    fn writer(directory: &Path, join_position: i64) -> RecordingWriter {
        RecordingWriter::new(
            directory,
            RECORDING_ID,
            0,
            join_position,
            TERM_LENGTH,
            SEGMENT_LENGTH,
            0,
            None,
        )
    }

    /// A recording that starts at the beginning of a term writes into the
    /// segment named for position zero.
    #[test]
    fn the_first_segment_is_the_one_the_recording_joined_in() {
        let directory = directory("first-segment");
        let mut writer = writer(&directory, 0);

        assert_eq!(0, writer.position(), "nothing written yet");
        writer.init().expect("opens");

        assert_eq!(
            directory.join("7-0.rec"),
            writer.path().expect("a segment"),
            "the reference names a segment <recordingId>-<basePosition>.rec"
        );
    }

    /// A recording that joined late is written from where it joined, and its
    /// position starts there rather than at zero
    /// (`RecordingWriter.java:98-101`).
    #[test]
    fn a_recording_that_joined_late_starts_where_it_joined() {
        let directory = directory("late-join");
        let join = 4096;
        let mut writer = writer(&directory, join);

        assert_eq!(
            join,
            writer.position(),
            "no bytes written, and already four kilobytes in"
        );
        writer.init().expect("opens");

        let mut tally = Tally::default();
        writer
            .write_block(&frame(join as i32, 0, 64), &mut tally)
            .expect("written");

        assert_eq!(join + 64, writer.position());
        assert_eq!(64, tally.bytes);
        assert_eq!(1, tally.writes);
    }

    /// The tally feeds the three counters, so the byte count has to be what was
    /// **written** rather than the block's length — the two differ by the
    /// padding rule (`RecordingWriter.java:113-114`, `:144`).
    #[test]
    fn a_padding_frame_is_counted_as_the_header_it_was_written_as() {
        let directory = directory("padding");
        let mut writer = writer(&directory, 0);
        writer.init().expect("opens");

        let mut tally = Tally::default();
        writer
            .write_block(&padding(1024), &mut tally)
            .expect("written");

        assert_eq!(
            DATA_HEADER_LENGTH as u64, tally.bytes,
            "the header and nothing else"
        );
        assert_eq!(
            1024,
            writer.position(),
            "the position moves by the block's length, not by what was written"
        );
    }

    /// Filling a segment rolls to the next one, and the position carries across
    /// the roll (`RecordingWriter.onFileRollOver`, `:234-247`).
    #[test]
    fn filling_a_segment_rolls_to_the_next_one_and_the_position_follows() {
        let directory = directory("rollover");
        let mut writer = writer(&directory, 0);
        writer.init().expect("opens");

        // Two blocks that together exactly fill the segment.
        let half = SEGMENT_LENGTH / 2;
        let mut tally = Tally::default();
        writer
            .write_block(&frame(0, 0, half), &mut tally)
            .expect("the first half");
        writer
            .write_block(&frame(half as i32, 0, half), &mut tally)
            .expect("the second half");

        assert_eq!(
            directory.join("7-131072.rec"),
            writer.path().expect("the next segment"),
            "the base advances by exactly one segment length"
        );
        assert_eq!(SEGMENT_LENGTH as i64, writer.position());

        writer
            .write_block(&frame(0, 2, 64), &mut tally)
            .expect("written into the new segment");

        assert_eq!(SEGMENT_LENGTH as i64 + 64, writer.position());
        assert_eq!(2 * half as u64 + 64, tally.bytes);
    }

    /// A segment that is already there is refused rather than overwritten
    /// (`RecordingWriter.java:241-244`).
    #[test]
    fn a_segment_that_is_already_there_is_refused() {
        let directory = directory("already-there");

        let mut first = writer(&directory, 0);
        first.init().expect("opens");

        let mut second = writer(&directory, 0);
        let error = second.init().expect_err("the file is in the way");

        assert!(
            matches!(error, SegmentError::Exists { .. }),
            "a recording does not overwrite a segment: {error:?}"
        );
    }

    #[test]
    fn writing_before_init_or_after_close_is_not_a_write() {
        let directory = directory("not-started");
        let mut writer = writer(&directory, 0);
        let mut tally = Tally::default();

        assert!(matches!(
            writer.write_block(&frame(0, 0, 64), &mut tally),
            Err(RecordingWriterError::NotStarted)
        ));

        writer.init().expect("opens");
        writer.close();
        writer.close(); // idempotent, as the reference's is

        assert!(matches!(
            writer.write_block(&frame(0, 0, 64), &mut tally),
            Err(RecordingWriterError::NotStarted)
        ));
        assert_eq!(Tally::default(), tally, "nothing was reported");
    }

    /// The reported time is a clock reading, so the assertion that holds is the
    /// shape of it: one per write, and never a wrapped negative.
    #[test]
    fn every_write_reports_a_time() {
        let directory = directory("timing");
        let mut writer = writer(&directory, 0);
        writer.init().expect("opens");

        let mut tally = Tally::default();
        writer
            .write_block(&frame(0, 0, 64), &mut tally)
            .expect("one");
        writer
            .write_block(&frame(64, 0, 64), &mut tally)
            .expect("two");

        assert_eq!(2, tally.writes);
        assert!(tally.nanos < u64::MAX / 2, "a wrapped clock reading");
    }
}
