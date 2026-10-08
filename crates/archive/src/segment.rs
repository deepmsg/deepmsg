//! Recording segments: the `.rec` files a recorded stream is written into.
//!
//! A recording is a set of files, one per segment of the stream, named
//! `<recordingId>-<segmentBasePosition>.rec` (`Archive.segmentFileName`,
//! `Archive.java:3903-3906`). Inside one is **Aeron's own frame format** — the
//! same 32-byte data header and 32-byte alignment as an IPC or UDP term — so
//! this module is about the file, not about the frames: where a segment begins,
//! how long it is before the next one, and the three things a recording does to
//! frames on the way in that a live stream does not.
//!
//! # Where a segment begins is not a multiple of its length
//!
//! `segmentFileBasePosition` (`AeronArchive.java:195-203`) rounds **down from
//! the term the recording starts in**, not from zero:
//!
//! ```text
//! startTermBase = startPosition - (startPosition & (termLength - 1))
//! fromBase      = position - startTermBase
//! base          = startTermBase + (fromBase & !(segmentLength - 1))
//! ```
//!
//! Both masks are powers of two. A recording that starts at a position which is
//! not a multiple of the segment length therefore has a **first segment whose
//! base is not a multiple of the segment length either** — and a recording that
//! joins a stream already in progress starts partway into its first file
//! (`segmentOffset = joinPosition - segmentBasePosition`,
//! `RecordingWriter.java:100-101`). Both are why this arithmetic is a function
//! with tests rather than two lines inlined where they are used.
//!
//! # The three things a recording does to frames
//!
//! * **a padding frame is written as its header alone** — 32 bytes, with the
//!   rest of what it covers left as the zeroes the file was preallocated with
//!   (`RecordingWriter.java:112-116`). `block_poll` hands a padding frame over as
//!   its own block for exactly this reason, so the test is on the block's first
//!   frame;
//! * **a checksum goes into the frame's session-id field** — computed over the
//!   frame's payload and stored where the session id was, which is what an
//!   archive that records with a checksum writes (`:198-212`);
//! * **the offset advances by the frame's length, not by what was written**,
//!   which is the padding rule again from the other side: 32 bytes written,
//!   a whole frame consumed.
//!
//! # What a reader has to know that a writer does not
//!
//! A reader starts **wherever the caller asks**, which is usually in the middle
//! of a term and often in the middle of a segment, so it has to place a stream
//! position in a file. Three numbers do it (`RecordingReader.java:83-96`):
//!
//! ```text
//! segmentOffset        = (fromPosition - startTermBase) & (segmentLength - 1)
//! termOffset           =  fromPosition & (termLength - 1)
//! termBaseSegmentOffset = segmentOffset - termOffset
//! ```
//!
//! and the last is where the term containing that position begins *in the
//! segment*, which is what makes a term a window of the file rather than
//! something the reader has to assemble. A position it cannot verify is refused:
//! the frame at the position has to carry the term offset, the term id and the
//! stream id the caller's recording implies (`:100-107`), because a position
//! that is a few bytes off is a position that reads somebody's payload as a
//! header.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use deepmsg_core::buffer::{AtomicBuffer, ReadOnly};
use deepmsg_core::logbuffer::descriptor::FRAME_ALIGNMENT;
use deepmsg_core::logbuffer::frame::{
    DATA_HEADER_LENGTH, FLAGS_OFFSET, FRAME_LENGTH_OFFSET, RESERVED_VALUE_OFFSET,
    SESSION_ID_FIELD_OFFSET, STREAM_ID_FIELD_OFFSET, TERM_ID_FIELD_OFFSET,
    TERM_OFFSET_FIELD_OFFSET, TYPE_OFFSET, TYPE_PAD,
};
use deepmsg_core::logbuffer::position::{align_up, bits_to_shift};
use deepmsg_core::pal::MappedFile;

use crate::checksum::Checksum;

/// `Archive.Configuration.RECORDING_SEGMENT_SUFFIX` (`Archive.java:289`).
pub const SUFFIX: &str = ".rec";

/// The name of one segment file
/// (`recordingId + "-" + segmentBasePosition + ".rec"`).
#[must_use]
pub fn segment_file_name(recording_id: i64, segment_base_position: i64) -> String {
    format!("{recording_id}-{segment_base_position}{SUFFIX}")
}

/// The base position of the segment a stream position falls in.
///
/// `AeronArchive.segmentFileBasePosition` (`:195-203`), and the two masks are
/// the whole of it: both lengths are powers of two, so `& (length - 1)` takes
/// the offset within one and `& !(length - 1)` rounds down to a boundary.
///
/// `start_position` is where the recording began, `position` where the caller is
/// asking about — and the answer is measured from the **term the recording
/// started in**, not from zero. That is why a recording whose start is not on a
/// segment boundary has a first segment that is not either.
#[must_use]
pub fn segment_file_base_position(
    start_position: i64,
    position: i64,
    term_buffer_length: i32,
    segment_file_length: i32,
) -> i64 {
    let term_mask = i64::from(term_buffer_length) - 1;
    let segment_mask = i64::from(segment_file_length) - 1;

    let start_term_base = start_position - (start_position & term_mask);
    let from_base = position - start_term_base;

    start_term_base + (from_base & !segment_mask)
}

/// Why a segment could not be written.
#[derive(Debug)]
pub enum SegmentError {
    /// The file system.
    Io(io::Error),
    /// A block that is not a run of frames this build can walk: a length of zero
    /// or less, or one that reaches past the block.
    Malformed {
        /// Where in the block it happened.
        offset: usize,
        /// What the frame said its length was.
        frame_length: i32,
    },
    /// The next segment file is already there
    /// (`RecordingWriter.onFileRollOver`, `:242-245`).
    Exists {
        /// The file that was there.
        path: PathBuf,
    },
    /// The last fragment of a segment crosses a page boundary and was not
    /// written whole, which is what a write interrupted by a crash leaves
    /// (`Catalog.recoverStopOffset`). The reference refuses it in the recovery
    /// path too, and truncates it only when `ArchiveTool verify` is told to.
    StraddlesPage {
        /// Where the fragment is.
        offset: usize,
        /// How long it claims to be.
        length: usize,
    },
    /// A segment file the reader was asked for is not there
    /// (`RecordingReader.openRecordingSegment`, `:207-210`).
    Missing {
        /// The file that was not.
        path: PathBuf,
    },
}

impl std::fmt::Display for SegmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Malformed {
                offset,
                frame_length,
            } => write!(
                f,
                "a frame at {offset} says its length is {frame_length}, which cannot be walked"
            ),
            Self::Exists { path } => {
                write!(f, "segment file already exists: {}", path.display())
            }
            Self::Missing { path } => write!(
                f,
                "failed to open recording segment file {}",
                path.display()
            ),
            Self::StraddlesPage { offset, length } => write!(
                f,
                "Found potentially incomplete last fragment straddling page boundary at \
                 offset {offset} of length {length}"
            ),
        }
    }
}

impl std::error::Error for SegmentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for SegmentError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// What a segment writer needs to know about the recording it writes for.
///
/// A struct rather than seven arguments, and not only for the linter: the three
/// positions are what the naming arithmetic is *made* of, and having them named
/// together is what makes it possible to read `segment_file_base_position` as
/// the function it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentSpec {
    /// The recording's id, which every one of its segment files is named with.
    pub recording_id: i64,
    /// Where the recording began — the publication's start position, not the
    /// position this writer starts at.
    pub start_position: i64,
    /// Where this writer joins the stream, which is where its first segment
    /// starts being written.
    pub join_position: i64,
    /// One term of the stream, which is what the segment base is measured from.
    pub term_buffer_length: i32,
    /// How long one segment file is, and how far the writer gets before rolling
    /// to the next.
    pub segment_length: usize,
}

/// Where a recording's frames go: one segment file at a time, rolled over when
/// it fills.
///
/// Mirrors `io.aeron.archive.RecordingWriter`, minus the parts that belong to a
/// recording **session** — the statistics it reports, the error handler it
/// counts through, the low-storage-space policy. What is here is the file: the
/// name, the length, the rollover, and the three things in the module docs.
pub struct SegmentWriter {
    directory: PathBuf,
    recording_id: i64,
    segment_base_position: i64,
    segment_length: usize,
    offset: usize,
    file: File,
    force_writes: bool,
    force_metadata: bool,
    checksum: Option<Checksum>,
    /// Where a checksummed block is prepared before it is written.
    ///
    /// Held rather than allocated per block: the reference keeps the same buffer
    /// in its context for the same reason (`ctx.recordChecksumBuffer()`), and a
    /// recording path that allocated per block would be allocating on the hot
    /// path ADR-0003 keeps clear.
    scratch: Vec<u8>,
    bytes_written: u64,
}

impl SegmentWriter {
    /// Create the segment a recording at `join_position` starts writing into.
    ///
    /// The file is **preallocated** to `segment_length` and the offset starts at
    /// `join_position - segment_base_position` — where the recording joined the
    /// stream inside its first segment, which is not zero for a recording that
    /// joined late (`RecordingWriter.java:100-101`).
    ///
    /// `file_sync_level` is the reference's (`:88-89`): above zero every block
    /// is forced, above one the metadata is too.
    ///
    /// # Errors
    ///
    /// [`SegmentError::Io`] for the file system, and
    /// [`SegmentError::Exists`] when a segment of this recording is already
    /// there — a recording does not overwrite one.
    pub fn create(
        directory: &Path,
        spec: SegmentSpec,
        file_sync_level: i32,
        checksum: Option<Checksum>,
    ) -> Result<Self, SegmentError> {
        let SegmentSpec {
            recording_id,
            start_position,
            join_position,
            term_buffer_length,
            segment_length,
        } = spec;

        let segment_base_position = segment_file_base_position(
            start_position,
            join_position,
            term_buffer_length,
            i32::try_from(segment_length).unwrap_or(i32::MAX),
        );

        let path = directory.join(segment_file_name(recording_id, segment_base_position));
        let file = Self::open_segment(&path, segment_length)?;

        Ok(Self {
            directory: directory.to_path_buf(),
            recording_id,
            segment_base_position,
            segment_length,
            offset: usize::try_from(join_position - segment_base_position).unwrap_or(0),
            file,
            force_writes: file_sync_level > 0,
            force_metadata: file_sync_level > 1,
            checksum,
            scratch: Vec::new(),
            bytes_written: 0,
        })
    }

    /// The file being written.
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.directory.join(segment_file_name(
            self.recording_id,
            self.segment_base_position,
        ))
    }

    /// Where in the stream the segment being written begins.
    #[must_use]
    pub const fn segment_base_position(&self) -> i64 {
        self.segment_base_position
    }

    /// How far into the current segment the writer has got.
    #[must_use]
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// How many bytes have been handed to the file system, padding frames
    /// counted as the header they are written as.
    #[must_use]
    pub const fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Write one block of frames — what `Image::block_poll` hands over.
    ///
    /// A block whose first frame is a **padding** frame is written as that
    /// frame's header alone: the rest of what it covers stays as the zeroes the
    /// file was created with, which is what a preallocated segment is for.
    /// `block_poll` hands a padding frame over as a block of its own for exactly
    /// this reason (`RecordingWriter.java:112-116`).
    ///
    /// The offset advances by the block's **length**, not by what was written —
    /// the two differ by the padding rule, and it is the length that says where
    /// the next frame goes. What is *returned* is the other one: the bytes that
    /// went to the file, which is the reference's `dataLength`
    /// (`RecordingWriter.java:114`) and what a recorder counts as written
    /// (`:144`).
    ///
    /// # Errors
    ///
    /// [`SegmentError::Malformed`] for a block that is not a walkable run of
    /// frames, and [`SegmentError::Io`] for the file system — including the next
    /// segment being in the way when this one fills.
    pub fn write_block(&mut self, block: &[u8]) -> Result<u64, SegmentError> {
        let length = block.len();
        if 0 == length {
            return Ok(0);
        }

        let padding = is_padding_frame(block);
        let written = if padding {
            // The header and nothing else. The header is at the front of the
            // block, so this is a prefix of it.
            if length < DATA_HEADER_LENGTH {
                return Err(SegmentError::Malformed {
                    offset: 0,
                    frame_length: i32::try_from(length).unwrap_or(i32::MAX),
                });
            }

            self.file
                .write_all_at(&block[..DATA_HEADER_LENGTH], self.offset as u64)?;
            DATA_HEADER_LENGTH as u64
        } else if let Some(checksum) = self.checksum {
            // A checksum goes into the frame's session-id field, so the block
            // cannot be written from the caller's bytes: it is copied, stamped
            // and written.
            self.scratch.clear();
            self.scratch.extend_from_slice(block);
            stamp_checksums(&mut self.scratch, checksum)?;

            self.file.write_all_at(&self.scratch, self.offset as u64)?;

            self.scratch.len() as u64
        } else {
            self.file.write_all_at(block, self.offset as u64)?;

            length as u64
        };

        if self.force_writes {
            if self.force_metadata {
                self.file.sync_all()?;
            } else {
                self.file.sync_data()?;
            }
        }

        self.bytes_written += written;
        self.offset += length;

        if self.offset >= self.segment_length {
            self.roll_over()?;
        }

        Ok(written)
    }

    /// Close the segment being written and start the next one
    /// (`RecordingWriter.onFileRollOver`, `:234-248`).
    ///
    /// The base position advances by exactly one segment length, which is the
    /// reference's arithmetic and only true because a segment is filled from its
    /// beginning: the file's last block is one that did not fit the one before
    /// it.
    fn roll_over(&mut self) -> Result<(), SegmentError> {
        self.segment_base_position += i64::try_from(self.segment_length).unwrap_or(i64::MAX);
        self.offset = 0;

        let path = self.path();
        let next = Self::open_segment(&path, self.segment_length)?;
        self.file = next;

        Ok(())
    }

    fn open_segment(path: &Path, segment_length: usize) -> Result<File, SegmentError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| match error.kind() {
                io::ErrorKind::AlreadyExists => SegmentError::Exists {
                    path: path.to_path_buf(),
                },
                _ => SegmentError::Io(error),
            })?;

        // Preallocated, which is what makes a padding frame's "header and
        // nothing else" leave zeroes behind it rather than a hole a reader has
        // to know about.
        file.set_len(segment_length as u64)?;

        Ok(file)
    }
}

impl std::fmt::Debug for SegmentWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentWriter")
            .field("path", &self.path())
            .field("offset", &self.offset)
            .field("segment_length", &self.segment_length)
            .field("checksum", &self.checksum)
            .finish()
    }
}

/// Whether the block's first frame is a padding frame.
///
/// A **type read**, which is what the reference's test is
/// (`RecordingWriter.java:112`): nothing about the length is checked here,
/// because a writer without a checksum writes the block it was handed and the
/// reference writes it too. A block that is not frames at all is refused where
/// it has to be walked — by [`stamp_checksums`] — and not before.
fn is_padding_frame(block: &[u8]) -> bool {
    read_i16(block, TYPE_OFFSET).is_some_and(|type_id| TYPE_PAD == type_id)
}

/// The frame length at `offset`, or why it cannot be used.
fn frame_length(block: &[u8], offset: usize) -> Result<i32, SegmentError> {
    let length = read_i32(block, offset + FRAME_LENGTH_OFFSET).ok_or(SegmentError::Malformed {
        offset,
        frame_length: 0,
    })?;

    if length < DATA_HEADER_LENGTH as i32 {
        return Err(SegmentError::Malformed {
            offset,
            frame_length: length,
        });
    }

    Ok(length)
}

/// Compute each frame's checksum over its payload and store it in that frame's
/// session-id field (`RecordingWriter.computeChecksum`, `:198-212`).
///
/// The payload is `alignedLength - HEADER_LENGTH` bytes from the frame's end of
/// header: the alignment padding is inside the frame and is part of what the
/// checksum covers, which is why the aligned length is what is used and not the
/// frame's own length.
fn stamp_checksums(block: &mut [u8], checksum: Checksum) -> Result<(), SegmentError> {
    let mut offset = 0;

    while offset < block.len() {
        let length = frame_length(block, offset)?;
        let aligned = align_up(length, FRAME_ALIGNMENT);

        if offset + aligned as usize > block.len() {
            return Err(SegmentError::Malformed {
                offset,
                frame_length: length,
            });
        }

        let payload = DATA_HEADER_LENGTH..aligned as usize;
        let computed = checksum.compute(&block[offset + payload.start..offset + payload.end]);

        let field = offset + SESSION_ID_FIELD_OFFSET;
        block[field..field + 4].copy_from_slice(&computed.to_le_bytes());

        offset += aligned as usize;
    }

    Ok(())
}

fn read_i16(bytes: &[u8], offset: usize) -> Option<i16> {
    Some(i16::from_le_bytes(
        bytes.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_i32(bytes: &[u8], offset: usize) -> Option<i32> {
    Some(i32::from_le_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mark::tests::TempDir;

    /// A term of 64 KiB and a segment of four of them, which is the smallest
    /// arrangement where a segment and a term are both visible in the numbers.
    const TERM_LENGTH: i32 = 64 * 1024;
    const SEGMENT_LENGTH: usize = 4 * TERM_LENGTH as usize;

    fn spec(start_position: i64, join_position: i64) -> SegmentSpec {
        SegmentSpec {
            recording_id: 7,
            start_position,
            join_position,
            term_buffer_length: TERM_LENGTH,
            segment_length: SEGMENT_LENGTH,
        }
    }

    /// One data frame of `payload` bytes, with the term's own field values, and
    /// the padding the alignment wants.
    fn frame(term_offset: i32, term_id: i32, payload: &[u8]) -> Vec<u8> {
        let length = DATA_HEADER_LENGTH + payload.len();
        let aligned = align_up(i32::try_from(length).expect("small"), FRAME_ALIGNMENT) as usize;
        let mut bytes = vec![0_u8; aligned];

        bytes[FRAME_LENGTH_OFFSET..FRAME_LENGTH_OFFSET + 4]
            .copy_from_slice(&i32::try_from(length).expect("small").to_le_bytes());
        bytes[TYPE_OFFSET..TYPE_OFFSET + 2].copy_from_slice(&1_i16.to_le_bytes());
        bytes[8..12].copy_from_slice(&term_offset.to_le_bytes());
        bytes[SESSION_ID_FIELD_OFFSET..SESSION_ID_FIELD_OFFSET + 4]
            .copy_from_slice(&42_i32.to_le_bytes());
        bytes[16..20].copy_from_slice(&1001_i32.to_le_bytes());
        bytes[20..24].copy_from_slice(&term_id.to_le_bytes());
        bytes[DATA_HEADER_LENGTH..length].copy_from_slice(payload);

        bytes
    }

    /// A padding frame covering `length` bytes, as a term's tail is.
    ///
    /// The body is deliberately **not** zeroes. A padding frame covers bytes the
    /// term already had, and the file it is written into was preallocated with
    /// zeroes — so a fixture whose body was also zeroes could not tell "the
    /// header alone was written" from "the whole frame was", which is the one
    /// thing the test using this is about. Fill it with something, and the two
    /// are different bytes.
    fn padding_frame(length: usize) -> Vec<u8> {
        let mut bytes = vec![0xAB_u8; length];

        bytes[FRAME_LENGTH_OFFSET..FRAME_LENGTH_OFFSET + 4]
            .copy_from_slice(&i32::try_from(length).expect("small").to_le_bytes());
        bytes[TYPE_OFFSET..TYPE_OFFSET + 2].copy_from_slice(&TYPE_PAD.to_le_bytes());

        bytes
    }

    fn writer(dir: &TempDir, start: i64, join: i64) -> SegmentWriter {
        SegmentWriter::create(dir.path(), spec(start, join), 1, None).expect("a writer")
    }

    /// The naming arithmetic, in the three arrangements that differ: a start on
    /// a segment boundary, a start inside one, and a join that is not where the
    /// recording started.
    ///
    /// The second is the one worth the test. A recording that starts partway
    /// into a segment has a **first segment whose base is not a multiple of the
    /// segment length** — the base is measured from the term the recording
    /// started in — so a build that rounded the absolute position down would
    /// name a file the reference does not and put the frames in the wrong one.
    #[test]
    fn a_segment_begins_at_the_term_the_recording_started_in() {
        // A start exactly on a segment boundary: the base is that boundary.
        assert_eq!(
            0,
            segment_file_base_position(0, 0, TERM_LENGTH, SEGMENT_LENGTH as i32)
        );
        assert_eq!(
            SEGMENT_LENGTH as i64,
            segment_file_base_position(
                0,
                SEGMENT_LENGTH as i64 + 5,
                TERM_LENGTH,
                SEGMENT_LENGTH as i32
            )
        );

        // A start one term in: the base is that term's base, and stays so until
        // a whole segment has been written from it.
        let start = TERM_LENGTH as i64;

        assert_eq!(
            start,
            segment_file_base_position(start, start, TERM_LENGTH, SEGMENT_LENGTH as i32)
        );
        assert_eq!(
            start,
            segment_file_base_position(
                start,
                start + TERM_LENGTH as i64,
                TERM_LENGTH,
                SEGMENT_LENGTH as i32
            ),
            "still in the first segment"
        );
        assert_eq!(
            start + SEGMENT_LENGTH as i64,
            segment_file_base_position(
                start,
                start + SEGMENT_LENGTH as i64,
                TERM_LENGTH,
                SEGMENT_LENGTH as i32
            ),
            "and the second begins a whole segment after the first"
        );

        // A start that is not on either boundary: the base is the term's, so it
        // is not a multiple of the segment length at all.
        let odd = 3 * TERM_LENGTH as i64 + 4096;
        let base = segment_file_base_position(odd, odd, TERM_LENGTH, SEGMENT_LENGTH as i32);

        assert_eq!(3 * TERM_LENGTH as i64, base);
        assert_ne!(
            0,
            base % SEGMENT_LENGTH as i64,
            "which is what a build that rounded the position itself would get wrong"
        );
        assert_eq!("7-196608.rec", segment_file_name(7, base));
    }

    /// The file is there and it is the length it was asked to be: preallocated,
    /// so that a padding frame written as its header leaves zeroes behind it.
    #[test]
    fn a_segment_is_created_at_its_full_length() {
        let dir = TempDir::new();
        let writer = writer(&dir, 0, 0);

        assert_eq!(0, writer.offset());
        assert_eq!(0, writer.segment_base_position());
        assert_eq!(
            "7-0.rec",
            writer.path().file_name().expect("a name").to_string_lossy()
        );

        let length = std::fs::metadata(writer.path()).expect("the file").len();

        assert_eq!(
            u64::try_from(SEGMENT_LENGTH).expect("fits"),
            length,
            "the file is its full length before anything is written"
        );
    }

    /// A block of frames goes in at the offset, the offset advances by the
    /// block's length, and what is in the file is what was handed over.
    #[test]
    fn a_block_is_written_where_the_offset_says() {
        let dir = TempDir::new();
        let mut writer = writer(&dir, 0, 0);

        let first = frame(0, 7, b"the first message");
        let second = frame(64, 7, b"the second message");

        writer.write_block(&first).expect("the first");
        assert_eq!(first.len(), writer.offset());

        writer.write_block(&second).expect("the second");

        let written = std::fs::read(writer.path()).expect("the file");

        assert_eq!(&first[..], &written[..first.len()]);
        assert_eq!(
            &second[..],
            &written[first.len()..first.len() + second.len()]
        );
        assert_eq!(
            0,
            written[first.len() + second.len()],
            "and nothing past them: the rest is the preallocated zeroes"
        );
    }

    /// A padding frame is written as its **header alone**, and the offset still
    /// advances over the whole of what it covers.
    ///
    /// The two halves are one claim: the file has 32 bytes where the frame was
    /// and zeroes where its body would have been, and the next block lands after
    /// the padding rather than inside it.
    #[test]
    fn a_padding_frame_is_written_as_its_header_alone() {
        let dir = TempDir::new();
        let mut writer = writer(&dir, 0, 0);

        let data = frame(0, 7, b"before the padding");
        let padding = padding_frame(1024);

        writer.write_block(&data).expect("data");
        writer.write_block(&padding).expect("padding");

        assert_eq!(
            data.len() + padding.len(),
            writer.offset(),
            "the offset advanced over the whole frame, not over what was written"
        );

        writer
            .write_block(&frame(1024, 7, b"after"))
            .expect("after");

        let written = std::fs::read(writer.path()).expect("the file");
        let padding_at = data.len();

        assert_eq!(
            &padding[..DATA_HEADER_LENGTH],
            &written[padding_at..padding_at + DATA_HEADER_LENGTH],
            "the header is there"
        );
        assert!(
            written[padding_at + DATA_HEADER_LENGTH..padding_at + padding.len()]
                .iter()
                .all(|byte| 0 == *byte),
            "and the body it covers is the zeroes the file was created with"
        );
        assert_eq!(
            data.len() + padding.len() + frame(1024, 7, b"after").len(),
            writer.offset(),
            "and the frame after the padding lands after all of it"
        );
    }

    /// The checksum goes into the frame's **session-id field** and covers the
    /// frame's payload — padding included, because the aligned length is what a
    /// reader recomputes from.
    #[test]
    fn a_checksum_replaces_the_session_id_of_every_frame() {
        let dir = TempDir::new();
        let mut writer = SegmentWriter::create(dir.path(), spec(0, 0), 1, Some(Checksum::Crc32c))
            .expect("a writer");

        let one = frame(0, 7, b"first");
        let two = frame(64, 7, b"second");
        let mut block = Vec::new();
        block.extend_from_slice(&one);
        block.extend_from_slice(&two);

        writer.write_block(&block).expect("written");

        let written = std::fs::read(writer.path()).expect("the file");

        for (index, source) in [&one, &two].into_iter().enumerate() {
            let at = index * one.len();
            let stored = i32::from_le_bytes(
                written[at + SESSION_ID_FIELD_OFFSET..at + SESSION_ID_FIELD_OFFSET + 4]
                    .try_into()
                    .expect("four"),
            );

            assert_ne!(
                42, stored,
                "the session id the frame was built with is gone"
            );
            assert_eq!(
                Checksum::Crc32c.compute(&source[DATA_HEADER_LENGTH..]),
                stored,
                "and what is there is the payload's checksum, padding included"
            );
        }

        // The frames after the first are still walkable: a checksum written at
        // the wrong offset would have been a length or a type field, and this
        // block would not decode into two frames.
        assert_eq!(2 * one.len(), writer.offset());
    }

    /// Filling a segment rolls to the next one, which is named a whole segment
    /// further on and starts empty.
    #[test]
    fn a_segment_that_fills_rolls_over() {
        let dir = TempDir::new();

        // Two frames a segment, so that the second write fills it exactly.
        let half = SEGMENT_LENGTH / 2;
        let specs = SegmentSpec {
            segment_length: 2 * half,
            ..spec(0, 0)
        };
        let mut writer = SegmentWriter::create(dir.path(), specs, 1, None).expect("a writer");

        let block = vec![0_u8; half];
        // Not a walkable frame, so written as a block without a checksum: this
        // test is about the rollover, and a checksummed writer would refuse it.
        writer.write_block(&block).expect("half");
        assert_eq!(half, writer.offset());

        writer.write_block(&block).expect("the other half");

        assert_eq!(
            0,
            writer.offset(),
            "the new segment starts at its beginning"
        );
        assert_eq!(
            SEGMENT_LENGTH as i64,
            writer.segment_base_position(),
            "and a whole segment further along"
        );
        assert_eq!(
            "7-262144.rec",
            writer.path().file_name().expect("a name").to_string_lossy()
        );
        assert!(
            std::fs::metadata(writer.path()).is_ok(),
            "the second file is there"
        );
    }

    /// A summary of a recording written into `dir`, for the reader's tests.
    fn summary(
        recording_id: i64,
        start_position: i64,
        stop_position: Option<i64>,
    ) -> SegmentSummary {
        SegmentSummary {
            recording_id,
            start_position,
            stop_position,
            initial_term_id: 7,
            term_buffer_length: TERM_LENGTH,
            segment_file_length: SEGMENT_LENGTH as i32,
            stream_id: 1001,
        }
    }

    /// Write `blocks` into a recording's first segment, and answer with what a
    /// reader would need.
    fn recorded(dir: &TempDir, blocks: &[Vec<u8>]) -> (SegmentSummary, i64) {
        let mut writer = writer(dir, 0, 0);

        for block in blocks {
            writer.write_block(block).expect("written");
        }

        let stop = writer.offset() as i64;

        (summary(7, 0, Some(stop)), stop)
    }

    fn payloads(reader: &mut SegmentReader) -> Vec<Vec<u8>> {
        let mut seen = Vec::new();

        reader
            .poll(usize::MAX, |fragment| {
                let mut payload = vec![0_u8; fragment.payload_length()];
                fragment.copy_payload(&mut payload).expect("fits");

                seen.push(payload);
            })
            .expect("poll");

        seen
    }

    /// What a writer put in, a reader gets back — in order, one fragment per
    /// frame, with the payloads intact.
    #[test]
    fn a_recording_reads_back_frame_for_frame() {
        let dir = TempDir::new();
        let blocks = vec![
            frame(0, 7, b"the first message"),
            frame(64, 7, b"the second, longer message"),
            frame(128, 7, b"third"),
        ];
        let (summary, _stop) = recorded(&dir, &blocks);

        let mut reader = SegmentReader::open(dir.path(), summary, None, None).expect("a reader");

        assert_eq!(0, reader.replay_position());
        assert!(!reader.is_done());

        let read = payloads(&mut reader);

        assert_eq!(
            vec![
                b"the first message".to_vec(),
                b"the second, longer message".to_vec(),
                b"third".to_vec()
            ],
            read
        );
        assert!(
            reader.is_done(),
            "and the read ended at the hole behind them"
        );
    }

    /// A reader that starts in the middle of the recording has to be given a
    /// position that is a **frame boundary**, and the only way to know is to ask
    /// the frame there: its term offset, its term id and its stream id all have
    /// to be the ones the position implies.
    ///
    /// A position a few bytes off is the case this exists for: the bytes there
    /// would be read as a header, and a header read from a payload is a length
    /// that walks somewhere else entirely.
    #[test]
    fn a_position_that_is_not_a_frame_boundary_is_refused() {
        let dir = TempDir::new();
        let blocks = vec![frame(0, 7, b"the first message"), frame(64, 7, b"second")];
        let (summary, _stop) = recorded(&dir, &blocks);

        // The second frame's own start: a boundary.
        let second = blocks[0].len() as i64;
        let mut reader =
            SegmentReader::open(dir.path(), summary, Some(second), None).expect("a reader");
        assert_eq!(
            vec![b"second".to_vec()],
            payloads(&mut reader),
            "a frame boundary reads from there"
        );

        // Eight bytes into the first frame: inside its header, where a term
        // offset is not the one the position implies.
        let error =
            SegmentReader::open(dir.path(), summary, Some(8), None).expect_err("not a boundary");
        assert!(matches!(error, SegmentError::Malformed { .. }), "{error:?}");
    }

    /// The read ends where the writing did: a segment is preallocated, so what
    /// follows the last frame is zeroes, and a frame length of zero is the end.
    #[test]
    fn the_read_ends_at_the_hole_behind_the_recording() {
        let dir = TempDir::new();
        let blocks = vec![frame(0, 7, b"one"), frame(64, 7, b"two")];
        let (summary, _stop) = recorded(&dir, &blocks);

        let mut reader = SegmentReader::open(dir.path(), summary, None, None).expect("a reader");

        assert_eq!(2, payloads(&mut reader).len());
        assert!(reader.is_done());

        // And a second poll finds nothing rather than reading the zeroes as
        // frames.
        assert_eq!(0, reader.poll(10, |_| {}).expect("poll"));
    }

    /// A read is bounded by the recording's own stop position, not only by what
    /// the caller asked for: asking for more than there is reads what there is.
    ///
    /// The recording here **stops with a frame still in the file**, which is the
    /// only arrangement in which the clamp is visible: a recording whose stop is
    /// where the writing stopped would be bounded by the hole behind it whether
    /// the clamp was there or not, and the test would pass for a reader that
    /// ignored the stop position entirely.
    #[test]
    fn a_bounded_read_stops_at_the_recordings_stop() {
        let dir = TempDir::new();
        let blocks = vec![
            frame(0, 7, b"one"),
            frame(64, 7, b"two"),
            frame(128, 7, b"three, after the stop"),
        ];
        let (mut summary, stop) = recorded(&dir, &blocks);

        // Where the recording stopped: after two frames, with the third still
        // in the segment.
        let stopped_at = (blocks[0].len() + blocks[1].len()) as i64;
        assert!(stopped_at < stop, "the third frame is past the stop");

        summary.stop_position = Some(stopped_at);

        let mut reader =
            SegmentReader::open(dir.path(), summary, None, Some(stop * 10)).expect("a reader");
        assert_eq!(
            vec![b"one".to_vec(), b"two".to_vec()],
            payloads(&mut reader),
            "the recording's stop bounded it, not the hole"
        );

        // And the caller's own limit, when it is the smaller of the two.
        let mut reader =
            SegmentReader::open(dir.path(), summary, None, Some(blocks[0].len() as i64))
                .expect("a reader");
        assert_eq!(vec![b"one".to_vec()], payloads(&mut reader));
        assert!(reader.is_done(), "the limit ended it");

        // A recording that has not stopped is read to its hole instead.
        summary.stop_position = None;
        let mut reader = SegmentReader::open(dir.path(), summary, None, None).expect("a reader");
        assert_eq!(3, payloads(&mut reader).len());
    }

    /// A read that crosses a term boundary and a **segment** boundary hands
    /// back every frame, in order.
    ///
    /// Frames in a recording are contiguous — a live term's frames happen to
    /// reach a term's end, and the next term's follow in the same file — so the
    /// boundaries are where a reader that lost its place would be visibly wrong
    /// rather than merely slow. The count is one frame more than a segment
    /// holds, so both crossings happen: `next_term` moves the window within the
    /// file, and the last one opens the next file.
    #[test]
    fn a_read_crosses_term_and_segment_boundaries() {
        /// Frames as large as a term allows, so that few of them cross a term
        /// and the test does not pay for the crossings four times over. The term
        /// cannot be shrunk instead: 64 KiB is the log buffer's minimum, and
        /// `bits_to_shift` refuses anything smaller.
        const FRAME_LENGTH_BYTES: usize = 4 * 1024;
        const PAYLOAD: usize = FRAME_LENGTH_BYTES - DATA_HEADER_LENGTH;

        const TERM: i32 = TERM_LENGTH;
        const SEGMENT: usize = 2 * TERM as usize;

        let dir = TempDir::new();
        let per_term = TERM as usize / FRAME_LENGTH_BYTES;
        let frames = SEGMENT / FRAME_LENGTH_BYTES + 1;

        let mut writer = SegmentWriter::create(
            dir.path(),
            SegmentSpec {
                recording_id: 7,
                start_position: 0,
                join_position: 0,
                term_buffer_length: TERM,
                segment_length: SEGMENT,
            },
            1,
            None,
        )
        .expect("a writer");

        for index in 0..frames {
            let offset = (index % per_term) * FRAME_LENGTH_BYTES;
            let term_id = 7 + i32::try_from(index / per_term).expect("small");
            let mut payload = vec![b' '; PAYLOAD];
            let text = format!("message {index}");
            payload[..text.len()].copy_from_slice(text.as_bytes());

            writer
                .write_block(&frame(offset as i32, term_id, &payload))
                .expect("written");
        }

        assert!(
            writer.offset() < SEGMENT,
            "the last frame opened a second segment: {} of {SEGMENT}",
            writer.offset()
        );

        let summary = SegmentSummary {
            term_buffer_length: TERM,
            segment_file_length: SEGMENT as i32,
            ..summary(7, 0, None)
        };
        let mut reader = SegmentReader::open(dir.path(), summary, None, None).expect("a reader");
        let mut index = 0;

        reader
            .poll(usize::MAX, |fragment| {
                let mut payload = vec![0_u8; fragment.payload_length()];
                fragment.copy_payload(&mut payload).expect("fits");

                let expected = format!("message {index}");
                assert_eq!(
                    expected.as_bytes(),
                    &payload[..expected.len()],
                    "frame {index} is not where the read got to"
                );
                index += 1;
            })
            .expect("poll");

        assert_eq!(frames, index, "every frame, across both boundaries");
    }

    /// A block that is not a walkable run of frames is refused rather than
    /// written, and nothing is left half-written.
    #[test]
    fn a_block_that_is_not_frames_is_refused_when_it_is_checksummed() {
        let dir = TempDir::new();
        let mut writer = SegmentWriter::create(dir.path(), spec(0, 0), 1, Some(Checksum::Crc32))
            .expect("a writer");

        // A **data** frame whose length is zero: not walkable, and not a padding
        // frame either — `TYPE_PAD` is `0x00`, so a block of zeroes would be
        // classified as padding and written as a header rather than refused.
        let mut block = vec![0_u8; 64];
        block[TYPE_OFFSET..TYPE_OFFSET + 2].copy_from_slice(&1_i16.to_le_bytes());

        let error = writer.write_block(&block).expect_err("not a frame");

        assert!(matches!(error, SegmentError::Malformed { .. }), "{error:?}");
        assert_eq!(0, writer.offset(), "and the offset did not move");
        assert_eq!(0, writer.bytes_written());
    }
}

/// What a reader needs to know about the recording it is reading.
///
/// A subset of the catalog's descriptor, and named for it: the reference reads
/// a `RecordingSummary` (`RecordingReader.java:46-68`) rather than the whole
/// thing, because a reader places positions in files and has no use for a
/// channel or a timestamp.
///
/// [`From<&Recording>`] is how a caller gets one from the catalog, which keeps
/// the coupling one-way: this module does not know what a catalog is, and the
/// catalog does not know what a segment is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentSummary {
    /// The recording's id, which names its files.
    pub recording_id: i64,
    /// Where the recording began.
    pub start_position: i64,
    /// Where it stopped, or `None` while it has not.
    pub stop_position: Option<i64>,
    /// The term the stream started in.
    pub initial_term_id: i32,
    /// One term of the stream.
    pub term_buffer_length: i32,
    /// How long one segment file is.
    pub segment_file_length: i32,
    /// The stream's id, which every frame in it carries.
    pub stream_id: i32,
}

impl From<&crate::catalog::Recording> for SegmentSummary {
    fn from(recording: &crate::catalog::Recording) -> Self {
        Self {
            recording_id: recording.recording_id,
            start_position: recording.start_position,
            // The catalog writes a null stop position for a recording that has
            // not finished, which is `Aeron.NULL_VALUE` — every negative value
            // is "no stop" to this reader, as it is to the reference's.
            stop_position: (recording.stop_position >= 0).then_some(recording.stop_position),
            initial_term_id: recording.initial_term_id,
            term_buffer_length: recording.term_buffer_length,
            segment_file_length: recording.segment_file_length,
            stream_id: recording.stream_id,
        }
    }
}

/// One frame of a recording, handed to a poll's handler.
///
/// A **window**, not a copy — the segment is mapped and a fragment is a place in
/// it — and the payload is copied out with [`SegmentFragment::copy_payload`] for
/// the reason [`deepmsg_client::image::Fragment`] gives: this build's buffers do
/// not hand out slices of a mapping, so a caller that wants bytes asks for them.
pub struct SegmentFragment<'a> {
    segment: &'a AtomicBuffer<'a, ReadOnly>,
    offset: usize,
    length: usize,
    flags: u8,
    header_type: i16,
    reserved_value: i64,
    position: i64,
}

impl SegmentFragment<'_> {
    /// Where the frame is in the stream.
    #[must_use]
    pub const fn position(&self) -> i64 {
        self.position
    }

    /// How many payload bytes it carries: the frame less its header.
    #[must_use]
    pub const fn payload_length(&self) -> usize {
        self.length
    }

    /// A frame's header type (`DATA` is 1, and 0 is padding).
    #[must_use]
    pub const fn header_type(&self) -> i16 {
        self.header_type
    }

    /// The `BEGIN`/`END`/`EOS`/`REVOKED` bits.
    #[must_use]
    pub const fn flags(&self) -> u8 {
        self.flags
    }

    /// The frame's reserved value, which the publisher sets.
    #[must_use]
    pub const fn reserved_value(&self) -> i64 {
        self.reserved_value
    }

    /// Copy the payload out.
    ///
    /// `None` when `dst` is shorter than [`SegmentFragment::payload_length`],
    /// or when the window does not lie within the mapping — which it does, since
    /// the poll proved it before handing this over.
    pub fn copy_payload(&self, dst: &mut [u8]) -> Option<()> {
        self.segment
            .copy_out(self.offset, dst.get_mut(..self.length)?)
    }
}

/// Reads one recording's frames out of its segment files.
///
/// Mirrors `io.aeron.archive.RecordingReader`: the same three numbers place a
/// stream position in a file, the same check refuses a position that is not a
/// frame boundary, and the same rule ends the read — **a frame length of zero or
/// less is the end**, because that is what the preallocated tail of a segment
/// reads as once the recording has been read to its end.
pub struct SegmentReader {
    directory: PathBuf,
    summary: SegmentSummary,
    replay_position: i64,
    replay_limit: i64,
    segment_file_position: i64,
    term_offset: usize,
    term_base_segment_offset: usize,
    mapping: MappedFile,
    done: bool,
}

impl SegmentReader {
    /// Open the reader for `from_position`, at most `length` bytes of it.
    ///
    /// `None` for `from_position` means the recording's start, and `None` for
    /// `length` means "to the end" — which is the recording's stop position when
    /// it has one, and unbounded when it has not, so a bounded request is
    /// clamped to the recording rather than reading past it
    /// (`RecordingReader.java:70-77`).
    ///
    /// # Errors
    ///
    /// [`SegmentError::Malformed`] when the position is not a frame boundary —
    /// the frame there has to carry the term offset, term id and stream id this
    /// recording implies — and [`SegmentError::Io`] when the segment file is not
    /// there or cannot be mapped.
    pub fn open(
        directory: &Path,
        summary: SegmentSummary,
        from_position: Option<i64>,
        length: Option<i64>,
    ) -> Result<Self, SegmentError> {
        let bits_to_shift =
            bits_to_shift(summary.term_buffer_length).ok_or(SegmentError::Malformed {
                offset: 0,
                frame_length: summary.term_buffer_length,
            })?;

        let from = from_position.unwrap_or(summary.start_position);
        let max_length = match summary.stop_position {
            Some(stop) => stop - from,
            None => i64::MAX - from,
        };
        let replay_length = length.map_or(max_length, |asked| asked.min(max_length));

        if replay_length < 0 {
            return Err(SegmentError::Malformed {
                offset: 0,
                frame_length: i32::try_from(replay_length).unwrap_or(i32::MIN),
            });
        }

        let term_length = summary.term_buffer_length;
        let segment_file_length = summary.segment_file_length;
        let start_term_base =
            summary.start_position - (summary.start_position & i64::from(term_length - 1));
        let segment_offset =
            usize::try_from((from - start_term_base) & i64::from(segment_file_length - 1))
                .unwrap_or(0);
        let term_offset = usize::try_from(from & i64::from(term_length - 1)).unwrap_or(0);
        let term_id = i32::try_from(from >> bits_to_shift)
            .unwrap_or(0)
            .wrapping_add(summary.initial_term_id);

        let segment_file_position = segment_file_base_position(
            summary.start_position,
            from,
            term_length,
            segment_file_length,
        );
        let mapping = map_segment(directory, summary.recording_id, segment_file_position)?;

        let reader = Self {
            directory: directory.to_path_buf(),
            summary,
            replay_position: from,
            replay_limit: from + replay_length,
            segment_file_position,
            term_offset,
            term_base_segment_offset: segment_offset - term_offset,
            mapping,
            done: false,
        };

        // A position that is not the recording's start has to be a frame
        // boundary, and the only way to know is to ask the frame there
        // (`:100-107`).
        if from > summary.start_position {
            reader.check_aligned_to_fragment(term_id)?;
        }

        Ok(reader)
    }

    /// Where the read has got to.
    #[must_use]
    pub const fn replay_position(&self) -> i64 {
        self.replay_position
    }

    /// Whether there is nothing left to read.
    #[must_use]
    pub const fn is_done(&self) -> bool {
        self.done
    }

    /// Read up to `fragment_limit` frames, handing each to `handler`.
    ///
    /// The read ends at the earlier of the limit the caller asked for and the
    /// recording's own stop, and before either of those it ends at the first
    /// frame that cannot be read — a length of zero or less, which is what the
    /// preallocated tail of a segment reads as past the end of what was written.
    ///
    /// # Errors
    ///
    /// [`SegmentError`] when the next segment file is missing, or when a frame
    /// is not walkable.
    pub fn poll<H>(&mut self, fragment_limit: usize, mut handler: H) -> Result<usize, SegmentError>
    where
        H: FnMut(&SegmentFragment<'_>),
    {
        let mut fragments = 0;

        while self.replay_position < self.replay_limit && fragments < fragment_limit {
            if self.term_offset == self.summary.term_buffer_length as usize {
                self.next_term()?;
            }

            let frame_offset = self.term_offset;
            let term = self.term();

            let Some(frame_length) = read_i32_in(&term, frame_offset + FRAME_LENGTH_OFFSET) else {
                self.done = true;
                break;
            };

            if frame_length <= 0 {
                // The end: a segment is preallocated, so what follows what was
                // written is zeroes, and a zero length is where the writing
                // stopped.
                self.done = true;
                break;
            }

            let aligned = usize::try_from(align_up(frame_length, FRAME_ALIGNMENT)).unwrap_or(0);
            let payload_length = usize::try_from(frame_length).unwrap_or(0) - DATA_HEADER_LENGTH;

            let fragment = SegmentFragment {
                segment: &term,
                offset: frame_offset + DATA_HEADER_LENGTH,
                length: payload_length,
                flags: read_u8_in(&term, frame_offset + FLAGS_OFFSET).unwrap_or(0),
                header_type: read_i16_in(&term, frame_offset + TYPE_OFFSET).unwrap_or(0),
                reserved_value: read_i64_in(&term, frame_offset + RESERVED_VALUE_OFFSET)
                    .unwrap_or(0),
                position: self.replay_position,
            };

            handler(&fragment);

            self.replay_position += i64::try_from(aligned).unwrap_or(0);
            self.term_offset += aligned;
            fragments += 1;

            if self.replay_position >= self.replay_limit {
                self.done = true;
                break;
            }
        }

        Ok(fragments)
    }

    /// Move to the next term, which is a window of the segment until the segment
    /// itself runs out (`RecordingReader.nextTerm`, `:178-193`).
    fn next_term(&mut self) -> Result<(), SegmentError> {
        self.term_offset = 0;
        self.term_base_segment_offset += self.summary.term_buffer_length as usize;

        if self.term_base_segment_offset == self.summary.segment_file_length as usize {
            self.segment_file_position += i64::from(self.summary.segment_file_length);
            self.mapping = map_segment(
                &self.directory,
                self.summary.recording_id,
                self.segment_file_position,
            )?;
            self.term_base_segment_offset = 0;
        }

        Ok(())
    }

    /// The term the reader is in, as a window of the segment.
    ///
    /// A term is a window of the file rather than something the reader
    /// assembles: `term_base_segment_offset` is where it begins in the segment,
    /// and a segment is a whole number of terms, so the window always fits.
    fn term(&self) -> AtomicBuffer<'_, ReadOnly> {
        self.mapping
            .region(
                self.term_base_segment_offset,
                self.summary.term_buffer_length as usize,
            )
            .unwrap_or_else(|| {
                // The window was proven to fit when the segment was mapped: it
                // is a term inside a segment, and a segment is a whole number of
                // terms. `region` cannot fail here.
                unreachable!("the term window lies inside the segment")
            })
    }

    /// Whether the frame at the current position is the one this position
    /// implies.
    fn check_aligned_to_fragment(&self, term_id: i32) -> Result<(), SegmentError> {
        let term = self.term();
        let offset = self.term_offset;

        let frame_term_offset = read_i32_in(&term, offset + TERM_OFFSET_FIELD_OFFSET).unwrap_or(-1);
        let frame_term_id = read_i32_in(&term, offset + TERM_ID_FIELD_OFFSET).unwrap_or(-1);
        let frame_stream_id = read_i32_in(&term, offset + STREAM_ID_FIELD_OFFSET).unwrap_or(-1);

        if frame_term_offset != i32::try_from(offset).unwrap_or(-1)
            || frame_term_id != term_id
            || frame_stream_id != self.summary.stream_id
        {
            return Err(SegmentError::Malformed {
                offset: self.term_base_segment_offset + offset,
                frame_length: read_i32_in(&term, offset + FRAME_LENGTH_OFFSET).unwrap_or(0),
            });
        }

        Ok(())
    }
}

impl std::fmt::Debug for SegmentReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentReader")
            .field("segment_file_position", &self.segment_file_position)
            .field("replay_position", &self.replay_position)
            .field("replay_limit", &self.replay_limit)
            .field("term_offset", &self.term_offset)
            .field("done", &self.done)
            .finish()
    }
}

/// Map one segment file, read-only.
fn map_segment(
    directory: &Path,
    recording_id: i64,
    segment_file_position: i64,
) -> Result<MappedFile, SegmentError> {
    let path = directory.join(segment_file_name(recording_id, segment_file_position));

    MappedFile::open_readonly(&path).map_err(|error| match error.kind() {
        io::ErrorKind::NotFound => SegmentError::Missing { path },
        _ => SegmentError::Io(error),
    })
}

fn read_u8_in(buffer: &AtomicBuffer<'_, ReadOnly>, offset: usize) -> Option<u8> {
    buffer.load_u8(offset)
}

fn read_i16_in(buffer: &AtomicBuffer<'_, ReadOnly>, offset: usize) -> Option<i16> {
    buffer.load_i16(offset)
}

fn read_i32_in(buffer: &AtomicBuffer<'_, ReadOnly>, offset: usize) -> Option<i32> {
    buffer.load_i32(offset)
}

fn read_i64_in(buffer: &AtomicBuffer<'_, ReadOnly>, offset: usize) -> Option<i64> {
    buffer.load_i64(offset)
}

/// The base position encoded in a segment file's name
/// (`Catalog.parseSegmentFilePosition`).
///
/// `None` for a name that is not one: no dash, nothing between the dash and the
/// suffix, or digits that are not a number.
#[must_use]
pub fn parse_segment_file_position(file_name: &str) -> Option<i64> {
    let (_, rest) = file_name.split_once('-')?;
    let digits = rest.strip_suffix(SUFFIX)?;

    if digits.is_empty() {
        return None;
    }

    digits.parse().ok()
}

/// The segment files of one recording, by base position, highest last.
///
/// The reference indexes every file in the directory and keeps the lists by
/// recording id (`Catalog.indexSegmentFiles`), then asks for the highest of one
/// recording's list (`findSegmentFileWithHighestPosition`, which *selects* by
/// parsing every name). Same answer, and this does the selecting while listing.
#[must_use]
pub fn segment_files(directory: &Path, recording_id: i64) -> Vec<(i64, PathBuf)> {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };

    let mut files: Vec<(i64, PathBuf)> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            let position = parse_segment_file_position(&name)?;

            // The id is the part before the dash, and it has to be *this*
            // recording's: two recordings in one directory share it.
            let id: i64 = name.split_once('-')?.0.parse().ok()?;

            (id == recording_id).then_some((position, entry.path()))
        })
        .collect();

    files.sort_by_key(|(position, _)| *position);

    files
}

/// How far into a segment its frames reach, from `offset`: one past the last
/// frame that is there (`Catalog.recoverStopOffset`).
///
/// The read stops at the first frame whose length is zero or less, which is what
/// the preallocated tail of a segment reads as — and the fragment before that is
/// the last one.
///
/// # Errors
///
/// [`SegmentError::StraddlesPage`] when that last fragment **crosses a page
/// boundary and was not fully written**, which is the shape a crash in the
/// middle of a write leaves: the reference refuses it here too, in the recovery
/// path, and offers to truncate it only from `ArchiveTool verify`
/// (`Catalog.recoverStopOffset`, and the `onStraddleError` it is given at
/// `:1092-1096`).
pub fn recover_stop_offset(
    directory: &Path,
    file_name: &str,
    offset: usize,
    checksum: Option<Checksum>,
) -> Result<usize, SegmentError> {
    let path = directory.join(file_name);
    let mapping = MappedFile::open_readonly(&path).map_err(|error| match error.kind() {
        io::ErrorKind::NotFound => SegmentError::Missing { path },
        _ => SegmentError::Io(error),
    })?;

    let limit = mapping.len();
    let segment = mapping.region(0, limit).ok_or(SegmentError::Malformed {
        offset: 0,
        frame_length: i32::try_from(limit).unwrap_or(i32::MAX),
    })?;

    let mut next_offset = offset;
    let mut last_length = 0_usize;

    while next_offset < limit {
        let Some(frame_length) = read_i32_in(&segment, next_offset + FRAME_LENGTH_OFFSET) else {
            break;
        };

        if frame_length <= 0 {
            break;
        }

        let aligned = usize::try_from(align_up(frame_length, FRAME_ALIGNMENT)).unwrap_or(0);
        if 0 == aligned || next_offset + aligned > limit {
            break;
        }

        last_length = aligned;
        next_offset += aligned;
    }

    let last_offset = next_offset - last_length;

    if last_length > 0
        && straddles_page_boundary(last_offset, last_length)
        && !is_write_complete(&segment, last_offset, last_length, checksum)
    {
        return Err(SegmentError::StraddlesPage {
            offset: last_offset,
            length: last_length,
        });
    }

    Ok(next_offset)
}

/// The stop position a recording's segments imply
/// (`Catalog.computeStopPosition`).
///
/// `max_segment_file` is the highest-numbered segment the recording has, or
/// `None` when it has none — and then the answer is where the recording started,
/// because nothing was written.
///
/// The scan begins at the start term's offset when the highest segment is the
/// one the recording started in, and at zero otherwise, which is what makes this
/// correct for a recording whose first segment is not a whole one. Both branches
/// begin at or after the recording's start, so the answer cannot be before it.
///
/// The reference clamps the answer to the start position anyway
/// (`max(segmentFileBasePosition + segmentStopOffset, startPosition)`) and this
/// keeps that, with the honest note that **no test can falsify it**: the two
/// branches above make the clamp unreachable. It is cheap, it is what the
/// reference does, and the day it stops being unreachable is a day nobody will
/// be looking here.
pub fn compute_stop_position(
    directory: &Path,
    summary: &SegmentSummary,
    max_segment_file: Option<&str>,
    checksum: Option<Checksum>,
) -> Result<i64, SegmentError> {
    let Some(max_segment_file) = max_segment_file else {
        return Ok(summary.start_position);
    };

    let term_length = summary.term_buffer_length;
    let start_term_offset = summary.start_position & i64::from(term_length - 1);
    let start_term_base = summary.start_position - start_term_offset;
    let segment_base =
        parse_segment_file_position(max_segment_file).ok_or(SegmentError::Malformed {
            offset: 0,
            frame_length: 0,
        })?;

    let offset = if segment_base == start_term_base {
        usize::try_from(start_term_offset).unwrap_or(0)
    } else {
        0
    };

    let segment_stop_offset = recover_stop_offset(directory, max_segment_file, offset, checksum)?;

    Ok(
        (segment_base + i64::try_from(segment_stop_offset).unwrap_or(0))
            .max(summary.start_position),
    )
}

/// Whether a fragment reaches past the end of the page it starts in
/// (`Catalog.fragmentStraddlesPageBoundary`).
fn straddles_page_boundary(offset: usize, length: usize) -> bool {
    const PAGE_SIZE: usize = 4096;

    0 != length && (offset / PAGE_SIZE) != ((offset + (length - 1)) / PAGE_SIZE)
}

/// Whether a fragment that straddles a page was **written whole**
/// (`Catalog.isValidFragment`).
///
/// Two ways to be sure, and the reference asks both: the checksum matches, or
/// every page the fragment reaches into holds something. The second is what a
/// recording with no checksum falls back on, and it is the reason the question
/// is asked of whole pages rather than of the fragment's bytes — a fragment
/// whose straddled part was never written leaves those pages **zero**, which is
/// what a truncated write looks like.
fn is_write_complete(
    segment: &AtomicBuffer<'_, ReadOnly>,
    offset: usize,
    length: usize,
    checksum: Option<Checksum>,
) -> bool {
    if let Some(checksum) = checksum {
        let mut payload = vec![0_u8; length - DATA_HEADER_LENGTH];
        if segment
            .copy_out(offset + DATA_HEADER_LENGTH, &mut payload)
            .is_none()
        {
            return false;
        }

        let recorded = read_i32_in(segment, offset + SESSION_ID_FIELD_OFFSET).unwrap_or(0);

        if recorded == checksum.compute(&payload) {
            return true;
        }
    }

    let end = offset + length;
    let mut page = (offset / PAGE_SIZE) * PAGE_SIZE + PAGE_SIZE;

    while page < end {
        let Some(byte) = segment.load_u8(page) else {
            return false;
        };

        if 0 == byte {
            return false;
        }

        page += PAGE_SIZE;
    }

    true
}

/// The page size the straddle rule is measured in
/// (`Catalog.PAGE_SIZE`, `:110`).
const PAGE_SIZE: usize = 4096;
