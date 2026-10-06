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

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use deepmsg_core::logbuffer::descriptor::FRAME_ALIGNMENT;
use deepmsg_core::logbuffer::frame::{
    DATA_HEADER_LENGTH, FRAME_LENGTH_OFFSET, SESSION_ID_FIELD_OFFSET, TYPE_OFFSET, TYPE_PAD,
};
use deepmsg_core::logbuffer::position::align_up;

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
    /// the next frame goes.
    ///
    /// # Errors
    ///
    /// [`SegmentError::Malformed`] for a block that is not a walkable run of
    /// frames, and [`SegmentError::Io`] for the file system — including the next
    /// segment being in the way when this one fills.
    pub fn write_block(&mut self, block: &[u8]) -> Result<(), SegmentError> {
        let length = block.len();
        if 0 == length {
            return Ok(());
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

        Ok(())
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
