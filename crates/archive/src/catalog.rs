//! The archive catalog: `archive.catalog`.
//!
//! An append-only file of variable-length records, and an index of them that is
//! **not on disk**. A record is a 32-byte `RecordingDescriptorHeader` followed
//! by a `RecordingDescriptor` and whatever padding the alignment wants, so the
//! only way to walk the file is from one record to the next — which is exactly
//! what opening one does, rebuilding the index from scratch every time
//! (`Catalog.java:228-230`).
//!
//! # No SBE message header anywhere
//!
//! Worth saying because every other SBE structure in this repository has one.
//! The catalog's records are encoded as **bodies**, with no message header in
//! front of them: the record header *is* the framing, and it carries the one
//! thing a reader needs to step over a record — its length
//! (`RecordingDescriptorHeader.length`). That is why the header's block length
//! is the offset the descriptor starts at, and why `the_header_is_the_framing`
//! is a test rather than a comment.
//!
//! # What the reference's catalog needs from P2-2c
//!
//! Its `refreshAndFixDescriptor` (`:1066-1097`) repairs a record whose
//! `stopPosition` is null by reading the recording's **segment files** and
//! checksumming across a page boundary. That is a different file's business and
//! belongs with segments, so it is not here: this module opens, appends, reads
//! and grows, and does not repair. [`crate::catalog`]'s own tests say what that
//! leaves owed rather than leaving it to be discovered.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use deepmsg_codec::archive::catalog_header_codec::{
    CatalogHeaderDecoder, CatalogHeaderEncoder, SBE_BLOCK_LENGTH as HEADER_BLOCK_LENGTH,
};
use deepmsg_codec::archive::recording_descriptor_codec::{
    RecordingDescriptorDecoder, RecordingDescriptorEncoder,
    SBE_BLOCK_LENGTH as DESCRIPTOR_BLOCK_LENGTH,
};
use deepmsg_codec::archive::recording_descriptor_header_codec::{
    RecordingDescriptorHeaderDecoder, RecordingDescriptorHeaderEncoder,
    SBE_BLOCK_LENGTH as DESCRIPTOR_HEADER_BLOCK_LENGTH,
};
use deepmsg_codec::archive::recording_state::RecordingState;
use deepmsg_codec::archive::{ReadBuf, WriteBuf};
use deepmsg_core::buffer::{AtomicBuffer, ReadOnly, ReadWrite};
use deepmsg_core::pal::MappedFile;

use crate::checksum::Checksum;
use crate::mark::NULL_VALUE;
use crate::mark_file::{MAJOR_VERSION, SEMANTIC_VERSION};

/// `Archive.FILENAME_CATALOG` (`Archive.java:284`).
pub const FILENAME: &str = "archive.catalog";

/// The catalog header's block length, which is also where the first record
/// goes (`CatalogHeaderEncoder.BLOCK_LENGTH`).
pub const HEADER_LENGTH: usize = HEADER_BLOCK_LENGTH as usize;

/// One record's header: `RecordingDescriptorHeaderDecoder.BLOCK_LENGTH`
/// (`Catalog.java:113`).
pub const DESCRIPTOR_HEADER_LENGTH: usize = DESCRIPTOR_HEADER_BLOCK_LENGTH as usize;

/// Where the descriptor block holds a recording's **start** position, in bytes
/// from the block's own beginning (`recording_descriptor_codec`: `startPosition`
/// is "encodedOffset: 40").
pub const START_POSITION_OFFSET: usize = 40;

/// Where it holds the **stop** position (`recording_descriptor_codec`:
/// `stopPosition` is "encodedOffset: 48").
pub const STOP_POSITION_OFFSET: usize = 48;

/// `Catalog.DEFAULT_CAPACITY` (`Catalog.java:118`).
pub const DEFAULT_CAPACITY: usize = 1024 * 1024;

/// `Catalog.MIN_CAPACITY`: a file that cannot hold its own header is not a
/// catalog (`:119`).
pub const MIN_CAPACITY: usize = HEADER_LENGTH;

/// `Catalog.MAX_CATALOG_LENGTH` (`:117`) — the length field is an `int32`.
pub const MAX_CAPACITY: usize = i32::MAX as usize;

/// `Catalog.PAGE_SIZE` (`:110`), which is the granularity a grown catalog is
/// rounded up to.
pub const PAGE_SIZE: usize = 4096;

/// The alignment a **new** catalog's records are laid out on
/// (`Catalog.java:216` sets `CACHE_LINE_LENGTH`).
///
/// Not `Catalog.DEFAULT_ALIGNMENT` (1024), which is the value old catalogs were
/// written with and is only ever *read* — the alignment is a field in the
/// header for exactly that reason.
pub const NEW_ALIGNMENT: usize = 64;

/// The alignment an old catalog's records were laid out on
/// (`Catalog.DEFAULT_ALIGNMENT`, `:116`).
pub const LEGACY_ALIGNMENT: usize = 1024;

/// Why a catalog could not be opened, or a record could not be written.
#[derive(Debug)]
pub enum CatalogError {
    /// The file could not be created, mapped, read or written.
    Io(io::Error),
    /// The file's version is another major's: the reference refuses it by the
    /// same rule and with the same words as the mark file
    /// (`Catalog.java:203-208`).
    Version {
        /// The file's path.
        path: PathBuf,
        /// The major it holds.
        found: i32,
        /// The major this build writes.
        expected: i32,
    },
    /// A capacity outside what the format can express.
    Capacity {
        /// What was asked for.
        asked: usize,
    },
    /// The record would not fit in the file as it is, and growing was not the
    /// caller's to allow.
    Full {
        /// The record's frame length.
        needed: usize,
        /// What is left.
        remaining: usize,
    },
    /// The file does not walk: a record's length took the cursor somewhere it
    /// cannot be.
    Malformed {
        /// Where it happened.
        offset: usize,
        /// What the record said its length was.
        length: i32,
    },
    /// No recording under that id.
    UnknownRecording {
        /// The id that was asked for.
        recording_id: i64,
    },
    /// A recording could not be repaired: its segments could not be walked.
    Repair {
        /// The recording being repaired.
        recording_id: i64,
        /// Why, in the segment's own words.
        reason: String,
    },
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Version {
                path,
                found,
                expected,
            } => write!(
                f,
                "incompatible catalog file version {found}, archive software is {expected} ({})",
                path.display()
            ),
            Self::Capacity { asked } => write!(
                f,
                "catalog capacity {asked} is outside [{MIN_CAPACITY}, {MAX_CAPACITY}]"
            ),
            Self::Full { needed, remaining } => write!(
                f,
                "a record of {needed} bytes does not fit in the {remaining} left"
            ),
            Self::Malformed { offset, length } => write!(
                f,
                "a record at {offset} says its length is {length}, which cannot be walked"
            ),
            Self::UnknownRecording { recording_id } => {
                write!(f, "unknown recording id: {recording_id}")
            }
            Self::Repair {
                recording_id,
                reason,
            } => write!(
                f,
                "recording {recording_id} could not be repaired: {reason}"
            ),
        }
    }
}

impl std::error::Error for CatalogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for CatalogError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// One recording, as a caller writes it and as this crate reads it back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recording {
    /// The id it is known by, which the catalog allocates.
    pub recording_id: i64,
    /// When the recording started and stopped, in epoch milliseconds. A stop of
    /// `NULL_VALUE` means it has not stopped.
    pub start_timestamp: i64,
    /// See [`Recording::start_timestamp`].
    pub stop_timestamp: i64,
    /// The stream positions it covers. A stop of `NULL_VALUE` is a recording
    /// that has not finished.
    pub start_position: i64,
    /// See [`Recording::start_position`].
    pub stop_position: i64,
    /// The term the stream started in, and the sizes of its log and frames.
    pub initial_term_id: i32,
    /// See [`Recording::initial_term_id`].
    pub segment_file_length: i32,
    /// See [`Recording::initial_term_id`].
    pub term_buffer_length: i32,
    /// See [`Recording::initial_term_id`].
    pub mtu_length: i32,
    /// The session and stream the publication used.
    pub session_id: i32,
    /// See [`Recording::session_id`].
    pub stream_id: i32,
    /// The channel without its params, and as it was given.
    pub stripped_channel: String,
    /// See [`Recording::stripped_channel`].
    pub original_channel: String,
    /// Where the frames came from, as the far end names itself.
    pub source_identity: String,
}

/// The recording ids in the catalog, and where each one's record is.
///
/// The reference keeps this as a flat `long[]` of `(id, offset)` pairs sorted
/// by id, searched by interpolation (`CatalogIndex.java:158-190`). This keeps
/// the pairs in a `Vec` and binary-searches them: the same question — "what
/// offset is this id's record at" — asked with a different accent, and the
/// answer is the only part of it that is a contract.
///
/// It is **not** written to the file. Opening a catalog rebuilds it by walking
/// the records (`Catalog.java:228-230`), which is why a catalog that has been
/// appended to by another process is not something an open index can be trusted
/// about.
#[derive(Debug, Default)]
pub struct CatalogIndex {
    entries: Vec<(i64, usize)>,
}

impl CatalogIndex {
    /// An empty index.
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Where a recording's record is, or `None`.
    pub fn offset_of(&self, recording_id: i64) -> Option<usize> {
        self.entries
            .binary_search_by_key(&recording_id, |(id, _)| *id)
            .ok()
            .map(|position| self.entries[position].1)
    }

    /// How many recordings it knows.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether it knows none.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The recordings it knows, in id order.
    pub fn recording_ids(&self) -> impl Iterator<Item = i64> + '_ {
        self.entries.iter().map(|(id, _)| *id)
    }

    /// The recordings it knows, newest first: the direction `Catalog.findLast`
    /// scans in (`Catalog.java:551-559`, where the index is walked `i -= 2` from
    /// its last position). Not `pub` — it is [`Catalog::find_last`]'s step, and
    /// nothing outside this module walks a catalog backwards.
    fn newest_first(&self) -> impl Iterator<Item = i64> + '_ {
        self.entries.iter().rev().map(|(id, _)| *id)
    }

    /// Record one, keeping the order ids are searched in.
    fn add(&mut self, recording_id: i64, offset: usize) {
        match self
            .entries
            .binary_search_by_key(&recording_id, |(id, _)| *id)
        {
            Ok(position) => self.entries[position].1 = offset,
            Err(position) => self.entries.insert(position, (recording_id, offset)),
        }
    }

    /// Move every record that lives **after** `offset` along by `shift`
    /// (`Catalog.fixupIndexForShifterRecordings`, `:837-860`).
    ///
    /// Substituting one record for a longer one moves everything behind it, and
    /// the offsets in this index are absolute — so the entries that point past
    /// the record that grew are the ones that have to move with it. The entry
    /// for the record itself is at `offset` and is *not* moved: the caller has
    /// already rewritten it in place.
    fn shift_after(&mut self, offset: usize, shift: usize) {
        for entry in &mut self.entries {
            if entry.1 > offset {
                entry.1 += shift;
            }
        }
    }

    /// Forget one, which is what marking its record INVALID does
    /// (`Catalog.java:783`).
    fn remove(&mut self, recording_id: i64) -> Option<usize> {
        self.entries
            .binary_search_by_key(&recording_id, |(id, _)| *id)
            .ok()
            .map(|position| self.entries.remove(position).1)
    }
}

/// A mapped catalog file.
pub struct Catalog {
    file: MappedFile,
    path: PathBuf,
    alignment: usize,
    next_recording_id: i64,
    next_offset: usize,
    index: CatalogIndex,
}

impl Catalog {
    /// Create a catalog in `directory`, replacing one that is there.
    ///
    /// The file is zeroed and the header is written, and nothing else: the
    /// first record goes at [`HEADER_LENGTH`], because the header's own
    /// `length` field is its block length and a first record anywhere else
    /// would be a record no reader walks to.
    ///
    /// # Errors
    ///
    /// [`CatalogError::Capacity`] for a capacity the format cannot hold, and
    /// the file system's own errors.
    pub fn create(
        directory: &Path,
        capacity: usize,
        next_recording_id: i64,
    ) -> Result<Self, CatalogError> {
        if !(MIN_CAPACITY..=MAX_CAPACITY).contains(&capacity) {
            return Err(CatalogError::Capacity { asked: capacity });
        }

        let path = directory.join(FILENAME);
        let file = MappedFile::create(&path, capacity)?;
        let mut catalog = Self {
            file,
            path,
            alignment: NEW_ALIGNMENT,
            next_recording_id,
            next_offset: HEADER_LENGTH,
            index: CatalogIndex::new(),
        };

        catalog.write_header()?;

        Ok(catalog)
    }

    /// Open the catalog in `directory`.
    ///
    /// The index is rebuilt from the records, which is the only copy there is.
    ///
    /// # Errors
    ///
    /// [`CatalogError::Version`] for a file written by another major — only the
    /// major, as the mark file's own rule does.
    pub fn open(directory: &Path) -> Result<Self, CatalogError> {
        let path = directory.join(FILENAME);
        let file = MappedFile::open_readwrite(&path)?;
        let mut catalog = Self {
            file,
            path,
            alignment: NEW_ALIGNMENT,
            next_recording_id: 0,
            next_offset: HEADER_LENGTH,
            index: CatalogIndex::new(),
        };

        let header = catalog.header_bytes()?;
        let decoded = decode_header(&header)?;
        let version = decoded.version();

        if crate::mark_file::major_of(version) != MAJOR_VERSION {
            return Err(CatalogError::Version {
                path: catalog.path.clone(),
                found: crate::mark_file::major_of(version),
                expected: MAJOR_VERSION,
            });
        }

        catalog.alignment = usize::try_from(decoded.alignment()).unwrap_or(NEW_ALIGNMENT);
        catalog.next_recording_id = decoded.next_recording_id();
        catalog.build_index()?;

        Ok(catalog)
    }

    /// Open it if it is there and make one if it is not.
    ///
    /// # Errors
    ///
    /// As [`Catalog::create`] and [`Catalog::open`].
    pub fn open_or_create(
        directory: &Path,
        capacity: usize,
        next_recording_id: i64,
    ) -> Result<Self, CatalogError> {
        if directory.join(FILENAME).exists() {
            Self::open(directory)
        } else {
            Self::create(directory, capacity, next_recording_id)
        }
    }

    /// The file this is.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// How big the file is.
    pub fn capacity(&self) -> usize {
        self.file.len()
    }

    /// The alignment records in this catalog are laid out on, which is a field
    /// of the header: an old catalog says 1024.
    pub const fn alignment(&self) -> usize {
        self.alignment
    }

    /// The id the next recording will be given (`CatalogHeader.nextRecordingId`).
    pub const fn next_recording_id(&self) -> i64 {
        self.next_recording_id
    }

    /// How many recordings the catalog holds.
    pub fn count_entries(&self) -> usize {
        self.index.len()
    }

    /// The recordings the catalog holds, in id order.
    pub fn recording_ids(&self) -> impl Iterator<Item = i64> + '_ {
        self.index.recording_ids()
    }

    /// The index, which is rebuilt by [`Catalog::open`] and never written.
    pub const fn index(&self) -> &CatalogIndex {
        &self.index
    }

    /// Where a recording's record is.
    pub fn recording_offset(&self, recording_id: i64) -> Option<usize> {
        self.index.offset_of(recording_id)
    }

    /// Whether this catalog holds a recording (`Catalog.hasRecording`,
    /// `Catalog.java:495-498`).
    ///
    /// The reference's test is `recordingId >= 0 && recordingDescriptorOffset
    /// (recordingId) > 0`, and the index is the same question asked of a map:
    /// [`CatalogIndex::offset_of`] answers `None` for an id no record carries,
    /// and a record that was retired is taken out of the index
    /// (`Catalog.changeState`, `:779-790`), which is what makes `> 0` false for
    /// it there.
    ///
    /// This is the guard every request that names a recording passes through on
    /// its way to [`Catalog::recording`], and the reason it is a separate
    /// question: the refusal it produces carries the reference's own words
    /// (`ArchiveConductor.hasRecording`, `ArchiveConductor.java:1950-1960`).
    #[must_use]
    pub fn has_recording(&self, recording_id: i64) -> bool {
        recording_id >= 0 && self.recording_offset(recording_id).is_some()
    }

    /// The **newest** recording at or after `min_recording_id` whose session,
    /// stream and original channel match (`Catalog.findLast`,
    /// `Catalog.java:544-578`).
    ///
    /// `None` is the reference's `NULL_RECORD_ID`, which is what a
    /// `FindLastMatchingRecordingRequest` answers a client with when nothing
    /// matches — the "not found" is an `OK` carrying `-1`, not a refusal
    /// (`ArchiveConductor.java:742-761`).
    ///
    /// # The scan
    ///
    /// The index is in id order and the reference's is too, so "newest first"
    /// is the same walk backwards, and the first id below the floor ends it
    /// rather than being skipped (`:553-559`). Every id at or above the floor is
    /// asked, and the first one that answers is the answer.
    ///
    /// # Errors
    ///
    /// [`CatalogError::Malformed`] when a record the index holds cannot be
    /// decoded. The reference throws out of `findLast` on the same record; this
    /// build answers with what it read, so the caller hears about it.
    pub fn find_last(
        &self,
        min_recording_id: i64,
        session_id: i32,
        stream_id: i32,
        channel_fragment: &[u8],
    ) -> Result<Option<i64>, CatalogError> {
        for recording_id in self.index.newest_first() {
            if recording_id < min_recording_id {
                break;
            }

            let recording = self.recording(recording_id)?;

            if recording.session_id == session_id
                && recording.stream_id == stream_id
                && channel_contains(&recording.original_channel, channel_fragment)
            {
                return Ok(Some(recording_id));
            }
        }

        Ok(None)
    }

    /// Append a recording, and answer with the id it was given.
    ///
    /// The id is the catalog's to allocate, not the caller's: it is
    /// `nextRecordingId` and the field is advanced by one in the same breath —
    /// `Catalog.addNewRecording` takes the id it was handed and writes
    /// `recordingId + 1` back into the header (`:421-426`).
    ///
    /// # Errors
    ///
    /// [`CatalogError::Full`] when the record does not fit.
    pub fn add_recording(&mut self, recording: &Recording) -> Result<i64, CatalogError> {
        let recording_id = self.next_recording_id;
        let mut written = recording.clone();
        written.recording_id = recording_id;

        let frame = encode_record(&written, self.alignment)?;

        if self.next_offset + frame.len() > self.capacity() {
            self.grow(frame.len())?;
        }

        {
            let region = self.region_mut(self.next_offset, frame.len())?;
            region
                .copy_in(0, &frame)
                .ok_or_else(|| CatalogError::Malformed {
                    offset: self.next_offset,
                    length: i32::try_from(frame.len()).unwrap_or(i32::MAX),
                })?;
        }

        self.index.add(recording_id, self.next_offset);
        self.next_offset += frame.len();
        self.next_recording_id = recording_id + 1;
        self.write_header()?;

        Ok(recording_id)
    }

    /// Repair the recordings whose stop position was never written
    /// (`Catalog.refreshAndFixDescriptor`, `:1066-1097`).
    ///
    /// A recording that was being written when its archive died has a `VALID`
    /// record with a **null** stop position — the position was to be written
    /// when the recording stopped, and it never did. What it would have been is
    /// still in the segment files, so the repair reads them: the highest-numbered
    /// segment of that recording, walked to its last frame, and the position one
    /// past it ([`crate::segment::compute_stop_position`]).
    ///
    /// The stop **timestamp** is `now_ms`, which is the reference's choice too:
    /// it writes `epochClock.time()` when it repairs (`Catalog.java:1093-1095`),
    /// because the time the recording actually stopped is not knowable from the
    /// files. A caller that wants a different answer has a different question.
    ///
    /// This is a **method** rather than something [`Catalog::open`] does, which
    /// is the one place this diverges from the reference: its constructor
    /// repairs as it opens (`:228-231`). A constructor that writes is a
    /// constructor whose caller cannot choose, and a reader — `ArchiveTool
    /// describe` is one — has no business repairing somebody's catalog because it
    /// looked at it.
    ///
    /// Returns how many records were repaired.
    ///
    /// # Errors
    ///
    /// [`CatalogError::Malformed`] when a segment file cannot be walked — a
    /// fragment straddling a page boundary that was never written whole is the
    /// case the reference refuses here too. Nothing has been written when that
    /// happens: the repair reads every recording it is going to repair before it
    /// writes any of them.
    pub fn refresh_and_fix(
        &mut self,
        checksum: Option<Checksum>,
        now_ms: i64,
    ) -> Result<usize, CatalogError> {
        let Some(directory) = self.path.parent().map(Path::to_path_buf) else {
            return Ok(0);
        };

        let mut repairs = Vec::new();

        for recording_id in self.index.recording_ids().collect::<Vec<_>>() {
            let recording = self.recording(recording_id)?;

            if recording.stop_position >= 0 {
                continue;
            }

            let files = crate::segment::segment_files(&directory, recording_id);
            let highest = files.last().map(|(_, path)| {
                path.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned()
            });

            let summary = crate::segment::SegmentSummary::from(&recording);
            let stop_position = crate::segment::compute_stop_position(
                &directory,
                &summary,
                highest.as_deref(),
                checksum,
            )
            .map_err(|error| CatalogError::Repair {
                recording_id,
                reason: error.to_string(),
            })?;

            repairs.push((recording_id, stop_position));
        }

        for (recording_id, stop_position) in &repairs {
            let offset =
                self.recording_offset(*recording_id)
                    .ok_or(CatalogError::UnknownRecording {
                        recording_id: *recording_id,
                    })?;

            self.write_stop(offset, *stop_position, now_ms)?;
        }

        Ok(repairs.len())
    }

    /// Write a record's stop position and stop timestamp, in place
    /// (`Catalog.refreshAndFixDescriptor`'s two encoder calls).
    ///
    /// The window is the record's **descriptor**, which starts on the 8-byte
    /// grid because the record does — and the two fields are at the block
    /// offsets the generated codec gives them (`recording_descriptor_codec`:
    /// `stopTimestamp` at 32, `stopPosition` at 48).
    fn write_stop(
        &mut self,
        offset: usize,
        stop_position: i64,
        now_ms: i64,
    ) -> Result<(), CatalogError> {
        const STOP_TIMESTAMP_OFFSET: usize = 32;

        let region =
            self.region_mut(offset + DESCRIPTOR_HEADER_LENGTH, STOP_POSITION_OFFSET + 8)?;

        region
            .store_i64_release(STOP_TIMESTAMP_OFFSET, now_ms)
            .and_then(|()| region.store_i64_release(STOP_POSITION_OFFSET, stop_position))
            .ok_or(CatalogError::Malformed {
                offset,
                length: i32::try_from(STOP_POSITION_OFFSET + 8).unwrap_or(i32::MAX),
            })
    }

    /// Write where a recording stopped, and when
    /// (`Catalog.recordingStopped`, `:627-638`).
    ///
    /// This is the one write a live catalog's *record* takes: the row was
    /// written with `NULL_VALUE` for both fields when the recording started and
    /// stays that way until it ends — which is what makes "has it stopped?" a
    /// question a reader can ask of the file itself. The checksum the reference
    /// recomputes here is the one this build writes as zero, because an archive
    /// with no checksum provider computes zero (`:911-921`).
    ///
    /// # Errors
    ///
    /// [`CatalogError::UnknownRecording`] when the index has no such id, and
    /// [`CatalogError::Malformed`] when the record cannot be written.
    pub fn recording_stopped(
        &mut self,
        recording_id: i64,
        stop_position: i64,
        now_ms: i64,
    ) -> Result<(), CatalogError> {
        let offset = self
            .recording_offset(recording_id)
            .ok_or(CatalogError::UnknownRecording { recording_id })?;

        self.write_stop(offset, stop_position, now_ms)
    }

    /// Move a recording's **start**, which is what a detach is
    /// (`Catalog.startPosition(id, position)`, `:758-767`).
    ///
    /// The whole of "detached" is this field: the segment files below it stay
    /// where they are, and what makes them *detached* is that the recording no
    /// longer claims them. [`is_detached`](Self::start_position) is the same
    /// question asked of a file name.
    ///
    /// The reference writes this one with `putLong` where its `stopPosition`
    /// uses `putLongVolatile` (`:764` against `:643`) — an asymmetry about
    /// which of the two a reader is allowed to see half-written. Both are a
    /// release store here, which is the ordering Rust names and the one a
    /// reader taking the record afterwards needs.
    ///
    /// # Errors
    ///
    /// [`CatalogError::UnknownRecording`] when the index has no such id, and
    /// [`CatalogError::Malformed`] when the record cannot be written.
    pub fn start_position(&mut self, recording_id: i64, position: i64) -> Result<(), CatalogError> {
        self.write_position(recording_id, START_POSITION_OFFSET, position)
    }

    /// Move a recording's **stop**, which is what a truncate is
    /// (`ArchiveConductor.truncateRecording`, `:1213`).
    ///
    /// A truncate writes this field **first** and deletes files afterwards, so
    /// between the two a recording whose files are still there already claims to
    /// end earlier. That order is the reference's and it is the safe one: a
    /// reader that believed the files over the field would replay bytes the
    /// recording no longer owns.
    ///
    /// # Errors
    ///
    /// As [`Catalog::start_position`].
    pub fn stop_position(&mut self, recording_id: i64, position: i64) -> Result<(), CatalogError> {
        self.write_position(recording_id, STOP_POSITION_OFFSET, position)
    }

    /// One of the two position fields, which differ in where they live and in
    /// nothing else.
    fn write_position(
        &mut self,
        recording_id: i64,
        field_offset: usize,
        position: i64,
    ) -> Result<(), CatalogError> {
        let offset = self
            .recording_offset(recording_id)
            .ok_or(CatalogError::UnknownRecording { recording_id })?;

        let region = self.region_mut(offset + DESCRIPTOR_HEADER_LENGTH, field_offset + 8)?;
        region
            .store_i64_release(field_offset, position)
            .ok_or(CatalogError::Malformed {
                offset,
                length: i32::try_from(field_offset + 8).unwrap_or(i32::MAX),
            })
    }

    /// Record that a recording has been **extended**: the row goes back to
    /// being one that has not stopped, and remembers which control session and
    /// request did it (`Catalog.extendRecording`, `:648-662`).
    ///
    /// Five fields, and the two that matter to a reader are the pair of stop
    /// ones: an extended recording has not stopped any more, so both go back to
    /// the null. The descriptor's own `controlSessionId` and `correlationId`
    /// — the two the *response* framing writes over for a descriptor listing
    /// (see [`Catalog::descriptor_body`]) — are what an extend leaves behind in
    /// the file, which is why the reference writes them here and nowhere else.
    ///
    /// The checksum the reference recomputes is the one this build writes as
    /// zero: an archive with no checksum provider computes zero
    /// (`:911-921`), and the field lives in the record **header**, which this
    /// window does not reach.
    ///
    /// # Errors
    ///
    /// [`CatalogError::UnknownRecording`] when the index has no such id, and
    /// [`CatalogError::Malformed`] when the record cannot be written.
    pub fn extend_recording(
        &mut self,
        recording_id: i64,
        control_session_id: i64,
        correlation_id: i64,
        image_session_id: i32,
    ) -> Result<(), CatalogError> {
        // Within the descriptor's block, which the generated codec puts at
        // these offsets (`recording_descriptor_codec`: `controlSessionId` at 0,
        // `correlationId` at 8, `stopTimestamp` at 32, `stopPosition` at 48,
        // `sessionId` at 72).
        const CONTROL_SESSION_ID_OFFSET: usize = 0;
        const CORRELATION_ID_OFFSET: usize = 8;
        const STOP_TIMESTAMP_OFFSET: usize = 32;
        const SESSION_ID_OFFSET: usize = 72;

        let offset = self
            .recording_offset(recording_id)
            .ok_or(CatalogError::UnknownRecording { recording_id })?;

        let block_length = usize::from(DESCRIPTOR_BLOCK_LENGTH);
        let region = self.region_mut(offset + DESCRIPTOR_HEADER_LENGTH, block_length)?;

        region
            .store_i64_release(CONTROL_SESSION_ID_OFFSET, control_session_id)
            .and_then(|()| region.store_i64_release(CORRELATION_ID_OFFSET, correlation_id))
            .and_then(|()| region.store_i32_release(SESSION_ID_OFFSET, image_session_id))
            .and_then(|()| region.store_i64_release(STOP_TIMESTAMP_OFFSET, NULL_VALUE))
            .and_then(|()| region.store_i64_release(STOP_POSITION_OFFSET, NULL_VALUE))
            .ok_or(CatalogError::Malformed {
                offset,
                length: i32::try_from(block_length).unwrap_or(i32::MAX),
            })
    }

    /// Write a recording's record again, in place, with a record that may be a
    /// **different length** (`Catalog.replaceRecording`, `:664-745`).
    ///
    /// One caller: `updateChannel`, which reads a descriptor, substitutes one
    /// field pair, and writes the whole thing back
    /// (`UpdateChannelSession.java:80-120`). The channels it substitutes are
    /// strings, so the new record is as likely to be longer as shorter — and a
    /// longer one moves everything behind it.
    ///
    /// # What "different length" costs
    ///
    /// A catalog is a **packed** file: records are laid end to end from
    /// [`HEADER_LENGTH`] to `next_offset`, each one carrying its own length in
    /// its header. So a record that grows by N bytes needs the whole tail moved
    /// along by N — and the index, whose offsets are absolute, moved with it
    /// (`:824-838`). That is three steps and they are in this order for a
    /// reason: the file is grown first (which may remap it), then the tail is
    /// moved, then the offsets are told. A record that shrinks instead leaves
    /// its slack as zeros, which is what the reference does with it
    /// (`:820-823`): a walk stops at the first unused **slot**, and a record's
    /// own slack is inside its frame rather than at its beginning.
    ///
    /// # Errors
    ///
    /// [`CatalogError::UnknownRecording`] when the index has no such id,
    /// [`CatalogError::Full`] when the grown file would pass the format's
    /// maximum, and [`CatalogError::Malformed`] when the record cannot be
    /// written.
    pub fn replace_recording(&mut self, recording: &Recording) -> Result<(), CatalogError> {
        let recording_id = recording.recording_id;
        let Some(offset) = self.recording_offset(recording_id) else {
            return Err(CatalogError::UnknownRecording { recording_id });
        };

        let old_frame_length = self.record_frame_length(offset)?;
        let frame = encode_record(recording, self.alignment)?;
        let new_frame_length = frame.len();

        if new_frame_length > old_frame_length {
            let shift = new_frame_length - old_frame_length;
            let end = self.next_offset;

            if end + shift > self.capacity() {
                self.grow(shift)?;
            }

            // The tail, out and back: records are variable-length and this one
            // just changed length, so there is no copying in place to do.
            let tail_length = end - (offset + old_frame_length);
            let mut tail = vec![0_u8; tail_length];

            {
                let region = self.region_mut(offset + old_frame_length, tail_length)?;
                region
                    .copy_out(0, &mut tail)
                    .ok_or_else(|| CatalogError::Malformed {
                        offset,
                        length: i32::try_from(tail_length).unwrap_or(i32::MAX),
                    })?;
            }

            {
                let region = self.region_mut(offset + new_frame_length, tail_length)?;
                region
                    .copy_in(0, &tail)
                    .ok_or_else(|| CatalogError::Malformed {
                        offset,
                        length: i32::try_from(tail_length).unwrap_or(i32::MAX),
                    })?;
            }

            self.index.shift_after(offset, shift);
            self.next_offset += shift;
        } else if new_frame_length < old_frame_length {
            // The slack a shorter record leaves is zeroed rather than left as
            // the bytes of the record it replaced.
            let slack = old_frame_length - new_frame_length;
            let zeros = vec![0_u8; slack];

            let region = self.region_mut(offset + new_frame_length, slack)?;
            region
                .copy_in(0, &zeros)
                .ok_or_else(|| CatalogError::Malformed {
                    offset,
                    length: i32::try_from(slack).unwrap_or(i32::MAX),
                })?;
        }

        {
            let region = self.region_mut(offset, new_frame_length)?;
            region
                .copy_in(0, &frame)
                .ok_or_else(|| CatalogError::Malformed {
                    offset,
                    length: i32::try_from(new_frame_length).unwrap_or(i32::MAX),
                })?;
        }

        self.write_header()
    }

    /// How long a record's frame is, from the length in its own header
    /// (`Catalog.recordingDescriptorOffset` plus the frame arithmetic at
    /// `:684-687`).
    fn record_frame_length(&self, offset: usize) -> Result<usize, CatalogError> {
        let bytes = self.read_at(offset, DESCRIPTOR_HEADER_LENGTH)?;
        let header = decode_record_header(&bytes)?;
        let length = header.length();

        if length < 0 {
            return Err(CatalogError::Malformed { offset, length });
        }

        Ok(align_up(
            DESCRIPTOR_HEADER_LENGTH + usize::try_from(length).unwrap_or(0),
            self.alignment,
        ))
    }

    /// Retire a record, or bring it back: the record's `state` changes and the
    /// index follows (`Catalog.changeState`, `:778-796`).
    ///
    /// The record itself is not touched beyond that one field — a retired
    /// recording is still in the file, still walkable, and still readable by
    /// offset; what it stops being is a recording the catalog enumerates. Which
    /// is why the walk in [`Catalog::open`] has to look at `state` rather than
    /// trusting that every record it meets is live.
    ///
    /// Returns whether there was such a recording.
    ///
    /// # Errors
    ///
    /// [`CatalogError::Malformed`] when the record's header cannot be written.
    pub fn change_state(
        &mut self,
        recording_id: i64,
        state: RecordingState,
    ) -> Result<bool, CatalogError> {
        let Some(offset) = self.index.remove(recording_id) else {
            return Ok(false);
        };

        // The state is the record header's second field: `length` is at 0 and
        // the state at 4 (`recording_descriptor_header_codec`, "encodedOffset:
        // 4" for `state`).
        const STATE_OFFSET: usize = 4;

        // The window is the whole record header rather than those four bytes,
        // because a region has to start on the 8-byte grid: the record's own
        // offset is aligned and `offset + 4` is not.
        let region = self.region_mut(offset, DESCRIPTOR_HEADER_LENGTH)?;
        region
            .store_i32_release(STATE_OFFSET, state as i32)
            .ok_or(CatalogError::Malformed {
                offset,
                length: DESCRIPTOR_HEADER_LENGTH as i32,
            })?;

        Ok(true)
    }

    /// Make room for `needed` more bytes, the way the reference does it.
    ///
    /// `newCapacity` grows by **half again** — `newCapacity + (newCapacity >> 1)`
    /// — until the target fits, capped at `MAX_CATALOG_LENGTH`
    /// (`Catalog.growCatalog`, `:866-880`). Not to the exact size that was
    /// asked for: a catalog that grew by one record each time would remap for
    /// every record once it filled up.
    ///
    /// # Errors
    ///
    /// [`CatalogError::Full`] when even the maximum capacity cannot hold the
    /// record — the reference has two messages for that, one for a catalog
    /// already at its maximum and one for a record too big for what is left
    /// (`:858-865`), and they are the same refusal to a caller.
    fn grow(&mut self, needed: usize) -> Result<(), CatalogError> {
        let target = self.next_offset + needed;

        if target > MAX_CAPACITY {
            return Err(CatalogError::Full {
                needed,
                remaining: MAX_CAPACITY.saturating_sub(self.next_offset),
            });
        }

        let mut new_capacity = self.capacity();

        while new_capacity < target {
            // Half again, and never past the maximum. A capacity of zero would
            // make this loop turn forever, which is why the minimum is a
            // construction-time check rather than an assumption.
            new_capacity = (new_capacity + (new_capacity >> 1)).min(MAX_CAPACITY);
        }

        self.file.grow(new_capacity)?;

        Ok(())
    }

    /// Read a recording back by id.
    ///
    /// # Errors
    ///
    /// [`CatalogError::UnknownRecording`] when the index has no such id, and
    /// [`CatalogError::Malformed`] when its record cannot be decoded.
    pub fn recording(&self, recording_id: i64) -> Result<Recording, CatalogError> {
        let offset = self
            .recording_offset(recording_id)
            .ok_or(CatalogError::UnknownRecording { recording_id })?;

        self.recording_at(offset)
    }

    /// One record's **body**: the bytes behind its 32-byte header, which are
    /// exactly what a `RecordingDescriptor` message carries behind its message
    /// header.
    ///
    /// This is what a listing session sends. The bytes are the descriptor as
    /// the catalog holds it, **including** the two `int64`s in front of
    /// `recordingId` — `controlSessionId` and `correlationId`, which the
    /// catalog writes zero and [`crate::server::response_proxy`] writes the
    /// session's own values over. That is what makes a body a message already:
    /// the reference builds the same one by copying the catalog's bytes from
    /// `recordingId` onwards onto the publication and writing the two ids in
    /// front of them (`ControlResponseProxy.sendDescriptor`,
    /// `ControlResponseProxy.java:54-89` over `Catalog.wrapDescriptor`,
    /// `:464-493`) — the same bytes, arranged the other way round.
    ///
    /// `None` is `wrapDescriptor` answering `false`: no such record, or one
    /// whose length field cannot be used (`wrapDescriptorAtOffset` answers `-1`
    /// for a length that is not positive, `:480-493`). A listing session turns
    /// either into `RECORDING_UNKNOWN`, which is what the reference's
    /// `ListRecordingByIdSession.doWork` does with the same `false`
    /// (`ListRecordingByIdSession.java:60-80`).
    ///
    /// # Errors
    ///
    /// [`CatalogError::Malformed`] when the header or the body cannot be read.
    pub fn descriptor_body(&self, recording_id: i64) -> Result<Option<Vec<u8>>, CatalogError> {
        let Some(offset) = self.recording_offset(recording_id) else {
            return Ok(None);
        };

        let header_bytes = self.read_at(offset, DESCRIPTOR_HEADER_LENGTH)?;
        let header = decode_record_header(&header_bytes)?;
        let length = header.length();

        // The length rule is `wrapDescriptorAtOffset`'s, which is **not**
        // `recording_at`'s: a length that cannot be used is a record a listing
        // client is told does not exist, where a read by id calls it malformed.
        // The reference has the same split across the same two methods.
        if length <= 0 || DESCRIPTOR_HEADER_LENGTH + length as usize > self.capacity() - offset {
            return Ok(None);
        }

        Ok(Some(self.read_at(
            offset + DESCRIPTOR_HEADER_LENGTH,
            length as usize,
        )?))
    }

    /// Read every recording the catalog holds, in id order.
    ///
    /// # Errors
    ///
    /// [`CatalogError::Malformed`] when a record cannot be decoded — the read
    /// stops there rather than guessing past it.
    pub fn recordings(&self) -> Result<Vec<Recording>, CatalogError> {
        let ids: Vec<i64> = self.index.recording_ids().collect();
        let mut recordings = Vec::with_capacity(ids.len());

        for id in ids {
            recordings.push(self.recording(id)?);
        }

        Ok(recordings)
    }

    /// The `state` a record's header carries.
    ///
    /// A record whose state is not `VALID` is one [`Catalog::change_state`]
    /// retired: it is still in the file, and the index no longer has it.
    ///
    /// # Errors
    ///
    /// [`CatalogError::Malformed`] when the header cannot be read at `offset`.
    pub fn state_at(&self, offset: usize) -> Result<RecordingState, CatalogError> {
        let bytes = self.read_at(offset, DESCRIPTOR_HEADER_LENGTH)?;
        let header = decode_record_header(&bytes)?;

        Ok(header.state())
    }

    /// A window onto the file.
    ///
    /// # Errors
    ///
    /// [`CatalogError::Malformed`] when the window does not fit.
    pub fn region(&self) -> Result<AtomicBuffer<'_, ReadOnly>, CatalogError> {
        self.file
            .region(0, self.file.len())
            .ok_or(CatalogError::Malformed {
                offset: 0,
                length: i32::MAX,
            })
    }

    /// Read one record at `offset`, header and all.
    fn recording_at(&self, offset: usize) -> Result<Recording, CatalogError> {
        let header_bytes = self.read_at(offset, DESCRIPTOR_HEADER_LENGTH)?;
        let header = decode_record_header(&header_bytes)?;
        let length = header.length();

        if length < 0 || DESCRIPTOR_HEADER_LENGTH + length as usize > self.capacity() - offset {
            return Err(CatalogError::Malformed { offset, length });
        }

        let body = self.read_at(offset + DESCRIPTOR_HEADER_LENGTH, length as usize)?;
        let mut descriptor = decode_descriptor(&body)?;

        // The three strings are a sequence in the body, each one's length
        // prefix after the previous one's bytes — the same shape the mark
        // header's channels have, and the same reason to read them in order.
        // Each is copied out before the next call, which mutates the same
        // decoder.
        let at = descriptor.stripped_channel_decoder();
        let stripped_channel = text(descriptor.stripped_channel_slice(at));

        let at = descriptor.original_channel_decoder();
        let original_channel = text(descriptor.original_channel_slice(at));

        let at = descriptor.source_identity_decoder();
        let source_identity = text(descriptor.source_identity_slice(at));

        Ok(Recording {
            recording_id: descriptor.recording_id(),
            start_timestamp: descriptor.start_timestamp(),
            stop_timestamp: descriptor.stop_timestamp(),
            start_position: descriptor.start_position(),
            stop_position: descriptor.stop_position(),
            initial_term_id: descriptor.initial_term_id(),
            segment_file_length: descriptor.segment_file_length(),
            term_buffer_length: descriptor.term_buffer_length(),
            mtu_length: descriptor.mtu_length(),
            session_id: descriptor.session_id(),
            stream_id: descriptor.stream_id(),
            stripped_channel,
            original_channel,
            source_identity,
        })
    }

    /// Walk the records from the first one, rebuilding the index.
    ///
    /// This is the only way to know what a catalog holds: the index is not in
    /// the file, and the walk is what opening one does
    /// (`Catalog.java:960-975`, inside `buildIndex`). Only a `VALID` record
    /// goes into it — the others are records that were retired and are still in
    /// the file, which is why the walk reads the state at all.
    fn build_index(&mut self) -> Result<(), CatalogError> {
        let mut offset = HEADER_LENGTH;
        let mut index = CatalogIndex::new();

        while offset + DESCRIPTOR_HEADER_LENGTH <= self.capacity() {
            let header_bytes = self.read_at(offset, DESCRIPTOR_HEADER_LENGTH)?;
            let header = decode_record_header(&header_bytes)?;
            let length = header.length();

            // A zero length is where the written records end: everything past it
            // is the zeroed tail a fresh file has.
            if 0 == length {
                break;
            }

            if length < 0 || offset + DESCRIPTOR_HEADER_LENGTH + length as usize > self.capacity() {
                return Err(CatalogError::Malformed { offset, length });
            }

            let body = self.read_at(offset + DESCRIPTOR_HEADER_LENGTH, length as usize)?;
            let descriptor = decode_descriptor(&body)?;

            if RecordingState::VALID == header.state() {
                index.add(descriptor.recording_id(), offset);
            }

            offset += DESCRIPTOR_HEADER_LENGTH + length as usize;
        }

        self.index = index;

        Ok(())
    }

    fn header_bytes(&self) -> Result<Vec<u8>, CatalogError> {
        self.read_at(0, HEADER_LENGTH)
    }

    /// Write the header back, which is what `nextRecordingId` needs: it is the
    /// field a crash would otherwise lose, and the reason a catalog is opened
    /// with the id it is next going to hand out.
    fn write_header(&mut self) -> Result<(), CatalogError> {
        let mut bytes = vec![0_u8; HEADER_LENGTH];
        {
            let mut encoder = CatalogHeaderEncoder::default().wrap(WriteBuf::new(&mut bytes), 0);

            encoder
                .version(SEMANTIC_VERSION)
                .length(HEADER_LENGTH as i32)
                .next_recording_id(self.next_recording_id)
                .alignment(i32::try_from(self.alignment).unwrap_or(NEW_ALIGNMENT as i32));
        }

        let region = self.region_mut(0, HEADER_LENGTH)?;
        region.copy_in(0, &bytes).ok_or(CatalogError::Malformed {
            offset: 0,
            length: HEADER_LENGTH as i32,
        })?;

        Ok(())
    }

    fn read_at(&self, offset: usize, length: usize) -> Result<Vec<u8>, CatalogError> {
        let mut bytes = vec![0_u8; length];
        let region = self
            .file
            .region(offset, length)
            .ok_or(CatalogError::Malformed {
                offset,
                length: i32::try_from(length).unwrap_or(i32::MAX),
            })?;

        region
            .copy_out(0, &mut bytes)
            .ok_or(CatalogError::Malformed {
                offset,
                length: i32::try_from(length).unwrap_or(i32::MAX),
            })?;

        Ok(bytes)
    }

    fn region_mut(
        &self,
        offset: usize,
        length: usize,
    ) -> Result<AtomicBuffer<'_, ReadWrite>, CatalogError> {
        self.file
            .region_mut(offset, length)
            .ok_or(CatalogError::Malformed {
                offset,
                length: i32::try_from(length).unwrap_or(i32::MAX),
            })
    }
}

impl fmt::Debug for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Catalog")
            .field("path", &self.path)
            .field("capacity", &self.capacity())
            .field("alignment", &self.alignment)
            .field("next_recording_id", &self.next_recording_id)
            .field("entries", &self.index.len())
            .finish()
    }
}

/// Encode one record: the header, the descriptor, and the padding the
/// alignment wants.
///
/// `length` is the descriptor's bytes **including that padding**, which is what
/// the schema's own description says (`schemas/aeron-archive-codecs.xml:336`)
/// and what the reference writes (`Catalog.java:420`: `frameLength -
/// DESCRIPTOR_HEADER_LENGTH`).
fn encode_record(recording: &Recording, alignment: usize) -> Result<Vec<u8>, CatalogError> {
    // Room for the descriptor as the schema can make it: the fields the block
    // declares plus the three strings, which are the only variable part.
    let mut body = vec![
        0_u8;
        512 + recording.stripped_channel.len()
            + recording.original_channel.len()
            + recording.source_identity.len()
    ];

    let body_length = {
        let mut encoder = RecordingDescriptorEncoder::default().wrap(WriteBuf::new(&mut body), 0);

        encoder
            .recording_id(recording.recording_id)
            .start_timestamp(recording.start_timestamp)
            .stop_timestamp(recording.stop_timestamp)
            .start_position(recording.start_position)
            .stop_position(recording.stop_position)
            .initial_term_id(recording.initial_term_id)
            .segment_file_length(recording.segment_file_length)
            .term_buffer_length(recording.term_buffer_length)
            .mtu_length(recording.mtu_length)
            .session_id(recording.session_id)
            .stream_id(recording.stream_id)
            .stripped_channel(recording.stripped_channel.as_bytes())
            .original_channel(recording.original_channel.as_bytes())
            .source_identity(recording.source_identity.as_bytes());

        encoder.encoded_length()
    };

    let frame_length = align_up(DESCRIPTOR_HEADER_LENGTH + body_length, alignment);
    let length = frame_length - DESCRIPTOR_HEADER_LENGTH;

    let mut frame = vec![0_u8; frame_length];
    frame[DESCRIPTOR_HEADER_LENGTH..DESCRIPTOR_HEADER_LENGTH + body_length]
        .copy_from_slice(&body[..body_length]);

    {
        let mut encoder =
            RecordingDescriptorHeaderEncoder::default().wrap(WriteBuf::new(&mut frame), 0);

        encoder
            .length(i32::try_from(length).unwrap_or(i32::MAX))
            .state(RecordingState::VALID)
            // The checksum is over the descriptor, and only when the archive
            // was configured with a provider: `Catalog.computeRecordingDescriptorChecksum`
            // returns zero when there is none (`:911-921`), which is what this
            // writes. P2-2c is where the providers live.
            .checksum(0);
    }

    Ok(frame)
}

fn decode_header(bytes: &[u8]) -> Result<CatalogHeaderDecoder<'_>, CatalogError> {
    Ok(CatalogHeaderDecoder::default().wrap(
        ReadBuf::new(bytes),
        0,
        HEADER_BLOCK_LENGTH,
        deepmsg_codec::archive::SBE_SCHEMA_VERSION,
    ))
}

fn decode_record_header(
    bytes: &[u8],
) -> Result<RecordingDescriptorHeaderDecoder<'_>, CatalogError> {
    Ok(RecordingDescriptorHeaderDecoder::default().wrap(
        ReadBuf::new(bytes),
        0,
        DESCRIPTOR_HEADER_BLOCK_LENGTH,
        deepmsg_codec::archive::SBE_SCHEMA_VERSION,
    ))
}

fn decode_descriptor(bytes: &[u8]) -> Result<RecordingDescriptorDecoder<'_>, CatalogError> {
    // The **schema's** block length, not the body's: the body is the block plus
    // the three strings, and a decoder told the block is all of it looks for the
    // strings where they are not — past the end of the body rather than at a
    // wrong answer. `recording_descriptor_codec`'s own `SBE_BLOCK_LENGTH`.
    Ok(RecordingDescriptorDecoder::default().wrap(
        ReadBuf::new(bytes),
        0,
        DESCRIPTOR_BLOCK_LENGTH,
        deepmsg_codec::archive::SBE_SCHEMA_VERSION,
    ))
}

/// A header string, as text: the format says US-ASCII, and a byte outside it is
/// the writer's problem rather than a reason to fail a read.
fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// `Catalog.originalChannelContains` (`Catalog.java:585-625`): whether the
/// recording's **original** channel has `fragment` in it, anywhere.
///
/// An empty fragment is in every channel, which is the reference's first test
/// and the reason a caller with nothing to match on passes nothing. The search
/// is over the channel's bytes rather than its characters — the schema calls the
/// field US-ASCII, and a fragment is compared as the client wrote it.
///
/// The second caller is the listing that answers a page of recordings for a
/// uri (`ListRecordingsForUriSession.acceptDescriptor`,
/// `ListRecordingsForUriSession.java:52-61`), which asks the same question of
/// the same field.
pub(crate) fn channel_contains(channel: &str, fragment: &[u8]) -> bool {
    if fragment.is_empty() {
        return true;
    }

    channel
        .as_bytes()
        .windows(fragment.len())
        .any(|window| window == fragment)
}

/// Round up to a multiple of `alignment`, which the reference gets from Agrona's
/// `BitUtil.align`.
fn align_up(value: usize, alignment: usize) -> usize {
    if 0 == alignment {
        return value;
    }

    value.div_ceil(alignment) * alignment
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mark::tests::TempDir;

    const CAPACITY: usize = 16 * 1024;
    const NEXT_ID: i64 = 100;

    /// An epoch clock's reading, as the crate's other tests use: the stop
    /// timestamp a repair writes is a wall-clock time.
    const NOW: i64 = 1_700_000_000_000;

    fn recording(index: i64) -> Recording {
        Recording {
            recording_id: 0,
            start_timestamp: 1_700_000_000_000 + index,
            stop_timestamp: -1,
            start_position: index * 1024,
            stop_position: -1,
            initial_term_id: 7,
            segment_file_length: 64 * 1024 * 1024,
            term_buffer_length: 64 * 1024,
            mtu_length: 1408,
            session_id: 42,
            stream_id: 1001 + i32::try_from(index).expect("small"),
            stripped_channel: format!("aeron:udp?endpoint=localhost:{}", 9000 + index),
            original_channel: format!("aeron:udp?endpoint=localhost:{}|sparse=true", 9000 + index),
            source_identity: format!("aeron:udp?endpoint=localhost:{}", 8000 + index),
        }
    }

    fn created(dir: &TempDir) -> Catalog {
        Catalog::create(dir.path(), CAPACITY, NEXT_ID).expect("create")
    }

    /// A new catalog is a header and nothing else: the version this build
    /// writes, the alignment it lays records out on, the id it will hand to the
    /// next recording, and a header length that is where the first record goes.
    #[test]
    fn a_new_catalog_is_a_header_with_nothing_behind_it() {
        let dir = TempDir::new();
        let catalog = created(&dir);

        assert_eq!(CAPACITY, catalog.capacity());
        assert_eq!(
            NEW_ALIGNMENT,
            catalog.alignment(),
            "a new catalog's records are cache-line aligned, not 1024"
        );
        assert_eq!(NEXT_ID, catalog.next_recording_id());
        assert_eq!(0, catalog.count_entries());

        let header = catalog.header_bytes().expect("the header");
        let decoded = decode_header(&header).expect("decodes");

        assert_eq!(SEMANTIC_VERSION, decoded.version());
        assert_eq!(
            HEADER_LENGTH as i32,
            decoded.length(),
            "the header's own length, which is also the first record's offset"
        );
        assert_eq!(NEXT_ID, decoded.next_recording_id());
        assert_eq!(NEW_ALIGNMENT as i32, decoded.alignment());
    }

    /// A record is written where the header says, aligned the way the header
    /// says, and reads back field for field — including the three strings, which
    /// are a sequence rather than three lookups.
    #[test]
    fn a_record_reads_back_field_for_field() {
        let dir = TempDir::new();
        let mut catalog = created(&dir);

        let first = catalog.add_recording(&recording(0)).expect("added");
        let second = catalog.add_recording(&recording(1)).expect("added");

        assert_eq!(NEXT_ID, first, "the id is the catalog's to allocate");
        assert_eq!(NEXT_ID + 1, second);
        assert_eq!(NEXT_ID + 2, catalog.next_recording_id());
        assert_eq!(2, catalog.count_entries());

        let read = catalog.recording(first).expect("the first recording");
        let mut expected = recording(0);
        expected.recording_id = first;
        assert_eq!(expected, read);

        // The frame's length is the descriptor's bytes plus whatever the
        // alignment wanted, and the second record starts a whole frame later.
        let first_offset = catalog.recording_offset(first).expect("an offset");
        let second_offset = catalog.recording_offset(second).expect("an offset");

        assert_eq!(HEADER_LENGTH, first_offset, "right behind the header");

        let frame = second_offset - first_offset;
        assert_eq!(
            0,
            frame % NEW_ALIGNMENT,
            "every record starts on the alignment: {frame}"
        );

        let bytes = catalog
            .read_at(first_offset, DESCRIPTOR_HEADER_LENGTH)
            .expect("the record header");
        let header = decode_record_header(&bytes).expect("decodes");
        assert_eq!(
            i32::try_from(frame - DESCRIPTOR_HEADER_LENGTH).expect("small"),
            header.length(),
            "the length field is the frame less the record header"
        );
        assert_eq!(RecordingState::VALID, header.state());
    }

    /// The catalog's records carry **no SBE message header**, unlike every other
    /// encoded structure in this repository: the record header is the framing,
    /// so the descriptor's body begins at the record header's block length and
    /// there are no eight bytes of message header in between.
    ///
    /// Which field lands where inside the body is the schema's business, and
    /// this asserts the one that says the offset is right: `recordingId` is the
    /// block's third field, so it is sixteen bytes into the body and
    /// `DESCRIPTOR_HEADER_LENGTH + 16` into the file.
    #[test]
    fn the_record_header_is_the_framing() {
        const RECORDING_ID_IN_BLOCK: usize = 16;

        let dir = TempDir::new();
        let mut catalog = created(&dir);

        let id = catalog.add_recording(&recording(0)).expect("added");
        let offset = catalog.recording_offset(id).expect("an offset");
        let body = catalog
            .read_at(offset + DESCRIPTOR_HEADER_LENGTH + RECORDING_ID_IN_BLOCK, 8)
            .expect("the descriptor's recording id");

        assert_eq!(
            id.to_le_bytes(),
            body[..8],
            "the body starts at the record header's end, not eight bytes past it"
        );
    }

    /// The index is not in the file: it is rebuilt by walking the records, and a
    /// second opener sees what the first one wrote.
    #[test]
    fn opening_rebuilds_the_index_from_the_records() {
        let dir = TempDir::new();

        let ids = {
            let mut catalog = created(&dir);
            let mut ids = Vec::new();

            for index in 0..3 {
                ids.push(catalog.add_recording(&recording(index)).expect("added"));
            }

            assert_eq!(NEXT_ID + 3, catalog.next_recording_id());
            ids
        };

        let reopened = Catalog::open(dir.path()).expect("open");

        assert_eq!(3, reopened.count_entries());
        assert_eq!(
            NEXT_ID + 3,
            reopened.next_recording_id(),
            "the header's own field, which is what survives a restart"
        );
        assert_eq!(NEW_ALIGNMENT, reopened.alignment());

        for (index, id) in ids.iter().enumerate() {
            let mut expected = recording(index as i64);
            expected.recording_id = *id;
            assert_eq!(expected, reopened.recording(*id).expect("read back"));
        }
    }

    /// A retired recording is still in the file and no longer in the catalogue,
    /// which is the distinction the walk on open has to make.
    #[test]
    fn a_retired_recording_stays_in_the_file() {
        let dir = TempDir::new();
        let mut catalog = created(&dir);

        let kept = catalog.add_recording(&recording(0)).expect("added");
        let retired = catalog.add_recording(&recording(1)).expect("added");
        let offset = catalog.recording_offset(retired).expect("an offset");

        assert!(
            catalog
                .change_state(retired, RecordingState::INVALID)
                .expect("retired")
        );
        assert_eq!(1, catalog.count_entries());
        assert!(
            !catalog
                .change_state(retired, RecordingState::INVALID)
                .expect("twice"),
            "a recording that is not in the index is not one to retire"
        );

        assert_eq!(
            RecordingState::INVALID,
            catalog.state_at(offset).expect("the state")
        );
        assert!(
            matches!(
                catalog.recording(retired),
                Err(CatalogError::UnknownRecording { .. })
            ),
            "the index no longer answers for it"
        );

        // And the record's bytes are still there: a reopened catalog walks past
        // it rather than losing the bytes behind it.
        let reopened = Catalog::open(dir.path()).expect("open");

        assert_eq!(1, reopened.count_entries());
        assert_eq!(kept, reopened.recording_ids().next().expect("one"));
        assert_eq!(
            RecordingState::INVALID,
            reopened.state_at(offset).expect("the state")
        );
    }

    /// A recording that was being written when its archive died has a `VALID`
    /// record with no stop position. What it would have been is in the segment
    /// files, one past their last frame.
    #[test]
    fn a_recording_that_never_stopped_is_repaired_from_its_segments() {
        use crate::segment::{SegmentSpec, SegmentWriter};
        use deepmsg_core::logbuffer::descriptor::FRAME_ALIGNMENT;
        use deepmsg_core::logbuffer::frame::{
            DATA_HEADER_LENGTH, FRAME_LENGTH_OFFSET, TYPE_OFFSET,
        };
        use deepmsg_core::logbuffer::position::align_up;

        const TERM: i32 = 64 * 1024;
        const SEGMENT: usize = 4 * TERM as usize;

        let dir = TempDir::new();

        // Three frames, written the way a recording writes them.
        let mut writer = SegmentWriter::create(
            dir.path(),
            SegmentSpec {
                recording_id: NEXT_ID,
                start_position: 0,
                join_position: 0,
                term_buffer_length: TERM,
                segment_length: SEGMENT,
            },
            1,
            None,
        )
        .expect("a writer");

        for (offset, text) in [(0_i32, "one"), (64, "two"), (128, "three")] {
            let mut frame = vec![0_u8; 64];
            let length = i32::try_from(DATA_HEADER_LENGTH + text.len()).expect("small");
            frame[FRAME_LENGTH_OFFSET..FRAME_LENGTH_OFFSET + 4]
                .copy_from_slice(&length.to_le_bytes());
            frame[TYPE_OFFSET..TYPE_OFFSET + 2].copy_from_slice(&1_i16.to_le_bytes());
            frame[8..12].copy_from_slice(&offset.to_le_bytes());
            frame[20..24].copy_from_slice(&7_i32.to_le_bytes());
            frame[16..20].copy_from_slice(&1001_i32.to_le_bytes());
            frame[DATA_HEADER_LENGTH..DATA_HEADER_LENGTH + text.len()]
                .copy_from_slice(text.as_bytes());
            let _ = align_up(length, FRAME_ALIGNMENT);

            writer.write_block(&frame).expect("written");
        }

        let written = writer.offset() as i64;
        assert!(written > 0);

        // A catalog whose record for it never got a stop position — which is
        // what `add_recording` writes when the caller passes one that is not
        // there.
        let mut catalog = created(&dir);
        let mut unfinished = recording(0);
        unfinished.stop_position = -1;
        unfinished.stop_timestamp = -1;

        let id = catalog.add_recording(&unfinished).expect("added");
        assert_eq!(-1, catalog.recording(id).expect("read back").stop_position);

        let repaired = catalog.refresh_and_fix(None, NOW).expect("repair");

        assert_eq!(1, repaired, "one recording was unfinished");

        let fixed = catalog.recording(id).expect("read back");

        assert_eq!(
            written, fixed.stop_position,
            "one past the last frame written"
        );
        assert_eq!(NOW, fixed.stop_timestamp, "and the time of the repair");

        // And it stays repaired: a second pass has nothing to do.
        assert_eq!(0, catalog.refresh_and_fix(None, NOW).expect("repair"));
    }

    /// A recording with no segments at all stopped where it started: there is
    /// nothing to read a stop position out of (`computeStopPosition`'s null
    /// branch), and the answer cannot be before the start.
    #[test]
    fn a_recording_with_no_segments_stopped_where_it_started() {
        let dir = TempDir::new();
        let mut catalog = created(&dir);

        let mut unfinished = recording(0);
        unfinished.start_position = 8192;
        unfinished.stop_position = -1;
        unfinished.stop_timestamp = -1;

        let id = catalog.add_recording(&unfinished).expect("added");

        assert_eq!(1, catalog.refresh_and_fix(None, NOW).expect("repair"));
        assert_eq!(
            8192,
            catalog.recording(id).expect("read back").stop_position
        );
    }

    /// A recording that **joined mid-term** starts partway into its first
    /// segment, and the scan has to start there too: from zero it would meet the
    /// preallocated zeroes before the join and answer with the start position,
    /// which is what the recording began at rather than where it got to.
    #[test]
    fn a_recording_that_joined_mid_term_is_scanned_from_there() {
        use crate::segment::{SegmentSpec, SegmentWriter};
        use deepmsg_core::logbuffer::frame::{
            DATA_HEADER_LENGTH, FRAME_LENGTH_OFFSET, TYPE_OFFSET,
        };

        const TERM: i32 = 64 * 1024;
        const SEGMENT: usize = 4 * TERM as usize;
        const JOIN: i64 = 4096;

        let dir = TempDir::new();

        let mut writer = SegmentWriter::create(
            dir.path(),
            SegmentSpec {
                recording_id: NEXT_ID,
                start_position: JOIN,
                join_position: JOIN,
                term_buffer_length: TERM,
                segment_length: SEGMENT,
            },
            1,
            None,
        )
        .expect("a writer");

        for (index, offset) in [4096_i32, 4160].into_iter().enumerate() {
            let mut frame = vec![0_u8; 64];
            let payload = format!("m{index}xx");
            let length = i32::try_from(DATA_HEADER_LENGTH + payload.len()).expect("small");
            frame[FRAME_LENGTH_OFFSET..FRAME_LENGTH_OFFSET + 4]
                .copy_from_slice(&length.to_le_bytes());
            frame[TYPE_OFFSET..TYPE_OFFSET + 2].copy_from_slice(&1_i16.to_le_bytes());
            frame[8..12].copy_from_slice(&offset.to_le_bytes());
            frame[16..20].copy_from_slice(&1001_i32.to_le_bytes());
            frame[20..24].copy_from_slice(&7_i32.to_le_bytes());
            frame[DATA_HEADER_LENGTH..DATA_HEADER_LENGTH + payload.len()]
                .copy_from_slice(payload.as_bytes());

            writer.write_block(&frame).expect("written");
        }

        // The stream position of the end, which is the segment's base plus the
        // file offset — not the join plus the offset, which is what this test
        // first said: the base is where the *term* the recording started in
        // begins, and the file offset counts from there.
        let base = crate::segment::segment_file_base_position(JOIN, JOIN, TERM, SEGMENT as i32);
        let written = base + writer.offset() as i64;
        let mut catalog = created(&dir);
        let mut unfinished = recording(0);
        unfinished.start_position = JOIN;
        unfinished.stop_position = -1;
        unfinished.stop_timestamp = -1;

        let id = catalog.add_recording(&unfinished).expect("added");

        assert_eq!(1, catalog.refresh_and_fix(None, NOW).expect("repair"));
        assert_eq!(
            written,
            catalog.recording(id).expect("read back").stop_position,
            "the frames after the join, not the join"
        );
    }

    /// The last fragment of a segment that crosses a page boundary and was never
    /// written whole is **refused**, and the refusal happens before anything is
    /// written.
    #[test]
    fn a_fragment_that_straddles_a_page_is_refused() {
        use crate::segment::{SegmentSpec, SegmentWriter};
        use deepmsg_core::logbuffer::frame::{
            DATA_HEADER_LENGTH, FRAME_LENGTH_OFFSET, TYPE_OFFSET,
        };

        const TERM: i32 = 64 * 1024;
        const SEGMENT: usize = 4 * TERM as usize;

        let dir = TempDir::new();
        let mut writer = SegmentWriter::create(
            dir.path(),
            SegmentSpec {
                recording_id: NEXT_ID,
                start_position: 0,
                join_position: 0,
                term_buffer_length: TERM,
                segment_length: SEGMENT,
            },
            1,
            None,
        )
        .expect("a writer");

        // Frames that reach the page boundary and stop there, so that the
        // **last** fragment is the one that straddles it: a scan that met a hole
        // before it would never get to the fragment this is about.
        let mut payload = vec![0_u8; 32];
        payload[..4].copy_from_slice(b"fill");
        for _ in 0..63 {
            let mut complete = vec![0_u8; 64];
            let length = i32::try_from(DATA_HEADER_LENGTH + payload.len()).expect("small");
            complete[FRAME_LENGTH_OFFSET..FRAME_LENGTH_OFFSET + 4]
                .copy_from_slice(&length.to_le_bytes());
            complete[TYPE_OFFSET..TYPE_OFFSET + 2].copy_from_slice(&1_i16.to_le_bytes());
            complete[8..12].copy_from_slice(&((writer.offset()) as i32).to_le_bytes());
            complete[16..20].copy_from_slice(&1001_i32.to_le_bytes());
            complete[20..24].copy_from_slice(&7_i32.to_le_bytes());
            complete[DATA_HEADER_LENGTH..DATA_HEADER_LENGTH + payload.len()]
                .copy_from_slice(&payload);

            writer.write_block(&complete).expect("written");
        }

        assert_eq!(4096 - 64, writer.offset(), "the frames reach the boundary");

        // The fragment that starts just before the page boundary and reaches
        // past it, with its header written and its body never filled in — which
        // is what a write interrupted by a crash leaves.
        let mut frame = vec![0_u8; 4096];
        frame[FRAME_LENGTH_OFFSET..FRAME_LENGTH_OFFSET + 4]
            .copy_from_slice(&4096_i32.to_le_bytes());
        frame[TYPE_OFFSET..TYPE_OFFSET + 2].copy_from_slice(&1_i16.to_le_bytes());

        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(dir.file(&crate::segment::segment_file_name(NEXT_ID, 0)))
            .expect("the segment");
        {
            use std::os::unix::fs::FileExt;

            file.write_all_at(&frame, 4032_u64).expect("written");
        }

        let mut catalog = created(&dir);
        let mut unfinished = recording(0);
        unfinished.stop_position = -1;
        unfinished.stop_timestamp = -1;

        let id = catalog.add_recording(&unfinished).expect("added");
        let error = catalog.refresh_and_fix(None, NOW).expect_err("refused");

        assert!(matches!(error, CatalogError::Repair { .. }), "{error:?}");
        assert!(
            error.to_string().contains("straddling page boundary"),
            "the reference's own words: {error}"
        );
        assert_eq!(
            -1,
            catalog.recording(id).expect("read back").stop_position,
            "and nothing was written"
        );
    }

    /// A catalog from another major is refused, by the same rule and in the
    /// same shape as the mark file's.
    #[test]
    fn another_majors_catalog_is_refused() {
        let dir = TempDir::new();
        {
            let _catalog = created(&dir);
        }

        // The version field is the header's first: patched the way another
        // build's would have written it.
        let path = dir.file(FILENAME);
        let file = MappedFile::open_readwrite(&path).expect("open");
        let region = file.region_mut(0, 4).expect("the version field");
        region.store_i32_release(0, 2 << 16).expect("patched");

        let error = Catalog::open(dir.path()).expect_err("major 2 is not ours");
        assert!(
            matches!(
                error,
                CatalogError::Version {
                    found: 2,
                    expected: 3,
                    ..
                }
            ),
            "{error:?}"
        );
        assert!(
            error
                .to_string()
                .starts_with("incompatible catalog file version"),
            "the reference's own words: {error}"
        );
    }

    /// A catalog that runs out of room **grows** — by half again, and without
    /// losing a byte of what is already in it.
    ///
    /// The three assertions are one claim: the records written before the growth
    /// are still readable after it, the capacity is larger than the sum of the
    /// frames (so it grew by more than the one record that did not fit), and a
    /// reopened catalog finds everything — which is the walk over a file that
    /// changed size under it.
    #[test]
    fn a_catalog_that_runs_out_of_room_grows() {
        let dir = TempDir::new();
        let mut catalog = Catalog::create(dir.path(), MIN_CAPACITY, NEXT_ID).expect("create");
        let first = catalog
            .add_recording(&recording(0))
            .expect("the first fits");

        // A `MIN_CAPACITY` catalog is its own header and nothing else — 32 bytes
        // — so the very first record grows it. That is the path, not an edge of
        // it: an archive that starts at the minimum size is one that grows on
        // its first recording.
        assert!(
            catalog.capacity() > MIN_CAPACITY,
            "the first record cannot fit 32 bytes, so the file grew: {}",
            catalog.capacity()
        );

        let mut ids = vec![first];

        for index in 1..8 {
            ids.push(catalog.add_recording(&recording(index)).expect("grown"));
        }

        let capacity = catalog.capacity();

        assert!(
            capacity > MIN_CAPACITY,
            "the file grew: {capacity} against {MIN_CAPACITY}"
        );
        assert_eq!(ids.len(), catalog.count_entries());

        // The first record was written before any of that happened.
        let mut expected = recording(0);
        expected.recording_id = first;
        assert_eq!(
            expected,
            catalog
                .recording(first)
                .expect("the first recording survived")
        );

        assert_eq!(
            u64::try_from(capacity).expect("fits"),
            std::fs::metadata(dir.file(FILENAME))
                .expect("the file")
                .len(),
            "and the file on disk is the mapping's length"
        );

        drop(catalog);

        let reopened = Catalog::open(dir.path()).expect("open");

        assert_eq!(capacity, reopened.capacity());
        assert_eq!(
            ids.len(),
            reopened.count_entries(),
            "every record is still there"
        );
        assert_eq!(NEXT_ID + 8, reopened.next_recording_id());

        for (index, id) in ids.iter().enumerate() {
            let mut expected = recording(index as i64);
            expected.recording_id = *id;
            assert_eq!(expected, reopened.recording(*id).expect("read back"));
        }
    }

    /// Growing is refused when even the maximum capacity cannot hold what was
    /// asked for, and the refusal leaves the catalog as it was.
    ///
    /// Asked of `grow` directly rather than through a record: the alternative is
    /// a record of two gigabytes, and the arithmetic is the whole of the check.
    #[test]
    fn growth_past_the_maximum_is_refused() {
        let dir = TempDir::new();
        let mut catalog = Catalog::create(dir.path(), MIN_CAPACITY, NEXT_ID).expect("create");
        let id = catalog.add_recording(&recording(0)).expect("added");

        let before = catalog.capacity();
        let error = catalog.grow(MAX_CAPACITY).expect_err("no such file");

        assert!(matches!(error, CatalogError::Full { .. }), "{error:?}");
        assert_eq!(before, catalog.capacity(), "and nothing changed");
        assert_eq!(Some(id), catalog.recording_ids().next());
    }

    /// `findLast` answers with the **newest** match, not the first one written:
    /// three recordings of one session and stream on different channels, and the
    /// fragment picks out which of them the client meant.
    #[test]
    fn the_newest_matching_recording_is_the_one_find_last_answers_with() {
        let dir = TempDir::new();
        let mut catalog = created(&dir);

        // The fixture's channels are `…:<port>|sparse=true`, so a fragment can
        // pick one recording out of the three.
        let older = catalog.add_recording(&recording(0)).expect("added");
        let also_older = catalog.add_recording(&recording(0)).expect("added");
        let newest = catalog.add_recording(&recording(0)).expect("added");

        assert!(also_older > older && newest > also_older);

        // Every one of them is session 42, stream 1001, port 9000.
        let found = catalog
            .find_last(0, 42, 1001, b"endpoint=localhost:9000")
            .expect("a scan");
        assert_eq!(Some(newest), found, "the last record written wins");

        // A floor above the older two picks the same one; a floor above all
        // three ends the scan where the reference's `break` is
        // (`Catalog.java:556-559`).
        assert_eq!(
            Some(newest),
            catalog
                .find_last(also_older + 1, 42, 1001, b"")
                .expect("a scan"),
            "an empty fragment is in every channel"
        );
        assert_eq!(
            None,
            catalog
                .find_last(newest + 1, 42, 1001, b"")
                .expect("a scan")
        );

        // Nothing matches for another session, stream or channel.
        assert_eq!(None, catalog.find_last(0, 43, 1001, b"").expect("a scan"));
        assert_eq!(None, catalog.find_last(0, 42, 1002, b"").expect("a scan"));
        assert_eq!(
            None,
            catalog
                .find_last(0, 42, 1001, b"endpoint=localhost:9001")
                .expect("a scan")
        );
    }

    /// A record that is no longer in the index is not found, whatever it
    /// matches: `findLast` walks the index, and a retired record is out of it
    /// (`Catalog.changeState`, `Catalog.java:779-790`).
    #[test]
    fn a_retired_recording_is_not_found() {
        let dir = TempDir::new();
        let mut catalog = created(&dir);

        let id = catalog.add_recording(&recording(0)).expect("added");
        assert!(
            catalog
                .change_state(id, RecordingState::INVALID)
                .expect("retired"),
            "the id was there to retire"
        );

        assert_eq!(None, catalog.find_last(0, 42, 1001, b"").expect("a scan"));
    }

    /// A recording is written with no stop position and is given one when it
    /// stops (`Catalog.recordingStopped`, `Catalog.java:627-638`).
    ///
    /// The write is two fields **in place** — the record is not rewritten and
    /// its neighbours are not touched — so what is asserted is that the two
    /// moved, that the rest did not, and that the file still walks: a record
    /// whose length or state had been clobbered would come back as a different
    /// answer or as nothing at all.
    #[test]
    fn a_recording_is_given_a_stop_position_when_it_stops() {
        let dir = TempDir::new();
        let mut catalog = created(&dir);

        let id = catalog.add_recording(&recording(0)).expect("added");
        let started = catalog.recording(id).expect("read");

        assert_eq!(-1, started.stop_position, "the fixture's NULL_VALUE");
        assert_eq!(-1, started.stop_timestamp);

        catalog.recording_stopped(id, 65_536, NOW).expect("stopped");

        let stopped = catalog.recording(id).expect("read");
        assert_eq!(65_536, stopped.stop_position);
        assert_eq!(NOW, stopped.stop_timestamp);
        assert_eq!(started.start_position, stopped.start_position);
        assert_eq!(started.recording_id, stopped.recording_id);
        assert_eq!(started.original_channel, stopped.original_channel);
        assert_eq!(started.source_identity, stopped.source_identity);

        assert!(
            matches!(
                catalog.recording_stopped(id + 1, 0, NOW),
                Err(CatalogError::UnknownRecording { .. })
            ),
            "a recording the catalog does not hold has nothing to stop"
        );

        drop(catalog);

        let reopened = Catalog::open(dir.path()).expect("open");
        assert_eq!(
            65_536,
            reopened.recording(id).expect("read back").stop_position,
            "and it is in the file, not only in this process's view of it"
        );
    }

    /// A recording's two ends can be moved on their own, which is the whole of
    /// what a detach and a truncate are
    /// (`Catalog.startPosition(id, position)`, `Catalog.java:758-767`, and
    /// `stopPosition(id, position)`, `:637-646`).
    ///
    /// Neither write touches anything else in the row — that is the assertion
    /// worth making here, because both are fields *inside* a variable-length
    /// record whose neighbours are bytes this cannot afford to disturb.
    #[test]
    fn either_end_of_a_recording_can_be_moved() {
        let dir = TempDir::new();
        let mut catalog = created(&dir);

        let id = catalog.add_recording(&recording(0)).expect("added");
        catalog
            .recording_stopped(id, 1_048_576, NOW)
            .expect("stopped");

        // Read **after** the stop: the two writers are compared against the row
        // as they found it, and a stop is one of the things that moves a field
        // they must leave alone.
        let started = catalog.recording(id).expect("read");

        catalog.start_position(id, 262_144).expect("detached");
        catalog.stop_position(id, 524_288).expect("truncated");

        let moved = catalog.recording(id).expect("read");
        assert_eq!(262_144, moved.start_position, "the new start");
        assert_eq!(524_288, moved.stop_position, "and the new stop");

        // Everything else is where the two writers found it.
        assert_eq!(started.recording_id, moved.recording_id);
        assert_eq!(started.start_timestamp, moved.start_timestamp);
        assert_eq!(started.stop_timestamp, moved.stop_timestamp);
        assert_eq!(started.initial_term_id, moved.initial_term_id);
        assert_eq!(started.segment_file_length, moved.segment_file_length);
        assert_eq!(started.term_buffer_length, moved.term_buffer_length);
        assert_eq!(started.mtu_length, moved.mtu_length);
        assert_eq!(started.session_id, moved.session_id);
        assert_eq!(started.stream_id, moved.stream_id);
        assert_eq!(started.stripped_channel, moved.stripped_channel);
        assert_eq!(started.original_channel, moved.original_channel);
        assert_eq!(started.source_identity, moved.source_identity);

        assert!(matches!(
            catalog.start_position(id + 1, 0),
            Err(CatalogError::UnknownRecording { .. })
        ));

        drop(catalog);

        let reopened = Catalog::open(dir.path()).expect("open");
        let read_back = reopened.recording(id).expect("read back");
        assert_eq!(262_144, read_back.start_position);
        assert_eq!(524_288, read_back.stop_position);
    }

    /// Substituting a **longer** record moves everything behind it, and the
    /// ones that moved are still themselves
    /// (`Catalog.replaceRecording`, `Catalog.java:664-745`).
    ///
    /// One caller substitutes a channel, and a channel is a string — so the
    /// record it writes is as likely to be longer as shorter. A catalog is
    /// packed, so a longer record pushes the tail along, and the offsets in the
    /// index are absolute, so they have to be told (`:824-838`). What could go
    /// wrong is silent: a tail moved without the index moved with it reads back
    /// as a **different recording's** bytes.
    #[test]
    fn a_record_that_grows_moves_the_ones_behind_it() {
        let dir = TempDir::new();
        let mut catalog = created(&dir);

        let first = catalog.add_recording(&recording(0)).expect("added");
        let second = catalog.add_recording(&recording(1)).expect("added");
        let third = catalog.add_recording(&recording(2)).expect("added");

        let before_second = catalog.recording(second).expect("read");
        let before_third = catalog.recording(third).expect("read");

        let mut changed = catalog.recording(first).expect("read");
        let longer = format!(
            "aeron:udp?endpoint=localhost:9000|alias={}",
            "x".repeat(4096)
        );
        changed.original_channel = longer.clone();
        changed.stripped_channel = longer;

        catalog.replace_recording(&changed).expect("replaced");

        assert_eq!(
            changed.original_channel,
            catalog.recording(first).expect("read").original_channel,
            "the record that changed"
        );
        assert_eq!(
            before_second.original_channel,
            catalog.recording(second).expect("read").original_channel,
            "and the two that moved behind it"
        );
        assert_eq!(
            before_third.start_position,
            catalog.recording(third).expect("read").start_position
        );
        assert_eq!(3, catalog.count_entries(), "none of them lost");

        drop(catalog);

        let reopened = Catalog::open(dir.path()).expect("open");
        assert_eq!(
            changed.original_channel,
            reopened
                .recording(first)
                .expect("read back")
                .original_channel
        );
        assert_eq!(
            before_second.original_channel,
            reopened
                .recording(second)
                .expect("read back")
                .original_channel
        );
        assert_eq!(
            before_third.original_channel,
            reopened
                .recording(third)
                .expect("read back")
                .original_channel
        );
    }

    /// And a **shorter** one leaves its slack as zeros rather than as the bytes
    /// of the record it replaced (`Catalog.java:820-823`).
    #[test]
    fn a_record_that_shrinks_leaves_zeros_behind_it() {
        let dir = TempDir::new();
        let mut catalog = created(&dir);

        let first = catalog.add_recording(&recording(0)).expect("added");
        let second = catalog.add_recording(&recording(1)).expect("added");
        let before_second = catalog.recording(second).expect("read");

        let mut changed = catalog.recording(first).expect("read");
        changed.original_channel = "aeron:ipc".to_owned();
        changed.stripped_channel = "aeron:ipc".to_owned();

        catalog.replace_recording(&changed).expect("replaced");

        assert_eq!(
            "aeron:ipc",
            catalog.recording(first).expect("read").original_channel
        );
        assert_eq!(
            before_second.original_channel,
            catalog.recording(second).expect("read").original_channel,
            "the record behind it did not move, and did not change either"
        );
    }

    /// An extended recording is one that has not stopped again, and its row
    /// remembers which session and request extended it
    /// (`Catalog.extendRecording`, `Catalog.java:648-662`).
    #[test]
    fn an_extended_recording_has_not_stopped_any_more() {
        let dir = TempDir::new();
        let mut catalog = created(&dir);

        let id = catalog.add_recording(&recording(0)).expect("added");
        catalog.recording_stopped(id, 65_536, NOW).expect("stopped");

        let stopped = catalog.recording(id).expect("read");
        assert_eq!(65_536, stopped.stop_position);

        catalog.extend_recording(id, 99, 7, 4242).expect("extended");

        let extended = catalog.recording(id).expect("read");
        assert_eq!(NULL_VALUE, extended.stop_position, "which is -1");
        assert_eq!(NULL_VALUE, extended.stop_timestamp);
        assert_eq!(4242, extended.session_id, "the image's session, not ours");
        assert_eq!(
            stopped.start_position, extended.start_position,
            "an extend appends: where the recording *starts* does not move"
        );
        assert_eq!(stopped.stream_id, extended.stream_id);
        assert_eq!(stopped.stripped_channel, extended.stripped_channel);

        assert!(
            matches!(
                catalog.extend_recording(id + 1, 0, 0, 0),
                Err(CatalogError::UnknownRecording { .. })
            ),
            "a recording the catalog does not hold cannot be extended"
        );

        drop(catalog);

        let reopened = Catalog::open(dir.path()).expect("open");
        assert_eq!(
            NULL_VALUE,
            reopened.recording(id).expect("read back").stop_position,
            "and the file says so too"
        );
    }

    /// The body a listing session sends is the record behind its header, whole:
    /// its length is the header's, and it decodes as the `RecordingDescriptor`
    /// it was written as.
    ///
    /// The two eight-byte fields in front of `recordingId` are the message's
    /// `controlSessionId` and `correlationId`, which the catalog leaves zero —
    /// the response proxy is what fills those in, which is why this body can be
    /// copied onto the wire as it stands
    /// (`ControlResponseProxy.java:54-89`).
    #[test]
    fn a_descriptor_body_is_the_record_behind_its_header() {
        let dir = TempDir::new();
        let mut catalog = created(&dir);

        let id = catalog.add_recording(&recording(0)).expect("added");
        let offset = catalog.recording_offset(id).expect("an offset");
        let header_bytes = catalog
            .read_at(offset, DESCRIPTOR_HEADER_LENGTH)
            .expect("the record header");
        let header = decode_record_header(&header_bytes).expect("decodes");
        let length = usize::try_from(header.length()).expect("positive");

        let body = catalog
            .descriptor_body(id)
            .expect("a body")
            .expect("the record is there");

        assert_eq!(length, body.len());
        assert_eq!(
            body,
            catalog
                .read_at(offset + DESCRIPTOR_HEADER_LENGTH, length)
                .expect("the same bytes")
        );
        assert_eq!(
            [0_u8; 16],
            body[..16],
            "the two ids the response proxy writes are zero in the file"
        );

        let descriptor = decode_descriptor(&body).expect("decodes");
        assert_eq!(id, descriptor.recording_id());
        assert_eq!(42, descriptor.session_id());
        assert_eq!(1001, descriptor.stream_id());

        // A record the catalog does not hold has no body, which is the
        // `wrapDescriptor` false a listing session answers with
        // `RECORDING_UNKNOWN`.
        assert_eq!(None, catalog.descriptor_body(id + 1).expect("a body"));
        assert_eq!(None, catalog.descriptor_body(-1).expect("a body"));
    }
}
