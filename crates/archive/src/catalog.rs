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

use crate::mark_file::{MAJOR_VERSION, SEMANTIC_VERSION};

/// `Archive.FILENAME_CATALOG` (`Archive.java:284`).
pub const FILENAME: &str = "archive.catalog";

/// The catalog header's block length, which is also where the first record
/// goes (`CatalogHeaderEncoder.BLOCK_LENGTH`).
pub const HEADER_LENGTH: usize = HEADER_BLOCK_LENGTH as usize;

/// One record's header: `RecordingDescriptorHeaderDecoder.BLOCK_LENGTH`
/// (`Catalog.java:113`).
pub const DESCRIPTOR_HEADER_LENGTH: usize = DESCRIPTOR_HEADER_BLOCK_LENGTH as usize;

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
        let end = self.next_offset + frame.len();

        if end > self.capacity() {
            return Err(CatalogError::Full {
                needed: frame.len(),
                remaining: self.capacity().saturating_sub(self.next_offset),
            });
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
        self.next_offset = end;
        self.next_recording_id = recording_id + 1;
        self.write_header()?;

        Ok(recording_id)
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

    /// A record that does not fit is refused rather than written past the end.
    #[test]
    fn a_record_that_does_not_fit_is_refused() {
        let dir = TempDir::new();
        let mut catalog = Catalog::create(dir.path(), MIN_CAPACITY, NEXT_ID).expect("create");

        let error = catalog.add_recording(&recording(0)).expect_err("no room");
        assert!(matches!(error, CatalogError::Full { .. }), "{error:?}");

        assert_eq!(0, catalog.count_entries());
        assert_eq!(NEXT_ID, catalog.next_recording_id(), "and nothing moved");
    }
}
