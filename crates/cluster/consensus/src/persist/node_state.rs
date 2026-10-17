//! `node-state.dat`: the candidate term id an election must not forget.
//!
//! Counterpart of `NodeStateFile.java` (395 lines). It exists for one
//! property, and the property is a safety one: **a vote must never be cast
//! twice in the same term**, so the highest `candidateTermId` this node has
//! ever used is written to disk before the vote goes out.
//!
//! # Layout (schema 112, blockLength 8 header, then framed records)
//!
//! ```text
//! off  0  nodeStateHeader — a *bare* body, no message header: version i32 + padding
//! off  8  CandidateTerm frame, 16-byte message header
//!           blockLength 24, templateId 301, schemaId 112, version 10,
//!           frameLength 40, padding
//! off 24    candidateTermId i64   <- 8-byte aligned, checked at open
//! off 32    timestamp       i64   (epoch ms)
//! off 40    logPosition     i64
//! off 48  NodeStateFooter frame, templateId 304, frameLength 16, no body
//! rest    zero, up to 1 MiB (`MINIMUM_FILE_LENGTH`)
//! ```
//!
//! The scan from `:309-345` steps by `frameLength`: negative is corruption,
//! zero is unwritten, and the footer ends it. That is why a new record type
//! can be added **before** the footer without breaking older readers — and
//! why `NodeStateHeader.blockLength` is where the walk starts.
//!
//! # The write order is the safety property
//!
//! `updateCandidateTermId` (`:258-263`) writes `logPosition` and `timestamp`
//! **plainly**, then `candidateTermId` **volatile**, then forces the file. A
//! reader that sees the new term id must therefore also see the position and
//! timestamp that go with it. These three steps are not reorderable, and
//! [`NodeStateFile::update_candidate_term_id`] is the only writer.

use std::fmt;
use std::io;
use std::path::Path;

use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};
use deepmsg_core::pal::MappedFile;

/// `NodeStateFile.FILENAME`.
pub const FILENAME: &str = "node-state.dat";
/// `MINIMUM_FILE_LENGTH` (`:77`): the file is always at least 1 MiB.
pub const MINIMUM_FILE_LENGTH: usize = 1 << 20;

/// `NodeStateHeaderDecoder.BLOCK_LENGTH` — a bare body, no message header.
const HEADER_BLOCK_LENGTH: usize = 8;
/// `NodeStateHeaderDecoder.versionEncodingOffset`.
const VERSION_OFFSET: usize = 0;
/// Where the `CandidateTerm` frame starts: right after the bare header body.
const CANDIDATE_TERM_FRAME_OFFSET: usize = HEADER_BLOCK_LENGTH;
/// `CandidateTermDecoder.blockLength`.
const CANDIDATE_TERM_BLOCK_LENGTH: usize = 24;
/// The message header every record but the first carries.
const MESSAGE_HEADER_LENGTH: usize = 16;
/// `CandidateTerm` body offsets.
const CANDIDATE_TERM_ID_BODY_OFFSET: usize = 0;
const TIMESTAMP_BODY_OFFSET: usize = 8;
const LOG_POSITION_BODY_OFFSET: usize = 16;
/// The frame's own length, `16 + 24`.
const CANDIDATE_TERM_FRAME_LENGTH: usize = MESSAGE_HEADER_LENGTH + CANDIDATE_TERM_BLOCK_LENGTH;
/// `CandidateTermDecoder.TEMPLATE_ID`.
const CANDIDATE_TERM_TEMPLATE_ID: u16 = 301;
/// `NodeStateFooterDecoder.TEMPLATE_ID`.
const NODE_STATE_FOOTER_TEMPLATE_ID: u16 = 304;
/// Where the footer lands: the header plus one aligned candidate-term frame.
const FOOTER_FRAME_OFFSET: usize = CANDIDATE_TERM_FRAME_OFFSET + CANDIDATE_TERM_FRAME_LENGTH;
/// The schema id all three record types carry.
const SCHEMA_ID: u16 = 112;
/// `NodeStateHeader`'s SBE schema version.
const SCHEMA_VERSION: u16 = 10;
/// `ClusterMarkFile.MAJOR_VERSION`, which the node-state header borrows.
const MARK_FILE_MAJOR_VERSION: i32 = 0;
/// `ClusterMarkFile.SEMANTIC_VERSION` (0.3.0), what the version field holds.
const MARK_FILE_SEMANTIC_VERSION: i32 = 3 << 8;
/// `Aeron.NULL_VALUE`, the initial candidate term id.
pub const NULL_VALUE: i64 = -1;

/// Why a node-state file could not be opened.
#[derive(Debug)]
pub enum NodeStateError {
    /// The file is shorter than its own header plus one record.
    TooShort {
        /// Bytes found.
        found: usize,
        /// Bytes needed.
        needed: usize,
    },
    /// The major version does not match this build's mark-file major.
    Version {
        /// Major found.
        found: i32,
        /// Major expected.
        expected: i32,
    },
    /// The footer was not where the walk from the header put it.
    Corrupt,
    /// The file system.
    Io(io::Error),
}

impl fmt::Display for NodeStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooShort { found, needed } => {
                write!(f, "node state file is {found} bytes, needs {needed}")
            }
            Self::Version { found, expected } => {
                write!(
                    f,
                    "node state major version {found} does not match {expected}"
                )
            }
            Self::Corrupt => write!(f, "node state file is corrupt: no footer"),
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for NodeStateError {}

impl From<io::Error> for NodeStateError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// The candidate term record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct CandidateTerm {
    /// The highest term id this node has used.
    pub candidate_term_id: i64,
    /// When it was written, epoch milliseconds.
    pub timestamp_ms: i64,
    /// The log position that went with it.
    pub log_position: i64,
}

/// The mapped `node-state.dat`.
pub struct NodeStateFile {
    mapping: MappedFile,
}

impl NodeStateFile {
    /// Create the file, or open the one that is there and keep its candidate
    /// term id.
    ///
    /// A created file carries `NULL_VALUE` as its candidate term id — the
    /// reference does the same (`:124`) — because "no term yet" and "term 0"
    /// are different things.
    ///
    /// # Errors
    ///
    /// See [`NodeStateError`].
    pub fn open_or_create(path: &Path) -> Result<Self, NodeStateError> {
        if path.exists() {
            return Self::open(path);
        }
        let mapping = MappedFile::create(path, MINIMUM_FILE_LENGTH)?;
        let file = Self { mapping };
        {
            let buffer = file.header()?;
            let _ = buffer.store_i32_relaxed(VERSION_OFFSET, MARK_FILE_SEMANTIC_VERSION);
            write_candidate_term_frame(&buffer, NULL_VALUE, 0, 0);
            write_footer(&buffer);
        }
        file.sync()?;
        Ok(file)
    }

    /// Open an existing file, checking the version and the footer.
    ///
    /// # Errors
    ///
    /// [`NodeStateError::Version`] for a file from another major,
    /// [`NodeStateError::Corrupt`] when the footer is not where the walk from
    /// the header says it is.
    pub fn open(path: &Path) -> Result<Self, NodeStateError> {
        let mapping = MappedFile::open_readwrite(path)?;
        let file = Self { mapping };
        {
            let buffer = file.header()?;
            let version = buffer.load_i32(VERSION_OFFSET).unwrap_or(0);
            if major_of(version) != MARK_FILE_MAJOR_VERSION {
                return Err(NodeStateError::Version {
                    found: major_of(version),
                    expected: MARK_FILE_MAJOR_VERSION,
                });
            }
            // The walk: start at the header's block length, step by frame
            // length until the footer. A zero length is unwritten space, a
            // negative one is corruption.
            let mut offset = CANDIDATE_TERM_FRAME_OFFSET;
            let limit = buffer.len().min(MINIMUM_FILE_LENGTH);
            while offset + 16 <= limit {
                let frame_length = buffer.load_i32(offset + 8).unwrap_or(0);
                if frame_length < 0 {
                    return Err(NodeStateError::Corrupt);
                }
                if frame_length == 0 {
                    return Err(NodeStateError::Corrupt);
                }
                let template_id = load_u16(&buffer, offset + 2);
                if template_id == Some(NODE_STATE_FOOTER_TEMPLATE_ID) {
                    return Ok(file);
                }
                offset += frame_length as usize;
            }
        }
        Err(NodeStateError::Corrupt)
    }

    /// The candidate term id, with the release a reader in another thread
    /// needs to also see the position and timestamp written before it.
    pub fn candidate_term_id(&self) -> i64 {
        self.header()
            .ok()
            .and_then(|b| b.load_i64(CANDIDATE_TERM_ID_OFFSET))
            .unwrap_or(NULL_VALUE)
    }

    /// The record as a whole.
    pub fn candidate_term(&self) -> CandidateTerm {
        let Ok(buffer) = self.header() else {
            return CandidateTerm::default();
        };
        CandidateTerm {
            candidate_term_id: buffer
                .load_i64(CANDIDATE_TERM_ID_OFFSET)
                .unwrap_or(NULL_VALUE),
            timestamp_ms: buffer.load_i64(TIMESTAMP_OFFSET).unwrap_or(0),
            log_position: buffer.load_i64(LOG_POSITION_OFFSET).unwrap_or(0),
        }
    }

    /// Write a new candidate term id. `updateCandidateTermId` (`:258`).
    ///
    /// **The order is the point**: position and timestamp first, plainly; the
    /// term id last, with release ordering; then force. A reader that sees the
    /// new id therefore sees the position and timestamp that belong to it.
    ///
    /// # Errors
    ///
    /// The file system, from the force.
    pub fn update_candidate_term_id(
        &self,
        candidate_term_id: i64,
        log_position: i64,
        timestamp_ms: i64,
    ) -> Result<(), NodeStateError> {
        let buffer = self.header()?;
        let _ = buffer.store_i64_relaxed(LOG_POSITION_OFFSET, log_position);
        let _ = buffer.store_i64_relaxed(TIMESTAMP_OFFSET, timestamp_ms);
        let _ = buffer.store_i64_release(CANDIDATE_TERM_ID_OFFSET, candidate_term_id);
        self.sync()
    }

    /// Advance the candidate term id, but only upwards. `proposeMaxCandidateTermId`
    /// (`:274`).
    ///
    /// # Errors
    ///
    /// The file system, from the force.
    pub fn propose_max_candidate_term_id(
        &self,
        candidate_term_id: i64,
        log_position: i64,
        timestamp_ms: i64,
    ) -> Result<(), NodeStateError> {
        if candidate_term_id > self.candidate_term_id() {
            return self.update_candidate_term_id(candidate_term_id, log_position, timestamp_ms);
        }
        Ok(())
    }

    /// Force the mapping to disk.
    pub fn sync(&self) -> Result<(), NodeStateError> {
        self.mapping.sync()?;
        Ok(())
    }

    fn header(&self) -> Result<AtomicBuffer<'_, ReadWrite>, io::Error> {
        self.mapping
            .region_mut(0, MINIMUM_FILE_LENGTH)
            .ok_or_else(|| io::Error::other("node state file is shorter than its minimum"))
    }
}

/// `SemanticVersion.major`.
pub const fn major_of(version: i32) -> i32 {
    (version >> 16) & 0xff
}

fn write_candidate_term_frame(
    buffer: &AtomicBuffer<'_, ReadWrite>,
    candidate_term_id: i64,
    timestamp_ms: i64,
    log_position: i64,
) {
    // 16-byte message header.
    let _ = buffer.copy_in(
        CANDIDATE_TERM_FRAME_OFFSET,
        &CANDIDATE_TERM_BLOCK_LENGTH.to_le_bytes(),
    );
    let _ = buffer.copy_in(
        CANDIDATE_TERM_FRAME_OFFSET + 2,
        &CANDIDATE_TERM_TEMPLATE_ID.to_le_bytes(),
    );
    let _ = buffer.copy_in(CANDIDATE_TERM_FRAME_OFFSET + 4, &SCHEMA_ID.to_le_bytes());
    let _ = buffer.copy_in(
        CANDIDATE_TERM_FRAME_OFFSET + 6,
        &SCHEMA_VERSION.to_le_bytes(),
    );
    let _ = buffer.copy_in(
        CANDIDATE_TERM_FRAME_OFFSET + 8,
        &(CANDIDATE_TERM_FRAME_LENGTH as i32).to_le_bytes(),
    );
    let _ = buffer.store_i64_relaxed(CANDIDATE_TERM_ID_OFFSET, candidate_term_id);
    let _ = buffer.store_i64_relaxed(TIMESTAMP_OFFSET, timestamp_ms);
    let _ = buffer.store_i64_relaxed(LOG_POSITION_OFFSET, log_position);
}

fn write_footer(buffer: &AtomicBuffer<'_, ReadWrite>) {
    let _ = buffer.copy_in(FOOTER_FRAME_OFFSET, &0u16.to_le_bytes());
    let _ = buffer.copy_in(
        FOOTER_FRAME_OFFSET + 2,
        &NODE_STATE_FOOTER_TEMPLATE_ID.to_le_bytes(),
    );
    let _ = buffer.copy_in(FOOTER_FRAME_OFFSET + 4, &SCHEMA_ID.to_le_bytes());
    let _ = buffer.copy_in(FOOTER_FRAME_OFFSET + 6, &SCHEMA_VERSION.to_le_bytes());
    let _ = buffer.copy_in(
        FOOTER_FRAME_OFFSET + 8,
        &(MESSAGE_HEADER_LENGTH as i32).to_le_bytes(),
    );
}

fn load_u16<A>(buffer: &AtomicBuffer<'_, A>, offset: usize) -> Option<u16> {
    let mut bytes = [0_u8; 2];
    buffer.copy_out(offset, &mut bytes)?;
    Some(u16::from_le_bytes(bytes))
}

/// File offsets, spelled once so the tests and the layout comment agree.
const CANDIDATE_TERM_ID_OFFSET: usize =
    CANDIDATE_TERM_FRAME_OFFSET + MESSAGE_HEADER_LENGTH + CANDIDATE_TERM_ID_BODY_OFFSET;
const TIMESTAMP_OFFSET: usize =
    CANDIDATE_TERM_FRAME_OFFSET + MESSAGE_HEADER_LENGTH + TIMESTAMP_BODY_OFFSET;
const LOG_POSITION_OFFSET: usize =
    CANDIDATE_TERM_FRAME_OFFSET + MESSAGE_HEADER_LENGTH + LOG_POSITION_BODY_OFFSET;
