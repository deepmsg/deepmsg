//! `recording.log`: which recordings hold which leadership terms.
//!
//! Counterpart of `RecordingLog.java` (1,797 lines). The file has **no header,
//! no magic and no version** — offset 0 is the first entry — and it is not
//! mapped: the reference seeks with a `FileChannel` (`:566-587`) and so does
//! this, through `FileExt::read_at`/`write_at`.
//!
//! # Entry layout (little-endian)
//!
//! ```text
//! +0   recordingId            i64
//! +8   leadershipTermId       i64
//! +16  termBaseLogPosition    i64
//! +24  logPosition            i64   (-1 while uncommitted)
//! +32  timestamp              i64
//! +40  serviceId              i32   (the consensus module itself is -1)
//! +44  entryType              i32   0 TERM, 1 SNAPSHOT, 2 STANDBY_SNAPSHOT,
//!                                    bit 31 = INVALID
//! +48  endpointLength         i32   (STANDBY_SNAPSHOT only)
//! +52  archiveEndpoint        ascii (STANDBY_SNAPSHOT only)
//! ```
//!
//! An entry occupies `align(its length, 64)` bytes (`RECORD_ALIGNMENT`,
//! `:502`), up to `MAX_ENTRY_LENGTH` 4096.
//!
//! # Three edits that are not appends
//!
//! `commitLogPosition` rewrites the eight bytes at `+24` **in place**;
//! `invalidateLatestSnapshot` sets bit 31 of the type at `+44`; restoring it
//! clears the bit **at the same offset the entry already occupies**. All three
//! address the entry by its recorded file position, which is why
//! [`Entry::position`] is part of the in-memory row and not an artefact.
//!
//! # Disk order is not logical order
//!
//! Appends can back-fill an older term, so [`RecordingLog::reload`] sorts by
//! `(leadershipTermId, termBaseLogPosition, TERM before SNAPSHOT, …)`
//! (`:504-545`). Everything that reads the log reads the sorted view.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// `RecordingLog.FILENAME` (`:432`).
pub const FILENAME: &str = "recording.log";
/// `ENTRY_TYPE_TERM` (`:437`).
pub const ENTRY_TYPE_TERM: i32 = 0;
/// `ENTRY_TYPE_SNAPSHOT` (`:442`).
pub const ENTRY_TYPE_SNAPSHOT: i32 = 1;
/// `ENTRY_TYPE_STANDBY_SNAPSHOT` (`:448`).
pub const ENTRY_TYPE_STANDBY_SNAPSHOT: i32 = 2;
/// `ENTRY_TYPE_INVALID_FLAG` (`:453`).
pub const ENTRY_TYPE_INVALID_FLAG: i32 = 1 << 31;
/// `RECORDING_ID_OFFSET` (`:458`).
pub const RECORDING_ID_OFFSET: usize = 0;
/// `LEADERSHIP_TERM_ID_OFFSET` (`:463`).
pub const LEADERSHIP_TERM_ID_OFFSET: usize = 8;
/// `TERM_BASE_LOG_POSITION_OFFSET` (`:468`).
pub const TERM_BASE_LOG_POSITION_OFFSET: usize = 16;
/// `LOG_POSITION_OFFSET` (`:473`).
pub const LOG_POSITION_OFFSET: usize = 24;
/// `TIMESTAMP_OFFSET` (`:478`).
pub const TIMESTAMP_OFFSET: usize = 32;
/// `SERVICE_ID_OFFSET` (`:483`).
pub const SERVICE_ID_OFFSET: usize = 40;
/// `ENTRY_TYPE_OFFSET` (`:488`).
pub const ENTRY_TYPE_OFFSET: usize = 44;
/// `ENDPOINT_OFFSET` (`:493`).
pub const ENDPOINT_OFFSET: usize = 48;
/// `MAX_ENTRY_LENGTH` (`:498`).
pub const MAX_ENTRY_LENGTH: usize = 4096;
/// `MAX_ENDPOINT_LENGTH` (`:500`).
pub const MAX_ENDPOINT_LENGTH: usize = MAX_ENTRY_LENGTH - ENDPOINT_OFFSET - 4;
/// `RECORD_ALIGNMENT` (`:502`).
pub const RECORD_ALIGNMENT: usize = 64;
/// `ConsensusModule.Configuration.SERVICE_ID` (`CM:450`).
pub const CONSENSUS_MODULE_SERVICE_ID: i32 = -1;
/// `Aeron.NULL_POSITION`.
pub const NULL_POSITION: i64 = -1;

/// One entry, as it sits in memory and on disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Where the entry starts in the file. Needed by every in-place edit.
    pub position: u64,
    /// The recording that holds this term or snapshot.
    pub recording_id: i64,
    /// The leadership term.
    pub leadership_term_id: i64,
    /// The log position the term starts at.
    pub term_base_log_position: i64,
    /// The log position at commit, or -1 while uncommitted.
    pub log_position: i64,
    /// Epoch milliseconds.
    pub timestamp: i64,
    /// The service, or [`CONSENSUS_MODULE_SERVICE_ID`] for the module itself.
    pub service_id: i32,
    /// One of the three [`ENTRY_TYPE_*`] values, without the invalid flag.
    pub entry_type: i32,
    /// Whether the entry is still trusted; the invalid flag on disk is this,
    /// inverted.
    pub is_valid: bool,
    /// The archive endpoint a STANDBY_SNAPSHOT names.
    pub archive_endpoint: Option<String>,
    /// Where the entry sits **on disk**, counted from zero in file order
    /// (`Entry.entryIndex`). The sorted view loses that order, and
    /// `ClusterTool sort-recording-log` is the command that needs it back: it
    /// compares this with the position in the sorted view to decide whether the
    /// file is already sorted (`ClusterToolOperator.java:1368-1379`).
    pub entry_index: u32,
}

impl Entry {
    /// `Entry.length()`: 48 bytes for the two fixed types, 52 plus the endpoint
    /// for a standby snapshot.
    pub fn length(&self) -> usize {
        match &self.archive_endpoint {
            Some(endpoint) if self.entry_type == ENTRY_TYPE_STANDBY_SNAPSHOT => {
                ENDPOINT_OFFSET + 4 + endpoint.len()
            }
            _ => ENDPOINT_OFFSET,
        }
    }

    /// `Entry.alignedLength()`: the entry rounded up to the record alignment.
    pub fn aligned_length(&self) -> usize {
        align(self.length(), RECORD_ALIGNMENT)
    }

    /// `Entry.isTerm()`.
    pub fn is_term(&self) -> bool {
        self.is_valid && self.entry_type == ENTRY_TYPE_TERM
    }

    /// `Entry.isSnapshot()` — a valid snapshot of either kind.
    pub fn is_snapshot(&self) -> bool {
        self.is_valid
            && (self.entry_type == ENTRY_TYPE_SNAPSHOT
                || self.entry_type == ENTRY_TYPE_STANDBY_SNAPSHOT)
    }
}

/// Why the log could not be read or written.
#[derive(Debug)]
pub enum RecordingLogError {
    /// The file system.
    Io(io::Error),
    /// An entry names more bytes than a record may be.
    EntryTooLong {
        /// Bytes the entry claims.
        length: usize,
    },
    /// The endpoint is longer than a record can hold.
    EndpointTooLong {
        /// Bytes the endpoint needs.
        length: usize,
    },
    /// A recovery plan named a recording the archive does not know.
    UnknownRecording(i64),
    /// A commit position named a leadership term the log has no entry for.
    UnknownLeadershipTerm(i64),
    /// A term was appended twice (`:959-962`).
    DuplicateTerm(i64),
    /// The last term has no committed position and the caller did not give a
    /// base for it (`:1398-1406`).
    PriorTermNotCommitted,
    /// A recovery plan found the consensus module's snapshot but not the one
    /// for a service it expects immediately before it.
    SnapshotMissing {
        /// The service whose snapshot is missing.
        service_id: i32,
        /// The index the module's snapshot was at.
        snapshot_index: usize,
    },
}

impl fmt::Display for RecordingLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::EntryTooLong { length } => write!(f, "entry of {length} bytes is too long"),
            Self::EndpointTooLong { length } => write!(f, "endpoint of {length} bytes is too long"),
            Self::UnknownRecording(recording_id) => {
                write!(f, "unknown recording id: {recording_id}")
            }
            Self::UnknownLeadershipTerm(leadership_term_id) => {
                write!(f, "unknown leadershipTermId={leadership_term_id}")
            }
            Self::DuplicateTerm(leadership_term_id) => {
                write!(
                    f,
                    "duplicate TERM entry for leadershipTermId={leadership_term_id}"
                )
            }
            Self::PriorTermNotCommitted => {
                write!(
                    f,
                    "prior term was not committed and termBaseLogPosition was not specified"
                )
            }
            Self::SnapshotMissing {
                service_id,
                snapshot_index,
            } => write!(
                f,
                "snapshot missing for service at index {service_id} in entries ending at {snapshot_index}"
            ),
        }
    }
}

impl std::error::Error for RecordingLogError {}

impl From<io::Error> for RecordingLogError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// The log, as a file plus its sorted in-memory view.
pub struct RecordingLog {
    file: File,
    entries: Vec<Entry>,
    length: u64,
}

impl RecordingLog {
    /// Open (or create) the log at `path` and load it.
    ///
    /// # Errors
    ///
    /// See [`RecordingLogError`]; a file that does not end on a record boundary
    /// is read up to the last whole entry, which is what the reference's
    /// `while (fileChannel.read(buffer) == ENTRY_LENGTH)` does.
    pub fn open(path: &Path) -> Result<Self, RecordingLogError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let mut log = Self {
            file,
            entries: Vec::new(),
            length: 0,
        };
        log.reload()?;
        Ok(log)
    }

    /// Re-read the whole file and sort it. `reload()` (`:648-693`).
    ///
    /// # Errors
    ///
    /// The file system, or an entry whose length is not a record length.
    pub fn reload(&mut self) -> Result<(), RecordingLogError> {
        self.entries.clear();
        self.length = self.file.metadata()?.len();
        let mut position = 0_u64;
        while position + ENDPOINT_OFFSET as u64 <= self.length {
            let mut fixed = [0_u8; ENDPOINT_OFFSET];
            if read_at(&self.file, position, &mut fixed)? < ENDPOINT_OFFSET {
                break;
            }
            let entry_type = i32::from_le_bytes(
                fixed[ENTRY_TYPE_OFFSET..ENTRY_TYPE_OFFSET + 4]
                    .try_into()
                    .unwrap(),
            );
            let is_valid = entry_type & ENTRY_TYPE_INVALID_FLAG == 0;
            let plain_type = entry_type & !ENTRY_TYPE_INVALID_FLAG;
            let archive_endpoint = if plain_type == ENTRY_TYPE_STANDBY_SNAPSHOT {
                let mut length_bytes = [0_u8; 4];
                read_at(
                    &self.file,
                    position + ENDPOINT_OFFSET as u64,
                    &mut length_bytes,
                )?;
                let length = u32::from_le_bytes(length_bytes) as usize;
                if length > MAX_ENDPOINT_LENGTH {
                    return Err(RecordingLogError::EndpointTooLong { length });
                }
                let mut bytes = vec![0_u8; length];
                read_at(
                    &self.file,
                    position + ENDPOINT_OFFSET as u64 + 4,
                    &mut bytes,
                )?;
                Some(String::from_utf8_lossy(&bytes).into_owned())
            } else {
                None
            };
            let entry = Entry {
                position,
                recording_id: i64::from_le_bytes(fixed[0..8].try_into().unwrap()),
                leadership_term_id: i64::from_le_bytes(fixed[8..16].try_into().unwrap()),
                term_base_log_position: i64::from_le_bytes(fixed[16..24].try_into().unwrap()),
                log_position: i64::from_le_bytes(fixed[24..32].try_into().unwrap()),
                timestamp: i64::from_le_bytes(fixed[32..40].try_into().unwrap()),
                service_id: i32::from_le_bytes(fixed[40..44].try_into().unwrap()),
                entry_type: plain_type,
                is_valid,
                archive_endpoint,
                entry_index: u32::try_from(self.entries.len()).unwrap_or(u32::MAX),
            };
            if entry.length() > MAX_ENTRY_LENGTH {
                return Err(RecordingLogError::EntryTooLong {
                    length: entry.length(),
                });
            }
            let step = entry.aligned_length() as u64;
            if step == 0 || position + step > self.length {
                break;
            }
            self.entries.push(entry);
            position += step;
        }
        self.entries.sort_by(compare_entries);
        Ok(())
    }

    /// The sorted entries.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Append a TERM entry (`:950`).
    ///
    /// # Errors
    ///
    /// The file system.
    pub fn append_term(
        &mut self,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        timestamp: i64,
        recording_id: i64,
        service_id: i32,
    ) -> Result<Entry, RecordingLogError> {
        self.append(Entry {
            position: 0,
            recording_id,
            leadership_term_id,
            term_base_log_position,
            log_position,
            timestamp,
            service_id,
            entry_type: ENTRY_TYPE_TERM,
            is_valid: true,
            archive_endpoint: None,
            entry_index: 0,
        })
    }

    /// Append a SNAPSHOT entry (`:1000`).
    ///
    /// # Errors
    ///
    /// The file system.
    pub fn append_snapshot(
        &mut self,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        timestamp: i64,
        recording_id: i64,
        service_id: i32,
    ) -> Result<Entry, RecordingLogError> {
        self.append(Entry {
            position: 0,
            recording_id,
            leadership_term_id,
            term_base_log_position,
            log_position,
            timestamp,
            service_id,
            entry_type: ENTRY_TYPE_SNAPSHOT,
            is_valid: true,
            archive_endpoint: None,
            entry_index: 0,
        })
    }

    /// Write one entry at the next aligned offset, in a single write.
    ///
    /// `append_term` and `append_snapshot` are the two the consensus module
    /// uses; this general form exists so a STANDBY_SNAPSHOT can be written by
    /// a later build without changing either.
    pub fn append(&mut self, mut entry: Entry) -> Result<Entry, RecordingLogError> {
        // Checked before anything is written, as `RecordingLog.java:1052` does:
        // an entry that cannot be read back is not a partial success.
        if let Some(endpoint) = &entry.archive_endpoint {
            if endpoint.len() > MAX_ENDPOINT_LENGTH {
                return Err(RecordingLogError::EndpointTooLong {
                    length: endpoint.len(),
                });
            }
        }
        entry.position = align(self.length as usize, RECORD_ALIGNMENT) as u64;
        // The disk index (`nextEntryIndex`, `RecordingLog.java:1530`): the new
        // entry is the last one in the file however the sorted view falls out.
        entry.entry_index = u32::try_from(self.entries.len()).unwrap_or(u32::MAX);
        let mut bytes = vec![0_u8; entry.aligned_length()];
        bytes[0..8].copy_from_slice(&entry.recording_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&entry.leadership_term_id.to_le_bytes());
        bytes[16..24].copy_from_slice(&entry.term_base_log_position.to_le_bytes());
        bytes[24..32].copy_from_slice(&entry.log_position.to_le_bytes());
        bytes[32..40].copy_from_slice(&entry.timestamp.to_le_bytes());
        bytes[40..44].copy_from_slice(&entry.service_id.to_le_bytes());
        let type_field = if entry.is_valid {
            entry.entry_type
        } else {
            entry.entry_type | ENTRY_TYPE_INVALID_FLAG
        };
        bytes[44..48].copy_from_slice(&type_field.to_le_bytes());
        if let Some(endpoint) = &entry.archive_endpoint {
            bytes[48..52].copy_from_slice(&(endpoint.len() as u32).to_le_bytes());
            bytes[52..52 + endpoint.len()].copy_from_slice(endpoint.as_bytes());
        }
        write_all_at(&self.file, entry.position, &bytes)?;
        self.length = entry.position + bytes.len() as u64;
        self.entries.push(entry.clone());
        self.entries.sort_by(compare_entries);
        Ok(entry)
    }

    /// Rewrite the committed position of one entry **in place** (`:1085`).
    ///
    /// # Errors
    ///
    /// The file system.
    pub fn commit_log_position(
        &mut self,
        index: usize,
        log_position: i64,
    ) -> Result<(), RecordingLogError> {
        let position = self.entries[index].position;
        write_all_at(
            &self.file,
            position + LOG_POSITION_OFFSET as u64,
            &log_position.to_le_bytes(),
        )?;
        self.entries[index].log_position = log_position;
        Ok(())
    }

    /// `commitLogPosition(leadershipTermId, logPosition)` (`:1085-1100`): the
    /// same in-place rewrite, naming the term.
    ///
    /// # Errors
    ///
    /// The file system, or [`RecordingLogError::UnknownLeadershipTerm`] — the
    /// reference's `unknown leadershipTermId`.
    pub fn commit_log_position_for_term(
        &mut self,
        leadership_term_id: i64,
        log_position: i64,
    ) -> Result<(), RecordingLogError> {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.is_term() && entry.leadership_term_id == leadership_term_id)
            .ok_or(RecordingLogError::UnknownLeadershipTerm(leadership_term_id))?;

        self.commit_log_position(index, log_position)
    }

    /// The term entry for `leadership_term_id`, if the log has one
    /// (`findTermEntry`, `:714-726`).
    pub fn find_term_entry(&self, leadership_term_id: i64) -> Option<&Entry> {
        self.entries
            .iter()
            .find(|entry| entry.is_term() && entry.leadership_term_id == leadership_term_id)
    }

    /// `ensureCoherent` (`:1369-1444`): make the log describe `leadership_term_id`
    /// before the term is used.
    ///
    /// Three shapes, exactly as the reference:
    /// * no term yet — every term from `initial_log_leadership_term_id` is
    ///   written, each starting where the previous one ended;
    /// * the log is behind — a prior term without a committed position is
    ///   committed at the base the caller supplied (or refused), then the terms
    ///   in between and the new one are appended;
    /// * the log already has the term — only the commit position moves.
    ///
    /// `None` is the reference's `NULL_VALUE` for the two optional positions.
    ///
    /// # Errors
    ///
    /// The file system, a duplicate term, or an uncommitted prior term with no
    /// base to commit it at.
    #[allow(clippy::too_many_arguments)]
    pub fn ensure_coherent(
        &mut self,
        recording_id: i64,
        initial_log_leadership_term_id: i64,
        initial_term_base_log_position: i64,
        leadership_term_id: i64,
        term_base_log_position: Option<i64>,
        log_position: Option<i64>,
        timestamp: i64,
        file_sync_level: i32,
    ) -> Result<(), RecordingLogError> {
        match self.find_last_term().cloned() {
            None => {
                let mut term_id = initial_log_leadership_term_id.max(0);
                while term_id < leadership_term_id {
                    self.append_term_entry(
                        recording_id,
                        term_id,
                        initial_term_base_log_position,
                        timestamp,
                    )?;
                    term_id += 1;
                }
                self.append_term_entry(
                    recording_id,
                    leadership_term_id,
                    term_base_log_position.unwrap_or(initial_term_base_log_position),
                    timestamp,
                )?;
            }
            Some(last) if last.leadership_term_id < leadership_term_id => {
                let mut last = last;
                if last.log_position == NULL_POSITION {
                    let base =
                        term_base_log_position.ok_or(RecordingLogError::PriorTermNotCommitted)?;
                    self.commit_log_position_for_term(last.leadership_term_id, base)?;
                    last = self
                        .find_last_term()
                        .cloned()
                        .ok_or(RecordingLogError::PriorTermNotCommitted)?;
                }

                let mut term_id = last.leadership_term_id + 1;
                while term_id < leadership_term_id {
                    self.append_term_entry(recording_id, term_id, last.log_position, timestamp)?;
                    term_id += 1;
                }
                self.append_term_entry(
                    recording_id,
                    leadership_term_id,
                    last.log_position,
                    timestamp,
                )?;
            }
            Some(_) => {}
        }

        if let Some(position) = log_position {
            self.commit_log_position_for_term(leadership_term_id, position)?;
        }

        self.force(file_sync_level)
    }

    /// The four-argument `appendTerm` (`:950-988`): the committed position is
    /// the next term's base when there is a next term, otherwise uncommitted;
    /// the term before this one is committed at this term's base.
    fn append_term_entry(
        &mut self,
        recording_id: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        timestamp: i64,
    ) -> Result<Entry, RecordingLogError> {
        if self
            .entries
            .iter()
            .any(|entry| entry.is_term() && entry.leadership_term_id == leadership_term_id)
        {
            return Err(RecordingLogError::DuplicateTerm(leadership_term_id));
        }

        if self
            .entries
            .iter()
            .any(|entry| entry.is_term() && entry.leadership_term_id == leadership_term_id - 1)
        {
            self.commit_log_position_for_term(leadership_term_id - 1, term_base_log_position)?;
        }

        let log_position = self
            .find_term_entry(leadership_term_id + 1)
            .map_or(NULL_POSITION, |entry| entry.term_base_log_position);

        self.append_term(
            leadership_term_id,
            term_base_log_position,
            log_position,
            timestamp,
            recording_id,
            -1,
        )
    }

    /// Mark the newest valid snapshot invalid by setting bit 31 of its type
    /// (`:804`). Answers its index.
    ///
    /// # Errors
    ///
    /// The file system.
    pub fn invalidate_latest_snapshot(&mut self) -> Result<Option<usize>, RecordingLogError> {
        let Some(index) = self.entries.iter().rposition(|entry| entry.is_snapshot()) else {
            return Ok(None);
        };
        self.set_valid(index, false)?;
        Ok(Some(index))
    }

    /// Clear the invalid flag at the same offset the entry already occupies
    /// (`:1461-1505`).
    ///
    /// # Errors
    ///
    /// The file system.
    pub fn restore_invalid_snapshot(&mut self, index: usize) -> Result<(), RecordingLogError> {
        self.set_valid(index, true)
    }

    fn set_valid(&mut self, index: usize, is_valid: bool) -> Result<(), RecordingLogError> {
        let entry = &mut self.entries[index];
        let value = if is_valid {
            entry.entry_type
        } else {
            entry.entry_type | ENTRY_TYPE_INVALID_FLAG
        };
        write_all_at(
            &self.file,
            entry.position + ENTRY_TYPE_OFFSET as u64,
            &value.to_le_bytes(),
        )?;
        entry.is_valid = is_valid;
        Ok(())
    }

    /// Force the file to disk at `file_sync_level` (`:610-623`): 0 does
    /// nothing, 1 is `sync_data`, 2 or more is `sync_all`.
    ///
    /// # Errors
    ///
    /// The file system.
    pub fn force(&self, file_sync_level: i32) -> Result<(), RecordingLogError> {
        if file_sync_level <= 0 {
            return Ok(());
        }
        if file_sync_level > 1 {
            self.file.sync_all()?;
        } else {
            self.file.sync_data()?;
        }
        Ok(())
    }

    /// The file's length in bytes.
    pub fn length(&self) -> u64 {
        self.length
    }
}

/// The reference's `ENTRY_COMPARATOR` (`:504-545`).
fn compare_entries(a: &Entry, b: &Entry) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match a.leadership_term_id.cmp(&b.leadership_term_id) {
        Ordering::Equal => {}
        other => return other,
    }
    match a.term_base_log_position.cmp(&b.term_base_log_position) {
        Ordering::Equal => {}
        other => return other,
    }
    if a.entry_type != b.entry_type {
        if a.entry_type == ENTRY_TYPE_TERM {
            return Ordering::Less;
        }
        if b.entry_type == ENTRY_TYPE_TERM {
            return Ordering::Greater;
        }
    } else if a.entry_type == ENTRY_TYPE_TERM {
        return a.position.cmp(&b.position);
    }
    match a.log_position.cmp(&b.log_position) {
        Ordering::Equal => {}
        other => return other,
    }
    // Reverse entry type order, as the reference does.
    match b.entry_type.cmp(&a.entry_type) {
        Ordering::Equal => {}
        other => return other,
    }
    // And reverse service id, which is what puts a service's snapshot before
    // the consensus module's at the same log position (`:540-544`).
    b.service_id.cmp(&a.service_id)
}

const fn align(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

#[cfg(unix)]
fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset)
}

#[cfg(unix)]
fn write_all_at(file: &File, offset: u64, buf: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

// ---------------------------------------------------------------------------
// Recovery: `findLastTermRecordingId`, and `createRecoveryPlan`
// (`RecordingLog.java:707-730`, `:890-935`, `:1694-1760`).
// ---------------------------------------------------------------------------

/// The six fields of an archive recording a recovery plan needs.
///
/// The reference fills its `RecordingExtent` from `AeronArchive.listRecording`
/// (`RecordingLog.java:1710-1715`). Keeping the query behind this value means
/// the plan can be tested without an archive — which is how the reference's own
/// `RecordingLogTest` tests it, with a mock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordingExtent {
    /// The recording.
    pub recording_id: i64,
    /// Its first position.
    pub start_position: i64,
    /// Its last position, or the current one for a live recording.
    pub stop_position: i64,
    /// The term the recording's log began in.
    pub initial_term_id: i32,
    /// The term buffer length it was made with.
    pub term_buffer_length: i32,
    /// Its MTU.
    pub mtu_length: i32,
    /// The recording's publication session.
    pub session_id: i32,
}

/// One snapshot the plan replays before the log (`RecordingLog.Snapshot`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// The recording holding it.
    pub recording_id: i64,
    /// The term it was taken in.
    pub leadership_term_id: i64,
    /// The log position the term started at.
    pub term_base_log_position: i64,
    /// The log position the snapshot covers.
    pub log_position: i64,
    /// Epoch milliseconds.
    pub timestamp: i64,
    /// The service, or `CONSENSUS_MODULE_SERVICE_ID`.
    pub service_id: i32,
}

/// The log the plan replays after the snapshots (`RecordingLog.Log`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryLog {
    /// The recording holding the log.
    pub recording_id: i64,
    /// The last term's id.
    pub leadership_term_id: i64,
    /// Where that term begins.
    pub term_base_log_position: i64,
    /// The committed position, or [`NULL_POSITION`].
    pub log_position: i64,
    /// Where replay starts: the snapshot's position, or the recording's start.
    pub start_position: i64,
    /// Where it stops.
    pub stop_position: i64,
    /// The recording's log shape, carried into the log publication.
    pub initial_term_id: i32,
    /// See [`RecordingExtent::term_buffer_length`].
    pub term_buffer_length: i32,
    /// See [`RecordingExtent::mtu_length`].
    pub mtu_length: i32,
    /// See [`RecordingExtent::session_id`].
    pub session_id: i32,
}

/// What to replay, and where to restart the log (`RecordingLog.RecoveryPlan`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryPlan {
    /// The last leadership term known to the log, or [`NULL_POSITION`].
    pub last_leadership_term_id: i64,
    /// The base position of that term.
    pub last_term_base_log_position: i64,
    /// The position the log was appended to.
    pub appended_log_position: i64,
    /// The position known to be committed.
    pub committed_log_position: i64,
    /// Snapshots to load, consensus module first, then by service id.
    pub snapshots: Vec<Snapshot>,
    /// The log to replay, if there is one.
    pub log: Option<RecoveryLog>,
}

impl Default for RecoveryPlan {
    /// The reference's initial values (`RecordingLog.java:894-897`): the term is
    /// **`NULL_VALUE`**, not zero. A zero here would be a term that never
    /// existed, and `Election` would start the next term at 1.
    fn default() -> Self {
        Self {
            last_leadership_term_id: NULL_POSITION,
            last_term_base_log_position: 0,
            appended_log_position: 0,
            committed_log_position: 0,
            snapshots: Vec::new(),
            log: None,
        }
    }
}

impl RecordingLog {
    /// `findLastTermRecordingId()` (`:707-711`), with the reference's
    /// `RecordingPos.NULL_RECORDING_ID` = `-1` for "none".
    #[must_use]
    pub fn find_last_term_recording_id(&self) -> i64 {
        self.find_last_term().map_or(-1, |entry| entry.recording_id)
    }

    /// `findLastTerm()` (`:718-730`): the last valid term entry.
    #[must_use]
    pub fn find_last_term(&self) -> Option<&Entry> {
        self.entries.iter().rev().find(|entry| entry.is_term())
    }

    /// `createRecoveryPlan` (`:890-935`).
    ///
    /// `list_recording` stands in for the archive: it is called once per
    /// recording the plan needs, exactly as the reference calls
    /// `AeronArchive.listRecording` (`:1710`, `:1740`). A recording the archive
    /// does not know is [`RecordingLogError::UnknownRecording`], which the
    /// reference raises as `ClusterException("unknown recording id: …")`.
    ///
    /// # Errors
    ///
    /// [`RecordingLogError::UnknownRecording`] for a named recording the closure
    /// cannot describe, and [`RecordingLogError::SnapshotMissing`] when a
    /// snapshot's service entry is not where the reference expects it.
    pub fn create_recovery_plan(
        &self,
        mut list_recording: impl FnMut(i64) -> Option<RecordingExtent>,
        service_count: i32,
        replicated_recording_id: i64,
    ) -> Result<RecoveryPlan, RecordingLogError> {
        let mut plan = RecoveryPlan::default();
        let mut snapshots: Vec<Snapshot> = Vec::new();
        let mut log: Option<RecoveryLog> = None;

        if self.entries.is_empty() {
            if replicated_recording_id != -1 {
                let Some(extent) = list_recording(replicated_recording_id) else {
                    return Err(RecordingLogError::UnknownRecording(replicated_recording_id));
                };
                log = Some(RecoveryLog {
                    recording_id: replicated_recording_id,
                    leadership_term_id: -1,
                    term_base_log_position: 0,
                    log_position: 0,
                    start_position: extent.start_position,
                    stop_position: extent.stop_position,
                    initial_term_id: extent.initial_term_id,
                    term_buffer_length: extent.term_buffer_length,
                    mtu_length: extent.mtu_length,
                    session_id: extent.session_id,
                });
            }
        } else {
            // `planRecovery` walks backwards and takes the first valid snapshot
            // of the consensus module and the first valid term with a recording.
            let mut log_index: Option<usize> = None;
            let mut snapshot_index: Option<usize> = None;

            for (i, entry) in self.entries.iter().enumerate().rev() {
                if snapshot_index.is_none()
                    && entry.is_valid
                    && entry.entry_type == ENTRY_TYPE_SNAPSHOT
                    && entry.service_id == CONSENSUS_MODULE_SERVICE_ID
                {
                    snapshot_index = Some(i);
                } else if log_index.is_none() && entry.is_term() && entry.recording_id != -1 {
                    log_index = Some(i);
                } else if snapshot_index.is_some() && log_index.is_some() {
                    break;
                }
            }

            if let Some(index) = snapshot_index {
                add_snapshots(&mut snapshots, &self.entries, service_count, index)?;
            }

            if let Some(index) = log_index {
                let entry = &self.entries[index];
                let Some(extent) = list_recording(entry.recording_id) else {
                    return Err(RecordingLogError::UnknownRecording(entry.recording_id));
                };
                let start_position = match snapshot_index {
                    None => extent.start_position,
                    Some(_) => snapshots[0].log_position,
                };
                log = Some(RecoveryLog {
                    recording_id: entry.recording_id,
                    leadership_term_id: entry.leadership_term_id,
                    term_base_log_position: entry.term_base_log_position,
                    log_position: entry.log_position,
                    start_position,
                    stop_position: extent.stop_position,
                    initial_term_id: extent.initial_term_id,
                    term_buffer_length: extent.term_buffer_length,
                    mtu_length: extent.mtu_length,
                    session_id: extent.session_id,
                });
            }
        }

        // The summary (`:897-926`): the snapshot's values first, then the log's.
        if let Some(snapshot) = snapshots.first() {
            plan.last_leadership_term_id = snapshot.leadership_term_id;
            plan.last_term_base_log_position = snapshot.term_base_log_position;
            plan.appended_log_position = snapshot.log_position;
            plan.committed_log_position = snapshot.log_position;
        }
        if let Some(log) = log {
            plan.last_leadership_term_id = log.leadership_term_id;
            plan.last_term_base_log_position = log.term_base_log_position;
            plan.appended_log_position = log.stop_position;
            if log.log_position != NULL_POSITION {
                plan.committed_log_position = log.log_position;
            }
            plan.log = Some(log);
        }
        plan.snapshots = snapshots;

        Ok(plan)
    }
}

/// `addSnapshots` (`:1300-1340`): the consensus module's snapshot, then one per
/// service, each looked for immediately before the module's entry.
fn add_snapshots(
    snapshots: &mut Vec<Snapshot>,
    entries: &[Entry],
    service_count: i32,
    snapshot_index: usize,
) -> Result<(), RecordingLogError> {
    let snapshot = &entries[snapshot_index];
    snapshots.push(Snapshot {
        recording_id: snapshot.recording_id,
        leadership_term_id: snapshot.leadership_term_id,
        term_base_log_position: snapshot.term_base_log_position,
        log_position: snapshot.log_position,
        timestamp: snapshot.timestamp,
        service_id: snapshot.service_id,
    });

    for i in 1..=service_count {
        if i as usize > snapshot_index {
            return Err(RecordingLogError::SnapshotMissing {
                service_id: i,
                snapshot_index,
            });
        }
        let entry = &entries[snapshot_index - i as usize];
        if entry.entry_type == ENTRY_TYPE_SNAPSHOT
            && entry.leadership_term_id == snapshot.leadership_term_id
            && entry.log_position == snapshot.log_position
        {
            // `snapshots.add(serviceId + 1, ..)`: the service's id is the slot,
            // so the list is ordered by service and the module's entry stays
            // first only because it is appended first.
            let slot = (entry.service_id + 1).max(0) as usize;
            let slot = slot.min(snapshots.len());
            snapshots.insert(
                slot,
                Snapshot {
                    recording_id: entry.recording_id,
                    leadership_term_id: entry.leadership_term_id,
                    term_base_log_position: entry.term_base_log_position,
                    log_position: entry.log_position,
                    timestamp: entry.timestamp,
                    service_id: entry.service_id,
                },
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_entry_is_forty_eight_bytes_aligned_to_sixty_four() {
        let entry = Entry {
            position: 0,
            recording_id: 1,
            leadership_term_id: 1,
            term_base_log_position: 0,
            log_position: -1,
            timestamp: 0,
            service_id: CONSENSUS_MODULE_SERVICE_ID,
            entry_type: ENTRY_TYPE_TERM,
            is_valid: true,
            archive_endpoint: None,
            entry_index: 0,
        };
        assert_eq!(entry.length(), 48);
        assert_eq!(entry.aligned_length(), 64);
    }

    #[test]
    fn the_offsets_are_the_reference_ones() {
        assert_eq!(RECORDING_ID_OFFSET, 0);
        assert_eq!(LEADERSHIP_TERM_ID_OFFSET, 8);
        assert_eq!(TERM_BASE_LOG_POSITION_OFFSET, 16);
        assert_eq!(LOG_POSITION_OFFSET, 24);
        assert_eq!(TIMESTAMP_OFFSET, 32);
        assert_eq!(SERVICE_ID_OFFSET, 40);
        assert_eq!(ENTRY_TYPE_OFFSET, 44);
        assert_eq!(ENDPOINT_OFFSET, 48);
        assert_eq!(RECORD_ALIGNMENT, 64);
        assert_eq!(MAX_ENTRY_LENGTH, 4096);
        assert_eq!(MAX_ENDPOINT_LENGTH, 4044);
    }
}
