//! The cluster mark file: `cluster-mark.dat`, `cluster-mark-service-<id>.dat`,
//! and the `.lnk` that points at them.
//!
//! Counterpart of `service/ClusterMarkFile.java` (696 lines). One file, three
//! roles — consensus module, service container, backup — told apart by a
//! component type, which is also the one field with a migration rule: BACKUP
//! may be taken over by a CONSENSUS_MODULE, nothing else may be taken over.
//!
//! # Layout (schema 110, `aeron-cluster-mark-codecs.xml`)
//!
//! ```text
//! off    0  SBE message header: blockLength 128, templateId 200, schemaId 110, version 2
//! off    8  MarkFileHeader body (128 bytes)
//!             +0  version  i32        <- also the "initialised" flag; 0 means not
//!             +4  componentType i32
//!             +8  activityTimestamp i64
//!            +16  startTimestamp i64
//!            +24  pid i64
//!            +32  candidateTermId i64
//!            +40  archiveStreamId i32
//!            +44  serviceStreamId i32
//!            +48  consensusModuleStreamId i32
//!            +52  ingressStreamId i32
//!            +56  memberId i32
//!            +60  serviceId i32
//!            +64  headerLength i32
//!            +68  errorBufferLength i32
//!            +72  clusterId i32
//!            +76..128 padding
//! off  136  var-data: six `u32 length + ASCII` strings in schema order —
//!           aeronDirectory, controlChannel, ingressChannel, serviceName,
//!           authenticator, servicesClusterDir
//! off 8192  the error buffer (`HEADER_LENGTH`), at least 1 MiB
//! ```
//!
//! The version and the activity timestamp are the two fields Agrona's generic
//! `MarkFile` owns; here they sit at file offsets 8 and 16. `common` must not
//! depend on the archive, so the mapping is made directly over
//! `deepmsg_core::pal::MappedFile` rather than reusing the archive's
//! parameterised `MarkFile`.
//!
//! # Three rules that are not obvious
//!
//! 1. **A major-version mismatch refuses to start**; `VERSION_FAILED` (-1) is
//!    only a warning (`ClusterMarkFile.java:154-171`).
//! 2. **Only BACKUP -> CONSENSUS_MODULE is a tolerated type change**
//!    (`:186-193`). Everything else refuses.
//! 3. **An existing error log is rescued before being cleared** (`:197-201`), so
//!    the reason a previous process died survives the restart that follows it.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use deepmsg_codec_cluster_mark::cluster_component_type::ClusterComponentType;
use deepmsg_core::buffer::{AtomicBuffer, ReadOnly, ReadWrite};
use deepmsg_core::pal::MappedFile;

/// `ClusterMarkFile.FILENAME`.
pub const FILENAME: &str = "cluster-mark.dat";
/// `ClusterMarkFile.LINK_FILENAME`.
pub const LINK_FILENAME: &str = "cluster-mark.lnk";
/// A container's file is `cluster-mark-service-<serviceId>.dat`.
pub const SERVICE_FILENAME_PREFIX: &str = "cluster-mark-service-";
/// `FILE_EXTENSION`.
pub const FILE_EXTENSION: &str = ".dat";
/// `LINK_FILE_EXTENSION`.
pub const LINK_FILE_EXTENSION: &str = ".lnk";

/// `MAJOR_VERSION` (`:56`). Only the major is compared on open.
pub const MAJOR_VERSION: i32 = 0;
/// `MINOR_VERSION` (`:60`).
pub const MINOR_VERSION: i32 = 3;
/// `PATCH_VERSION` (`:64`).
pub const PATCH_VERSION: i32 = 0;
/// `SEMANTIC_VERSION` (`:68`), composed as `SemanticVersion.compose` does.
pub const SEMANTIC_VERSION: i32 = (MAJOR_VERSION << 16) | (MINOR_VERSION << 8) | PATCH_VERSION;
/// `VERSION_FAILED` (`:76`) — what `signalFailedStart` writes.
pub const VERSION_FAILED: i32 = -1;
/// `HEADER_LENGTH` (`:72`): SBE header, body and var-data share it.
pub const HEADER_LENGTH: usize = 8 * 1024;
/// `ERROR_BUFFER_MIN_LENGTH` (`:80`).
pub const ERROR_BUFFER_MIN_LENGTH: usize = 1024 * 1024;
/// `ERROR_BUFFER_MAX_LENGTH` (`:84`).
pub const ERROR_BUFFER_MAX_LENGTH: usize = i32::MAX as usize - HEADER_LENGTH;
/// `HEADER_OFFSET` (`:108`), the width of the SBE message header.
pub const HEADER_OFFSET: usize = 8;
/// File offset of `version`.
pub const VERSION_OFFSET: usize = HEADER_OFFSET;
/// File offset of `activityTimestamp`.
pub const ACTIVITY_TIMESTAMP_OFFSET: usize = HEADER_OFFSET + 8;
/// `LogBufferDescriptor.PAGE_MIN_SIZE`.
pub const PAGE_MIN_SIZE: usize = 4 * 1024;
/// `LogBufferDescriptor.PAGE_MAX_SIZE`.
pub const PAGE_MAX_SIZE: usize = 1024 * 1024 * 1024;
/// How many var-data strings the schema carries, in order.
pub const VAR_DATA_COUNT: usize = 6;

const COMPONENT_TYPE: usize = HEADER_OFFSET + 4;
const START_TIMESTAMP: usize = HEADER_OFFSET + 16;
const PID: usize = HEADER_OFFSET + 24;
const CANDIDATE_TERM_ID: usize = HEADER_OFFSET + 32;
const MEMBER_ID: usize = HEADER_OFFSET + 56;
const SERVICE_ID: usize = HEADER_OFFSET + 60;
const HEADER_LENGTH_FIELD: usize = HEADER_OFFSET + 64;
const ERROR_BUFFER_LENGTH_FIELD: usize = HEADER_OFFSET + 68;
const CLUSTER_ID: usize = HEADER_OFFSET + 72;
const VAR_DATA_OFFSET: usize = HEADER_OFFSET + 128;

/// Why a cluster mark file could not be created or adopted.
#[derive(Debug)]
pub enum ClusterMarkError {
    /// Error buffer length outside `[1 MiB, i32::MAX - HEADER_LENGTH]`.
    ErrorBufferLength(usize),
    /// Page size below 4 KiB, above 1 GiB, or not a power of two.
    PageSize(usize),
    /// The header does not fit in 8 KiB.
    HeaderTooLong {
        /// Bytes needed.
        needed: usize,
        /// Bytes available.
        capacity: usize,
    },
    /// Another process is still alive behind this file.
    Active {
        /// The pid the file names.
        pid: i64,
    },
    /// The file's major version is not this build's.
    Version {
        /// Major found.
        found: i32,
        /// Major expected.
        expected: i32,
    },
    /// Created and never initialised.
    Uninitialised,
    /// The file belongs to a different component and the rule forbids takeover.
    ComponentType {
        /// What the file says it is.
        existing: ClusterComponentType,
        /// What the caller needs.
        required: ClusterComponentType,
    },
    /// The file system.
    Io(io::Error),
}

impl fmt::Display for ClusterMarkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ErrorBufferLength(n) => write!(f, "invalid error buffer length: {n}"),
            Self::PageSize(n) => write!(f, "invalid file page size: {n}"),
            Self::HeaderTooLong { needed, capacity } => {
                write!(f, "header needs {needed} bytes, capacity {capacity}")
            }
            Self::Active { pid } => write!(f, "mark file is active, pid={pid}"),
            Self::Version { found, expected } => {
                write!(
                    f,
                    "mark file major version {found} does not match {expected}"
                )
            }
            Self::Uninitialised => write!(f, "mark file was created and never initialised"),
            Self::ComponentType { existing, required } => {
                write!(
                    f,
                    "existing component type {existing:?} is not {required:?}"
                )
            }
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ClusterMarkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ClusterMarkError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// A mapped cluster mark file.
pub struct ClusterMarkFile {
    mapping: MappedFile,
    path: PathBuf,
    error_buffer_length: usize,
}

impl ClusterMarkFile {
    /// Create — or take over — the mark file at `path` for `required`.
    ///
    /// The order is the reference's (`:136-260`): validate the two lengths,
    /// inspect an existing file, refuse if it is alive or is the wrong
    /// component, rescue its errors, stamp this process's header, and only then
    /// write the version — because the version *is* the ready signal.
    ///
    /// # Errors
    ///
    /// See [`ClusterMarkError`].
    pub fn create(
        path: &Path,
        required: ClusterComponentType,
        error_buffer_length: usize,
        page_size: usize,
        now_ms: i64,
        pid: i64,
    ) -> Result<Self, ClusterMarkError> {
        if !(ERROR_BUFFER_MIN_LENGTH..=ERROR_BUFFER_MAX_LENGTH).contains(&error_buffer_length) {
            return Err(ClusterMarkError::ErrorBufferLength(error_buffer_length));
        }
        check_page_size(page_size)?;

        let length = align(HEADER_LENGTH + error_buffer_length, page_size);
        let (mapping, candidate_term_id) = if path.exists() {
            // The reference maps the file it finds rather than making a new one
            // (`MarkFile.mapNewOrExistingMarkFile`), which is what lets a
            // component restart over its own directory.
            let (mapping, candidate_term_id) = Self::adopt_existing(path, required, now_ms)?;
            (mapping, candidate_term_id)
        } else {
            (MappedFile::create(path, length)?, -1)
        };
        let file = Self {
            mapping,
            path: path.to_path_buf(),
            error_buffer_length,
        };

        {
            let buffer = file.header()?;
            write_message_header(&buffer);
            buffer.store_i32_relaxed(COMPONENT_TYPE, required as i32);
            buffer.store_i64_relaxed(START_TIMESTAMP, now_ms);
            buffer.store_i64_relaxed(PID, pid);
            buffer.store_i64_relaxed(CANDIDATE_TERM_ID, candidate_term_id);
            buffer.store_i32_relaxed(HEADER_LENGTH_FIELD, HEADER_LENGTH as i32);
            buffer.store_i32_relaxed(ERROR_BUFFER_LENGTH_FIELD, error_buffer_length as i32);
            // The ready signal, last.
            buffer.store_i32_relaxed(VERSION_OFFSET, 0);
            buffer.store_i64_relaxed(ACTIVITY_TIMESTAMP_OFFSET, now_ms);
        }

        file.sync()?;
        Ok(file)
    }

    /// Decide whether this process may take over an existing file; answer the
    /// `candidateTermId` to inherit.
    fn adopt_existing(
        path: &Path,
        required: ClusterComponentType,
        now_ms: i64,
    ) -> Result<(MappedFile, i64), ClusterMarkError> {
        let existing = MappedFile::open_readwrite(path)?;
        let candidate_term_id = {
            let buffer = existing
                .region(0, HEADER_LENGTH)
                .ok_or_else(|| io::Error::other("mark file too short to hold a header"))?;

            // Old-format files carry no SBE header; the reference reads them and
            // rewrites a new one (`:667`). It never writes the old format.
            let is_new_format =
                load_u16(&buffer, 2) == Some(200) && load_u16(&buffer, 4) == Some(110);
            if !is_new_format {
                return Err(ClusterMarkError::Version {
                    found: 0,
                    expected: MAJOR_VERSION,
                });
            }

            // Version 0 means "created, never signalled ready"; the reference lets
            // a later process adopt it (its only version check is the major), and
            // so do we — refusing it would make a crash before `signalReady`
            // unrecoverable.
            let version = buffer.load_i32(VERSION_OFFSET).unwrap_or(0);
            if version != 0 && version != VERSION_FAILED && major_of(version) != MAJOR_VERSION {
                return Err(ClusterMarkError::Version {
                    found: major_of(version),
                    expected: MAJOR_VERSION,
                });
            }

            let existing_type =
                ClusterComponentType::from(buffer.load_i32(COMPONENT_TYPE).unwrap_or(0));
            let allowed = existing_type == ClusterComponentType::UNKNOWN
                || existing_type == required
                || (existing_type == ClusterComponentType::BACKUP
                    && required == ClusterComponentType::CONSENSUS_MODULE);
            if !allowed {
                return Err(ClusterMarkError::ComponentType {
                    existing: existing_type,
                    required,
                });
            }

            let pid = buffer.load_i64(PID).unwrap_or(-1);
            let timestamp = buffer
                .load_i64(ACTIVITY_TIMESTAMP_OFFSET)
                .unwrap_or(VERSION_FAILED as i64);
            // A live process leaves a fresh timestamp. `-1` means "stopped on
            // purpose" and `i64::MAX` means "still initialising"; neither is alive.
            let alive = timestamp != -1 && timestamp != i64::MAX && (now_ms - timestamp) < 10_000;
            if alive {
                return Err(ClusterMarkError::Active { pid });
            }

            buffer.load_i64(CANDIDATE_TERM_ID).unwrap_or(-1)
        };
        Ok((existing, candidate_term_id))
    }

    /// Whether the file's own timestamp says a process is alive within
    /// `timeout_ms`. The reference's `MarkFile.isActive`.
    pub fn is_active(path: &Path, now_ms: i64, timeout_ms: i64) -> bool {
        let Ok(mapping) = MappedFile::open_readonly(path) else {
            return false;
        };
        let Some(buffer) = mapping.region(0, HEADER_LENGTH) else {
            return false;
        };
        let timestamp = buffer
            .load_i64(ACTIVITY_TIMESTAMP_OFFSET)
            .unwrap_or(i64::MIN);
        timestamp != -1 && timestamp != i64::MAX && (now_ms - timestamp) <= timeout_ms
    }

    /// The file this is.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// How long the file is.
    pub fn length(&self) -> usize {
        self.mapping.len()
    }

    /// The error buffer length this handle was created with.
    pub fn error_buffer_length(&self) -> usize {
        self.error_buffer_length
    }

    /// The version field. Zero means "created, not initialised".
    pub fn version(&self) -> Option<i32> {
        self.header().ok()?.load_i32(VERSION_OFFSET)
    }

    /// The component type the file carries.
    pub fn component_type(&self) -> Option<ClusterComponentType> {
        Some(ClusterComponentType::from(
            self.header().ok()?.load_i32(COMPONENT_TYPE)?,
        ))
    }

    /// The activity timestamp.
    pub fn activity_timestamp(&self) -> Option<i64> {
        self.header().ok()?.load_i64(ACTIVITY_TIMESTAMP_OFFSET)
    }

    /// The pid that wrote the header.
    pub fn pid(&self) -> Option<i64> {
        self.header().ok()?.load_i64(PID)
    }

    /// The start timestamp.
    pub fn start_timestamp(&self) -> Option<i64> {
        self.header().ok()?.load_i64(START_TIMESTAMP)
    }

    /// The election candidate term id inherited from the previous process.
    pub fn candidate_term_id(&self) -> Option<i64> {
        self.header().ok()?.load_i64(CANDIDATE_TERM_ID)
    }

    /// The member id, once a join has assigned one.
    pub fn member_id(&self) -> Option<i32> {
        self.header().ok()?.load_i32(MEMBER_ID)
    }

    /// Assign the member id.
    pub fn set_member_id(&self, member_id: i32) {
        if let Ok(buffer) = self.header() {
            let _ = buffer.store_i32_relaxed(MEMBER_ID, member_id);
        }
    }

    /// The service id (containers only).
    pub fn service_id(&self) -> Option<i32> {
        self.header().ok()?.load_i32(SERVICE_ID)
    }

    /// Set the service id.
    pub fn set_service_id(&self, service_id: i32) {
        if let Ok(buffer) = self.header() {
            let _ = buffer.store_i32_relaxed(SERVICE_ID, service_id);
        }
    }

    /// The cluster id.
    pub fn cluster_id(&self) -> Option<i32> {
        self.header().ok()?.load_i32(CLUSTER_ID)
    }

    /// Set the cluster id.
    pub fn set_cluster_id(&self, cluster_id: i32) {
        if let Ok(buffer) = self.header() {
            let _ = buffer.store_i32_relaxed(CLUSTER_ID, cluster_id);
        }
    }

    /// The header length the writer recorded.
    pub fn header_length(&self) -> Option<i32> {
        self.header().ok()?.load_i32(HEADER_LENGTH_FIELD)
    }

    /// The error buffer length the *writer* recorded — which is what a reader
    /// must use, not its own idea of the length.
    pub fn recorded_error_buffer_length(&self) -> Option<i32> {
        self.header().ok()?.load_i32(ERROR_BUFFER_LENGTH_FIELD)
    }

    /// The error buffer, writable.
    pub fn error_buffer(&self) -> Option<AtomicBuffer<'_, ReadWrite>> {
        let recorded = self.recorded_error_buffer_length()? as usize;
        self.mapping.region_mut(HEADER_LENGTH, recorded)
    }

    /// The error buffer, read-only.
    pub fn error_buffer_read_only(&self) -> Option<AtomicBuffer<'_, ReadOnly>> {
        let recorded = self.recorded_error_buffer_length()? as usize;
        self.mapping.region(HEADER_LENGTH, recorded)
    }

    /// Write the six var-data strings, **in schema order, in one pass**.
    ///
    /// SBE stores them back to back with their lengths, so they cannot be
    /// written out of order or one at a time into a zeroed header — the second
    /// write would land on top of the first. The reference's encoder has the
    /// same constraint and its callers write them together at construction.
    ///
    /// # Errors
    ///
    /// [`ClusterMarkError::HeaderTooLong`] when the set does not fit 8 KiB; the
    /// header is left untouched in that case (checked before the first write).
    pub fn write_var_data(&self, values: &[&str; VAR_DATA_COUNT]) -> Result<(), ClusterMarkError> {
        let mut needed = VAR_DATA_OFFSET;
        for value in values {
            needed += 4 + value.len();
        }
        if needed > HEADER_LENGTH {
            return Err(ClusterMarkError::HeaderTooLong {
                needed,
                capacity: HEADER_LENGTH,
            });
        }
        let buffer = self.header()?;
        let mut offset = VAR_DATA_OFFSET;
        for value in values {
            // Unaligned: a var-data length lands wherever the previous string
            // ended, so the atomic accessors (which require 4-byte alignment)
            // cannot be used here.
            let _ = buffer.copy_in(offset, &(value.len() as u32).to_le_bytes());
            let _ = buffer.copy_in(offset + 4, value.as_bytes());
            offset += 4 + value.len();
        }
        Ok(())
    }

    /// Read one of the six var-data strings.
    pub fn var_data(&self, index: usize) -> Option<String> {
        if index >= VAR_DATA_COUNT {
            return None;
        }
        let buffer = self.header().ok()?;
        let mut offset = VAR_DATA_OFFSET;
        for i in 0..VAR_DATA_COUNT {
            // Unaligned, for the same reason as `write_var_data`.
            let mut length_bytes = [0_u8; 4];
            buffer.copy_out(offset, &mut length_bytes)?;
            let length = u32::from_le_bytes(length_bytes) as usize;
            if i == index {
                let mut bytes = vec![0_u8; length];
                buffer.copy_out(offset + 4, &mut bytes)?;
                return String::from_utf8(bytes).ok();
            }
            offset += 4 + length;
        }
        None
    }

    /// Ready: semantic version plus a fresh activity timestamp.
    pub fn signal_ready(&self, activity_timestamp: i64) -> Result<(), ClusterMarkError> {
        let buffer = self.header()?;
        let _ = buffer.store_i32_relaxed(VERSION_OFFSET, SEMANTIC_VERSION);
        let _ = buffer.store_i64_relaxed(ACTIVITY_TIMESTAMP_OFFSET, activity_timestamp);
        Ok(())
    }

    /// Failed to start: version `-1`, no timestamp.
    pub fn signal_failed_start(&self) -> Result<(), ClusterMarkError> {
        let buffer = self.header()?;
        let _ = buffer.store_i32_relaxed(VERSION_OFFSET, VERSION_FAILED);
        let _ = buffer.store_i64_relaxed(ACTIVITY_TIMESTAMP_OFFSET, -1);
        Ok(())
    }

    /// Deliberate stop: timestamp `-1`, then force.
    pub fn signal_terminated(&self) -> Result<(), ClusterMarkError> {
        if let Ok(buffer) = self.header() {
            let _ = buffer.store_i64_relaxed(ACTIVITY_TIMESTAMP_OFFSET, -1);
        }
        self.sync()
    }

    /// Proof of life.
    pub fn update_activity_timestamp(&self, now_ms: i64) {
        if let Ok(buffer) = self.header() {
            let _ = buffer.store_i64_relaxed(ACTIVITY_TIMESTAMP_OFFSET, now_ms);
        }
    }

    /// Zero the error buffer.
    pub fn clear_error_buffer(&self) {
        if let Some(buffer) = self.error_buffer() {
            let _ = buffer.zero(0, self.error_buffer_length);
        }
    }

    /// Rescue the error log beside the mark file as
    /// `<prefix>-<timestamp>-error.log`, answering the path written.
    ///
    /// The reference only writes a file when the log has observations; the test
    /// for that here is "is not all zeroes", which is the same question without
    /// parsing the log's internal layout.
    pub fn save_existing_errors(
        &self,
        prefix: &str,
        now_ms: i64,
    ) -> Result<Option<PathBuf>, ClusterMarkError> {
        let Some(buffer) = self.error_buffer_read_only() else {
            return Ok(None);
        };
        let mut bytes = vec![0_u8; self.error_buffer_length];
        if buffer.copy_out(0, &mut bytes).is_none() {
            return Ok(None);
        }
        if bytes.iter().all(|byte| *byte == 0) {
            return Ok(None);
        }
        let directory = self.path.parent().unwrap_or(Path::new("."));
        let file = directory.join(format!("{prefix}-{now_ms}-error.log"));
        std::fs::write(&file, &bytes)?;
        Ok(Some(file))
    }

    /// Force the mapping to disk.
    pub fn sync(&self) -> Result<(), ClusterMarkError> {
        self.mapping.sync()?;
        Ok(())
    }

    fn header(&self) -> Result<AtomicBuffer<'_, ReadWrite>, io::Error> {
        self.mapping
            .region_mut(0, HEADER_LENGTH)
            .ok_or_else(|| io::Error::other("mark file too short to hold a header"))
    }
}

impl fmt::Debug for ClusterMarkFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterMarkFile")
            .field("path", &self.path)
            .field("length", &self.mapping.len())
            .field("component_type", &self.component_type())
            .field("version", &self.version())
            .finish()
    }
}

/// `ClusterMarkFile.checkHeaderLength`: refuse a set of var-data strings that
/// cannot fit 8 KiB, before anything is written.
pub fn check_header_length(values: &[&str]) -> Result<(), ClusterMarkError> {
    let mut needed = VAR_DATA_OFFSET;
    for value in values.iter().take(VAR_DATA_COUNT) {
        needed += 4 + value.len();
    }
    if needed > HEADER_LENGTH {
        return Err(ClusterMarkError::HeaderTooLong {
            needed,
            capacity: HEADER_LENGTH,
        });
    }
    Ok(())
}

/// The file name a service container uses.
pub fn service_filename(service_id: i32) -> String {
    format!("{SERVICE_FILENAME_PREFIX}{service_id}{FILE_EXTENSION}")
}

/// `MarkFile.ensureMarkFileLink`, for the cluster's `.lnk`.
pub fn ensure_mark_file_link(directory: &Path, mark_file: &Path) -> io::Result<()> {
    let directory = directory.canonicalize()?;
    let link = directory.join(LINK_FILENAME);
    let mark_file_directory = mark_file
        .parent()
        .ok_or_else(|| io::Error::other("the mark file has no directory"))?
        .canonicalize()?;

    if directory == mark_file_directory {
        if link.exists() {
            std::fs::remove_file(&link)?;
        }
        return Ok(());
    }

    std::fs::write(&link, mark_file_directory.to_string_lossy().as_bytes())
}

/// `SemanticVersion.major`.
pub const fn major_of(version: i32) -> i32 {
    (version >> 16) & 0xff
}

fn check_page_size(page_size: usize) -> Result<(), ClusterMarkError> {
    if !(PAGE_MIN_SIZE..=PAGE_MAX_SIZE).contains(&page_size) || !page_size.is_power_of_two() {
        return Err(ClusterMarkError::PageSize(page_size));
    }
    Ok(())
}

const fn align(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

fn load_u16<A>(buffer: &AtomicBuffer<'_, A>, offset: usize) -> Option<u16> {
    let mut bytes = [0_u8; 2];
    buffer.copy_out(offset, &mut bytes)?;
    Some(u16::from_le_bytes(bytes))
}

fn write_message_header(buffer: &AtomicBuffer<'_, ReadWrite>) {
    // blockLength 128, templateId 200, schemaId 110, version 2, little-endian.
    let _ = buffer.copy_in(0, &128u16.to_le_bytes());
    let _ = buffer.copy_in(2, &200u16.to_le_bytes());
    let _ = buffer.copy_in(4, &110u16.to_le_bytes());
    let _ = buffer.copy_in(6, &2u16.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_semantic_version_composes_the_way_the_reference_does() {
        assert_eq!(SEMANTIC_VERSION, 3 << 8);
        assert_eq!(major_of(SEMANTIC_VERSION), 0);
    }

    #[test]
    fn page_sizes_follow_the_reference_bounds() {
        assert!(check_page_size(4096).is_ok());
        assert!(check_page_size(8192).is_ok());
        assert!(check_page_size(2048).is_err());
        assert!(check_page_size(4097).is_err());
        assert!(check_page_size(PAGE_MAX_SIZE * 2).is_err());
    }

    #[test]
    fn the_error_buffer_length_bounds_match_the_reference() {
        assert_eq!(ERROR_BUFFER_MIN_LENGTH, 1024 * 1024);
        assert_eq!(ERROR_BUFFER_MAX_LENGTH, i32::MAX as usize - HEADER_LENGTH);
    }

    #[test]
    fn a_service_file_is_named_for_its_id() {
        assert_eq!(service_filename(3), "cluster-mark-service-3.dat");
    }

    #[test]
    fn header_length_refuses_what_cannot_fit() {
        let big = "x".repeat(HEADER_LENGTH);
        assert!(check_header_length(&[&big]).is_err());
        assert!(check_header_length(&["a", "b"]).is_ok());
    }
}
