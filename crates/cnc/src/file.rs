//! Opening a CnC file and reading the things that live in it.
//!
//! The open sequence matters as much as the offsets do. The driver fills every
//! other metadata field first and only then publishes `cnc_version` with a
//! release store (`aeron-driver/src/main/c/aeron_driver.c:252-260`, then
//! `:972`); a reader that reads the version with an acquire, and refuses to
//! look at anything else until it is non-zero, gets a coherent view of the
//! whole block for free. Reading the other fields first would be reading
//! memory the driver had not finished writing.

use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use deepmsg_core::buffer::ReadWrite;
use deepmsg_core::pal::MappedFile;
use deepmsg_core::version::{self, CncVersionCompatibility};

use crate::counters::CountersReader;
use crate::error::CncError;
use crate::error_log::ErrorLogReader;
use crate::layout;
use crate::metadata::{CncMetadata, RegionLayout};
use crate::ring::ToDriverRing;

/// Name of the CnC file inside an aeron directory
/// (`aeron-client/src/main/java/io/aeron/CncFileDescriptor.java:6`).
pub const CNC_FILE_NAME: &str = "cnc.dat";

/// How long the reference waits between attempts while a driver starts up
/// (`aeron-client/src/main/c/aeron_cnc.c:79`).
pub const RETRY_INTERVAL: Duration = Duration::from_millis(16);

/// Why a CnC file could not be opened.
#[derive(Debug)]
pub enum CncOpenError {
    /// The file could not be opened, measured, or mapped.
    Io(io::Error),
    /// The file is not longer than the metadata region. Retryable: a driver
    /// creates the file and fills it afterwards.
    TooShort {
        /// The file's length as measured.
        length: usize,
    },
    /// The file exists but its version field is still zero. This is the
    /// readiness gate, not a failure — retry.
    NotReady,
    /// The version rules rejected the file. Not retryable: compatibility is a
    /// property of the file.
    Incompatible(CncVersionCompatibility),
    /// The metadata does not describe a layout this build can read.
    Malformed(CncError),
}

impl CncOpenError {
    /// Whether waiting and looking again could produce a different answer.
    ///
    /// Only "not there yet" states qualify. A version mismatch and a malformed
    /// block are properties of the file, not of how long we waited.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Io(error) => io::ErrorKind::NotFound == error.kind(),
            Self::TooShort { .. } | Self::NotReady => true,
            Self::Incompatible(_) | Self::Malformed(_) => false,
        }
    }
}

impl std::fmt::Display for CncOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "CnC file could not be opened: {error}"),
            Self::TooShort { length } => {
                write!(f, "CnC file is {length} bytes, no region could fit")
            }
            Self::NotReady => f.write_str("CnC file exists but its metadata is not published yet"),
            Self::Incompatible(compatibility) => {
                write!(f, "CnC version is not usable: {compatibility:?}")
            }
            Self::Malformed(error) => write!(f, "CnC metadata is not usable: {error}"),
        }
    }
}

impl std::error::Error for CncOpenError {}

/// A mapped, validated CnC file.
///
/// # Validity
///
/// A `CncFile` describes the driver instance that created it, identified by
/// `metadata.pid` and `metadata.start_timestamp_ms`. If that driver exits and
/// another reclaims the directory, the file it holds refers to an inode that
/// no longer exists — the mapping stays valid, its contents do not. Reopen.
pub struct CncFile {
    mapping: MappedFile,
    metadata: CncMetadata,
    layout: RegionLayout,
    path: PathBuf,
}

impl CncFile {
    /// Open `<aeron_dir>/cnc.dat` once.
    ///
    /// Retryable outcomes are returned rather than looped on, so that a caller
    /// watching a child process can poll both. [`CncFile::open`] is the loop.
    pub fn try_open(aeron_dir: &Path) -> Result<Self, CncOpenError> {
        Self::try_open_with(aeron_dir, false)
    }

    /// Open `<aeron_dir>/cnc.dat` read-write, for the command path.
    ///
    /// Same validation as [`CncFile::try_open`] — a client that cannot read a
    /// file has no business writing to it — with a mapping that
    /// [`CncFile::to_driver_ring`] can hand out a writable window over. Nothing
    /// else about the type changes: a `CncFile` opened this way still refuses
    /// to expose a writable window for any region but the command ring.
    pub fn try_open_writable(aeron_dir: &Path) -> Result<Self, CncOpenError> {
        Self::try_open_with(aeron_dir, true)
    }

    fn try_open_with(aeron_dir: &Path, writable: bool) -> Result<Self, CncOpenError> {
        let path = aeron_dir.join(CNC_FILE_NAME);
        let mapping = if writable {
            MappedFile::open_readwrite(&path)
        } else {
            MappedFile::open_readonly(&path)
        }
        .map_err(CncOpenError::Io)?;

        // Strictly greater: a file of exactly the metadata length has no room
        // for a region. The reference uses the same comparison
        // (`aeron-client/src/main/c/aeron_cnc_file_descriptor.c:70,88`).
        if mapping.len() <= layout::VERSION_AND_METADATA_LENGTH {
            return Err(CncOpenError::TooShort {
                length: mapping.len(),
            });
        }

        let head = mapping
            .region(0, layout::VERSION_AND_METADATA_LENGTH)
            .ok_or(CncOpenError::TooShort {
                length: mapping.len(),
            })?;

        // The acquire that makes the rest of the block readable. The module
        // doc explains why this has to be first.
        let cnc_version =
            head.load_i32_acquire(layout::CNC_VERSION_OFFSET)
                .ok_or(CncOpenError::TooShort {
                    length: mapping.len(),
                })?;

        let compatibility = version::check_cnc_version(cnc_version);
        if CncVersionCompatibility::NotReady == compatibility {
            return Err(CncOpenError::NotReady);
        }
        if CncVersionCompatibility::Compatible != compatibility {
            return Err(CncOpenError::Incompatible(compatibility));
        }

        // Now that the acquire has ordered the block, a bounded copy is a
        // coherent snapshot, and decoding from owned bytes keeps the layout
        // code pure and testable.
        let mut block = [0u8; layout::VERSION_AND_METADATA_LENGTH];
        head.copy_out(0, &mut block).ok_or(CncOpenError::TooShort {
            length: mapping.len(),
        })?;

        let metadata = CncMetadata::decode(&block).map_err(CncOpenError::Malformed)?;
        let region_layout =
            RegionLayout::compute(&metadata, mapping.len()).map_err(CncOpenError::Malformed)?;

        Ok(Self {
            mapping,
            metadata,
            layout: region_layout,
            path,
        })
    }

    /// Open `<aeron_dir>/cnc.dat`, retrying while it is not ready yet.
    ///
    /// # Errors
    ///
    /// The last retryable error once `timeout` elapses, or the first
    /// non-retryable one immediately.
    pub fn open(aeron_dir: &Path, timeout: Duration) -> Result<Self, CncOpenError> {
        let deadline = Instant::now() + timeout;

        loop {
            match Self::try_open(aeron_dir) {
                Ok(file) => return Ok(file),
                Err(error) => {
                    if !error.is_retryable() || Instant::now() >= deadline {
                        return Err(error);
                    }
                }
            }

            std::thread::sleep(RETRY_INTERVAL);
        }
    }

    /// The metadata block, as published.
    pub fn metadata(&self) -> &CncMetadata {
        &self.metadata
    }

    /// Where each region of the file lives.
    pub fn layout(&self) -> &RegionLayout {
        &self.layout
    }

    /// The file this was opened from.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The whole file's length, as measured.
    pub fn file_length(&self) -> usize {
        self.mapping.len()
    }

    /// The packed version the driver published.
    pub fn cnc_version(&self) -> i32 {
        self.metadata.cnc_version
    }

    /// A view over the counters, or `None` if the regions are unreachable —
    /// which the layout checks should already have ruled out.
    pub fn counters(&self) -> Option<CountersReader<'_>> {
        let metadata = self.mapping.region(
            self.layout.counters_metadata.start,
            self.layout.counters_metadata.len(),
        )?;
        let values = self.mapping.region(
            self.layout.counters_values.start,
            self.layout.counters_values.len(),
        )?;

        Some(CountersReader::new(metadata, values))
    }

    /// A view over the error log, or `None` if its region is unreachable.
    pub fn error_log(&self) -> Option<ErrorLogReader<'_>> {
        let buffer = self
            .mapping
            .region(self.layout.error_log.start, self.layout.error_log.len())?;

        Some(ErrorLogReader::new(buffer))
    }

    /// A **writable** view over the counters, for a client's own heartbeat.
    ///
    /// `None` on a read-only file, as [`CncFile::to_driver_ring`] is: the
    /// ability to write follows from how the file was opened, not from which
    /// method was called.
    pub fn counters_writable(&self) -> Option<CountersReader<'_, ReadWrite>> {
        let metadata = self.mapping.region_mut(
            self.layout.counters_metadata.start,
            self.layout.counters_metadata.len(),
        )?;
        let values = self.mapping.region_mut(
            self.layout.counters_values.start,
            self.layout.counters_values.len(),
        )?;

        Some(CountersReader::new(metadata, values))
    }

    /// A producer over the to-driver command ring.
    ///
    /// `None` if this file was opened read-only — the window is writable or it
    /// is not, and a read-only mapping returned here would fault on the first
    /// store — or if the region cannot be a ring at all.
    ///
    /// `deepmsg-codec`, `deepmsg-client` and `deepmsg-archive` all forbid
    /// `unsafe`, and this is how they get to write a command without needing
    /// it: the window is built where the mapping's mode is known.
    pub fn to_driver_ring(&self) -> Option<ToDriverRing<'_>> {
        let region = self
            .mapping
            .region_mut(self.layout.to_driver.start, self.layout.to_driver.len())?;

        ToDriverRing::new(region)
    }

    /// The to-driver ring's consumer heartbeat, epoch milliseconds.
    ///
    /// This is the field the reference reads to decide liveness — there is no
    /// lock file and no pid check anywhere in the aeron directory discipline.
    pub fn consumer_heartbeat_ms(&self) -> Option<i64> {
        let trailer = self
            .layout
            .to_driver
            .end
            .checked_sub(layout::MPSC_RB_TRAILER_LENGTH)?;

        self.mapping
            .region(trailer, layout::MPSC_RB_TRAILER_LENGTH)?
            .load_i64_acquire(layout::MPSC_CONSUMER_HEARTBEAT_OFFSET)
    }

    /// Whether the driver looks alive as of `now_ms`.
    ///
    /// The reference's rule (`aeron-driver/src/main/c/aeron_driver_context.c:1625-1643`)
    /// is that the heartbeat's age must not exceed the timeout. A heartbeat of
    /// [`layout::NULL_VALUE`] means the driver stopped deliberately — it wrote
    /// that value on the way out
    /// (`aeron-driver/src/main/c/aeron_driver_conductor.c:3493`) — which is
    /// reported as not active rather than as an impossibly old age.
    pub fn driver_is_active(&self, now_ms: i64, driver_timeout_ms: i64) -> bool {
        match self.consumer_heartbeat_ms() {
            None | Some(layout::NULL_VALUE) => false,
            Some(heartbeat) => now_ms.saturating_sub(heartbeat) <= driver_timeout_ms,
        }
    }
}

impl std::fmt::Debug for CncFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CncFile")
            .field("path", &self.path)
            .field("length", &self.mapping.len())
            .field(
                "version",
                &version::format_version(self.metadata.cnc_version),
            )
            .field("pid", &self.metadata.pid)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout;
    use std::io::Write as _;

    /// A synthetic CnC file on disk, removed when dropped.
    ///
    /// Built by hand rather than captured from a driver so that the cases the
    /// live driver cannot produce — an unpublished version, a version from the
    /// future, a hostile length — can be exercised against the real mapping
    /// path. The interop suite covers the other half: a real file.
    struct TempCnc {
        dir: PathBuf,
    }

    /// Region sizes small enough to keep the file trivial, but valid: a ring
    /// region must exceed its trailer and the counter regions must satisfy the
    /// reference's 4:1 ratio.
    const TO_DRIVER: usize = layout::MPSC_RB_TRAILER_LENGTH + 1024;
    const TO_CLIENTS: usize = layout::BROADCAST_TRAILER_LENGTH + 1024;
    const COUNTERS_VALUES: usize = 2 * layout::COUNTER_VALUE_LENGTH;
    const COUNTERS_METADATA: usize = 4 * COUNTERS_VALUES;
    const ERROR_LOG: usize = 8 * layout::ERROR_LOG_HEADER_LENGTH;

    const TOTAL: usize = layout::VERSION_AND_METADATA_LENGTH
        + TO_DRIVER
        + TO_CLIENTS
        + COUNTERS_METADATA
        + COUNTERS_VALUES
        + ERROR_LOG;

    struct Layout {
        to_driver: usize,
        to_clients: usize,
        counters_metadata: usize,
        counters_values: usize,
        error_log: usize,
    }

    fn offsets() -> Layout {
        let to_driver = layout::VERSION_AND_METADATA_LENGTH;
        let to_clients = to_driver + TO_DRIVER;
        let counters_metadata = to_clients + TO_CLIENTS;
        let counters_values = counters_metadata + COUNTERS_METADATA;
        Layout {
            to_driver,
            to_clients,
            counters_metadata,
            counters_values,
            error_log: counters_values + COUNTERS_VALUES,
        }
    }

    impl TempCnc {
        /// Write a well-formed file, then let the caller mutate the bytes
        /// before it is mapped. The metadata is filled in assuming the region
        /// lengths above.
        fn new(version: i32) -> Self {
            static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir =
                std::env::temp_dir().join(format!("deepmsg-cnc-{}-{n}.dir", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create temp dir");

            let mut bytes = vec![0u8; TOTAL];
            let put_i32 = |bytes: &mut Vec<u8>, offset: usize, v: i32| {
                bytes[offset..offset + 4].copy_from_slice(&v.to_le_bytes());
            };
            let put_i64 = |bytes: &mut Vec<u8>, offset: usize, v: i64| {
                bytes[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
            };

            put_i32(
                &mut bytes,
                layout::TO_DRIVER_BUFFER_LENGTH_OFFSET,
                TO_DRIVER as i32,
            );
            put_i32(
                &mut bytes,
                layout::TO_CLIENTS_BUFFER_LENGTH_OFFSET,
                TO_CLIENTS as i32,
            );
            put_i32(
                &mut bytes,
                layout::COUNTER_METADATA_BUFFER_LENGTH_OFFSET,
                COUNTERS_METADATA as i32,
            );
            put_i32(
                &mut bytes,
                layout::COUNTER_VALUES_BUFFER_LENGTH_OFFSET,
                COUNTERS_VALUES as i32,
            );
            put_i32(
                &mut bytes,
                layout::ERROR_LOG_BUFFER_LENGTH_OFFSET,
                ERROR_LOG as i32,
            );
            put_i64(
                &mut bytes,
                layout::CLIENT_LIVENESS_TIMEOUT_OFFSET,
                10_000_000_000,
            );
            put_i64(
                &mut bytes,
                layout::START_TIMESTAMP_OFFSET,
                1_700_000_000_000,
            );
            put_i64(&mut bytes, layout::PID_OFFSET, 4242);
            put_i32(&mut bytes, layout::FILE_PAGE_SIZE_OFFSET, 4096);
            // Published last, as the driver does.
            put_i32(&mut bytes, layout::CNC_VERSION_OFFSET, version);

            let mut file = std::fs::File::create(dir.join(CNC_FILE_NAME)).expect("create cnc.dat");
            file.write_all(&bytes).expect("write cnc.dat");
            file.sync_all().expect("sync cnc.dat");

            Self { dir }
        }

        /// Rewrite one 4-byte field in the file on disk.
        fn patch_i32(self, offset: usize, value: i32) -> Self {
            use std::io::{Seek, SeekFrom};
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(self.dir.join(CNC_FILE_NAME))
                .expect("reopen");
            file.seek(SeekFrom::Start(offset as u64)).expect("seek");
            file.write_all(&value.to_le_bytes()).expect("patch");
            file.sync_all().expect("sync");
            self
        }

        /// Rewrite one 8-byte field in the file on disk.
        fn patch_i64(self, offset: usize, value: i64) -> Self {
            use std::io::{Seek, SeekFrom};
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(self.dir.join(CNC_FILE_NAME))
                .expect("reopen");
            file.seek(SeekFrom::Start(offset as u64)).expect("seek");
            file.write_all(&value.to_le_bytes()).expect("patch");
            file.sync_all().expect("sync");
            self
        }

        fn path(&self) -> &Path {
            &self.dir
        }
    }

    impl Drop for TempCnc {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn opens_a_well_formed_file() {
        let temp = TempCnc::new(deepmsg_core::version::CNC_VERSION);
        let cnc = CncFile::try_open(temp.path()).expect("open");

        assert_eq!(deepmsg_core::version::CNC_VERSION, cnc.cnc_version());
        assert_eq!(4242, cnc.metadata().pid);
        assert_eq!(1_700_000_000_000, cnc.metadata().start_timestamp_ms);
        assert_eq!(TOTAL, cnc.file_length());

        let offsets = offsets();
        assert_eq!(offsets.to_driver, cnc.layout().to_driver.start);
        assert_eq!(offsets.to_clients, cnc.layout().to_clients.start);
        assert_eq!(
            offsets.counters_metadata,
            cnc.layout().counters_metadata.start
        );
        assert_eq!(offsets.counters_values, cnc.layout().counters_values.start);
        assert_eq!(offsets.error_log, cnc.layout().error_log.start);
        assert_eq!(TOTAL, cnc.layout().error_log.end);

        // Regions are contiguous: each starts where the last one ended.
        assert_eq!(cnc.layout().to_driver.end, cnc.layout().to_clients.start);
        assert_eq!(
            cnc.layout().counters_values.end,
            cnc.layout().error_log.start
        );
    }

    #[test]
    fn an_unpublished_version_is_the_readiness_gate() {
        let temp = TempCnc::new(0);
        let error = CncFile::try_open(temp.path()).expect_err("version 0 is not ready");

        assert!(matches!(error, CncOpenError::NotReady), "got {error:?}");
        assert!(
            error.is_retryable(),
            "a driver that has not published yet must be waited for, not rejected"
        );
    }

    #[test]
    fn rejects_a_version_from_the_future_and_an_older_minor() {
        let future = TempCnc::new(deepmsg_core::version::semantic_version_compose(1, 0, 0));
        assert!(matches!(
            CncFile::try_open(future.path()),
            Err(CncOpenError::Incompatible(
                CncVersionCompatibility::MajorMismatch
            ))
        ));

        let older = TempCnc::new(deepmsg_core::version::semantic_version_compose(0, 1, 0));
        let error = CncFile::try_open(older.path()).expect_err("older minor must be refused");
        assert!(
            !error.is_retryable(),
            "a version mismatch is a property of the file, not of how long we waited"
        );
        assert!(matches!(
            error,
            CncOpenError::Incompatible(CncVersionCompatibility::InsufficientMinor)
        ));
    }

    #[test]
    fn refuses_to_retry_what_waiting_cannot_fix() {
        let temp = TempCnc::new(deepmsg_core::version::CNC_VERSION);
        let malformed = temp.patch_i32(layout::TO_DRIVER_BUFFER_LENGTH_OFFSET, -1);

        let error = CncFile::try_open(malformed.path()).expect_err("negative length");
        assert!(matches!(error, CncOpenError::Malformed(_)), "got {error:?}");
        assert!(!error.is_retryable());
    }

    #[test]
    fn reports_a_missing_directory_as_retryable() {
        let missing = std::env::temp_dir().join("deepmsg-cnc-does-not-exist.dir");
        let error = CncFile::try_open(&missing).expect_err("no such directory");

        assert!(error.is_retryable(), "a driver may not have started yet");
    }

    #[test]
    fn open_times_out_on_a_file_that_never_becomes_ready() {
        let temp = TempCnc::new(0);
        let started = Instant::now();
        let error =
            CncFile::open(temp.path(), Duration::from_millis(80)).expect_err("never published");

        assert!(matches!(error, CncOpenError::NotReady));
        assert!(
            started.elapsed() >= Duration::from_millis(80),
            "it must actually wait, not fail fast"
        );
    }

    #[test]
    fn reaches_the_mapped_regions_through_real_ones() {
        let temp = TempCnc::new(deepmsg_core::version::CNC_VERSION);
        let cnc = CncFile::try_open(temp.path()).expect("open");

        // A driver that has just started has written no counters and no
        // errors, so these views exist and are empty -- which is a stronger
        // statement than "the accessor returned None".
        let counters = cnc.counters().expect("counter regions reachable");
        let mut seen = 0;
        let scan = counters.for_each(|_| seen += 1);
        assert_eq!(0, seen);
        assert_eq!(0, scan.allocated);
        assert_eq!(1, counters.max_counter_id());

        let errors = cnc.error_log().expect("error log reachable");
        assert!(!errors.has_entries());
        let mut out = Vec::new();
        assert_eq!(0, errors.read(i64::MIN, &mut out).entries);
    }

    #[test]
    fn reads_the_consumer_heartbeat_from_the_ring_trailer() {
        let temp = TempCnc::new(deepmsg_core::version::CNC_VERSION);
        let heartbeat_offset = offsets().to_driver + TO_DRIVER - layout::MPSC_RB_TRAILER_LENGTH
            + layout::MPSC_CONSUMER_HEARTBEAT_OFFSET;

        // Fresh file: heartbeat zero. A driver that never started writes
        // nothing there, which is why zero is not "active".
        let cnc = CncFile::try_open(temp.path()).expect("open");
        assert_eq!(Some(0), cnc.consumer_heartbeat_ms());
        assert!(cnc.driver_is_active(1_000, 10_000), "age 1000 <= timeout");

        // A heartbeat far in the past is a dead driver.
        assert!(!cnc.driver_is_active(1_000_000, 10_000));

        // A deliberate shutdown is recorded as -1, and must not read as a
        // very old timestamp.
        let stopped = temp.patch_i64(heartbeat_offset, layout::NULL_VALUE);
        let cnc = CncFile::try_open(stopped.path()).expect("open");
        assert_eq!(Some(layout::NULL_VALUE), cnc.consumer_heartbeat_ms());
        assert!(!cnc.driver_is_active(0, i64::MAX));
    }
}
