//! Creating a CnC file — the driver's half of the file's life.
//!
//! Only a media driver creates `cnc.dat`. The reference has no client-side
//! creation path at all: `aeron_cnc_length` is a function of the *driver's*
//! context (`aeron-driver/src/main/c/aeron_driver_context.c:1690`), and the
//! client's context merely unmaps the file it was given
//! (`aeron-client/src/main/c/aeron_context.c:220`). A client that finds a
//! directory with no CnC in it is looking at a dead driver, not at a job to do.
//!
//! # The order that matters
//!
//! Every field is written before the version, and the version is the only
//! release store in the block:
//!
//! | step | reference |
//! |---|---|
//! | the nine fields, plainly | `aeron-driver/src/main/c/aeron_driver.c:250-269` |
//! | `cnc_version`, release | `aeron_driver.c:972`, inline store at `aeron_driver_context.h:456-459` |
//! | `msync` over the file | `aeron_driver.c:973` |
//!
//! A reader acquires on the version and only then copies the rest
//! (`crate::file`), so this order is what lets the block be read as plain
//! little-endian bytes rather than field by field.
//!
//! # Publishing is a step of its own
//!
//! [`CncFile::create`] leaves the version at zero and [`CncFile::publish`]
//! stores it, because the version is the gate for *everything* in the block,
//! not just for the fields written beside it. The reference puts work in that
//! gap — the conductor, the sender and the receiver all start between the
//! metadata being written and the version being stored — and one piece of that
//! work is the first heartbeat on the to-driver ring
//! (`aeron-driver/src/main/c/aeron_driver.c:971`, one line before the version
//! at `:972`). A client that passes the version gate must not then find a
//! heartbeat of zero and conclude the driver is dead, so the same gap has to
//! exist here even though it is currently one call deep.

use std::io;
use std::path::Path;

use deepmsg_core::pal::MappedFile;

use crate::error::{CncError, Region};
use crate::file::{CNC_FILE_NAME, CncFile};
use crate::layout;
use crate::metadata::{CncMetadata, RegionLayout};

/// Default length of the to-driver command ring region:
/// `1024 * 1024 + AERON_RB_TRAILER_LENGTH`
/// (`aeron-driver/src/main/c/aeron_driver_context.h:62`).
pub const TO_DRIVER_BUFFER_LENGTH_DEFAULT: usize = 1024 * 1024 + layout::MPSC_RB_TRAILER_LENGTH;

/// Default length of the to-clients broadcast region:
/// `1024 * 1024 + AERON_BROADCAST_BUFFER_TRAILER_LENGTH`
/// (`aeron-driver/src/main/c/aeron_driver_context.h:63`).
pub const TO_CLIENTS_BUFFER_LENGTH_DEFAULT: usize = 1024 * 1024 + layout::BROADCAST_TRAILER_LENGTH;

/// Default counters values region length (`aeron_driver_context.h:64`).
pub const COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT: usize = 8 * 1024 * 1024;

/// Smallest counters values region the reference accepts (`:65`).
pub const COUNTERS_VALUES_BUFFER_LENGTH_MIN: usize = 1024 * 1024;

/// Largest counters values region the reference accepts (`:66`).
pub const COUNTERS_VALUES_BUFFER_LENGTH_MAX: usize = 500 * 1024 * 1024;

/// Default error-log region length (`:67`).
pub const ERROR_LOG_BUFFER_LENGTH_DEFAULT: usize = 4 * 1024 * 1024;

/// Default page size the total is aligned to (`aeron_driver_context.c:185`).
pub const FILE_PAGE_SIZE_DEFAULT: usize = 4096;

/// Smallest page size the reference accepts
/// (`aeron-client/src/main/c/concurrent/aeron_logbuffer_descriptor.h:30`,
/// checked with the power-of-two rule at `aeron-driver/src/main/c/aeron_driver.c:467-485`).
pub const FILE_PAGE_SIZE_MIN: usize = 4 * 1024;

/// Largest page size the reference accepts (`aeron_logbuffer_descriptor.h:31`).
pub const FILE_PAGE_SIZE_MAX: usize = 1024 * 1024 * 1024;

/// Default client liveness timeout, in nanoseconds (`aeron_driver_context.c:178`).
pub const CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT: i64 = 10_000_000_000;

/// The five region lengths, plus the page size the total is aligned to.
///
/// The counters *metadata* length is not here: the reference derives it as
/// four times the values length (`aeron-client/src/main/c/concurrent/aeron_counters_manager.h:25-29`),
/// and a field a caller could set to a disagreeing number would be a way to
/// write a file that no reader accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CncLayout {
    /// The MPSC command ring region, trailer included.
    pub to_driver_length: usize,
    /// The broadcast region, trailer included.
    pub to_clients_length: usize,
    /// The counter value records region.
    pub counters_values_length: usize,
    /// The distinct error log region.
    pub error_log_length: usize,
    /// What the total is rounded up to.
    pub page_size: usize,
}

impl Default for CncLayout {
    /// The lengths a default-configured reference driver writes, which is what
    /// the captured `tests/fixtures/cnc-header.bin` came from.
    fn default() -> Self {
        Self {
            to_driver_length: TO_DRIVER_BUFFER_LENGTH_DEFAULT,
            to_clients_length: TO_CLIENTS_BUFFER_LENGTH_DEFAULT,
            counters_values_length: COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT,
            error_log_length: ERROR_LOG_BUFFER_LENGTH_DEFAULT,
            page_size: FILE_PAGE_SIZE_DEFAULT,
        }
    }
}

impl CncLayout {
    /// The counters metadata region length this layout implies.
    pub const fn counters_metadata_length(&self) -> usize {
        self.counters_values_length
            * (layout::COUNTER_METADATA_LENGTH / layout::COUNTER_VALUE_LENGTH)
    }

    /// The regions end to end, before the page alignment of the total.
    pub const fn unaligned_length(&self) -> usize {
        layout::VERSION_AND_METADATA_LENGTH
            + self.to_driver_length
            + self.to_clients_length
            + self.counters_metadata_length()
            + self.counters_values_length
            + self.error_log_length
    }

    /// The file length: the regions, plus the metadata region, rounded up to
    /// the page size (`aeron-client/src/main/c/aeron_cnc_file_descriptor.h:93-96`).
    ///
    /// # Errors
    ///
    /// [`CncCreateError::FileLengthTooLarge`] if the total does not fit in the
    /// `int32` the metadata field is — the reference caps it at `INT32_MAX` at
    /// `aeron-driver/src/main/c/aeron_driver.c:298`.
    pub fn file_length(&self) -> Result<usize, CncCreateError> {
        let total = self.unaligned_length();
        let length = layout::align_up(total, self.page_size);

        if length > i32::MAX as usize {
            return Err(CncCreateError::FileLengthTooLarge { length });
        }

        Ok(length)
    }

    /// Reject a layout whose lengths no reader — and no reference driver —
    /// would accept.
    ///
    /// The bounds are the ones the reference parses its configuration with
    /// (`aeron-driver/src/main/c/aeron_driver_context.c:670-695`): the two ring
    /// regions have their defaults as *floors* and `INT32_MAX` as a ceiling,
    /// the counters region has a range of its own, and the page size must be a
    /// power of two so that [`layout::align_up`] means anything.
    ///
    /// # Errors
    ///
    /// [`CncCreateError::LengthOutOfRange`] or
    /// [`CncCreateError::InvalidPageSize`], naming the field.
    pub fn validate(&self) -> Result<(), CncCreateError> {
        for (name, value, min, max) in [
            (
                "to_driver_length",
                self.to_driver_length,
                TO_DRIVER_BUFFER_LENGTH_DEFAULT,
                i32::MAX as usize,
            ),
            (
                "to_clients_length",
                self.to_clients_length,
                TO_CLIENTS_BUFFER_LENGTH_DEFAULT,
                i32::MAX as usize,
            ),
            (
                "counters_values_length",
                self.counters_values_length,
                COUNTERS_VALUES_BUFFER_LENGTH_MIN,
                COUNTERS_VALUES_BUFFER_LENGTH_MAX,
            ),
            (
                "error_log_length",
                self.error_log_length,
                ERROR_LOG_BUFFER_LENGTH_DEFAULT,
                i32::MAX as usize,
            ),
            (
                "page_size",
                self.page_size,
                FILE_PAGE_SIZE_MIN,
                FILE_PAGE_SIZE_MAX,
            ),
        ] {
            if value < min || value > max {
                return Err(CncCreateError::LengthOutOfRange {
                    name,
                    value,
                    min,
                    max,
                });
            }
        }

        if !self.page_size.is_power_of_two() {
            return Err(CncCreateError::InvalidPageSize {
                value: self.page_size,
            });
        }

        // A ring is not its region. The capacity is what is left after the
        // trailer, and both rings mask with `capacity - 1`, so a capacity that
        // is not a power of two computes an index that is not where the record
        // is. The reference checks this when it *builds* each ring
        // (`aeron-client/src/main/c/concurrent/aeron_mpsc_rb.c:27` via
        // `aeron_rb.h:79-82`; `aeron_broadcast_transmitter.c:29` via
        // `aeron_broadcast_descriptor.h:43`), which is why
        // `-Daeron.to.conductor.buffer.length=2m` — a round number that leaves
        // a capacity 768 bytes short of one — starts a driver that dies in
        // conductor init with "Invalid capacity". Checking it while the length
        // is still the thing being reported says which setting is wrong.
        for (region, length, trailer, minimum) in [
            (
                Region::ToDriver,
                self.to_driver_length,
                layout::MPSC_RB_TRAILER_LENGTH,
                layout::MPSC_MIN_CAPACITY,
            ),
            (
                Region::ToClients,
                self.to_clients_length,
                layout::BROADCAST_TRAILER_LENGTH,
                1,
            ),
        ] {
            let capacity = length.saturating_sub(trailer);

            if !layout::ring_capacity_is_valid(capacity, minimum) {
                return Err(CncCreateError::RingCapacityInvalid {
                    region,
                    length,
                    capacity,
                });
            }
        }

        // Every length is at most INT32_MAX and there are five of them, so this
        // cannot overflow a usize on any platform this runs on; the check is
        // for the *metadata field*, which is an int32.
        let file_length = self.file_length()?;

        // The reader's own layout rule, applied to the lengths this build is
        // about to write. Without it a value like `counters.values.buffer.length
        // = 1048579` passes every range check above and then fails *after* the
        // 46 MB file has been written and mapped, as "the CnC file could not be
        // read" about a file this process just wrote. `RegionLayout::compute`
        // is the authority for the rule (`metadata.rs:214-227`); calling it here
        // rather than restating it is what keeps the two from drifting.
        let intended = CncMetadata {
            cnc_version: deepmsg_core::version::CNC_VERSION,
            to_driver_buffer_length: as_i32(self.to_driver_length),
            to_clients_buffer_length: as_i32(self.to_clients_length),
            counter_metadata_buffer_length: as_i32(self.counters_metadata_length()),
            counter_values_buffer_length: as_i32(self.counters_values_length),
            error_log_buffer_length: as_i32(self.error_log_length),
            client_liveness_timeout_ns: 0,
            start_timestamp_ms: 0,
            pid: 0,
            file_page_size: as_i32(self.page_size),
        };

        RegionLayout::compute(&intended, file_length).map_err(|error| match error {
            CncError::RegionUnaligned { region, offset } => CncCreateError::BaseUnaligned {
                name: length_setting_before(region),
                region,
                offset,
            },
            other => CncCreateError::NotReadable(other),
        })?;

        Ok(())
    }
}

/// Who created the file, and how it should treat the clients it will have.
///
/// Three fields the layout does not cover, all of them facts about the running
/// driver rather than about the file's shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CncIdentity {
    /// How long a client may go unheard before the driver may reap it. Written
    /// for clients to read; the driver reaps from its own configuration.
    pub liveness_timeout_ns: i64,
    /// Driver start time, epoch milliseconds.
    pub start_timestamp_ms: i64,
    /// Driver process id.
    pub pid: i64,
}

/// The setting whose length decides where a region's base lands.
///
/// See [`CncCreateError::BaseUnaligned`]: each base is the sum of what is in
/// front of it, so the setting to look at is the length of the region directly
/// before — except for the counters metadata, whose length is four times the
/// values length and so is decided by the values setting.
const fn length_setting_before(region: Region) -> &'static str {
    match region {
        // Bases of 0 and 128 are always aligned; these arms are here because
        // the match has to be total.
        Region::Metadata | Region::ToDriver => "to.conductor.buffer.length",
        Region::ToClients => "to.conductor.buffer.length",
        Region::CountersMetadata => "to.clients.buffer.length",
        Region::CountersValues | Region::ErrorLog => "counters.values.buffer.length",
    }
}

/// Why a CnC file could not be created.
#[derive(Debug)]
pub enum CncCreateError {
    /// A configured length is outside the range the reference accepts.
    LengthOutOfRange {
        /// The field, by the name a caller set it by.
        name: &'static str,
        /// What it was set to.
        value: usize,
        /// The smallest accepted value.
        min: usize,
        /// The largest accepted value.
        max: usize,
    },
    /// A region's base does not land on the 8-byte grid a reader requires, so
    /// the file this build would write is one it could not read back
    /// (`RegionLayout::compute`, `metadata.rs:214-227`).
    ///
    /// `name` is the setting whose length decides where that base lands: a base
    /// is the sum of everything before it, and the last term is the length of
    /// the region in front of it.
    BaseUnaligned {
        /// The setting to look at, by the name a caller set it by.
        name: &'static str,
        /// The region whose base is off the grid.
        region: Region,
        /// Where the base landed.
        offset: usize,
    },
    /// The page size is not a power of two, so the total cannot be aligned to
    /// it (`aeron-driver/src/main/c/aeron_driver.c:475-482`).
    InvalidPageSize {
        /// What it was set to.
        value: usize,
    },
    /// A ring region whose capacity — its length less the trailer — is not one
    /// the ring can be built over.
    RingCapacityInvalid {
        /// The region whose length is wrong.
        region: Region,
        /// The region length that was configured.
        length: usize,
        /// The capacity it implies, after the trailer.
        capacity: usize,
    },
    /// The regions add up to more than the `int32` length field can hold.
    FileLengthTooLarge {
        /// The length that would have been written.
        length: usize,
    },
    /// The file could not be created, mapped or flushed.
    Io(io::Error),
    /// The bytes just written do not read back as a valid CnC file.
    ///
    /// Not a caller error: it means this build would write a file it cannot
    /// read, which the round-trip inside [`CncFile::create`] exists to make
    /// impossible to ship.
    NotReadable(CncError),
    /// The version was about to be published before the driver had written a
    /// heartbeat, which would produce a file every client reads as a dead
    /// driver (`aeron-driver/src/main/c/aeron_driver.c:971-972`).
    NoHeartbeat,
    /// The metadata block is not inside the mapping.
    ///
    /// Every constructor of this type produces a file longer than its own
    /// metadata region, so this is not a caller error — but the mapping is
    /// shared, and something else truncating the file between the two is not
    /// impossible. It used to be reported as [`CncCreateError::FileLengthTooLarge`],
    /// which sent whoever read the message looking at a setting.
    MetadataNotMapped {
        /// The mapping's length as it was when the write was attempted.
        length: usize,
    },
    /// The version read back is not one this build accepts.
    ///
    /// The publish store is the last thing `create` does, so reaching this
    /// means the store did not land — which a shared mapping makes a fault in
    /// the platform seam rather than a caller's mistake.
    VersionNotPublished {
        /// What the block holds instead.
        read_back: i32,
    },
}

impl std::fmt::Display for CncCreateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LengthOutOfRange {
                name,
                value,
                min,
                max,
            } => write!(f, "{name} is {value}, outside {min}..={max}"),
            Self::InvalidPageSize { value } => {
                write!(f, "page size {value} is not a power of two")
            }
            Self::RingCapacityInvalid {
                region,
                length,
                capacity,
            } => write!(
                f,
                "{region} region is {length} bytes, a capacity of {capacity} that is not one the ring can use"
            ),
            Self::MetadataNotMapped { length } => write!(
                f,
                "the mapping is {length} bytes and does not contain the metadata block it \
                 should have been created with"
            ),
            Self::NoHeartbeat => f.write_str(
                "the version cannot be published before the driver's first heartbeat: \
                 every client would read the file as a dead driver",
            ),
            Self::BaseUnaligned {
                name,
                region,
                offset,
            } => write!(
                f,
                "{name} puts the {region:?} region at {offset}, which is not on the 8-byte grid \
                 a reader requires"
            ),
            Self::FileLengthTooLarge { length } => {
                write!(f, "CnC file length {length} does not fit in an int32")
            }
            Self::Io(error) => write!(f, "CnC file could not be created: {error}"),
            Self::NotReadable(error) => {
                write!(f, "the CnC file just written does not read back: {error}")
            }
            Self::VersionNotPublished { read_back } => write!(
                f,
                "the CnC version read back is {read_back}, not the one just stored"
            ),
        }
    }
}

impl std::error::Error for CncCreateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::NotReadable(error) => Some(error),
            Self::LengthOutOfRange { .. }
            | Self::InvalidPageSize { .. }
            | Self::RingCapacityInvalid { .. }
            | Self::BaseUnaligned { .. }
            | Self::NoHeartbeat
            | Self::MetadataNotMapped { .. }
            | Self::FileLengthTooLarge { .. }
            | Self::VersionNotPublished { .. } => None,
        }
    }
}

impl From<io::Error> for CncCreateError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl CncFile {
    /// Create and publish `<aeron_dir>/cnc.dat`.
    ///
    /// The directory must already exist: an aeron directory is the driver's to
    /// make, and with the reference's discipline the decision to *reuse* one —
    /// the stale-driver check, the `EBUSY` on a live one — happens before this
    /// is called (`aeron-driver/src/main/c/aeron_driver.c:136-235`). Creating
    /// parents here would quietly make that decision for the caller.
    ///
    /// The file is left mapped read-write and its layout is validated by the
    /// same code the reader uses, so a `CncFile` from here behaves exactly like
    /// one from [`CncFile::try_open_writable`] — with one deliberate
    /// difference: **the version is still zero**, because nothing may read the
    /// block until [`CncFile::publish`] says so.
    ///
    /// # Errors
    ///
    /// [`CncCreateError`]: an invalid layout, a file that already exists
    /// (`O_EXCL`, `aeron-client/src/main/c/util/aeron_fileutil.c:967`), a
    /// missing directory, or a write that does not read back.
    pub fn create(
        aeron_dir: &Path,
        layout: &CncLayout,
        identity: &CncIdentity,
    ) -> Result<Self, CncCreateError> {
        layout.validate()?;
        let file_length = layout.file_length()?;
        let path = aeron_dir.join(CNC_FILE_NAME);

        let mapping = MappedFile::create(&path, file_length)?;

        // The version stays zero through the whole block: it is the readiness
        // gate, and everything else in the block must be in place before any
        // reader is entitled to look at it.
        let intended = CncMetadata {
            cnc_version: 0,
            to_driver_buffer_length: as_i32(layout.to_driver_length),
            to_clients_buffer_length: as_i32(layout.to_clients_length),
            counter_metadata_buffer_length: as_i32(layout.counters_metadata_length()),
            counter_values_buffer_length: as_i32(layout.counters_values_length),
            error_log_buffer_length: as_i32(layout.error_log_length),
            client_liveness_timeout_ns: identity.liveness_timeout_ns,
            start_timestamp_ms: identity.start_timestamp_ms,
            pid: identity.pid,
            file_page_size: as_i32(layout.page_size),
        };

        let mut block = [0u8; layout::VERSION_AND_METADATA_LENGTH];
        intended
            .encode(&mut block)
            .map_err(CncCreateError::NotReadable)?;

        let head = mapping
            .region_mut(0, layout::VERSION_AND_METADATA_LENGTH)
            .ok_or(CncCreateError::MetadataNotMapped {
                length: file_length,
            })?;

        // Plain bytes for the fields, and no release store: the version stays
        // zero until `publish`, which is what keeps this block unreadable
        // while it is still being filled (`aeron_driver.c:250-269`).
        head.copy_in(0, &block)
            .ok_or(CncCreateError::MetadataNotMapped {
                length: file_length,
            })?;

        // Read back what the mapping actually holds, with the reader's own
        // rules, rather than trusting the values we meant to write. Cheap (one
        // 128-byte copy) and it makes "create produced a file this build cannot
        // lay out" unshippable rather than merely unlikely. The version is not
        // checked here because it is deliberately still zero; `publish` is
        // where it becomes a value, and where it is checked.
        let mut reread = [0u8; layout::VERSION_AND_METADATA_LENGTH];
        mapping
            .region(0, layout::VERSION_AND_METADATA_LENGTH)
            .and_then(|head| head.copy_out(0, &mut reread))
            .ok_or(CncCreateError::MetadataNotMapped {
                length: file_length,
            })?;

        let metadata = CncMetadata::decode(&reread).map_err(CncCreateError::NotReadable)?;
        let region_layout =
            RegionLayout::compute(&metadata, file_length).map_err(CncCreateError::NotReadable)?;

        Ok(Self::from_parts(mapping, metadata, region_layout, path))
    }
}

/// A length that has already been validated to fit its field.
#[allow(clippy::cast_possible_truncation)] // every caller validated <= i32::MAX
fn as_i32(length: usize) -> i32 {
    length as i32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::CncOpenError;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A directory of our own in the system temp directory, removed on drop.
    ///
    /// Hand-rolled, like the one in `deepmsg_core::pal`: the workspace has no
    /// test dependencies and this is all one would be used for.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("deepmsg-cnc-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The smallest layout the reference would accept, so that tests which only
    /// care about the write path do not allocate 46 MB to prove a point.
    fn small() -> CncLayout {
        CncLayout {
            counters_values_length: COUNTERS_VALUES_BUFFER_LENGTH_MIN,
            error_log_length: ERROR_LOG_BUFFER_LENGTH_DEFAULT,
            ..CncLayout::default()
        }
    }

    fn identity() -> CncIdentity {
        CncIdentity {
            liveness_timeout_ns: CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT,
            start_timestamp_ms: TIMESTAMP_MS,
            pid: 4242,
        }
    }

    /// The clock these tests pretend it is, one constant so that a heartbeat
    /// and a start timestamp cannot drift apart.
    const TIMESTAMP_MS: i64 = 1_700_000_000_000;

    #[test]
    fn the_defaults_produce_the_length_a_reference_driver_writes() {
        // The number in `tests/fixtures/README.md`: the file behind
        // `cnc-header.bin` was 48,238,592 bytes.
        assert_eq!(48_238_592, CncLayout::default().file_length().expect("fit"));

        let layout = CncLayout::default();
        assert_eq!(1_049_344, layout.to_driver_length);
        assert_eq!(1_048_704, layout.to_clients_length);
        assert_eq!(33_554_432, layout.counters_metadata_length());
        assert_eq!(8_388_608, layout.counters_values_length);
        assert_eq!(4_194_304, layout.error_log_length);
        assert_eq!(48_235_520, layout.unaligned_length());
    }

    #[test]
    fn a_length_that_puts_a_region_off_the_grid_is_refused_before_the_directory() {
        // Three bytes past the minimum: every range check passes, and the
        // counters metadata region four times that length puts the counters
        // *values* base four bytes off the grid. Without this check the driver
        // writes 46 MB and then fails to read its own file back, minutes after
        // the setting was parsed.
        let layout = CncLayout {
            counters_values_length: COUNTERS_VALUES_BUFFER_LENGTH_MIN + 3,
            ..CncLayout::default()
        };

        let error = layout
            .validate()
            .expect_err("an odd counters length moves every base after it");

        match error {
            CncCreateError::BaseUnaligned { name, region, .. } => {
                assert_eq!("counters.values.buffer.length", name);
                assert_eq!(Region::CountersValues, region);
            }
            other => panic!("expected the setting to be named, got {other:?}"),
        }
    }

    #[test]
    fn a_length_that_puts_the_to_clients_region_off_the_grid_is_refused() {
        // The other end of the same rule: the to-clients base is 128 plus the
        // to-driver length, so a to-driver length off the grid moves it. The
        // ring check refuses this length first (a capacity that is not a power
        // of two), which is why the message names *that* rule — the point of
        // the test is that the refusal happens at all, and at validation.
        let layout = CncLayout {
            to_driver_length: CncLayout::default().to_driver_length + 4,
            ..CncLayout::default()
        };

        assert!(layout.validate().is_err(), "refused, and before any file");
    }

    #[test]
    fn counters_metadata_is_four_times_the_values_region() {
        for values in [COUNTERS_VALUES_BUFFER_LENGTH_MIN, 8 * 1024 * 1024] {
            let layout = CncLayout {
                counters_values_length: values,
                ..small()
            };
            assert_eq!(4 * values, layout.counters_metadata_length());
        }
    }

    #[test]
    fn the_default_layout_validates() {
        assert!(CncLayout::default().validate().is_ok());
    }

    #[test]
    fn rejects_lengths_the_reference_would_reject() {
        let cases: [(&str, CncLayout, usize); 5] = [
            (
                "to_driver_length",
                CncLayout {
                    to_driver_length: TO_DRIVER_BUFFER_LENGTH_DEFAULT - 8,
                    ..small()
                },
                TO_DRIVER_BUFFER_LENGTH_DEFAULT - 8,
            ),
            (
                "to_clients_length",
                CncLayout {
                    to_clients_length: 1024,
                    ..small()
                },
                1024,
            ),
            (
                "counters_values_length",
                CncLayout {
                    counters_values_length: 512 * 1024,
                    ..small()
                },
                512 * 1024,
            ),
            (
                "error_log_length",
                CncLayout {
                    error_log_length: ERROR_LOG_BUFFER_LENGTH_DEFAULT - 8,
                    ..small()
                },
                ERROR_LOG_BUFFER_LENGTH_DEFAULT - 8,
            ),
            (
                // A floor is not the only bound: each of these fields is an
                // int32 in the block, so the ceiling is one too.
                "to_clients_length",
                CncLayout {
                    to_clients_length: i32::MAX as usize + 1,
                    ..small()
                },
                i32::MAX as usize + 1,
            ),
        ];

        for (name, layout, value) in cases {
            let error = layout
                .validate()
                .expect_err("the reference rejects this, so a driver must too");
            match error {
                CncCreateError::LengthOutOfRange {
                    name: got_name,
                    value: got_value,
                    min,
                    max,
                } => {
                    assert_eq!(name, got_name);
                    assert_eq!(value, got_value);
                    assert!(
                        got_value < min || got_value > max,
                        "{got_value} was reported as out of {min}..={max}, which contains it"
                    );
                }
                other => panic!("expected LengthOutOfRange for {name}, got {other:?}"),
            }
        }

        // And the floors are boundaries rather than values one past them: the
        // error-log default and the counters minimum are the smallest lengths
        // the reference takes, so the smallest valid layout must pass.
        assert!(
            small().validate().is_ok(),
            "the floors are accepted, and the cases above are below them"
        );
    }

    #[test]
    fn rejects_a_ring_whose_capacity_is_not_a_power_of_two() {
        // `-Daeron.to.conductor.buffer.length=2m` is the mistake this exists
        // for: two mebibytes of region leave a capacity 768 bytes short of a
        // power of two, and the reference dies on it in conductor init with
        // "Invalid capacity".
        let error = CncLayout {
            to_driver_length: 2 * 1024 * 1024,
            ..small()
        }
        .validate()
        .expect_err("2 MiB of region is not a legal ring");

        assert!(matches!(
            error,
            CncCreateError::RingCapacityInvalid {
                region: Region::ToDriver,
                ..
            }
        ));

        // The same mistake on the broadcast region, where the trailer is 128
        // rather than 768: 2 MiB of region leaves 128 bytes short of one.
        let error = CncLayout {
            to_clients_length: 2 * 1024 * 1024,
            ..small()
        }
        .validate()
        .expect_err("2 MiB of region is not a legal broadcast ring");

        assert!(matches!(
            error,
            CncCreateError::RingCapacityInvalid {
                region: Region::ToClients,
                ..
            }
        ));

        // And the legal spelling of the same intent: a gibibyte of capacity,
        // plus its trailer, is a length no round-number setting would give
        // you.
        assert!(
            CncLayout {
                to_clients_length: (1 << 30) + layout::BROADCAST_TRAILER_LENGTH,
                ..small()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn rejects_a_page_size_that_is_not_a_power_of_two() {
        let layout = CncLayout {
            page_size: 4096 + 8,
            ..small()
        };

        assert_eq!(
            "page size 4104 is not a power of two",
            layout
                .validate()
                .expect_err("4104 is not a power of two")
                .to_string()
        );
    }

    #[test]
    fn rejects_a_total_that_does_not_fit_an_int32() {
        // Two rings of a gibibyte each: legal capacities, and a total the
        // int32 length fields cannot describe.
        let layout = CncLayout {
            to_driver_length: (1 << 30) + layout::MPSC_RB_TRAILER_LENGTH,
            to_clients_length: (1 << 30) + layout::BROADCAST_TRAILER_LENGTH,
            ..small()
        };

        assert!(matches!(
            layout.validate(),
            Err(CncCreateError::FileLengthTooLarge { .. })
        ));
    }

    #[test]
    fn creates_a_file_the_reader_accepts_once_it_is_published() {
        let dir = TempDir::new();
        let layout = small();
        let mut created = CncFile::create(&dir.0, &layout, &identity()).expect("create");

        assert_eq!(layout.file_length().expect("fit"), created.file_length());
        assert!(created.to_driver_ring().is_some(), "read-write mapping");

        // Created is not readable, and that is the gate doing its job: the
        // version is still zero, so a reader must be told to retry rather than
        // handed a block whose fields are still being filled.
        assert_eq!(0, created.cnc_version());
        assert!(matches!(
            CncFile::try_open_writable(&dir.0),
            Err(CncOpenError::NotReady)
        ));

        // The driver's first heartbeat, one line before it publishes — the
        // order `publish` now insists on.
        created
            .write_consumer_heartbeat(TIMESTAMP_MS)
            .expect("the ring is writable");

        created.publish().expect("publish");

        assert_eq!(deepmsg_core::version::CNC_VERSION, created.cnc_version());

        // The same file, through the path a client takes.
        let reopened = CncFile::try_open_writable(&dir.0).expect("reopen");
        assert_eq!(created.metadata(), reopened.metadata());
        assert_eq!(created.layout(), reopened.layout());

        let metadata = reopened.metadata();
        assert_eq!(
            layout.to_driver_length as i32,
            metadata.to_driver_buffer_length
        );
        assert_eq!(
            layout.counters_metadata_length() as i32,
            metadata.counter_metadata_buffer_length
        );
        assert_eq!(4_242, metadata.pid);
        assert_eq!(1_700_000_000_000, metadata.start_timestamp_ms);
        assert_eq!(
            CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT,
            metadata.client_liveness_timeout_ns
        );
        assert_eq!(layout.page_size as i32, metadata.file_page_size);
    }

    #[test]
    fn creates_a_file_whose_regions_start_where_the_layout_says() {
        let dir = TempDir::new();
        let layout = small();
        let created = CncFile::create(&dir.0, &layout, &identity()).expect("create");
        let regions = created.layout();

        assert_eq!(layout::VERSION_AND_METADATA_LENGTH, regions.to_driver.start);
        assert_eq!(
            regions.to_driver.start + layout.to_driver_length,
            regions.to_clients.start
        );
        assert_eq!(
            regions.to_clients.start + layout.to_clients_length,
            regions.counters_metadata.start
        );
        assert_eq!(
            regions.counters_values.end, regions.error_log.start,
            "contiguous, no padding between regions"
        );
        assert_eq!(
            layout.unaligned_length(),
            regions.error_log.end,
            "and the last region ends at the unaligned total"
        );
    }

    #[test]
    fn a_second_driver_cannot_create_over_the_first() {
        let dir = TempDir::new();
        let mut first = CncFile::create(&dir.0, &small(), &identity()).expect("first");
        first
            .write_consumer_heartbeat(TIMESTAMP_MS)
            .expect("the ring is writable");
        first.publish().expect("publish");

        let error = CncFile::create(&dir.0, &small(), &identity()).expect_err("must not clobber");

        // Not malformed, not a panic: the file is still the first driver's, and
        // the decision to reuse it belongs to the directory discipline.
        assert!(!matches!(error, CncCreateError::NotReadable(_)));
        assert_eq!(io::ErrorKind::AlreadyExists, io_error(&error).kind());
        assert!(CncFile::try_open_writable(&dir.0).is_ok());
    }

    #[test]
    fn publishing_twice_is_just_the_same_store_twice() {
        let dir = TempDir::new();
        let mut created = CncFile::create(&dir.0, &small(), &identity()).expect("create");
        created
            .write_consumer_heartbeat(TIMESTAMP_MS)
            .expect("the ring is writable");

        created.publish().expect("first");
        created.publish().expect("second");

        assert_eq!(deepmsg_core::version::CNC_VERSION, created.cnc_version());
        assert!(
            CncFile::try_open_writable(&dir.0).is_ok(),
            "the file is still the one this type describes"
        );
    }

    #[test]
    fn refuses_to_create_in_a_directory_that_is_not_there() {
        let dir = TempDir::new();
        let missing = dir.0.join("not-created");

        let error = CncFile::create(&missing, &small(), &identity()).expect_err("no directory");

        assert_eq!(io::ErrorKind::NotFound, io_error(&error).kind());
        assert!(!missing.exists(), "and it does not make one");
    }

    #[test]
    fn a_rejected_layout_creates_nothing() {
        let dir = TempDir::new();
        let layout = CncLayout {
            counters_values_length: 1,
            ..small()
        };

        assert!(CncFile::create(&dir.0, &layout, &identity()).is_err());
        assert!(!dir.0.join(CNC_FILE_NAME).exists());
    }

    fn io_error(error: &CncCreateError) -> &io::Error {
        match error {
            CncCreateError::Io(io) => io,
            other => panic!("expected an io error, got {other:?}"),
        }
    }
}
