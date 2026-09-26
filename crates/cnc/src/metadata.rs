//! The CnC metadata block, and the region layout it describes.
//!
//! Both types are decoded from a *snapshot* of the bytes rather than read live
//! through atomics. That is deliberate and it is what makes them testable:
//! the caller reads `cnc_version` with an acquire first, and an acquire on
//! that field orders every other field in the block, because the driver fills
//! them before publishing the version (`aeron_driver.c:252-260` then `:972`).
//! After the acquire, a bounded copy is a coherent view of the block.

use core::ops::Range;

use crate::error::{CncError, Region};
use crate::layout;

/// The ten fields the driver publishes about the file it created.
///
/// Offsets and widths are in [`crate::layout`]; this is the same data as a
/// typed value. Field names keep the reference's, with units made explicit
/// where the reference leaves them implicit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CncMetadata {
    /// Packed semantic version; zero means "not published yet". Classify it
    /// with [`deepmsg_core::version::check_cnc_version`].
    pub cnc_version: i32,
    /// Length of the to-driver command ring region.
    pub to_driver_buffer_length: i32,
    /// Length of the to-clients broadcast region.
    pub to_clients_buffer_length: i32,
    /// Length of the counters metadata region.
    pub counter_metadata_buffer_length: i32,
    /// Length of the counters values region.
    pub counter_values_buffer_length: i32,
    /// Length of the error-log region.
    pub error_log_buffer_length: i32,
    /// Client liveness timeout, **nanoseconds**.
    pub client_liveness_timeout_ns: i64,
    /// Driver start time, epoch **milliseconds**.
    pub start_timestamp_ms: i64,
    /// Driver process id.
    pub pid: i64,
    /// Page size the writer aligned the file to. Exposed as written; a reader
    /// does not need it, and there is deliberately no fallback for the zero a
    /// pre-1.48 writer leaves here.
    pub file_page_size: i32,
}

impl CncMetadata {
    /// Decode a snapshot of the metadata region.
    ///
    /// # Errors
    ///
    /// [`CncError::FileTooShort`] if `block` is shorter than the metadata
    /// region. The fields themselves are not validated here — a negative
    /// region length is legal to *decode* and is rejected by
    /// [`RegionLayout::compute`], so that a caller can inspect a corrupt block
    /// before deciding what to do about it.
    pub fn decode(block: &[u8]) -> Result<Self, CncError> {
        if block.len() < layout::VERSION_AND_METADATA_LENGTH {
            return Err(CncError::FileTooShort {
                length: block.len(),
            });
        }

        Ok(Self {
            cnc_version: le_i32(block, layout::CNC_VERSION_OFFSET),
            to_driver_buffer_length: le_i32(block, layout::TO_DRIVER_BUFFER_LENGTH_OFFSET),
            to_clients_buffer_length: le_i32(block, layout::TO_CLIENTS_BUFFER_LENGTH_OFFSET),
            counter_metadata_buffer_length: le_i32(
                block,
                layout::COUNTER_METADATA_BUFFER_LENGTH_OFFSET,
            ),
            counter_values_buffer_length: le_i32(
                block,
                layout::COUNTER_VALUES_BUFFER_LENGTH_OFFSET,
            ),
            error_log_buffer_length: le_i32(block, layout::ERROR_LOG_BUFFER_LENGTH_OFFSET),
            client_liveness_timeout_ns: le_i64(block, layout::CLIENT_LIVENESS_TIMEOUT_OFFSET),
            start_timestamp_ms: le_i64(block, layout::START_TIMESTAMP_OFFSET),
            pid: le_i64(block, layout::PID_OFFSET),
            file_page_size: le_i32(block, layout::FILE_PAGE_SIZE_OFFSET),
        })
    }
}

/// Where each region of the file lives.
///
/// The five regions are contiguous, beginning immediately after the 128-byte
/// metadata region, with no padding between them. Only the *total* is aligned
/// to the writer's page size (`aeron_cnc_file_descriptor.h:93-96`); the
/// individual lengths are not, which is why every base is checked for the
/// alignment its accessors need rather than assumed to have it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegionLayout {
    /// The MPSC command ring.
    pub to_driver: Range<usize>,
    /// The broadcast region.
    pub to_clients: Range<usize>,
    /// Counter metadata records.
    pub counters_metadata: Range<usize>,
    /// Counter value records.
    pub counters_values: Range<usize>,
    /// The distinct error log.
    pub error_log: Range<usize>,
    /// The file's real length, as measured — not as claimed.
    pub file_length: usize,
}

impl RegionLayout {
    /// Lay the regions out end to end and check them against the file.
    ///
    /// # Errors
    ///
    /// [`CncError::RegionLengthNotPositive`] for a zero or negative length,
    /// [`CncError::CountersBuffersInconsistent`] if the two counter regions
    /// disagree, [`CncError::RegionUnaligned`] if a base is off the 8-byte
    /// grid, or [`CncError::RegionsExceedFile`] if the whole thing does not
    /// fit.
    pub fn compute(metadata: &CncMetadata, file_length: usize) -> Result<Self, CncError> {
        let to_driver_len = positive(Region::ToDriver, metadata.to_driver_buffer_length)?;
        let to_clients_len = positive(Region::ToClients, metadata.to_clients_buffer_length)?;
        let counters_metadata_len = positive(
            Region::CountersMetadata,
            metadata.counter_metadata_buffer_length,
        )?;
        let counters_values_len = positive(
            Region::CountersValues,
            metadata.counter_values_buffer_length,
        )?;
        let error_log_len = positive(Region::ErrorLog, metadata.error_log_buffer_length)?;

        if counters_metadata_len < 4 * counters_values_len {
            return Err(CncError::CountersBuffersInconsistent {
                metadata_length: metadata.counter_metadata_buffer_length,
                values_length: metadata.counter_values_buffer_length,
            });
        }

        // Every length is at most i32::MAX and there are five of them, so the
        // running total stays far inside u64; the arithmetic below cannot
        // overflow, which is why the wrapping the C reader relies on has no
        // analogue here.
        let base = layout::VERSION_AND_METADATA_LENGTH as u64;
        let to_driver_start = base;
        let to_clients_start = to_driver_start + to_driver_len;
        let counters_metadata_start = to_clients_start + to_clients_len;
        let counters_values_start = counters_metadata_start + counters_metadata_len;
        let error_log_start = counters_values_start + counters_values_len;
        let end = error_log_start + error_log_len;

        for (region, start) in [
            (Region::ToDriver, to_driver_start),
            (Region::ToClients, to_clients_start),
            (Region::CountersMetadata, counters_metadata_start),
            (Region::CountersValues, counters_values_start),
            (Region::ErrorLog, error_log_start),
        ] {
            if 0 != start % 8 {
                return Err(CncError::RegionUnaligned {
                    region,
                    offset: start as usize,
                });
            }
        }

        for (region, length, minimum) in [
            (
                Region::ToDriver,
                to_driver_len,
                layout::MPSC_RB_TRAILER_LENGTH,
            ),
            (
                Region::ToClients,
                to_clients_len,
                layout::BROADCAST_TRAILER_LENGTH,
            ),
            (
                Region::CountersMetadata,
                counters_metadata_len,
                layout::COUNTER_METADATA_LENGTH,
            ),
            (
                Region::CountersValues,
                counters_values_len,
                layout::COUNTER_VALUE_LENGTH,
            ),
            (
                Region::ErrorLog,
                error_log_len,
                layout::ERROR_LOG_HEADER_LENGTH,
            ),
        ] {
            if length < minimum as u64 {
                return Err(CncError::RegionTooSmall {
                    region,
                    length: length as usize,
                    minimum,
                });
            }
        }

        if end > file_length as u64 {
            return Err(CncError::RegionsExceedFile {
                required: end as usize,
                file_length,
            });
        }

        #[allow(clippy::cast_possible_truncation)] // proven <= file_length, a usize
        let at = |start: u64, len: u64| start as usize..(start + len) as usize;

        Ok(Self {
            to_driver: at(to_driver_start, to_driver_len),
            to_clients: at(to_clients_start, to_clients_len),
            counters_metadata: at(counters_metadata_start, counters_metadata_len),
            counters_values: at(counters_values_start, counters_values_len),
            error_log: at(error_log_start, error_log_len),
            file_length,
        })
    }
}

/// Reject a length the file cannot possibly mean.
fn positive(region: Region, length: i32) -> Result<u64, CncError> {
    if length <= 0 {
        return Err(CncError::RegionLengthNotPositive { region, length });
    }

    Ok(length as u64)
}

/// Read a little-endian `i32` from a snapshot.
///
/// Indexing is safe because every caller has already established that the
/// block is at least as long as the metadata region.
fn le_i32(block: &[u8], offset: usize) -> i32 {
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&block[offset..offset + 4]);
    i32::from_le_bytes(bytes)
}

/// Read a little-endian `i64` from a snapshot.
fn le_i64(block: &[u8], offset: usize) -> i64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&block[offset..offset + 8]);
    i64::from_le_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a metadata block with plausible defaults, overridable by closure.
    ///
    /// Hand-built rather than captured from a driver so that the hostile cases
    /// below — negative lengths, off-grid bases — can be expressed at all;
    /// the live driver only ever produces well-formed blocks.
    pub(crate) fn block_with(f: impl FnOnce(&mut CncMetadata)) -> [u8; 128] {
        let mut metadata = CncMetadata {
            cnc_version: deepmsg_core::version::CNC_VERSION,
            to_driver_buffer_length: 1024 * 1024 + layout::MPSC_RB_TRAILER_LENGTH as i32,
            to_clients_buffer_length: 1024 * 1024 + layout::BROADCAST_TRAILER_LENGTH as i32,
            counter_metadata_buffer_length: 32 * 1024 * 1024,
            counter_values_buffer_length: 8 * 1024 * 1024,
            error_log_buffer_length: 4 * 1024 * 1024,
            client_liveness_timeout_ns: 10_000_000_000,
            start_timestamp_ms: 1_700_000_000_000,
            pid: 4242,
            file_page_size: 4096,
        };
        f(&mut metadata);

        let mut block = [0u8; layout::VERSION_AND_METADATA_LENGTH];
        put_i32(&mut block, layout::CNC_VERSION_OFFSET, metadata.cnc_version);
        put_i32(
            &mut block,
            layout::TO_DRIVER_BUFFER_LENGTH_OFFSET,
            metadata.to_driver_buffer_length,
        );
        put_i32(
            &mut block,
            layout::TO_CLIENTS_BUFFER_LENGTH_OFFSET,
            metadata.to_clients_buffer_length,
        );
        put_i32(
            &mut block,
            layout::COUNTER_METADATA_BUFFER_LENGTH_OFFSET,
            metadata.counter_metadata_buffer_length,
        );
        put_i32(
            &mut block,
            layout::COUNTER_VALUES_BUFFER_LENGTH_OFFSET,
            metadata.counter_values_buffer_length,
        );
        put_i32(
            &mut block,
            layout::ERROR_LOG_BUFFER_LENGTH_OFFSET,
            metadata.error_log_buffer_length,
        );
        put_i64(
            &mut block,
            layout::CLIENT_LIVENESS_TIMEOUT_OFFSET,
            metadata.client_liveness_timeout_ns,
        );
        put_i64(
            &mut block,
            layout::START_TIMESTAMP_OFFSET,
            metadata.start_timestamp_ms,
        );
        put_i64(&mut block, layout::PID_OFFSET, metadata.pid);
        put_i32(
            &mut block,
            layout::FILE_PAGE_SIZE_OFFSET,
            metadata.file_page_size,
        );
        block
    }

    /// Write a little-endian `i32` into a block under construction.
    fn put_i32(block: &mut [u8], offset: usize, value: i32) {
        block[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    /// Write a little-endian `i64` into a block under construction.
    fn put_i64(block: &mut [u8], offset: usize, value: i64) {
        block[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    /// The length a well-formed file would have.
    pub(crate) fn total_length(metadata: &CncMetadata) -> usize {
        let sum = layout::VERSION_AND_METADATA_LENGTH
            + metadata.to_driver_buffer_length as usize
            + metadata.to_clients_buffer_length as usize
            + metadata.counter_metadata_buffer_length as usize
            + metadata.counter_values_buffer_length as usize
            + metadata.error_log_buffer_length as usize;
        layout::align_up(sum, metadata.file_page_size as usize)
    }

    #[test]
    fn decodes_a_well_formed_block() {
        let block = block_with(|_| {});
        let metadata = CncMetadata::decode(&block).expect("decode");

        assert_eq!(deepmsg_core::version::CNC_VERSION, metadata.cnc_version);
        assert_eq!(1024 * 1024 + 768, metadata.to_driver_buffer_length);
        assert_eq!(1024 * 1024 + 128, metadata.to_clients_buffer_length);
        assert_eq!(32 * 1024 * 1024, metadata.counter_metadata_buffer_length);
        assert_eq!(8 * 1024 * 1024, metadata.counter_values_buffer_length);
        assert_eq!(4 * 1024 * 1024, metadata.error_log_buffer_length);
        assert_eq!(10_000_000_000, metadata.client_liveness_timeout_ns);
        assert_eq!(1_700_000_000_000, metadata.start_timestamp_ms);
        assert_eq!(4242, metadata.pid);
        assert_eq!(4096, metadata.file_page_size);
    }

    #[test]
    fn refuses_a_block_shorter_than_the_region() {
        let block = [0u8; layout::METADATA_STRUCT_LENGTH];
        assert_eq!(
            Err(CncError::FileTooShort { length: 52 }),
            CncMetadata::decode(&block),
            "52 bytes is the struct, not the region -- the region is what must fit"
        );
    }

    #[test]
    fn lays_regions_out_end_to_end() {
        let block = block_with(|_| {});
        let metadata = CncMetadata::decode(&block).expect("decode");
        let layout = RegionLayout::compute(&metadata, total_length(&metadata)).expect("layout");

        assert_eq!(128..128 + 1024 * 1024 + 768, layout.to_driver);
        assert_eq!(layout.to_driver.end, layout.to_clients.start);
        assert_eq!(layout.to_clients.end, layout.counters_metadata.start);
        assert_eq!(layout.counters_metadata.end, layout.counters_values.start);
        assert_eq!(layout.counters_values.end, layout.error_log.start);
    }

    #[test]
    fn rejects_a_negative_region_length_without_wrapping() {
        // The C reader survives this by casting to size_t, which wraps huge and
        // makes its length check fail by accident. In Rust the same cast would
        // overflow the sum, so it is rejected explicitly.
        for bad in [-1, i32::MIN, 0] {
            let block = block_with(|m| m.to_driver_buffer_length = bad);
            let metadata = CncMetadata::decode(&block).expect("decode");
            assert_eq!(
                Err(CncError::RegionLengthNotPositive {
                    region: Region::ToDriver,
                    length: bad,
                }),
                RegionLayout::compute(&metadata, usize::MAX),
                "length {bad} must be rejected before any arithmetic"
            );
        }
    }

    /// The sum of the regions and the 128-byte metadata region, *without* the
    /// page alignment the writer applies to the total.
    fn unaligned_total(metadata: &CncMetadata) -> usize {
        layout::VERSION_AND_METADATA_LENGTH
            + metadata.to_driver_buffer_length as usize
            + metadata.to_clients_buffer_length as usize
            + metadata.counter_metadata_buffer_length as usize
            + metadata.counter_values_buffer_length as usize
            + metadata.error_log_buffer_length as usize
    }

    #[test]
    fn rejects_regions_that_do_not_fit_the_file() {
        let block = block_with(|_| {});
        let metadata = CncMetadata::decode(&block).expect("decode");
        let needed = unaligned_total(&metadata);

        let error = RegionLayout::compute(&metadata, needed - 1).expect_err("must not fit");
        match error {
            CncError::RegionsExceedFile {
                required,
                file_length,
            } => {
                assert_eq!(
                    needed, required,
                    "the requirement is the unaligned sum, not the page-aligned total"
                );
                assert_eq!(needed - 1, file_length);
            }
            other => panic!("expected RegionsExceedFile, got {other:?}"),
        }
    }

    #[test]
    fn the_page_aligned_total_is_what_the_writer_produces() {
        // The writer aligns the total to its page size
        // (`aeron_cnc_file_descriptor.h:93-96`), so a real file is usually
        // longer than the sum of its regions. This pins the relationship the
        // interop suite then checks against an actual driver.
        let block = block_with(|_| {});
        let metadata = CncMetadata::decode(&block).expect("decode");

        let aligned = total_length(&metadata);
        assert!(aligned >= unaligned_total(&metadata));
        assert_eq!(0, aligned % metadata.file_page_size as usize);
        assert!(RegionLayout::compute(&metadata, aligned).is_ok());
    }

    #[test]
    fn accepts_a_file_with_trailing_page_padding() {
        // Only the total is page-aligned; the extra slack is normal.
        let block = block_with(|_| {});
        let metadata = CncMetadata::decode(&block).expect("decode");
        assert!(RegionLayout::compute(&metadata, total_length(&metadata) + 4096).is_ok());
    }

    #[test]
    fn rejects_an_off_grid_region_base() {
        // An odd to-driver length pushes every later region off the 8-byte
        // grid, which no atomic accessor could read from.
        let block = block_with(|m| m.to_driver_buffer_length = 1024 * 1024 + 768 + 4);
        let metadata = CncMetadata::decode(&block).expect("decode");

        let error = RegionLayout::compute(&metadata, usize::MAX).expect_err("must be unaligned");
        assert_eq!(
            CncError::RegionUnaligned {
                region: Region::ToClients,
                offset: 128 + 1024 * 1024 + 768 + 4,
            },
            error
        );
    }

    #[test]
    fn rejects_counter_regions_that_disagree() {
        let block = block_with(|m| m.counter_metadata_buffer_length = 8 * 1024 * 1024);
        let metadata = CncMetadata::decode(&block).expect("decode");

        assert_eq!(
            Err(CncError::CountersBuffersInconsistent {
                metadata_length: 8 * 1024 * 1024,
                values_length: 8 * 1024 * 1024,
            }),
            RegionLayout::compute(&metadata, usize::MAX),
            "the reference floor is metadata >= 4 * values"
        );
    }
}
