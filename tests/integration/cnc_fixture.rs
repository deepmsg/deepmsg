//! Golden test over a captured CnC header.
//!
//! The interop suite only runs where the reference checkout exists, so without
//! this file CI would be green while proving nothing about the one contract
//! P0-a exists to prove. These 512 bytes were written by a real
//! default-configured `aeronmd` 1.53.2, and `tests/fixtures/README.md` records
//! how to capture them again.
//!
//! What this covers: the metadata block, every field's offset and width, the
//! packed version encoding, and the region arithmetic against a real file
//! length. What it does *not* cover: the contents of the regions themselves,
//! which sit megabytes into the file and are exercised by the synthetic
//! buffers in `deepmsg-cnc` and by the interop suite.

use deepmsg_cnc::layout;
use deepmsg_cnc::metadata::{CncMetadata, RegionLayout};
use deepmsg_core::version::{self, CncVersionCompatibility};

/// The first 512 bytes of the driver's `cnc.dat`.
const HEADER: &[u8] = include_bytes!("../fixtures/cnc-header.bin");

/// How long that file actually was before being truncated for committing.
///
/// A default-configured driver: the counter regions alone are 40 MB of it.
/// This number is the crux of the test — it is what the region arithmetic has
/// to reproduce from the metadata alone.
const CAPTURED_FILE_LENGTH: usize = 48_238_592;

#[test]
fn the_captured_header_decodes_to_the_reference_defaults() {
    let metadata = CncMetadata::decode(HEADER).expect("the fixture is a valid metadata block");

    // The trap this whole slice was planned around: 0.2.0 is packed into one
    // int32 as (major << 16) | (minor << 8) | patch, so it reads as 512 -- not
    // as a pair of int32s, and not as 0.2.
    assert_eq!(512, metadata.cnc_version);
    assert_eq!(
        CncVersionCompatibility::Compatible,
        version::check_cnc_version(metadata.cnc_version)
    );
    assert_eq!("0.2.0", version::format_version(metadata.cnc_version));

    // The very bytes, so that a future reader can see where 512 comes from
    // without running anything.
    assert_eq!(
        [0x00, 0x02, 0x00, 0x00],
        HEADER[0..4],
        "cnc_version, little-endian"
    );

    assert_eq!(1_049_344, metadata.to_driver_buffer_length);
    assert_eq!(1_048_704, metadata.to_clients_buffer_length);
    assert_eq!(33_554_432, metadata.counter_metadata_buffer_length);
    assert_eq!(8_388_608, metadata.counter_values_buffer_length);
    assert_eq!(4_194_304, metadata.error_log_buffer_length);
    assert_eq!(10_000_000_000, metadata.client_liveness_timeout_ns);
    assert_eq!(4096, metadata.file_page_size);
}

#[test]
fn the_region_arithmetic_reproduces_the_real_file_length() {
    let metadata = CncMetadata::decode(HEADER).expect("decode");
    let regions = RegionLayout::compute(&metadata, CAPTURED_FILE_LENGTH)
        .expect("a real driver's block must describe a readable layout");

    // The unaligned sum of the regions: where the last one ends.
    let sum = layout::VERSION_AND_METADATA_LENGTH
        + metadata.to_driver_buffer_length as usize
        + metadata.to_clients_buffer_length as usize
        + metadata.counter_metadata_buffer_length as usize
        + metadata.counter_values_buffer_length as usize
        + metadata.error_log_buffer_length as usize;
    assert_eq!(sum, regions.error_log.end);

    // Only the total is page-aligned, and that is exactly what the driver
    // wrote to disk. Reproducing 48,238,592 from the metadata alone is the
    // assertion that the offsets are right.
    assert_eq!(
        CAPTURED_FILE_LENGTH,
        layout::align_up(sum, metadata.file_page_size as usize)
    );

    // The regions are contiguous, beginning right after the metadata region.
    assert_eq!(layout::VERSION_AND_METADATA_LENGTH, regions.to_driver.start);
    assert_eq!(regions.to_driver.end, regions.to_clients.start);
    assert_eq!(regions.to_clients.end, regions.counters_metadata.start);
    assert_eq!(regions.counters_metadata.end, regions.counters_values.start);
    assert_eq!(regions.counters_values.end, regions.error_log.start);
}

#[test]
fn the_captured_driver_identification_is_plausible() {
    let metadata = CncMetadata::decode(HEADER).expect("decode");

    assert!(metadata.pid > 0, "a real driver has a real pid");
    // 2020-01-01, and before 2100: a start timestamp outside that is a
    // decoding mistake, not a clock.
    assert!((1_577_836_800_000..4_102_444_800_000).contains(&metadata.start_timestamp_ms));
}

#[test]
fn the_truncated_fixture_is_not_mistaken_for_a_whole_file() {
    // The fixture is 512 bytes and the metadata says the regions need ~48 MB.
    // A reader that trusted the metadata without checking it against the real
    // length would happily compute offsets into memory it does not have.
    let metadata = CncMetadata::decode(HEADER).expect("decode");

    assert_eq!(512, HEADER.len());
    let error = RegionLayout::compute(&metadata, HEADER.len())
        .expect_err("the fixture is far too short to hold the regions it describes");

    match error {
        deepmsg_cnc::CncError::RegionsExceedFile {
            required,
            file_length,
        } => {
            assert_eq!(512, file_length);
            assert!(required > file_length);
        }
        other => panic!("expected RegionsExceedFile, got {other:?}"),
    }
}
