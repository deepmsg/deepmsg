//! What a driver *writes*, against what a reference driver wrote.
//!
//! `tests/fixtures/cnc-header.bin` is the first 512 bytes of the `cnc.dat` a
//! default-configured `aeronmd` 1.53.2 left behind. P0 used it to prove a
//! reader — that this build can understand a real driver's file. This uses it
//! to prove a writer, which is the harder direction: a reader that gets an
//! offset wrong fails, while a writer that gets one wrong produces a file that
//! looks entirely plausible until something else reads it.
//!
//! No reference checkout is needed, so this runs in CI.

use deepmsg_cnc::metadata::{CncMetadata, RegionLayout};
use deepmsg_cnc::{CncFile, CncIdentity, CncLayout};
use deepmsg_tests::temp::TempDir;

/// The first 512 bytes of a real driver's `cnc.dat`.
const HEADER: &[u8] = include_bytes!("../fixtures/cnc-header.bin");

/// How long that file was, from `tests/fixtures/README.md`.
const CAPTURED_FILE_LENGTH: usize = 48_238_592;

/// The two fields that describe the *process* rather than the file, and which
/// therefore cannot match a capture from another process.
const START_TIMESTAMP: std::ops::Range<usize> = 32..40;
const PID: std::ops::Range<usize> = 40..48;

fn identity() -> CncIdentity {
    CncIdentity {
        liveness_timeout_ns: 10_000_000_000,
        start_timestamp_ms: 1_700_000_000_000,
        pid: 4242,
    }
}

/// A written CnC file: how long it is, and the metadata block at the front.
///
/// The block is read and the length is measured, rather than reading the file
/// into memory: a default driver's file is 46 MB and nothing here looks past
/// its first 128 bytes. What the length is *for* is the region arithmetic —
/// the same thing the capture's own length proves — and that comes from the
/// file system just as well.
struct Written {
    length: usize,
    block: Vec<u8>,
}

/// A CnC file as a default-configured driver writes it.
fn created() -> (TempDir, Written) {
    let dir = TempDir::new("deepmsg-create");
    let mut cnc = CncFile::create(dir.path(), &CncLayout::default(), &identity())
        .expect("the default layout is one a driver may be configured with");

    // The driver's first heartbeat, then the version: the order `publish`
    // insists on, and the reason it can.
    cnc.write_consumer_heartbeat(1_700_000_000_000)
        .expect("the ring is writable");
    cnc.publish().expect("publish");

    let path = dir.path().join(deepmsg_cnc::CNC_FILE_NAME);
    let length = std::fs::metadata(&path).expect("measure it").len() as usize;
    let mut block = vec![0u8; 128];
    std::io::Read::read_exact(
        &mut std::fs::File::open(&path).expect("open it"),
        &mut block,
    )
    .expect("read the metadata block");

    (dir, Written { length, block })
}

#[test]
fn the_default_layout_is_the_one_the_capture_came_from() {
    // The region arithmetic on its own, before any file is involved: the sum
    // rounded to the page size is the length a real driver left on disk.
    assert_eq!(
        CAPTURED_FILE_LENGTH,
        CncLayout::default().file_length().expect("fits")
    );
}

#[test]
fn a_written_file_reproduces_the_reference_metadata_field_by_field() {
    let (_dir, file) = created();
    let written = CncMetadata::decode(&file.block).expect("decode what we wrote");
    let reference = CncMetadata::decode(HEADER).expect("decode the capture");

    // Field by field first, so that a mismatch names the field rather than
    // pointing at a byte offset.
    assert_eq!(reference.cnc_version, written.cnc_version);
    assert_eq!(
        reference.to_driver_buffer_length,
        written.to_driver_buffer_length
    );
    assert_eq!(
        reference.to_clients_buffer_length,
        written.to_clients_buffer_length
    );
    assert_eq!(
        reference.counter_metadata_buffer_length,
        written.counter_metadata_buffer_length
    );
    assert_eq!(
        reference.counter_values_buffer_length,
        written.counter_values_buffer_length
    );
    assert_eq!(
        reference.error_log_buffer_length,
        written.error_log_buffer_length
    );
    assert_eq!(
        reference.client_liveness_timeout_ns,
        written.client_liveness_timeout_ns
    );
    assert_eq!(reference.file_page_size, written.file_page_size);

    // The two ranges a capture from another process cannot match are what this
    // process said they are, and nothing more is claimed about them.
    assert_eq!(1_700_000_000_000, written.start_timestamp_ms);
    assert_eq!(4242, written.pid);

    // Then the bytes themselves, so that a field this test does not know about
    // is still compared.
    assert_eq!(
        HEADER[..START_TIMESTAMP.start],
        file.block[..START_TIMESTAMP.start]
    );
    assert_eq!(HEADER[PID.end..52], file.block[PID.end..52]);
    assert_eq!(
        HEADER[52..128],
        file.block[52..128],
        "the rest of the metadata region is reserved and stays zero"
    );
    assert_eq!(
        [0u8; 76],
        file.block[52..128],
        "and that is what zero looks like"
    );
}

#[test]
fn a_written_file_is_the_length_and_shape_the_capture_was() {
    let (_dir, file) = created();

    assert_eq!(CAPTURED_FILE_LENGTH, file.length);

    let written = CncMetadata::decode(&file.block).expect("decode");
    let regions = RegionLayout::compute(&written, file.length).expect("the regions fit");

    // The offsets a real driver's file has, to the byte: 128 + the to-driver
    // region, then the five regions end to end.
    assert_eq!(128, regions.to_driver.start);
    assert_eq!(1_049_472, regions.to_driver.end);
    assert_eq!(1_049_472, regions.to_clients.start);
    assert_eq!(2_098_176, regions.to_clients.end);
    assert_eq!(2_098_176, regions.counters_metadata.start);
    assert_eq!(35_652_608, regions.counters_metadata.end);
    assert_eq!(35_652_608, regions.counters_values.start);
    assert_eq!(44_041_216, regions.counters_values.end);
    assert_eq!(44_041_216, regions.error_log.start);
    assert_eq!(
        48_235_520, regions.error_log.end,
        "and the end of the last region is the unaligned total"
    );
}
