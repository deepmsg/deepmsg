//! Segments and recovery, read by the reference's own tools.
//!
//! Two questions, and both are ones only somebody else's code can answer.
//!
//! **Are the checksums this build writes the checksums the reference verifies?**
//! `ArchiveTool <dir> checksum <className>` walks a recording's segments and
//! recomputes each frame's checksum from what is stored in the frame. A build
//! whose name-to-algorithm mapping were wrong, or whose checksum covered the
//! wrong bytes, would produce a recording the reference reports as corrupt.
//!
//! **Does the reference recover the same stop position this build does?** A
//! recording that never stopped is repaired by reading its segments, and that
//! repair happens inside the reference's `Catalog` constructor — so the answer
//! comes from opening the catalog with `RecoverCatalog`, a probe beside the
//! golden's generator. The file has to be one this test wrote, which is why it
//! is a probe and not a fixture.

use std::path::{Path, PathBuf};

use deepmsg_archive::catalog::{Catalog, DEFAULT_CAPACITY, Recording};
use deepmsg_archive::checksum::Checksum;
use deepmsg_archive::mark_file::{
    ArchiveMarkFile, ERROR_BUFFER_LENGTH_DEFAULT, FILENAME as MARK_FILENAME, Header,
};
use deepmsg_archive::segment::{
    SegmentSpec, SegmentSummary, SegmentWriter, compute_stop_position, segment_file_name,
};
use deepmsg_core::logbuffer::descriptor::FRAME_ALIGNMENT;
use deepmsg_core::logbuffer::frame::{DATA_HEADER_LENGTH, FRAME_LENGTH_OFFSET, TYPE_OFFSET};
use deepmsg_core::logbuffer::position::align_up;
use deepmsg_tests::driver;
use deepmsg_tests::java;
use deepmsg_tests::temp::TempDir;

const NOW: i64 = 1_700_000_000_000;
const PAGE_SIZE: usize = 4096;
const TERM: i32 = 64 * 1024;
const SEGMENT: usize = 4 * TERM as usize;
const RECORDING_ID: i64 = 3;
const STREAM_ID: i32 = 1001;
const PAYLOADS: [&[u8]; 4] = [b"first", b"second", b"third", b"fourth"];

/// One frame, as a publisher's term holds it: a data header and the payload,
/// aligned.
fn frame(term_offset: i32, payload: &[u8]) -> Vec<u8> {
    let length = DATA_HEADER_LENGTH + payload.len();
    let aligned = align_up(i32::try_from(length).expect("small"), FRAME_ALIGNMENT) as usize;
    let mut bytes = vec![0_u8; aligned];

    bytes[FRAME_LENGTH_OFFSET..FRAME_LENGTH_OFFSET + 4]
        .copy_from_slice(&i32::try_from(length).expect("small").to_le_bytes());
    bytes[TYPE_OFFSET..TYPE_OFFSET + 2].copy_from_slice(&1_i16.to_le_bytes());
    bytes[8..12].copy_from_slice(&term_offset.to_le_bytes());
    bytes[12..16].copy_from_slice(&7_i32.to_le_bytes());
    bytes[16..20].copy_from_slice(&STREAM_ID.to_le_bytes());
    bytes[20..24].copy_from_slice(&7_i32.to_le_bytes());
    bytes[DATA_HEADER_LENGTH..length].copy_from_slice(payload);

    bytes
}

/// An archive directory this build owns: a mark file, a catalog, and a recording
/// whose frames are written into one segment.
///
/// The catalog's record is left **unfinished** — `stop_position` is null — which
/// is what a recording being written when its archive died looks like, and what
/// the recovery test is about. The checksum test wants the same directory, so
/// both get it.
fn a_died_mid_write_recording(dir: &TempDir, checksum: Option<Checksum>) -> (Catalog, i64, i64) {
    let mark = ArchiveMarkFile::create(
        dir.path(),
        &Header {
            start_timestamp: NOW,
            control_channel: Some("aeron:udp?endpoint=localhost:9010"),
            local_control_channel: "aeron:ipc",
            events_channel: None,
            aeron_directory: dir.path().to_str().expect("a path"),
            control_stream_id: 101,
            local_control_stream_id: 102,
            events_stream_id: 103,
            archive_id: 7,
        },
        ERROR_BUFFER_LENGTH_DEFAULT,
        PAGE_SIZE,
        i64::from(std::process::id()),
    )
    .expect("a mark file");
    mark.signal_ready(NOW).expect("signalled");

    let mut writer = SegmentWriter::create(
        dir.path(),
        SegmentSpec {
            recording_id: RECORDING_ID,
            start_position: 0,
            join_position: 0,
            term_buffer_length: TERM,
            segment_length: SEGMENT,
        },
        1,
        checksum,
    )
    .expect("a segment writer");

    let mut term_offset = 0;
    for payload in PAYLOADS {
        let block = frame(term_offset, payload);
        term_offset += i32::try_from(block.len()).expect("small");
        writer.write_block(&block).expect("written");
    }

    let written = writer.offset() as i64;

    let mut catalog =
        Catalog::create(dir.path(), DEFAULT_CAPACITY, RECORDING_ID).expect("a catalog");
    let id = catalog
        .add_recording(&Recording {
            recording_id: 0,
            start_timestamp: NOW,
            stop_timestamp: -1,
            start_position: 0,
            stop_position: -1,
            initial_term_id: 7,
            segment_file_length: SEGMENT as i32,
            term_buffer_length: TERM,
            mtu_length: 1408,
            session_id: 42,
            stream_id: STREAM_ID,
            stripped_channel: "aeron:udp?endpoint=localhost:9100".to_string(),
            original_channel: "aeron:udp?endpoint=localhost:9100|sparse=true".to_string(),
            source_identity: "aeron:udp?endpoint=localhost:8100".to_string(),
        })
        .expect("added");

    (catalog, id, written)
}

fn probe_directory(dir: &TempDir) -> PathBuf {
    let classes = dir.path().join("probe-classes");
    std::fs::create_dir_all(&classes).expect("a directory for the probe");

    classes
}

fn fixture_probe() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/catalog/RecoverCatalog.java")
}

/// The reference's own `ArchiveTool checksum` **stamping** a recording this
/// build wrote, with each algorithm it can be asked about — and this build
/// checking what it stamped.
///
/// `checksum` is not a verifier: it recomputes each frame's checksum and writes
/// it into that frame's session-id field (`ArchiveTool.java:1610-1614`), and
/// prints nothing when it worked. So the direction has to be this one, which is
/// the better direction anyway: the reference writes the checksums and this build
/// compares them with its own computation over the same bytes. Three things have
/// to agree for that to hold — which algorithm the name selects, what the
/// checksum covers, and where it lands — and the reference is the one that
/// decided all three.
#[test]
fn the_reference_stamps_the_checksums_this_build_reads_back() {
    let Some(jar) = driver::locate_aeron_all() else {
        driver::announce_tool_skip("ArchiveTool");
        return;
    };

    for (checksum, name) in [(Checksum::Crc32, "CRC-32"), (Checksum::Crc32c, "CRC-32C")] {
        let dir = TempDir::new("segment-checksum");
        let (_catalog, _id, _written) = a_died_mid_write_recording(&dir, None);

        let said = java::archive_tool(&jar, dir.path(), &["checksum", name, "-a"]);

        assert!(
            !said.contains("ERR"),
            "{name}: the reference refused to stamp what this build wrote:\n{said}"
        );

        // Every frame, as the reference left it: the session-id field holds the
        // checksum of the payload, over the aligned length — which is what this
        // build computes for the same bytes.
        let bytes = std::fs::read(dir.path().join(segment_file_name(RECORDING_ID, 0)))
            .expect("the segment");

        let mut offset = 0;
        let mut checked = 0;

        for payload in PAYLOADS {
            let length = DATA_HEADER_LENGTH + payload.len();
            let aligned = align_up(i32::try_from(length).expect("small"), FRAME_ALIGNMENT) as usize;

            let stored =
                i32::from_le_bytes(bytes[offset + 12..offset + 16].try_into().expect("four"));

            assert_eq!(
                checksum.compute(&bytes[offset + DATA_HEADER_LENGTH..offset + aligned]),
                stored,
                "{name}: frame {checked} does not carry the checksum this build computes"
            );

            offset += aligned;
            checked += 1;
        }

        assert_eq!(PAYLOADS.len(), checked);
    }
}

/// Both readers, one directory: the stop position this build recovers from a
/// recording that never stopped, and the one the reference recovers by opening
/// the same catalog.
///
/// This is the acceptance P2-2b deferred and P2-2c took on. The reference does
/// the repair inside its constructor, so its answer cannot be read out of the
/// file without writing — which is why the probe exists, and why it has to be
/// compiled here rather than recorded as a golden: the file it reads is one this
/// test just wrote.
#[test]
fn both_readers_recover_the_same_stop_position() {
    let Some(jar) = driver::locate_aeron_all() else {
        driver::announce_tool_skip("ArchiveTool");
        return;
    };

    let dir = TempDir::new("segment-recovery");
    let (catalog, id, written) = a_died_mid_write_recording(&dir, None);
    let classes = probe_directory(&dir);

    if java::compile_probe(&fixture_probe(), &classes).is_none() {
        driver::announce_tool_skip("javac");
        return;
    }

    // What this build recovers, computed rather than written: the probe repairs
    // the file, so asking afterwards would be asking a different question.
    let recording = catalog.recording(id).expect("the recording");
    let summary = SegmentSummary::from(&recording);
    let files = deepmsg_archive::segment::segment_files(dir.path(), id);
    let highest = files.last().map(|(_, path)| {
        path.file_name()
            .expect("a name")
            .to_string_lossy()
            .into_owned()
    });

    let ours =
        compute_stop_position(dir.path(), &summary, highest.as_deref(), None).expect("recovered");
    assert_eq!(
        written, ours,
        "the frames this test wrote, one past the last"
    );

    // And the reference's, from opening the catalog — which repairs it.
    let said = java::run_probe(
        &jar,
        &classes,
        "io.aeron.archive.RecoverCatalog",
        &[dir.path().to_str().expect("a path")],
    );
    let quoted = said
        .lines()
        .find_map(|line| {
            let mut parts = line.split_whitespace();

            (parts.next() == Some(&id.to_string())).then(|| parts.next().unwrap_or("").to_string())
        })
        .unwrap_or_else(|| panic!("the probe printed nothing for recording {id}:\n{said}"));

    assert_eq!(
        ours.to_string(),
        quoted,
        "the reference recovered a different stop position:\n{said}"
    );

    // And the file now holds what the reference wrote, which is the same thing
    // again from the file's side.
    let reopened = Catalog::open(dir.path()).expect("open");
    assert_eq!(
        ours,
        reopened.recording(id).expect("the record").stop_position
    );
    assert_eq!(
        NOW,
        reopened.recording(id).expect("the record").stop_timestamp,
        "with the time of the repair, which is the probe's fixed clock"
    );
}

/// The mark file the probe's directory needs is one this build wrote, and the
/// probe's own catalog open does not need it — but `ArchiveTool` does, so the
/// directory has one either way. This asserts the fixture is the shape the other
/// tests assume, rather than leaving it to be discovered by a command that
/// fails for a different reason.
#[test]
fn the_fixture_directory_is_an_archive_directory() {
    let dir = TempDir::new("segment-fixture");
    a_died_mid_write_recording(&dir, None);

    let mark = dir.path().join(MARK_FILENAME);

    assert!(mark.exists(), "a mark file");
    assert!(
        Path::new(&dir.path().join(segment_file_name(RECORDING_ID, 0))).exists(),
        "and the segment the record describes"
    );
}
