//! The catalog, read by the reference's own `ArchiveTool`.
//!
//! The CI test (`crates/archive/tests/catalog.rs`) proves this build reads a
//! reference-written catalog correctly — but it reads it with **this** build's
//! arithmetic, and a wrong arithmetic that is consistently wrong would pass it.
//! What can only be asked with somebody else's code is the other direction: does
//! the reference's own reader walk what this build writes?
//!
//! `ArchiveTool` is that reader, and it has three commands worth running:
//! `count-entries` (the index) and `describe-all` — which prints every descriptor
//! it finds, the catalog's capacity, **and needs a mark file in the directory**
//! (`ArchiveTool.java:422-429` opens one and prints the mark information before
//! the catalog). So the fixture here is an archive directory with both files,
//! written by this build: the mark file from P2-2a and the catalog from P2-2b.
//!
//! There is no `capacity` **command**, though the plan assumed one: the method
//! exists (`ArchiveTool.capacity(File)`, `:390`) and the deprecation text for
//! `max-entries` points at it, but the argument parser has no branch for it — so
//! the capacity a caller can see is the line `describe-all` prints.
//!
//! The golden's own test compares this build against the reference's *recorded*
//! reading. The last test here compares the two readers **live** on the same
//! file, which is what catches a recorded reading taken from the wrong field.

use std::path::PathBuf;

use deepmsg_archive::catalog::{
    Catalog, DEFAULT_CAPACITY, FILENAME as CATALOG_FILENAME, MIN_CAPACITY, Recording,
};
use deepmsg_archive::mark_file::{
    ArchiveMarkFile, ERROR_BUFFER_LENGTH_DEFAULT, FILENAME as MARK_FILENAME, Header,
};
use deepmsg_tests::driver;
use deepmsg_tests::java::archive_tool;
use deepmsg_tests::temp::TempDir;

const NOW: i64 = 1_700_000_000_000;
const PAGE_SIZE: usize = 4096;
const RECORDINGS: i64 = 3;

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/catalog")
}

/// One recording, distinguishable from the one beside it by every field a
/// reader can see — the channels included, so a walk that lost its place among
/// them is a mismatch rather than a coincidence.
fn recording(index: i64) -> Recording {
    Recording {
        recording_id: 0,
        start_timestamp: NOW + index,
        stop_timestamp: NOW + index + 1000,
        start_position: index * 4096,
        stop_position: (index + 1) * 4096,
        initial_term_id: 7,
        segment_file_length: 64 * 1024 * 1024,
        term_buffer_length: 64 * 1024,
        mtu_length: 1408,
        session_id: 42,
        stream_id: 1001 + i32::try_from(index).expect("small"),
        stripped_channel: format!("aeron:udp?endpoint=localhost:{}", 9100 + index),
        original_channel: format!("aeron:udp?endpoint=localhost:{}|sparse=true", 9100 + index),
        source_identity: format!("aeron:udp?endpoint=localhost:{}", 8100 + index),
    }
}

/// The mark header an archive directory needs beside its catalog.
fn mark_header(aeron_directory: &str) -> Header<'_> {
    Header {
        start_timestamp: NOW,
        control_channel: Some("aeron:udp?endpoint=localhost:9010"),
        local_control_channel: "aeron:ipc",
        events_channel: None,
        aeron_directory,
        control_stream_id: 101,
        local_control_stream_id: 102,
        events_stream_id: 103,
        archive_id: 0x0A0B_0C0D_0E0F_1011,
    }
}

/// Write an archive directory this build owns: a mark file and a catalog with
/// `RECORDINGS` recordings in it.
fn our_archive_directory(dir: &TempDir) -> (Catalog, Vec<i64>) {
    let mark = ArchiveMarkFile::create(
        dir.path(),
        &mark_header(dir.path().to_str().expect("a path")),
        ERROR_BUFFER_LENGTH_DEFAULT,
        PAGE_SIZE,
        i64::from(std::process::id()),
    )
    .expect("a mark file");
    mark.signal_ready(NOW).expect("signalled");

    let mut catalog = Catalog::create(dir.path(), DEFAULT_CAPACITY, 0).expect("a catalog");
    let mut ids = Vec::new();

    for index in 0..RECORDINGS {
        ids.push(catalog.add_recording(&recording(index)).expect("added"));
    }

    (catalog, ids)
}

/// The three commands the reference's own tool can answer about a catalog, on an
/// archive directory **this build wrote**.
///
/// `describe-all` is the one with weight: it walks the catalog with the
/// reference's own arithmetic — record header, length, step — and prints each
/// descriptor it lands on, so a record written at an offset the reference would
/// not walk to is a record it never mentions. The other two are one number each.
#[test]
fn the_reference_reads_the_catalog_this_build_writes() {
    let Some(jar) = driver::locate_aeron_all() else {
        driver::announce_tool_skip("ArchiveTool");
        return;
    };

    let dir = TempDir::new("catalog-interop");
    let (catalog, ids) = our_archive_directory(&dir);
    let capacity = catalog.capacity();

    assert_eq!(RECORDINGS as usize, catalog.count_entries());
    assert!(
        dir.path().join(MARK_FILENAME).exists() && dir.path().join(CATALOG_FILENAME).exists(),
        "an archive directory is both files"
    );

    let counted = archive_tool(&jar, dir.path(), &["count-entries"]);
    assert_eq!(
        RECORDINGS.to_string(),
        counted.trim(),
        "the reference's own count of the entries this build wrote"
    );

    // The descriptors themselves, in the reference's reading — and the capacity,
    // which has no command of its own and is this line.
    let described = archive_tool(&jar, dir.path(), &["describe-all"]);

    assert!(
        described.contains(&format!("Catalog capacity in bytes: {capacity}")),
        "the tool reports the capacity it read:\n{described}"
    );

    for (index, id) in ids.iter().enumerate() {
        let channel = format!("aeron:udp?endpoint=localhost:{}", 9100 + index);
        let source = format!("aeron:udp?endpoint=localhost:{}", 8100 + index);

        assert!(
            described.contains(&format!("recordingId={id}")),
            "the reference did not walk to recording {id}:\n{described}"
        );
        assert!(
            described.contains(&channel),
            "nor read its channel {channel}:\n{described}"
        );
        assert!(
            described.contains(&source),
            "nor its source identity {source}:\n{described}"
        );
    }
}

/// A catalog that **grew** under the writer, read back by the reference.
///
/// The file this build hands over is not the one it started: `Catalog::create`
/// at the minimum capacity, then enough recordings that the mapping is extended
/// several times. So the reference's reader is walking a file whose size changed
/// while it was being written — which is the arrangement the growth path exists
/// for and the one an in-memory test cannot ask about, because only another
/// process's mapping can disagree with ours about what the file is.
#[test]
fn the_reference_reads_a_catalog_that_grew() {
    let Some(jar) = driver::locate_aeron_all() else {
        driver::announce_tool_skip("ArchiveTool");
        return;
    };

    let dir = TempDir::new("catalog-grown-interop");
    let mark = ArchiveMarkFile::create(
        dir.path(),
        &mark_header(dir.path().to_str().expect("a path")),
        ERROR_BUFFER_LENGTH_DEFAULT,
        PAGE_SIZE,
        i64::from(std::process::id()),
    )
    .expect("a mark file");
    mark.signal_ready(NOW).expect("signalled");

    let mut catalog = Catalog::create(dir.path(), MIN_CAPACITY, 0).expect("a catalog");
    let mut ids = Vec::new();

    // Eight recordings into a catalog that is 32 bytes to begin with: the file
    // is extended, by half again each time, before most of these are written.
    for index in 0..8 {
        ids.push(
            catalog
                .add_recording(&recording(index))
                .expect("the catalog grows for it"),
        );
    }

    let capacity = catalog.capacity();

    assert!(
        capacity > MIN_CAPACITY,
        "the fixture is only interesting if the file grew: {capacity}"
    );

    let described = archive_tool(&jar, dir.path(), &["describe-all"]);

    assert!(
        described.contains(&format!("Catalog capacity in bytes: {capacity}")),
        "the reference read the capacity the file ended at:\n{described}"
    );

    for id in &ids {
        assert!(
            described.contains(&format!("recordingId={id}")),
            "the reference did not walk to recording {id} through the grown file:\n{described}"
        );
    }

    assert_eq!(
        ids.len().to_string(),
        archive_tool(&jar, dir.path(), &["count-entries"]).trim()
    );
}

/// Both readers, one file: the golden the previous commit brought in, counted by
/// this build and by the reference's own tool.
///
/// The golden's own test checks this build against the reference's *recorded*
/// reading — which a generator could have taken from the wrong field and got
/// wrong consistently. This asks the reference live, on the same bytes.
#[test]
fn both_readers_agree_about_the_golden() {
    let Some(jar) = driver::locate_aeron_all() else {
        driver::announce_tool_skip("ArchiveTool");
        return;
    };

    let golden = fixtures();
    let catalog = Catalog::open(&golden).expect("the golden opens");

    let counted = archive_tool(&jar, &golden, &["count-entries"]);

    assert_eq!(
        catalog.count_entries().to_string(),
        counted.trim(),
        "the two readings of the same file's entry count"
    );
}
