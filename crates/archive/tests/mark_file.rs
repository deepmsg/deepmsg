//! The mark file the reference wrote, read back by this build.
//!
//! `tests/fixtures/mark-file/archive-mark.dat` is a real archive mark file,
//! produced by the reference's own `ArchiveMarkFile` (see the generator beside
//! it), and `mark-file.tsv` is the **reference's own reading** of every field in
//! it — taken through the same decoder a live archive would use, not from what
//! the generator intended to write. Both sides of the comparison therefore come
//! from the reference, which is what makes a disagreement here this
//! repository's defect: `fixtures/sbe/README.md` makes the same argument for
//! the SBE goldens, and this is the file-level half of the same idea.
//!
//! What only a whole file can pin, and the reason this exists beside the
//! header golden next door:
//!
//! * **where the error buffer begins** — the SBE field says 8192, and this
//!   checks that a reader which trusts that number finds the errors the
//!   reference wrote through its own `DistinctErrorLog`;
//! * **how long the file is** — `align(8192 + errorBufferLength, page_size)`,
//!   checked against the file the reference actually produced rather than
//!   against our own arithmetic;
//! * that the two fields the **generic** half owns — `version` and
//!   `activityTimestamp` — are where this build thinks they are, in a file
//!   written by somebody else.
//!
//! Everything here runs in CI: the fixtures are committed, and nothing in this
//! test needs a JDK or a reference checkout.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use deepmsg_archive::mark::NULL_VALUE;
use deepmsg_archive::mark_file::{
    self, ArchiveMarkFile, ERROR_BUFFER_LENGTH_DEFAULT, HEADER_LENGTH, LIVENESS_TIMEOUT_MS,
};

/// The reference's reading of the golden, by field name.
fn readings() -> BTreeMap<String, String> {
    let path = fixtures().join("mark-file.tsv");
    let table =
        fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));

    table
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let (name, value) = line.split_once('\t').unwrap_or_else(|| panic!("{line}"));
            (name.to_owned(), value.to_owned())
        })
        .collect()
}

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/mark-file")
}

fn number(readings: &BTreeMap<String, String>, name: &str) -> i64 {
    readings
        .get(name)
        .unwrap_or_else(|| panic!("the table has no {name}"))
        .parse()
        .unwrap_or_else(|error| panic!("{name} is not a number: {error}"))
}

#[test]
fn every_field_the_reference_read_is_a_field_this_build_reads() {
    let readings = readings();
    let mark = ArchiveMarkFile::open(&fixtures()).expect("the golden opens");

    assert_eq!(number(&readings, "pid"), mark.pid().expect("a pid"));
    assert_eq!(
        number(&readings, "version") as i32,
        mark.version().expect("a version")
    );
    assert_eq!(
        number(&readings, "activityTimestamp"),
        mark.activity_timestamp().expect("a timestamp")
    );
    assert_eq!(
        number(&readings, "startTimestamp"),
        mark.start_timestamp().expect("a start")
    );
    assert_eq!(
        number(&readings, "archiveId"),
        mark.archive_id().expect("an archive id")
    );

    assert_eq!(
        number(&readings, "controlStreamId") as i32,
        mark.control_stream_id().expect("a stream id")
    );
    assert_eq!(
        number(&readings, "localControlStreamId") as i32,
        mark.local_control_stream_id().expect("a stream id")
    );
    assert_eq!(
        number(&readings, "eventsStreamId") as i32,
        mark.events_stream_id().expect("a stream id")
    );

    // The two numbers a reader cannot compute and has to be told: where the
    // error buffer starts, and how long it is.
    assert_eq!(
        number(&readings, "headerLength") as i32,
        mark.header_length().expect("a header length")
    );
    assert_eq!(
        number(&readings, "errorBufferLength"),
        i64::try_from(mark.error_buffer_length()).expect("small")
    );

    let channels = mark.channels().expect("the four strings");
    assert_eq!(readings["controlChannel"], channels.control);
    assert_eq!(readings["localControlChannel"], channels.local_control);
    assert_eq!(readings["eventsChannel"], channels.events);
    assert_eq!(readings["aeronDirectory"], channels.aeron_directory);
}

/// The length arithmetic, against the file the reference actually produced.
///
/// Ours is `align(HEADER_LENGTH + error_buffer_length, page_size)`; the golden
/// is that number as the reference computed it, measured as the file's size.
/// Checking one against the other is what makes the arithmetic a contract
/// rather than a claim — and the page size is not in the file, so a reader that
/// disagreed about it would read an error buffer that is not there.
#[test]
fn the_file_is_as_long_as_the_two_lengths_say() {
    let readings = readings();
    let path = fixtures().join("archive-mark.dat");
    let length = fs::metadata(&path).expect("the golden").len();

    assert_eq!(
        u64::try_from(number(&readings, "fileLength")).expect("positive"),
        length,
        "the reading and the file agree"
    );

    let mark = ArchiveMarkFile::open(&fixtures()).expect("the golden opens");

    assert_eq!(
        usize::try_from(length).expect("fits"),
        mark.length(),
        "and this build maps what the reference wrote"
    );
    assert_eq!(
        HEADER_LENGTH + ERROR_BUFFER_LENGTH_DEFAULT,
        mark.length(),
        "which for this golden is the default error buffer, page-aligned already"
    );
}

/// The error buffer is where the header says it is, and holds what the
/// reference's own error log wrote into it.
///
/// Read as **bytes**: this build has its own reader for the distinct error log
/// (`deepmsg_cnc::error_log`), and what is asserted here is the window's
/// placement — that the offset a reader computes from `headerLength` is the
/// offset the reference wrote at. Getting that wrong gives a buffer of zeroes,
/// which is a reader that reports an archive with no errors rather than one
/// that fails.
#[test]
fn the_error_buffer_holds_what_the_reference_wrote() {
    let mark = ArchiveMarkFile::open(&fixtures()).expect("the golden opens");
    let buffer = mark.error_buffer().expect("the error buffer");

    let mut bytes = vec![0_u8; mark.error_buffer_length()];
    buffer
        .copy_out(0, &mut bytes)
        .expect("the window is readable");

    let text = String::from_utf8_lossy(&bytes);
    for expected in ["the first distinct error", "the second distinct error"] {
        assert!(
            text.contains(expected),
            "the reference's own error log wrote {expected:?} into this buffer"
        );
    }

    // And the bytes before the header are not the buffer: the header itself is
    // not where the errors went.
    let mut header = vec![0_u8; HEADER_LENGTH];
    mark.region()
        .expect("the whole file")
        .copy_out(0, &mut header)
        .expect("readable");
    assert!(
        !String::from_utf8_lossy(&header).contains("the first distinct error"),
        "the errors are behind the header, not in it"
    );
}

/// Liveness, on a file written by somebody else: the timestamp the reference
/// stamped is one this build judges the same way.
#[test]
fn the_golden_is_alive_for_its_own_timeout() {
    let readings = readings();
    let mark = ArchiveMarkFile::open(&fixtures()).expect("the golden opens");
    let written_at = number(&readings, "activityTimestamp");

    assert!(
        mark.is_active(written_at),
        "the moment it was stamped, it is alive"
    );
    assert!(
        mark.is_active(written_at + LIVENESS_TIMEOUT_MS),
        "and for the whole timeout"
    );
    assert!(
        !mark.is_active(written_at + LIVENESS_TIMEOUT_MS + 1),
        "and not a millisecond past it"
    );

    assert_eq!(
        Some(mark_file::SEMANTIC_VERSION),
        mark.version(),
        "the version it was signalled with is the one this build writes"
    );
    assert_eq!(
        3,
        mark_file::major_of(mark.version().expect("a version")),
        "which is major 3, the only major this build accepts"
    );
    assert_ne!(Some(NULL_VALUE), mark.activity_timestamp());
}
