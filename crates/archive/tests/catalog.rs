//! The catalog the reference wrote, read back by this build.
//!
//! `tests/fixtures/catalog/archive.catalog` is a real archive catalog, produced
//! by the reference's own `Catalog` (see the generator beside it), and
//! `catalog.tsv` is the **reference's own reading** of it — every header field,
//! every record's location and length, and every descriptor field, taken through
//! the reference's decoders rather than from what the generator intended to
//! write. Both sides of the comparison therefore come from the reference, which
//! is what makes a disagreement here this repository's defect.
//!
//! What only a whole file can pin, and what the SBE goldens next door cannot:
//!
//! * **where the first record is** — offset 32, the header's own block length,
//!   and not the deprecated `DEFAULT_ALIGNMENT` of 1024 that an earlier plan
//!   assumed;
//! * **how a record's length relates to the frame the alignment produces** —
//!   `length` is the descriptor's bytes and the frame is that plus the header,
//!   rounded up to the alignment, which is why the second record starts a whole
//!   frame after the first rather than a descriptor later;
//! * that the records carry **no SBE message header**: the record header is the
//!   framing, so the reference's `location`s are what a reader that expects one
//!   would get wrong.
//!
//! Everything here runs in CI: the fixtures are committed, and nothing needs a
//! JDK or a reference checkout.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use deepmsg_archive::catalog::{Catalog, DESCRIPTOR_HEADER_LENGTH, FILENAME, HEADER_LENGTH};
use deepmsg_codec::archive::recording_state::RecordingState;

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/catalog")
}

/// The reference's reading of the golden: `(key, field) -> value`, where the key
/// is `header`, `file`, or `recordN`.
fn readings() -> BTreeMap<(String, String), String> {
    let path = fixtures().join("catalog.tsv");
    let table =
        fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));

    table
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let mut parts = line.split('\t');
            let key = parts.next().expect("a key").to_owned();
            let field = parts.next().expect("a field").to_owned();
            let value = parts.next().unwrap_or_default().to_owned();

            ((key, field), value)
        })
        .collect()
}

fn number(readings: &BTreeMap<(String, String), String>, key: &str, field: &str) -> i64 {
    readings
        .get(&(key.to_owned(), field.to_owned()))
        .unwrap_or_else(|| panic!("the table has no {key}.{field}"))
        .parse()
        .unwrap_or_else(|error| panic!("{key}.{field} is not a number: {error}"))
}

fn text(readings: &BTreeMap<(String, String), String>, key: &str, field: &str) -> String {
    readings
        .get(&(key.to_owned(), field.to_owned()))
        .unwrap_or_else(|| panic!("the table has no {key}.{field}"))
        .clone()
}

/// The header, read out of a file the reference wrote.
#[test]
fn the_headers_fields_are_what_the_reference_read() {
    let readings = readings();
    let catalog = Catalog::open(&fixtures()).expect("the golden opens");

    assert_eq!(
        number(&readings, "header", "nextRecordingId"),
        catalog.next_recording_id(),
        "the id the next recording will be given"
    );
    assert_eq!(
        number(&readings, "header", "alignment") as usize,
        catalog.alignment(),
        "the alignment this catalog's records were laid out on"
    );
    assert_eq!(
        HEADER_LENGTH,
        usize::try_from(number(&readings, "header", "length")).expect("positive"),
        "the header's own length, which is where the first record goes"
    );
    assert_eq!(
        u64::try_from(number(&readings, "file", "length")).expect("positive"),
        fs::metadata(fixtures().join(FILENAME))
            .expect("the golden")
            .len(),
        "and the reading and the file agree about how long it is"
    );
    assert_eq!(
        usize::try_from(number(&readings, "file", "length")).expect("positive"),
        catalog.capacity()
    );
}

/// Every record, at the offset the reference walked to, with every field the
/// reference read.
///
/// The offsets are the half with weight: this build walks the file with its own
/// arithmetic — a record header, a length, the alignment — and if any of that
/// disagreed with the reference's the walk would arrive at a byte that is not a
/// record. The three records have deliberately different frame lengths, so a
/// walk that stepped by one record's length for all of them would land in the
/// middle of one of these.
#[test]
fn every_record_is_where_the_reference_walked_to_it() {
    let readings = readings();
    let catalog = Catalog::open(&fixtures()).expect("the golden opens");
    let records = usize::try_from(number(&readings, "file", "records")).expect("positive");

    assert_eq!(records, catalog.count_entries());
    assert_eq!(records, catalog.recording_ids().count());

    let mut previous_end = HEADER_LENGTH;

    for index in 0..records {
        let key = format!("record{index}");

        let recording_id = number(&readings, &key, "recordingId");
        let location = usize::try_from(number(&readings, &key, "location")).expect("positive");
        let length = usize::try_from(number(&readings, &key, "length")).expect("positive");

        assert_eq!(
            Some(location),
            catalog.recording_offset(recording_id),
            "{key}: the offset this build walks to"
        );
        assert_eq!(
            location, previous_end,
            "{key}: the records follow one another with nothing between them"
        );
        previous_end = location + DESCRIPTOR_HEADER_LENGTH + length;

        assert_eq!(
            RecordingState::VALID,
            catalog.state_at(location).expect("a state"),
            "{key}"
        );

        let recording = catalog.recording(recording_id).expect("a recording");

        assert_eq!(recording_id, recording.recording_id);
        assert_eq!(
            number(&readings, &key, "startTimestamp"),
            recording.start_timestamp
        );
        assert_eq!(
            number(&readings, &key, "stopTimestamp"),
            recording.stop_timestamp
        );
        assert_eq!(
            number(&readings, &key, "startPosition"),
            recording.start_position
        );
        assert_eq!(
            number(&readings, &key, "stopPosition"),
            recording.stop_position
        );
        assert_eq!(
            number(&readings, &key, "initialTermId") as i32,
            recording.initial_term_id
        );
        assert_eq!(
            number(&readings, &key, "segmentFileLength") as i32,
            recording.segment_file_length
        );
        assert_eq!(
            number(&readings, &key, "termBufferLength") as i32,
            recording.term_buffer_length
        );
        assert_eq!(
            number(&readings, &key, "mtuLength") as i32,
            recording.mtu_length
        );
        assert_eq!(
            number(&readings, &key, "sessionId") as i32,
            recording.session_id
        );
        assert_eq!(
            number(&readings, &key, "streamId") as i32,
            recording.stream_id
        );

        // The three strings, which are a sequence in the body rather than three
        // lookups: a reader that asked for the third first would read whatever
        // the first left behind.
        assert_eq!(
            text(&readings, &key, "strippedChannel"),
            recording.stripped_channel
        );
        assert_eq!(
            text(&readings, &key, "originalChannel"),
            recording.original_channel
        );
        assert_eq!(
            text(&readings, &key, "sourceIdentity"),
            recording.source_identity
        );
    }

    assert_eq!(
        records,
        catalog.recordings().expect("every recording").len(),
        "and reading them all agrees with reading them one at a time"
    );
}

/// The alignment is a **field**, not a constant: a catalog whose header says
/// 1024 is laid out differently from one that says 64, and a reader that assumed
/// either would read the wrong bytes of somebody's file.
///
/// 1024 is `Catalog.DEFAULT_ALIGNMENT`, what every catalog written before
/// `CACHE_LINE_LENGTH` was, and no Aeron this repository can build writes one
/// today — so the fixture for it is the golden with that one field patched. The
/// walk does not use the alignment (a reader steps by each record's own length),
/// which is why patching it leaves the records where they are and the test can
/// tell "read the field" from "used the constant".
#[test]
fn the_alignment_come_from_the_header_not_from_a_constant() {
    const ALIGNMENT_OFFSET: usize = 16;
    const LEGACY_ALIGNMENT: i32 = 1024;

    let dir = TempDir::new();
    let path = dir.path().join(FILENAME);
    fs::copy(fixtures().join(FILENAME), &path).expect("a copy of the golden");

    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("the copy");
        use std::io::{Seek, SeekFrom, Write};

        file.seek(SeekFrom::Start(ALIGNMENT_OFFSET as u64))
            .expect("seek");
        file.write_all(&LEGACY_ALIGNMENT.to_le_bytes())
            .expect("patched");
    }

    let catalog = Catalog::open(dir.path()).expect("the patched copy opens");

    assert_eq!(
        LEGACY_ALIGNMENT as usize,
        catalog.alignment(),
        "the header is the authority on how its own records are laid out"
    );
    assert_eq!(
        3,
        catalog.count_entries(),
        "and the records are where they were: the walk uses lengths, not the alignment"
    );
}

/// A directory that removes itself, as in `crate::mark`'s tests.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("deepmsg-catalog-{}-{n}", std::process::id()));
        fs::create_dir_all(&path).expect("a temp directory");

        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The alignment the reference laid its records out on, checked against the
/// arithmetic rather than against the number: every record's offset is a whole
/// frame from the one before it, and a whole number of alignments from the
/// start.
#[test]
fn the_records_are_on_the_alignment_the_header_names() {
    let readings = readings();
    let catalog = Catalog::open(&fixtures()).expect("the golden opens");
    let alignment = catalog.alignment();

    assert_ne!(
        0, alignment,
        "an alignment of zero would make every assertion below vacuous"
    );

    for index in 0..catalog.count_entries() {
        let key = format!("record{index}");
        let location = usize::try_from(number(&readings, &key, "location")).expect("positive");
        let length = usize::try_from(number(&readings, &key, "length")).expect("positive");
        let frame = DESCRIPTOR_HEADER_LENGTH + length;

        assert_eq!(
            0,
            frame % alignment,
            "{key}: the frame is a whole alignment"
        );
        assert_eq!(
            HEADER_LENGTH,
            location % alignment,
            "{key}: and so the record after the header starts where the header left off, \
             which is why the first record is at 32 and not at 1024"
        );
    }
}
