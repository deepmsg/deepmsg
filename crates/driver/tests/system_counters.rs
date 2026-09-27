//! Golden: the counters this driver publishes, against a real driver's.
//!
//! `tests/fixtures/counters-metadata.bin` is the first forty-six counter
//! metadata records of a default-configured `aeronmd` 1.53.2 — the catalogue
//! `AeronStat` prints. This test runs a `deepmsg` conductor over a fresh CnC
//! file and compares what it published, record by record.
//!
//! Three things are masked, and each for a reason that is in the fixture's
//! README: the counter *values* (a byte count and a cycle time are facts about
//! the run), the two labels that name a build, and the runtime suffixes a
//! driver appends from its own configuration. The mask for the last two is the
//! same one: compare each label up to its first colon. Everything before that
//! colon is the contract; everything after it is what the driver made of its
//! own config.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use deepmsg_cnc::layout;
use deepmsg_cnc::{CncFile, CncIdentity, CncLayout, CounterDescriptor};
use deepmsg_core::clock;
use deepmsg_driver::conductor::Conductor;
use deepmsg_driver::config::DriverConfig;

/// The forty-six records a real driver wrote, verbatim.
const RECORDS: &[u8] = include_bytes!("../../../tests/fixtures/counters-metadata.bin");

/// How many system counters the reference publishes
/// (`AERON_SYSTEM_COUNTER_DUMMY_LAST`, `aeron_system_counters.h:73`).
const COUNT: usize = 46;

/// The capture's stride is the layout's stride — if it were not, the file
/// would not be a counter region at all.
const RECORD: usize = layout::COUNTER_METADATA_LENGTH;

/// A directory of our own in the system temp directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("deepmsg-counters-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One record of the capture, decoded with the layout's own offsets.
struct Reference {
    state: i32,
    type_id: i32,
    free_for_reuse_deadline_ms: i64,
    key: Vec<u8>,
    label: String,
}

fn reference(index: usize) -> Reference {
    let base = index * RECORD;
    let i32_at = |offset: usize| {
        i32::from_le_bytes(
            RECORDS[base + offset..base + offset + 4]
                .try_into()
                .expect("four bytes"),
        )
    };
    let label_length = usize::try_from(i32_at(layout::COUNTER_LABEL_LENGTH_OFFSET))
        .expect("a non-negative length");

    Reference {
        state: i32_at(layout::COUNTER_STATE_OFFSET),
        type_id: i32_at(layout::COUNTER_TYPE_ID_OFFSET),
        free_for_reuse_deadline_ms: i64::from_le_bytes(
            RECORDS[base + layout::COUNTER_FREE_FOR_REUSE_DEADLINE_OFFSET
                ..base + layout::COUNTER_FREE_FOR_REUSE_DEADLINE_OFFSET + 8]
                .try_into()
                .expect("eight bytes"),
        ),
        key: RECORDS[base + layout::COUNTER_KEY_OFFSET
            ..base + layout::COUNTER_KEY_OFFSET + layout::COUNTER_KEY_LENGTH]
            .to_vec(),
        label: String::from_utf8_lossy(
            &RECORDS[base + layout::COUNTER_LABEL_OFFSET
                ..base + layout::COUNTER_LABEL_OFFSET + label_length],
        )
        .into_owned(),
    }
}

/// The contract part of a label: everything before its first colon.
fn prefix(label: &str) -> &str {
    label.split(':').next().unwrap_or(label)
}

/// Run a conductor over a fresh CnC file, and read its counters back from a
/// second mapping of the same file — the way a client sees them.
fn published_counters() -> (
    TempDir,
    Vec<CounterDescriptor>,
    Vec<[u8; layout::COUNTER_KEY_LENGTH]>,
) {
    let temp = TempDir::new();
    let cnc = CncFile::create(
        &temp.0,
        &CncLayout::default(),
        &CncIdentity {
            liveness_timeout_ns: deepmsg_cnc::CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT,
            start_timestamp_ms: clock::epoch_millis(),
            pid: i64::from(std::process::id()),
        },
    )
    .expect("create the CnC file");

    let conductor = Conductor::new(
        cnc,
        &DriverConfig {
            aeron_dir: temp.0.clone(),
            ..DriverConfig::default()
        },
    )
    .expect("the conductor takes the file over");

    // A second mapping, so what is read back is what a client would read
    // rather than what the writer still has in a register.
    let reader_cnc = CncFile::try_open(&temp.0).expect("the file is published");
    let reader = reader_cnc.counters().expect("the counter regions");

    let mut descriptors = Vec::new();
    reader.for_each(|descriptor| descriptors.push(descriptor.clone()));
    let keys = (0..COUNT as i32)
        .map(|id| reader.key(id).expect("a key"))
        .collect();

    drop(conductor);
    (temp, descriptors, keys)
}

#[test]
fn the_counter_catalogue_matches_a_real_drivers_record_for_record() {
    let (_temp, ours, keys) = published_counters();

    assert_eq!(
        COUNT,
        ours.len(),
        "the reference allocates forty-six, and ids are the contract"
    );

    for (index, descriptor) in ours.iter().enumerate() {
        let theirs = reference(index);
        let what = format!("counter {index} ({})", prefix(&theirs.label));

        assert_eq!(index as i32, descriptor.counter_id, "id == index: {what}");
        assert_eq!(theirs.state, layout::COUNTER_STATE_ALLOCATED, "{what}");
        assert_eq!(theirs.type_id, descriptor.type_id, "{what}");
        assert_eq!(0, descriptor.type_id, "{what}");
        assert_eq!(
            theirs.free_for_reuse_deadline_ms,
            layout::COUNTER_NOT_FREE_TO_REUSE,
            "{what}"
        );
        assert_eq!(
            theirs.key, keys[index],
            "the key is the four-byte index: {what}"
        );
        assert_eq!(
            prefix(&theirs.label),
            prefix(&descriptor.label),
            "the label's contract part: {what}"
        );
        assert_eq!(
            i64::from(descriptor.counter_id),
            descriptor.registration_id,
            "registration id == index: {what}"
        );
        assert_eq!(layout::NULL_VALUE, descriptor.owner_id, "{what}");
        assert_eq!(0, descriptor.reference_id, "{what}");
    }
}

#[test]
fn the_two_build_identity_labels_are_ours_and_not_the_references() {
    let (_temp, ours, _keys) = published_counters();

    // The mask in the test above hides these two, which is the point of having
    // this one: they must be *present*, and they must name this build.
    let errors = &ours[15].label;
    let software = &ours[34].label;

    assert!(
        errors.starts_with("Errors: version=1.53.2 commit=deepmsg-"),
        "{errors}"
    );
    assert!(
        software.starts_with("Aeron software: version=1.53.2 commit=deepmsg-"),
        "{software}"
    );
    assert_ne!(
        reference(34).label,
        *software,
        "the reference names its own build, and pretending to be it would be a lie in the file"
    );
}

#[test]
fn the_values_the_reference_sets_once_are_set_here_too() {
    let (_temp, ours, _keys) = published_counters();

    assert_eq!(
        79_106, ours[34].value,
        "the Aeron version this build implements, packed: 1.53.2"
    );
    assert_eq!(
        i64::from(deepmsg_core::version::semantic_version_compose(1, 0, 0)),
        ours[43].value,
        "the control protocol version"
    );
    assert!(
        ours[35].value > 0,
        "bytes currently mapped: this driver maps the CnC file, and reports it"
    );
}
