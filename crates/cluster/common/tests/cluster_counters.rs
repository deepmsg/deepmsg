//! The cluster counters, checked against metadata records the reference wrote.
//!
//! Two fixtures, both generated from the reference rather than from this code:
//!
//! * `fixtures/counters.tsv` and `fixtures/counters-metadata.bin` —
//!   `regen/GenCounters.java` allocates real counters through
//!   `ClusterCounters`, `RecoveryState` and `ClusterSession` on an embedded
//!   Java media driver, then dumps the 512-byte metadata record of each one.
//!   So both halves are pinned: the encoders produce the same key and label
//!   bytes, and the finders find the same records the reference allocated.
//! * `fixtures/counter-type-ids.tsv` — a `grep` of `AeronCounters.java`, so a
//!   missing or renamed type id is a red test.
//!
//! The values region is not part of the fixture: it carries the client
//! registration id, which changes per run and which no key, label or finder
//! contract reads. A zeroed region of the same record count is sufficient.

use deepmsg_cluster_common::counters::{
    self, CounterError, RecoveryStateKey, TYPE_IDS, find_cluster_counter, find_recovery_state,
};
use deepmsg_cnc::{CountersReader, layout};
use deepmsg_core::buffer::AtomicBuffer;

/// One metadata record per case in `counters.tsv`.
const CASES: usize = 7;

#[repr(align(8))]
struct Aligned<const N: usize>([u8; N]);

static METADATA: Aligned<{ CASES * layout::COUNTER_METADATA_LENGTH }> =
    Aligned(*include_bytes!("fixtures/counters-metadata.bin"));
static VALUES: Aligned<{ CASES * layout::COUNTER_VALUE_LENGTH }> =
    Aligned([0; CASES * layout::COUNTER_VALUE_LENGTH]);

fn reader() -> CountersReader<'static> {
    CountersReader::new(
        AtomicBuffer::from_slice(&METADATA.0).unwrap(),
        AtomicBuffer::from_slice(&VALUES.0).unwrap(),
    )
}

struct Row {
    case: String,
    type_id: i32,
    cluster_id: i32,
    service_id: Option<i32>,
    key: Vec<u8>,
    version: Option<String>,
    commit: Option<String>,
    label: String,
}

fn rows() -> Vec<Row> {
    let text = include_str!("fixtures/counters.tsv");
    let mut rows = Vec::new();

    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(9, fields.len(), "malformed fixture row: {line}");

        rows.push(Row {
            case: fields[0].to_owned(),
            type_id: fields[1].parse().unwrap(),
            cluster_id: fields[2].parse().unwrap(),
            service_id: (fields[3] != "-1").then(|| fields[3].parse().unwrap()),
            key: decode_hex(fields[5]),
            version: (fields[6] != "-").then(|| fields[6].to_owned()),
            commit: (fields[7] != "-").then(|| fields[7].to_owned()),
            label: fields[8].to_owned(),
        });
    }

    assert_eq!(CASES, rows.len());
    rows
}

fn decode_hex(text: &str) -> Vec<u8> {
    assert_eq!(0, text.len() % 2);
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

/// The key and label each encoder builds, compared with the bytes the
/// reference's own allocation left in the driver's CnC file.
#[test]
fn the_encoders_produce_the_key_and_label_the_reference_wrote() {
    for row in rows() {
        let spec = match row.case.as_str() {
            "cluster-state" | "cluster-state-other-cluster" => {
                counters::cluster_counter("Consensus Module state", row.cluster_id).unwrap()
            }
            "cluster-errors-versioned" => counters::versioned_cluster_counter(
                "Cluster Errors",
                row.cluster_id,
                row.version.as_deref().unwrap(),
                row.commit.as_deref().unwrap(),
            )
            .unwrap(),
            "service-counter" => counters::service_counter(
                "Cluster container max cycle time in ns",
                row.cluster_id,
                row.service_id.unwrap(),
            )
            .unwrap(),
            "service-error-versioned" => counters::service_error_counter(
                row.cluster_id,
                row.service_id.unwrap(),
                row.version.as_deref().unwrap(),
                row.commit.as_deref().unwrap(),
            )
            .unwrap(),
            "recovery-state" => {
                RecoveryStateKey::new(5, 1024, 1_700_000_000_000, row.cluster_id, vec![11, 12, 13])
                    .unwrap()
                    .encode()
                    .unwrap()
            }
            "session" => {
                counters::session_counter(row.cluster_id, 1_234_567_890_123, "test client").unwrap()
            }
            other => panic!("unknown case {other}"),
        };

        assert_eq!(row.key, spec.key(), "key bytes differ for {}", row.case);
        assert_eq!(row.label, spec.label(), "label differs for {}", row.case);
    }
}

/// `ClusterCounters.find` reads the cluster id at offset 0;
/// `RecoveryState.findCounterId` reads it at offset 24. Both are run against
/// the reference's records.
#[test]
fn the_finders_find_the_records_by_type_and_key() {
    let counters = reader();

    assert_eq!(
        Some(0),
        find_cluster_counter(
            &counters,
            counters::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
            7
        )
    );
    assert_eq!(
        Some(1),
        find_cluster_counter(
            &counters,
            counters::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
            9
        ),
        "the second counter of the same type belongs to the other cluster"
    );
    assert_eq!(
        None,
        find_cluster_counter(
            &counters,
            counters::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
            8
        )
    );
    assert_eq!(
        Some(6),
        find_cluster_counter(&counters, counters::CLUSTER_SESSION_TYPE_ID, 7)
    );
    assert_eq!(None, find_cluster_counter(&counters, 999, 7));

    assert_eq!(Some(5), find_recovery_state(&counters, 7));
    assert_eq!(None, find_recovery_state(&counters, 8));

    // The recovery key starts with `leadershipTermId = 5`, so a finder that
    // read the cluster id at offset 0 would answer 5 here.
    assert_eq!(
        None,
        find_recovery_state(&counters, 5),
        "the recovery state's cluster id is at offset {}, not at 0",
        counters::recovery::CLUSTER_ID_OFFSET
    );
}

/// The reader sees the labels the reference wrote, which is what `AeronStat`
/// and `ClusterTool` read.
#[test]
fn the_reader_sees_the_labels_the_reference_wrote() {
    let counters = reader();
    let mut seen = Vec::new();
    counters.for_each(|counter| {
        seen.push((counter.counter_id, counter.type_id, counter.label.clone()));
    });

    let expected: Vec<(i32, i32, String)> = rows()
        .into_iter()
        .enumerate()
        .map(|(index, row)| (index as i32, row.type_id, row.label))
        .collect();

    assert_eq!(expected, seen);
}

/// The recovery key round-trips through the 112-byte field a reader sees.
#[test]
fn the_recovery_state_key_decodes_from_the_counter() {
    let counters = reader();
    let counter_id = find_recovery_state(&counters, 7).unwrap();
    let key = counters.key(counter_id).unwrap();

    let state = RecoveryStateKey::decode(&key).unwrap();
    assert_eq!(5, state.leadership_term_id);
    assert_eq!(1024, state.log_position);
    assert_eq!(1_700_000_000_000, state.timestamp);
    assert_eq!(7, state.cluster_id);
    assert_eq!(vec![11, 12, 13], state.snapshot_recording_ids);

    assert_eq!(11, state.snapshot_recording_id(0).unwrap());
    assert_eq!(13, state.snapshot_recording_id(2).unwrap());
    assert_eq!(
        Err(CounterError::ServiceIdOutOfRange {
            service_id: 3,
            service_count: 3,
        }),
        state.snapshot_recording_id(3)
    );
    assert_eq!(
        Err(CounterError::ServiceIdOutOfRange {
            service_id: -1,
            service_count: 3,
        }),
        state.snapshot_recording_id(-1)
    );
}

/// The type-id table equals the ids `AeronCounters.java` defines in `200..=241`.
#[test]
fn the_type_id_table_matches_the_reference() {
    let text = include_str!("fixtures/counter-type-ids.tsv");
    let expected: Vec<(i32, &str)> = text
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| {
            let (id, name) = line.split_once('\t').unwrap();
            (id.parse().unwrap(), name)
        })
        .collect();

    assert_eq!(expected, TYPE_IDS);

    for window in TYPE_IDS.windows(2) {
        assert!(
            window[0].0 < window[1].0,
            "type ids are not strictly ascending: {:?}",
            window
        );
    }

    for (id, _) in TYPE_IDS {
        assert!(
            (200..=241).contains(id),
            "type id {id} is outside the cluster range"
        );
    }
}

/// The two bounds the reference enforces: a recovery key past 112 bytes
/// (`RecoveryState.java:129-133`) and a label past 380.
#[test]
fn the_field_widths_are_enforced() {
    assert!(RecoveryStateKey::new(0, 0, 0, 0, vec![0; 10]).is_ok());
    assert_eq!(
        Err(CounterError::KeyTooLong { length: 120 }),
        RecoveryStateKey::new(0, 0, 0, 0, vec![0; 11]).map(|_| ())
    );

    let long_name = "x".repeat(layout::COUNTER_LABEL_LENGTH_MAX);
    assert_eq!(
        Err(CounterError::LabelTooLong {
            length: long_name.len() + " - clusterId=0".len(),
        }),
        counters::cluster_counter(&long_name, 0).map(|_| ())
    );
}
