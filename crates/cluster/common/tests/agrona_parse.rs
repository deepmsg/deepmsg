//! The two property parsers must agree with Agrona 2.6.1 exactly — including
//! the cases where it *throws*, because a value we accept and Agrona rejects
//! (or the reverse) is a configuration that starts here and not there.
//!
//! `tests/fixtures/agrona-parse.tsv` was produced by running the jar; see
//! `tests/fixtures/regen/Gen6.java`.

use deepmsg_cluster_common::config::{duration_ns, size};

const PARSE: &str = include_str!("fixtures/agrona-parse.tsv");

struct Row {
    value: String,
    duration: String,
    size: String,
}

fn rows() -> Vec<Row> {
    let mut out = Vec::new();
    for line in PARSE.lines() {
        if line.starts_with('#') || line.trim().is_empty() || line.starts_with("kind\t") {
            continue;
        }
        let cols: Vec<&str> = line.split('\t').collect();
        assert_eq!(cols.len(), 4, "malformed row: {line}");
        out.push(Row {
            value: if cols[1] == "<empty>" {
                String::new()
            } else {
                cols[1].to_owned()
            },
            duration: cols[2].to_owned(),
            size: cols[3].to_owned(),
        });
    }
    out
}

#[test]
fn duration_matches_agrona_on_every_probe() {
    for row in rows() {
        match duration_ns(&row.value) {
            Ok(n) => assert_eq!(
                n.to_string(),
                row.duration,
                "duration of {:?}: parsed {n}, Agrona says {}",
                row.value,
                row.duration
            ),
            Err(_) => assert_eq!(
                row.duration, "throws",
                "duration of {:?}: we rejected it, Agrona gives {}",
                row.value, row.duration
            ),
        }
    }
}

#[test]
fn size_matches_agrona_on_every_probe() {
    for row in rows() {
        match size(&row.value) {
            Ok(n) => assert_eq!(
                n.to_string(),
                row.size,
                "size of {:?}: parsed {n}, Agrona says {}",
                row.value,
                row.size
            ),
            Err(_) => assert_eq!(
                row.size, "throws",
                "size of {:?}: we rejected it, Agrona gives {}",
                row.value, row.size
            ),
        }
    }
}

#[test]
fn the_two_traps_are_named_in_the_fixture() {
    // `m` is mebibytes for size and not a duration unit at all — the reference
    // has no minutes. Pinned separately from the table above so that a future
    // "helpful" change to either parser fails with this message.
    assert_eq!(size("3m").unwrap(), 3 * 1024 * 1024);
    assert!(duration_ns("3m").is_err());
    // A size is an i32 in the reference; a duration is not.
    assert!(size("3g").is_err());
    assert_eq!(duration_ns("2147483648").unwrap(), 2_147_483_648);
}
