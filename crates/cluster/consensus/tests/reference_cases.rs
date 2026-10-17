//! The guard P3-0b owed: the reference case table must stay complete and
//! classified.
//!
//! `crates/cluster/tests/reference-cases-{consensus,other}.tsv` are the
//! denominator for how much of the reference's cluster suite this build
//! carries. P3-0b built them by hand once and verified the counts by hand; this
//! makes the same checks run on every `cargo test`, which is what the card's
//! DoD actually asked for.
//!
//! What is checked is only what can be checked without cheating: the row count,
//! the per-kind split, that every row is classified, and that the `n/a`
//! vocabulary stays closed. Whether a row's classification is *right* is a
//! reading of the reference, not a property a test can decide — that is what
//! `reference-cases.gen.py` is for.

const CONSENSUS: &str = include_str!("../../tests/reference-cases-consensus.tsv");
const OTHER: &str = include_str!("../../tests/reference-cases-other.tsv");

#[derive(Debug)]
struct Row {
    kind: String,
    surface: String,
    disposition: String,
}

fn rows() -> Vec<Row> {
    let mut out = Vec::new();
    for text in [CONSENSUS, OTHER] {
        for line in text.lines() {
            if line.starts_with('#') || line.trim().is_empty() {
                continue;
            }
            let cols: Vec<&str> = line.split('\t').collect();
            assert_eq!(cols.len(), 9, "malformed row: {line}");
            out.push(Row {
                kind: cols[0].to_owned(),
                surface: cols[5].to_owned(),
                disposition: cols[7].to_owned(),
            });
        }
    }
    out
}

#[test]
fn the_denominator_is_five_hundred_and_seventy_four() {
    let rows = rows();
    assert_eq!(rows.len(), 574, "574 = 178 system + 396 unit");
    assert_eq!(rows.iter().filter(|r| r.kind == "system").count(), 178);
    assert_eq!(rows.iter().filter(|r| r.kind == "unit").count(), 396);
}

#[test]
fn every_row_is_classified() {
    for row in rows() {
        assert!(!row.surface.is_empty(), "{row:?} has no assert_surface");
    }
}

#[test]
fn every_java_injection_row_has_a_disposition() {
    for row in rows() {
        if row.kind == "system" && row.surface.contains("java-injection") {
            assert!(
                !row.disposition.is_empty(),
                "a java-injection row without a disposition"
            );
        }
    }
}

#[test]
fn the_na_vocabulary_stays_closed() {
    const ALLOWED: &[&str] = &[
        "n/a:premium",
        "n/a:logging",
        "n/a:removed-upstream",
        "n/a:java-injection",
        "n/a:fixture-self-test",
    ];
    for row in rows() {
        let d = row.disposition.as_str();
        if d.starts_with("n/a:") {
            assert!(ALLOWED.contains(&d), "unexpected n/a reason: {d}");
        } else if !d.is_empty() {
            assert!(
                d.starts_with("hook:"),
                "disposition must be n/a:* or hook:*, got {d}"
            );
        }
    }
}
