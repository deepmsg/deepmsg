//! The archive's own test suite — and, for now, the ledger of what it owes.
//!
//! Two suites test this crate and they answer different questions.
//!
//! **The reference's C suite** answers "can this replace the Java archive?".
//! It is the acceptance standard, because its cases are not ours: they were
//! written by the reference's authors against the Java implementation and
//! encode behaviour we would not think to ask about. It is also an instrument
//! rather than a test suite — it needs a reference checkout, a CMake build of
//! nine binaries, a shim and a JDK, and it runs for seven minutes serialised
//! (ports are hard-coded). It cannot be this crate's tests.
//!
//! **This suite** answers "is our archive correct, and did it stay correct?".
//! It is ours, it lives here, it runs in `cargo test --workspace`, and it is
//! the only thing that reaches the storage layer at all: the reference's C
//! suite drives through the client API and never opens a catalog or a segment.
//!
//! `reference-cases.tsv` is the join between the two. It lists every case the
//! reference runs — 306 of them, from `--gtest_list_tests` on the suite built
//! at `664f58e705` — and records which of them this suite owns, so that
//! "covered" is a fact about a name in a file rather than a feeling. Forty of
//! them are `client-unit`: unit tests of the reference's *client* library
//! (`shouldThrowIf…` argument validation, an async-op leak), which the client
//! track answers to and this crate does not.
//!
//! What CI can check is that the ledger is well formed and that nobody has
//! quietly dropped a row. What CI *cannot* check is whether the ledger still
//! matches the reference — that takes the acceptance run, which lists the cases
//! from the binaries themselves and holds this file to that live list. A file
//! of names is a denominator, not a proof.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

/// The nine binaries the reference builds, with the number of cases each
/// contributes. Pinned so that a row disappearing is a deliberate edit.
const REFERENCE_BINARIES: &[(&str, usize, Kind)] = &[
    ("aeron_archive_test", 74, Kind::System),
    (
        "aeron_archive_persistent_subscription_test",
        80,
        Kind::System,
    ),
    (
        "aeron_archive_persistent_subscription_resilience_test",
        50,
        Kind::System,
    ),
    ("aeron_archive_async_client_test", 3, Kind::System),
    ("archiveTestW", 50, Kind::System),
    ("persistentSubscriptionTestW", 9, Kind::System),
    (
        "aeron_archive_persistent_subscription_context_test",
        31,
        Kind::ClientUnit,
    ),
    ("aeron_archive_async_connect_leak_test", 1, Kind::ClientUnit),
    ("persistentSubscriptionContextTestW", 8, Kind::ClientUnit),
];

/// How many cases the ledger holds. 266 exercise the archive server; 40 are the
/// reference client's own unit tests.
const REFERENCE_CASES: usize = 306;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    System,
    ClientUnit,
}

impl Kind {
    fn parse(text: &str) -> Option<Self> {
        match text {
            "system" => Some(Self::System),
            "client-unit" => Some(Self::ClientUnit),
            _ => None,
        }
    }
}

struct Row {
    case: String,
    binary: String,
    kind: Kind,
    status: String,
    covered_by: String,
}

fn ledger() -> Vec<Row> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/reference-cases.tsv");
    let text = fs::read_to_string(&path).expect("the ledger is missing");

    text.lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .map(|line| {
            let columns: Vec<&str> = line.split('\t').collect();
            let [case, binary, kind, status, covered_by] = columns[..] else {
                panic!("a ledger row is not five columns: {line}");
            };
            Row {
                case: case.to_string(),
                binary: binary.to_string(),
                kind: Kind::parse(kind).unwrap_or_else(|| panic!("{case}: unknown kind {kind:?}")),
                status: status.to_string(),
                covered_by: covered_by.to_string(),
            }
        })
        .collect()
}

/// The denominator, and that nobody has lost a row.
///
/// A count on its own would be weak — 306 minus one plus one is still 306 — so
/// it is checked per binary, against the list above, and the case names are
/// checked for duplicates. What this cannot see is a case the *reference* gained
/// since; that is the acceptance run's comparison, and the module comment says
/// so rather than leaving it implied.
#[test]
fn the_ledger_is_the_reference_suites_case_set() {
    let rows = ledger();
    assert_eq!(REFERENCE_CASES, rows.len(), "the denominator moved");

    let mut per_binary: BTreeMap<&str, usize> = BTreeMap::new();
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for row in &rows {
        *per_binary.entry(row.binary.as_str()).or_default() += 1;
        assert!(seen.insert(&row.case), "{} is listed twice", row.case);

        // A case name is `Class.method`; the parameterised ones carry a `/index`.
        let (class, method) = row
            .case
            .split_once('.')
            .unwrap_or_else(|| panic!("{}", row.case));
        assert!(
            !class.is_empty() && !method.is_empty(),
            "{} is not Class.method",
            row.case
        );
    }

    let expected: BTreeMap<&str, usize> = REFERENCE_BINARIES
        .iter()
        .map(|(name, count, _)| (*name, *count))
        .collect();
    assert_eq!(expected, per_binary, "a binary's case count moved");
}

/// The kind column is a property of the binary, not of the row.
///
/// It is derivable, and holding it to that is what stops a one-off edit from
/// quietly reclassifying a client unit test as an archive obligation.
#[test]
fn every_row_agrees_with_its_binary_about_what_kind_of_case_it_is() {
    let by_binary: BTreeMap<&str, Kind> = REFERENCE_BINARIES
        .iter()
        .map(|(name, _, kind)| (*name, *kind))
        .collect();

    for row in ledger() {
        assert_eq!(
            by_binary[row.binary.as_str()],
            row.kind,
            "{} is filed as {:?}, and its binary says otherwise",
            row.case,
            row.kind
        );
    }
}

/// Where a case stands, and that the two columns agree about it.
///
/// A `ported` row with no test named would be the worst of the three states —
/// it reads as done and points at nothing — so the columns are held together
/// here rather than left to a reviewer to notice.
#[test]
fn a_case_is_only_claimed_when_something_claims_it() {
    let rows = ledger();
    let mut ported = 0;
    let mut reference_only = 0;

    for row in &rows {
        match row.status.as_str() {
            "pending" => assert!(
                row.covered_by.is_empty(),
                "{} is pending and names a test",
                row.case
            ),
            "ported" => {
                ported += 1;
                assert!(
                    row.covered_by.starts_with("tests/"),
                    "{} is ported but does not name a test: {:?}",
                    row.case,
                    row.covered_by
                );
            }
            "reference-only" => {
                reference_only += 1;
                assert!(
                    !row.covered_by.is_empty(),
                    "{} is left to the acceptance instrument without saying why",
                    row.case
                );
            }
            other => panic!("{}: unknown status {other:?}", row.case),
        }
    }

    // Not an assertion about progress — at the time of writing nothing is
    // ported, and that is the honest number. It is here so that the suite says
    // how far along it is in a line of `cargo test` output rather than only in
    // a document nobody re-reads.
    println!(
        "archive suite: {ported} of {REFERENCE_CASES} reference cases ported, \
         {reference_only} left to the acceptance instrument, \
         {} pending",
        rows.len() - ported - reference_only
    );
}
