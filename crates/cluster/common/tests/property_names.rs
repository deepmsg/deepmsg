//! LC-6's denominator: the `aeron.cluster.*` names the reference reads, pinned
//! against the reference's own literals.
//!
//! `tests/fixtures/property-names.txt` is the verbatim output of
//! ```text
//! grep -rhoE '"aeron\.cluster\.[A-Za-z0-9._-]+"' aeron-cluster/src/main/java | tr -d '"' | sort -u
//! ```
//! at 664f58e705. If a reference bump adds a name, this test fails until the
//! table grows — which is the point: a name nobody classified is a name this
//! build silently ignores.

use deepmsg_cluster_common::config::{Disposition, NAMES};

const FIXTURE: &str = include_str!("fixtures/property-names.txt");

fn fixture_names() -> Vec<String> {
    FIXTURE
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_owned)
        .collect()
}

#[test]
fn the_table_holds_exactly_the_reference_names() {
    let expected = fixture_names();
    let actual: Vec<String> = NAMES.iter().map(|(name, _)| (*name).to_owned()).collect();
    assert_eq!(actual, expected, "table and fixture disagree");
    assert_eq!(actual.len(), 80, "the plan's 80 names");
}

#[test]
fn no_name_appears_twice_and_the_table_is_sorted() {
    for pair in NAMES.windows(2) {
        assert!(
            pair[0].0 < pair[1].0,
            "{} should sort before {}",
            pair[1].0,
            pair[0].0
        );
    }
}

#[test]
fn every_name_is_classified_or_pending() {
    // Pending is allowed today and must be gone by P3-11; what is never allowed
    // is a name with no row at all, which the test above already catches.
    let mut pending = 0;
    for (name, disposition) in NAMES {
        match disposition {
            Disposition::Pending => pending += 1,
            Disposition::Honoured => {}
            Disposition::AcceptedNotHonoured(reason) | Disposition::Refused(reason) => {
                assert!(!reason.is_empty(), "{name} needs a reason");
            }
        }
    }
    // P3-4b honoured five more (the two supplier names, the ingress fragment
    // limit and IPC switch, the session limit) and P3-4c the three timer
    // names, so 41 rows are still Pending and must be gone by P3-11.
    assert_eq!(pending, 41, "the P3-4a..4c rows are classified");
}

#[test]
fn the_disposition_labels_are_the_ones_the_tool_prints() {
    assert_eq!(Disposition::Pending.label(), "pending");
    assert_eq!(Disposition::Honoured.label(), "honoured");
    assert_eq!(
        Disposition::AcceptedNotHonoured("x").label(),
        "accepted-not-honoured"
    );
    assert_eq!(Disposition::Refused("x").label(), "refused");
}
