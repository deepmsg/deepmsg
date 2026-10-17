//! Pins the two frozen election seams against the reference's own call surface.
//!
//! This is the gate `P3-5a-α`'s DoD asks for: the trait's method set must equal
//! what `Election.java` actually calls, and the counts in
//! `tests/fixtures/election-surface.tsv` must not drift. It does not check
//! parameter *types* — a `&str`/`String` swap would not fail here — so the
//! fixture stays the place to read them.

use deepmsg_cluster::election::ElectionState;
use deepmsg_cluster::election::{HOST_METHODS, SENDER_METHODS};

const SURFACE: &str = include_str!("fixtures/election-surface.tsv");

struct Row<'a> {
    side: &'a str,
    java_name: &'a str,
    rust_name: &'a str,
    calls: usize,
}

fn rows() -> Vec<Row<'static>> {
    let mut out = Vec::new();
    for line in SURFACE.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&'static str> = line.split('\t').collect();
        if cols[0] == "side" {
            continue; // header
        }
        assert_eq!(cols.len(), 8, "malformed fixture row: {line}");
        out.push(Row {
            side: cols[0],
            java_name: cols[1],
            rust_name: cols[3],
            calls: cols[6].parse().expect("call count"),
        });
    }
    out
}

#[test]
fn the_host_surface_is_thirty_signatures_across_forty_four_calls() {
    let host: Vec<_> = rows().into_iter().filter(|r| r.side == "host").collect();
    let names: std::collections::BTreeSet<_> = host.iter().map(|r| r.java_name).collect();
    assert_eq!(host.len(), 30, "host signatures");
    assert_eq!(names.len(), 26, "distinct Java names — the plan's 26");
    assert_eq!(
        host.iter().map(|r| r.calls).sum::<usize>(),
        44,
        "host calls"
    );
}

#[test]
fn the_sender_surface_is_six_signatures_across_eight_calls() {
    let sender: Vec<_> = rows().into_iter().filter(|r| r.side == "sender").collect();
    assert_eq!(sender.len(), 6, "sender signatures");
    assert_eq!(
        sender.iter().map(|r| r.calls).sum::<usize>(),
        8,
        "sender calls"
    );
}

#[test]
fn the_trait_method_sets_equal_the_reference_call_surface() {
    let expected = |side: &str| -> Vec<String> {
        let mut v: Vec<String> = rows()
            .into_iter()
            .filter(|r| r.side == side)
            .map(|r| r.rust_name.to_owned())
            .collect();
        v.sort();
        v
    };

    let mut host_actual: Vec<String> = HOST_METHODS.iter().map(|s| (*s).to_owned()).collect();
    host_actual.sort();
    assert_eq!(
        host_actual,
        expected("host"),
        "ElectionHost vs EL host calls"
    );

    let mut sender_actual: Vec<String> = SENDER_METHODS.iter().map(|s| (*s).to_owned()).collect();
    sender_actual.sort();
    assert_eq!(
        sender_actual,
        expected("sender"),
        "ConsensusSender vs EL publisher calls"
    );
}

#[test]
fn no_method_list_has_a_duplicate() {
    for (label, list) in [
        ("HOST_METHODS", HOST_METHODS),
        ("SENDER_METHODS", SENDER_METHODS),
    ] {
        let set: std::collections::BTreeSet<_> = list.iter().collect();
        assert_eq!(set.len(), list.len(), "{label} has a duplicate");
    }
}

#[test]
fn the_eighteen_election_states_keep_their_counter_codes() {
    assert_eq!(ElectionState::ALL.len(), 18);
    for (code, state) in ElectionState::ALL.iter().enumerate() {
        assert_eq!(state.code(), code as i32, "{state:?} moved");
    }
}
