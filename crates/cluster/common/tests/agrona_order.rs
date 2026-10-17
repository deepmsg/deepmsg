//! The Agrona `Long2LongHashMap` must iterate in the jar's order, not merely
//! hold the jar's pairs.
//!
//! `tests/fixtures/agrona-order.tsv` was produced by running agrona-2.6.1; see
//! `tests/fixtures/regen/Gen.java`. If this test fails, the snapshot bytes the
//! consensus module writes would differ from the reference's even though every
//! timer pair is correct — which is the failure mode plan §7.4 exists for.

use deepmsg_cluster_common::collections::Long2LongHashMap;

const ORDER: &str = include_str!("fixtures/agrona-order.tsv");

fn expected(case: &str) -> String {
    for line in ORDER.lines() {
        if line.starts_with('#') || line.trim().is_empty() || line.starts_with("case\t") {
            continue;
        }
        let (name, order) = line.split_once('\t').expect("case<TAB>order");
        if name == case {
            return order.to_owned();
        }
    }
    panic!("fixture has no case {case}");
}

fn order_of(map: &Long2LongHashMap) -> String {
    let mut out = String::new();
    map.for_each(|k, v| {
        out.push_str(&format!("{k}={v},"));
    });
    out
}

fn sequential(n: i64) -> Long2LongHashMap {
    let mut map = Long2LongHashMap::new(i64::MAX);
    for i in 1..=n {
        map.put(i, i * 10);
    }
    map
}

#[test]
fn sequential_keys_without_a_resize() {
    assert_eq!(order_of(&sequential(16)), expected("seq-16"));
}

#[test]
fn sequential_keys_across_several_resizes() {
    assert_eq!(order_of(&sequential(100)), expected("seq-100"));
}

#[test]
fn scattered_keys() {
    let mut map = Long2LongHashMap::new(i64::MAX);
    // The Java generator starts from the literal `0x9E3779B97F4A7C15L`, which
    // is a negative long there too — keep the same bit pattern.
    let mut k: i64 = 0x9E37_79B9_7F4A_7C15u64 as i64;
    for i in 0..64i64 {
        map.put(k, i);
        k = k.wrapping_add(0x5851_F42D_4C95_7F2D);
    }
    assert_eq!(order_of(&map), expected("scatter-64"));
}

#[test]
fn removals_shift_the_probe_chain() {
    let mut map = sequential(64);
    let mut i = 1;
    while i <= 64 {
        map.remove(i);
        i += 2;
    }
    assert_eq!(order_of(&map), expected("remove-odd-64"));
}

#[test]
fn remove_then_reinsert_reuses_different_slots() {
    let mut map = Long2LongHashMap::new(i64::MAX);
    for i in 1..=40 {
        map.put(i, i);
    }
    for i in (5..=35).step_by(5) {
        map.remove(i);
    }
    for i in (5..=35).step_by(5) {
        map.put(i, i * 100);
    }
    assert_eq!(order_of(&map), expected("reinsert-40"));
}

#[test]
fn capacity_and_size_follow_the_same_growth() {
    let mut map = Long2LongHashMap::new(i64::MAX);
    assert_eq!(map.capacity(), 8);
    for i in 1..=16 {
        map.put(i, i);
    }
    // 0.65 * 8 = 5, so the sixth insert doubles to 16, the eleventh to 32.
    assert_eq!(map.capacity(), 32);
    assert_eq!(map.size(), 16);
    assert_eq!(map.get(7), 7);
    assert_eq!(map.get(99), i64::MAX);
}
