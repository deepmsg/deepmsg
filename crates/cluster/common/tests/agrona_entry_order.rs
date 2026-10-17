//! `entrySet().iterator()` order, which is **not** `forEachLong`'s.
//!
//! `WheelTimerService.snapshot` (`WheelTimerService.java:120-129`) walks the
//! map with an `EntryIterator`, and Agrona's `AbstractIterator` walks the slots
//! **downwards with a wrap**, not upwards. The two orders differ over the same
//! map, so a port that reuses the `forEachLong` order writes different timer
//! records into the snapshot.
//!
//! `tests/fixtures/agrona-entry-order.tsv` was produced by running agrona-2.6.1
//! (`java Gen entry`, see `fixtures/regen/Gen.java`).

use deepmsg_cluster_common::collections::Long2LongHashMap;

const ENTRY_ORDER: &str = include_str!("fixtures/agrona-entry-order.tsv");

fn expected(case: &str) -> String {
    for line in ENTRY_ORDER.lines() {
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

fn entry_order_of(map: &Long2LongHashMap) -> String {
    let mut out = String::new();
    map.for_each_entry(|k, v| {
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
fn entry_iterator_order_without_a_resize() {
    assert_eq!(entry_order_of(&sequential(16)), expected("seq-16"));
}

#[test]
fn entry_iterator_order_across_resizes() {
    assert_eq!(entry_order_of(&sequential(100)), expected("seq-100"));
}

#[test]
fn entry_iterator_order_for_scattered_keys() {
    let mut map = Long2LongHashMap::new(i64::MAX);
    let mut k: i64 = 0x9E37_79B9_7F4A_7C15u64 as i64;
    for i in 0..64i64 {
        map.put(k, i);
        k = k.wrapping_add(0x5851_F42D_4C95_7F2D);
    }
    assert_eq!(entry_order_of(&map), expected("scatter-64"));
}

#[test]
fn entry_iterator_order_after_removals() {
    let mut map = sequential(64);
    let mut i = 1;
    while i <= 64 {
        map.remove(i);
        i += 2;
    }
    assert_eq!(entry_order_of(&map), expected("remove-odd-64"));
}

#[test]
fn entry_iterator_order_after_reinsertion() {
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
    assert_eq!(entry_order_of(&map), expected("reinsert-40"));
}

#[test]
fn the_two_orders_are_not_the_same_order() {
    // The premise of the Q8 note: one map, two iteration orders.
    let map = sequential(16);
    let mut ascending = String::new();
    map.for_each(|k, v| ascending.push_str(&format!("{k}={v},")));
    assert_ne!(ascending, entry_order_of(&map));
}
