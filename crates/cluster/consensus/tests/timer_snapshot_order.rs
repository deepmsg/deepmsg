//! The same timers, two implementations, two snapshot orders.
//!
//! Plan §7.4: `WheelTimerService.snapshot` writes the records in
//! `Long2LongHashMap.entrySet().iterator()` order, `PriorityHeapTimerService
//! .snapshot` writes them in its heap array's order. The two orders are not the
//! same, so the reference's own two timer suppliers are not snapshot-byte
//! compatible with each other — LC-3/LC-5's byte comparison carries a
//! "same supplier on both sides" premise because of this.
//!
//! What is asserted:
//!
//! * the wheel's order is the wheel's own map's entry-iterator order (not the
//!   map's `forEachLong` order);
//! * the heap's order is its level order;
//! * the two orders differ for the same set of timers.

use deepmsg_cluster::timer::priority_heap::PriorityHeapTimerService;
use deepmsg_cluster::timer::wheel::WheelTimerService;
use deepmsg_cluster::timer::{TimerService, TimerSnapshotTaker};
use deepmsg_cluster_common::collections::Long2LongHashMap;
use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;

const TIMERS: [(i64, i64); 5] = [(1, 10), (2, 14), (3, 30), (4, 29), (5, 15)];

fn wheel() -> WheelTimerService {
    WheelTimerService::new(ClusterTimeUnit::MILLIS, 0, 8, 128)
}

fn heap() -> PriorityHeapTimerService {
    PriorityHeapTimerService::new()
}

#[derive(Default)]
struct Recorder(Vec<(i64, i64)>);

impl TimerSnapshotTaker for Recorder {
    fn snapshot_timer(&mut self, correlation_id: i64, deadline: i64) {
        self.0.push((correlation_id, deadline));
    }
}

#[test]
fn the_wheel_snapshot_is_the_entry_iterator_order_of_its_timer_map() {
    let mut service = wheel();
    for (correlation_id, deadline) in TIMERS {
        service.schedule_timer_for_correlation_id(correlation_id, deadline);
    }

    let mut recorder = Recorder::default();
    service.snapshot(&mut recorder);
    let snapshot = recorder.0;

    // The same pairs, in a map of their own, read with Agrona's `EntryIterator`.
    let mut map = Long2LongHashMap::new(i64::MAX);
    for (correlation_id, _) in TIMERS {
        map.put(
            correlation_id,
            service
                .timer_id_for_correlation_id(correlation_id)
                .expect("scheduled"),
        );
    }
    let mut entry_order = Vec::new();
    map.for_each_entry(|correlation_id, timer_id| {
        entry_order.push((correlation_id, service.wheel().deadline(timer_id)));
    });

    assert_eq!(entry_order, snapshot);

    // And it is not the map's `forEachLong` order, which is what P3-1a's
    // fixture pinned.
    let mut for_each_long = Vec::new();
    map.for_each(|correlation_id, timer_id| {
        for_each_long.push((correlation_id, service.wheel().deadline(timer_id)));
    });
    assert_ne!(for_each_long, snapshot);
}

#[test]
fn the_heap_snapshot_is_the_level_order_of_its_array() {
    let mut service = heap();
    for (correlation_id, deadline) in TIMERS {
        service.schedule_timer_for_correlation_id(correlation_id, deadline);
    }
    // Expire the two earliest, as `PriorityHeapTimerServiceTest
    // .snapshotProcessesAllScheduledTimers` does, so the array has the shape
    // that test asserts: 5 at the root, 4 and 3 below it.
    assert_eq!(2, service.poll(14, &mut |_: i64| true));

    let mut recorder = Recorder::default();
    service.snapshot(&mut recorder);
    let snapshot = recorder.0;
    assert_eq!(vec![(5, 15), (4, 29), (3, 30)], snapshot);

    let mut level_order = Vec::new();
    service.for_each(|correlation_id, deadline, _| level_order.push((correlation_id, deadline)));
    assert_eq!(level_order, snapshot);
}

#[test]
fn the_two_implementations_write_the_same_timers_in_different_orders() {
    let mut wheel_service = wheel();
    let mut heap_service = heap();
    for (correlation_id, deadline) in TIMERS {
        wheel_service.schedule_timer_for_correlation_id(correlation_id, deadline);
        heap_service.schedule_timer_for_correlation_id(correlation_id, deadline);
    }

    let mut wheel_recorder = Recorder::default();
    wheel_service.snapshot(&mut wheel_recorder);
    let wheel_snapshot = wheel_recorder.0;

    let mut heap_recorder = Recorder::default();
    heap_service.snapshot(&mut heap_recorder);
    let heap_snapshot = heap_recorder.0;

    let mut wheel_sorted = wheel_snapshot.clone();
    let mut heap_sorted = heap_snapshot.clone();
    wheel_sorted.sort_unstable();
    heap_sorted.sort_unstable();
    assert_eq!(wheel_sorted, heap_sorted, "the same five timers");
    assert_ne!(
        wheel_snapshot, heap_snapshot,
        "which is why LC-3's byte comparison needs the same supplier on both sides"
    );
}
