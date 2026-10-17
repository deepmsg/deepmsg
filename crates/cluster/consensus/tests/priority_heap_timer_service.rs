//! `PriorityHeapTimerServiceTest` (`PriorityHeapTimerServiceTest.java:30-618`),
//! one test per reference test.
//!
//! The reference mocks `TimerHandler` and `TimerSnapshotTaker` with Mockito;
//! here a recording handler plays both roles. Every expected order below is the
//! reference's, which means it is also the heap's array order — that order is
//! what the snapshot writes, so a "more sensible" heap that differs would fail
//! here.

use std::cell::RefCell;
use std::rc::Rc;

use deepmsg_cluster::timer::priority_heap::PriorityHeapTimerService;
use deepmsg_cluster::timer::{POLL_LIMIT, TimerService};

#[derive(Default)]
struct Recorder {
    events: RefCell<Vec<i64>>,
    snapshots: RefCell<Vec<(i64, i64)>>,
    answer: bool,
}

impl Recorder {
    fn answering(answer: bool) -> Rc<Self> {
        Rc::new(Self {
            answer,
            ..Self::default()
        })
    }

    fn events(&self) -> Vec<i64> {
        self.events.borrow().clone()
    }

    fn snapshots(&self) -> Vec<(i64, i64)> {
        self.snapshots.borrow().clone()
    }
}

fn service(recorder: &Rc<Recorder>) -> PriorityHeapTimerService {
    let events = Rc::clone(recorder);
    PriorityHeapTimerService::new(Box::new(move |correlation_id: i64| {
        events.events.borrow_mut().push(correlation_id);
        events.answer
    }))
}

fn snapshot(service: &PriorityHeapTimerService, recorder: &Rc<Recorder>) {
    let snapshots = Rc::clone(recorder);
    service.snapshot(&mut move |correlation_id: i64, deadline: i64| {
        snapshots
            .snapshots
            .borrow_mut()
            .push((correlation_id, deadline));
    });
}

#[test]
fn the_constructor_requires_a_handler() {
    // The reference asserts a `NullPointerException` when the handler is null.
    // A `Box<dyn TimerHandler>` cannot be null, so the type system is the
    // assertion; there is nothing to run.
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 1);
    assert_eq!(1, service.poll(1));
}

#[test]
fn poll_is_a_no_op_when_no_timers_where_scheduled() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);

    assert_eq!(0, service.poll(i64::MIN));
    assert!(recorder.events().is_empty());
}

#[test]
fn poll_is_a_no_op_when_no_scheduled_timers_are_expired() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 100);

    assert_eq!(0, service.poll(7));
    assert!(recorder.events().is_empty());
}

#[test]
fn poll_should_not_expire_timer_if_handler_returns_false() {
    let recorder = Recorder::answering(false);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 100);

    assert_eq!(0, service.poll(i64::MAX));
    assert_eq!(vec![1], recorder.events());

    // The timer is still there.
    assert!(service.cancel_timer_by_correlation_id(1));
}

#[test]
fn poll_should_expire_single_timer() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 100);

    assert_eq!(1, service.poll(200));
    assert_eq!(0, service.poll(200));
    assert_eq!(vec![1], recorder.events());
}

#[test]
fn poll_should_removed_expired_timers() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    for (correlation_id, deadline) in [(1, 100), (2, 200), (3, 300), (4, 400), (5, 500)] {
        service.schedule_timer_for_correlation_id(correlation_id, deadline);
    }

    assert_eq!(2, service.poll(200));
    assert_eq!(3, service.poll(500));
    assert_eq!(vec![1, 2, 3, 4, 5], recorder.events());
}

#[test]
fn poll_should_expire_timers_in_order_of_deadline_but_within_the_deadline_order_is_undefined() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    for (correlation_id, deadline) in [(1, 10), (4, 3), (2, 0), (3, 3), (5, 0), (6, 11)] {
        service.schedule_timer_for_correlation_id(correlation_id, deadline);
    }

    assert_eq!(5, service.poll(10));
    assert_eq!(vec![2, 5, 3, 4, 1], recorder.events());
}

#[test]
fn cancel_timer_by_correlation_id_is_a_no_op_if_no_timers_registered() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    assert!(!service.cancel_timer_by_correlation_id(100));
}

#[test]
fn cancel_timer_by_correlation_id_returns_false_for_unknown_correlation_id() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 100);
    service.schedule_timer_for_correlation_id(7, 50);

    assert!(!service.cancel_timer_by_correlation_id(3));
}

#[test]
fn cancel_timer_by_correlation_id_returns_true_after_cancelling_the_timer() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 100);
    service.schedule_timer_for_correlation_id(7, 50);
    service.schedule_timer_for_correlation_id(2, 90);

    assert!(service.cancel_timer_by_correlation_id(7));
    assert_eq!(2, service.poll(111));
    assert_eq!(vec![2, 1], recorder.events());
}

#[test]
fn cancel_timer_by_correlation_id_returns_true_after_cancelling_the_last_timer() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 100);
    service.schedule_timer_for_correlation_id(7, 50);
    service.schedule_timer_for_correlation_id(2, 90);

    assert!(service.cancel_timer_by_correlation_id(1));
    assert_eq!(2, service.poll(111));
    assert_eq!(vec![7, 2], recorder.events());
}

#[test]
fn cancel_timer_by_correlation_id_after_poll() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 30);
    service.schedule_timer_for_correlation_id(5, 15);
    service.schedule_timer_for_correlation_id(2, 20);
    service.schedule_timer_for_correlation_id(4, 40);

    assert_eq!(1, service.poll(19));
    assert!(service.cancel_timer_by_correlation_id(1));
    assert_eq!(2, service.poll(40));
    assert_eq!(vec![5, 2, 4], recorder.events());
}

#[test]
fn schedule_timer_for_an_existing_correlation_id_is_a_no_op_if_deadline_does_not_change() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(3, 30);
    service.schedule_timer_for_correlation_id(5, 50);
    service.schedule_timer_for_correlation_id(7, 70);

    service.schedule_timer_for_correlation_id(5, 50);
    assert_eq!(3, service.poll(70));
    assert_eq!(vec![3, 5, 7], recorder.events());
}

#[test]
fn schedule_timer_for_an_existing_correlation_id_should_shift_entry_up_when_deadline_is_decreasing()
{
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 10);
    service.schedule_timer_for_correlation_id(2, 10);
    service.schedule_timer_for_correlation_id(3, 30);
    service.schedule_timer_for_correlation_id(4, 30);
    service.schedule_timer_for_correlation_id(5, 50);

    service.schedule_timer_for_correlation_id(5, 10);
    assert_eq!(3, service.poll(10));
    assert_eq!(vec![1, 2, 5], recorder.events());
}

#[test]
fn schedule_timer_for_an_existing_correlation_id_should_shift_entry_down_when_deadline_is_increasing()
 {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 10);
    service.schedule_timer_for_correlation_id(2, 10);
    service.schedule_timer_for_correlation_id(3, 30);
    service.schedule_timer_for_correlation_id(4, 30);
    service.schedule_timer_for_correlation_id(5, 50);

    service.schedule_timer_for_correlation_id(1, 30);
    assert_eq!(4, service.poll(30));
    assert_eq!(vec![2, 4, 1, 3], recorder.events());
}

#[test]
fn poll_should_stop_after_poll_limit_is_reached() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    for i in 0..POLL_LIMIT * 2 {
        #[allow(clippy::cast_possible_wrap)]
        service.schedule_timer_for_correlation_id(i as i64, i as i64);
    }

    assert_eq!(POLL_LIMIT, service.poll(i64::MAX));
    assert_eq!(POLL_LIMIT, recorder.events().len());
}

#[test]
fn snapshot_processes_all_scheduled_timers() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 10);
    service.schedule_timer_for_correlation_id(2, 14);
    service.schedule_timer_for_correlation_id(3, 30);
    service.schedule_timer_for_correlation_id(4, 29);
    service.schedule_timer_for_correlation_id(5, 15);

    assert_eq!(2, service.poll(14));
    snapshot(&service, &recorder);

    assert_eq!(vec![1, 2], recorder.events());
    assert_eq!(vec![(5, 15), (4, 29), (3, 30)], recorder.snapshots());
}

#[test]
fn expire_than_cancel_timer() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    for (correlation_id, deadline) in [(1, 100), (2, 2), (3, 30), (4, 4), (5, 50)] {
        service.schedule_timer_for_correlation_id(correlation_id, deadline);
    }

    assert_eq!(2, service.poll(5));
    assert!(service.cancel_timer_by_correlation_id(1));
    assert_eq!(2, service.poll(1000));
    assert_eq!(vec![2, 4, 3, 5], recorder.events());
}

#[test]
fn move_up_an_existing_timer_and_cancel_another_one() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    for (correlation_id, deadline) in [(1, 1), (2, 1), (3, 3), (4, 4), (5, 5), (5, 1)] {
        service.schedule_timer_for_correlation_id(correlation_id, deadline);
    }

    assert!(service.cancel_timer_by_correlation_id(3));
    assert_eq!(4, service.poll(5));
    assert_eq!(vec![1, 2, 5, 4], recorder.events());
}

#[test]
fn move_down_an_existing_timer_and_cancel_another_one() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    for (correlation_id, deadline) in [(1, 1), (2, 1), (3, 3), (4, 4), (5, 5), (1, 5)] {
        service.schedule_timer_for_correlation_id(correlation_id, deadline);
    }

    assert!(service.cancel_timer_by_correlation_id(3));
    assert_eq!(4, service.poll(5));
    assert_eq!(vec![2, 4, 1, 5], recorder.events());
}

#[test]
fn cancel_expired_timer_is_a_no_op() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 1);
    service.schedule_timer_for_correlation_id(2, 1);
    service.schedule_timer_for_correlation_id(3, 3);

    assert_eq!(2, service.poll(1));
    assert!(!service.cancel_timer_by_correlation_id(1));
    assert!(!service.cancel_timer_by_correlation_id(2));
}

#[test]
fn schedule_must_retain_order_between_deadlines() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(3, 3);
    service.schedule_timer_for_correlation_id(4, 4);
    service.schedule_timer_for_correlation_id(5, 5);
    service.schedule_timer_for_correlation_id(6, 0);

    assert_eq!(4, service.poll(5));
    assert_eq!(vec![6, 3, 4, 5], recorder.events());
}

#[test]
fn should_reuse_expired_entries_from_a_free_list() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 1);
    service.schedule_timer_for_correlation_id(2, 2);
    service.schedule_timer_for_correlation_id(3, 3);
    assert_eq!(3, service.allocated_timer_count());

    assert_eq!(2, service.poll(2));

    service.schedule_timer_for_correlation_id(4, 4);
    service.schedule_timer_for_correlation_id(5, 5);
    service.schedule_timer_for_correlation_id(6, 0);
    // The two expired entries came back off the free list.
    assert_eq!(4, service.allocated_timer_count());

    assert_eq!(4, service.poll(5));
    // The first poll's expiries came first; the four the reference verifies
    // in order follow them.
    assert_eq!(vec![1, 2, 6, 3, 4, 5], recorder.events());
}

#[test]
fn should_reuse_canceled_timer_entries_from_a_free_list() {
    let recorder = Recorder::answering(true);
    let mut service = service(&recorder);
    service.schedule_timer_for_correlation_id(1, 1);
    service.schedule_timer_for_correlation_id(2, 2);
    service.schedule_timer_for_correlation_id(3, 3);
    service.schedule_timer_for_correlation_id(4, 4);
    assert_eq!(4, service.allocated_timer_count());

    assert!(service.cancel_timer_by_correlation_id(2));
    assert!(service.cancel_timer_by_correlation_id(4));

    service.schedule_timer_for_correlation_id(5, 5);
    service.schedule_timer_for_correlation_id(6, 0);
    assert_eq!(4, service.allocated_timer_count());

    assert_eq!(4, service.poll(5));
    assert_eq!(vec![6, 1, 3, 5], recorder.events());
}

#[test]
fn many_random_operations() {
    let mut harness = Harness::new();
    for _ in 0..10_000 {
        harness.random_operation();
    }
}

struct Harness {
    service: PriorityHeapTimerService,
    deadlines: Rc<RefCell<std::collections::BTreeMap<i64, i64>>>,
    time: Rc<std::cell::Cell<i64>>,
    state: u64,
    next_correlation_id: i64,
}

impl Harness {
    fn new() -> Self {
        let deadlines = Rc::new(RefCell::new(std::collections::BTreeMap::new()));
        let time = Rc::new(std::cell::Cell::new(1000));
        let checked_deadlines = Rc::clone(&deadlines);
        let checked_time = Rc::clone(&time);
        Self {
            service: PriorityHeapTimerService::new(Box::new(move |correlation_id: i64| {
                let mut deadlines = checked_deadlines.borrow_mut();
                let deadline = deadlines
                    .remove(&correlation_id)
                    .expect("a scheduled timer");
                assert!(deadline >= 1000, "deadline={deadline}");
                assert!(deadline <= checked_time.get(), "deadline={deadline}");
                true
            })),
            deadlines,
            time,
            state: 1,
            next_correlation_id: 1,
        }
    }

    fn random(&mut self, bound: usize) -> usize {
        // A deterministic LCG: the reference uses `new Random(1)`, and the
        // point of the test is the invariants, not the exact script.
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.state >> 33) as usize) % bound
    }

    fn random_operation(&mut self) {
        let len = self.deadlines.borrow().len();
        if len < 5 {
            self.schedule_timer();
        } else if len > 20 {
            self.increment_time_and_poll();
        } else {
            match self.random(5) {
                0 => self.increment_time_and_poll(),
                1 => self.cancel_random_timer(),
                2 => self.reschedule_random_timer(),
                _ => self.schedule_timer(),
            }
        }
        self.validate_ordering();
    }

    fn schedule_timer(&mut self) {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id += 1;
        let deadline = self.time.get() + self.random(20) as i64;
        self.service
            .schedule_timer_for_correlation_id(correlation_id, deadline);
        self.deadlines.borrow_mut().insert(correlation_id, deadline);
    }

    fn cancel_random_timer(&mut self) {
        let correlation_id = self.random_scheduled_correlation_id();
        assert!(
            self.deadlines
                .borrow_mut()
                .remove(&correlation_id)
                .is_some()
        );
        assert!(self.service.cancel_timer_by_correlation_id(correlation_id));
    }

    fn reschedule_random_timer(&mut self) {
        let correlation_id = self.random_scheduled_correlation_id();
        let deadline = self.time.get() + self.random(20) as i64;
        assert!(
            self.deadlines
                .borrow_mut()
                .insert(correlation_id, deadline)
                .is_some()
        );
        self.service
            .schedule_timer_for_correlation_id(correlation_id, deadline);
    }

    fn random_scheduled_correlation_id(&mut self) -> i64 {
        let len = self.deadlines.borrow().len();
        let index = self.random(len);
        let deadlines = self.deadlines.borrow();
        *deadlines.keys().nth(index).expect("an index below len")
    }

    fn increment_time_and_poll(&mut self) {
        let time = self.time.get() + self.random(2) as i64;
        self.time.set(time);
        self.service.poll(time);
    }

    fn validate_ordering(&mut self) {
        let mut next_index = 0;
        let mut entries: Vec<(i64, i64, usize)> = Vec::new();
        self.service.for_each(|correlation_id, deadline, index| {
            assert_eq!(next_index, index);
            next_index += 1;
            if index > 0 {
                let parent = entries[(index - 1) / 2];
                assert!(
                    parent.1 <= deadline,
                    "parent={parent:?} child=(..,{deadline},{index})"
                );
            }
            entries.push((correlation_id, deadline, index));
        });
        assert_eq!(self.deadlines.borrow().len(), entries.len());
    }
}
