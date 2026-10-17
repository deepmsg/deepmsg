//! `PriorityHeapTimerService` (`PriorityHeapTimerService.java`).
//!
//! A binary min-heap keyed on **deadline only** — equal deadlines are unordered,
//! which the reference's javadoc says outright — with a free list of removed
//! entries that a later schedule reuses. Every shift is the reference's, in the
//! reference's order, because the heap array's shape is what its snapshot
//! writes (`:179-187`): a level-order walk, not deadline order and not
//! insertion order.

use deepmsg_cluster_common::collections::HashMap;

use crate::timer::{POLL_LIMIT, TimerHandler, TimerService, TimerSnapshotTaker};

/// `EMPTY_TIMERS` / `MIN_CAPACITY`: the first allocation is 8 entries.
const MIN_CAPACITY: usize = 8;

/// `Aeron.NULL_VALUE`, the free list's reset value.
const NULL_VALUE: i64 = -1;

/// One heap entry (`PriorityHeapTimerService.Timer`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct HeapTimer {
    correlation_id: i64,
    deadline: i64,
    index: usize,
}

impl HeapTimer {
    const fn new(correlation_id: i64, deadline: i64, index: usize) -> Self {
        Self {
            correlation_id,
            deadline,
            index,
        }
    }

    fn reset(&mut self) {
        self.correlation_id = NULL_VALUE;
        self.deadline = NULL_VALUE;
        self.index = usize::MAX;
    }
}

/// The heap (`PriorityHeapTimerService.java:37-47`).
pub struct PriorityHeapTimerService {
    handler: Box<dyn TimerHandler>,
    /// `timerByCorrelationId`: correlation id → heap index.
    timer_by_correlation_id: HashMap<i64, usize>,
    timers: Vec<Option<HeapTimer>>,
    free_timers: Vec<HeapTimer>,
    size: usize,
    /// How many `Timer` objects have ever been made. The reference's
    /// free-list tests observe object identity; this is the same fact.
    allocated: usize,
}

impl PriorityHeapTimerService {
    /// `new PriorityHeapTimerService(TimerHandler)`.
    pub fn new(handler: Box<dyn TimerHandler>) -> Self {
        Self {
            handler,
            timer_by_correlation_id: HashMap::default(),
            timers: Vec::new(),
            free_timers: Vec::new(),
            size: 0,
            allocated: 0,
        }
    }

    /// How many `Timer` objects have been allocated (free-list reuse keeps it
    /// from growing).
    pub const fn allocated_timer_count(&self) -> usize {
        self.allocated
    }

    /// Walk the heap array in level order, as `forEach` does.
    pub fn for_each<F: FnMut(i64, i64, usize)>(&self, mut visit: F) {
        for timer in self.timers[..self.size].iter().flatten() {
            visit(timer.correlation_id, timer.deadline, timer.index);
        }
    }

    fn ensure_capacity(&mut self, required_capacity: usize) {
        let current_capacity = self.timers.len();
        if required_capacity > current_capacity {
            if current_capacity == 0 {
                self.timers.resize(MIN_CAPACITY, None);
                self.free_timers.reserve(MIN_CAPACITY);
            } else {
                let new_capacity = current_capacity + (current_capacity >> 1);
                self.timers.resize(new_capacity, None);
                self.free_timers
                    .reserve(new_capacity - self.free_timers.len());
            }
        }
    }

    fn add_to_free_list(&mut self, mut timer: HeapTimer) {
        timer.reset();
        self.free_timers.push(timer);
    }

    /// Place `timer` at `index`, keeping its own index and the map in step.
    fn place(&mut self, index: usize, mut timer: HeapTimer) {
        timer.index = index;
        self.timer_by_correlation_id
            .insert(timer.correlation_id, index);
        self.timers[index] = Some(timer);
    }

    /// `shiftUp(timers, startIndex, timer)` (`:206-226`).
    fn shift_up(&mut self, start_index: usize, timer: HeapTimer) {
        let mut index = start_index;
        while index > 0 {
            let prev_index = (index - 1) >> 1;
            let prev = self.timers[prev_index]
                .take()
                .expect("an index below size holds a timer");

            if timer.deadline >= prev.deadline {
                self.timers[prev_index] = Some(prev);
                break;
            }

            self.place(index, prev);
            index = prev_index;
        }
        self.place(index, timer);
    }

    /// `shiftDown(timers, size, startIndex, timer)` (`:228-256`).
    fn shift_down(&mut self, size: usize, start_index: usize, timer: HeapTimer) {
        let half = size >> 1;
        let mut index = start_index;

        while index < half {
            let mut next_index = (index << 1) + 1;
            let right = next_index + 1;
            let mut next = self.timers[next_index]
                .take()
                .expect("a child below size holds a timer");

            if right < size {
                let right_timer = self.timers[right]
                    .as_ref()
                    .expect("a child below size holds a timer");
                if next.deadline > right_timer.deadline {
                    // The left child goes back; the right one is the candidate.
                    self.timers[next_index] = Some(next);
                    next_index = right;
                    next = self.timers[right]
                        .take()
                        .expect("a child below size holds a timer");
                }
            }

            if timer.deadline < next.deadline {
                self.timers[next_index] = Some(next);
                break;
            }

            self.place(index, next);
            index = next_index;
        }

        self.place(index, timer);
    }
}

impl TimerService for PriorityHeapTimerService {
    /// `poll(now)` (`:66-100`).
    fn poll(&mut self, now: i64) -> usize {
        let mut expired_timers = 0;

        while self.size > 0 && expired_timers < POLL_LIMIT {
            let timer = self.timers[0].expect("the root of a non-empty heap");
            if timer.deadline > now {
                break;
            }

            if !self.handler.on_timer_event(timer.correlation_id) {
                break;
            }

            expired_timers += 1;

            self.size -= 1;
            let last_index = self.size;
            let last_timer = self.timers[last_index].take();
            self.timers[0] = None;

            if last_index != 0 {
                let last_timer = last_timer.expect("the last index below size holds a timer");
                self.shift_down(last_index, 0, last_timer);
            }

            self.timer_by_correlation_id.remove(&timer.correlation_id);
            self.add_to_free_list(timer);
        }

        expired_timers
    }

    /// `scheduleTimerForCorrelationId` (`:106-143`).
    fn schedule_timer_for_correlation_id(&mut self, correlation_id: i64, deadline: i64) {
        if let Some(&index) = self.timer_by_correlation_id.get(&correlation_id) {
            let existing = self.timers[index]
                .take()
                .expect("the map points at a live timer");
            if deadline < existing.deadline {
                let timer = HeapTimer {
                    deadline,
                    ..existing
                };
                self.shift_up(existing.index, timer);
            } else if deadline > existing.deadline {
                let timer = HeapTimer {
                    deadline,
                    ..existing
                };
                self.shift_down(self.size, existing.index, timer);
            } else {
                // Equal deadline: a no-op, as the reference's `if` chain is.
                self.timers[index] = Some(existing);
            }
            return;
        }

        self.ensure_capacity(self.size + 1);
        let index = self.size;
        self.size += 1;

        let timer = if let Some(mut free) = self.free_timers.pop() {
            free.correlation_id = correlation_id;
            free.deadline = deadline;
            free.index = index;
            free
        } else {
            self.allocated += 1;
            HeapTimer::new(correlation_id, deadline, index)
        };

        self.timer_by_correlation_id.insert(correlation_id, index);
        self.shift_up(index, timer);
    }

    /// `cancelTimerByCorrelationId` (`:149-173`).
    fn cancel_timer_by_correlation_id(&mut self, correlation_id: i64) -> bool {
        let Some(removed_index) = self.timer_by_correlation_id.remove(&correlation_id) else {
            return false;
        };

        let removed = self.timers[removed_index]
            .take()
            .expect("the map points at a live timer");

        self.size -= 1;
        let last_index = self.size;
        let last_timer = self.timers[last_index].take();

        if last_index != removed_index {
            let last_timer = last_timer.expect("the last index below size holds a timer");
            let last_correlation_id = last_timer.correlation_id;
            self.shift_down(last_index, removed_index, last_timer);

            let still_at_start = self.timers[removed_index]
                .as_ref()
                .is_some_and(|timer| timer.correlation_id == last_correlation_id);
            if still_at_start {
                let timer = self.timers[removed_index]
                    .take()
                    .expect("it was just checked");
                self.shift_up(removed_index, timer);
            }
        }

        self.add_to_free_list(removed);
        true
    }

    /// `snapshot` (`:179-187`): the heap array in level order.
    fn snapshot(&self, taker: &mut dyn TimerSnapshotTaker) {
        for timer in self.timers[..self.size].iter().flatten() {
            taker.snapshot_timer(timer.correlation_id, timer.deadline);
        }
    }

    /// `currentTime` (`:193-195`): a no-op for the heap.
    fn current_time(&mut self, _now: i64) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_deadlines_all_expire_and_their_order_is_the_heaps() {
        let expired = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let recorder = std::rc::Rc::clone(&expired);
        let mut service = PriorityHeapTimerService::new(Box::new(move |correlation_id: i64| {
            recorder.borrow_mut().push(correlation_id);
            true
        }));
        service.schedule_timer_for_correlation_id(1, 10);
        service.schedule_timer_for_correlation_id(2, 10);
        service.schedule_timer_for_correlation_id(3, 10);

        assert_eq!(3, service.poll(10));
        let mut order = expired.borrow().clone();
        order.sort_unstable();
        assert_eq!(vec![1, 2, 3], order);
    }

    #[test]
    fn a_cancelled_entry_is_reused_not_reallocated() {
        let mut service = PriorityHeapTimerService::new(Box::new(|_: i64| true));
        service.schedule_timer_for_correlation_id(1, 10);
        service.schedule_timer_for_correlation_id(2, 20);
        assert_eq!(2, service.allocated_timer_count());
        assert!(service.cancel_timer_by_correlation_id(1));
        service.schedule_timer_for_correlation_id(3, 30);
        assert_eq!(2, service.allocated_timer_count());
    }
}
