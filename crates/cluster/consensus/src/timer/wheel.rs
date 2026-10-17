//! `WheelTimerService` and the Agrona `DeadlineTimerWheel` it extends.
//!
//! The reference's timer wheel is Agrona's, and its behaviour is the timer's
//! behaviour: a fixed tick resolution, one bucket region per tick, `POLL_LIMIT`
//! expiries per pass, and a `pollIndex` that survives across calls so a sweep
//! of a large bucket resumes where it stopped. Everything here was read out of
//! `agrona-2.6.1.jar` with `javap -p -c`; there is no Agrona source in this
//! checkout.
//!
//! # The two orders that matter
//!
//! * **Expiry** is per tick, in slot order within the tick's block, starting at
//!   `pollIndex`. That order is what a leader appends to the log, so it is the
//!   log's byte order.
//! * **Snapshot** is not the wheel at all: `WheelTimerService.snapshot` walks
//!   `timerIdByCorrelationIdMap` with Agrona's `EntryIterator`, whose order is
//!   [`Long2LongHashMap::for_each_entry`], and reads each deadline back out of
//!   the wheel (`WheelTimerService.java:118-131`).

use deepmsg_cluster_common::collections::Long2LongHashMap;
use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;

use crate::timer::{POLL_LIMIT, TimerHandler, TimerService, TimerSnapshotTaker};

/// `DeadlineTimerWheel.NULL_DEADLINE`: an empty slot, and never an expiry.
pub const NULL_DEADLINE: i64 = i64::MAX;

/// `INITIAL_TICK_ALLOCATION`.
const INITIAL_TICK_ALLOCATION: usize = 16;

/// The smallest `ticks_per_wheel` the wheel accepts (and it must be a power of
/// two).
const MIN_TICKS_PER_WHEEL: usize = 2;

/// Agrona's `DeadlineTimerWheel`.
///
/// `wheel` is a flat array: `ticks_per_wheel` blocks of `tick_allocation`
/// deadline slots. A timer id names `(tick, slot)` as `(tick << 32) | slot`.
pub struct DeadlineTimerWheel {
    tick_resolution: i64,
    start_time: i64,
    current_tick: i64,
    timer_count: usize,
    ticks_per_wheel: usize,
    tick_mask: usize,
    resolution_bits_to_shift: u32,
    tick_allocation: usize,
    allocation_bits_to_shift: u32,
    poll_index: usize,
    wheel: Vec<i64>,
}

impl DeadlineTimerWheel {
    /// `new DeadlineTimerWheel(timeUnit, startTime, tickResolution,
    /// ticksPerWheel)` with Agrona's initial tick allocation of 16.
    ///
    /// # Panics
    ///
    /// If `tick_resolution` is not a power of two, or `ticks_per_wheel` is not
    /// a power of two at least 2 — Agrona's own `checkResolution` /
    /// `checkTicksPerWheel`.
    pub fn new(start_time: i64, tick_resolution: i64, ticks_per_wheel: usize) -> Self {
        assert!(
            tick_resolution > 0 && (tick_resolution as u64).is_power_of_two(),
            "tick resolution must be a power of two: {tick_resolution}"
        );
        assert!(
            ticks_per_wheel >= MIN_TICKS_PER_WHEEL && ticks_per_wheel.is_power_of_two(),
            "ticks per wheel must be a power of two >= {MIN_TICKS_PER_WHEEL}: {ticks_per_wheel}"
        );

        let tick_allocation = INITIAL_TICK_ALLOCATION;
        Self {
            tick_resolution,
            start_time,
            current_tick: 0,
            timer_count: 0,
            ticks_per_wheel,
            tick_mask: ticks_per_wheel - 1,
            resolution_bits_to_shift: tick_resolution.trailing_zeros(),
            tick_allocation,
            allocation_bits_to_shift: tick_allocation.trailing_zeros(),
            poll_index: 0,
            wheel: vec![NULL_DEADLINE; ticks_per_wheel * tick_allocation],
        }
    }

    /// The configured resolution, in the clock's unit.
    pub const fn tick_resolution(&self) -> i64 {
        self.tick_resolution
    }

    /// How many timers are scheduled.
    pub const fn timer_count(&self) -> usize {
        self.timer_count
    }

    /// `currentTickTime()`: the start of the **next** tick.
    pub const fn current_tick_time(&self) -> i64 {
        self.start_time + ((self.current_tick + 1) << self.resolution_bits_to_shift)
    }

    /// `currentTickTime(long now)`: move the current tick forward to `now`,
    /// never backwards. It does **not** reset `pollIndex` — the reference does
    /// not either, and a reset here would change which slot a resumed sweep
    /// visits first.
    pub fn set_current_tick_time(&mut self, now: i64) {
        self.current_tick = self
            .current_tick
            .max((now - self.start_time) >> self.resolution_bits_to_shift);
    }

    /// `scheduleTimer(deadline)`: place it in the next occurrence of the tick
    /// its deadline names, and return the timer id.
    ///
    /// A deadline in the past is clamped to the current tick, so it expires on
    /// the next poll.
    pub fn schedule_timer(&mut self, deadline: i64) -> i64 {
        let tick = self
            .current_tick
            .max((deadline - self.start_time) >> self.resolution_bits_to_shift);
        #[allow(clippy::cast_sign_loss)] // a tick is never negative here: it is clamped
        let tick_index = (tick as usize) & self.tick_mask;
        let base = tick_index << self.allocation_bits_to_shift;

        for slot in 0..self.tick_allocation {
            let index = base + slot;
            if self.wheel[index] == NULL_DEADLINE {
                self.wheel[index] = deadline;
                self.timer_count += 1;
                return timer_id_for_slot(tick_index, slot);
            }
        }

        self.increase_capacity(deadline, tick_index)
    }

    /// `cancelTimer(timerId)`.
    pub fn cancel_timer(&mut self, timer_id: i64) -> bool {
        let index = self.wheel_index(timer_id);
        if self.wheel[index] == NULL_DEADLINE {
            false
        } else {
            self.wheel[index] = NULL_DEADLINE;
            self.timer_count -= 1;
            true
        }
    }

    /// `deadline(timerId)`, or [`NULL_DEADLINE`] for an id whose slot is out of
    /// range.
    pub fn deadline(&self, timer_id: i64) -> i64 {
        let tick_index = tick_for_timer_id(timer_id);
        let slot = index_in_tick_array(timer_id);
        if tick_index >= self.ticks_per_wheel || slot >= self.tick_allocation {
            return NULL_DEADLINE;
        }
        self.wheel[(tick_index << self.allocation_bits_to_shift) + slot]
    }

    /// `clear()`.
    pub fn clear(&mut self) {
        self.wheel.fill(NULL_DEADLINE);
        self.timer_count = 0;
        self.current_tick = 0;
        self.poll_index = 0;
    }

    /// `poll(now, handler, limit)`.
    ///
    /// Scans the current tick's block from `poll_index`, expiring every slot
    /// whose deadline is at or before `now`. A handler answering `false` puts
    /// the deadline back and stops the pass. When the pass ends with the tick's
    /// time behind `now`, the tick advances by one and `poll_index` resets —
    /// this is where the wheel advances, and it follows the bytecode, not an
    /// intuition about "advance then expire".
    pub fn poll<H: TimerHandler>(&mut self, now: i64, handler: &mut H, limit: usize) -> usize {
        let mut expired = 0;

        if self.timer_count > 0 {
            #[allow(clippy::cast_sign_loss)]
            let tick_index = (self.current_tick as usize) & self.tick_mask;
            let tick_allocation = self.tick_allocation;
            let mut i = 0;

            while i < tick_allocation && expired < limit {
                let wheel_index = (tick_index << self.allocation_bits_to_shift) + self.poll_index;
                let deadline = self.wheel[wheel_index];

                if now >= deadline {
                    self.wheel[wheel_index] = NULL_DEADLINE;
                    self.timer_count -= 1;
                    expired += 1;

                    let timer_id = timer_id_for_slot(tick_index, self.poll_index);
                    if !handler.on_timer_event(timer_id) {
                        self.wheel[wheel_index] = deadline;
                        self.timer_count += 1;
                        expired -= 1;
                        return expired;
                    }
                }

                self.poll_index = if self.poll_index + 1 < tick_allocation {
                    self.poll_index + 1
                } else {
                    0
                };
                i += 1;
            }

            if expired < limit && now >= self.current_tick_time() {
                self.current_tick += 1;
                self.poll_index = 0;
            } else if self.poll_index >= tick_allocation {
                self.poll_index = 0;
            }
        } else if now >= self.current_tick_time() {
            self.current_tick += 1;
            self.poll_index = 0;
        }

        expired
    }

    fn increase_capacity(&mut self, deadline: i64, tick_index: usize) -> i64 {
        let old_allocation = self.tick_allocation;
        let new_allocation = old_allocation * 2;
        let mut new_wheel = vec![NULL_DEADLINE; self.ticks_per_wheel * new_allocation];

        for tick in 0..self.ticks_per_wheel {
            let old_base = tick << self.allocation_bits_to_shift;
            let new_base = tick << new_allocation.trailing_zeros();
            new_wheel[new_base..new_base + old_allocation]
                .copy_from_slice(&self.wheel[old_base..old_base + old_allocation]);
        }

        self.wheel = new_wheel;
        self.tick_allocation = new_allocation;
        self.allocation_bits_to_shift = new_allocation.trailing_zeros();

        let index = (tick_index << self.allocation_bits_to_shift) + old_allocation;
        self.wheel[index] = deadline;
        self.timer_count += 1;
        timer_id_for_slot(tick_index, old_allocation)
    }

    fn wheel_index(&self, timer_id: i64) -> usize {
        (tick_for_timer_id(timer_id) << self.allocation_bits_to_shift)
            + index_in_tick_array(timer_id)
    }
}

/// `timerIdForSlot(tick, index)`.
const fn timer_id_for_slot(tick: usize, index: usize) -> i64 {
    ((tick as i64) << 32) | (index as i64)
}

/// `tickForTimerId(id)`.
const fn tick_for_timer_id(timer_id: i64) -> usize {
    ((timer_id >> 32) & 0xFFFF_FFFF) as usize
}

/// `indexInTickArray(id)`.
const fn index_in_tick_array(timer_id: i64) -> usize {
    (timer_id as i32) as usize
}

/// `WheelTimerService`: the wheel plus the two correlation maps.
pub struct WheelTimerService {
    wheel: DeadlineTimerWheel,
    /// `timerIdByCorrelationIdMap` — `Long2LongHashMap(Long.MAX_VALUE)`. Its
    /// **entry order** is the snapshot order.
    timer_id_by_correlation_id: Long2LongHashMap,
    /// `correlationIdByTimerIdMap`.
    correlation_id_by_timer_id: Long2LongHashMap,
    handler: Box<dyn TimerHandler>,
    abort: bool,
}

impl WheelTimerService {
    /// `new WheelTimerService(handler, timeUnit, startTime, tickResolution,
    /// ticksPerWheel)`.
    ///
    /// `time_unit` is kept for the signature the reference has; every tick and
    /// deadline here is already in that unit.
    pub fn new(
        handler: Box<dyn TimerHandler>,
        _time_unit: ClusterTimeUnit,
        start_time: i64,
        tick_resolution: i64,
        ticks_per_wheel: usize,
    ) -> Self {
        Self {
            wheel: DeadlineTimerWheel::new(start_time, tick_resolution, ticks_per_wheel),
            timer_id_by_correlation_id: Long2LongHashMap::new(NULL_DEADLINE),
            correlation_id_by_timer_id: Long2LongHashMap::new(NULL_DEADLINE),
            handler,
            abort: false,
        }
    }

    /// The wheel, for tests that need its tick state.
    pub const fn wheel(&self) -> &DeadlineTimerWheel {
        &self.wheel
    }

    /// The wheel's timer id for a correlation id, or `None` when unscheduled.
    pub fn timer_id_for_correlation_id(&self, correlation_id: i64) -> Option<i64> {
        let timer_id = self.timer_id_by_correlation_id.get(correlation_id);
        (timer_id != NULL_DEADLINE).then_some(timer_id)
    }
}

impl TimerService for WheelTimerService {
    /// `WheelTimerService.poll(now)` (`:57-74`).
    ///
    /// The outer loop can make the total exceed `POLL_LIMIT` (19 then 20), which
    /// is the reference's own behaviour and the reason the wheel's expiry count
    /// differs from the heap's.
    fn poll(&mut self, now: i64) -> usize {
        let mut expired = 0;
        self.abort = false;

        loop {
            let Self {
                wheel,
                timer_id_by_correlation_id,
                correlation_id_by_timer_id,
                handler,
                abort,
            } = self;

            let mut on_expiry = |timer_id: i64| -> bool {
                let correlation_id = correlation_id_by_timer_id.get(timer_id);
                if handler.on_timer_event(correlation_id) {
                    correlation_id_by_timer_id.remove(timer_id);
                    timer_id_by_correlation_id.remove(correlation_id);
                    true
                } else {
                    *abort = true;
                    false
                }
            };

            expired += wheel.poll(now, &mut on_expiry, POLL_LIMIT);

            if *abort || !(expired < POLL_LIMIT && wheel.current_tick_time() < now) {
                break;
            }
        }

        expired
    }

    /// `scheduleTimerForCorrelationId` (`:95-102`): cancel first, then schedule,
    /// so a reschedule is a remove-and-reinsert in the wheel.
    fn schedule_timer_for_correlation_id(&mut self, correlation_id: i64, deadline: i64) {
        self.cancel_timer_by_correlation_id(correlation_id);
        let timer_id = self.wheel.schedule_timer(deadline);
        self.timer_id_by_correlation_id
            .put(correlation_id, timer_id);
        self.correlation_id_by_timer_id
            .put(timer_id, correlation_id);
    }

    /// `cancelTimerByCorrelationId` (`:104-116`).
    fn cancel_timer_by_correlation_id(&mut self, correlation_id: i64) -> bool {
        let timer_id = self.timer_id_by_correlation_id.remove(correlation_id);
        if timer_id != NULL_DEADLINE {
            self.wheel.cancel_timer(timer_id);
            self.correlation_id_by_timer_id.remove(timer_id);
            true
        } else {
            false
        }
    }

    /// `snapshot` (`:118-131`): the **entry-iterator** order of
    /// `timerIdByCorrelationIdMap`, with each deadline read back out of the
    /// wheel.
    fn snapshot(&self, taker: &mut dyn TimerSnapshotTaker) {
        let wheel = &self.wheel;
        self.timer_id_by_correlation_id
            .for_each_entry(|correlation_id, timer_id| {
                taker.snapshot_timer(correlation_id, wheel.deadline(timer_id));
            });
    }

    /// `currentTime` (`:133-136`).
    fn current_time(&mut self, now: i64) {
        self.wheel.set_current_tick_time(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_timer_scheduled_for_the_past_is_clamped_to_the_current_tick() {
        let mut wheel = DeadlineTimerWheel::new(0, 8, 128);
        let timer_id = wheel.schedule_timer(-1);
        assert_eq!(0, tick_for_timer_id(timer_id));
        assert_eq!(-1, wheel.deadline(timer_id));
    }

    #[test]
    fn a_full_tick_block_grows_the_allocation_and_keeps_the_old_slots() {
        let mut wheel = DeadlineTimerWheel::new(0, 8, 128);
        let mut ids = Vec::new();
        for i in 0..INITIAL_TICK_ALLOCATION {
            let id = wheel.schedule_timer(100);
            assert_eq!(i, index_in_tick_array(id));
            ids.push(id);
        }
        let grown = wheel.schedule_timer(100);
        assert_eq!(INITIAL_TICK_ALLOCATION, index_in_tick_array(grown));
        assert_eq!(INITIAL_TICK_ALLOCATION * 2, wheel.tick_allocation);
        for id in ids.iter().copied().chain(std::iter::once(grown)) {
            assert_eq!(100, wheel.deadline(id));
        }
    }

    #[test]
    fn the_service_poll_walks_the_ticks_in_order_within_one_call() {
        let expired = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let recorder = std::rc::Rc::clone(&expired);
        let mut service = WheelTimerService::new(
            Box::new(move |correlation_id: i64| {
                recorder.borrow_mut().push(correlation_id);
                true
            }),
            ClusterTimeUnit::MILLIS,
            0,
            8,
            128,
        );
        service.schedule_timer_for_correlation_id(1, 10);
        service.schedule_timer_for_correlation_id(2, 10);
        service.schedule_timer_for_correlation_id(3, 20);

        assert_eq!(3, service.poll(25));
        assert_eq!(vec![1, 2, 3], *expired.borrow());
        assert_eq!(0, service.poll(25));
    }

    #[test]
    fn a_reschedule_is_a_remove_and_reinsert() {
        let mut service =
            WheelTimerService::new(Box::new(|_: i64| true), ClusterTimeUnit::MILLIS, 0, 8, 128);
        service.schedule_timer_for_correlation_id(7, 100);
        let first = service.timer_id_by_correlation_id.get(7);
        service.schedule_timer_for_correlation_id(7, 100);
        let second = service.timer_id_by_correlation_id.get(7);
        // With the same deadline the freed slot is the one the next schedule
        // finds, so the id comes back the same; the count is still one.
        assert_eq!(first, second);
        assert_eq!(1, service.timer_id_by_correlation_id.size());
        assert_eq!(1, service.wheel.timer_count());

        // A deadline in a different tick names a different slot.
        service.schedule_timer_for_correlation_id(7, 200);
        assert_ne!(first, service.timer_id_by_correlation_id.get(7));
        assert_eq!(1, service.wheel.timer_count());
    }
}
