//! Timers (`TimerService.java`), with the reference's two implementations.
//!
//! The interface is small: poll for expired timers, schedule and cancel by
//! correlation id, write the scheduled timers into a snapshot, and move the
//! clock forward. What makes it a byte contract is **which order the snapshot
//! is written in** — the wheel walks an Agrona `Long2LongHashMap` with its
//! `entrySet().iterator()`, the heap walks its own array — so the two
//! implementations write different bytes for the same set of timers (plan
//! §7.4). `tests/timer_snapshot_order.rs` asserts exactly that.

pub mod priority_heap;
pub mod wheel;

use std::time::Duration;

use deepmsg_codec_cluster::cluster_time_unit::ClusterTimeUnit;

/// `TimerService.POLL_LIMIT` (`TimerService.java:57`): the most timers one poll
/// may expire.
pub const POLL_LIMIT: usize = 20;

/// `aeron.cluster.timer.service.supplier` (`ConsensusModule.java:935`).
pub const TIMER_SERVICE_SUPPLIER_PROP_NAME: &str = "aeron.cluster.timer.service.supplier";

/// `TIMER_SERVICE_SUPPLIER_WHEEL` (`ConsensusModule.java:941`).
pub const TIMER_SERVICE_SUPPLIER_WHEEL: &str = "io.aeron.cluster.WheelTimerServiceSupplier";

/// `TIMER_SERVICE_SUPPLIER_PRIORITY_HEAP` (`ConsensusModule.java:947-948`).
pub const TIMER_SERVICE_SUPPLIER_PRIORITY_HEAP: &str =
    "io.aeron.cluster.PriorityHeapTimerServiceSupplier";

/// `TIMER_SERVICE_SUPPLIER_DEFAULT` (`ConsensusModule.java:954`).
pub const TIMER_SERVICE_SUPPLIER_DEFAULT: &str = TIMER_SERVICE_SUPPLIER_WHEEL;

/// `aeron.cluster.wheel.tick.resolution` (`ConsensusModule.java:892`).
pub const WHEEL_TICK_RESOLUTION_PROP_NAME: &str = "aeron.cluster.wheel.tick.resolution";

/// `WHEEL_TICK_RESOLUTION_DEFAULT_NS` (`ConsensusModule.java:898`).
pub const WHEEL_TICK_RESOLUTION_DEFAULT_NS: i64 = 8_000_000;

/// `aeron.cluster.ticks.per.wheel` (`ConsensusModule.java:905`).
pub const TICKS_PER_WHEEL_PROP_NAME: &str = "aeron.cluster.ticks.per.wheel";

/// `TICKS_PER_WHEEL_DEFAULT` (`ConsensusModule.java:912`).
pub const TICKS_PER_WHEEL_DEFAULT: i32 = 128;

/// `TimerService.TimerHandler`: answer whether the expiry was handled.
///
/// `false` leaves the timer where it is — the reference's way of saying "the
/// log append failed, try again next poll".
pub trait TimerHandler {
    /// One expired timer.
    fn on_timer_event(&mut self, correlation_id: i64) -> bool;
}

impl<F> TimerHandler for F
where
    F: FnMut(i64) -> bool,
{
    fn on_timer_event(&mut self, correlation_id: i64) -> bool {
        self(correlation_id)
    }
}

/// `TimerService.TimerSnapshotTaker`: one scheduled timer, in the order the
/// implementation owns.
pub trait TimerSnapshotTaker {
    /// Write one timer record.
    fn snapshot_timer(&mut self, correlation_id: i64, deadline: i64);
}

impl<F> TimerSnapshotTaker for F
where
    F: FnMut(i64, i64),
{
    fn snapshot_timer(&mut self, correlation_id: i64, deadline: i64) {
        self(correlation_id, deadline);
    }
}

/// `TimerService.java`.
pub trait TimerService {
    /// Expire everything due at `now`, up to `POLL_LIMIT`.
    fn poll(&mut self, now: i64) -> usize;

    /// Schedule `correlation_id` for `deadline`; a second schedule for the same
    /// id replaces the first.
    fn schedule_timer_for_correlation_id(&mut self, correlation_id: i64, deadline: i64);

    /// Cancel by correlation id; `false` when it was not scheduled.
    fn cancel_timer_by_correlation_id(&mut self, correlation_id: i64) -> bool;

    /// Write every scheduled timer, in this implementation's order.
    fn snapshot(&self, taker: &mut dyn TimerSnapshotTaker);

    /// Move the clock forward (a no-op for the heap).
    fn current_time(&mut self, now: i64);
}

/// What a supplier name asks for, or why this build cannot make it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimerSupplierError {
    /// A name that is neither of the reference's two.
    Unknown(String),
}

impl std::fmt::Display for TimerSupplierError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown(name) => write!(
                f,
                "invalid TimerServiceSupplier: {name} (this build knows \
                 {TIMER_SERVICE_SUPPLIER_WHEEL} and {TIMER_SERVICE_SUPPLIER_PRIORITY_HEAP})"
            ),
        }
    }
}

impl std::error::Error for TimerSupplierError {}

/// Which implementation a supplier name selects
/// (`ConsensusModule.java:4589-4607`).
///
/// The reference builds the object here; this returns the name of the shape so
/// the agent can build it with its own handler.
///
/// # Errors
///
/// [`TimerSupplierError::Unknown`] for any other name — the reference's own
/// `ClusterException("invalid TimerServiceSupplier: …")`.
pub fn timer_service_kind(name: &str) -> Result<TimerServiceKind, TimerSupplierError> {
    match name {
        TIMER_SERVICE_SUPPLIER_WHEEL => Ok(TimerServiceKind::Wheel),
        TIMER_SERVICE_SUPPLIER_PRIORITY_HEAP => Ok(TimerServiceKind::PriorityHeap),
        other => Err(TimerSupplierError::Unknown(other.to_owned())),
    }
}

/// The two shapes a supplier name can name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerServiceKind {
    /// `WheelTimerServiceSupplier` (the default).
    Wheel,
    /// `PriorityHeapTimerServiceSupplier`.
    PriorityHeap,
}

/// `WheelTimerServiceSupplier`'s tick derivation
/// (`ConsensusModule.java:4594-4600`): the configured resolution, converted
/// into the clock's own unit, rounded up to a power of two.
///
/// With the millisecond clock the default is `nextPow2(8) = 8` ms; with a
/// nanosecond clock it would be `nextPow2(8_000_000) = 8_388_608` ns. Copying
/// "8 ms" as a constant is wrong from the first tick on any other clock.
pub fn wheel_tick_resolution(tick_resolution_ns: i64, time_unit: ClusterTimeUnit) -> i64 {
    let converted = convert_ns_to_unit(tick_resolution_ns, time_unit);
    let rounded = deepmsg_cluster_common::collections::find_next_positive_power_of_two(
        usize::try_from(converted).unwrap_or(1),
    );
    i64::try_from(rounded).unwrap_or(i64::MAX)
}

/// `TimeUnit.convert(ns, NANOSECONDS)` for the two cluster units.
pub fn convert_ns_to_unit(value_ns: i64, time_unit: ClusterTimeUnit) -> i64 {
    match time_unit {
        ClusterTimeUnit::MILLIS => value_ns / 1_000_000,
        ClusterTimeUnit::MICROS => value_ns / 1_000,
        ClusterTimeUnit::NANOS | ClusterTimeUnit::NullVal => value_ns,
    }
}

/// `ClusterClock.timeUnit().convert(duration, NANOSECONDS)`.
pub fn duration_ns_to_unit(duration: Duration, time_unit: ClusterTimeUnit) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
        / match time_unit {
            ClusterTimeUnit::MILLIS => 1_000_000,
            ClusterTimeUnit::MICROS => 1_000,
            ClusterTimeUnit::NANOS | ClusterTimeUnit::NullVal => 1,
        }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_tick_is_eight_milliseconds_for_the_millisecond_clock() {
        assert_eq!(
            8,
            wheel_tick_resolution(WHEEL_TICK_RESOLUTION_DEFAULT_NS, ClusterTimeUnit::MILLIS)
        );
    }

    #[test]
    fn a_nanosecond_clock_gets_the_next_power_of_two_above_eight_million() {
        assert_eq!(
            8_388_608,
            wheel_tick_resolution(WHEEL_TICK_RESOLUTION_DEFAULT_NS, ClusterTimeUnit::NANOS)
        );
    }

    #[test]
    fn an_unknown_supplier_name_is_refused_with_the_reference_words() {
        let error = timer_service_kind("com.example.Timers").unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("invalid TimerServiceSupplier:")
        );
        assert_eq!(
            Ok(TimerServiceKind::Wheel),
            timer_service_kind(TIMER_SERVICE_SUPPLIER_DEFAULT)
        );
        assert_eq!(
            Ok(TimerServiceKind::PriorityHeap),
            timer_service_kind(TIMER_SERVICE_SUPPLIER_PRIORITY_HEAP)
        );
    }
}
