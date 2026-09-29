//! The clock, and a cache of it.
//!
//! Two readings with different jobs. The *nanosecond* one drives deadlines —
//! the conductor compares it against a deadline every duty cycle. The
//! *epoch millisecond* one is what goes into every timestamp another process
//! reads: counter values, the CnC metadata's start time, the ring's consumer
//! heartbeat. The reference keeps both, and re-derives the second from the
//! first at most once per millisecond
//! (`aeron-driver/src/main/c/aeron_driver_conductor.c:3276-3280`, against
//! `AERON_DRIVER_CONDUCTOR_CLOCK_UPDATE_INTERNAL_NS` = 1 ms, declared at
//! `aeron_driver_conductor.h:38`), because dividing on every iteration to get
//! a value that changes a thousand times less often is work for nothing.
//!
//! [`CachedClock`] is that cache. It deliberately does not decide *when* to
//! refresh: the caller knows its own duty cycle, and a clock that refreshed
//! itself on a timer would be a second scheduler inside the first one.

use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Nanoseconds since the Unix epoch.
///
/// Saturates rather than panicking. A machine whose clock is before 1970 is
/// broken in a way no driver can fix, and aborting a duty cycle over it would
/// turn a bad timestamp into a dead process.
pub fn epoch_nano_time() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_nanos()).ok())
        .unwrap_or(i64::MAX)
}

/// The reading [`monotonic_nano_time`] counts from.
static MONOTONIC_BASE: OnceLock<Instant> = OnceLock::new();

/// Nanoseconds from a clock that only goes forwards, for measuring elapsed time
/// inside the driver.
///
/// [`epoch_nano_time`] answers "what time is it", which is what a timestamp
/// another process will read has to mean. Nothing that *compares* two readings
/// may use it. An NTP step, a leap second or a hand-set clock moves every
/// deadline at once: backwards, and a timeout stops expiring — a liveness check
/// that never fires, a retransmit that never lingers out; forwards, and every
/// connection times out together, every heartbeat goes out in one pass.
///
/// The driver's own timing — liveness timeouts, NAK delays and linger, the
/// status-message and heartbeat cadences, retransmit delays — wants a clock
/// that cannot go backwards. That is what the reference's `nano_clock` is:
/// `aeron_nano_clock` (`aeron-client/src/main/c/util/aeron_clock.c:113-122`)
/// calls `aeron_clock_gettime_monotonic`, and it is the default
/// `context->nano_clock` the sender and receiver read
/// (`aeron_driver_sender.c:136`, `aeron_driver_receiver.c:128`).
///
/// The value is nanoseconds since the first call in this process, so it is a
/// duration and not a date: it is meaningless to anything outside this process,
/// and nothing here writes it to shared memory.
///
/// Saturates rather than panicking, like [`epoch_nano_time`].
pub fn monotonic_nano_time() -> i64 {
    let base = MONOTONIC_BASE.get_or_init(Instant::now);

    i64::try_from(base.elapsed().as_nanos()).unwrap_or(i64::MAX)
}

/// Milliseconds since the Unix epoch.
pub fn epoch_millis() -> i64 {
    epoch_nano_time() / 1_000_000
}

/// The epoch-millisecond value, refreshed when the caller says so.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CachedClock {
    epoch_ms: i64,
}

impl CachedClock {
    /// A clock that has not been read yet, and so reads as the epoch.
    pub const fn new() -> Self {
        Self { epoch_ms: 0 }
    }

    /// Take a reading and return the millisecond value it implies.
    ///
    /// Truncating division, which is what the reference does with a nanosecond
    /// reading too: a timestamp rounded *up* would be a millisecond in the
    /// future, and a client comparing it against its own clock would find the
    /// driver slightly younger than it is.
    pub const fn update(&mut self, now_ns: i64) -> i64 {
        self.epoch_ms = now_ns / 1_000_000;
        self.epoch_ms
    }

    /// The value from the most recent [`CachedClock::update`].
    pub const fn epoch_ms(&self) -> i64 {
        self.epoch_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cache_reports_what_it_was_given() {
        let mut clock = CachedClock::new();

        assert_eq!(0, clock.epoch_ms(), "an unread clock is the epoch");

        assert_eq!(1_700_000_000_000, clock.update(1_700_000_000_000_123_456));
        assert_eq!(1_700_000_000_000, clock.epoch_ms());
    }

    #[test]
    fn the_cache_truncates_rather_than_rounding() {
        let mut clock = CachedClock::new();

        clock.update(999_999);

        assert_eq!(
            0,
            clock.epoch_ms(),
            "a rounded value would be in the future"
        );
    }

    #[test]
    fn the_monotonic_clock_advances_and_never_goes_backwards() {
        let first = monotonic_nano_time();
        let second = monotonic_nano_time();

        assert!(first >= 0);
        assert!(second >= first, "{second} came after {first}");
    }

    #[test]
    fn the_system_clock_is_after_the_epoch_and_before_2100() {
        // The same plausibility window `tests/integration/cnc_fixture.rs`
        // applies to a captured driver's start timestamp: outside it, the
        // reading is a decoding mistake rather than a clock.
        let now = epoch_millis();

        assert!(
            (1_577_836_800_000..4_102_444_800_000).contains(&now),
            "{now}"
        );
        assert_eq!(now, epoch_nano_time() / 1_000_000);
    }
}
