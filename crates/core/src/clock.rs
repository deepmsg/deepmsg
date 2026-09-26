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

use std::time::{SystemTime, UNIX_EPOCH};

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
