//! How a driver agent waits when it has nothing to do.
//!
//! One strategy, because it is the reference's default for every agent and
//! because the choice is load-bearing: a control plane that parks for a
//! millisecond after every quiet cycle is a control plane that answers a
//! command up to a millisecond late. Backoff spins first, yields next, and only
//! then parks, doubling the park each time it finds nothing
//! (`aeron-client/src/main/c/aeron_agent.c:112-163`, with the parameters from
//! `aeron_agent.h:43-46`).
//!
//! It lives in the library rather than in the binary because the agents that
//! need it — sender, receiver, name resolver — are library types, and because a
//! driver embedded in another process has the same problem the standalone one
//! has.

use std::time::Duration;

/// The idle strategy a driver agent uses when it has nothing to do.
///
/// The default for every agent of the reference driver, and the reason it is
/// this one rather than a sleep: a control plane that parks for a millisecond
/// after every quiet cycle is a control plane that answers a command up to a
/// millisecond late. Backoff spins first, yields next, and only then parks,
/// doubling the park each time it finds nothing
/// (`aeron-client/src/main/c/aeron_agent.c:112-163`, with the parameters from
/// `aeron_agent.h:43-46`).
pub struct Backoff {
    spins: u32,
    yields: u32,
    park_ns: u64,
}

impl Backoff {
    /// `AERON_IDLE_STRATEGY_BACKOFF_MAX_SPINS` (`aeron_agent.h:43`).
    const MAX_SPINS: u32 = 10;
    /// `AERON_IDLE_STRATEGY_BACKOFF_MAX_YIELDS` (`:44`).
    const MAX_YIELDS: u32 = 20;
    /// `AERON_IDLE_STRATEGY_BACKOFF_MIN_PARK_PERIOD_NS` (`:45`).
    const MIN_PARK_NS: u64 = 1_000;
    /// `AERON_IDLE_STRATEGY_BACKOFF_MAX_PARK_PERIOD_NS` (`:46`).
    const MAX_PARK_NS: u64 = 1_000_000;

    /// A fresh strategy: no spins, no yields, the shortest park.
    ///
    /// `Default` as well, for the callers that want one without naming the
    /// strategy — which is every agent the reference starts without an explicit
    /// idle strategy set.
    pub const fn new() -> Self {
        Self {
            spins: 0,
            yields: 0,
            park_ns: Self::MIN_PARK_NS,
        }
    }

    /// Idle after a cycle that did `work_count` units of work.
    /// Idle after a cycle that did `work_count` units of work.
    pub fn idle(&mut self, work_count: usize) {
        if 0 != work_count {
            // Something happened, so the next quiet cycle starts over rather
            // than inheriting a park period that was growing.
            *self = Self::new();
            return;
        }

        if self.spins < Self::MAX_SPINS {
            self.spins += 1;
            std::hint::spin_loop();
        } else if self.yields < Self::MAX_YIELDS {
            self.yields += 1;
            std::thread::yield_now();
        } else {
            std::thread::sleep(Duration::from_nanos(self.park_ns));
            self.park_ns = (self.park_ns * 2).min(Self::MAX_PARK_NS);
        }
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_resets_the_backoff() {
        let mut idle = Backoff::new();

        for _ in 0..(Backoff::MAX_SPINS + Backoff::MAX_YIELDS + 4) {
            idle.idle(0);
        }
        assert!(idle.park_ns > Backoff::MIN_PARK_NS, "it has parked by now");

        idle.idle(1);

        assert_eq!(0, idle.spins);
        assert_eq!(0, idle.yields);
        assert_eq!(Backoff::MIN_PARK_NS, idle.park_ns);
    }

    #[test]
    fn the_park_period_doubles_up_to_a_ceiling() {
        let mut idle = Backoff::new();
        idle.spins = Backoff::MAX_SPINS;
        idle.yields = Backoff::MAX_YIELDS;

        let mut periods = Vec::new();
        for _ in 0..4 {
            idle.idle(0);
            periods.push(idle.park_ns);
        }

        assert_eq!(
            vec![
                Backoff::MIN_PARK_NS * 2,
                Backoff::MIN_PARK_NS * 4,
                Backoff::MIN_PARK_NS * 8,
                Backoff::MIN_PARK_NS * 16
            ],
            periods
        );

        for _ in 0..40 {
            idle.idle(0);
        }
        assert_eq!(Backoff::MAX_PARK_NS, idle.park_ns, "and stops there");
    }

    #[test]
    fn spinning_comes_before_yielding_comes_before_parking() {
        let mut idle = Backoff::new();

        for expected in 1..=Backoff::MAX_SPINS {
            idle.idle(0);
            assert_eq!(expected, idle.spins);
            assert_eq!(0, idle.yields);
        }

        idle.idle(0);
        assert_eq!(Backoff::MAX_SPINS, idle.spins);
        assert_eq!(1, idle.yields, "spins are exhausted, so it yields");

        for expected in 2..=Backoff::MAX_YIELDS {
            idle.idle(0);
            assert_eq!(expected, idle.yields, "still yielding");
            assert_eq!(Backoff::MIN_PARK_NS, idle.park_ns, "and not parking yet");
        }

        idle.idle(0);
        assert_eq!(Backoff::MAX_YIELDS, idle.yields);
        assert_eq!(
            Backoff::MIN_PARK_NS * 2,
            idle.park_ns,
            "yields exhausted, so it parks — and the period doubles"
        );
    }
}
