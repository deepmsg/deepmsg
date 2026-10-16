//! How an archive agent waits when it has nothing to do (P2-9a).
//!
//! The reference names one strategy per agent, and three properties can name
//! them (`Archive.java:397-412`):
//!
//! | property | who idles with it |
//! |---|---|
//! | `aeron.archive.idle.strategy` (`:400`) | the conductor, and the two below when they are unset |
//! | `aeron.archive.recorder.idle.strategy` (`:406`) | the recorder |
//! | `aeron.archive.replayer.idle.strategy` (`:412`) | the replayer |
//!
//! The default is `org.agrona.concurrent.BackoffIdleStrategy` (`:420`), and the
//! fallback is at the **supplier** rather than at the name: when a recorder
//! property is unset the context hands out the archive's own supplier
//! (`:1423-1437`), so an archive that names one strategy gets that one
//! everywhere and one that names three gets three.
//!
//! # Why the name, and not a strategy
//!
//! `Context.idleStrategy()` is `idleStrategySupplier.get()` — a **new** strategy
//! per call (`:2440-2443`), because a strategy carries state: how many times it
//! has spun, how long it is parking for. Two agents sharing one `Backoff` would
//! share that state, and the second agent's first quiet pass would inherit a
//! park period the first one grew. So what a configuration holds is the *name*,
//! and [`IdleStrategy::strategy`] is the supplier: one fresh strategy per agent
//! that asks.
//!
//! # Java is the authority here, and its table is not the C one
//!
//! The C tree ships no archive server, so — per `CLAUDE.md`'s rule about which
//! tree is authoritative where both exist — the Java side decides what a name
//! means. `Archive.Configuration.idleStrategySupplier` resolves through
//! `io.aeron.driver.Configuration.agentIdleStrategy` (`:2098-2143`), whose table
//! is the Agrona aliases rather than the C loader's names; the two disagree
//! about `sleep-ns`'s default and about whether `sleep-ms` exists at all, which
//! is why [`deepmsg_driver::idle::Strategy::by_agrona_name`] is a second table
//! beside [`deepmsg_driver::idle::Strategy::by_name`] and not a second name for
//! the same one.
//!
//! # What this build refuses that the reference does not
//!
//! The Java switch's `default ->` arm loads the named class by reflection
//! (`:2134-2142`), so a deployment with its own `IdleStrategy` on the classpath
//! gets it. This build cannot load a Java class, so a name that is not one of
//! the six is **refused by name** instead. A typo fails in both — the reference
//! rethrows the `ClassNotFoundException` out of `conclude` — so the gap is
//! exactly "a custom strategy class", and it fails loudly rather than idling
//! with something the deployment did not ask for.
//!
//! `controllable` is refused for a reason of its own: its arm requires a
//! `StatusIndicator`, and an archive passes `null` for one (`Archive.java:1399`
//! into `Configuration.idleStrategySupplier`, forwarded at `:781`), so naming it
//! there is a `NullPointerException` rather than a strategy. See
//! [`deepmsg_driver::idle::Strategy::by_agrona_name`].

use deepmsg_driver::idle::Strategy;

#[cfg(test)]
use deepmsg_driver::idle::{Backoff, Sleeping, SleepingMillis};

/// One of the idle strategies a deployment can name for an archive agent.
///
/// The six `io.aeron.driver.Configuration.agentIdleStrategy` answers to, as a
/// value a configuration can hold and compare. It is the *name* rather than a
/// strategy because each agent needs one of its own — see the module note.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleStrategy {
    /// `noop`.
    Noop,
    /// `spin`.
    BusySpinning,
    /// `yield`.
    Yielding,
    /// `sleep-ns`.
    Sleeping,
    /// `sleep-ms`.
    SleepingMillis,
    /// `backoff`.
    Backoff,
}

impl IdleStrategy {
    /// `Archive.Configuration.DEFAULT_IDLE_STRATEGY`, which is set by **class
    /// name** (`Archive.java:420`) — and is the same value as the alias.
    pub const DEFAULT: Self = Self::Backoff;

    /// The strategy `name` asks for, or [`None`] for one this build has none
    /// for. See the module note for the two names the reference resolves and
    /// this does not.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        match Strategy::by_agrona_name(name)? {
            Strategy::Noop => Some(Self::Noop),
            Strategy::BusySpinning => Some(Self::BusySpinning),
            Strategy::Yielding => Some(Self::Yielding),
            Strategy::Sleeping(_) => Some(Self::Sleeping),
            Strategy::SleepingMillis(_) => Some(Self::SleepingMillis),
            Strategy::Backoff(_) => Some(Self::Backoff),
        }
    }

    /// A fresh strategy of this kind, which is what `ctx.idleStrategy()` is: the
    /// supplier's `get()`, not a shared instance.
    #[must_use]
    pub fn strategy(self) -> Strategy {
        match self {
            Self::Noop => Strategy::Noop,
            Self::BusySpinning => Strategy::BusySpinning,
            Self::Yielding => Strategy::Yielding,
            Self::Sleeping => Strategy::by_agrona_name("sleep-ns").expect("the alias"),
            Self::SleepingMillis => Strategy::by_agrona_name("sleep-ms").expect("the alias"),
            Self::Backoff => Strategy::by_agrona_name("backoff").expect("the alias"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The names a deployment may write, both spellings of each — the aliases
    /// and the class names the Java switch accepts beside them.
    #[test]
    fn every_name_the_reference_resolves_is_one_this_does() {
        for (name, expected) in [
            ("noop", IdleStrategy::Noop),
            ("org.agrona.concurrent.NoOpIdleStrategy", IdleStrategy::Noop),
            ("spin", IdleStrategy::BusySpinning),
            (
                "org.agrona.concurrent.BusySpinIdleStrategy",
                IdleStrategy::BusySpinning,
            ),
            ("yield", IdleStrategy::Yielding),
            (
                "org.agrona.concurrent.YieldingIdleStrategy",
                IdleStrategy::Yielding,
            ),
            ("sleep-ns", IdleStrategy::Sleeping),
            (
                "org.agrona.concurrent.SleepingIdleStrategy",
                IdleStrategy::Sleeping,
            ),
            ("sleep-ms", IdleStrategy::SleepingMillis),
            (
                "org.agrona.concurrent.SleepingMillisIdleStrategy",
                IdleStrategy::SleepingMillis,
            ),
            ("backoff", IdleStrategy::Backoff),
            (
                "org.agrona.concurrent.BackoffIdleStrategy",
                IdleStrategy::Backoff,
            ),
        ] {
            assert_eq!(Some(expected), IdleStrategy::parse(name), "{name}");
        }
    }

    /// The reference's default is written as a **class name** in
    /// `DEFAULT_IDLE_STRATEGY` (`Archive.java:420`), so it has to be one of the
    /// names that parses — an archive that set no property at all resolves
    /// through it.
    #[test]
    fn the_references_default_is_a_name_that_parses() {
        assert_eq!(
            Some(IdleStrategy::DEFAULT),
            IdleStrategy::parse("org.agrona.concurrent.BackoffIdleStrategy")
        );
    }

    /// The names this build has none for, and each one is a refusal rather than
    /// a silent fall back to the default.
    #[test]
    fn a_name_this_build_cannot_give_it_a_strategy_is_refused() {
        for name in [
            // The C loader's spelling of `sleep-ns`, which is not an Agrona alias.
            "sleeping",
            // Wants a `StatusIndicator` the archive does not give it.
            "controllable",
            // A deployment's own class: the reference loads it, this cannot.
            "com.example.MyIdleStrategy",
            // And a typo, which the reference refuses too.
            "backof",
            "",
        ] {
            assert_eq!(None, IdleStrategy::parse(name), "{name}");
        }
    }

    /// Each name gives the strategy of that kind, and not merely one that idles
    /// — a mapping that crossed two arms would idle every agent on the wrong
    /// terms.
    ///
    /// Compared by variant rather than by value: a strategy carries state
    /// (`Backoff`'s park period, for one), so two of the same kind are not equal
    /// and the *kind* is the question.
    #[test]
    fn every_name_gives_the_strategy_of_that_kind() {
        for (name, expected) in [
            (IdleStrategy::Noop, Strategy::Noop),
            (IdleStrategy::BusySpinning, Strategy::BusySpinning),
            (IdleStrategy::Yielding, Strategy::Yielding),
            (IdleStrategy::Sleeping, Strategy::Sleeping(Sleeping::new(0))),
            (
                IdleStrategy::SleepingMillis,
                Strategy::SleepingMillis(SleepingMillis::new(0)),
            ),
            (IdleStrategy::Backoff, Strategy::Backoff(Backoff::new())),
        ] {
            assert_eq!(
                std::mem::discriminant(&expected),
                std::mem::discriminant(&name.strategy()),
                "{name:?}"
            );
        }
    }
}
