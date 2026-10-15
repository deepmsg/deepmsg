//! How a driver agent waits when it has nothing to do.
//!
//! The reference picks one of six by name, per runner, from
//! `aeron.<slot>.idle.strategy` — `sleeping`/`sleep-ns`, `yield`, `spin`,
//! `noop`, `backoff` (`aeron-client/src/main/c/aeron_agent.c:286-294`, the
//! symbol table `aeron_idle_strategy_load` reads). Every one of them takes the
//! work count of the pass that just ran and does nothing at all when it is not
//! zero, which is what keeps a busy agent busy.
//!
//! [`Backoff`] is the default for five of the six runners and the reason the
//! choice is load-bearing at all: a control plane that parks for a millisecond
//! after every quiet cycle is a control plane that answers a command up to a
//! millisecond late. It spins first, yields next, and only then parks, doubling
//! the park each time it finds nothing
//! (`aeron-client/src/main/c/aeron_agent.c:112-163`, with the parameters from
//! `aeron_agent.h:43-46`). The sixth — the native resource agent — defaults to
//! `sleep-ns` (`aeron_driver_context.c:1148`): it maps files and resolves names,
//! and a thread that spins on a work queue nothing is filling is a core nobody
//! can use.
//!
//! It lives in the library rather than in the binary because the agents that
//! need it — sender, receiver, name resolver — are library types, and because a
//! driver embedded in another process has the same problem the standalone one
//! has.

use std::time::Duration;

use crate::config::duration_ns;

/// The idle strategy a driver agent uses when it has nothing to do.
///
/// The default for every agent of the reference driver, and the reason it is
/// this one rather than a sleep: a control plane that parks for a millisecond
/// after every quiet cycle is a control plane that answers a command up to a
/// millisecond late. Backoff spins first, yields next, and only then parks,
/// doubling the park each time it finds nothing
/// (`aeron-client/src/main/c/aeron_agent.c:112-163`, with the parameters from
/// `aeron_agent.h:43-46`).
#[derive(Debug)]
pub struct Backoff {
    /// `AERON_IDLE_STRATEGY_BACKOFF_MAX_SPINS` (`aeron_agent.h:43`).
    max_spins: u32,
    /// `AERON_IDLE_STRATEGY_BACKOFF_MAX_YIELDS` (`:44`).
    max_yields: u32,
    /// `AERON_IDLE_STRATEGY_BACKOFF_MIN_PARK_PERIOD_NS` (`:45`).
    min_park_ns: u64,
    /// `AERON_IDLE_STRATEGY_BACKOFF_MAX_PARK_PERIOD_NS` (`:46`).
    max_park_ns: u64,
    spins: u32,
    yields: u32,
    park_ns: u64,
}

impl Backoff {
    /// `AERON_IDLE_STRATEGY_BACKOFF_MAX_SPINS` (`aeron_agent.h:43`).
    pub const MAX_SPINS: u32 = 10;
    /// `AERON_IDLE_STRATEGY_BACKOFF_MAX_YIELDS` (`:44`).
    pub const MAX_YIELDS: u32 = 20;
    /// `AERON_IDLE_STRATEGY_BACKOFF_MIN_PARK_PERIOD_NS` (`:45`).
    pub const MIN_PARK_NS: u64 = 1_000;
    /// `AERON_IDLE_STRATEGY_BACKOFF_MAX_PARK_PERIOD_NS` (`:46`).
    pub const MAX_PARK_NS: u64 = 1_000_000;

    /// A fresh strategy: no spins, no yields, the shortest park.
    ///
    /// `Default` as well, for the callers that want one without naming the
    /// strategy — which is every agent the reference starts without an explicit
    /// idle strategy set.
    pub const fn new() -> Self {
        Self::with_limits(
            Self::MAX_SPINS,
            Self::MAX_YIELDS,
            Self::MIN_PARK_NS,
            Self::MAX_PARK_NS,
        )
    }

    /// The same, with the four numbers an init args string can replace
    /// (`aeron_idle_strategy_backoff_state_init`, `aeron_agent.c:165-183`).
    pub const fn with_limits(
        max_spins: u32,
        max_yields: u32,
        min_park_ns: u64,
        max_park_ns: u64,
    ) -> Self {
        Self {
            max_spins,
            max_yields,
            min_park_ns,
            max_park_ns,
            spins: 0,
            yields: 0,
            park_ns: min_park_ns,
        }
    }

    /// Idle after a cycle that did `work_count` units of work.
    pub fn idle(&mut self, work_count: usize) {
        if 0 != work_count {
            // Something happened, so the next quiet cycle starts over rather
            // than inheriting a park period that was growing.
            self.spins = 0;
            self.yields = 0;
            self.park_ns = self.min_park_ns;
            return;
        }

        if self.spins < self.max_spins {
            self.spins += 1;
            std::hint::spin_loop();
        } else if self.yields < self.max_yields {
            self.yields += 1;
            std::thread::yield_now();
        } else {
            std::thread::sleep(Duration::from_nanos(self.park_ns));
            self.park_ns = (self.park_ns.saturating_mul(2)).min(self.max_park_ns);
        }
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

/// `sleeping` / `sleep-ns`: sleep for a fixed period when there was no work.
///
/// The reference's own name for the strategy is the first of the two; the
/// second is the same implementation under the name its init args make sense
/// of (`aeron_agent.c:286-294`).
#[derive(Debug)]
pub struct Sleeping {
    duration_ns: u64,
}

impl Sleeping {
    /// What the strategy sleeps for when nothing named a duration: **one
    /// nanosecond** (`aeron_idle_strategy_sleeping_init_args`,
    /// `aeron_agent.c:56-59`, whose `NULL == init_args` arm is this).
    pub const DEFAULT_DURATION_NS: u64 = 1;

    /// What the **Java** table's `SleepingIdleStrategy()` sleeps for when
    /// nothing named a duration: 1000 ns, its `DEFAULT_SLEEP_PERIOD_NS`.
    ///
    /// Three orders of magnitude away from [`Sleeping::DEFAULT_DURATION_NS`],
    /// and that is the difference between the two tables rather than a slip: the
    /// C loader hands the strategy no arguments at all
    /// (`aeron_idle_strategy_sleeping_init_args`, `aeron_agent.c:47-66`) while
    /// Agrona's no-argument constructor carries its own default.
    pub const JAVA_DEFAULT_DURATION_NS: u64 = 1_000;

    /// A strategy that sleeps for `duration_ns` after a pass with no work.
    pub const fn new(duration_ns: u64) -> Self {
        Self { duration_ns }
    }

    /// Idle after a cycle that did `work_count` units of work.
    pub fn idle(&mut self, work_count: usize) {
        if 0 != work_count {
            return;
        }

        std::thread::sleep(Duration::from_nanos(self.duration_ns));
    }
}

/// `sleep-ms`: sleep for a fixed period measured in **milliseconds** when there
/// was no work (`org.agrona.concurrent.SleepingMillisIdleStrategy`).
///
/// The one strategy that has a name in the Java table and none in the C one:
/// `aeron_idle_strategy_load` has no `sleep-ms`
/// (`aeron-client/src/main/c/aeron_agent.c:286-294`), and the Java side has both
/// spellings (`io.aeron.driver.Configuration:2114-2120`). It is here rather than
/// in the crate that names it because it is one of the six, and a caller
/// resolving a Java-side name gets the same [`Strategy`] the C-side loader
/// gives.
#[derive(Debug)]
pub struct SleepingMillis {
    period_ms: u64,
}

impl SleepingMillis {
    /// What the strategy sleeps for when nothing named a period: **one
    /// millisecond** (`SleepingMillisIdleStrategy.DEFAULT_SLEEP_PERIOD_MS`,
    /// which is the no-argument constructor `agentIdleStrategy` calls).
    pub const DEFAULT_PERIOD_MS: u64 = 1;

    /// A strategy that sleeps for `period_ms` after a pass with no work.
    pub const fn new(period_ms: u64) -> Self {
        Self { period_ms }
    }

    /// Idle after a cycle that did `work_count` units of work.
    pub fn idle(&mut self, work_count: usize) {
        if 0 != work_count {
            return;
        }

        std::thread::sleep(Duration::from_millis(self.period_ms));
    }
}

/// Every strategy the reference can be configured with, by the names its own
/// table answers to (`aeron_agent.c:286-294`).
///
/// A name that is not in that table is `None`, and the reference answers it by
/// **refusing to start the driver** — its context calls the loader and jumps to
/// its error arm when the loader returns null
/// (`aeron_driver_context.c:1152-1158`), so a typo is not a strategy that
/// quietly becomes backoff.
///
/// # Two tables, and this type is in both
///
/// The C driver loads a name through `aeron_idle_strategy_load` and the Java
/// side resolves one through `io.aeron.driver.Configuration.agentIdleStrategy`
/// (`:2098-2143`), and the two lists are **not** the same list: the Java one has
/// aliases the C one has no spelling for (`sleep-ms`), accepts the six
/// strategies' full class names as well, and falls back to loading an arbitrary
/// class by reflection. [`Strategy::by_name`] is the C table and
/// [`Strategy::by_agrona_name`] is the Java one; a caller uses the table its own
/// reference reads, which for the media driver is the C one
/// (`docs/CLAUDE.md`: the C implementation is the authority where one exists)
/// and for the archive — which the C tree does not ship — the Java one.
#[derive(Debug)]
pub enum Strategy {
    /// `sleeping`, `sleep-ns`; Java calls the same one `SleepingIdleStrategy`.
    Sleeping(Sleeping),
    /// `yield`.
    Yielding,
    /// `spin`.
    BusySpinning,
    /// `noop`.
    Noop,
    /// `backoff`.
    Backoff(Backoff),
    /// `sleep-ms`, which only the Java table names.
    SleepingMillis(SleepingMillis),
}

impl Strategy {
    /// The strategy `name` asks for, with the init args its environment
    /// variable carried — or `None` for a name the reference's table does not
    /// have, and for init args it cannot parse.
    ///
    /// The three that take no arguments ignore theirs, as
    /// `aeron_idle_strategy_init_null` does (`aeron_agent.c:250-255`).
    pub fn by_name(name: &str, init_args: Option<&str>) -> Option<Self> {
        match name {
            "sleeping" | "sleep-ns" => {
                let duration_ns = match init_args {
                    None => Sleeping::DEFAULT_DURATION_NS,
                    Some(args) => u64::try_from(duration_ns(args)?).ok()?,
                };

                Some(Self::Sleeping(Sleeping::new(duration_ns)))
            }
            "yield" => Some(Self::Yielding),
            "spin" => Some(Self::BusySpinning),
            "noop" => Some(Self::Noop),
            "backoff" => Some(Self::Backoff(match init_args {
                None => Backoff::new(),
                Some(args) => backoff_limits(args)?,
            })),
            _ => None,
        }
    }

    /// The strategy a **Java-side** configuration names
    /// (`io.aeron.driver.Configuration.agentIdleStrategy`, `:2098-2143`).
    ///
    /// Two things about it differ from [`Strategy::by_name`], and both are the
    /// Java table's own doing rather than this build's:
    ///
    /// * they are the **Agrona aliases** — `sleep-ms` and `controllable` are
    ///   names the C loader has no arm for, and `sleep-ns` means something else
    ///   here. `SleepingIdleStrategy()` with no constructor arguments sleeps for
    ///   a **microsecond's worth of nanoseconds** (`DEFAULT_SLEEP_PERIOD_NS` is
    ///   1000), where the C strategy's `NULL == init_args` arm sleeps for one
    ///   nanosecond (`aeron_agent.c:56-59`);
    /// * the six strategies are also spelled by their full class names, which
    ///   the switch accepts beside the alias.
    ///
    /// The `default ->` arm there loads the named class by reflection, which a
    /// Rust build cannot do; a custom Java `IdleStrategy` is therefore `None`
    /// rather than a strategy this build does not have.
    ///
    /// `controllable` is `None` too, and for a subtler reason: its arm requires
    /// a `StatusIndicator`, and the archive's supplier is built with `null` for
    /// one (`Archive.java:1399` passes null into
    /// `Configuration.idleStrategySupplier`, which forwards it at `:781`), so a
    /// deployment that names it does not get a controllable strategy there — it
    /// gets a `NullPointerException` out of `conclude`.
    pub fn by_agrona_name(name: &str) -> Option<Self> {
        match name {
            "noop" | "org.agrona.concurrent.NoOpIdleStrategy" => Some(Self::Noop),
            "spin" | "org.agrona.concurrent.BusySpinIdleStrategy" => Some(Self::BusySpinning),
            "yield" | "org.agrona.concurrent.YieldingIdleStrategy" => Some(Self::Yielding),
            "sleep-ns" | "org.agrona.concurrent.SleepingIdleStrategy" => Some(Self::Sleeping(
                Sleeping::new(Sleeping::JAVA_DEFAULT_DURATION_NS),
            )),
            "sleep-ms" | "org.agrona.concurrent.SleepingMillisIdleStrategy" => Some(
                Self::SleepingMillis(SleepingMillis::new(SleepingMillis::DEFAULT_PERIOD_MS)),
            ),
            // The Java driver's four numbers (`Configuration.java:487-504`),
            // which are the same four the C strategy defaults to — so the two
            // tables agree here by value even though they are different lists.
            "backoff" | "org.agrona.concurrent.BackoffIdleStrategy" => {
                Some(Self::Backoff(Backoff::new()))
            }
            _ => None,
        }
    }

    /// Idle after a cycle that did `work_count` units of work.
    pub fn idle(&mut self, work_count: usize) {
        match self {
            Self::Sleeping(strategy) => strategy.idle(work_count),
            Self::SleepingMillis(strategy) => strategy.idle(work_count),
            // `sched_yield` (`aeron_idle_strategy_yielding_idle`,
            // `aeron_agent.c:68-76`).
            Self::Yielding => {
                if 0 == work_count {
                    std::thread::yield_now();
                }
            }
            // `proc_yield`, which on the platforms this build targets is the
            // CPU's own pause instruction rather than a trip through the
            // scheduler (`concurrent/aeron_thread.c:652-658`) — what
            // `spin_loop` emits.
            Self::BusySpinning => {
                if 0 == work_count {
                    std::hint::spin_loop();
                }
            }
            // `aeron_idle_strategy_noop_idle` is an empty function
            // (`aeron_agent.c:88-90`).
            Self::Noop => {}
            Self::Backoff(strategy) => strategy.idle(work_count),
        }
    }
}

/// The four numbers `backoff`'s init args carry: `spins-yields-minPark-maxPark`
/// (`aeron_idle_strategy_backoff_state_init_args`, `aeron_agent.c:186-248`).
///
/// The parks are durations — `1us`, `1ms`, or a bare count of nanoseconds —
/// and the other two are plain counts. The reference refuses anything else
/// rather than falling back to the defaults, and so does this.
fn backoff_limits(args: &str) -> Option<Backoff> {
    let mut parts = args.split('-');
    let spins = parts.next()?.parse::<u32>().ok()?;
    let yields = parts.next()?.parse::<u32>().ok()?;
    let min_park = u64::try_from(duration_ns(parts.next()?)?).ok()?;
    let max_park = u64::try_from(duration_ns(parts.next()?)?).ok()?;

    if parts.next().is_some() {
        return None;
    }

    Some(Backoff::with_limits(spins, yields, min_park, max_park))
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

    /// The names are the reference's own table
    /// (`aeron_agent.c:286-294`), `sleeping` and `sleep-ns` being one
    /// implementation under two names.
    #[test]
    fn every_name_the_reference_answers_to_is_one_it_answers_to() {
        for name in ["sleeping", "sleep-ns", "yield", "spin", "noop", "backoff"] {
            assert!(
                Strategy::by_name(name, None).is_some(),
                "{name} is in the reference's idle strategy table"
            );
        }

        for name in ["Sleeping", "busy-spin", "sleep", "", "backof"] {
            assert!(
                Strategy::by_name(name, None).is_none(),
                "{name} is not, and the reference refuses to start rather than \
                 treating an unknown name as the default"
            );
        }
    }

    /// `sleep-ns` sleeps for what its args say, and for one nanosecond when
    /// there are none (`aeron_idle_strategy_sleeping_init_args`,
    /// `aeron_agent.c:47-66`).
    #[test]
    fn sleeping_takes_a_duration_and_defaults_to_one_nanosecond() {
        assert_eq!(
            Sleeping::DEFAULT_DURATION_NS,
            match Strategy::by_name("sleep-ns", None) {
                Some(Strategy::Sleeping(sleeping)) => sleeping.duration_ns,
                other => panic!("sleep-ns is a sleeping strategy: {other:?}"),
            }
        );

        for (args, expected) in [("5us", 5_000), ("1ms", 1_000_000), ("250", 250)] {
            let strategy = Strategy::by_name("sleeping", Some(args));
            assert!(
                matches!(strategy, Some(Strategy::Sleeping(sleeping)) if sleeping.duration_ns == expected),
                "{args} is {expected} ns"
            );
        }

        assert!(Strategy::by_name("sleep-ns", Some("1 fortnight")).is_none());
    }

    /// The Java table (`Configuration.java:2098-2143`), which is a *different*
    /// list from the C one above — and the difference is the point of having it:
    /// a name resolution that used the wrong table would accept `sleeping` on a
    /// Java-side setting and refuse `sleep-ms`.
    #[test]
    fn the_java_table_is_a_different_list_with_the_same_six_strategies() {
        for (name, expected) in [
            ("noop", "Noop"),
            ("spin", "BusySpinning"),
            ("yield", "Yielding"),
            ("sleep-ns", "Sleeping(1000ns)"),
            ("sleep-ms", "SleepingMillis(1ms)"),
            ("backoff", "Backoff(10,20,1000ns,1ms)"),
        ] {
            let strategy = Strategy::by_agrona_name(name).unwrap_or_else(|| {
                panic!("{name} is one of the aliases the Java switch answers to")
            });

            assert_eq!(expected, describe(&strategy), "{name}");
        }

        // The six are also spelled by their full class names, which the switch
        // accepts beside the aliases.
        for (class_name, alias) in [
            ("org.agrona.concurrent.NoOpIdleStrategy", "noop"),
            ("org.agrona.concurrent.BusySpinIdleStrategy", "spin"),
            ("org.agrona.concurrent.YieldingIdleStrategy", "yield"),
            ("org.agrona.concurrent.SleepingIdleStrategy", "sleep-ns"),
            (
                "org.agrona.concurrent.SleepingMillisIdleStrategy",
                "sleep-ms",
            ),
            ("org.agrona.concurrent.BackoffIdleStrategy", "backoff"),
        ] {
            assert_eq!(
                describe(&Strategy::by_agrona_name(alias).expect("the alias")),
                describe(&Strategy::by_agrona_name(class_name).expect("the class name")),
                "{class_name} is {alias}"
            );
        }

        // `sleeping` is the C loader's spelling and not an Agrona alias, and
        // `controllable` is refused because its arm wants a `StatusIndicator`
        // the archive does not give it.
        for name in [
            "sleeping",
            "controllable",
            "org.agrona.concurrent.ControllableIdleStrategy",
        ] {
            assert!(
                Strategy::by_agrona_name(name).is_none(),
                "{name} is not a name this table gives a strategy to"
            );
        }
    }

    /// `sleep-ns` means 1000 ns in the Java table and 1 ns in the C one, and the
    /// two are read side by side: a caller that resolved a Java-side name
    /// through the C loader would idle for a thousandth of what it asked for.
    #[test]
    fn the_two_tables_disagree_about_what_sleep_ns_defaults_to() {
        let java = match Strategy::by_agrona_name("sleep-ns") {
            Some(Strategy::Sleeping(sleeping)) => sleeping.duration_ns,
            other => panic!("sleep-ns is a sleeping strategy: {other:?}"),
        };

        let c = match Strategy::by_name("sleep-ns", None) {
            Some(Strategy::Sleeping(sleeping)) => sleeping.duration_ns,
            other => panic!("sleep-ns is a sleeping strategy: {other:?}"),
        };

        assert_eq!(Sleeping::JAVA_DEFAULT_DURATION_NS, java);
        assert_eq!(Sleeping::DEFAULT_DURATION_NS, c);
        assert_ne!(java, c);
    }

    /// `work_count` is the whole of the condition for the added strategy too:
    /// a pass that did work does not sleep (`SleepingMillisIdleStrategy.idle`).
    #[test]
    fn sleeping_millis_does_not_sleep_after_work() {
        let mut idle = SleepingMillis::new(50);

        let began = std::time::Instant::now();
        idle.idle(1);
        assert!(
            began.elapsed() < Duration::from_millis(50),
            "a pass with work does not idle at all"
        );

        let began = std::time::Instant::now();
        idle.idle(0);
        assert!(began.elapsed() >= Duration::from_millis(50));
    }

    /// One strategy per line, for the table tests above.
    fn describe(strategy: &Strategy) -> String {
        match strategy {
            Strategy::Noop => "Noop".to_owned(),
            Strategy::BusySpinning => "BusySpinning".to_owned(),
            Strategy::Yielding => "Yielding".to_owned(),
            Strategy::Sleeping(sleeping) => format!("Sleeping({}ns)", sleeping.duration_ns),
            Strategy::SleepingMillis(millis) => format!("SleepingMillis({}ms)", millis.period_ms),
            Strategy::Backoff(backoff) => format!(
                "Backoff({},{},{}ns,{}ms)",
                backoff.max_spins,
                backoff.max_yields,
                backoff.min_park_ns,
                backoff.max_park_ns / 1_000_000
            ),
        }
    }

    /// The three that take no arguments ignore theirs rather than refuse them,
    /// which is `aeron_idle_strategy_init_null` (`aeron_agent.c:250-255`).
    #[test]
    fn the_argumentless_strategies_ignore_their_arguments() {
        assert!(matches!(
            Strategy::by_name("yield", Some("nonsense")),
            Some(Strategy::Yielding)
        ));
        assert!(matches!(
            Strategy::by_name("spin", None),
            Some(Strategy::BusySpinning)
        ));
        assert!(matches!(
            Strategy::by_name("noop", Some("1ms")),
            Some(Strategy::Noop)
        ));
    }

    /// `spins-yields-minPark-maxPark`
    /// (`aeron_idle_strategy_backoff_state_init_args`, `aeron_agent.c:186-248`).
    #[test]
    fn backoff_takes_four_numbers_and_refuses_anything_else() {
        let Some(Strategy::Backoff(backoff)) = Strategy::by_name("backoff", Some("2-3-10us-1ms"))
        else {
            panic!("four well-formed values are a backoff");
        };

        assert_eq!(2, backoff.max_spins);
        assert_eq!(3, backoff.max_yields);
        assert_eq!(10_000, backoff.min_park_ns);
        assert_eq!(1_000_000, backoff.max_park_ns);
        assert_eq!(backoff.min_park_ns, backoff.park_ns, "and it starts there");

        for malformed in ["2-3-10us", "2-3-10us-1ms-9", "a-3-10us-1ms", "2-3-x-1ms"] {
            assert!(
                Strategy::by_name("backoff", Some(malformed)).is_none(),
                "{malformed} is not four values"
            );
        }
    }

    /// Only a pass with no work idles at all — the guard every strategy in the
    /// reference opens with (`aeron_agent.c:39-42`).
    #[test]
    fn a_pass_with_work_never_idles() {
        let mut sleeping = Sleeping::new(1_000_000_000); // a second, if it slept
        let started = std::time::Instant::now();
        sleeping.idle(1);
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "a busy pass does not sleep, however long the strategy's period is"
        );

        // And every strategy agrees, including the ones with nothing to do.
        for name in ["sleep-ns", "yield", "spin", "noop", "backoff"] {
            let Some(mut strategy) = Strategy::by_name(name, None) else {
                panic!("{name} is a strategy");
            };
            strategy.idle(1);
        }
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
