//! Standalone media driver process — the counterpart of the reference
//! `aeronmd` (deployment forms: M17).
//!
//! The shape of `main` is the reference's (`aeron-driver/src/main/c/aeronmd.c:100-190`),
//! and the order matters more than it looks:
//!
//! 1. parse the configuration,
//! 2. settle the aeron directory — somebody else may already own it,
//! 3. create the CnC file (not published yet),
//! 4. start the conductor, which writes the first heartbeat and then publishes
//!    the ready version,
//! 5. loop until a termination command or a signal,
//! 6. publish the stop signal, flush, and delete the directory if configured.
//!
//! Step 2 before step 3 is the reference's order too, and it is the one that
//! cannot be swapped: creating a file over a live driver's is not something a
//! later check can undo.

use std::io;
use std::process::ExitCode;
use std::time::Duration;

use deepmsg_cnc::{CncCreateError, CncFile, CncIdentity};
use deepmsg_core::clock;
use deepmsg_driver::conductor::Conductor;
use deepmsg_driver::config::DriverConfig;
use deepmsg_driver::{dir, sys};

fn main() -> ExitCode {
    let config = match DriverConfig::from_args(std::env::args().skip(1)) {
        Ok(config) => config,
        Err(error) => return fail(&format_args!("{error}")),
    };

    let now_ms = clock::epoch_millis();

    match dir::prepare(&config, now_ms) {
        Ok(messages) => {
            for message in messages {
                eprintln!("deepmsg-driver: {message}");
            }
        }
        Err(error) => return fail(&format_args!("{error}")),
    }

    if let Err(error) = sys::install_stop_handler() {
        eprintln!("deepmsg-driver: could not install the signal handler: {error}");
        let _ = dir::remove(&config);
        return ExitCode::FAILURE;
    }

    let identity = CncIdentity {
        liveness_timeout_ns: config.client_liveness_timeout_ns,
        start_timestamp_ms: now_ms,
        pid: i64::from(std::process::id()),
    };

    // The file is created but not published: nothing may read it until the
    // conductor has a heartbeat to show, and the conductor is what publishes.
    let cnc = match CncFile::create(&config.aeron_dir, &config.layout, &identity) {
        Ok(cnc) => cnc,
        // Another driver won the `O_EXCL` race — it created its file between
        // `prepare` looking at the directory and this call. Deleting the
        // directory here would unlink the winner's `cnc.dat` and its
        // `publications/` and `images/` trees and leave it running as a driver
        // nobody can reach: the failure the directory discipline exists to
        // prevent, reached through the other door. The directory is not ours,
        // so it is not ours to remove.
        Err(CncCreateError::Io(source)) if io::ErrorKind::AlreadyExists == source.kind() => {
            eprintln!(
                "deepmsg-driver: another driver is creating {}: EBUSY",
                config.aeron_dir.display()
            );
            return ExitCode::FAILURE;
        }
        // Everything past this point happens in a directory this process
        // created, so removing it on the way out is removing our own work.
        Err(error) => {
            eprintln!("deepmsg-driver: {error}");
            let _ = dir::remove(&config);
            return ExitCode::FAILURE;
        }
    };

    let mut conductor = match Conductor::new(cnc, &config) {
        Ok(conductor) => conductor,
        Err(error) => {
            eprintln!("deepmsg-driver: {error}");
            let _ = dir::remove(&config);
            return ExitCode::FAILURE;
        }
    };

    eprintln!(
        "deepmsg-driver: media driver running, aeron.dir={} (CnC version {}, pid {})",
        config.aeron_dir.display(),
        deepmsg_core::version::format_version(deepmsg_core::version::CNC_VERSION),
        std::process::id()
    );

    let mut idle = Backoff::new();
    while conductor.is_running() && sys::stop_signal().is_none() {
        idle.idle(conductor.do_work());
    }

    shutdown(&mut conductor, &config)
}

/// Stop the driver the way the reference does, and report how it stopped.
fn shutdown(conductor: &mut Conductor, config: &DriverConfig) -> ExitCode {
    if let Err(error) = conductor.close() {
        eprintln!("deepmsg-driver: the shutdown signal could not be published: {error}");
    }

    // After the close, never before: the flush inside it is what the *next*
    // driver reads to decide whether this one is still alive, and removing the
    // directory first would leave that check with nothing to read.
    if let Err(error) = dir::remove(config) {
        eprintln!("deepmsg-driver: {error}");
    }

    let unhandled = conductor.unhandled_commands();
    let unknown = conductor.unknown_commands();
    if 0 != unhandled || 0 != unknown {
        eprintln!(
            "deepmsg-driver: {unhandled} commands were not implemented and {unknown} were not in the \
             protocol; {last:?} was the last, and the clients that sent one have timed out",
            last = conductor.last_unhandled(),
        );
    }

    match sys::stop_signal() {
        // The signal number, not a failure code: the reference records what it
        // was stopped by (`aeronmd.c:39-42`), and returning 0 here would make a
        // signalled driver indistinguishable from one that was asked to stop.
        Some(signal) => {
            eprintln!("deepmsg-driver: stopping on signal {signal}");
            ExitCode::from(u8::try_from(signal).unwrap_or(1))
        }
        None => ExitCode::SUCCESS,
    }
}

fn fail(message: &dyn std::fmt::Display) -> ExitCode {
    eprintln!("deepmsg-driver: {message}");
    ExitCode::FAILURE
}

/// The idle strategy a driver agent uses when it has nothing to do.
///
/// The default for every agent of the reference driver, and the reason it is
/// this one rather than a sleep: a control plane that parks for a millisecond
/// after every quiet cycle is a control plane that answers a command up to a
/// millisecond late. Backoff spins first, yields next, and only then parks,
/// doubling the park each time it finds nothing
/// (`aeron-client/src/main/c/aeron_agent.c:112-163`, with the parameters from
/// `aeron_agent.h:43-46`).
struct Backoff {
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

    const fn new() -> Self {
        Self {
            spins: 0,
            yields: 0,
            park_ns: Self::MIN_PARK_NS,
        }
    }

    /// Idle after a cycle that did `work_count` units of work.
    fn idle(&mut self, work_count: usize) {
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
