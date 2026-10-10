//! The seam the system under test is plugged into, and the two things the rig
//! needs from the world besides it: a clock and a way to wait.
//!
//! Mirrors `benchmarks-api/.../MessageTransceiver.java` and the `NanoClock` and
//! `IdleStrategy` it is handed.
//!
//! # One deliberate difference from the reference's shape
//!
//! In the reference the transceiver *owns* the value recorder and the counter of
//! received messages — they are fields of its base class — and the rig reads
//! `receivedMessages()` back off it. Here the rig owns a [`Recorder`] and hands
//! it to `send` and `receive` as `&mut`.
//!
//! What that buys is the absence of a cell: with the reference's shape in Rust
//! the recorder would have to be behind an `Rc<RefCell<..>>` shared between the
//! rig and the transceiver, and every recorded message would pay a borrow check
//! to say what the borrow checker could have said once. What it costs is that
//! the trait's methods carry one more argument, and that a transceiver which
//! wants to record during `send` — a synchronous one, as the reference's
//! doc calls it — is handed the recorder then too, which it would have had
//! anyway.
//!
//! The clock travels inside the recorder for the same reason: there is exactly
//! one in a run, both sides read it, and this way neither has to reach for the
//! other's.

use std::fmt;

use crate::loadtest::config::{Configuration, IdleStrategy};
use crate::loadtest::recorder::Recorder;

/// Where the time comes from.
///
/// `org.agrona.concurrent.NanoClock`. A trait rather than a call to
/// `std::time::Instant` because the rig's timing is compared against a scripted
/// sequence of readings in its tests, and because a reading that cannot be
/// scripted cannot be pinned down at all.
pub trait Clock {
    /// Nanoseconds from a clock that only goes forwards.
    ///
    /// Which clock, and from where, is the implementation's business — the rig
    /// only ever subtracts two readings.
    fn nano_time(&self) -> i64;
}

/// The clock a run uses: the kernel's raw `CLOCK_MONOTONIC`, read through
/// [`deepmsg_core::pal::monotonic_nanos`].
///
/// The driver's own timing uses [`deepmsg_core::clock::monotonic_nano_time`]
/// instead, which counts from this process's first reading. The rig cannot
/// afford it here: its pacing loop reads the clock once per inner turn —
/// `LoadTestRig.send`'s `now_ns = self.recorder.nano_time()`
/// (`crates/bench/src/loadtest/rig.rs:312`, between `receive` calls) — so the
/// reading is on the measured path, and [`deepmsg_core::pal::monotonic_nanos`]
/// is the raw `clock_gettime` where `monotonic_nano_time` builds a `Duration`
/// and multiplies in `u128`. The difference is a few nanoseconds a reading, and
/// the rig reads the clock once a turn.
///
/// The origin is not why this clock is chosen — a run only ever subtracts two of
/// its own readings — but it is the origin the reference's side of the round
/// trip is on: the echo node subtracts a stamp from its own `System.nanoTime()`
/// (`EchoNode.java:165-166`), which on Linux is `CLOCK_MONOTONIC`, so a reading
/// that ever leaves the process is already comparable and needs no rebasing.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn nano_time(&self) -> i64 {
        deepmsg_core::pal::monotonic_nanos()
    }
}

/// What a transceiver can fail at.
#[derive(Debug)]
pub enum TransceiverError {
    /// The system under test refused something, with what it was doing.
    Failed {
        /// The step that failed — `init` or `destroy`.
        action: &'static str,
        /// What the system said.
        message: String,
    },
}

impl fmt::Display for TransceiverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed { action, message } => write!(f, "{action} failed: {message}"),
        }
    }
}

impl std::error::Error for TransceiverError {}

/// The system under test.
///
/// `MessageTransceiver`. The rig drives it by rate and by batch and reads
/// nothing back off it but what it says through the recorder.
pub trait MessageTransceiver<C: Clock> {
    /// Establish whatever the transceiver needs, and block until `send` and
    /// `receive` are safe to call.
    fn init(&mut self, configuration: &Configuration) -> Result<(), TransceiverError>;

    /// Tear down. Called whatever happened to the run, as the reference's
    /// `finally` does.
    fn destroy(&mut self) -> Result<(), TransceiverError>;

    /// Send `number_of_messages` at one timestamp, and say how many actually
    /// went.
    ///
    /// The payload is at least `message_length` bytes with `timestamp` at its
    /// front and `checksum` at its end; anything the transceiver adds around
    /// those does not count towards the length. The return value may be less
    /// than asked for — the rig sends the remainder as its next batch — and it
    /// may not block forever.
    fn send(
        &mut self,
        number_of_messages: usize,
        message_length: usize,
        timestamp: i64,
        checksum: i64,
        recorder: &mut Recorder<C>,
    ) -> usize;

    /// Take whatever has arrived and hand each message to
    /// [`Recorder::on_message_received`].
    fn receive(&mut self, recorder: &mut Recorder<C>);

    /// Called before the warmup or the measurement, with which of the two it is.
    fn on_benchmark_start(&mut self, warmup: bool) {
        let _ = warmup;
    }

    /// Called after it, likewise.
    fn on_benchmark_complete(&mut self, warmup: bool) {
        let _ = warmup;
    }
}

/// Waiting for something to happen.
///
/// The rig calls `idle` when a look for replies found none, and `reset` as soon
/// as one does. `reset` is not bookkeeping: it is what makes a backoff start its
/// climb again rather than staying parked while messages are arriving.
///
/// A trait, and not just the enum below, so that a test can count the calls —
/// the reference's tests assert how many times a run idled, and how many times
/// it reset, and that is a property of the rig's control flow rather than of the
/// waiting.
pub trait Idle {
    /// Wait for something to happen.
    fn idle(&mut self);

    /// Something happened, so start again.
    fn reset(&mut self);
}

/// The five names `io.aeron.benchmarks.idle.strategy` can hold, as Agrona's
/// classes behave.
///
/// `BusySpin` is `Thread.onSpinWait`, which is the reference's default and the
/// only one any grid runs. `Backoff` and `Sleeping` are approximations of
/// Agrona's: neither is reachable from the reference's own scripts, and their
/// exact park durations are not part of what a run measures.
impl Idle for IdleStrategy {
    fn idle(&mut self) {
        match self {
            Self::BusySpin => std::hint::spin_loop(),
            Self::NoOp => {}
            Self::Yielding => std::thread::yield_now(),
            Self::Backoff { idle_count } => {
                if *idle_count < BACKOFF_SPINS {
                    *idle_count += 1;
                    std::thread::yield_now();
                } else {
                    std::thread::sleep(BACKOFF_PARK);
                }
            }
            Self::Sleeping => std::thread::sleep(SLEEP_PERIOD),
        }
    }

    fn reset(&mut self) {
        if let Self::Backoff { idle_count } = self {
            *idle_count = 0;
        }
    }
}

/// How many times `Backoff` yields before it starts sleeping.
const BACKOFF_SPINS: u32 = 10;

/// How long `Backoff` parks once it is sleeping.
const BACKOFF_PARK: std::time::Duration = std::time::Duration::from_micros(1);

/// What `Sleeping` sleeps. Agrona's `SleepingIdleStrategy` defaults to a
/// microsecond.
const SLEEP_PERIOD: std::time::Duration = std::time::Duration::from_micros(1);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_system_clock_goes_forwards() {
        let clock = SystemClock;
        let first = clock.nano_time();
        let second = clock.nano_time();

        assert!(second >= first, "{second} came after {first}");
        assert!(
            first > 0,
            "the clock counts from the kernel's boot, which is in the past"
        );
    }

    #[test]
    fn a_backoff_starts_again_when_it_is_reset() {
        let mut idle = IdleStrategy::Backoff { idle_count: 0 };

        for _ in 0..BACKOFF_SPINS + 2 {
            idle.idle();
        }
        assert!(matches!(
            idle,
            IdleStrategy::Backoff {
                idle_count: BACKOFF_SPINS
            }
        ));

        idle.reset();
        assert!(matches!(idle, IdleStrategy::Backoff { idle_count: 0 }));
    }

    #[test]
    fn the_strategies_that_keep_nothing_still_take_a_reset() {
        for mut idle in [
            IdleStrategy::BusySpin,
            IdleStrategy::NoOp,
            IdleStrategy::Yielding,
            IdleStrategy::Sleeping,
        ] {
            idle.idle();
            idle.reset();
        }
    }
}
