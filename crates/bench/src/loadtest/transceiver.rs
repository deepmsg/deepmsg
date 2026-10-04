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

/// The clock a run uses: [`deepmsg_core::clock::monotonic_nano_time`], the same
/// one the driver's sender and receiver read.
///
/// The value is nanoseconds since this process's first reading, so it is a
/// duration and not a date. That is also what the reference's `SystemNanoClock`
/// is — `System.nanoTime()`'s origin is the JVM's, not any calendar's — and it
/// is why the two sides' absolute numbers never have to agree.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn nano_time(&self) -> i64 {
        deepmsg_core::clock::monotonic_nano_time()
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

    /// Discard whatever the transceiver has accumulated, as the reference does
    /// between warmup and the measurement.
    fn reset(&mut self) {}
}

/// The `idle` and `reset` the rig calls, one per name in
/// [`crate::loadtest::config::IdleStrategy`].
///
/// `reset` is not bookkeeping: the rig calls it every time a receive made
/// progress, which is what makes a backoff start its climb again instead of
/// staying parked while messages are arriving.
///
/// `BusySpin` is `Thread.onSpinWait`, which is the reference's default and the
/// only one any grid runs. `Backoff` and `Sleeping` are approximations of
/// Agrona's: neither is reachable from the reference's own scripts, and their
/// exact park durations are not part of what a run measures.
impl IdleStrategy {
    /// Wait for something to happen.
    pub fn idle(&mut self) {
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

    /// Something happened, so start again.
    pub fn reset(&mut self) {
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
            "the clock counts from this process's first reading"
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
