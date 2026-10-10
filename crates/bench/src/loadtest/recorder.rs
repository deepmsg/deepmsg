//! What a received message does to the run's numbers.
//!
//! Mirrors `MessageTransceiver.onMessageReceived` and the fields it reaches —
//! `MessageTransceiverHotFields`'s clock, value recorder and count of received
//! messages (`MessageTransceiver.java:37-53, 136-202`), with the checksum that
//! decides whether a message is one of ours at all.
//!
//! # Why the recorder is a value the rig holds rather than a field of the
//! transceiver
//!
//! See [`super::transceiver`]'s module documentation: it is the one place this
//! port deliberately differs from the reference's shape, and the reason is that
//! the alternative in Rust is a borrow check per message.

use std::cell::Cell;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;

use crate::loadtest::transceiver::Clock;

/// The value every message carries at its end, one per process.
///
/// `MessageTransceiver.CHECKSUM`, which is
/// `ThreadLocalRandom.current().nextLong()` read once at class initialisation.
/// Its job is to tell a message this run sent from whatever else might be in the
/// buffer; it is not a checksum in any arithmetic sense, and the reference does
/// not compute anything to produce it either.
#[must_use]
pub fn checksum() -> i64 {
    static CHECKSUM: OnceLock<i64> = OnceLock::new();

    *CHECKSUM.get_or_init(random_i64)
}

/// A process-unique value, without a random number generator.
///
/// The crate takes no dependencies for one number, and the standard library has
/// no generator, so this mixes the clock with the process id through
/// SplitMix64's finaliser. It is not a cryptographic source and does not need to
/// be: what it must not be is the same in two runs, and what it must be is
/// cheap.
fn random_i64() -> i64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos() as u64);
    let mut mixed = nanos ^ (u64::from(std::process::id()) << 32) ^ 0x9E37_79B9_7F4A_7C15;

    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);

    (mixed ^ (mixed >> 31)) as i64
}

/// The run's histogram, how many messages came back, and the clock they were
/// timed against.
///
/// One per run: the reference's transceiver holds these, the rig reads them
/// back, and between warmup and the measurement it clears them.
pub struct Recorder<C: Clock> {
    histogram: Histogram<u64>,
    received_messages: i64,
    checksum: i64,
    clock: C,
    /// The last reading this recorder took, kept so that a caller wanting a
    /// reading **inside the same turn** can have one without a second
    /// `clock_gettime`.
    ///
    /// The caller that wants one is the gate: `EchoTransceiver::receive` asks
    /// whether an interval has gone by before it decides to run the client's
    /// duty cycle, and the rig's own waiting loop has just read the same clock
    /// (`LoadTestRig::send`, `now_ns = self.recorder.nano_time()`). A reading
    /// taken there is at most one turn old — 127-180 ns against an interval of
    /// a millisecond — and a clock read is 14-19 ns on the bench machine
    /// (`doc/deepmsg-rust-rig-delta.md` §2.1), measured once a turn whether or
    /// not the turn had anything to do.
    ///
    /// A `Cell` because [`Recorder::nano_time`] takes `&self`, as the rig's own
    /// clock reads do.
    last_nano_time: Cell<i64>,
}

impl<C: Clock> Recorder<C> {
    /// A recorder that will accept `checksum` and time against `clock`.
    #[must_use]
    pub fn new(histogram: Histogram<u64>, checksum: i64, clock: C) -> Self {
        Self {
            histogram,
            received_messages: 0,
            checksum,
            clock,
            // Zero, not a real reading: the gate's own `due` starts at zero too,
            // so the first turn polls whatever this holds.
            last_nano_time: Cell::new(0),
        }
    }

    /// One message arrived.
    ///
    /// The round trip is `clock.nano_time() - timestamp`, where `timestamp` is
    /// when the message was *meant* to go out and not when it did — so queueing
    /// counts as latency, which is the reference's definition and not this
    /// port's to change.
    ///
    /// # Panics
    ///
    /// When the checksum is not this run's, with the reference's own message
    /// (`MessageTransceiver.java:146`). The reference throws here and does not
    /// catch, so the run dies with the exception and its `finally` still tears
    /// the transceiver down; a Rust run dies with this panic, and tearing down
    /// on the way out is the rig's business rather than this function's.
    pub fn on_message_received(&mut self, timestamp: i64, checksum: i64) {
        assert!(
            self.checksum == checksum,
            "Invalid checksum: expected={}, actual={checksum}",
            self.checksum
        );

        let now = self.clock.nano_time();
        self.last_nano_time.set(now);

        let round_trip = now - timestamp;
        self.histogram
            .record(u64::try_from(round_trip).unwrap_or(u64::MAX))
            .expect("the histogram's bounds cover any round trip a run can produce");
        self.received_messages += 1;
    }

    /// How many messages have come back since the last reset.
    #[must_use]
    pub fn received_messages(&self) -> i64 {
        self.received_messages
    }

    /// The checksum a message has to carry to be one of this run's.
    #[must_use]
    pub fn checksum(&self) -> i64 {
        self.checksum
    }

    /// A reading from the run's clock, which is the same clock every message is
    /// timed against.
    ///
    /// Remembers what it read, so that [`Recorder::last_nano_time`] can answer
    /// with it rather than with a second read.
    #[must_use]
    pub fn nano_time(&self) -> i64 {
        let now = self.clock.nano_time();
        self.last_nano_time.set(now);

        now
    }

    /// The most recent reading this recorder took, or zero before it took one.
    ///
    /// Zero is before any real reading (a monotonic clock is far past it), which
    /// is what a caller comparing against its own deadline wants: it polls on the
    /// first turn rather than waiting for a reading that does not exist yet.
    #[must_use]
    pub fn last_nano_time(&self) -> i64 {
        self.last_nano_time.get()
    }

    /// Throw away what warmup produced.
    pub fn reset(&mut self) {
        self.histogram.reset();
        self.received_messages = 0;
    }

    /// The histogram the results are reported from.
    #[must_use]
    pub fn histogram(&self) -> &Histogram<u64> {
        &self.histogram
    }

    /// The same, for a transceiver that records into it itself.
    pub fn histogram_mut(&mut self) -> &mut Histogram<u64> {
        &mut self.histogram
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loadtest::result;

    /// A clock that hands out a scripted sequence and then repeats its last
    /// reading, as the reference's mocked `NanoClock` does.
    ///
    /// The rig's own tests need more than this — they assert *how many* readings
    /// a run took — and get a clock they can hold a handle to as well. Here the
    /// script is enough: a reading taken out of turn is a reading that comes
    /// back wrong, and the assertions are on the values.
    #[derive(Debug, Default)]
    struct ScriptedClock {
        readings: std::cell::RefCell<std::collections::VecDeque<i64>>,
        last: std::cell::Cell<i64>,
    }

    impl ScriptedClock {
        fn new(readings: impl IntoIterator<Item = i64>) -> Self {
            Self {
                readings: std::cell::RefCell::new(readings.into_iter().collect()),
                last: std::cell::Cell::new(0),
            }
        }
    }

    impl Clock for ScriptedClock {
        fn nano_time(&self) -> i64 {
            match self.readings.borrow_mut().pop_front() {
                Some(reading) => {
                    self.last.set(reading);
                    reading
                }
                None => self.last.get(),
            }
        }
    }

    fn recorder(clock: ScriptedClock, checksum: i64) -> Recorder<ScriptedClock> {
        Recorder::new(result::histogram(), checksum, clock)
    }

    #[test]
    fn a_message_is_timed_against_the_clock_and_counted() {
        let mut recorder = recorder(ScriptedClock::new([1500]), 7);

        recorder.on_message_received(1111, 7);

        assert_eq!(recorder.received_messages(), 1);
        assert_eq!(recorder.histogram().max(), 389);
    }

    #[test]
    fn a_message_that_is_not_this_runs_is_refused() {
        let mut recorder = recorder(ScriptedClock::new([1500]), 7);

        let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            recorder.on_message_received(1111, 8);
        }));

        assert!(
            refused.is_err(),
            "a foreign checksum is not a message of ours"
        );
    }

    /// The reading the gate reuses is the one the recorder took, not a second
    /// look at the clock.
    #[test]
    fn the_last_reading_is_the_one_that_was_taken() {
        let mut recorder = recorder(ScriptedClock::new([1111, 2222]), 7);

        assert_eq!(0, recorder.last_nano_time(), "before anything was read");

        assert_eq!(1111, recorder.nano_time());
        assert_eq!(1111, recorder.last_nano_time(), "the reading just taken");

        // A message records its own reading, which is the freshest one there is.
        recorder.on_message_received(1000, 7);
        assert_eq!(2222, recorder.last_nano_time());
    }

    #[test]
    fn a_reset_throws_away_what_warmup_produced() {
        let mut recorder = recorder(ScriptedClock::new([1500, 1600]), 7);

        recorder.on_message_received(1111, 7);
        recorder.on_message_received(1111, 7);
        assert_eq!(recorder.histogram().len(), 2);

        recorder.reset();

        assert_eq!(recorder.received_messages(), 0);
        assert_eq!(recorder.histogram().len(), 0);
    }

    #[test]
    fn the_checksum_is_one_value_for_the_process() {
        assert_eq!(checksum(), checksum());
    }
}
