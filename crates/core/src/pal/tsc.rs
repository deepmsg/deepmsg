//! A monotonic nanosecond clock read from the CPU's timestamp counter.
//!
//! Why this exists: the driver reads a monotonic clock once a pass on each of
//! the three threads that make up its one busy thread, a pass is what a frame
//! waits for, and the *length* of a pass was measured to cost about three times
//! its own size on the round trip (`analysis/dpdk/deepmsg-dpdk-d8-record.md`
//! §7.6). How much a cheaper reading is worth was pre-registered before this
//! file was written (`analysis/dpdk/d8-tsc-clock.md`).
//!
//! Three things it must not get wrong, and each one is why the code below looks
//! the way it does:
//!
//! * **The scale is measured rather than assumed.** The counter's frequency
//!   could be asked of the firmware (CPUID leaf 15h/16h) — the reference has no
//!   such code at all, having no counter clock — but calibrating against
//!   `CLOCK_MONOTONIC` keeps a machine-dependent path out of the tree *and*
//!   cannot be wrong by construction on any machine whose kernel times itself
//!   with this counter, which is the usual case (`dmesg`: `Switched to
//!   clocksource tsc`). One span, taken once, at first use.
//! * **The value never goes backwards** — that is the whole of the contract this
//!   clock is named after, and a bare `rdtsc` does not provide it: it is not
//!   ordered, so two readings can be *observed* in the order the hardware
//!   happened to execute them rather than the order the program wrote them.
//!   `lfence` first, which is what the kernel's own `CLOCK_MONOTONIC` path pays
//!   for too. What that fence costs, and whether it is needed, is measured
//!   rather than asserted.
//! * **It answers what it replaces.** Nanoseconds from this process's first
//!   reading — the value [`crate::clock::monotonic_nano_time`] promises, with the
//!   same saturation contract.
//!
//! Unsafe policy (ADR-0002): this is zone 1's file, and the block below is the
//! only new `unsafe` it adds.

use std::sync::OnceLock;
use std::time::Duration;

/// How long the one calibration span is.
///
/// It sets the scale's accuracy: the error is the reading noise over this span,
/// which for a span this long is parts per million — and the span is spent once,
/// on the first call, where a process that is already opening ports and memory
/// does not notice it.
const CALIBRATION_SPAN: Duration = Duration::from_millis(20);

/// How much of its own span a calibration may spend inside one of its two
/// readings, in parts per million, before it takes another span.
///
/// It is the scale's error bound: the interval between the ends is off by at
/// most this much, and so is everything computed from it.
const TRUSTWORTHY_PPM: u64 = 100;

/// The counter's reading at the process's first call, and the scale.
static CALIBRATION: OnceLock<Calibration> = OnceLock::new();

/// Nanoseconds from this process's first call, read from the timestamp counter.
pub fn nanos() -> i64 {
    CALIBRATION.get_or_init(Calibration::take).elapsed()
}

struct Calibration {
    /// The counter at the first call, which the answer counts from.
    base_ticks: u64,
    /// Nanoseconds per tick, as a 32-bit fraction: `nanos = ticks * scale >> 32`.
    ///
    /// Fixed point rather than a `f64`: one multiply and a shift, no conversion,
    /// and a `u128` intermediate so that the product cannot overflow for any
    /// uptime this process could have.
    ticks_to_nanos: u64,
    /// How far apart the two readings inside this calibration's span were, in
    /// parts per million of the span — this calibration's own opinion of itself.
    quality_ppm: u64,
}

impl Calibration {
    /// One span: both ends read as a *pair*, so that the interval between them
    /// is the interval the counter saw.
    fn measure() -> Self {
        let ticks_before_start = read_ordered();
        let nanos_at_start = super::monotonic_nanos();
        let ticks_at_start = read_ordered();
        std::thread::sleep(CALIBRATION_SPAN);
        let ticks_before_end = read_ordered();
        let nanos_at_end = super::monotonic_nanos();
        let ticks_at_end = read_ordered();

        let start = midpoint(ticks_before_start, ticks_at_start);
        let end = midpoint(ticks_before_end, ticks_at_end);
        let span = end.wrapping_sub(start);
        let gap = (ticks_at_start.wrapping_sub(ticks_before_start))
            .max(ticks_at_end.wrapping_sub(ticks_before_end));
        let nanos = (nanos_at_end - nanos_at_start).max(1) as u64;

        let ticks_to_nanos = if span == 0 {
            1
        } else {
            u64::try_from((nanos as u128) << 32).unwrap_or(1 << 32) / span.max(1)
        };

        Self {
            base_ticks: read_ordered(),
            ticks_to_nanos: ticks_to_nanos.max(1),
            quality_ppm: gap.saturating_mul(1_000_000) / span.max(1),
        }
    }

    /// Whether this span is one the calibration trusts: how much of it was spent
    /// inside one of its own two readings, in parts per million.
    fn trustworthy(&self) -> bool {
        self.quality_ppm < TRUSTWORTHY_PPM
    }

    /// The span, taken until it is one the calibration trusts.
    ///
    /// **The two ends of the span have to be read next to each other**, and that
    /// is not a detail: the first version of this read the counter and the
    /// kernel clock as two consecutive statements and produced a scale **25%
    /// wrong** on a machine running the test suite in parallel, because a thread
    /// descheduled between those two statements makes the interval the kernel
    /// saw a different interval from the one the counter saw. It was caught by
    /// the interval test below — a wrong scale is otherwise invisible, since
    /// every reading is still monotonic and every deadline still fires, just
    /// late by a constant factor.
    ///
    /// So each end is read as a *pair* — counter, kernel clock, counter — and
    /// the pair's midpoint is what the interval is measured between. The gap
    /// inside a pair bounds how much the thread could have been moved inside it,
    /// and a span whose gap is more than one ten-thousandth of itself is thrown
    /// away and taken again. The bound needs no scale: it is a ratio of two
    /// counts of the same thing.
    fn take() -> Self {
        let mut best = Self::measure();

        for _ in 0..16 {
            if best.trustworthy() {
                break;
            }
            let candidate = Self::measure();
            if candidate.quality_ppm < best.quality_ppm {
                best = candidate;
            }
        }

        // Anchored **here**, at the end of the calibration, so that the first
        // reading a caller gets is a few nanoseconds into the process rather
        // than up to a whole calibration span — the contract says "since the
        // first call", and the first call is the one that triggered all this.
        best.base_ticks = read_ordered();
        best
    }

    fn elapsed(&self) -> i64 {
        let ticks = read_ordered().wrapping_sub(self.base_ticks) as u128;
        let nanos = (ticks * self.ticks_to_nanos as u128) >> 32;

        i64::try_from(nanos).unwrap_or(i64::MAX)
    }
}

/// The middle of two readings, which is the best estimate of the moment the
/// kernel clock was read between them.
fn midpoint(earlier: u64, later: u64) -> u64 {
    earlier + later.wrapping_sub(earlier) / 2
}

/// The counter, read where the program asked for it.
///
/// A bare `rdtsc` is not a serialising instruction: it may be executed before
/// the instructions above it that the program put it after. For a clock whose
/// whole promise is that it does not go backwards, that is the difference
/// between a reading and a plausible one — so the load fence goes first, and the
/// hardware will not begin the read until everything before it has completed.
#[inline]
fn read_ordered() -> u64 {
    // SAFETY: both are available on every x86-64 part that can run this build
    // (`_rdtsc` needs only the base ISA, and `lfence` with it), neither writes
    // memory, and neither has a precondition beyond that. This is ADR-0002's
    // zone 1, which owns exactly this kind of platform call.
    unsafe {
        core::arch::x86_64::_mm_lfence();
        core::arch::x86_64::_rdtsc()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    /// **The scale, against a second clock that shares its source.** The kernel
    /// times itself with this counter, so the two agree on an interval unless
    /// the scale is wrong — a wrong scale is the one failure that would be
    /// invisible everywhere else (every reading would still be monotonic, and
    /// every deadline would still fire, just late or early by a constant
    /// factor).
    #[test]
    fn the_scale_agrees_with_the_kernel_clock_on_an_interval() {
        // **The first call is not free**: it is the one that calibrates, and it
        // spends `CALIBRATION_SPAN` doing it. A version of this test that read
        // the kernel clock *before* its first counter reading therefore compared
        // a 60 ms counter interval against an 80 ms kernel one and reported a
        // scale 25% wrong — the clock was right and the test was reading one of
        // its ends before the thing it was measuring had even started.
        let _ = super::nanos();

        let span = Duration::from_millis(60);
        let kernel_at_start = super::super::monotonic_nanos();
        let counter_at_start = super::nanos();
        std::thread::sleep(span);
        let kernel_at_end = super::super::monotonic_nanos();
        let counter_at_end = super::nanos();

        let kernel = (kernel_at_end - kernel_at_start) as f64;
        let counter = (counter_at_end - counter_at_start) as f64;
        let relative = (counter - kernel).abs() / kernel;

        let quality = super::CALIBRATION
            .get()
            .expect("the reading above calibrated it")
            .quality_ppm;

        assert!(
            quality < 1_000,
            "the calibration's own span was poor: {quality} ppm of it was spent \
             inside one of its two readings, so the scale it produced is worth \
             about that much"
        );
        assert!(
            relative < 1e-3,
            "the counter clock and CLOCK_MONOTONIC disagree on a {span:?} interval by \
             {:.4}% ({counter:.0} ns against {kernel:.0} ns) — the scale is wrong \
             (calibration quality {quality} ppm)",
            relative * 100.0
        );
    }

    /// **The promise in the name: it does not go backwards.** This is the one
    /// the fence is for — see the module note, and the falsification recorded in
    /// the pre-registration, which is what a bare counter does to this test.
    #[test]
    fn repeated_readings_never_go_backwards() {
        let mut previous = super::nanos();
        for _ in 0..1_000_000 {
            let now = super::nanos();
            assert!(
                now >= previous,
                "a reading went backwards: {previous} then {now}"
            );
            previous = now;
        }
    }

    /// **Every caller but one arrives after the calibration**, and reads the
    /// time since it ended rather than since the process began. That is what CI
    /// found — a parallel test read 99 ms — so it is asserted here instead of
    /// left to the scheduler: this test calibrates from another thread, waits,
    /// and checks that a caller arriving afterwards reads that wait, and not
    /// zero and not hours.
    #[test]
    fn a_caller_that_arrives_after_the_calibration_reads_the_time_since_it() {
        std::thread::spawn(|| {
            let _ = super::nanos();
        })
        .join()
        .expect("the thread that pays for the calibration");

        let waited = Duration::from_millis(150);
        std::thread::sleep(waited);
        let late = super::nanos();

        assert!(
            late >= waited.as_nanos() as i64,
            "a caller that arrived after the calibration read {late} ns, which is \
             less than the {waited:?} that passed after it"
        );
        assert!(
            late < 60_000_000_000,
            "and it read {late} ns, which is not this process's own calibration"
        );
    }

    /// The clock counts from somewhere in this process and moves in the units
    /// the rest of the crate's timing uses, so a caller cannot tell which clock
    /// it was handed.
    ///
    /// **Not "the first reading is zero", which is what this test asserted until
    /// CI said otherwise**, and which no version of this clock has promised
    /// under a parallel run — the `Instant` base before it no more than this
    /// one. The calibration is a `OnceLock`: whichever thread reaches it first
    /// spends the span and anchors the clock at the end of it, so a thread that
    /// arrives later — including one the test runner happens to schedule beside
    /// it — reads the time elapsed since then. On CI that was 99 ms, which is a
    /// fact about the scheduling and not about the clock.
    ///
    /// What *is* worth asserting is the thing a wrong anchor would break: a
    /// reading counted from boot (or from an epoch) would be hours, not
    /// milliseconds, into the process.
    #[test]
    fn the_first_reading_counts_from_this_process_and_time_moves_forward() {
        let start = super::nanos();
        assert!(
            start < 60_000_000_000,
            "the first reading is {start} ns into the process — the clock is \
             counting from somewhere other than this process's own calibration"
        );

        // A sanity band and not a precision one: this machine may be running the
        // rest of the suite beside it, and a sleep that overshoots says nothing
        // about the clock. A scale that is wrong by a *lot* is what this
        // catches, and a scale that is wrong by a little is the interval test's
        // job above.
        std::thread::sleep(Duration::from_millis(5));
        let elapsed = super::nanos() - start;
        assert!(
            (3_000_000..50_000_000).contains(&elapsed),
            "5 ms of sleep measured {elapsed} ns"
        );
    }
}
