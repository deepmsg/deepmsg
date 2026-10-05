//! `debug.stage.timing`: how long a datagram sits between the kernel taking it
//! off the queue and this driver being done with it.
//!
//! Not the reference's instrument, and not a counter of its: it exists because
//! the same benchmark run against the reference driver is quicker in a way none
//! of that driver's counters account for, and because the reference's binary is
//! not ours to change — so the measurement has to stand on its own rather than
//! be compared stage by stage.
//!
//! # The two marks, and why there are two
//!
//! Both are measured from the **same** starting point — the kernel's arrival
//! stamp — so they are not two stages of a pipeline but one stage cut at two
//! places:
//!
//! | mark | from → to | what it is |
//! |---|---|---|
//! | `pickup` | kernel → the receiver's datagram loop | **waiting**: how long the driver took to notice a datagram that was already here |
//! | `logged` | kernel → `insert_packet` returning | waiting **plus** the work of getting it into the log: parsing, validating, flow control, the write |
//!
//! The difference between them is the driver's own per-datagram work; `pickup`
//! alone is the polling latency. Which of the two carries a difference against
//! the reference is not knowable in advance, and they cost the same to take —
//! one `CLOCK_REALTIME` read each, on a datagram path whose polling is a busy
//! spin (`aeron.receiver.idle.strategy=noop` in the benchmark's own driver
//! properties), where "the driver was slow to look" and "the driver was slow to
//! write" are different answers.
//!
//! # What it is measuring, and why the two clocks are not the same one
//!
//! The kernel's arrival stamp (`SO_TIMESTAMPNS`) is **`CLOCK_REALTIME`**. The
//! driver's own readings elsewhere are [`deepmsg_core::clock::monotonic_nano_time`],
//! which counts from this process's first call — subtracting one from the other
//! would be subtracting a date from a duration. So the marks are taken against
//! [`deepmsg_core::clock::epoch_nano_time`], which is the same clock the kernel
//! stamped with, and the difference is a real elapsed time. That is what
//! [`since_kernel_ns`] is for, and it is the only subtraction either mark may
//! use.
//!
//! # Why the numbers are kept here rather than in a counter
//!
//! A counter would mean three shared-memory writes per datagram — on the very
//! path being measured. The accumulators below are plain integers in the
//! receiver's own memory, written once per datagram and published once a
//! second, which is the same shape the driver's cycle-time counters use.
//!
//! # Why a file rather than a counter
//!
//! Allocation is the conductor's, and this runs on the receiver's thread: a
//! counter would need its id carried down to an agent that has no business
//! knowing about it. The driver already writes diagnostics into its own
//! directory (`loss-report.dat`), and a debug instrument is at home there.

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

/// The file the running distribution is written to, inside the aeron directory.
pub const FILE_NAME: &str = "stage-timing.txt";

/// How often the accumulators are published.
const FLUSH_INTERVAL_NS: i64 = 1_000_000_000;

/// Nanoseconds from the kernel's arrival stamp to now.
///
/// The saturating subtraction is not defensive noise: a stamp is taken by
/// whichever CPU took the packet off the queue, and this thread reads its own
/// clock on the CPU it was pinned to. Two `CLOCK_REALTIME` readings of the same
/// instant from two cores can differ by more than zero, so a "negative" elapsed
/// time is a clock skew of a few nanoseconds and not an error — it is reported
/// as zero.
#[must_use]
pub fn since_kernel_ns(kernel_ns: i64) -> u64 {
    u64::try_from(deepmsg_core::clock::epoch_nano_time().saturating_sub(kernel_ns)).unwrap_or(0)
}

/// A running distribution of one mark's elapsed times.
///
/// Deliberately not an HdrHistogram: what is being asked is whether a mark is
/// hundreds of nanoseconds or whole microseconds, and a maximum, a mean and a
/// count answer that without a new file format or a dependency the driver does
/// not already have.
#[derive(Debug, Default)]
pub struct Stage {
    sum_ns: u64,
    count: u64,
    max_ns: u64,
}

impl Stage {
    /// One sample, in nanoseconds.
    ///
    /// Three integer operations and a compare — this runs once per datagram, so
    /// what it must not do is call anything.
    fn record(&mut self, nanos: u64) {
        self.sum_ns = self.sum_ns.saturating_add(nanos);
        self.count += 1;

        if nanos > self.max_ns {
            self.max_ns = nanos;
        }
    }

    /// How many samples have been recorded.
    #[must_use]
    pub const fn count(&self) -> u64 {
        self.count
    }

    /// The mean sample, in nanoseconds; zero when there are none.
    #[must_use]
    pub fn mean_ns(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_ns as f64 / self.count as f64
        }
    }

    /// The largest sample, in nanoseconds.
    #[must_use]
    pub const fn max_ns(&self) -> u64 {
        self.max_ns
    }
}

/// Both marks, and the file they are published into.
///
/// One instance lives on the receiver and is `Some` only when the driver was
/// configured to time (`debug.stage.timing`); with it `None` the receive path
/// does not read a clock at all.
#[derive(Debug)]
pub struct StageTiming {
    path: PathBuf,
    pickup: Stage,
    logged: Stage,
    /// Every datagram the receive loop walked, stamped or not.
    walked: u64,
    /// The ones the kernel did not stamp.
    ///
    /// Not decoration, and the reason it is counted at all: a mark is only
    /// taken for a datagram that carries a stamp, so **this number being
    /// anything but zero means every distribution in the file is over a subset
    /// of the traffic** — and a subset that arrives in batches is not a random
    /// one. It was put here after a run in which the loop walked 90 million
    /// datagrams and only 43 million carried a stamp, which made the whole
    /// measurement unreadable until the split was visible.
    unstamped: u64,
    last_flush_ns: i64,
}

impl StageTiming {
    /// Accumulators publishing into `aeron_dir`.
    #[must_use]
    pub fn new(aeron_dir: &Path) -> Self {
        Self {
            path: aeron_dir.join(FILE_NAME),
            pickup: Stage::default(),
            logged: Stage::default(),
            walked: 0,
            unstamped: 0,
            last_flush_ns: 0,
        }
    }

    /// One datagram walked by the receive loop, before it is known whether the
    /// kernel stamped it.
    pub fn record_walked(&mut self) {
        self.walked += 1;
    }

    /// One that was walked and had no stamp to mark it from.
    pub fn record_unstamped(&mut self) {
        self.unstamped += 1;
    }

    /// The kernel's stamp to the datagram loop — see the module table.
    pub fn record_pickup(&mut self, nanos: u64) {
        self.pickup.record(nanos);
    }

    /// The kernel's stamp to `insert_packet` returning - see the module table.
    pub fn record_logged(&mut self, nanos: u64) {
        self.logged.record(nanos);
    }

    /// The waiting mark.
    #[must_use]
    pub const fn pickup(&self) -> &Stage {
        &self.pickup
    }

    /// The waiting-and-writing mark.
    #[must_use]
    pub const fn logged(&self) -> &Stage {
        &self.logged
    }

    /// Every datagram the loop walked.
    #[must_use]
    pub const fn walked(&self) -> u64 {
        self.walked
    }

    /// How many of those the kernel did not stamp. **Read this first**: a
    /// non-zero number means the distributions below are over a subset.
    #[must_use]
    pub const fn unstamped(&self) -> u64 {
        self.unstamped
    }

    /// Write the distribution out if a second has passed since the last time.
    ///
    /// `now_ns` is the caller's own clock, whatever it counts from: only the
    /// interval between two of its readings is used.
    ///
    /// A failure to write is not reported: this is an instrument, and a run
    /// that cannot write its diagnostics is still a run. The file is rewritten
    /// whole each time, so what it holds is always the distribution so far.
    pub fn flush_if_due(&mut self, now_ns: i64) {
        if now_ns.saturating_sub(self.last_flush_ns) < FLUSH_INTERVAL_NS {
            return;
        }

        self.last_flush_ns = now_ns;
        let _ = self.flush();
    }

    /// Write the distribution out now.
    ///
    /// # Errors
    ///
    /// Whatever the filesystem says.
    pub fn flush(&self) -> std::io::Result<()> {
        let mut text = String::new();

        let _ = writeln!(
            text,
            "# debug.stage.timing, from the kernel's arrival stamp (SO_TIMESTAMPNS, CLOCK_REALTIME)"
        );
        let _ = writeln!(
            text,
            "# pickup  the receiver's datagram loop taking it — waiting"
        );
        let _ = writeln!(
            text,
            "# logged  the write into an image log returning — waiting and writing"
        );
        // Before the distributions, because it says whether they cover the
        // traffic: `unstamped` not zero means they do not.
        let _ = writeln!(
            text,
            "# walked {} datagrams, {} of them unstamped",
            self.walked, self.unstamped
        );
        let _ = writeln!(text, "# stage samples mean_ns max_ns");

        for (name, stage) in [("pickup", &self.pickup), ("logged", &self.logged)] {
            let _ = writeln!(
                text,
                "{name} {} {:.1} {}",
                stage.count(),
                stage.mean_ns(),
                stage.max_ns()
            );
        }

        fs::write(&self.path, text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "deepmsg-stage-timing-{}-{name}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);

        path
    }

    #[test]
    fn each_mark_holds_its_own_count_its_own_sum_and_its_own_maximum() {
        let directory = scratch("accumulate");
        let mut timing = StageTiming::new(&directory);

        for sample in [100, 300, 200] {
            timing.record_pickup(sample);
        }

        timing.record_logged(7);

        assert_eq!(3, timing.pickup().count());
        assert_eq!(300, timing.pickup().max_ns());
        assert_eq!(200.0, timing.pickup().mean_ns());
        // The two marks are separate distributions, not one with a flag.
        assert_eq!(1, timing.logged().count());
        assert_eq!(7.0, timing.logged().mean_ns());
    }

    #[test]
    fn what_is_written_is_what_was_recorded() {
        let directory = scratch("flush");
        fs::create_dir_all(&directory).expect("a scratch directory");
        let mut timing = StageTiming::new(&directory);
        timing.record_pickup(100);
        timing.record_pickup(300);
        timing.record_logged(400);

        timing.flush().expect("a writable directory");

        let text = fs::read_to_string(directory.join(FILE_NAME)).expect("the file it wrote");
        assert!(text.contains("pickup 2 200.0 300"), "{text}");
        assert!(text.contains("logged 1 400.0 400"), "{text}");

        let _ = fs::remove_dir_all(&directory);
    }

    /// A second has to pass, or the instrument would spend the run writing to a
    /// file — which is the one thing it must not do.
    #[test]
    fn nothing_is_written_until_a_second_has_passed() {
        let directory = scratch("interval");
        fs::create_dir_all(&directory).expect("a scratch directory");
        let mut timing = StageTiming::new(&directory);
        timing.record_pickup(100);

        timing.flush_if_due(FLUSH_INTERVAL_NS - 1);
        assert!(!directory.join(FILE_NAME).exists(), "not due yet");

        timing.flush_if_due(FLUSH_INTERVAL_NS);
        assert!(directory.join(FILE_NAME).exists(), "due after a second");

        let _ = fs::remove_dir_all(&directory);
    }

    #[test]
    fn an_empty_distribution_is_a_zero_and_not_a_division() {
        let directory = scratch("empty");
        fs::create_dir_all(&directory).expect("a scratch directory");
        let timing = StageTiming::new(&directory);

        timing.flush().expect("a writable directory");

        let text = fs::read_to_string(directory.join(FILE_NAME)).expect("the file it wrote");
        assert!(text.contains("pickup 0 0.0 0"), "{text}");
        assert!(text.contains("logged 0 0.0 0"), "{text}");

        let _ = fs::remove_dir_all(&directory);
    }

    /// The one number that says whether the distributions above it cover the
    /// traffic. A datagram the kernel did not stamp cannot be marked from, and
    /// leaving it out silently is the failure this counter exists to make
    /// visible.
    #[test]
    fn a_datagram_the_kernel_did_not_stamp_is_counted_rather_than_dropped() {
        let directory = scratch("unstamped");
        fs::create_dir_all(&directory).expect("a scratch directory");
        let mut timing = StageTiming::new(&directory);

        timing.record_walked();
        timing.record_pickup(100);
        timing.record_walked();
        timing.record_unstamped();

        assert_eq!(2, timing.walked());
        assert_eq!(1, timing.unstamped());
        assert_eq!(1, timing.pickup().count(), "the unstamped one is no sample");

        timing.flush().expect("a writable directory");

        let text = fs::read_to_string(directory.join(FILE_NAME)).expect("the file it wrote");
        assert!(
            text.contains("walked 2 datagrams, 1 of them unstamped"),
            "{text}"
        );

        let _ = fs::remove_dir_all(&directory);
    }

    /// The mark is taken against the same clock the kernel stamped with. A
    /// stamp in this process's *own* monotonic clock would be a small number,
    /// and subtracting it from the epoch would be a duration minus a date —
    /// which is the mistake this test is here to keep out.
    #[test]
    fn the_elapsed_time_is_taken_against_the_kernel_s_own_clock() {
        let now = deepmsg_core::clock::epoch_nano_time();

        assert!(since_kernel_ns(now - 1_000) >= 1_000, "a microsecond ago");
        assert_eq!(0, since_kernel_ns(now + 1_000_000_000), "a stamp ahead");
    }
}
