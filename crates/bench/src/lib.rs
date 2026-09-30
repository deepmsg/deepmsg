//! Micro-benchmarks and the latency harness.
//!
//! Two instruments, because they answer different questions.
//!
//! - **criterion** (`benches/micro.rs`) measures one path in isolation, in this
//!   process, with no driver and no socket: the log buffer's append path, the
//!   term scanner, the buffer accessors. What it is for is *regressions* — a
//!   change that makes one of them twice as slow is visible here and nowhere
//!   else.
//! - **the live harness** (`benches/latency.rs`) measures what a caller waits:
//!   a round trip through a media driver, in a process of its own, recorded
//!   into a histogram. What it is for is the number the project is judged by,
//!   and the comparison against the reference's own `Ping`/`Pong`.
//!
//! Neither is a gate. A number from a shared machine is not a contract, so
//! nothing here runs in CI and nothing here asserts a threshold — the harness
//! *compiles* and *lints* in CI like every other target, and that is all.
//! `docs/benchmarks.md` holds the numbers that were actually observed, with the
//! machine they were observed on.
//!
//! This module is the part both instruments share: what a scenario is, the
//! histogram the reference uses so the two are comparable at all, how a
//! histogram is summarised, and the fingerprint without which a latency number
//! means nothing.

#![forbid(unsafe_code)]

use std::fmt;
use std::process::Command;
use std::thread;

use hdrhistogram::Histogram;

/// The lowest value the histogram records, in nanoseconds: one.
///
/// The reference's own samples initialise theirs as
/// `hdr_init(1, 10 * 1000 * 1000 * 1000, 3, …)` — one nanosecond to ten
/// seconds, three significant digits (`aeron-samples/src/main/c/cping.c:353`)
/// — and a histogram with different bounds reports different percentiles for
/// the same samples. Ours has to be that one.
pub const HDR_LOW: u64 = 1;

/// The highest value the histogram records, in nanoseconds: ten seconds.
pub const HDR_HIGH: u64 = 10 * 1000 * 1000 * 1000;

/// How many significant digits the histogram keeps.
pub const HDR_SIGNIFICANT_DIGITS: u8 = 3;

/// A histogram with the reference's bounds, ready to record nanoseconds into.
///
/// # Panics
///
/// Never in practice: the bounds are constants that satisfy
/// [`Histogram::new_with_bounds`]'s requirements (low >= 1, low < high,
/// sigfig in 1..=5). A panic here is a bug in this module, not a runtime
/// condition.
#[must_use]
pub fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(HDR_LOW, HDR_HIGH, HDR_SIGNIFICANT_DIGITS)
        .expect("the bounds are the reference's own and are valid")
}

/// Which transport a measurement runs over.
///
/// The two shapes a P1 driver serves, and they are not variants of one thing:
/// an IPC round trip never leaves the machine's memory, and a UDP one goes
/// through the kernel's loopback path. Reporting one number for "the driver"
/// would average the two into something that describes neither.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Channel {
    /// `aeron:ipc` — a shared log buffer, no socket.
    Ipc,
    /// `aeron:udp` on the loopback address — a real socket, a real syscall.
    Udp,
}

impl Channel {
    /// Every channel a run measures by default, in the order they are reported.
    pub const ALL: [Self; 2] = [Self::Ipc, Self::Udp];

    /// The word this channel is reported under.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Ipc => "aeron:ipc",
            Self::Udp => "aeron:udp",
        }
    }

    /// The channel URI both sides are given.
    ///
    /// One URI for both ends on purpose: on a unicast channel the subscriber
    /// binds the endpoint and the publisher sends to it, so naming the same
    /// endpoint is what pairs them — which is exactly the shape
    /// `tests/interop/udp_transport.rs` runs.
    #[must_use]
    pub fn uri(self, port: u16) -> String {
        match self {
            Self::Ipc => "aeron:ipc".to_owned(),
            Self::Udp => format!("aeron:udp?endpoint=localhost:{port}"),
        }
    }
}

/// One measurement: a channel and a message size.
///
/// Both dimensions earn their place. The channel says what the round trip costs
/// in syscalls, and the size says how much of the cost is per message rather
/// than per byte — 32 bytes is the reference's own default
/// (`samples_configuration.h:28`), and a kilobyte is where the copy starts to
/// matter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Scenario {
    /// The transport.
    pub channel: Channel,
    /// The payload length in bytes, header excluded.
    pub length: usize,
}

impl Scenario {
    /// Every scenario a run measures by default.
    #[must_use]
    pub fn all() -> Vec<Self> {
        let mut scenarios = Vec::new();

        for channel in Channel::ALL {
            for length in [32, 1024] {
                scenarios.push(Self { channel, length });
            }
        }

        scenarios
    }

    /// A payload of this scenario's length, with `stamp` in its first eight
    /// bytes.
    ///
    /// The stamp is what makes the measurement a round trip: it goes out with
    /// the message and is read back off the reply, so what is recorded is the
    /// time from "the caller asked" to "the reply was in hand" — the same thing
    /// the reference's samples measure, and the same way
    /// (`aeron-samples/src/main/c/cping.c:109`, `:79-88`).
    #[must_use]
    pub fn payload(&self, stamp: i64) -> Vec<u8> {
        let mut payload = vec![0_u8; self.length];
        let bytes = stamp.to_le_bytes();

        // A message shorter than a stamp cannot carry the round trip's own time;
        // the harness refuses those sizes rather than measuring something else.
        if let Some(head) = payload.get_mut(..bytes.len()) {
            head.copy_from_slice(&bytes);
        }

        payload
    }

    /// The stamp a payload carries, if it is long enough to carry one.
    #[must_use]
    pub fn stamp(payload: &[u8]) -> Option<i64> {
        let head = payload.get(..size_of::<i64>())?;
        let mut bytes = [0_u8; size_of::<i64>()];
        bytes.copy_from_slice(head);

        Some(i64::from_le_bytes(bytes))
    }
}

/// What a latency number is only comparable within.
///
/// Two runs on different machines, or on the same machine either side of a
/// kernel upgrade, are not two measurements of one thing — so a number recorded
/// without this is not a baseline, it is an anecdote. It goes into
/// `docs/benchmarks.md` next to the numbers it belongs to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MachineFingerprint {
    /// The CPU model, as the kernel reports it.
    pub cpu: String,
    /// How many threads the process may run at once.
    pub cpus: usize,
    /// The kernel release.
    pub kernel: String,
    /// The compiler this build was made with, as `rustc -V` reports it.
    pub rustc: String,
    /// `release` or `debug`, read from the build itself.
    pub profile: &'static str,
}

impl MachineFingerprint {
    /// Read the machine this is running on.
    ///
    /// Nothing here is fatal: a field that cannot be read is reported as
    /// unknown rather than failing the run, because a benchmark that refuses to
    /// measure because it could not name the CPU is worse than one that
    /// measures and says so.
    #[must_use]
    pub fn capture() -> Self {
        Self {
            cpu: cpu_model().unwrap_or_else(|| "unknown CPU".to_owned()),
            cpus: thread::available_parallelism().map_or(0, std::num::NonZeroUsize::get),
            kernel: command_output("uname", &["-r"]).unwrap_or_else(|| "unknown kernel".to_owned()),
            rustc: command_output("rustc", &["-V"]).unwrap_or_else(|| "unknown rustc".to_owned()),
            profile: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
        }
    }
}

impl fmt::Display for MachineFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}, {} CPUs, kernel {}, {}, {} build",
            self.cpu, self.cpus, self.kernel, self.rustc, self.profile
        )
    }
}

/// The part of a histogram a decision is made from.
///
/// The percentiles are the reference's own reporting choice, and the ones a
/// latency budget is written in; the mean is carried because a mean far above
/// the median is how a run says "something stalled" rather than "this is
/// slow".
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Summary {
    /// How many samples were recorded.
    pub count: u64,
    /// The arithmetic mean, in nanoseconds.
    pub mean: f64,
    /// Half the samples are at or below this, in nanoseconds.
    pub p50: u64,
    /// Nine tenths are.
    pub p90: u64,
    /// Ninety-nine hundredths are.
    pub p99: u64,
    /// And all but one in a thousand.
    pub p99_9: u64,
    /// The slowest sample recorded, as the histogram reports it.
    ///
    /// The top of the bucket the slowest sample fell in, not the sample: with
    /// three significant digits a histogram stores ranges, and every value it
    /// answers with is the highest one in the matching range
    /// (`hdrhistogram::Histogram::value_at_quantile`). That is the reference's
    /// own behaviour, and matching it is the point — a percentile from this
    /// histogram has to be the same number the reference's would report.
    pub max: u64,
}

impl Summary {
    /// Summarise a histogram of nanoseconds.
    #[must_use]
    pub fn of(histogram: &Histogram<u64>) -> Self {
        Self {
            count: histogram.len(),
            mean: histogram.mean(),
            p50: histogram.value_at_quantile(0.50),
            p90: histogram.value_at_quantile(0.90),
            p99: histogram.value_at_quantile(0.99),
            p99_9: histogram.value_at_quantile(0.999),
            max: histogram.max(),
        }
    }

    /// The row this summary is one line of, for `docs/benchmarks.md`.
    ///
    /// Nanoseconds are reported as microseconds, which is the unit the
    /// reference's own samples print in: they record nanoseconds and print with
    /// a scale of 1000 (`aeron-samples/src/main/c/cping.c:388`).
    #[must_use]
    pub fn markdown_row(&self, leading: &[&str]) -> String {
        let micros = |value: u64| format!("{:.1}", value as f64 / 1000.0);

        let mut cells: Vec<String> = leading.iter().map(|cell| (*cell).to_owned()).collect();
        cells.push(micros(self.p50));
        cells.push(micros(self.p90));
        cells.push(micros(self.p99));
        cells.push(micros(self.p99_9));
        cells.push(micros(self.max));
        cells.push(self.count.to_string());

        format!("| {} |", cells.join(" | "))
    }
}

impl fmt::Display for Summary {
    /// The human-readable form: microseconds, one line, percentiles in order.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "p50 {:.1}us  p90 {:.1}us  p99 {:.1}us  p99.9 {:.1}us  max {:.1}us  \
             mean {:.1}us  n {}",
            self.p50 as f64 / 1000.0,
            self.p90 as f64 / 1000.0,
            self.p99 as f64 / 1000.0,
            self.p99_9 as f64 / 1000.0,
            self.max as f64 / 1000.0,
            self.mean / 1000.0,
            self.count,
        )
    }
}

/// The model name the kernel reports for this CPU.
fn cpu_model() -> Option<String> {
    let text = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    let line = text.lines().find(|line| line.starts_with("model name"))?;
    let (_, value) = line.split_once(':')?;

    Some(value.trim().to_owned())
}

/// The first line of a command's stdout, trimmed, or `None` if it could not run.
fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;

    if !output.status.success() {
        return None;
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let first = text.lines().next()?.trim();

    if first.is_empty() {
        return None;
    }

    Some(first.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_histogram_has_the_bounds_the_reference_uses() {
        let mut histogram = histogram();
        assert_eq!(HDR_LOW, histogram.low());
        assert_eq!(HDR_HIGH, histogram.high());

        histogram.record(1_000).expect("in range");
    }

    #[test]
    fn a_summary_places_every_percentile_where_the_samples_put_it() {
        let mut histogram = histogram();

        // A hundred samples, one per microsecond: 1..=100 us. The median is the
        // fiftieth and the ninetieth is just under 90 us.
        for micros in 1..=100_u64 {
            histogram.record(micros * 1000).expect("in range");
        }

        let summary = Summary::of(&histogram);

        assert_eq!(100, summary.count);

        // Within a bucket, not exactly: three significant digits is the
        // reference's own resolution (`cping.c:353`), and a percentile reports
        // the highest value in the bucket the quantile lands in. Asserting the
        // exact sample would be asserting a precision this histogram does not
        // claim — and the point of these bounds is that ours and the
        // reference's report the same number for the same samples.
        for (percentile, expected) in [
            (summary.p50, 50_000),
            (summary.p90, 90_000),
            (summary.p99, 99_000),
            (summary.p99_9, 100_000),
            (summary.max, 100_000),
        ] {
            let drift = (percentile as f64 / expected as f64 - 1.0).abs();
            assert!(
                drift < 0.001,
                "{percentile} is not within a thousandth of {expected}"
            );
        }

        assert!(summary.p50 <= summary.p90 && summary.p90 <= summary.p99);
        assert!(summary.p99 <= summary.p99_9 && summary.p99_9 <= summary.max);
    }

    #[test]
    fn a_markdown_row_is_microseconds_and_ends_with_the_sample_count() {
        let mut histogram = histogram();
        histogram.record(12_345).expect("in range");

        let summary = Summary::of(&histogram);
        let row = summary.markdown_row(&["ours", "ours", "aeron:ipc", "32 B"]);
        let cells: Vec<&str> = row.split('|').map(str::trim).collect();

        // |, four labels, five percentiles, the count, and the trailing |
        assert_eq!(12, cells.len());
        assert_eq!(
            ["ours", "ours", "aeron:ipc", "32 B"],
            cells[1..5],
            "the caller's own columns come first, in order"
        );
        assert_eq!(
            format!("{:.1}", summary.p50 as f64 / 1000.0),
            cells[5],
            "nanoseconds are reported as microseconds"
        );
        assert_eq!("1", cells[10], "the sample count closes the row");
    }

    #[test]
    fn a_udp_scenario_names_the_endpoint_both_ends_are_given() {
        assert_eq!("aeron:ipc", Channel::Ipc.uri(0));
        assert_eq!(
            "aeron:udp?endpoint=localhost:40456",
            Channel::Udp.uri(40456)
        );
    }

    #[test]
    fn a_payload_carries_the_stamp_the_reply_is_measured_against() {
        let scenario = Scenario {
            channel: Channel::Ipc,
            length: 32,
        };

        assert_eq!(Some(1234), Scenario::stamp(&scenario.payload(1234)));
        assert_eq!(32, scenario.payload(0).len());
    }

    #[test]
    fn a_message_too_short_for_a_stamp_carries_none_rather_than_a_truncated_one() {
        let scenario = Scenario {
            channel: Channel::Ipc,
            length: 4,
        };

        assert_eq!(None, Scenario::stamp(&scenario.payload(1234)));
    }

    #[test]
    fn the_default_matrix_is_both_channels_at_both_sizes() {
        let scenarios = Scenario::all();

        assert_eq!(4, scenarios.len());
        assert!(scenarios.contains(&Scenario {
            channel: Channel::Udp,
            length: 1024
        }));
    }

    #[test]
    fn a_fingerprint_names_the_machine_it_was_taken_on() {
        let fingerprint = MachineFingerprint::capture();

        // The CPU line is Linux-specific and this suite only runs on Linux; the
        // other fields have fallbacks, so only this one is asserted on.
        assert!(!fingerprint.cpu.is_empty());
        assert!(fingerprint.to_string().contains("CPUs"));
    }
}
