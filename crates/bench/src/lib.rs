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
use std::path::PathBuf;
use std::process::Command;
use std::thread;

use hdrhistogram::Histogram;

/// The reference's `LoadTestRig`, rebuilt here.
///
/// A second instrument with a different meaning from everything else in this
/// crate — see the module's own documentation for why the two are never
/// reported side by side.
pub mod loadtest;

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

/// What a run is asking for: a timed round trip, or a counted one-way stream.
///
/// The two are different measurements and are written down separately — a
/// round trip is a distribution and a one-way stream is a rate — so which one
/// is being asked for is the first thing a run says about itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// A message out and back, timed.
    PingPong,
    /// Messages in one direction, counted.
    Throughput,
}

/// Who produces the numbers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Peer {
    /// This build's client.
    Ours,
    /// The reference's own `Ping` and `Pong`, C++ samples over the C client.
    Reference,
    /// The reference's `Ping` and `Pong` from its **Java** build.
    ///
    /// A different instrument, not a different build of the same one: the Java
    /// samples take their settings from `aeron.sample.*` system properties
    /// rather than a command line (`SampleConfiguration.java:28-45`), and the
    /// client underneath them is the Java client. It is here as a reference
    /// baseline of its own — the Java pair, on the Java driver — and not as a
    /// combination with anything of ours.
    ReferenceJava,
    /// Both, side by side in one table.
    Both,
}

impl Peer {
    /// Whether this choice runs our own instrument.
    #[must_use]
    pub const fn measures_ours(self) -> bool {
        matches!(self, Self::Ours | Self::Both)
    }

    /// Whether it runs the reference's C/C++ instrument.
    #[must_use]
    pub const fn measures_reference(self) -> bool {
        matches!(self, Self::Reference | Self::Both)
    }

    /// Whether it runs the reference's Java instrument.
    #[must_use]
    pub const fn measures_reference_java(self) -> bool {
        matches!(self, Self::ReferenceJava)
    }
}

/// Which driver both instruments measure against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WhichDriver {
    /// `deepmsg-driver`, this build's.
    Own,
    /// The reference's `aeronmd`, which is the C driver.
    Reference,
    /// The reference's Java media driver (`io.aeron.driver.MediaDriver`).
    ReferenceJava,
}

/// A whole run's arguments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Args {
    /// The transports to measure.
    pub channels: Vec<Channel>,
    /// The payload sizes to measure, on every channel.
    pub lengths: Vec<usize>,
    /// Messages per scenario: round trips when timed, messages published when
    /// counted.
    pub messages: u64,
    /// Unmeasured round trips before the timed ones.
    pub warmup: u64,
    /// Which measurement.
    pub mode: Mode,
    /// Who measures.
    pub peer: Peer,
    /// Which driver they measure against.
    pub driver: WhichDriver,
    /// A driver binary the caller named.
    pub driver_binary: Option<PathBuf>,
    /// Whether to print rows for `docs/benchmarks.md`.
    pub markdown: bool,
}

/// One end of a measurement, as the child process sees it.
///
/// Run as a child of the measuring process rather than as a binary of its own,
/// so the two ends are one build and the arguments that pair them are written
/// once — and so a child can be handed the channels its parent chose instead of
/// deriving its own, which would land on other ports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildArgs {
    /// The aeron directory to connect to.
    pub aeron_dir: PathBuf,
    /// The channel this end subscribes to.
    pub ping_uri: String,
    /// The channel this end publishes on.
    pub pong_uri: String,
    /// The stream it subscribes to, which an IPC channel must vary per row.
    pub ping_stream_id: i32,
    /// The stream it publishes on, likewise.
    pub pong_stream_id: i32,
    /// How many messages a counting end waits for.
    pub count: Option<u64>,
}

/// What an invocation of the harness is: a measurement, or one end of one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Invocation {
    /// Measure.
    Measure(Args),
    /// Mirror whatever arrives.
    Echo(ChildArgs),
    /// Count what arrives, and say so.
    Sink(ChildArgs),
}

impl Invocation {
    /// The harness's own usage, kept next to the parser so they cannot drift.
    pub const USAGE: &'static str = "\
usage: cargo bench -p deepmsg-bench --bench latency -- [options]

  --channel ipc|udp|all   transport to measure (default: all)
  --length N|all          payload bytes (default: all: 32, 1024)
  --messages N            round trips, or messages to publish (default: 20000)
  --warmup N              unmeasured round trips first (default: 2000)
  --mode ping-pong|throughput
                          a timed round trip, or a counted one-way stream
  --peer ours|reference|reference-java|both
                          who measures: this build's client, the reference's own
                          Ping and Pong (C++), its Java Ping and Pong, or ours
                          and the C++ one in one table (default: ours)
  --driver own|reference|reference-java
                          which driver they run against (default: own).
                          The Java peer and the Java driver go together: the
                          only Java combination this harness runs.
  --driver-binary PATH    the driver to start (default: target/release/deepmsg-driver)
  --md                    print the rows as markdown, for docs/benchmarks.md
  --echo DIR --ping URI --pong URI --ping-stream N --pong-stream N
                          internal: run as the echoing end
  --sink DIR --ping URI --pong URI --ping-stream N --pong-stream N --count N
                          internal: run as the counting end";

    /// Parse the arguments, or say what was wrong with them.
    ///
    /// # Errors
    ///
    /// A message naming the argument that could not be read, or the
    /// combination that does not make sense.
    pub fn parse(mut args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut measure = Args {
            channels: Channel::ALL.to_vec(),
            lengths: vec![32, 1024],
            messages: 20_000,
            warmup: 2_000,
            mode: Mode::PingPong,
            peer: Peer::Ours,
            driver: WhichDriver::Own,
            driver_binary: None,
            markdown: false,
        };
        let (mut echo, mut sink) = (false, false);
        let (mut aeron_dir, mut ping, mut pong, mut count) = (None, None, None, None);
        let (mut ping_stream_id, mut pong_stream_id) = (None, None);

        while let Some(arg) = args.next() {
            let mut value = || args.next().ok_or_else(|| format!("{arg} needs a value"));

            match arg.as_str() {
                "--channel" => measure.channels = parse_channels(&value()?)?,
                "--length" => measure.lengths = parse_lengths(&value()?)?,
                "--messages" => measure.messages = parse_number(&value()?, &arg)?,
                "--warmup" => measure.warmup = parse_number(&value()?, &arg)?,
                "--mode" => {
                    measure.mode = match value()?.as_str() {
                        "ping-pong" => Mode::PingPong,
                        "throughput" => Mode::Throughput,
                        other => return Err(format!("unknown mode {other:?}")),
                    };
                }
                "--peer" => {
                    measure.peer = match value()?.as_str() {
                        "ours" => Peer::Ours,
                        "reference" => Peer::Reference,
                        "reference-java" => Peer::ReferenceJava,
                        "both" => Peer::Both,
                        other => return Err(format!("unknown peer {other:?}")),
                    };
                }
                "--driver" => {
                    measure.driver = match value()?.as_str() {
                        "own" => WhichDriver::Own,
                        "reference" => WhichDriver::Reference,
                        "reference-java" => WhichDriver::ReferenceJava,
                        other => return Err(format!("unknown driver {other:?}")),
                    };
                }
                "--driver-binary" => measure.driver_binary = Some(PathBuf::from(value()?)),
                "--md" => measure.markdown = true,
                "--echo" => {
                    echo = true;
                    aeron_dir = Some(PathBuf::from(value()?));
                }
                "--sink" => {
                    sink = true;
                    aeron_dir = Some(PathBuf::from(value()?));
                }
                "--ping" => ping = Some(value()?),
                "--pong" => pong = Some(value()?),
                "--ping-stream" => ping_stream_id = Some(parse_number(&value()?, &arg)?),
                "--pong-stream" => pong_stream_id = Some(parse_number(&value()?, &arg)?),
                "--count" => count = Some(parse_number(&value()?, &arg)?),
                // `cargo bench` appends its own flags to whatever the caller
                // wrote after `--`, and `--bench` is one of them. It means
                // "you are the benchmark", which this target already knows; a
                // harness that failed on it could not be run through cargo at
                // all.
                "--bench" | "--test" => {}
                other => return Err(format!("unknown argument {other:?}")),
            }
        }

        if echo || sink {
            let (Some(aeron_dir), Some(ping), Some(pong)) = (aeron_dir, ping, pong) else {
                return Err("--echo and --sink need a directory, --ping and --pong".to_owned());
            };
            let (Some(ping_stream_id), Some(pong_stream_id)) = (ping_stream_id, pong_stream_id)
            else {
                return Err("--echo and --sink need --ping-stream and --pong-stream".to_owned());
            };

            if sink && count.is_none() {
                return Err("--sink needs a --count".to_owned());
            }

            let child = ChildArgs {
                aeron_dir,
                ping_uri: ping,
                pong_uri: pong,
                ping_stream_id,
                pong_stream_id,
                count,
            };

            return Ok(if echo {
                Self::Echo(child)
            } else {
                Self::Sink(child)
            });
        }

        if measure
            .lengths
            .iter()
            .any(|length| *length < size_of::<i64>())
        {
            return Err(format!(
                "a payload shorter than {} bytes cannot carry the round trip's own stamp",
                size_of::<i64>()
            ));
        }

        if measure.messages == 0 {
            return Err("--messages 0 would measure nothing".to_owned());
        }

        let java_pair = (
            measure.peer.measures_reference_java(),
            measure.driver == WhichDriver::ReferenceJava,
        );

        if java_pair.0 != java_pair.1 {
            return Err(
                "--peer reference-java and --driver reference-java go together: the Java pair is \
                 the one Java combination this harness runs. A Java client against this build's \
                 driver is a compatibility question no test here has answered, and against the C \
                 driver it is a different experiment."
                    .to_owned(),
            );
        }

        Ok(Self::Measure(measure))
    }
}

/// The channels named on the command line.
fn parse_channels(text: &str) -> Result<Vec<Channel>, String> {
    match text {
        "all" => Ok(Channel::ALL.to_vec()),
        "ipc" => Ok(vec![Channel::Ipc]),
        "udp" => Ok(vec![Channel::Udp]),
        other => Err(format!("unknown channel {other:?}")),
    }
}

/// The payload sizes named on the command line.
fn parse_lengths(text: &str) -> Result<Vec<usize>, String> {
    match text {
        "all" => Ok(vec![32, 1024]),
        other => Ok(vec![parse_number(other, "--length")?]),
    }
}

/// A number argument, or why it is not one.
fn parse_number<T: std::str::FromStr>(text: &str, name: &str) -> Result<T, String> {
    text.parse()
        .map_err(|_| format!("{name} needs a number, got {text:?}"))
}

/// The percentiles out of the table `hdr_percentiles_print` writes.
///
/// The reference's own instrument prints this table
/// (`aeron-samples/src/main/c/cping.c:388`), and reading it is what makes one
/// table's p99 comparable with the other's. Each row carries the value and the
/// **cumulative count** of samples at or below it, so a quantile read out of
/// the counts is the same number this crate's own histogram would report for
/// the same samples — which is why the counts are used and not the percentile
/// column, whose rows are at `1 - 2^-k` rather than at the round numbers a
/// latency budget is written in.
///
/// The values are printed scaled by 1000 (`cping.c:388`), so they arrive in
/// microseconds; they are scaled back, because every [`Summary`] holds
/// nanoseconds.
///
/// # Errors
///
/// A message naming what was missing when the output holds no table — a `Ping`
/// that failed to start prints `aeron_init:` and nothing else, and a zero
/// reported for it would read as a very fast run.
pub fn summary_from_hdr_table(output: &str) -> Result<Summary, String> {
    let mut samples: Vec<(u64, u64)> = Vec::new();

    for line in output.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();

        // value, percentile, cumulative count, 1/(1-percentile) — the classic
        // layout, with the last column dropped on the final row.
        let (Some(value), Some(count)) = (fields.first(), fields.get(2)) else {
            continue;
        };
        let (Ok(value), Ok(count)) = (value.parse::<f64>(), count.parse::<u64>()) else {
            continue;
        };

        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        samples.push(((value * 1000.0) as u64, count));
    }

    let total = samples.last().map_or(0, |(_, count)| *count);
    if total == 0 {
        return Err(format!(
            "the reference's output had no percentile table in it:\n{output}"
        ));
    }

    // The lowest value whose cumulative count has reached the quantile — the
    // definition of a percentile, applied to the counts the table gives.
    let at = |quantile: f64| -> u64 {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let target = (total as f64 * quantile) as u64;

        samples
            .iter()
            .find(|(_, count)| *count >= target)
            .map_or(0, |(value, _)| *value)
    };

    // The table's counts are cumulative, so each row's own weight is what it
    // adds to the row before it.
    let mut mean = 0.0;
    let mut previous = 0_u64;
    for (value, count) in &samples {
        mean += *value as f64 * count.saturating_sub(previous) as f64;
        previous = *count;
    }

    Ok(Summary {
        count: total,
        mean: mean / total as f64,
        p50: at(0.50),
        p90: at(0.90),
        p99: at(0.99),
        p99_9: at(0.999),
        max: samples.last().map_or(0, |(value, _)| *value),
    })
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

    #[test]
    fn the_default_run_is_the_whole_matrix() {
        let Ok(Invocation::Measure(args)) = Invocation::parse(std::iter::empty()) else {
            panic!("no arguments should be a measurement");
        };

        assert_eq!(Channel::ALL.to_vec(), args.channels);
        assert_eq!(vec![32, 1024], args.lengths);
        assert_eq!(Mode::PingPong, args.mode);
        assert!(args.peer.measures_ours() && !args.peer.measures_reference());
    }

    #[test]
    fn a_payload_too_short_for_a_stamp_is_refused_before_anything_starts() {
        let parsed = Invocation::parse(["--length", "4"].map(String::from).into_iter());

        assert!(parsed.is_err(), "a 4-byte payload cannot carry the stamp");
    }

    #[test]
    fn cargos_own_bench_flag_is_not_an_argument() {
        // `cargo bench` appends `--bench` to whatever the caller wrote.
        let parsed = Invocation::parse(["--bench"].map(String::from).into_iter());

        assert!(parsed.is_ok(), "cargo's own flag must not be refused");
    }

    #[test]
    fn a_child_invocation_carries_the_channels_it_was_given() {
        let parsed = Invocation::parse(
            [
                "--echo",
                "/tmp/dir",
                "--ping",
                "aeron:ipc",
                "--pong",
                "aeron:ipc",
                "--ping-stream",
                "1002",
                "--pong-stream",
                "1003",
            ]
            .map(String::from)
            .into_iter(),
        );

        match parsed {
            Ok(Invocation::Echo(child)) => {
                assert_eq!(PathBuf::from("/tmp/dir"), child.aeron_dir);
                assert_eq!("aeron:ipc", child.ping_uri);
                assert_eq!(1002, child.ping_stream_id);
            }
            other => panic!("an --echo invocation should parse as an echo: {other:?}"),
        }
    }

    #[test]
    fn a_sink_without_a_count_is_refused() {
        let parsed = Invocation::parse(
            [
                "--sink",
                "/tmp/dir",
                "--ping",
                "aeron:ipc",
                "--pong",
                "aeron:ipc",
                "--ping-stream",
                "1002",
                "--pong-stream",
                "1003",
            ]
            .map(String::from)
            .into_iter(),
        );

        assert!(parsed.is_err(), "a sink without a count would never stop");
    }

    #[test]
    fn the_references_percentile_table_is_read_by_its_counts() {
        // The shape `hdr_percentiles_print` writes, values in microseconds.
        let table = "\
       Value     Percentile TotalCount 1/(1-Percentile)

       4.000 0.000000000000      1
       5.000 0.500000000000    500  2.00
       6.000 0.900000000000    900  10.00
       7.000 0.990000000000    990  100.00
       8.000 1.000000000000   1000
";

        let summary = summary_from_hdr_table(table).expect("a table");

        assert_eq!(1000, summary.count);
        // Back to nanoseconds, which is what every other row holds.
        assert_eq!(5_000, summary.p50);
        assert_eq!(6_000, summary.p90);
        assert_eq!(7_000, summary.p99);
        // With a thousand samples the 99.9th target is the 999th, which only
        // the last row has reached — so p99.9 is the maximum here, and would
        // not be for a table with more rows.
        assert_eq!(8_000, summary.p99_9);
        assert_eq!(8_000, summary.max);
    }

    #[test]
    fn output_with_no_table_in_it_is_a_failure_rather_than_a_zero() {
        assert!(summary_from_hdr_table("aeron_init: no such file").is_err());
    }
}
