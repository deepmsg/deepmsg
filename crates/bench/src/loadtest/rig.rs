//! The measurement itself: how fast messages are meant to go out, and what came
//! back.
//!
//! Mirrors `benchmarks-api/.../LoadTestRig.java`. This is the part of the port
//! where being *nearly* right is being wrong: the cadence, the meaning of a
//! timestamp, the order the clock is read in and the count of times it is read
//! all end up in the numbers a run reports, and a run that differs from the
//! reference's by any of them is measuring something else.
//!
//! # The shape of `send`, and the four things in it that are easy to get wrong
//!
//! A run of `iterations` one-second iterations sends `iterations × rate`
//! messages, pacing them by a *planned* timestamp rather than by the clock. The
//! inner loop waits out each interval while draining replies; when the interval
//! is up it sends the next batch.
//!
//! - **`timestamp` is when a message was meant to go out**, not when it did. It
//!   only ever advances by `sendIntervalNs`, never corrected against the clock —
//!   which is what puts queueing time inside the round trip, and is the
//!   reference's definition rather than this port's.
//! - **The clock is read after the "was that everything?" check**, so the batch
//!   that finishes the run is not timed at all. Reading it first is a
//!   plausibly-nicer arrangement that shifts every reported number.
//! - **A partial batch does not advance the timestamp.** The batch size shrinks
//!   by what went, and the next attempt is for the same instant.
//! - **`sendIntervalNs` is an integer division of `1e9 × batch` by the rate**,
//!   so a rate that does not divide evenly sends slightly more than asked for
//!   and the *duration* stays bounded. The reference says so in a comment; it is
//!   not an accident to be rounded away.

use std::fmt;
use std::io::{self, Write};

use crate::loadtest::config::Configuration;
use crate::loadtest::format::{four_decimals, grouped};
use crate::loadtest::progress::ProgressReporter;
use crate::loadtest::recorder::Recorder;
use crate::loadtest::result::{self, Status};
use crate::loadtest::transceiver::{Clock, Idle, MessageTransceiver, TransceiverError};

/// The nanosecond in a second.
const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// The nanosecond in a millisecond, for the send grace.
const NANOS_PER_MILLI: i64 = 1_000_000;

/// What a run of one rate and one iteration count produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SendResult {
    /// How many messages went out.
    pub sent_messages: i64,
    /// How many came back within the receive deadline.
    pub received_messages: i64,
}

impl SendResult {
    /// Which of the two words describes this run
    /// (`LoadTestRig.java:423-426`).
    ///
    /// Strictly all three counts agreeing: a run that sent everything and got
    /// everything back is `Ok`, and anything else is `Fail` — and a failing run
    /// still has its numbers written down, marked.
    #[must_use]
    pub fn status(self, expected_number_of_messages: i64) -> Status {
        if expected_number_of_messages == self.sent_messages
            && expected_number_of_messages == self.received_messages
        {
            Status::Ok
        } else {
            Status::Fail
        }
    }
}

/// What a run can fail at.
#[derive(Debug)]
pub enum RigError {
    /// A line could not be written.
    Output(io::Error),
    /// The system under test refused something.
    Transceiver(TransceiverError),
}

impl fmt::Display for RigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Output(error) => write!(f, "could not write a line of output: {error}"),
            Self::Transceiver(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for RigError {}

impl From<io::Error> for RigError {
    fn from(error: io::Error) -> Self {
        Self::Output(error)
    }
}

/// A measurement of one configuration against one transceiver.
///
/// Five type parameters, which is four more than a caller wants to name: the
/// transceiver, its clock, the idle strategy, the progress reporter and where
/// the output goes. They are all static because three of them are on the path
/// of every message. [`LoadTestRig::new`] is what a caller should reach for.
pub struct LoadTestRig<T, C, I, P, W>
where
    T: MessageTransceiver<C>,
    C: Clock,
    I: Idle,
    P: ProgressReporter,
    W: Write,
{
    configuration: Configuration,
    transceiver: T,
    recorder: Recorder<C>,
    idle: I,
    progress: P,
    out: W,
    receive_deadline_ns: i64,
}

impl<T, C, I, P, W> LoadTestRig<T, C, I, P, W>
where
    T: MessageTransceiver<C>,
    C: Clock,
    I: Idle,
    P: ProgressReporter,
    W: Write,
{
    /// A rig that will drive `transceiver`, time it against the clock inside
    /// `recorder`, and say what it finds through `out`.
    ///
    /// The idle strategy is taken by value from the configuration: the one in
    /// there is pristine, and a run mutates its own copy.
    #[must_use]
    pub fn new(
        configuration: Configuration,
        transceiver: T,
        recorder: Recorder<C>,
        idle: I,
        progress: P,
        out: W,
    ) -> Self {
        let receive_deadline_ns =
            i64::from(configuration.receive_deadline_seconds()) * NANOS_PER_SECOND;

        Self {
            configuration,
            transceiver,
            recorder,
            idle,
            progress,
            out,
            receive_deadline_ns,
        }
    }

    /// Run the warmup and the measurement, and say whether the run met its
    /// target.
    ///
    /// The transceiver is torn down however the run went, which is what the
    /// reference's `finally` is for. A panic is the exception: `panic = "abort"`
    /// is how the rig is built for a run (see the plan's Q9), so a checksum that
    /// does not match — which the reference throws on — ends the process rather
    /// than unwinding towards a `destroy`.
    ///
    /// # Errors
    ///
    /// [`RigError`] when the transceiver refuses to start, when a line cannot be
    /// written, or when it refuses to stop. A failure in the run itself is not
    /// an error: it is a [`Status::Fail`], and the numbers are written down
    /// either way.
    pub fn run(&mut self) -> Result<Status, RigError> {
        let measured = self.measure();
        let destroyed = self.transceiver.destroy();

        match (measured, destroyed) {
            (Ok(status), Ok(())) => Ok(status),
            (Ok(_), Err(error)) => Err(RigError::Transceiver(error)),
            // The run's own failure is the one worth reporting; a teardown that
            // also failed is downstream of it.
            (Err(error), _) => Err(error),
        }
    }

    /// The body of [`LoadTestRig::run`], without the teardown.
    fn measure(&mut self) -> Result<Status, RigError> {
        writeln!(
            self.out,
            "\nStarting latency benchmark using the following configuration:\n{}\n",
            self.configuration
        )?;

        self.transceiver
            .init(&self.configuration)
            .map_err(RigError::Transceiver)?;

        if self.configuration.warmup_iterations() > 0 {
            writeln!(
                self.out,
                "\nRunning warmup for {} iterations of {} messages each, with {} bytes payload and a burst \
                 size of {}...",
                grouped(i64::from(self.configuration.warmup_iterations())),
                grouped(i64::from(self.configuration.warmup_message_rate())),
                grouped(i64::from(self.configuration.message_length())),
                grouped(i64::from(self.configuration.batch_size())),
            )?;

            self.transceiver.on_benchmark_start(true);
            self.send(
                self.configuration.warmup_iterations(),
                self.configuration.warmup_message_rate(),
            );
            self.transceiver.on_benchmark_complete(true);

            self.recorder.reset();
            self.progress.reset();
        }

        writeln!(
            self.out,
            "\nRunning measurement for {} iterations of {} messages each, with {} bytes payload and a burst \
             size of {}...",
            grouped(i64::from(self.configuration.iterations())),
            grouped(i64::from(self.configuration.message_rate())),
            grouped(i64::from(self.configuration.message_length())),
            grouped(i64::from(self.configuration.batch_size())),
        )?;

        self.transceiver.on_benchmark_start(false);
        let result = self.send(
            self.configuration.iterations(),
            self.configuration.message_rate(),
        );
        self.transceiver.on_benchmark_complete(false);

        self.progress.reset();

        let unit = self.configuration.output_time_unit();
        writeln!(self.out, "\nHistogram of RTT latencies in {unit}.")?;
        result::output_percentile_distribution(
            self.recorder.histogram(),
            &mut self.out,
            unit.scale_ratio(),
        )?;

        let expected = i64::from(self.configuration.iterations())
            * i64::from(self.configuration.message_rate());
        self.warn_if_target_rate_not_achieved(result, expected)?;

        let status = result.status(expected);
        result::save_to_file(
            self.recorder.histogram(),
            self.configuration.output_directory(),
            self.configuration.output_file_name_prefix(),
            status,
        )?;

        Ok(status)
    }

    /// `LoadTestRig.send` (`:191-311`).
    ///
    /// Returns how many messages went out and how many came back; it does not
    /// judge them, because the caller knows how many there should have been.
    pub fn send(&mut self, iterations: u32, number_of_messages: u32) -> SendResult {
        let burst_size = i64::from(self.configuration.batch_size());
        let message_size =
            usize::try_from(self.configuration.message_length()).unwrap_or(usize::MAX);

        // Multiplied before it is divided, and left as an integer: the reference
        // says why in a comment — a rate that does not divide evenly sends a
        // message or two over, which is what keeps the *duration* at `iterations`
        // seconds instead of stretching it.
        let send_interval_ns = NANOS_PER_SECOND * burst_size / i64::from(number_of_messages);
        let total_number_of_messages = i64::from(iterations) * i64::from(number_of_messages);
        let start_time_ns = self.recorder.nano_time();
        let stop_time_ns = start_time_ns + i64::from(iterations) * NANOS_PER_SECOND;
        let send_deadline_ns =
            stop_time_ns + i64::from(self.configuration.send_grace_millis()) * NANOS_PER_MILLI;

        let mut sent_messages = 0_i64;
        let mut now_ns = start_time_ns;
        let mut timestamp_ns = start_time_ns;
        let mut next_report_time_ns = start_time_ns + NANOS_PER_SECOND;

        let mut batch_size = total_number_of_messages.min(burst_size);

        while sent_messages < total_number_of_messages {
            let checksum = self.recorder.checksum();
            let sent = self.transceiver.send(
                usize::try_from(batch_size).unwrap_or(usize::MAX),
                message_size,
                timestamp_ns,
                checksum,
                &mut self.recorder,
            );
            let sent = i64::try_from(sent).unwrap_or(i64::MAX);
            sent_messages += sent;

            // Before the clock is read: the batch that finishes the run is not
            // timed, and a run that reads the clock first reports different
            // numbers for the same traffic.
            if total_number_of_messages == sent_messages {
                self.progress
                    .report_progress(start_time_ns, now_ns, sent_messages, iterations);
                break;
            }

            now_ns = self.recorder.nano_time();

            if sent == batch_size {
                batch_size = (total_number_of_messages - sent_messages).min(burst_size);
                timestamp_ns += send_interval_ns;

                // Wait out this interval, draining whatever has come back.
                let mut received_message_count = 0_i64;
                while now_ns < timestamp_ns && now_ns < stop_time_ns {
                    if now_ns >= next_report_time_ns {
                        self.progress.report_progress(
                            start_time_ns,
                            now_ns,
                            sent_messages,
                            iterations,
                        );
                        next_report_time_ns += NANOS_PER_SECOND;
                    }

                    if received_message_count < sent_messages {
                        self.transceiver.receive(&mut self.recorder);
                        let new_received_message_count = self.recorder.received_messages();

                        if new_received_message_count == received_message_count {
                            self.idle.idle();
                        } else {
                            received_message_count = new_received_message_count;
                            self.idle.reset();
                        }
                    } else {
                        self.idle.idle();
                    }

                    now_ns = self.recorder.nano_time();
                }
            } else {
                // Some of the batch did not fit. The next attempt is for what is
                // left of it, at the same instant.
                batch_size -= sent;
                self.transceiver.receive(&mut self.recorder);
            }

            if now_ns >= send_deadline_ns {
                break;
            }

            if now_ns >= stop_time_ns {
                // Inside the grace window the pacing loop above no longer runs,
                // so replies have to be drained here or their round trips would
                // be measured by the loop after this one.
                self.transceiver.receive(&mut self.recorder);
            }

            if now_ns >= next_report_time_ns {
                self.progress
                    .report_progress(start_time_ns, now_ns, sent_messages, iterations);
                next_report_time_ns += NANOS_PER_SECOND;
            }
        }

        self.idle.reset();

        let mut received_message_count = self.recorder.received_messages();
        let deadline = self.recorder.nano_time() + self.receive_deadline_ns;

        while received_message_count < sent_messages {
            self.transceiver.receive(&mut self.recorder);
            let new_received_message_count = self.recorder.received_messages();

            if new_received_message_count == received_message_count {
                self.idle.idle();
                if self.recorder.nano_time() >= deadline {
                    break;
                }
            } else {
                received_message_count = new_received_message_count;
                self.idle.reset();
            }
        }

        SendResult {
            sent_messages,
            received_messages: received_message_count,
        }
    }

    /// The two complaints a run can end with, word for word
    /// (`LoadTestRig.java:313-335`).
    fn warn_if_target_rate_not_achieved(
        &mut self,
        result: SendResult,
        expected_number_of_messages: i64,
    ) -> Result<(), RigError> {
        if expected_number_of_messages != result.sent_messages {
            let loss =
                100.0 - (100.0 * result.sent_messages as f64 / expected_number_of_messages as f64);
            writeln!(
                self.out,
                "\n*** WARNING: Target message rate not achieved: expected to send {} messages in total but \
                 managed to send only {} messages (loss {}%)!",
                grouped(expected_number_of_messages),
                grouped(result.sent_messages),
                four_decimals(loss),
            )?;
        }

        if result.sent_messages != result.received_messages {
            let loss =
                100.0 - (100.0 * result.received_messages as f64 / result.sent_messages as f64);
            writeln!(
                self.out,
                "\n*** WARNING: Not all messages were received after {}s deadline: expected {} vs received \
                 {} (loss {}%)!",
                self.receive_deadline_ns / NANOS_PER_SECOND,
                grouped(result.sent_messages),
                grouped(result.received_messages),
                four_decimals(loss),
            )?;
        }

        Ok(())
    }
}
