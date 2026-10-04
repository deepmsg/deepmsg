//! What a run says about itself while it is running.
//!
//! Mirrors `ProgressReporter` and `AsyncProgressReporter`. The reference's is a
//! daemon thread reading a queue, so that the thread doing the measuring never
//! waits on a `printf`; this one collects the same lines and writes them when
//! the sending is over, which keeps the measuring thread clear of I/O by not
//! having a second thread at all.
//!
//! That is a difference in *when* the lines appear and not in what they say. It
//! is also off by default — `io.aeron.benchmarks.report.progress` is false, and
//! no grid turns it on — so it is not part of anything that gets measured.

use std::io::Write;

use crate::loadtest::format::grouped;

/// The nanosecond in a second, which is what turns readings into a rate.
const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// One `Send rate:` line, as it was reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Progress {
    /// When the run started, so the reader can work out how far in it is.
    pub start_time_ns: i64,
    /// When the report was made.
    pub now_ns: i64,
    /// How many messages had gone out by then.
    pub sent_messages: i64,
    /// How many one-second iterations the run is for.
    pub iterations: u32,
}

impl Progress {
    /// `AsyncProgressReporter.reportProgress`'s arithmetic, including that a
    /// report made inside the first second counts as one second rather than
    /// dividing by zero.
    #[must_use]
    fn line(&self) -> String {
        let elapsed_seconds =
            ((self.now_ns - self.start_time_ns) as f64 / NANOS_PER_SECOND as f64).round() as i64;
        let elapsed_seconds = elapsed_seconds.max(1);
        let send_rate = self.sent_messages / elapsed_seconds;

        format!(
            "Send rate: {} msgs/sec ({} of {})",
            grouped(send_rate),
            elapsed_seconds,
            self.iterations
        )
    }
}

/// Where a run's progress goes.
pub trait ProgressReporter {
    /// The run has sent `sent_messages` so far, `now_ns - start_time_ns` in.
    fn report_progress(
        &mut self,
        start_time_ns: i64,
        now_ns: i64,
        sent_messages: i64,
        iterations: u32,
    );

    /// Finish with what has been reported.
    ///
    /// The reference's `reset` waits for its queue to drain, so a caller can
    /// take it as "everything reported so far has been dealt with". It is called
    /// between the warmup and the measurement as well as after them, and in
    /// between it is also what throws away the warmup's lines.
    fn reset(&mut self);
}

/// Reports nothing, which is what a run does unless it is asked to.
#[derive(Clone, Copy, Debug, Default)]
pub struct NullProgressReporter;

impl ProgressReporter for NullProgressReporter {
    fn report_progress(
        &mut self,
        _start_time_ns: i64,
        _now_ns: i64,
        _sent_messages: i64,
        _iterations: u32,
    ) {
    }

    fn reset(&mut self) {}
}

/// Collects the lines and writes them out on [`ProgressReporter::reset`].
///
/// The buffer is reserved up front: a run reports once a second, so a minute-long
/// measurement adds sixty of these, and reserving for the iterations the
/// configuration already names costs nothing and keeps the reporting path free
/// of allocation anyway.
///
/// Public because a test wants to read what a run reported, and this is already
/// the thing that remembers it.
#[derive(Debug)]
pub struct DeferredProgressReporter<W: Write> {
    out: W,
    reported: Vec<Progress>,
}

impl<W: Write> DeferredProgressReporter<W> {
    /// A reporter that will write to `out`, with room for `iterations` lines.
    #[must_use]
    pub fn new(out: W, iterations: u32) -> Self {
        Self {
            out,
            reported: Vec::with_capacity(usize::try_from(iterations).unwrap_or(0) + 1),
        }
    }

    /// What has been reported since the last reset.
    #[must_use]
    pub fn reported(&self) -> &[Progress] {
        &self.reported
    }
}

impl<W: Write> ProgressReporter for DeferredProgressReporter<W> {
    fn report_progress(
        &mut self,
        start_time_ns: i64,
        now_ns: i64,
        sent_messages: i64,
        iterations: u32,
    ) {
        self.reported.push(Progress {
            start_time_ns,
            now_ns,
            sent_messages,
            iterations,
        });
    }

    fn reset(&mut self) {
        for progress in self.reported.drain(..) {
            // A report that cannot be written is not a reason to abandon a
            // measurement that has already been taken.
            let _ = writeln!(self.out, "{}", progress.line());
        }

        // The reference's `%n` is a line separator, which is what `writeln!`
        // gives on every platform this runs on.
        let _ = self.out.flush();
    }
}

/// Which of the reporters a configuration asks for.
///
/// `report.progress` is a setting and a setting cannot choose a type, so the
/// choice is an enum and the rig stays generic over the one it is handed.
#[derive(Debug)]
pub enum Reporter<W: Write> {
    /// Reports nothing, which is the default.
    Null(NullProgressReporter),
    /// Collects the lines and writes them when the sending is done.
    Deferred(DeferredProgressReporter<W>),
}

impl<W: Write> Reporter<W> {
    /// The reporter a configuration asks for: quiet unless it says otherwise.
    ///
    /// The buffer is reserved for the iterations the run has already declared,
    /// so even the reporting path does not allocate as it goes.
    #[must_use]
    pub fn of(configuration: &crate::loadtest::config::Configuration, out: W) -> Self {
        if configuration.report_progress() {
            Self::Deferred(DeferredProgressReporter::new(
                out,
                configuration.iterations(),
            ))
        } else {
            Self::Null(NullProgressReporter)
        }
    }
}

impl<W: Write> ProgressReporter for Reporter<W> {
    fn report_progress(
        &mut self,
        start_time_ns: i64,
        now_ns: i64,
        sent_messages: i64,
        iterations: u32,
    ) {
        match self {
            Self::Null(reporter) => {
                reporter.report_progress(start_time_ns, now_ns, sent_messages, iterations);
            }
            Self::Deferred(reporter) => {
                reporter.report_progress(start_time_ns, now_ns, sent_messages, iterations);
            }
        }
    }

    fn reset(&mut self) {
        match self {
            Self::Null(reporter) => reporter.reset(),
            Self::Deferred(reporter) => reporter.reset(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_report_names_the_rate_and_how_far_in_the_run_is() {
        let mut reporter = DeferredProgressReporter::new(Vec::new(), 10);

        // One second in, 777 messages sent.
        reporter.report_progress(0, NANOS_PER_SECOND, 777, 10);
        // And a tenth of a second later, which counts as the same second.
        reporter.report_progress(0, NANOS_PER_SECOND + NANOS_PER_SECOND / 10, 800, 10);
        reporter.reset();

        let lines = String::from_utf8(reporter.out).expect("the lines are text");
        let mut lines = lines.lines();

        assert_eq!(lines.next(), Some("Send rate: 777 msgs/sec (1 of 10)"));
        assert_eq!(lines.next(), Some("Send rate: 800 msgs/sec (1 of 10)"));
        assert_eq!(lines.next(), None);
    }

    #[test]
    fn a_report_made_inside_the_first_second_does_not_divide_by_zero() {
        let mut reporter = DeferredProgressReporter::new(Vec::new(), 10);

        reporter.report_progress(0, 0, 5000, 10);
        reporter.reset();

        let lines = String::from_utf8(reporter.out).expect("the lines are text");

        assert_eq!(lines.trim_end(), "Send rate: 5,000 msgs/sec (1 of 10)");
    }

    #[test]
    fn a_reset_empties_what_was_reported() {
        let mut reporter = DeferredProgressReporter::new(Vec::new(), 10);

        reporter.report_progress(0, NANOS_PER_SECOND, 1, 10);
        assert_eq!(reporter.reported().len(), 1);

        reporter.reset();

        assert!(reporter.reported().is_empty());
    }

    #[test]
    fn the_null_reporter_keeps_nothing() {
        let mut reporter = NullProgressReporter;

        reporter.report_progress(0, NANOS_PER_SECOND, 1, 10);
        reporter.reset();
    }
}
