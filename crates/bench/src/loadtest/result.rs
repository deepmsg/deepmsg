//! Where a run's numbers end up.
//!
//! Mirrors `PersistedHistogram`, `SinglePersistedHistogram` and the
//! `HistogramLogWriter` call underneath them (`benchmarks-api/.../
//! PersistedHistogram.java:108-169`), so that a file this produces is a file
//! the reference's own tooling reads: `ResultsAggregator` opens every `.hdr`
//! with `org.HdrHistogram.HistogramLogReader`
//! (`ResultsAggregator.java:124-134`), and `scripts/results-plotter.py` draws
//! what comes out.
//!
//! # The line, and the two fields in it that are not times
//!
//! The file is one line — `start,duration,max,base64` — because
//! `outputLogFormatVersion` and `outputLegend` are methods of their own and
//! `saveToFile` calls neither.
//!
//! The first two fields are the interesting ones. They come from
//! `Histogram.getStartTimeStamp()` and `getEndTimeStamp()`, and a plain
//! `Histogram` **never sets them**: those belong to `Recorder`, which tracks
//! `history` mode. So with the reference's default `track.history=false`,
//! `SinglePersistedHistogram` writes `Long.MAX_VALUE / 1000.0` and `0 / 1000.0`
//! — and a duration of minus the first. Measured rather than read: see
//! `analysis/bench/hdr-spike/`, and the first two fields of
//! `crates/bench/tests/fixtures/reference-interval-log.hdr`.
//!
//! This is reproduced and not corrected. A run that keeps no history has
//! exactly one interval, so there is no series for those fields to place, and
//! a file with a sensible-looking timestamp in them would be a file the
//! reference never writes — a difference to explain in every later comparison,
//! bought for nothing.
//!
//! # What is not here
//!
//! `PersistedHistogramSet`, the map from a name to a histogram, waits for the
//! multi-destination grid: nothing in this rig has more than one histogram, and
//! an abstraction with a single caller is a guess about the second one. The
//! functions below are what such a set would call.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use hdrhistogram::Histogram;
use hdrhistogram::serialization::{Serializer, V2DeflateSerializer};

/// The bounds the reference gives a reported histogram:
/// `new Histogram(HOURS.toNanos(1), 3)` (`PersistedHistogram.java:167`).
///
/// Not [`crate::histogram`]'s bounds. That one starts at the reference
/// *samples*' ten-second ceiling (`cping.c:353`), and with three significant
/// digits the two ceilings put the same sample in different buckets — so a rig
/// reporting into the wrong one would report slightly different percentiles for
/// identical input.
#[must_use]
pub fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, HIGHEST_TRACKABLE, SIGNIFICANT_DIGITS)
        .expect("the reference's own bounds are valid")
}

/// One hour in nanoseconds, which is `HOURS.toNanos(1)`.
const HIGHEST_TRACKABLE: u64 = 3_600_000_000_000;

/// Three significant digits, the reference's `numberOfSignificantValueDigits`.
const SIGNIFICANT_DIGITS: u8 = 3;

/// How many ticks the percentile table reports per halving of the tail. Five is
/// the reference's, and is not a display detail: it is what
/// `outputPercentileDistribution` means by "each exponentially decreasing
/// half-distance containing five percentile reporting tick points".
const TICKS_PER_HALF_DISTANCE: u32 = 5;

/// `Long.MAX_VALUE / 1000.0`, which is what the reference writes as the start
/// of an interval it kept no history for. See the module documentation.
const NO_HISTORY_START_SECONDS: f64 = i64::MAX as f64 / 1000.0;

/// Whether a run met its target, which decides the result file's name.
///
/// The reference's `SendResult.status` (`LoadTestRig.java:423-426`): OK only
/// when the expected count, the sent count and the received count agree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Everything that was meant to be sent was sent, and every one of them
    /// came back.
    Ok,
    /// Something did not, so the numbers are reported but are not a result.
    Fail,
}

impl Status {
    /// `PersistedHistogram.fileName` (`:115-123`): the suffix goes after the
    /// extension, so a failed run is `x.hdr.FAIL` and not `x.FAIL.hdr`.
    #[must_use]
    pub fn file_name_suffix(self) -> &'static str {
        match self {
            Self::Ok => "",
            Self::Fail => ".FAIL",
        }
    }
}

/// Write the histogram where the reference's tooling will find it.
///
/// The name is `<prefix>.hdr`, plus `.FAIL` when the run did not meet its
/// target — which is how the reference marks a result that must not be read as
/// a result. `ResultsAggregator` groups files by everything before the `.hdr`,
/// so the prefix has to be identical across the runs of one grid.
///
/// # Errors
///
/// [`io::ErrorKind::InvalidInput`] when the prefix is blank, which the
/// reference refuses too (`SinglePersistedHistogram.java:74-80`), and whatever
/// the filesystem says otherwise.
pub fn save_to_file(
    histogram: &Histogram<u64>,
    directory: &Path,
    prefix: &str,
    status: Status,
) -> io::Result<PathBuf> {
    let prefix = prefix.trim();
    if prefix.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "name prefix cannot be blank",
        ));
    }

    let path = directory.join(format!("{prefix}.hdr{}", status.file_name_suffix()));
    let mut file = BufWriter::new(File::create(&path)?);
    file.write_all(interval_log(histogram)?.as_bytes())?;
    file.flush()?;

    Ok(path)
}

/// The whole file: one line, and a newline to end it.
fn interval_log(histogram: &Histogram<u64>) -> io::Result<String> {
    let mut serializer = V2DeflateSerializer::new();
    let mut payload = Vec::new();
    serializer
        .serialize(histogram, &mut payload)
        .map_err(|error| io::Error::other(format!("{error:?}")))?;

    Ok(format!(
        "{:.3},{:.3},{:.3},{}\n",
        NO_HISTORY_START_SECONDS,
        0.0 - NO_HISTORY_START_SECONDS,
        histogram.max() as f64,
        encode_base64(&payload),
    ))
}

/// The percentile table, in the unit the run reports in.
///
/// `histogram.outputPercentileDistribution(printStream, scalingRatio)`. The
/// layout is the reference's so that two runs' tables can be read side by side;
/// the values are the same numbers either way, which is what a reader compares.
///
/// # Errors
///
/// Whatever the writer says.
pub fn output_percentile_distribution(
    histogram: &Histogram<u64>,
    out: &mut impl Write,
    scale_ratio: f64,
) -> io::Result<()> {
    writeln!(
        out,
        "{:>12} {:>12} {:>10} {:>14}",
        "Value", "Percentile", "TotalCount", "1/(1-Percentile)"
    )?;
    writeln!(out)?;

    for value in histogram.iter_quantiles(TICKS_PER_HALF_DISTANCE) {
        // The crate reports the percentile as a percentage; the reference's
        // table is a fraction and its last column is "one in how many", so the
        // division is what makes the two columns mean what a reader expects.
        let fraction = value.percentile() / 100.0;
        let one_in = if fraction >= 1.0 {
            f64::INFINITY
        } else {
            1.0 / (1.0 - fraction)
        };

        writeln!(
            out,
            "{:>12.3} {:>12.12} {:>10} {:>12.2}",
            value.value_iterated_to() as f64 / scale_ratio,
            fraction,
            value.count_at_value(),
            one_in
        )?;
    }

    writeln!(out)?;
    writeln!(
        out,
        "#[Mean    = {:>12.3}, StdDeviation = {:>12.3}]",
        histogram.mean() / scale_ratio,
        histogram.stdev() / scale_ratio
    )?;
    writeln!(
        out,
        "#[Max     = {:>12.3}, Total count  = {:>12}]",
        histogram.max() as f64 / scale_ratio,
        histogram.len()
    )?;

    Ok(())
}

/// Standard base64 with padding, which is what `HistogramLogWriter` writes.
///
/// The crate's writer does this for a whole line, but its first two fields take
/// `Duration`s and the reference's duration is negative, so the line is spelled
/// out here and only the payload comes from the crate.
fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);

    for chunk in bytes.chunks(3) {
        let first = u32::from(chunk[0]);
        let second = chunk.get(1).copied().map_or(0, u32::from);
        let third = chunk.get(2).copied().map_or(0, u32::from);
        let group = (first << 16) | (second << 8) | third;

        encoded.push(char::from(ALPHABET[(group >> 18) as usize & 63]));
        encoded.push(char::from(ALPHABET[(group >> 12) as usize & 63]));
        encoded.push(if chunk.len() > 1 {
            char::from(ALPHABET[(group >> 6) as usize & 63])
        } else {
            '='
        });
        encoded.push(if chunk.len() > 2 {
            char::from(ALPHABET[group as usize & 63])
        } else {
            '='
        });
    }

    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_histogram_has_the_reference_s_bounds() {
        let histogram = histogram();

        assert_eq!(histogram.high(), HIGHEST_TRACKABLE);
        assert_eq!(histogram.low(), 1);
    }

    /// A failed run's suffix goes after the extension, so that a reader that
    /// filters on `.hdr` still sees it and one that groups by prefix is not
    /// confused by it.
    #[test]
    fn a_failed_run_is_marked_after_the_extension() {
        assert_eq!(Status::Ok.file_name_suffix(), "");
        assert_eq!(Status::Fail.file_name_suffix(), ".FAIL");

        let histogram = histogram();
        let directory = std::env::temp_dir();

        let ok = save_to_file(&histogram, &directory, "t", Status::Ok).expect("writable");
        let failed = save_to_file(&histogram, &directory, "t", Status::Fail).expect("writable");

        assert!(ok.to_string_lossy().ends_with("t.hdr"));
        assert!(failed.to_string_lossy().ends_with("t.hdr.FAIL"));

        let _ = std::fs::remove_file(ok);
        let _ = std::fs::remove_file(failed);
    }

    #[test]
    fn a_blank_prefix_is_refused() {
        let error =
            save_to_file(&histogram(), Path::new("/tmp"), "  ", Status::Ok).expect_err("refused");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    /// The line's shape: four comma-separated fields, and a newline.
    #[test]
    fn the_file_is_one_line_of_four_fields() {
        let histogram = histogram();
        let directory = std::env::temp_dir();
        let path = save_to_file(&histogram, &directory, "shape", Status::Ok).expect("writable");
        let text = std::fs::read_to_string(&path).expect("readable");
        let _ = std::fs::remove_file(&path);

        assert!(text.ends_with('\n'));
        assert_eq!(text.lines().count(), 1);
        assert_eq!(text.trim_end().split(',').count(), 4);
        assert!(
            text.starts_with("9223372036854776.000,-9223372036854776.000,"),
            "unexpected first fields: {text}"
        );
    }

    #[test]
    fn base64_matches_the_encoding_a_java_writer_uses() {
        // "HIST" is what the compressed cookie's first three bytes look like in
        // this alphabet, which is why every interval log starts with it.
        assert_eq!(encode_base64(&[0x1c, 0x84, 0x93]), "HIST");
        assert_eq!(encode_base64(b"a"), "YQ==");
        assert_eq!(encode_base64(b"ab"), "YWI=");
        assert_eq!(encode_base64(b"abc"), "YWJj");
        assert_eq!(encode_base64(b""), "");
    }

    #[test]
    fn the_percentile_table_has_a_row_per_tick_and_a_footer() {
        let mut histogram = histogram();
        for value in [1_u64, 100, 1000, 10_000, 100_000] {
            histogram.record(value).expect("in range");
        }

        let mut text = Vec::new();
        output_percentile_distribution(&histogram, &mut text, 1000.0)
            .expect("a Vec takes anything");
        let text = String::from_utf8(text).expect("the table is text");

        assert!(text.contains("Value"));
        assert!(text.contains("Percentile"));
        assert!(text.contains("TotalCount"));
        assert!(text.contains("#[Mean"));
        assert!(text.contains("Total count"));
        // A header, a blank line, the rows, a blank line, the footer — so the
        // middle section is the table and every row of it is four columns.
        let rows = text.split("\n\n").nth(1).expect("a rows section");
        assert!(!rows.is_empty());
        for row in rows.lines() {
            assert_eq!(
                row.split_whitespace().count(),
                4,
                "a percentile row is value, percentile, count, one-in: {row}"
            );
        }
        // The values are in the unit asked for, which is 1000ths of a
        // nanosecond here, so the largest equivalent reads as 100.031 and not
        // as its nanoseconds. `tests/hdr_format.rs` checks the scaling against
        // the reference's own file.
        assert!(text.contains("100.031"), "{text}");
    }
}
