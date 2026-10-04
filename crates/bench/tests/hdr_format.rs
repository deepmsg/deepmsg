//! The rig's `.hdr` is a **Java** file format, and this is where that claim is
//! checked.
//!
//! The rig is built to be read back by the reference's own toolchain —
//! `ResultsAggregator` opens every `.hdr` with `org.HdrHistogram.HistogramLogReader`
//! (`benchmarks-api/src/main/java/io/aeron/benchmarks/ResultsAggregator.java:124-134`)
//! — so "we wrote a histogram to a file" is not the bar. The bar is *that reader*.
//!
//! The golden is `tests/fixtures/reference-interval-log.hdr`, produced by the
//! reference's own writer: `analysis/bench/hdr-spike/HdrWrite.java`, run against
//! HdrHistogram 2.2.2, making exactly the call `PersistedHistogram.saveToFile` makes
//! (`benchmarks-api/.../PersistedHistogram.java:134-152`: one `HistogramLogWriter`,
//! one `outputIntervalHistogram` over the histogram's own timestamps, close).
//!
//! # What the comparison can and cannot be
//!
//! The log is one line — the reference never calls `outputLogFormatVersion()`, which
//! is a method of its own and not something `outputIntervalHistogram` does on the way
//! past — of `start,duration,max,base64 payload`.
//!
//! The first three fields are compared **byte for byte**, because they can be. The
//! first two are the same degenerate pair the reference writes (see
//! [`deepmsg_bench::loadtest::result`] for why they are not times), and
//! `Interval_Max` is the *highest equivalent* value of the maximum rather than the
//! maximum recorded: recording 999_999_999 writes 1000341503.000.
//!
//! The payload is compared **by what it decodes to**, and the reason is worth writing
//! down. Java writes it deflated, and a deflate stream is not a canonical form of its
//! input: Java's `Deflater` runs at level 9 over zlib, `V2DeflateSerializer` runs
//! `flate2` at its default level 6 over miniz_oxide. Matching the bytes would mean
//! reimplementing Java's `Deflater` to satisfy a reader that cannot tell the
//! difference. So what is pinned is the *shape* — the compressed-form cookie, which
//! is a choice Java made and this follows — and the *contents*, read back.
//!
//! The end-to-end form of this check is `analysis/bench/hdr-spike/run.sh`, which
//! needs a JDK and the HdrHistogram jar and so is not a cargo test. Set
//! `DEEPMSG_HDR_SPIKE_DIR` when running this test and it leaves a copy of what it
//! wrote for that script to pick up.

use std::path::{Path, PathBuf};

use deepmsg_bench::loadtest::config::TimeUnit;
use deepmsg_bench::loadtest::result::{self, Status};
use hdrhistogram::Histogram;

/// The first four bytes of a payload, in the compressed form Java writes.
///
/// `V2_COMPRESSED_COOKIE` in the crate's own terms; written out here because the
/// point of the assertion is that the compressed form is the one being written, and
/// `HIST` at the front of every interval log is the base64 of these.
const COMPRESSED_COOKIE: [u8; 4] = [0x1c, 0x84, 0x93, 0x14];

/// The samples, in step with `HdrWrite.java` by hand.
///
/// They cross bucket boundaries rather than resemble traffic: 1000 and 1001 straddle
/// one, 999 and 65535 are the awkward ends of others, and the top value is the one
/// whose highest equivalent differs from itself.
const SAMPLES: &[(u64, u64)] = &[
    (1, 1),
    (2, 1),
    (3, 1),
    (10, 5),
    (100, 10),
    (999, 10),
    (1000, 50),
    (1001, 50),
    (4096, 100),
    (65535, 200),
    (1_000_000, 1000),
    (16_777_216, 2000),
    (250_000_000, 3000),
    (999_999_999, 4000),
];

/// A path under the temporary directory that goes away when the test does.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("deepmsg-bench-hdr-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("the temporary directory can be made");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The reference's histogram, with the reference's samples recorded into it.
fn reference_histogram() -> Histogram<u64> {
    let mut histogram = result::histogram();

    for &(value, count) in SAMPLES {
        histogram
            .record_n(value, count)
            .expect("every sample is inside the reference's bounds");
    }

    histogram
}

/// The line this build writes, without the newline the file ends with.
///
/// `name` keeps two tests from writing into the same scratch directory at once —
/// they run in parallel threads of one process.
fn written_line(name: &str) -> String {
    let scratch = Scratch::new(name);
    let path = result::save_to_file(&reference_histogram(), scratch.path(), "test", Status::Ok)
        .expect("the scratch directory is writable");

    std::fs::read_to_string(path)
        .expect("the result is text")
        .trim_end()
        .to_owned()
}

fn golden() -> String {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/reference-interval-log.hdr");

    std::fs::read_to_string(path)
        .expect("the golden file is in the tree")
        .trim_end()
        .to_owned()
}

/// Everything up to and including the third comma: the three fields with values in
/// them, and the separator before the payload.
fn fields(line: &str) -> &str {
    let mut commas = 0;

    for (index, character) in line.char_indices() {
        if character == ',' {
            commas += 1;
            if commas == 3 {
                return &line[..=index];
            }
        }
    }

    panic!("an interval log line has four comma-separated fields: {line}");
}

fn payload(line: &str) -> &str {
    line.rsplit(',').next().expect("the line has four fields")
}

/// The three fields that carry values, against the reference's own.
///
/// A failure here is not a formatting nit. It is the rig's whole way of reporting
/// results being a different format from the reference's, so the assertion prints
/// both lines rather than saying "not equal".
#[test]
fn the_interval_log_s_values_are_the_reference_s() {
    let ours = written_line("values");
    let theirs = golden();

    assert_eq!(
        fields(&ours),
        fields(&theirs),
        "\n  reference: {theirs}\n  ours:      {ours}"
    );
}

/// The compressed form, because Java writes the compressed form.
#[test]
fn the_payload_is_the_compressed_form_java_writes() {
    let ours = written_line("payload");
    let theirs = golden();

    assert_eq!(
        &decode_base64(payload(&ours))[..4],
        COMPRESSED_COOKIE,
        "our payload is not in the compressed form"
    );
    assert_eq!(
        &decode_base64(payload(&theirs))[..4],
        COMPRESSED_COOKIE,
        "the golden is not in the compressed form, so this test is checking the wrong thing"
    );
}

/// The samples survive a round trip through the crate's own reader.
///
/// Not the gate — `run.sh` reads this file with the *Java* reader, and that is the
/// gate. This one is here so that a change to the writer fails a cargo test, in CI,
/// instead of waiting for someone to run a script that needs a JDK.
#[test]
fn the_recorded_samples_survive_the_round_trip() {
    let scratch = Scratch::new("round-trip");
    let histogram = reference_histogram();
    let path =
        result::save_to_file(&histogram, scratch.path(), "test", Status::Ok).expect("writable");
    let line = std::fs::read_to_string(&path).expect("readable");

    hand_to_the_java_check(&std::fs::read(&path).expect("readable"));

    let mut deserializer = hdrhistogram::serialization::Deserializer::new();
    let recovered: Histogram<u64> = deserializer
        .deserialize(&mut std::io::Cursor::new(decode_base64(payload(
            line.trim_end(),
        ))))
        .expect("the payload is a compressed V2 histogram");

    assert_eq!(recovered.len(), histogram.len());
    assert_eq!(recovered.max(), histogram.max());

    for &(value, count) in SAMPLES {
        assert_eq!(
            recovered.count_at(value),
            count,
            "the count recorded at {value} did not survive"
        );
    }
}

/// The percentile table is printed in the unit the run reports in.
#[test]
fn the_percentile_table_is_scaled_to_the_unit() {
    let histogram = reference_histogram();
    let mut nanoseconds = Vec::new();

    result::output_percentile_distribution(
        &histogram,
        &mut nanoseconds,
        TimeUnit::Nanoseconds.scale_ratio(),
    )
    .expect("a Vec takes anything");

    let mut microseconds = Vec::new();
    result::output_percentile_distribution(
        &histogram,
        &mut microseconds,
        TimeUnit::Microseconds.scale_ratio(),
    )
    .expect("a Vec takes anything");

    let nanoseconds = String::from_utf8(nanoseconds).expect("the table is text");
    let microseconds = String::from_utf8(microseconds).expect("the table is text");

    // The table reports the *highest equivalent* of each value, so the largest
    // sample — 999_999_999 — shows as 1000341503. The same number a thousand
    // times smaller is what microseconds must show.
    assert!(nanoseconds.contains("1000341503.000"), "{nanoseconds}");
    assert!(microseconds.contains("1000341.503"), "{microseconds}");
}

/// Leaves a copy behind for `analysis/bench/hdr-spike/run.sh`, which needs a JDK and
/// so cannot be a cargo test.
fn hand_to_the_java_check(bytes: &[u8]) {
    let Ok(directory) = std::env::var("DEEPMSG_HDR_SPIKE_DIR") else {
        return;
    };

    let directory = PathBuf::from(directory);
    std::fs::create_dir_all(&directory).expect("the spike directory can be made");
    // The spike compares two files, so this one is named for which side it is.
    std::fs::write(directory.join("rust.hdr"), bytes).expect("the spike directory is writable");
}

/// Standard base64 with padding, as `HistogramLogWriter` emits it.
///
/// The crate's `IntervalLogIterator` hands the payload back as base64 text and nothing
/// that turns it into bytes, so the four lines that stand between the two are done
/// here rather than by adding a dependency to the test's build.
fn decode_base64(text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len() / 4 * 3);
    let mut accumulator = 0u32;
    let mut bits = 0u32;

    for byte in text.bytes().filter(|&byte| byte != b'=') {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            other => panic!("{other:#x} is not a base64 character"),
        };

        accumulator = (accumulator << 6) | u32::from(value);
        bits += 6;

        if bits >= 8 {
            bits -= 8;
            out.push((accumulator >> bits) as u8);
        }
    }

    out
}
