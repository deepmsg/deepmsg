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
//! (`benchmarks-api/.../PersistedHistogram.java:134-152`): one `HistogramLogWriter`,
//! one `outputIntervalHistogram(startSec, endSec, histogram, 1.0)`, close.
//!
//! # What the comparison can and cannot be
//!
//! The log is one line — the reference never calls `outputLogFormatVersion()`, which
//! is a method of its own and not something `outputIntervalHistogram` does on the way
//! past — of `start,duration,max,base64 payload`.
//!
//! The first three fields are compared **byte for byte** and they are the ones with
//! content in them: `Interval_Max` is the *highest equivalent* value of the maximum,
//! not the maximum recorded, so recording 999_999_999 writes 1000341503.000.
//!
//! The payload is compared **by what it decodes to**, not byte for byte, and the
//! reason is worth writing down. Java writes the payload deflated, and a deflate
//! stream is not a canonical form of its input: Java's `Deflater` runs at level 9
//! (`78 da`) over zlib, while `V2DeflateSerializer` runs `flate2` at its default
//! level 6 (`78 9c`) over miniz_oxide. Matching the bytes would mean reimplementing
//! Java's `Deflater`, and it would buy the rig nothing — the reader is what has to
//! accept the file, and it does not care which deflate produced a stream it can
//! inflate. So what is pinned here is the *shape* (the compressed-form cookie, which
//! is a choice Java made and we follow) and the *contents* (the counts, read back).
//!
//! The end-to-end form of this check — Java reading our file — is
//! `analysis/bench/hdr-spike/run.sh`, which needs a JDK and the HdrHistogram jar and
//! so is not a cargo test. Set `DEEPMSG_HDR_SPIKE_DIR` when running this test and it
//! leaves a copy of what it wrote for that script to pick up.

use std::path::{Path, PathBuf};
use std::time::Duration;

use hdrhistogram::Histogram;
use hdrhistogram::serialization::V2DeflateSerializer;
use hdrhistogram::serialization::interval_log::IntervalLogWriterBuilder;

/// The bounds `PersistedHistogram` gives its histogram: one hour, three significant
/// digits (`benchmarks-api/.../PersistedHistogram.java:167`).
///
/// Not the bounds the rest of this crate uses. The crate's own [`histogram`] helper
/// starts at the reference *samples*' ten-second ceiling (`cping.c:353`), and with
/// three significant digits the two ceilings put the same sample in different
/// buckets. The rig is measured against the benchmark's histogram, so it gets the
/// benchmark's bounds.
///
/// [`histogram`]: deepmsg_bench::histogram
const HIGHEST_TRACKABLE: u64 = 3_600_000_000_000;

/// `Histogram.getStartTimeStamp()` is milliseconds, and the reference divides it by
/// 1000.0 to get the log's seconds.
const START_MS: u64 = 1_759_560_000_123;
const DURATION_MS: u64 = 10_000;

/// The first four bytes of a payload, in the compressed form Java writes.
///
/// `V2_COMPRESSED_COOKIE` in the crate's own terms; written out here because the
/// point of the assertion is that we chose the form Java chose, and `HIST` at the
/// front of the line is the base64 of these.
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

/// The reference's histogram, with the reference's samples recorded into it.
fn reference_histogram() -> Histogram<u64> {
    let mut histogram = Histogram::new_with_bounds(1, HIGHEST_TRACKABLE, 3)
        .expect("the reference's own bounds are valid");

    for &(value, count) in SAMPLES {
        histogram
            .record_n(value, count)
            .expect("every sample is inside the reference's bounds");
    }

    histogram
}

/// The file, as bytes.
///
/// Written through a `Vec` rather than a file so what is compared is the bytes and
/// not two filesystems' idea of them.
fn interval_log(histogram: &Histogram<u64>) -> Vec<u8> {
    let mut bytes = Vec::new();

    {
        let mut serializer = V2DeflateSerializer::new();
        let mut writer = IntervalLogWriterBuilder::new()
            .begin_log_with(&mut bytes, &mut serializer)
            .expect("writing to a Vec cannot fail");

        writer
            .write_histogram(
                histogram,
                Duration::from_millis(START_MS),
                Duration::from_millis(DURATION_MS),
                None,
            )
            .expect("writing to a Vec cannot fail");
    }

    bytes
}

fn golden() -> String {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/reference-interval-log.hdr");

    std::fs::read_to_string(path)
        .expect("the golden file is in the tree")
        .trim_end()
        .to_owned()
}

/// The line this build writes, without the newline the writer ends it with.
fn written_log() -> String {
    String::from_utf8(interval_log(&reference_histogram()))
        .expect("the log is text")
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

/// Leaves a copy behind for `analysis/bench/hdr-spike/run.sh`, which needs a JDK and
/// so cannot be a cargo test.
fn hand_to_the_java_check(bytes: &[u8]) {
    let Ok(directory) = std::env::var("DEEPMSG_HDR_SPIKE_DIR") else {
        return;
    };

    let directory = PathBuf::from(directory);
    std::fs::create_dir_all(&directory).expect("the spike directory can be made");
    std::fs::write(directory.join("rust.hdr"), bytes).expect("the spike directory is writable");
}

/// The three fields that carry numbers, against the reference's own.
///
/// A failure here is not a formatting nit. It is the plan's format claim
/// (`deepmsg-bench-plan-vs-aeron.md` §2.5) being false, and the rig then owes a
/// hand-written encoder — or a different way of reporting results at all. So the
/// assertion prints both lines rather than saying "not equal".
#[test]
fn the_interval_log_s_values_are_the_reference_s() {
    let ours = written_log();
    let theirs = golden();

    assert_eq!(
        fields(&ours),
        fields(&theirs),
        "\n  reference: {theirs}\n  ours:      {ours}"
    );
}

/// We write the compressed form, because Java does.
///
/// `HIST` at the head of both lines is the base64 of this cookie, so this is also
/// what makes our file recognisable to the tooling as an interval log at all.
#[test]
fn the_payload_is_the_compressed_form_java_writes() {
    let ours = written_log();
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
    let histogram = reference_histogram();
    let bytes = interval_log(&histogram);

    hand_to_the_java_check(&bytes);

    let line = String::from_utf8(bytes).expect("the log is text");
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

/// Standard base64 with padding, as `HistogramLogWriter` emits it.
///
/// The crate's `IntervalLogIterator` hands the payload back as base64 text and
/// nothing that turns it into bytes, so the four lines that stand between the two are
/// done here rather than by adding a dependency to the test's build.
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
