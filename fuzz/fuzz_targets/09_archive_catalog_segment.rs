#![no_main]

//! Target 09 (slow tier): the archive catalog and a recording segment.
//!
//! Both entry points take a path — `Catalog::open(directory)` and
//! `compute_stop_position(directory, summary, max_segment_file, checksum)` —
//! so this target rewrites one fixed file in `/dev/shm` per iteration and runs
//! the production parser over it. Every field the summary carries comes from
//! the input too: a frame scan is a function of the bytes *and* the term
//! geometry the caller hands it.

use deepmsg_archive::catalog::Catalog;
use deepmsg_archive::segment::{SegmentSummary, compute_stop_position};
use deepmsg_fuzz::{scratch_dir, write_scratch};
use libfuzzer_sys::fuzz_target;

fn le_i64_at(data: &[u8], at: usize) -> i64 {
    let mut bytes = [0_u8; 8];
    for (index, byte) in bytes.iter_mut().enumerate() {
        if let Some(value) = data.get(at + index) {
            *byte = *value;
        }
    }
    i64::from_le_bytes(bytes)
}

fn le_i32_at(data: &[u8], at: usize) -> i32 {
    i32::try_from(le_i64_at(data, at) & i64::from(i32::MAX)).unwrap_or(i32::MAX)
}

fuzz_target!(|data: &[u8]| {
    let directory = scratch_dir("09_archive_catalog");
    write_scratch(&directory.join("archive.catalog"), data);
    let _ = Catalog::open(&directory, 0);

    // A term length a writer could have used: a power of two, so the mask in
    // `start_position & (term_length - 1)` is defined. Everything else is the
    // fuzzer's.
    let term_buffer_length = 1_i32 << (data.first().copied().unwrap_or(0) % 21);
    let summary = SegmentSummary {
        recording_id: le_i64_at(data, 0),
        start_position: le_i64_at(data, 8),
        stop_position: None,
        initial_term_id: le_i32_at(data, 16),
        term_buffer_length,
        segment_file_length: term_buffer_length,
        stream_id: le_i32_at(data, 20),
    };

    let segment_name = "segment-0.rec";
    write_scratch(&directory.join(segment_name), data);
    let _ = compute_stop_position(&directory, &summary, Some(segment_name), None);
});
