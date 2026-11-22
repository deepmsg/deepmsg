#![no_main]

//! Target 11 (slow tier): the cluster's `recording.log` and `node-state.dat`.
//!
//! A node that crashed and restarted reads both. `RecordingLog::open` walks
//! records until the file stops being a multiple of the entry size; the
//! queries and the recovery plan then reason about a history that may be
//! truncated, out of order, or full of invalidated entries. The assertion is
//! that every one of those answers `Err`/`None` rather than indexing past the
//! file.

use deepmsg_cluster::persist::node_state::NodeStateFile;
use deepmsg_cluster::persist::recording_log::RecordingLog;
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

fuzz_target!(|data: &[u8]| {
    let directory = scratch_dir("11_cluster_log");

    let log_path = directory.join("recording.log");
    write_scratch(&log_path, data);
    if let Ok(log) = RecordingLog::open(&log_path) {
        let service_count = i32::from(data.first().copied().unwrap_or(0) % 8);

        let _ = log.find_last_term();
        let _ = log.find_last_term_recording_id();
        let _ = log.find_snapshot_at_or_before_or_lowest(le_i64_at(data, 0), service_count);
        let _ = log.find_snapshot_at_or_before_or_lowest(le_i64_at(data, 8), service_count);

        let replicated = if data.len() > 1 && data[1] & 1 == 1 {
            le_i64_at(data, 16)
        } else {
            -1
        };
        let _ = log.create_recovery_plan(|_recording_id| None, service_count, replicated);
    }

    let state_path = directory.join("node-state.dat");
    write_scratch(&state_path, data);
    let _ = NodeStateFile::open(&state_path);
});
