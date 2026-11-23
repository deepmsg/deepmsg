#![no_main]

//! Target 13 (slow tier): the two mark files.
//!
//! A mark file is the first thing a component maps after a crash: the archive's
//! `archive-mark.dat` and the cluster's member mark file. Both readers take a
//! path, so this target rewrites a fixed file in `/dev/shm` per iteration and
//! runs the production parser over it.
//!
//! The file has to be at least an SBE header's worth of bytes, and both
//! readers require the full 8 KiB header region before they parse anything —
//! so the input is zero-padded to that length. The fuzzer still chooses every
//! byte the readers actually look at.

use deepmsg_archive::mark_file::{ArchiveMarkFile, FILENAME as ARCHIVE_MARK_FILENAME};
use deepmsg_cluster_common::mark::ClusterMarkFile;
use deepmsg_codec_cluster_mark::cluster_component_type::ClusterComponentType;
use deepmsg_fuzz::{scratch_dir, write_scratch};
use libfuzzer_sys::fuzz_target;

/// `HEADER_LENGTH` in both formats: the mark header and its variable fields
/// share this region, and neither reader maps less than it.
const HEADER_LENGTH: usize = 8 * 1024;

/// The input, zero-padded to the smallest file either reader will map.
fn mark_file_bytes(data: &[u8]) -> Vec<u8> {
    let length = data.len().max(HEADER_LENGTH);
    let mut bytes = vec![0_u8; length];
    let copied = data.len().min(length);
    bytes[..copied].copy_from_slice(&data[..copied]);
    bytes
}

fuzz_target!(|data: &[u8]| {
    let bytes = mark_file_bytes(data);
    let directory = scratch_dir("13_mark_files");

    // The read-only path: the header is mapped and the error-buffer length is
    // read out of it.
    let cluster_path = directory.join("cluster-mark.dat");
    write_scratch(&cluster_path, &bytes);
    let _ = ClusterMarkFile::open(&cluster_path);

    // The adopting path: a component restarting over an existing file parses
    // it (`adopt_existing`) before it may overwrite.
    let _ = ClusterMarkFile::create(
        &cluster_path,
        ClusterComponentType::CONSENSUS_MODULE,
        1024 * 1024,
        4096,
        0,
        0,
    );

    let archive_directory = directory.join("archive");
    std::fs::create_dir_all(&archive_directory).expect("the archive scratch directory");
    let archive_path = archive_directory.join(ARCHIVE_MARK_FILENAME);
    write_scratch(&archive_path, &bytes);
    let _ = ArchiveMarkFile::open(&archive_directory);
});
