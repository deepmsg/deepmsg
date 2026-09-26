//! A synthetic CnC file, for tests that need one without a driver.
//!
//! The interop suite gets a real driver and is the stronger evidence, but it
//! cannot run in CI — so the preconditions, the encoders and the client's
//! matching loop are exercised here instead, against a file the test wrote
//! itself.
//!
//! The sizes are the smallest that are still *valid*: a ring capacity that is
//! a power of two (so the mask works), counter regions in the reference's 4:1
//! ratio, and an error log with room for a record.

use std::io::{Seek, SeekFrom, Write as _};
use std::path::{Path, PathBuf};

use deepmsg_cnc::layout;
use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};

/// To-driver region: a 1024-byte ring plus its trailer.
pub const TO_DRIVER: usize = 1024 + layout::MPSC_RB_TRAILER_LENGTH;
/// To-clients region: the same shape, different trailer.
pub const TO_CLIENTS: usize = 1024 + layout::BROADCAST_TRAILER_LENGTH;
/// Ring capacity of the to-driver region.
pub const COMMAND_CAPACITY: usize = 1024;
/// Ring capacity of the to-clients region.
pub const EVENT_CAPACITY: usize = 1024;
/// Counter value records.
pub const COUNTERS_VALUES: usize = 2 * layout::COUNTER_VALUE_LENGTH;
/// Counter metadata records, in the reference's 4:1 ratio.
pub const COUNTERS_METADATA: usize = 4 * COUNTERS_VALUES;
/// Error log entries.
pub const ERROR_LOG: usize = 8 * layout::ERROR_LOG_HEADER_LENGTH;

/// The sum of every region and the metadata block, before page alignment.
pub const SUM: usize = layout::VERSION_AND_METADATA_LENGTH
    + TO_DRIVER
    + TO_CLIENTS
    + COUNTERS_METADATA
    + COUNTERS_VALUES
    + ERROR_LOG;

/// The file's length: the sum rounded up to the page size the metadata claims.
pub const FILE_LENGTH: usize = 8192;

/// Where the two ring regions begin.
pub struct Offsets {
    /// The to-driver command ring.
    pub to_driver: usize,
    /// The to-clients broadcast ring.
    pub to_clients: usize,
}

/// The region offsets for the sizes above.
pub const fn offsets() -> Offsets {
    let to_driver = layout::VERSION_AND_METADATA_LENGTH;
    Offsets {
        to_driver,
        to_clients: to_driver + TO_DRIVER,
    }
}

/// A synthetic `cnc.dat` in a directory of its own, removed when dropped.
pub struct SyntheticCnc {
    dir: PathBuf,
}

impl SyntheticCnc {
    /// Write a well-formed file, advertising `version`.
    pub fn new(version: i32) -> Self {
        assert_eq!(
            FILE_LENGTH,
            layout::align_up(SUM, 4096),
            "the region sizes must round up to FILE_LENGTH, or every test built \
             on this assumes the wrong file"
        );

        static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("deepmsg-synthetic-{}-{n}.dir", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the aeron directory");

        let mut bytes = vec![0u8; FILE_LENGTH];
        write_i32(
            &mut bytes,
            layout::TO_DRIVER_BUFFER_LENGTH_OFFSET,
            TO_DRIVER as i32,
        );
        write_i32(
            &mut bytes,
            layout::TO_CLIENTS_BUFFER_LENGTH_OFFSET,
            TO_CLIENTS as i32,
        );
        write_i32(
            &mut bytes,
            layout::COUNTER_METADATA_BUFFER_LENGTH_OFFSET,
            COUNTERS_METADATA as i32,
        );
        write_i32(
            &mut bytes,
            layout::COUNTER_VALUES_BUFFER_LENGTH_OFFSET,
            COUNTERS_VALUES as i32,
        );
        write_i32(
            &mut bytes,
            layout::ERROR_LOG_BUFFER_LENGTH_OFFSET,
            ERROR_LOG as i32,
        );
        write_i64(
            &mut bytes,
            layout::CLIENT_LIVENESS_TIMEOUT_OFFSET,
            10_000_000_000,
        );
        write_i64(
            &mut bytes,
            layout::START_TIMESTAMP_OFFSET,
            1_700_000_000_000,
        );
        write_i64(&mut bytes, layout::PID_OFFSET, 4242);
        write_i32(&mut bytes, layout::FILE_PAGE_SIZE_OFFSET, 4096);
        // Published last, as a driver does.
        write_i32(&mut bytes, layout::CNC_VERSION_OFFSET, version);

        write_file(&dir, &bytes);
        Self { dir }
    }

    /// The aeron directory.
    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// The `cnc.dat` inside it.
    pub fn cnc_path(&self) -> PathBuf {
        self.dir.join(deepmsg_cnc::CNC_FILE_NAME)
    }

    /// Rewrite bytes on disk, for the hostile cases a real driver never
    /// produces.
    pub fn patch(self, offset: usize, bytes: &[u8]) -> Self {
        let path = self.cnc_path();
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("reopen the CnC file");
        file.seek(SeekFrom::Start(offset as u64)).expect("seek");
        file.write_all(bytes).expect("patch");
        file.sync_all().expect("sync");
        self
    }

    /// Rewrite one 4-byte field.
    pub fn patch_i32(self, offset: usize, value: i32) -> Self {
        self.patch(offset, &value.to_le_bytes())
    }

    /// Rewrite one 8-byte field.
    pub fn patch_i64(self, offset: usize, value: i64) -> Self {
        self.patch(offset, &value.to_le_bytes())
    }
}

impl Drop for SyntheticCnc {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Transmit one record into a to-clients ring, the way the driver does.
///
/// The tests need the *writer* side of a ring that production only ever reads,
/// because in production the writer is another process. Mirrors
/// `aeron-client/src/main/c/concurrent/aeron_broadcast_transmitter.c`: announce
/// the intent, write the record, then move the counters.
///
/// `next` is the caller's cursor in the ring's counter space, carried across
/// calls.
pub fn publish_broadcast(
    region: &AtomicBuffer<'_, ReadWrite>,
    capacity: usize,
    next: &mut i64,
    type_id: i32,
    payload: &[u8],
) -> Option<()> {
    let trailer = capacity;
    let mask = capacity - 1;
    let length = layout::RECORD_HEADER_LENGTH + payload.len();
    let aligned = layout::align_up(length, layout::RECORD_ALIGNMENT);
    let mut offset = (*next as u32 as usize) & mask;

    if capacity - offset < aligned {
        let padding = capacity - offset;
        region.store_i64_release(
            trailer + layout::BROADCAST_TAIL_INTENT_COUNTER_OFFSET,
            *next + padding as i64,
        )?;
        region.store_i32_relaxed(offset + layout::RECORD_LENGTH_OFFSET, padding as i32)?;
        region.store_i32_relaxed(
            offset + layout::RECORD_MSG_TYPE_ID_OFFSET,
            layout::PADDING_MSG_TYPE_ID,
        )?;
        region.store_i64_release(
            trailer + layout::BROADCAST_TAIL_COUNTER_OFFSET,
            *next + padding as i64,
        )?;
        *next += padding as i64;
        offset = 0;
    }

    region.store_i64_release(
        trailer + layout::BROADCAST_TAIL_INTENT_COUNTER_OFFSET,
        *next + aligned as i64,
    )?;
    region.store_i32_relaxed(offset + layout::RECORD_LENGTH_OFFSET, length as i32)?;
    region.store_i32_relaxed(offset + layout::RECORD_MSG_TYPE_ID_OFFSET, type_id)?;
    region.copy_in(offset + layout::RECORD_HEADER_LENGTH, payload)?;
    region.store_i64_release(trailer + layout::BROADCAST_LATEST_COUNTER_OFFSET, *next)?;
    region.store_i64_release(
        trailer + layout::BROADCAST_TAIL_COUNTER_OFFSET,
        *next + aligned as i64,
    )?;
    *next += aligned as i64;

    Some(())
}

fn write_i32(bytes: &mut [u8], offset: usize, value: i32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_i64(bytes: &mut [u8], offset: usize, value: i64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn write_file(dir: &Path, bytes: &[u8]) {
    let mut file =
        std::fs::File::create(dir.join(deepmsg_cnc::CNC_FILE_NAME)).expect("create cnc.dat");
    file.write_all(bytes).expect("write cnc.dat");
    file.sync_all().expect("sync cnc.dat");
}
