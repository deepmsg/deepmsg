//! Shared helpers for the `fuzz_targets/`.
//!
//! Fuzzer bytes arrive at an arbitrary address, while `AtomicBuffer` requires
//! an 8-byte aligned base (`crates/core/src/buffer.rs`). Every target that
//! builds one copies its input into the thread-local scratch below first — a
//! 64-byte-aligned region with headroom over any frame, term or metadata block
//! this suite feeds. 64 rather than 8 because the test buffers in the crate
//! already use it and one alignment rule is easier to keep straight.
//!
//! Do **not** call a `with_*` helper from inside another one's closure: the
//! `RefCell` borrow is held for the call and a nested borrow panics. Two inputs
//! have their own helper (`with_two_read_only`).

pub mod archive_control;
pub mod cluster_messages;

use core::cell::RefCell;

use deepmsg_core::buffer::{AtomicBuffer, ReadOnly, ReadWrite};

/// Enough for a 64 KiB term plus its metadata, or the largest UDP datagram.
const SCRATCH_LEN: usize = 128 * 1024;

#[repr(align(64))]
struct Aligned([u8; SCRATCH_LEN]);

thread_local! {
    static SCRATCH_A: RefCell<Aligned> = const { RefCell::new(Aligned([0; SCRATCH_LEN])) };
    static SCRATCH_B: RefCell<Aligned> = const { RefCell::new(Aligned([0; SCRATCH_LEN])) };
}

/// Copy `data` in, zero the padding to the next 8 bytes, and return the padded
/// length. The zeroing is not cosmetic: a buffer's tail field is read when the
/// input is shorter than the field, and a previous iteration's bytes there
/// would make the test non-deterministic.
fn prepare(scratch: &mut Aligned, data: &[u8]) -> usize {
    let len = data.len().min(SCRATCH_LEN);
    scratch.0[..len].copy_from_slice(&data[..len]);
    let padded = len.next_multiple_of(8).max(8);
    scratch.0[len..padded].fill(0);
    padded
}

/// Run `f` over a read-only view of the input, copied into aligned memory.
///
/// The second argument is the input length before padding, which is what a
/// caller bounds its fields by; `buffer.len()` is the padded length.
pub fn with_read_only<R>(data: &[u8], f: impl FnOnce(AtomicBuffer<'_, ReadOnly>, usize) -> R) -> R {
    SCRATCH_A.with(|cell| {
        let mut scratch = cell.borrow_mut();
        let padded = prepare(&mut scratch, data);
        let buffer = AtomicBuffer::from_slice(&scratch.0[..padded])
            .expect("the thread-local scratch is 64-byte aligned");
        f(buffer, data.len().min(SCRATCH_LEN))
    })
}

/// As [`with_read_only`], for targets that write to the region.
pub fn with_read_write<R>(
    data: &[u8],
    f: impl FnOnce(AtomicBuffer<'_, ReadWrite>, usize) -> R,
) -> R {
    SCRATCH_A.with(|cell| {
        let mut scratch = cell.borrow_mut();
        let padded = prepare(&mut scratch, data);
        let buffer = AtomicBuffer::from_slice_mut(&mut scratch.0[..padded])
            .expect("the thread-local scratch is 64-byte aligned");
        f(buffer, data.len().min(SCRATCH_LEN))
    })
}

/// Two read-only views, for a consumer whose metadata and values are separate
/// regions (`CountersReader`, for one). Separate thread-locals, not two
/// borrows of one: see the module note.
pub fn with_two_read_only<R>(
    first: &[u8],
    second: &[u8],
    f: impl FnOnce(AtomicBuffer<'_, ReadOnly>, AtomicBuffer<'_, ReadOnly>) -> R,
) -> R {
    SCRATCH_A.with(|a| {
        SCRATCH_B.with(|b| {
            let mut a = a.borrow_mut();
            let mut b = b.borrow_mut();
            let a_padded = prepare(&mut a, first);
            let b_padded = prepare(&mut b, second);
            let a_buffer = AtomicBuffer::from_slice(&a.0[..a_padded])
                .expect("the thread-local scratch is 64-byte aligned");
            let b_buffer = AtomicBuffer::from_slice(&b.0[..b_padded])
                .expect("the thread-local scratch is 64-byte aligned");
            f(a_buffer, b_buffer)
        })
    })
}

/// Whether a payload holds an SBE message's fixed part, at the **schema's**
/// block length: the generated decoders read fields up to that offset whatever
/// the message's acting block length says.
pub fn fixed_part_fits(payload: &[u8], schema_block_length: u16) -> bool {
    payload.len() >= 8 + usize::from(schema_block_length)
}

/// Whether `count` variable-length fields fit in `payload`, each a `u32`
/// length and that many bytes, starting at `at`.
///
/// `at` is an offset the message declared, so nothing here trusts it: every
/// step is a `get` or a `checked_add`.
pub fn variable_data_fits(payload: &[u8], at: usize, count: usize) -> bool {
    let mut at = at;
    for _ in 0..count {
        let Some(prefix_end) = at.checked_add(4) else {
            return false;
        };
        let Some(prefix) = payload.get(at..prefix_end) else {
            return false;
        };
        let Ok(length) = <[u8; 4]>::try_from(prefix) else {
            return false;
        };
        let Ok(length) = usize::try_from(u32::from_le_bytes(length)) else {
            return false;
        };
        let Some(next) = prefix_end.checked_add(length) else {
            return false;
        };
        if next > payload.len() {
            return false;
        }
        at = next;
    }

    true
}

/// The absolute path of a file under `tests/fixtures/`.
pub fn fixture(relative: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tests")
        .join("fixtures")
        .join(relative)
}

/// A scratch directory for the file-backed targets (09 and 11).
///
/// Those two production entry points take a `&Path` — `Catalog::open`,
/// `compute_stop_position`, `RecordingLog::open`, `NodeStateFile::open` — and
/// this suite does not add a second, bytes-taking API just for fuzzing. The
/// directory is created once and each iteration rewrites a fixed file in it;
/// an existing file is truncated, never unlinked, so there is no directory
/// churn on the hot path.
///
/// `/dev/shm` when it exists (the cluster tests already put their shared
/// memory there), the temporary directory otherwise.
pub fn scratch_dir(subdir: &str) -> std::path::PathBuf {
    let base = std::env::var_os("DEEPMSG_FUZZ_SCRATCH").map_or_else(
        || {
            let shm = std::path::Path::new("/dev/shm");
            if shm.is_dir() {
                shm.join("deepmsg-fuzz")
            } else {
                std::env::temp_dir().join("deepmsg-fuzz")
            }
        },
        std::path::PathBuf::from,
    );
    let dir = base.join(subdir);
    std::fs::create_dir_all(&dir).expect("the fuzz scratch directory");
    dir
}

/// Rewrite `path` with exactly `data`, truncating what was there.
pub fn write_scratch(path: &std::path::Path, data: &[u8]) {
    use std::io::Write;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .expect("the fuzz scratch file opens");
    file.write_all(data).expect("the fuzz scratch file writes");
}
