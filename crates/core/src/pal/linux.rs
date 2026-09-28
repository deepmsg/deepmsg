//! Linux implementation of the platform seam.
//!
//! The raw calls here are deliberate trans-calls: no logic, nothing to audit
//! beyond the argument mapping. The public surface is the safe type below.
//!
//! Reference: `aeron-client/src/main/c/util/aeron_fileutil.c` is the reference
//! implementation's equivalent seam, and the flags below are copied from it.

use std::ffi::c_void;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;

use crate::buffer::{AtomicBuffer, ReadWrite};

/// Map `length` bytes of `fd` read-only and shared.
///
/// Private on purpose: the only thing above it is [`MappedFile`], so this
/// stays the smallest possible `unsafe` surface rather than a second public
/// API that callers could get wrong.
///
/// # Safety
///
/// `length` must be greater than zero and no greater than the size of the file
/// behind `fd`, and `fd` must be open for the duration of the call. The
/// returned mapping must be unmapped exactly once with the same `length`.
unsafe fn mmap_impl(fd: i32, length: usize, writable: bool) -> io::Result<*const u8> {
    // The protection follows the mode the caller asked for. The reference
    // chooses between the same two at `aeron_fileutil.c:839`, and its write
    // path (`aeron_context_request_driver_termination`) takes the writable one.
    let protection = if writable {
        libc::PROT_READ | libc::PROT_WRITE
    } else {
        libc::PROT_READ
    };

    // SAFETY: the caller guarantees `length` is non-zero and within the file,
    // so the kernel's length check cannot turn a caller error into memory
    // unsafety; every other argument is kernel-validated. `MAP_SHARED` rather
    // than `MAP_PRIVATE` is load-bearing either way: the CnC file is written by
    // another process while we use it, and `MAP_PRIVATE` would hand us a
    // copy-on-write snapshot that never sees those writes — the reference makes
    // the same choice at `aeron_fileutil.c:828`. Failure is reported only as
    // `MAP_FAILED`.
    let addr = unsafe {
        libc::mmap(
            std::ptr::null_mut::<c_void>(),
            length,
            protection,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };

    if libc::MAP_FAILED == addr {
        return Err(io::Error::last_os_error());
    }

    Ok(addr.cast::<u8>().cast_const())
}

/// Allocate `length` bytes of real space for `fd`.
///
/// Private for the same reason [`mmap_impl`] is: it is only ever the second
/// half of [`MappedFile::create`], and nothing else needs raw allocation.
///
/// `posix_fallocate` reports failure as a return value instead of through
/// `errno`, which is why this is a function rather than an inline call.
fn allocate(fd: i32, length: usize) -> io::Result<()> {
    let length = libc::off_t::try_from(length).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "length does not fit in an off_t",
        )
    })?;

    // SAFETY: `fd` is open for the duration of the call and was just created by
    // `MappedFile::create`, so the range starts at the file's beginning and
    // covers nothing that another process could be holding. The call only
    // reserves space; the bytes it reserves read as zero, which is what makes
    // the zero-fill guarantee a kernel property rather than an assumption about
    // what was on the disk before.
    let rc = unsafe { libc::posix_fallocate(fd, 0, length) };

    if 0 != rc {
        return Err(io::Error::from_raw_os_error(rc));
    }

    Ok(())
}

/// A shared mapping of a file, unmapped on drop.
///
/// Created read-only, read-write, or as a brand-new file of a given length;
/// the mode is fixed at construction and [`MappedFile::is_writable`] reports
/// which one this is.
///
/// This is the safe face of the seam: nothing above it needs to know about
/// `mmap`, and nothing above it can pick the wrong flags.
pub struct MappedFile {
    addr: *const u8,
    len: usize,
    /// Whether this mapping was taken with write access. Tracked as a value
    /// rather than encoded in the type because the one caller that needs both
    /// modes — the CnC reader, which validates a file and then, for the command
    /// path, writes a record into it — would otherwise have to be generic over
    /// a distinction it rarely cares about. The cost is that
    /// [`MappedFile::region_mut`] returns `None` on a read-only mapping instead
    /// of failing to compile; the benefit is that a mistaken call cannot fault
    /// on a read-only page.
    writable: bool,
}

// SAFETY: a mapping is process-wide memory, not thread-local state — every
// thread of this process may address it, and the kernel does not care which one
// faults on it. What the type tracks is `addr`, `len` and `writable`, all plain
// values, so moving one between threads moves a description of memory that was
// always reachable from both. Aliasing is not this impl's business: it is
// enforced by `AtomicBuffer`'s borrows and by the reference's own single-writer
// rules, which do not change with the thread.
//
// The reason this exists at all is the native resource agent: it creates a log
// buffer on a thread of its own and hands the mapping to the conductor, which
// is exactly the transfer this makes legal.
unsafe impl Send for MappedFile {}

// A mapping is also *shared* rather than moved: the conductor holds the CnC
// file and the data plane's agents each derive their own counter views over the
// same pages (P1-4). A mapping is a range of the process's address space that
// every thread can already reach, so sharing a description of it adds no
// capability, and what a thread may write through it is the atomic discipline
// the counter regions impose — not something this impl grants or withholds.
//
// SAFETY: this impl exists so that `Arc<MappedFile>` may be shared, and the
// safety argument is the same one `Send` above rests on plus one more: every
// access through the description is an atomic access through `AtomicBuffer`,
// whose borrows are what enforce the single-writer rules. The description
// itself (`addr`, `len`, `writable`) is plain data, and no method of this type
// reads through `addr` without going through that boundary.
unsafe impl Sync for MappedFile {}

impl MappedFile {
    /// Open `path` and map it read-only, shared with every other process.
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be opened or stat-ed, if it is empty, if its
    /// length does not fit in a `usize`, or if the mapping itself fails.
    pub fn open_readonly(path: &Path) -> io::Result<Self> {
        Self::open_with(path, false)
    }

    /// Open `path` and map it read-write, shared with every other process.
    ///
    /// The CnC command path needs this — a client writes a record into the
    /// to-driver ring — and the reference maps the file read-write for exactly
    /// that reason (`aeron-client/src/main/c/aeron_context.c:587`). Prefer
    /// [`MappedFile::open_readonly`] everywhere else: a read-only mapping is
    /// the one a bug cannot corrupt, and it is what every reader uses.
    ///
    /// # Errors
    ///
    /// As [`MappedFile::open_readonly`], plus a permission failure if the file
    /// or the filesystem is not writable.
    pub fn open_readwrite(path: &Path) -> io::Result<Self> {
        Self::open_with(path, true)
    }

    /// Create `path` at exactly `length` bytes and map it read-write, shared,
    /// with the length **allocated**.
    ///
    /// Exclusive by construction: an existing file is an error rather than
    /// something to overwrite, which is what the reference asks the kernel for
    /// (`aeron-client/src/main/c/util/aeron_fileutil.c:967` opens
    /// `O_RDWR|O_CREAT|O_EXCL`). A caller that has to cope with a leftover file
    /// — a media driver finding a stale aeron directory — deletes it
    /// deliberately, in the open, rather than by accident here.
    ///
    /// The length is *allocated*, not merely declared, and that is deliberate:
    /// the reference fills with zeroes for the CnC file
    /// (`aeron-driver/src/main/c/aeron_driver.c:313` passes `true`), which
    /// selects a non-sparse file (`aeron_fileutil.c:1135`, where the flag is
    /// inverted) and then touches every page (`aeron_fileutil.c:1123-1132`).
    /// `posix_fallocate` standing in for `fallocate` (`:985`) is the same
    /// bargain: space that reads as zero, and no page-fault storm when a client
    /// first writes forty megabytes into the counters region.
    ///
    /// # Errors
    ///
    /// Fails if the file already exists, if it cannot be created or allocated,
    /// or if the mapping itself fails. A file this call created and then
    /// abandoned is removed before returning, so no partial `cnc.dat` is left
    /// behind.
    pub fn create(path: &Path, length: usize) -> io::Result<Self> {
        Self::create_inner(path, length, true)
    }

    /// The same, as a **sparse** file: the length is declared and nothing is
    /// allocated — pages appear as the log is written into them and read as
    /// zero until then.
    ///
    /// This is the reference's sparse log buffer: its `aeron_raw_log_map`
    /// creates the file with the sparse flag and neither prefaults nor
    /// touches when `use_sparse_files` is set
    /// (`aeron-client/src/main/c/util/aeron_fileutil.c:1269-1292`), which is
    /// the default a driver runs with (`term.buffer.sparse.file`). The bytes
    /// are indistinguishable from a dense file's to a reader; the difference
    /// is disk usage, and the pages of a sparse file's holes are the
    /// filesystem's to hand out on first write.
    ///
    /// # Errors
    ///
    /// As [`MappedFile::create`], minus the allocation step.
    pub fn create_sparse(path: &Path, length: usize) -> io::Result<Self> {
        Self::create_inner(path, length, false)
    }

    /// Both constructors' shared body: open exclusively, make the length real
    /// the way `allocated` says, map.
    fn create_inner(path: &Path, length: usize, allocated: bool) -> io::Result<Self> {
        if 0 == length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "refusing to create a zero-length mapping",
            ));
        }

        let file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        // The two halves of "the file is there and it is really there": space,
        // then size. On Linux the first usually implies the second; the
        // reference separates them the same way (`aeron_fileutil.c:985` then
        // `:1015`), and a mapping taken over a short file would be a fault
        // waiting for the first reader. A sparse file skips the first half on
        // purpose — its length is a declaration, not a reservation.
        let prepared = if allocated {
            allocate(file.as_raw_fd(), length).and_then(|()| file.set_len(length as u64))
        } else {
            file.set_len(length as u64)
        };
        if let Err(error) = prepared {
            drop(file);
            let _ = std::fs::remove_file(path);
            return Err(error);
        }

        // SAFETY: `file` is open, so `as_raw_fd` yields a live descriptor for
        // the duration of this call, and `length` was just allocated and
        // declared on that same file, which establishes that it is non-zero and
        // within the file's size. Those are exactly the preconditions
        // `mmap_impl` documents.
        let addr = match unsafe { mmap_impl(file.as_raw_fd(), length, true) } {
            Ok(addr) => addr,
            Err(error) => {
                drop(file);
                let _ = std::fs::remove_file(path);
                return Err(error);
            }
        };

        Ok(Self {
            addr,
            len: length,
            writable: true,
        })
    }

    fn open_with(path: &Path, writable: bool) -> io::Result<Self> {
        // The descriptor's own access mode has to match the mapping's: a
        // read-only fd cannot produce a `PROT_WRITE` mapping.
        let file = File::options().read(true).write(writable).open(path)?;

        let len = usize::try_from(file.metadata()?.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "file is longer than a usize can address",
            )
        })?;

        if 0 == len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "refusing to map an empty file",
            ));
        }

        // SAFETY: `file` is open, so `as_raw_fd` yields a live descriptor for
        // the duration of this call, and `len` came from `metadata()` on that
        // same file, which establishes both that it is non-zero and that it is
        // within the file's size. Those are exactly the preconditions
        // `mmap_impl` documents.
        let addr = unsafe { mmap_impl(file.as_raw_fd(), len, writable)? };

        Ok(Self {
            addr,
            len,
            writable,
        })
    }

    /// A checked window into the mapping, for reading through atomics.
    ///
    /// This is the only way to reach the mapped bytes. There is deliberately no
    /// `as_slice`: a `&[u8]` would promise that the bytes do not change, and
    /// the driver writes to them for as long as it runs. See
    /// [`crate::buffer`] for why a false immutability promise is a real
    /// soundness problem rather than a documentation one.
    ///
    /// Returns `None` if the range is out of bounds or if the window is not
    /// 8-byte aligned. `mmap` was given a null address hint, so the kernel
    /// chose a page-aligned base and the window's alignment is decided by
    /// `offset` alone.
    pub fn region(&self, offset: usize, len: usize) -> Option<AtomicBuffer<'_>> {
        if offset.checked_add(len)? > self.len {
            return None;
        }

        // SAFETY: the range was just proven to lie inside the mapping, and
        // `base + offset` stays valid for as long as the returned window's
        // borrow — which is `&self`'s — because the mapping is unmapped only in
        // `Drop`, and `Drop` takes `&mut self`, so no window can outlive it.
        // The base is page-aligned by construction, so `base + offset` is
        // 8-byte aligned exactly when `offset` is; `from_raw` checks that and
        // rejects otherwise.
        unsafe { AtomicBuffer::from_raw(self.addr.add(offset), len) }
    }

    /// A checked **writable** window into the mapping.
    ///
    /// Returns `None` if the range is out of bounds, off the 8-byte grid, or if
    /// this mapping was taken read-only — so a mistaken call is a `None`, not a
    /// fault on a read-only page. The window is the only way to write into a
    /// mapping; there is no `as_mut_slice`, for the same reason there is no
    /// `as_slice` (see [`crate::buffer`]).
    pub fn region_mut(&self, offset: usize, len: usize) -> Option<AtomicBuffer<'_, ReadWrite>> {
        if !self.writable {
            return None;
        }
        if offset.checked_add(len)? > self.len {
            return None;
        }

        // SAFETY: the range was just proven to lie inside a mapping this
        // process holds with write access, and `base + offset` stays valid for
        // the returned window's borrow — which is `&self`'s — because the
        // mapping is unmapped only in `Drop`, and `Drop` takes `&mut self`.
        // The base is page-aligned by construction, so `base + offset` is
        // 8-byte aligned exactly when `offset` is; `from_raw_mut` checks that.
        unsafe { AtomicBuffer::from_raw_mut(self.addr.cast_mut().add(offset), len) }
    }

    /// Flush the mapping to durable storage.
    ///
    /// `MAP_SHARED` already makes writes visible to every other process without
    /// this call. What it adds is durability, and the reference makes the same
    /// distinction: it writes the ready version and then calls `aeron_msync`
    /// over the whole CnC file (`aeron-driver/src/main/c/aeron_driver.c:973`),
    /// so a reader that attaches *because* the version appeared cannot find a
    /// `cnc.dat` that is ready in memory and absent after a crash.
    ///
    /// # Errors
    ///
    /// The underlying `msync` error, if any. Callers that are publishing a
    /// readiness signal should treat a failure as fatal; the rest can ignore
    /// it, because the bytes are already shared.
    pub fn sync(&self) -> io::Result<()> {
        // SAFETY: `addr` and `len` describe a live mapping owned by `self` and
        // are passed to `msync` unchanged, which is the pairing that call
        // requires. `mmap` chose the base, so it is page-aligned as `msync`
        // demands, and `len` is non-zero because no constructor of this type
        // accepts zero.
        let rc = unsafe {
            libc::msync(
                self.addr.cast_mut().cast::<c_void>(),
                self.len,
                libc::MS_SYNC,
            )
        };

        if 0 != rc {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// Whether this mapping may be written through.
    pub const fn is_writable(&self) -> bool {
        self.writable
    }

    /// Length of the mapping in bytes.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Always `false`: `open_readonly` refuses to map an empty file, so a
    /// `MappedFile` that exists covers at least one byte. It is defined
    /// because `len` without it is an API trap, and because it lets a caller
    /// write the check without wondering whether the case can arise.
    pub const fn is_empty(&self) -> bool {
        0 == self.len
    }
}

impl Drop for MappedFile {
    fn drop(&mut self) {
        // SAFETY: `addr` and `len` are exactly the pair returned by the
        // successful `mmap` in `open_readonly`, and `Drop` runs once, so the
        // mapping is unmapped exactly once — which is the contract `munmap`
        // requires. Nothing else in this type ever calls `munmap`.
        let rc = unsafe { libc::munmap(self.addr.cast_mut().cast::<c_void>(), self.len) };
        debug_assert_eq!(0, rc, "munmap failed for a mapping this type owns");
    }
}

impl std::fmt::Debug for MappedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedFile")
            .field("addr", &format_args!("{:p}", self.addr))
            .field("len", &self.len)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A file in the system temp directory, removed when dropped.
    ///
    /// Hand-rolled rather than pulled from a crate: the workspace has no test
    /// dependencies and this is the only thing one would be used for.
    struct TempFile(std::path::PathBuf);

    impl TempFile {
        /// A path in the temp directory that no file occupies yet.
        fn vacant() -> Self {
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!("deepmsg-pal-{}-{n}.bin", std::process::id())))
        }

        fn with_bytes(bytes: &[u8]) -> Self {
            let temp = Self::vacant();
            let mut file = File::create(&temp.0).expect("create temp file");
            file.write_all(bytes).expect("write temp file");
            file.sync_all().expect("sync temp file");
            temp
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn maps_a_file_and_reads_its_bytes() {
        let bytes = b"deepmsg shared-memory seam";
        let temp = TempFile::with_bytes(bytes);

        let mapped = MappedFile::open_readonly(&temp.0).expect("map");
        assert_eq!(bytes.len(), mapped.len());

        let mut out = vec![0u8; bytes.len()];
        assert_eq!(
            Some(()),
            mapped
                .region(0, out.len())
                .expect("region")
                .copy_out(0, &mut out)
        );
        assert_eq!(bytes.as_slice(), out.as_slice());
    }

    #[test]
    fn region_refuses_to_reach_past_the_mapping() {
        let temp = TempFile::with_bytes(&[7u8; 64]);
        let mapped = MappedFile::open_readonly(&temp.0).expect("map");

        assert!(
            mapped.region(0, 64).is_some(),
            "the whole mapping is reachable"
        );
        assert!(
            mapped.region(64, 1).is_none(),
            "one byte past the end is not"
        );
        assert!(mapped.region(0, 65).is_none());
        assert!(
            mapped.region(usize::MAX, 1).is_none(),
            "offset maths must not wrap"
        );
        assert!(
            mapped.region(4, 8).is_none(),
            "a window off the 8-byte grid has no sound atomic access"
        );
    }

    #[test]
    fn refuses_an_empty_file() {
        let temp = TempFile::with_bytes(b"");
        let error = MappedFile::open_readonly(&temp.0).expect_err("empty file must be refused");
        assert_eq!(io::ErrorKind::InvalidData, error.kind());
    }

    #[test]
    fn reports_a_missing_file() {
        let path = std::env::temp_dir().join("deepmsg-pal-does-not-exist");
        let error = MappedFile::open_readonly(&path).expect_err("missing file must fail");
        assert_eq!(io::ErrorKind::NotFound, error.kind());
    }

    #[test]
    fn creates_a_zero_filled_file_of_the_requested_length() {
        let temp = TempFile::vacant();
        let mapped = MappedFile::create(&temp.0, 8192).expect("create");

        assert_eq!(8192, mapped.len());
        assert!(mapped.is_writable());

        // Space, not just a declared size: the file is its full length on disk
        // before anything writes to it, which is what keeps the first write
        // into a 46 MB counter region from faulting a page per 4 KB.
        assert_eq!(8192, std::fs::metadata(&temp.0).expect("stat").len());

        let mut head = [1u8; 8];
        mapped
            .region(0, 8)
            .expect("region")
            .copy_out(0, &mut head)
            .expect("copy");
        assert_eq!([0u8; 8], head, "a freshly created file reads as zero");

        let mut tail = [1u8; 8];
        mapped
            .region(8184, 8)
            .expect("region")
            .copy_out(0, &mut tail)
            .expect("copy");
        assert_eq!([0u8; 8], tail, "and so does its last page");
    }

    #[test]
    fn a_created_mapping_can_be_written_through() {
        let temp = TempFile::vacant();
        let mapped = MappedFile::create(&temp.0, 4096).expect("create");

        assert_eq!(
            Some(()),
            mapped
                .region_mut(64, 8)
                .expect("writable window")
                .store_i64_release(0, 0x0102_0304_0506_0708)
        );
        assert_eq!(
            Some(0x0102_0304_0506_0708),
            mapped.region(64, 8).expect("window").load_i64_acquire(0)
        );
    }

    #[test]
    fn refuses_to_create_over_an_existing_file() {
        let temp = TempFile::with_bytes(b"already here");

        let error = MappedFile::create(&temp.0, 64).expect_err("must not clobber");

        assert_eq!(io::ErrorKind::AlreadyExists, error.kind());
        assert_eq!(
            b"already here".as_slice(),
            std::fs::read(&temp.0).expect("read").as_slice(),
            "and the file it refused to touch is untouched"
        );
    }

    #[test]
    fn refuses_a_zero_length_create() {
        let temp = TempFile::vacant();

        let error = MappedFile::create(&temp.0, 0).expect_err("zero is not a length");

        assert_eq!(io::ErrorKind::InvalidInput, error.kind());
        assert!(!temp.0.exists(), "and it leaves no file behind");
    }

    #[test]
    fn sync_puts_a_write_on_the_device() {
        // The mapping is shared either way, so this is not about visibility. It
        // is about the file the driver hands a client: what `sync` adds is that
        // reading the file through a second path — a fresh `open`, after a
        // crash — sees the write too.
        let temp = TempFile::vacant();
        let mapped = MappedFile::create(&temp.0, 4096).expect("create");

        mapped
            .region_mut(0, 8)
            .expect("window")
            .store_i64_release(0, 42)
            .expect("store");
        mapped.sync().expect("sync");

        let bytes = std::fs::read(&temp.0).expect("read");
        assert_eq!(&42i64.to_le_bytes(), &bytes[0..8]);
    }

    #[test]
    fn observes_another_writers_shared_update() {
        // The reason `MAP_SHARED` is load-bearing: a write made through a
        // second mapping of the same file must become visible through ours.
        // `MAP_PRIVATE` would fail this test, which is the whole point.
        let temp = TempFile::with_bytes(&[1u8; 64]);
        let mapped = MappedFile::open_readonly(&temp.0).expect("map");

        let first_four = |m: &MappedFile| {
            let mut out = [0u8; 4];
            m.region(0, 4)
                .expect("region")
                .copy_out(0, &mut out)
                .expect("copy");
            out
        };

        assert_eq!([1u8; 4], first_four(&mapped));

        {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&temp.0)
                .expect("reopen for write");
            file.write_all(&[2u8; 4]).expect("write");
            file.sync_all().expect("sync");
        }

        assert_eq!(
            [2u8; 4],
            first_four(&mapped),
            "a shared mapping must observe writes from elsewhere"
        );
    }
}
