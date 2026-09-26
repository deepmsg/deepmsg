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

use crate::buffer::AtomicBuffer;

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
unsafe fn mmap_readonly(fd: i32, length: usize) -> io::Result<*const u8> {
    // SAFETY: the caller guarantees `length` is non-zero and within the file,
    // so the kernel's length check cannot turn a caller error into memory
    // unsafety; every other argument is kernel-validated. `MAP_SHARED` rather
    // than `MAP_PRIVATE` is load-bearing: the CnC file is written by another
    // process while we read it, and `MAP_PRIVATE` would hand us a
    // copy-on-write snapshot that never sees those writes. The reference makes
    // the same choice at `aeron_fileutil.c:828` (`flags = MAP_SHARED`) with
    // `PROT_READ` for the read-only case at `:839`. Failure is reported only as
    // `MAP_FAILED`.
    let addr = unsafe {
        libc::mmap(
            std::ptr::null_mut::<c_void>(),
            length,
            libc::PROT_READ,
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

/// A read-only, shared mapping of a file, unmapped on drop.
///
/// This is the safe face of the seam: nothing above it needs to know about
/// `mmap`, and nothing above it can pick the wrong flags.
pub struct MappedFile {
    addr: *const u8,
    len: usize,
}

impl MappedFile {
    /// Open `path` and map it read-only, shared with every other process.
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be opened or stat-ed, if it is empty, if its
    /// length does not fit in a `usize`, or if the mapping itself fails.
    pub fn open_readonly(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
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
        // `mmap_readonly` documents.
        let addr = unsafe { mmap_readonly(file.as_raw_fd(), len)? };

        Ok(Self { addr, len })
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
        fn with_bytes(bytes: &[u8]) -> Self {
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("deepmsg-pal-{}-{n}.bin", std::process::id()));
            let mut file = File::create(&path).expect("create temp file");
            file.write_all(bytes).expect("write temp file");
            file.sync_all().expect("sync temp file");
            Self(path)
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
