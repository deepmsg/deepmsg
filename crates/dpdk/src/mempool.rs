//! The pool every frame comes from, and the frames this build owns.

use std::ffi::{CString, c_void};
use std::marker::PhantomData;
use std::ptr::NonNull;

use crate::Error;
use crate::sys;

/// How a pool is made.
///
/// The defaults are the driver's: one pool per port, sized for the two
/// descriptor rings plus the queues that hold frames between a receive and a
/// send, with DPDK's usual per-thread cache.
#[derive(Clone, Copy, Debug)]
pub struct MempoolSettings {
    /// How many frames the pool starts with.
    pub count: u32,
    /// Frames cached per thread (`rte_mempool`'s `cache_size`).
    pub cache: u32,
    /// Bytes of data area per frame; `0` is DPDK's default, which is what an
    /// MTU of 1500 and its headers fit in.
    pub data_room: u32,
    /// The NUMA socket to allocate on. `-1` — `SOCKET_ID_ANY` — is what the
    /// ports this build runs on report, and what this crate hands back
    /// unchanged rather than turning into a socket number (D0).
    pub socket_id: i32,
}

impl Default for MempoolSettings {
    fn default() -> Self {
        Self {
            count: 8191,
            cache: 256,
            data_room: 0,
            socket_id: -1,
        }
    }
}

/// A pool of frames.
///
/// Shared by every thread that allocates: DPDK's pool is built for that (a
/// per-thread cache in front of a lock-free ring), which is what the two
/// `unsafe impl`s below record.
pub struct Mempool {
    handle: NonNull<c_void>,
}

// SAFETY: `rte_pktmbuf_pool_create` makes a pool that several threads may
// allocate from and free to concurrently — that is what its per-lcore caches
// and its shared ring are for. Nothing in this wrapper mutates state the pool
// does not already synchronise.
unsafe impl Send for Mempool {}
// SAFETY: as above: the handle is a token, and the pool behind it is
// thread-safe by construction.
unsafe impl Sync for Mempool {}

impl Mempool {
    /// Make a pool of `settings.count` frames.
    pub fn create(name: &str, settings: &MempoolSettings) -> Result<Self, Error> {
        let name = CString::new(name)
            .map_err(|_| Error::refused(format!("a pool name contains a NUL byte: {name:?}")))?;

        // SAFETY: the name is a valid NUL-terminated string that outlives the
        // call, and the numbers are the caller's settings. The handle either
        // owns a pool or is null, which is checked immediately.
        let handle = unsafe {
            sys::shim_pool_create(
                name.as_ptr(),
                settings.count,
                settings.cache,
                settings.data_room,
                settings.socket_id,
            )
        };

        let handle = NonNull::new(handle)
            .ok_or_else(|| Error::from_rte_errno("the mempool could not be made"))?;

        Ok(Self { handle })
    }

    /// How many frames are in the pool right now.
    pub fn available(&self) -> u32 {
        // SAFETY: the handle owns a live pool for as long as `self` does.
        unsafe { sys::shim_pool_avail(self.handle.as_ptr()) }
    }

    /// The handle a queue setup needs. Not for the driver's use.
    pub(crate) fn handle(&self) -> NonNull<c_void> {
        self.handle
    }
}

impl Drop for Mempool {
    fn drop(&mut self) {
        // SAFETY: this is the owner of the pool, so no frame can be out with
        // it — `ParkedFrame` borrows the pool it came from — and the pool is
        // freed exactly once.
        unsafe { sys::shim_pool_free(self.handle.as_ptr()) };
    }
}

impl std::fmt::Debug for Mempool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mempool")
            .field("available", &self.available())
            .finish()
    }
}

/// A frame this build owns.
///
/// The receive path hands them out, the send path consumes them, and a
/// transport parks them on its ready list in between. Going back to the pool is
/// [`Drop`]'s job, which is the point: on a data path, "someone forgot to free
/// it" is the expensive defect, and `Mempool::available` is where it shows.
#[must_use = "a parked frame goes back to its pool on drop"]
pub struct ParkedFrame<'pool> {
    handle: NonNull<c_void>,
    data: *mut u8,
    len: u16,
    _pool: PhantomData<&'pool Mempool>,
}

impl<'pool> ParkedFrame<'pool> {
    /// Take ownership of a frame that came out of `pool`.
    ///
    /// The caller promises the handle is a live mbuf from that pool whose bytes
    /// run from `data` for `len`, and that nothing else owns it.
    pub(crate) fn own(handle: NonNull<c_void>, data: *mut u8, len: u16) -> Self {
        Self {
            handle,
            data,
            len,
            _pool: PhantomData,
        }
    }

    /// The frame's bytes.
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: the frame owns the mbuf, so its data area is alive and its
        // length is the one the NIC wrote; nothing else may write to it while
        // this borrow lives.
        unsafe { std::slice::from_raw_parts(self.data, usize::from(self.len)) }
    }

    /// How long the frame is.
    pub fn len(&self) -> usize {
        usize::from(self.len)
    }

    /// Whether the frame is empty. A frame is never empty in practice; the
    /// question is here because `len` is.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The handle, for a ring that carries frames across threads.
    pub(crate) fn handle(&self) -> NonNull<c_void> {
        self.handle
    }
}

// SAFETY: a frame is an owned handle plus the bytes it points at; DPDK's mbufs
// carry no thread affinity, and moving the frame moves the only owner.
unsafe impl Send for ParkedFrame<'_> {}

impl Drop for ParkedFrame<'_> {
    fn drop(&mut self) {
        let mut handle = self.handle.as_ptr();
        // SAFETY: this is the owner of the mbuf, so it is freed exactly once;
        // the array of one is a valid pointer-and-length pair.
        unsafe { sys::shim_mbuf_free_bulk(&mut handle, 1) };
    }
}

impl std::fmt::Debug for ParkedFrame<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParkedFrame")
            .field("len", &self.len)
            .finish()
    }
}
