//! The ring frames cross threads on.
//!
//! One producer, one consumer, and the frames themselves: a receive thread
//! takes frames off the port, parks them, and hands the ones that belong to
//! another thread's transports over. Nothing is copied — what crosses is the
//! handle — which is why the ring carries frames rather than bytes.

use std::ffi::{CString, c_void};
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::Error;
use crate::mempool::{Mempool, ParkedFrame};
use crate::sys::{self, SHIM_BURST_MAX};

/// A ring of parked frames.
///
/// [`FrameRing::split`] is what makes it usable: the two halves go to the two
/// threads, and the ring itself is freed when the last of them is dropped.
pub struct FrameRing<'pool> {
    inner: Arc<RingHandle>,
    _pool: PhantomData<&'pool Mempool>,
}

struct RingHandle {
    handle: NonNull<c_void>,
}

// SAFETY: what the handle points at is an `rte_ring` created as single
// producer, single consumer — one thread enqueues and one dequeues, at the same
// time, which is the arrangement it is built for. The handle itself is a token
// nothing writes to, and both halves of a split ring hold an `Arc` of this,
// which is what the two impls make possible.
unsafe impl Send for RingHandle {}
// SAFETY: as above: sharing the token between the two halves is the point.
unsafe impl Sync for RingHandle {}

impl Drop for RingHandle {
    fn drop(&mut self) {
        // SAFETY: this is the only owner of the ring, and nothing can still be
        // in it that anyone owns — both halves hold an `Arc` of this, and the
        // frames in it are the ring's until they are dequeued.
        unsafe { sys::shim_ring_free(self.handle.as_ptr()) };
    }
}

impl<'pool> FrameRing<'pool> {
    /// Make a ring of `capacity` frames.
    pub fn create(name: &str, capacity: u32, pool: &'pool Mempool) -> Result<Self, Error> {
        let _ = pool;
        let name = CString::new(name)
            .map_err(|_| Error::refused(format!("a ring name contains a NUL byte: {name:?}")))?;

        // SAFETY: a NUL-terminated name that outlives the call, and a capacity
        // the caller chose. The answer is a ring or null, checked here.
        let handle = unsafe { sys::shim_ring_create(name.as_ptr(), capacity) };

        Ok(Self {
            inner: Arc::new(RingHandle {
                handle: NonNull::new(handle)
                    .ok_or_else(|| Error::from_rte_errno("the ring could not be made"))?,
            }),
            _pool: PhantomData,
        })
    }

    /// The two halves, one for each thread.
    pub fn split(self) -> (Producer<'pool>, Consumer<'pool>) {
        (
            Producer {
                inner: Arc::clone(&self.inner),
                _pool: PhantomData,
                _one_thread: PhantomData,
            },
            Consumer {
                inner: self.inner,
                _pool: PhantomData,
                _one_thread: PhantomData,
            },
        )
    }
}

/// The sending half: one thread, which is what a single producer means.
pub struct Producer<'pool> {
    inner: Arc<RingHandle>,
    _pool: PhantomData<&'pool Mempool>,
    _one_thread: PhantomData<*mut ()>,
}

// SAFETY: the half is moved to its thread and never shared; what it points at
// (the ring) is built for exactly this producer/consumer arrangement.
unsafe impl Send for Producer<'_> {}

impl<'pool> Producer<'pool> {
    /// Offer frames to the ring, and answer how many it took.
    ///
    /// The frames it took are gone from `frames` — the ring owns them now — and
    /// the ones it did not take are still there, which is what a full ring
    /// means: the caller keeps them (and counts the drop, if it wants to).
    pub fn enqueue(&mut self, frames: &mut Vec<ParkedFrame<'pool>>) -> usize {
        let mut taken = 0usize;
        while taken < frames.len() {
            let chunk = (frames.len() - taken).min(SHIM_BURST_MAX);
            let mut handles = [std::ptr::null_mut::<c_void>(); SHIM_BURST_MAX];
            for (slot, frame) in handles.iter_mut().zip(&frames[taken..taken + chunk]) {
                *slot = frame.handle().as_ptr();
            }

            // SAFETY: `chunk` handles are the frames at the front of the
            // vector, which this half owns while its borrow lives; the ring is
            // this half's to enqueue on.
            let enqueued = unsafe {
                sys::shim_ring_enqueue_burst(
                    self.inner.handle.as_ptr(),
                    handles.as_ptr(),
                    chunk as std::ffi::c_uint,
                )
            } as usize;

            taken += enqueued;
            if enqueued < chunk {
                break;
            }
        }

        // The frames the ring took are the ring's now: dropping them here would
        // free frames the other thread is about to receive.
        for frame in frames.drain(..taken) {
            std::mem::forget(frame);
        }

        taken
    }
}

/// The receiving half.
pub struct Consumer<'pool> {
    inner: Arc<RingHandle>,
    _pool: PhantomData<&'pool Mempool>,
    _one_thread: PhantomData<*mut ()>,
}

// SAFETY: as for `Producer`.
unsafe impl Send for Consumer<'_> {}

impl<'pool> Consumer<'pool> {
    /// Take up to `max` frames off the ring, appending them to `out`.
    pub fn dequeue(&mut self, out: &mut Vec<ParkedFrame<'pool>>, max: usize) -> usize {
        let mut taken = 0usize;
        let mut descs = [sys::ShimRxDesc {
            mbuf: std::ptr::null_mut(),
            data: std::ptr::null_mut(),
            len: 0,
            checksum: sys::SHIM_CKSUM_NONE,
        }; SHIM_BURST_MAX];

        while taken < max {
            let chunk = (max - taken).min(SHIM_BURST_MAX);
            let mut handles = [std::ptr::null_mut::<c_void>(); SHIM_BURST_MAX];

            // SAFETY: `chunk` writable handle slots, and the ring is this
            // half's to dequeue from.
            let count = unsafe {
                sys::shim_ring_dequeue_burst(
                    self.inner.handle.as_ptr(),
                    handles.as_mut_ptr(),
                    chunk as std::ffi::c_uint,
                )
            } as usize;
            if count == 0 {
                break;
            }

            // SAFETY: the first `count` handles are live mbufs the ring just
            // handed over, and the descriptor array holds `SHIM_BURST_MAX`.
            unsafe {
                sys::shim_mbuf_info_bulk(
                    handles.as_ptr(),
                    descs.as_mut_ptr(),
                    count as std::ffi::c_uint,
                )
            };

            for desc in &descs[..count] {
                let handle = NonNull::new(desc.mbuf).expect("a dequeued frame has a handle");
                out.push(ParkedFrame::own(handle, desc.data, desc.len));
            }

            taken += count;
            if count < chunk {
                break;
            }
        }

        taken
    }
}
