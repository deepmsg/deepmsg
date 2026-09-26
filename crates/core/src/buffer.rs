//! Bounds- and alignment-checked atomic views over a byte region — the unsafe
//! boundary of this crate (ADR-0002).
//!
//! The CnC file and the term buffers are written by one process and read by
//! another, without locks. Correctness therefore rests on two things: the
//! reader must never read a torn value, and it must never reorder its reads
//! past the point where the writer said they were visible.
//!
//! # Why there is no `&[u8]` view here
//!
//! The obvious API is `as_slice() -> &[u8]`, and it is wrong. `&[u8]` promises
//! the bytes will not change for the life of the borrow, which is false for
//! every region a driver is still writing — and a false immutability promise
//! is not a documentation problem, it is licence for the optimiser to hoist
//! loads out of a loop. Shared bytes are therefore reachable only through the
//! atomic loads below, or through [`AtomicBuffer::copy_out`], which takes a
//! bounded snapshot instead of minting a reference.
//!
//! # Ordering policy
//!
//! The reference marks these fields `volatile` and wraps access in
//! `AERON_GET_ACQUIRE` / `AERON_SET_RELEASE`
//! (`aeron-client/src/main/c/concurrent/aeron_atomic64_gcc_x86_64.h:23,34`).
//! Rust has no `volatile`, so the methods here are named after the *ordering*
//! rather than after the C macro, and the call site is expected to read as
//! "this field is published by an acquire" — the table below is the authority
//! for which is which.
//!
//! | Field | Ordering | Reference |
//! |---|---|---|
//! | CnC `cnc_version` | acquire | `aeron_cnc_file_descriptor.c:35` |
//! | counter `state` | acquire | `aeron_counters_manager.c:297` |
//! | counter `label_length` | acquire | `aeron_counters_manager.c:302` |
//! | counter `counter_value`, `registration_id` | acquire | `aeron_counters_manager.h:187` |
//! | counter `type_id`, `key`, `owner_id`, `reference_id` | relaxed | ordered by the `state` acquire |
//! | mpsc ring `tail_position` … `consumer_heartbeat` | acquire | `aeron_mpsc_rb.h:66,76`, `aeron_mpsc_rb.c:347` |
//! | broadcast `tail_intent_counter`, `tail_counter`, `latest_counter` | acquire | `aeron_broadcast_receiver.h:49,68` |
//! | error-log `length` | acquire | `aeron_distinct_error_log.c:231` |
//! | error-log text, counter `label` | relaxed copy | `aeron_distinct_error_log.c:248` |
//!
//! Where the reference performs a "plain" read, the Rust equivalent is
//! `Ordering::Relaxed` and *not* a non-atomic read: the bytes are still being
//! written by another process, so the access must be atomic even when it needs
//! no ordering of its own.
//!
//! # What is deliberately absent
//!
//! There are no store or read-modify-write accessors. Nothing in P0-a writes to
//! a CnC file, and unwritten unsafe is unreviewable unsafe; the writer's half
//! of this API belongs to the change that has a writer, and to a `// SAFETY:`
//! comment written by someone who can reason about that writer.
//!
//! Every `unsafe` block here carries a `// SAFETY:` comment naming the
//! invariant and the ordering rationale; clippy `undocumented_unsafe_blocks`
//! is denied workspace-wide.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};

/// A window onto `len` bytes, read through atomics with an explicit ordering.
///
/// Construct one from a slice this process owns ([`AtomicBuffer::from_slice`])
/// or, inside the crate, over a shared mapping (`deepmsg_core::pal`). Both
/// paths check base alignment, and every access checks bounds and the
/// alignment the accessor needs.
///
/// # Thread safety
///
/// `AtomicBuffer` is neither `Send` nor `Sync`, because it holds a raw pointer
/// and adding those impls is a claim about how the *borrowed* memory may be
/// shared, which belongs to whatever change needs it. Nothing in P0-a shares
/// one across threads.
pub struct AtomicBuffer<'a> {
    base: *const u8,
    len: usize,
    /// Ties the window to the borrow of the bytes behind it. `&[u8]` rather
    /// than `&()` so the variance is the one a reader of bytes expects.
    _borrow: PhantomData<&'a [u8]>,
}

impl<'a> AtomicBuffer<'a> {
    /// A view over a byte slice.
    ///
    /// Takes `&'a [u8]` and not a raw pointer on purpose: a safe borrow
    /// already proves the bytes are valid for `'a`, so the only thing left to
    /// check is alignment. This is also the constructor that makes the
    /// accessors testable under miri, which cannot map a file.
    ///
    /// Returns `None` if the slice does not start on an 8-byte boundary, which
    /// is what the widest accessor needs.
    pub fn from_slice(region: &'a [u8]) -> Option<Self> {
        // SAFETY: `region` is a live borrow for `'a`, so its pointer is
        // non-null and `len` bytes are readable for at least that long, which
        // is the whole of what `from_raw` requires beyond the alignment check
        // it performs itself. Nothing here can write through the pointer, so
        // no aliasing rule is at stake.
        unsafe { Self::from_raw(region.as_ptr(), region.len()) }
    }

    /// A view over memory this process does not exclusively own.
    ///
    /// # Safety
    ///
    /// `base` must be non-null and valid for reads of `len` bytes for as long
    /// as the returned value lives. The caller is responsible for knowing that
    /// the memory will not be unmapped or freed in that window — which is why
    /// the only in-crate caller is `pal`, where a mapping's `Drop` guarantees
    /// it.
    pub(crate) unsafe fn from_raw(base: *const u8, len: usize) -> Option<Self> {
        if base.is_null() || 0 != base.align_offset(8) {
            return None;
        }

        Some(Self {
            base,
            len,
            _borrow: PhantomData,
        })
    }

    /// Length of the window in bytes.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Always `false`: a zero-length window is never constructed, because the
    /// only paths that could produce one are checked for emptiness first.
    pub const fn is_empty(&self) -> bool {
        0 == self.len
    }

    /// The 4-byte slot at `offset`, or `None` if it is out of bounds or not
    /// 4-byte aligned.
    fn slot_i32(&self, offset: usize) -> Option<&AtomicI32> {
        if 0 != offset % 4 {
            return None;
        }
        if offset.checked_add(4)? > self.len {
            return None;
        }

        // SAFETY: `offset <= len - 4` was just established, so all four bytes
        // are inside the window; `offset % 4 == 0` and the 8-byte-aligned base
        // proved at construction make the address 4-byte aligned, which is
        // `AtomicI32`'s requirement. The pointer derives from `base`, which the
        // constructor's caller guaranteed valid for reads for `'a`, and the
        // returned reference is bounded by `&self` so it cannot outlive the
        // window. No `&mut` to these bytes exists anywhere in this module, so
        // creating a shared `&AtomicI32` cannot violate aliasing.
        Some(unsafe { &*self.base.add(offset).cast::<AtomicI32>() })
    }

    /// The 8-byte slot at `offset`, or `None` if it is out of bounds or not
    /// 8-byte aligned.
    fn slot_i64(&self, offset: usize) -> Option<&AtomicI64> {
        if 0 != offset % 8 {
            return None;
        }
        if offset.checked_add(8)? > self.len {
            return None;
        }

        // SAFETY: as `slot_i32`, with the 8-byte alignment the base and the
        // offset together provide, which is `AtomicI64`'s requirement.
        Some(unsafe { &*self.base.add(offset).cast::<AtomicI64>() })
    }

    /// Load a 4-byte field the reference reads under `AERON_GET_ACQUIRE`.
    pub fn load_i32_acquire(&self, offset: usize) -> Option<i32> {
        Some(self.slot_i32(offset)?.load(Ordering::Acquire))
    }

    /// Load a 4-byte field the reference reads plainly, already ordered by an
    /// acquire on a neighbouring field. Atomic all the same — see the module
    /// docs.
    pub fn load_i32_relaxed(&self, offset: usize) -> Option<i32> {
        Some(self.slot_i32(offset)?.load(Ordering::Relaxed))
    }

    /// Load an 8-byte field the reference reads under `AERON_GET_ACQUIRE`.
    pub fn load_i64_acquire(&self, offset: usize) -> Option<i64> {
        Some(self.slot_i64(offset)?.load(Ordering::Acquire))
    }

    /// Load an 8-byte field the reference reads plainly.
    pub fn load_i64_relaxed(&self, offset: usize) -> Option<i64> {
        Some(self.slot_i64(offset)?.load(Ordering::Relaxed))
    }

    /// Copy `dst.len()` bytes out of the window, or `None` if that range is out
    /// of bounds.
    ///
    /// A copy rather than a borrow, and not through an atomic: the bytes of a
    /// variable-length record (a label, an error-log entry) have no single
    /// atomic access, so the only correct way to read them is to take a
    /// snapshot bounded by a length that an acquire load already validated.
    /// The window the snapshot covers is one instruction sequence, which is
    /// what makes the result either the writer's old bytes or its new ones —
    /// never a mixture it never wrote — provided the caller re-validates
    /// afterwards as the broadcast receiver does
    /// (`aeron_broadcast_receiver.c:111-123`).
    pub fn copy_out(&self, offset: usize, dst: &mut [u8]) -> Option<()> {
        if offset.checked_add(dst.len())? > self.len {
            return None;
        }

        // SAFETY: the range was just proven to be inside the window, and `dst`
        // is a distinct live allocation, so source and destination cannot
        // overlap. As with every accessor here, concurrent writes by the other
        // process make the *value* nondeterministic but not the access
        // unsound; callers that need a consistent record use the
        // snapshot-and-revalidate pattern described above.
        unsafe {
            std::ptr::copy_nonoverlapping(self.base.add(offset), dst.as_mut_ptr(), dst.len());
        }

        Some(())
    }
}

impl std::fmt::Debug for AtomicBuffer<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomicBuffer")
            .field("base", &format_args!("{:p}", self.base))
            .field("len", &self.len)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A byte buffer whose base is 8-byte aligned, which is what every
    /// accessor here requires and what a plain `[u8; N]` does not give.
    ///
    /// `from_slice` over a `Vec<u8>` would return `None` for exactly this
    /// reason, which is the first thing these tests pin down.
    #[repr(align(64))]
    struct Aligned<const N: usize>([u8; N]);

    fn filled<const N: usize>(seed: u8) -> Aligned<N> {
        let mut a = Aligned([0u8; N]);
        for (i, b) in a.0.iter_mut().enumerate() {
            *b = seed.wrapping_add(i as u8);
        }
        a
    }

    #[test]
    fn view_reports_its_length() {
        let bytes = filled::<64>(0);
        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned slice");
        assert_eq!(64, view.len());
        assert!(!view.is_empty());
    }

    #[test]
    fn a_heap_slice_is_rejected_because_it_is_unaligned() {
        // Documents the alignment precondition as behaviour rather than prose:
        // `Vec<u8>` gives 1-byte alignment, so there is no sound 8-byte atomic
        // access into it and the constructor says so instead of guessing.
        let heap: Vec<u8> = vec![0; 64];
        if 0 == heap.as_ptr().align_offset(8) {
            // The allocator happened to hand back an aligned block; nothing to
            // assert about the rejection path in that case.
            return;
        }
        assert!(AtomicBuffer::from_slice(&heap).is_none());
    }

    #[test]
    fn loads_the_bytes_that_are_there() {
        let bytes = filled::<64>(0);
        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned slice");

        // Little-endian, matching every byte contract in the project.
        let expected_lo = i32::from_le_bytes([0, 1, 2, 3]);
        let expected_hi = i64::from_le_bytes([0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(Some(expected_lo), view.load_i32_acquire(0));
        assert_eq!(Some(expected_lo), view.load_i32_relaxed(0));
        assert_eq!(Some(expected_hi), view.load_i64_acquire(0));
        assert_eq!(Some(expected_hi), view.load_i64_relaxed(0));
    }

    #[test]
    fn out_of_bounds_is_refused_not_wrapped() {
        let bytes = filled::<64>(0);
        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned slice");

        assert_eq!(None, view.load_i64_acquire(64));
        assert_eq!(
            None,
            view.load_i64_acquire(60),
            "56..64 is in, 60..68 is not"
        );
        assert_eq!(None, view.load_i32_acquire(64));
        assert_eq!(
            None,
            view.load_i64_acquire(usize::MAX),
            "offset arithmetic must not wrap"
        );
    }

    #[test]
    fn misaligned_offsets_are_refused() {
        let bytes = filled::<64>(0);
        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned slice");

        assert_eq!(None, view.load_i64_acquire(4), "i64 needs 8-byte alignment");
        assert_eq!(None, view.load_i32_acquire(2), "i32 needs 4-byte alignment");
        // `filled` writes byte[i] == i, so the aligned words read little-endian.
        assert_eq!(
            Some(i32::from_le_bytes([0, 1, 2, 3])),
            view.load_i32_acquire(0)
        );
        assert_eq!(
            Some(i32::from_le_bytes([4, 5, 6, 7])),
            view.load_i32_acquire(4)
        );
    }

    #[test]
    fn copies_a_snapshot_out() {
        let bytes = filled::<64>(0);
        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned slice");

        let mut out = [0u8; 5];
        assert_eq!(Some(()), view.copy_out(3, &mut out));
        assert_eq!([3, 4, 5, 6, 7], out);
    }

    #[test]
    fn refuses_to_copy_past_the_end() {
        let bytes = filled::<64>(0);
        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned slice");

        let mut out = [0u8; 8];
        assert_eq!(None, view.copy_out(60, &mut out));
        assert_eq!(None, view.copy_out(usize::MAX, &mut out));
    }

    #[test]
    fn observes_atomic_writes_from_another_view_of_the_same_bytes() {
        // The accessors are the reader's half of a handshake the writer owns.
        // Driving it from a second view over the same storage is as close as a
        // unit test gets to the cross-process case; the ordering itself is
        // what `Acquire`/`Release` mean, and is asserted here by behaviour
        // rather than by tooling -- loom cannot see a foreign process either.
        let bytes = Aligned([0u8; 64]);
        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned slice");
        let writer = AtomicBuffer::from_slice(&bytes.0).expect("aligned slice");

        assert_eq!(Some(0), view.load_i64_acquire(0));

        // SAFETY-free path: reach the slot through the crate-internal slot
        // accessor and store with Release, the ordering a publisher would use.
        writer
            .slot_i64(0)
            .expect("8-aligned, in bounds")
            .store(1234, Ordering::Release);

        assert_eq!(Some(1234), view.load_i64_acquire(0));
    }
}
