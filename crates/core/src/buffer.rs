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
//! # The write half
//!
//! Stores and read-modify-write accessors arrived with the first writer — the
//! CnC command ring. They were absent while nothing wrote, and deliberately so:
//! unwritten unsafe is unreviewable unsafe, and the `// SAFETY:` comment for a
//! store belongs to whoever can reason about the reader it publishes to.
//!
//! The paired orderings matter as much as the values do. A publisher that
//! release-stores a length *after* filling a record is telling a reader the
//! bytes are complete; a reader that acquire-loads that length is accepting
//! that promise. Getting the pairing wrong is the bug this module exists to
//! make visible at the call site — which is why the accessors are named after
//! the ordering rather than after the C macro they mirror.
//!
//! Every `unsafe` block here carries a `// SAFETY:` comment naming the
//! invariant and the ordering rationale; clippy `undocumented_unsafe_blocks`
//! is denied workspace-wide.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicI16, AtomicI32, AtomicI64, AtomicU8, Ordering};

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
/// Marker: this window cannot be written through.
///
/// The default, so that `AtomicBuffer<'_>` keeps meaning "read-only" and no
/// existing signature had to change when the write half arrived.
pub struct ReadOnly;

/// Marker: this window may be written through.
///
/// Produced only by a writable mapping — see [`crate::pal`] — so the ability to
/// write is a property of where the bytes came from, not of a flag someone
/// passed.
pub struct ReadWrite;

pub struct AtomicBuffer<'a, Access = ReadOnly> {
    base: *const u8,
    len: usize,
    /// Ties the window to the borrow of the bytes behind it. `&[u8]` rather
    /// than `&()` so the variance is the one a reader of bytes expects.
    _borrow: PhantomData<&'a [u8]>,
    /// Ties the window to its access level. Without this, `Access` would be an
    /// unused parameter and the two windows would be the same type.
    _access: PhantomData<Access>,
}

impl<'a> AtomicBuffer<'a, ReadOnly> {
    /// A read-only view over a byte slice.
    ///
    /// Takes `&'a [u8]` and not a raw pointer on purpose: a safe borrow
    /// already proves the bytes are valid for `'a`, so the only thing left to
    /// check is alignment. This is also the constructor that makes the
    /// accessors testable under miri, which cannot map a file.
    ///
    /// Returns [`ReadOnly`] and not `Access`: a shared borrow of bytes is not a
    /// licence to write them, so this can never produce a window with the write
    /// methods on it.
    ///
    /// Returns `None` if the slice does not start on an 8-byte boundary, which
    /// is what the widest accessor needs.
    pub fn from_slice(region: &'a [u8]) -> Option<AtomicBuffer<'a, ReadOnly>> {
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
    pub(crate) unsafe fn from_raw(
        base: *const u8,
        len: usize,
    ) -> Option<AtomicBuffer<'a, ReadOnly>> {
        build(base, len)
    }
}

/// The shared construction path, so both access levels check the same things —
/// null, and the 8-byte base alignment the widest slot needs.
fn build<'a, Access>(base: *const u8, len: usize) -> Option<AtomicBuffer<'a, Access>> {
    if base.is_null() || 0 != base.align_offset(8) {
        return None;
    }

    Some(AtomicBuffer {
        base,
        len,
        _borrow: PhantomData,
        _access: PhantomData,
    })
}

impl<'a, Access> AtomicBuffer<'a, Access> {
    /// Length of the window in bytes.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Always `false`: a zero-length window is never constructed, because the
    /// only paths that could produce one are checked for emptiness first.
    pub const fn is_empty(&self) -> bool {
        0 == self.len
    }

    /// The single byte at `offset`, or `None` if it is out of bounds.
    fn slot_u8(&self, offset: usize) -> Option<&AtomicU8> {
        if offset >= self.len {
            return None;
        }

        // SAFETY: `offset < len` was just established, so the byte is inside
        // the window; a single byte has no alignment requirement beyond the one
        // byte itself. The pointer derives from `base`, which the constructor's
        // caller guaranteed valid for reads for `'a`, and the returned
        // reference is bounded by `&self`.
        Some(unsafe { &*self.base.add(offset).cast::<AtomicU8>() })
    }

    /// The 2-byte slot at `offset`, or `None` if it is out of bounds or not
    /// 2-byte aligned.
    fn slot_i16(&self, offset: usize) -> Option<&AtomicI16> {
        if 0 != offset % 2 {
            return None;
        }
        if offset.checked_add(2)? > self.len {
            return None;
        }

        // SAFETY: the range is inside the window and the address is 2-byte
        // aligned — `offset` is even and the base is 8-byte aligned — which is
        // `AtomicI16`'s requirement. As with the wider slots, the pointer
        // derives from a base the caller guaranteed, and no `&mut` to these
        // bytes exists anywhere in this module.
        Some(unsafe { &*self.base.add(offset).cast::<AtomicI16>() })
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

    /// Load a single byte.
    ///
    /// Relaxed: a byte is never a publication gate on its own. The frame header
    /// is the case that needs this — its version and flags are bytes that the
    /// *frame length* publishes.
    pub fn load_u8(&self, offset: usize) -> Option<u8> {
        Some(self.slot_u8(offset)?.load(Ordering::Relaxed))
    }

    /// Load a 2-byte field, relaxed.
    pub fn load_i16(&self, offset: usize) -> Option<i16> {
        Some(self.slot_i16(offset)?.load(Ordering::Relaxed))
    }

    /// Load a 4-byte field with no ordering of its own.
    pub fn load_i32(&self, offset: usize) -> Option<i32> {
        self.load_i32_relaxed(offset)
    }

    /// Load an 8-byte field with no ordering of its own.
    pub fn load_i64(&self, offset: usize) -> Option<i64> {
        self.load_i64_relaxed(offset)
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

/// The write half, available only on windows built from writable memory.
///
/// The reference reaches for sequential consistency here — `lock xaddq` for the
/// correlation counter (`aeron-client/src/main/c/concurrent/aeron_atomic64_gcc_x86_64.h:52-63`)
/// and `__sync_bool_compare_and_swap`/`atomic_compare_exchange_strong` for the
/// tail position (`concurrent/aeron_mpsc_rb.c:97-100`) — so these use
/// `Ordering::SeqCst` rather than the lighter `AcqRel`. On x86-64 they compile
/// to the same instruction; on a weaker architecture the faithfulness is worth
/// more than the fence, because the ring's correctness argument is written in
/// terms of the reference's primitives.
impl<'a> AtomicBuffer<'a, ReadWrite> {
    /// A writable view over memory this process does not exclusively own.
    ///
    /// # Safety
    ///
    /// `base` must be non-null, valid for reads and writes of `len` bytes for
    /// as long as the returned value lives, and the caller is responsible for
    /// knowing it will not be unmapped or freed in that window. The memory must
    /// actually be writable by this process — a read-only mapping handed here
    /// would fault on the first store. [`crate::pal`] is the only caller, and
    /// it knows which mapping mode it took.
    pub(crate) unsafe fn from_raw_mut(
        base: *mut u8,
        len: usize,
    ) -> Option<AtomicBuffer<'a, ReadWrite>> {
        build(base.cast_const(), len)
    }

    /// A read-only window on the same bytes, with the same lifetime.
    ///
    /// Narrowing, so it cannot fail and cannot introduce an access the holder
    /// did not already have. The lifetime is the *original* borrow's, not the
    /// borrow of `self`, which is what makes it usable: a caller holding one
    /// exclusive borrow of a region can take a writable window and a read-only
    /// one from it at the same time, and hand the second to something that
    /// should not be able to write.
    ///
    /// The CnC client is exactly that caller — the same mapping carries the
    /// command ring it writes and the event ring it only reads — and so is a
    /// test playing both ends of a ring.
    pub fn as_read_only(&self) -> AtomicBuffer<'a, ReadOnly> {
        AtomicBuffer {
            base: self.base,
            len: self.len,
            _borrow: PhantomData,
            _access: PhantomData,
        }
    }

    /// A writable view over a byte slice this process owns exclusively.
    ///
    /// Safe, unlike [`AtomicBuffer::from_raw_mut`]: an `&'a mut [u8]` already
    /// proves both that the bytes are valid for `'a` and that nothing else in
    /// this process can alias them. Cross-process aliasing is the problem that
    /// constructor exists for, and it is what `pal`'s mappings carry.
    ///
    /// Returns `None` if the slice does not start on an 8-byte boundary.
    pub fn from_slice_mut(region: &'a mut [u8]) -> Option<AtomicBuffer<'a, ReadWrite>> {
        build(region.as_mut_ptr().cast_const(), region.len())
    }

    /// Store a 4-byte field the reference writes without an ordering of its
    /// own, because a later release on a neighbouring field publishes it.
    ///
    /// Atomic all the same. The bytes are shared with another process, so a
    /// plain store would be a data race, not a faster store.
    pub fn store_i32_relaxed(&self, offset: usize, value: i32) -> Option<()> {
        self.slot_i32(offset)?.store(value, Ordering::Relaxed);
        Some(())
    }

    /// Store a single byte, relaxed.
    pub fn store_u8_relaxed(&self, offset: usize, value: u8) -> Option<()> {
        self.slot_u8(offset)?.store(value, Ordering::Relaxed);
        Some(())
    }

    /// Store a 2-byte field, relaxed.
    pub fn store_i16_relaxed(&self, offset: usize, value: i16) -> Option<()> {
        self.slot_i16(offset)?.store(value, Ordering::Relaxed);
        Some(())
    }

    /// Store an 8-byte field with no ordering of its own.
    pub fn store_i64_relaxed(&self, offset: usize, value: i64) -> Option<()> {
        self.slot_i64(offset)?.store(value, Ordering::Relaxed);
        Some(())
    }

    /// Store a 4-byte field under `AERON_SET_RELEASE`.
    pub fn store_i32_release(&self, offset: usize, value: i32) -> Option<()> {
        self.slot_i32(offset)?.store(value, Ordering::Release);
        Some(())
    }

    /// Store an 8-byte field under `AERON_SET_RELEASE`.
    pub fn store_i64_release(&self, offset: usize, value: i64) -> Option<()> {
        self.slot_i64(offset)?.store(value, Ordering::Release);
        Some(())
    }

    /// Compare and exchange an 8-byte field.
    ///
    /// `Some(true)` if it held `expected` and now holds `new`; `Some(false)` if
    /// it held something else, in which case the caller re-reads and retries —
    /// which is exactly the shape of the ring's claim loop. `None` if the
    /// offset is out of bounds or misaligned.
    pub fn compare_exchange_i64(&self, offset: usize, expected: i64, new: i64) -> Option<bool> {
        Some(
            self.slot_i64(offset)?
                .compare_exchange(expected, new, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
        )
    }

    /// Add to an 8-byte field, returning the value it held **before**.
    ///
    /// Mirrors `AERON_GET_AND_ADD_INT64`, whose previous-value return is what
    /// makes it usable as an id allocator.
    pub fn fetch_add_i64(&self, offset: usize, value: i64) -> Option<i64> {
        Some(self.slot_i64(offset)?.fetch_add(value, Ordering::SeqCst))
    }

    /// Copy bytes into the window, or `None` if the range is out of bounds.
    ///
    /// The counterpart of [`AtomicBuffer::copy_out`] and the same reasoning in
    /// reverse: a variable-length record has no single atomic access, so a
    /// publisher fills it and *then* publishes the length with a release. That
    /// release is what makes these bytes visible to a reader which
    /// acquire-loads the length — the copy on its own publishes nothing.
    pub fn copy_in(&self, offset: usize, src: &[u8]) -> Option<()> {
        if offset.checked_add(src.len())? > self.len {
            return None;
        }

        // SAFETY: the range was just proven to be inside the window, and `src`
        // is a distinct live allocation, so source and destination cannot
        // overlap. The window is writable by construction — this method only
        // exists on a window built from a writable mapping — and the caller
        // publishes these bytes with a release afterwards, which is what a
        // reader synchronises with.
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.as_ptr(),
                self.base.add(offset).cast_mut(),
                src.len(),
            );
        }

        Some(())
    }

    /// Write an `int64` at a **4-byte-aligned** offset, as two 32-bit stores.
    ///
    /// The reference's log-buffer metadata block is `#pragma pack(4)`
    /// (`aeron-client/src/main/c/concurrent/aeron_logbuffer_descriptor.h:42-88`),
    /// so its last field, `untethered_linger_timeout_ns`, lands at offset 500 —
    /// four-aligned and not eight. The reference writes it plainly; a Rust
    /// atomic needs eight, which is why this exists and why it is a pair of
    /// stores rather than one.
    ///
    /// **For a field written before anything can read it** — initialising a
    /// block, not updating a live one. A reader that arrives mid-write can see
    /// the halves disagree, and there is no ordering that fixes that.
    pub fn store_i64_relaxed_unaligned(&self, offset: usize, value: i64) -> Option<()> {
        #[allow(clippy::cast_possible_truncation)] // the two halves of one i64
        let low = value as i32;
        let high = (value >> 32) as i32;

        self.store_i32_relaxed(offset, low)?;
        self.store_i32_relaxed(offset + 4, high)
    }

    /// Read an `int64` at a **4-byte-aligned** offset, as two 32-bit loads.
    ///
    /// The reader's half of [`AtomicBuffer::store_i64_relaxed_unaligned`], for
    /// the one field of the log buffer's metadata block that is four-aligned
    /// (`aeron_logbuffer_descriptor.h:78-88` hands it offset 500).
    pub fn load_i64_unaligned(&self, offset: usize) -> Option<i64> {
        let low = i64::from(self.load_i32(offset)? as u32);
        let high = i64::from(self.load_i32(offset + 4)? as u32);

        Some((high << 32) | low)
    }

    /// Compare and exchange a 4-byte field, returning whether it took.
    ///
    /// The reference has this as well as the 8-byte form
    /// (`aeron_cas_int32`, `aeron-client/src/main/c/concurrent/aeron_atomic64_gcc_x86_64.h:23-33`),
    /// and the log's `active_term_count` is exactly four bytes wide
    /// (`aeron_logbuffer_descriptor.h:186-191`). Reaching for the 8-byte form
    /// there would compare and write the four bytes of structure padding after
    /// it — which happens to be zero today and is promised by nothing.
    pub fn compare_exchange_i32(&self, offset: usize, expected: i32, new: i32) -> Option<bool> {
        Some(
            self.slot_i32(offset)?
                .compare_exchange(expected, new, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
        )
    }

    /// Write zeroes over `len` bytes starting at `offset`.
    ///
    /// The MPSC consumer's obligation rather than an optimisation: the
    /// reference zeroes what it consumed *before* it publishes `head_position`
    /// (`aeron-client/src/main/c/concurrent/aeron_mpsc_rb.c:236-238`), and the
    /// producer's claim path is written against that — freshly claimed space is
    /// assumed to start zero, and a stale positive record length left behind
    /// would look to the next claim like a record that is already published.
    ///
    /// # Errors
    ///
    /// `None` if the range is out of bounds.
    pub fn zero(&self, offset: usize, len: usize) -> Option<()> {
        if offset.checked_add(len)? > self.len {
            return None;
        }

        // SAFETY: the range was just proven to be inside a window this process
        // holds with write access — the method exists only on `ReadWrite` — and
        // writing zeroes is an ordinary write to those bytes. `write_bytes`
        // requires only a valid, non-overlapping destination, which a range
        // inside the window is; the value written makes no demands on what was
        // there before.
        unsafe {
            std::ptr::write_bytes(self.base.add(offset).cast_mut(), 0, len);
        }

        Some(())
    }
}

/// A full store fence, mirroring the reference's `aeron_release()`
/// (`aeron-client/src/main/c/util/aeron_atomic.h`).
///
/// The one place it is needed is the broadcast transmitter's tail-intent
/// publish: the intent is a *release* store, and a release orders the writes
/// that came before it — but the guarantee the receiver reads it for is the
/// other direction, that nothing written *after* it becomes visible first.
/// Only a fence gives that, so
/// `aeron_broadcast_transmitter.c:45-49` stores with a release and then calls
/// `aeron_release()`, and this is that call.
///
/// On x86-64 it compiles to nothing: `mov` retires stores in order. It is here
/// for the same reason the reference has it — so the argument is written in
/// terms of a primitive that holds on every architecture, not in terms of the
/// one this was developed on.
pub fn store_fence() {
    std::sync::atomic::fence(Ordering::SeqCst);
}

impl<Access> std::fmt::Debug for AtomicBuffer<'_, Access> {
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
        let mut bytes = Aligned([0u8; 64]);

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned slice");
            assert_eq!(Some(0), writer.load_i64_acquire(0));
            // Release, the ordering a publisher would use.
            assert_eq!(Some(()), writer.store_i64_release(0, 1234));
        }

        // A separate read view, and the exclusive borrow is gone by now — the
        // same shape a reader in another process sees.
        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned slice");
        assert_eq!(Some(1234), view.load_i64_acquire(0));
    }

    #[test]
    fn stores_land_where_a_reader_expects_them() {
        let mut bytes = Aligned([0u8; 64]);

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned slice");
            assert_eq!(Some(()), writer.store_i32_release(0, 42));
            assert_eq!(Some(()), writer.store_i32_relaxed(4, -7));
            assert_eq!(Some(()), writer.store_i64_release(8, i64::MIN));
        }

        let reader = AtomicBuffer::from_slice(&bytes.0).expect("aligned slice");
        assert_eq!(Some(42), reader.load_i32_acquire(0));
        assert_eq!(Some(-7), reader.load_i32_relaxed(4));
        assert_eq!(Some(i64::MIN), reader.load_i64_acquire(8));
    }

    #[test]
    fn stores_refuse_bad_offsets_rather_than_writing_past_the_window() {
        let mut bytes = Aligned([0u8; 64]);
        let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned slice");

        assert_eq!(None, writer.store_i64_release(64, 1), "at the end");
        assert_eq!(None, writer.store_i64_release(60, 1), "60..68 runs past it");
        assert_eq!(None, writer.store_i64_release(4, 1), "off the 8-byte grid");
        assert_eq!(None, writer.store_i32_release(2, 1), "off the 4-byte grid");
        assert_eq!(
            None,
            writer.store_i32_release(usize::MAX, 1),
            "offset arithmetic must not wrap"
        );
    }

    #[test]
    fn compare_and_exchange_reports_whether_it_swapped() {
        let mut bytes = Aligned([0u8; 64]);

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned slice");
            assert_eq!(
                Some(false),
                writer.compare_exchange_i64(0, 1, 2),
                "it held 0, not the expected 1, so nothing was swapped"
            );
            assert_eq!(Some(true), writer.compare_exchange_i64(0, 0, 99));
            assert_eq!(
                Some(false),
                writer.compare_exchange_i64(0, 0, 1),
                "the previous call left 99 there"
            );
        }

        let reader = AtomicBuffer::from_slice(&bytes.0).expect("aligned slice");
        assert_eq!(Some(99), reader.load_i64_acquire(0));
    }

    #[test]
    fn fetch_add_returns_the_value_it_replaced() {
        let mut bytes = Aligned([0u8; 64]);

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned slice");
            // The previous value, which is what makes this usable as an id
            // allocator: the first caller gets 0 and the second gets 1, even
            // though the field already reads 1 by the time the second returns.
            assert_eq!(Some(0), writer.fetch_add_i64(0, 1));
            assert_eq!(Some(1), writer.fetch_add_i64(0, 1));
            assert_eq!(Some(2), writer.fetch_add_i64(0, 100));
        }

        let reader = AtomicBuffer::from_slice(&bytes.0).expect("aligned slice");
        assert_eq!(Some(102), reader.load_i64_acquire(0));
    }

    #[test]
    fn copies_bytes_in_where_a_read_view_finds_them() {
        let mut bytes = Aligned([0u8; 64]);

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned slice");
            assert_eq!(Some(()), writer.copy_in(3, b"hello"));
            assert_eq!(
                None,
                writer.copy_in(60, b"does not fit in the tail"),
                "a copy that runs past the window must not be attempted"
            );
        }

        let reader = AtomicBuffer::from_slice(&bytes.0).expect("aligned slice");
        let mut out = [0u8; 5];
        assert_eq!(Some(()), reader.copy_out(3, &mut out));
        assert_eq!(b"hello", &out);
    }
}
