//! The producer half of the to-driver MPSC ring.
//!
//! A client sends a command by claiming a record in this ring and filling it in
//! place; the driver is the ring's single consumer. There is no reply channel
//! on this path — a command's only acknowledgement is whatever the driver
//! publishes on the to-clients ring, and `TERMINATE_DRIVER` has none at all.
//!
//! Mirrors `aeron-client/src/main/c/concurrent/aeron_mpsc_rb.c:43-202`. The
//! protocol is subtle in two places, and both are reproduced exactly rather
//! than paraphrased:
//!
//! - **A record is published by a positive length.** While it is being filled
//!   the length is negative, and a consumer that arrives mid-write reads
//!   `length <= 0` and stops. That is the whole synchronisation: the consumer
//!   never compares indices, it just walks records until one is not ready.
//! - **A record that would straddle the end of the buffer is replaced by a
//!   padding record**, and the message goes at index 0. The tail is advanced
//!   past the padding *before* the padding header is published, so a consumer
//!   in that window sees a zero length and waits — which is exactly the stall
//!   `aeron_mpsc_rb_unblock` exists to break if a producer dies there.
//!
//! # What this type does not own
//!
//! `head_position` belongs to the consumer, and `consumer_heartbeat` belongs to
//! whatever is reading the ring. A producer writes exactly two descriptor
//! fields: `tail_position`, by compare-and-exchange, and `head_cache_position`.
//! It also never zeroes anything — the consumer zeroes what it consumes, and
//! that ordering is what lets a producer assume its claimed space starts zeroed.

use deepmsg_core::buffer::{AtomicBuffer, ReadWrite};

use crate::layout;

/// Why a claim did not produce a writable record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimError {
    /// The ring is full. The reference's callers spin and retry; a one-shot
    /// command should report it instead.
    Full,
    /// The message can never fit: a type id below 1, or a payload larger than
    /// [`ToDriverRing::max_message_length`]. Retrying will not help.
    Invalid,
}

/// A producer over the to-driver command ring.
///
/// Constructed from the **whole** region — the ring's index space *and* its
/// trailer — because the descriptor lives at the end of it, after the area the
/// record indices address.
pub struct ToDriverRing<'a> {
    buffer: AtomicBuffer<'a, ReadWrite>,
    /// The ring's index space: everything before the trailer.
    capacity: usize,
    /// `capacity / 8`, or zero at the minimum capacity. See
    /// [`ToDriverRing::max_message_length`].
    max_message_length: usize,
}

impl<'a> ToDriverRing<'a> {
    /// Wrap a region, or `None` if it cannot be a ring.
    ///
    /// The reference's capacity rules (`aeron-client/src/main/c/concurrent/aeron_rb.h:79-82`):
    /// a power of two, at least [`layout::MPSC_MIN_CAPACITY`], and below
    /// `INT32_MAX`.
    pub fn new(region: AtomicBuffer<'a, ReadWrite>) -> Option<Self> {
        let capacity = region.len().checked_sub(layout::MPSC_RB_TRAILER_LENGTH)?;

        if !capacity.is_power_of_two()
            || capacity < layout::MPSC_MIN_CAPACITY
            || capacity >= i32::MAX as usize
        {
            return None;
        }

        let max_message_length = Self::max_message_length_for(capacity);

        Some(Self {
            buffer: region,
            capacity,
            max_message_length,
        })
    }

    /// The ring's index space, trailer excluded.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// The largest payload a single record can carry.
    ///
    /// `capacity / 8`, and **zero** when the capacity is exactly the minimum
    /// (`aeron-client/src/main/c/concurrent/aeron_rb.h:57`). The zero is not a
    /// bug: the margin guarantees a record can never be so large relative to
    /// the ring that the wrap branch is unwinnable, and at the smallest legal
    /// capacity there is no room for any message at all. The reference pins the
    /// same behaviour in `aeron_c_terminate_test.cpp:209-214`.
    pub const fn max_message_length(&self) -> usize {
        self.max_message_length
    }

    const fn max_message_length_for(capacity: usize) -> usize {
        if capacity == layout::MPSC_MIN_CAPACITY {
            0
        } else {
            capacity / 8
        }
    }

    /// Reserve space for a record and return the offset to write the **message**
    /// at — past the record header, which this call has already written.
    ///
    /// # Errors
    ///
    /// [`ClaimError::Invalid`] if the message could never fit,
    /// [`ClaimError::Full`] if it does not fit right now.
    pub fn claim(&self, msg_type_id: i32, length: usize) -> Result<usize, ClaimError> {
        // `AERON_RB_INVALID_MSG_TYPE_ID` is `id < 1`, so 0 and the padding
        // sentinel are both refused (`aeron-client/src/main/c/concurrent/aeron_rb.h:58`).
        if msg_type_id < 1 || length > self.max_message_length {
            return Err(ClaimError::Invalid);
        }

        let record_length = length + layout::RECORD_HEADER_LENGTH;
        let index = self.claim_capacity(record_length)?;

        // In flight: the negative length is what a consumer stops at. Release,
        // so the type store below cannot be reordered before it — though the
        // commit's release is what actually publishes both.
        self.buffer
            .store_i32_release(
                index + layout::RECORD_LENGTH_OFFSET,
                -(record_length as i32),
            )
            .ok_or(ClaimError::Invalid)?;
        self.buffer
            .store_i32_relaxed(index + layout::RECORD_MSG_TYPE_ID_OFFSET, msg_type_id)
            .ok_or(ClaimError::Invalid)?;

        Ok(index + layout::RECORD_HEADER_LENGTH)
    }

    /// Publish a claimed record, making it visible to the consumer.
    ///
    /// Returns false if `offset` is not a claimed record — already committed,
    /// out of range, or never claimed.
    pub fn commit(&self, offset: usize) -> bool {
        let Some(index) = self.record_index(offset) else {
            return false;
        };

        let Some(length) = self
            .buffer
            .load_i32_acquire(index + layout::RECORD_LENGTH_OFFSET)
        else {
            return false;
        };

        if length < 0 {
            self.buffer
                .store_i32_release(index + layout::RECORD_LENGTH_OFFSET, -length)
                .is_some()
        } else {
            false
        }
    }

    /// Give a claimed record back, leaving it as padding.
    ///
    /// Mirrors `aeron_mpsc_rb_abort`: the type becomes the padding sentinel and
    /// the length is flipped positive, so the consumer skips the record rather
    /// than waiting on it forever. The bytes are not reclaimed — the tail has
    /// already moved past them.
    pub fn abort(&self, offset: usize) -> bool {
        let Some(index) = self.record_index(offset) else {
            return false;
        };

        let Some(length) = self
            .buffer
            .load_i32_acquire(index + layout::RECORD_LENGTH_OFFSET)
        else {
            return false;
        };

        if length >= 0 {
            return false;
        }

        self.buffer
            .store_i32_relaxed(
                index + layout::RECORD_MSG_TYPE_ID_OFFSET,
                layout::PADDING_MSG_TYPE_ID,
            )
            .is_some()
            && self
                .buffer
                .store_i32_release(index + layout::RECORD_LENGTH_OFFSET, -length)
                .is_some()
    }

    /// Claim, copy and commit in one step.
    ///
    /// # Errors
    ///
    /// As [`ToDriverRing::claim`].
    pub fn write(&self, msg_type_id: i32, payload: &[u8]) -> Result<(), ClaimError> {
        let offset = self.claim(msg_type_id, payload.len())?;

        self.buffer
            .copy_in(offset, payload)
            .ok_or(ClaimError::Invalid)?;

        if self.commit(offset) {
            Ok(())
        } else {
            // Only reachable if the record we just claimed was not ours to
            // commit, which would be a bug in this type rather than a state the
            // caller can cause.
            debug_assert!(false, "a record this call just claimed failed to commit");
            Err(ClaimError::Invalid)
        }
    }

    /// The next correlation id, or `None` if the trailer is unreadable.
    ///
    /// Shared by every producer of this ring, in this process and in others,
    /// and starts at zero — the driver burns one value at startup, so the first
    /// client sees 1.
    pub fn next_correlation_id(&self) -> Option<i64> {
        self.buffer.fetch_add_i64(
            self.descriptor_offset(layout::MPSC_CORRELATION_COUNTER_OFFSET)?,
            1,
        )
    }

    /// The tail position: the producer's byte counter, which only grows.
    pub fn producer_position(&self) -> Option<i64> {
        self.buffer
            .load_i64_acquire(self.descriptor_offset(layout::MPSC_TAIL_POSITION_OFFSET)?)
    }

    /// The consumer's position, which it advances only after zeroing what it
    /// consumed.
    pub fn consumer_position(&self) -> Option<i64> {
        self.buffer
            .load_i64_acquire(self.descriptor_offset(layout::MPSC_HEAD_POSITION_OFFSET)?)
    }

    /// The consumer's heartbeat, which this type reads and never writes.
    pub fn consumer_heartbeat(&self) -> Option<i64> {
        self.buffer
            .load_i64_acquire(self.descriptor_offset(layout::MPSC_CONSUMER_HEARTBEAT_OFFSET)?)
    }

    /// The offset of a trailer field, absolute in the region.
    fn descriptor_offset(&self, field_offset: usize) -> Option<usize> {
        self.capacity.checked_add(field_offset)
    }

    /// Turn a message offset back into a record index, rejecting anything that
    /// could not have come from [`ToDriverRing::claim`].
    fn record_index(&self, message_offset: usize) -> Option<usize> {
        let index = message_offset.checked_sub(layout::RECORD_HEADER_LENGTH)?;
        if index > self.capacity - layout::RECORD_HEADER_LENGTH {
            return None;
        }
        Some(index)
    }

    /// Reserve `record_length` bytes of index space.
    ///
    /// The whole of `aeron_mpsc_rb_claim_capacity`, including the two-stage
    /// full check and the wrap decision. `head` starts as the producers' shared
    /// cache and is refreshed from the authoritative `head_position` only when
    /// space looks tight.
    fn claim_capacity(&self, record_length: usize) -> Result<usize, ClaimError> {
        let required = layout::align_up(record_length, layout::RECORD_ALIGNMENT);
        let mask = self.capacity - 1;
        let capacity = self.capacity as i64;

        let mut head = self
            .buffer
            .load_i64_acquire(
                self.descriptor_offset(layout::MPSC_HEAD_CACHE_POSITION_OFFSET)
                    .ok_or(ClaimError::Invalid)?,
            )
            .ok_or(ClaimError::Invalid)?;

        loop {
            let tail = self
                .buffer
                .load_i64_acquire(
                    self.descriptor_offset(layout::MPSC_TAIL_POSITION_OFFSET)
                        .ok_or(ClaimError::Invalid)?,
                )
                .ok_or(ClaimError::Invalid)?;

            if required as i64 > capacity - (tail - head) {
                head = self
                    .buffer
                    .load_i64_acquire(
                        self.descriptor_offset(layout::MPSC_HEAD_POSITION_OFFSET)
                            .ok_or(ClaimError::Invalid)?,
                    )
                    .ok_or(ClaimError::Invalid)?;

                if required as i64 > capacity - (tail - head) {
                    return Err(ClaimError::Full);
                }

                // Publish the refreshed head for the other producers. Release,
                // because they acquire it.
                self.buffer
                    .store_i64_release(
                        self.descriptor_offset(layout::MPSC_HEAD_CACHE_POSITION_OFFSET)
                            .ok_or(ClaimError::Invalid)?,
                        head,
                    )
                    .ok_or(ClaimError::Invalid)?;
            }

            let mut padding = 0usize;
            let mut tail_index = (tail as usize) & mask;
            let to_buffer_end = self.capacity - tail_index;

            if required > to_buffer_end {
                let mut head_index = (head as usize) & mask;

                if required > head_index {
                    head = self
                        .buffer
                        .load_i64_acquire(
                            self.descriptor_offset(layout::MPSC_HEAD_POSITION_OFFSET)
                                .ok_or(ClaimError::Invalid)?,
                        )
                        .ok_or(ClaimError::Invalid)?;
                    head_index = (head as usize) & mask;

                    if required > head_index {
                        return Err(ClaimError::Full);
                    }

                    self.buffer
                        .store_i64_release(
                            self.descriptor_offset(layout::MPSC_HEAD_CACHE_POSITION_OFFSET)
                                .ok_or(ClaimError::Invalid)?,
                            head,
                        )
                        .ok_or(ClaimError::Invalid)?;
                }

                padding = to_buffer_end;
            }

            let new_tail = tail + (required + padding) as i64;
            let swapped = self
                .buffer
                .compare_exchange_i64(
                    self.descriptor_offset(layout::MPSC_TAIL_POSITION_OFFSET)
                        .ok_or(ClaimError::Invalid)?,
                    tail,
                    new_tail,
                )
                .ok_or(ClaimError::Invalid)?;

            if !swapped {
                // Another producer claimed first; re-read the tail and retry.
                // `padding` is recomputed from scratch each pass, as the
                // reference does.
                continue;
            }

            if 0 != padding {
                // After the CAS, deliberately. Between the two the tail is
                // already past this index but the header still reads zero, so a
                // consumer stops there rather than misreading — the stall
                // `aeron_mpsc_rb_unblock` breaks if a producer dies in it.
                let length = padding as i32;
                self.buffer
                    .store_i32_release(tail_index + layout::RECORD_LENGTH_OFFSET, -length)
                    .ok_or(ClaimError::Invalid)?;
                self.buffer
                    .store_i32_relaxed(
                        tail_index + layout::RECORD_MSG_TYPE_ID_OFFSET,
                        layout::PADDING_MSG_TYPE_ID,
                    )
                    .ok_or(ClaimError::Invalid)?;
                self.buffer
                    .store_i32_release(tail_index + layout::RECORD_LENGTH_OFFSET, length)
                    .ok_or(ClaimError::Invalid)?;

                tail_index = 0;
            }

            return Ok(tail_index);
        }
    }
}

impl std::fmt::Debug for ToDriverRing<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToDriverRing")
            .field("capacity", &self.capacity)
            .field("max_message_length", &self.max_message_length)
            .field("producer_position", &self.producer_position())
            .field("consumer_position", &self.consumer_position())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ring region with an 8-byte-aligned base, which a `Vec<u8>` does not
    /// guarantee — the heap allocation is only 1-aligned.
    ///
    /// Capacity 1024, so the region is 1024 + 768 = 1792 and
    /// `max_message_length` is 128.
    #[repr(align(64))]
    struct Region<const N: usize>([u8; N]);

    const CAPACITY: usize = 1024;
    const REGION: usize = CAPACITY + layout::MPSC_RB_TRAILER_LENGTH;

    fn region() -> Region<REGION> {
        Region([0u8; REGION])
    }

    /// Read a record header the way a consumer would — through the shared
    /// bytes, not through the producer's API.
    ///
    /// The ring is the only handle on those bytes here (it holds the exclusive
    /// borrow a writable window requires), which is itself a fair picture of
    /// the real relationship: the consumer has the memory, not this type.
    fn header(ring: &ToDriverRing<'_>, index: usize) -> (i32, i32) {
        (
            ring.buffer
                .load_i32_acquire(index + layout::RECORD_LENGTH_OFFSET)
                .expect("in range"),
            ring.buffer
                .load_i32_acquire(index + layout::RECORD_MSG_TYPE_ID_OFFSET)
                .expect("in range"),
        )
    }

    fn payload_at(ring: &ToDriverRing<'_>, index: usize, len: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        ring.buffer
            .copy_out(index + layout::RECORD_HEADER_LENGTH, &mut out)
            .expect("in range");
        out
    }

    /// Force the producer position, standing in for a ring that has already
    /// carried traffic.
    fn set_tail(ring: &ToDriverRing<'_>, value: i64) {
        ring.buffer
            .store_i64_release(
                ring.descriptor_offset(layout::MPSC_TAIL_POSITION_OFFSET)
                    .expect("in range"),
                value,
            )
            .expect("in range");
    }

    /// Stand in for a consumer that has already read and zeroed `value` bytes.
    ///
    /// This is a test writing a field real producers must not touch — but a
    /// ring with no consumer is a ring that can never wrap, and the wrap path is
    /// the one worth testing.
    fn set_consumed(ring: &ToDriverRing<'_>, value: i64) {
        for offset in [
            layout::MPSC_HEAD_POSITION_OFFSET,
            layout::MPSC_HEAD_CACHE_POSITION_OFFSET,
        ] {
            ring.buffer
                .store_i64_release(ring.descriptor_offset(offset).expect("in range"), value)
                .expect("in range");
        }
    }

    #[test]
    fn reports_its_shape() {
        let mut region = region();
        let ring =
            ToDriverRing::new(AtomicBuffer::from_slice_mut(&mut region.0).expect("aligned region"))
                .expect("a valid ring");

        assert_eq!(CAPACITY, ring.capacity());
        assert_eq!(CAPACITY / 8, ring.max_message_length());
    }

    #[test]
    fn refuses_a_region_that_cannot_be_a_ring() {
        // A capacity that is not a power of two, which the reference rejects at
        // init (`aeron_rb.h:79-82`) and pins in
        // `aeron_c_terminate_test.cpp:168-190`.
        const ODD: usize = 1000 + layout::MPSC_RB_TRAILER_LENGTH;
        let mut odd = Region::<ODD>([0u8; ODD]);
        assert!(
            ToDriverRing::new(AtomicBuffer::from_slice_mut(&mut odd.0).expect("aligned")).is_none()
        );

        // And a region too short to hold a trailer at all.
        const TINY: usize = layout::MPSC_RB_TRAILER_LENGTH;
        let mut tiny = Region::<TINY>([0u8; TINY]);
        assert!(
            ToDriverRing::new(AtomicBuffer::from_slice_mut(&mut tiny.0).expect("aligned"))
                .is_none()
        );
    }

    #[test]
    fn writes_a_record_a_consumer_can_follow() {
        let mut region = region();
        let ring =
            ToDriverRing::new(AtomicBuffer::from_slice_mut(&mut region.0).expect("aligned region"))
                .expect("a valid ring");

        ring.write(0x0E, b"exit please").expect("fits");

        let (length, msg_type_id) = header(&ring, 0);
        assert_eq!(
            (layout::RECORD_HEADER_LENGTH + 11) as i32,
            length,
            "a positive length is what publishes the record"
        );
        assert_eq!(0x0E, msg_type_id);
        assert_eq!(b"exit please".to_vec(), payload_at(&ring, 0, 11));
        assert_eq!(
            Some(
                layout::align_up(layout::RECORD_HEADER_LENGTH + 11, layout::RECORD_ALIGNMENT)
                    as i64
            ),
            ring.producer_position(),
            "the tail advances by the record length rounded up to the \
             alignment -- 19 bytes of record occupy 24 bytes of ring"
        );
    }

    #[test]
    fn a_claimed_record_is_invisible_until_it_is_committed() {
        let mut region = region();
        let ring =
            ToDriverRing::new(AtomicBuffer::from_slice_mut(&mut region.0).expect("aligned region"))
                .expect("a valid ring");

        let offset = ring.claim(0x0E, 4).expect("fits");

        let (length, msg_type_id) = header(&ring, 0);
        assert_eq!(
            -(layout::RECORD_HEADER_LENGTH as i32 + 4),
            length,
            "while in flight the length is negative, which is how a consumer \
             knows to stop rather than skip"
        );
        assert_eq!(
            0x0E, msg_type_id,
            "the type is written, but not yet published"
        );

        assert!(ring.commit(offset));

        let (length, _) = header(&ring, 0);
        assert_eq!((layout::RECORD_HEADER_LENGTH + 4) as i32, length);
    }

    #[test]
    fn commit_refuses_an_offset_that_is_not_a_claimed_record() {
        let mut region = region();
        let ring =
            ToDriverRing::new(AtomicBuffer::from_slice_mut(&mut region.0).expect("aligned region"))
                .expect("a valid ring");

        assert!(!ring.commit(0), "nothing was ever claimed at index 0");
        assert!(!ring.commit(usize::MAX), "not even a plausible offset");
        assert!(!ring.commit(3), "below the header length");

        let offset = ring.claim(0x0E, 4).expect("fits");
        assert!(ring.commit(offset));
        assert!(
            !ring.commit(offset),
            "committing twice must fail -- the second call would find the \
             length already positive"
        );
    }

    #[test]
    fn abort_leaves_the_record_as_padding_rather_than_a_stall() {
        let mut region = region();
        let ring =
            ToDriverRing::new(AtomicBuffer::from_slice_mut(&mut region.0).expect("aligned region"))
                .expect("a valid ring");

        let offset = ring.claim(0x0E, 4).expect("fits");
        assert!(ring.abort(offset));

        let (length, msg_type_id) = header(&ring, 0);
        assert!(length > 0, "a consumer must not wait on an aborted record");
        assert_eq!(
            layout::PADDING_MSG_TYPE_ID,
            msg_type_id,
            "and must skip it rather than dispatch it"
        );
    }

    #[test]
    fn refuses_a_message_or_type_that_could_never_fit() {
        let mut region = region();
        let ring =
            ToDriverRing::new(AtomicBuffer::from_slice_mut(&mut region.0).expect("aligned region"))
                .expect("a valid ring");

        assert_eq!(
            Err(ClaimError::Invalid),
            ring.claim(0x0E, ring.max_message_length() + 1)
        );
        assert_eq!(
            Err(ClaimError::Invalid),
            ring.claim(0, 4),
            "type 0 is invalid"
        );
        assert_eq!(
            Err(ClaimError::Invalid),
            ring.claim(-1, 4),
            "padding is not a type to send"
        );
        assert_eq!(
            Ok(()),
            ring.write(0x0E, &vec![0u8; ring.max_message_length()]),
            "exactly the maximum is allowed -- the check is `>`, not `>=`"
        );
    }

    #[test]
    fn wraps_with_a_padding_record_when_a_message_would_straddle_the_end() {
        let mut region = region();
        let ring =
            ToDriverRing::new(AtomicBuffer::from_slice_mut(&mut region.0).expect("aligned region"))
                .expect("a valid ring");

        // Eight bytes left at the end: not enough for a 16-byte record, so the
        // writer must pad and restart at index 0. `set_consumed` matters here —
        // with nothing consumed the ring is genuinely full and the reference
        // returns FULL rather than wrapping.
        set_consumed(&ring, 64);
        set_tail(&ring, (CAPACITY - 8) as i64);

        ring.write(0x0E, b"four")
            .expect("fits at index 0 after padding");

        // The padding record fills exactly the distance to the end of the ring.
        let (padding_length, padding_type) = header(&ring, CAPACITY - 8);
        assert_eq!(8, padding_length);
        assert_eq!(layout::PADDING_MSG_TYPE_ID, padding_type);

        // And the message landed at zero.
        let (length, msg_type_id) = header(&ring, 0);
        assert_eq!((layout::RECORD_HEADER_LENGTH + 4) as i32, length);
        assert_eq!(0x0E, msg_type_id);
        assert_eq!(b"four".to_vec(), payload_at(&ring, 0, 4));

        assert_eq!(
            Some(
                (CAPACITY
                    + layout::align_up(layout::RECORD_HEADER_LENGTH + 4, layout::RECORD_ALIGNMENT))
                    as i64
            ),
            ring.producer_position(),
            "the tail moved past both the padding (8) and the aligned record (16)"
        );
    }

    #[test]
    fn refuses_a_claim_when_the_ring_is_full() {
        let mut region = region();
        let ring =
            ToDriverRing::new(AtomicBuffer::from_slice_mut(&mut region.0).expect("aligned region"))
                .expect("a valid ring");

        // Head and tail one apart: the reference pins this shape in
        // `aeron_c_terminate_test.cpp:217-244`.
        set_tail(&ring, (CAPACITY - 1) as i64);

        assert_eq!(Err(ClaimError::Full), ring.claim(0x0E, 11));
    }

    #[test]
    fn correlation_ids_start_at_zero_and_never_repeat() {
        let mut region = region();
        let ring =
            ToDriverRing::new(AtomicBuffer::from_slice_mut(&mut region.0).expect("aligned region"))
                .expect("a valid ring");

        // The reference's terminate path takes two, filling `client_id` and
        // `correlation_id` from consecutive values.
        let first = ring.next_correlation_id().expect("readable");
        let second = ring.next_correlation_id().expect("readable");

        assert_eq!(0, first);
        assert_eq!(1, second);
    }

    #[test]
    fn maximum_message_length_collapses_to_zero_at_the_minimum_capacity() {
        // `AERON_RB_MAX_MESSAGE_LENGTH`'s `capacity == min_capacity` case
        // (`aeron_rb.h:57`), which makes *every* command invalid on such a ring.
        assert_eq!(
            0,
            ToDriverRing::max_message_length_for(layout::MPSC_MIN_CAPACITY)
        );
        assert_eq!(128, ToDriverRing::max_message_length_for(1024));
    }

    #[test]
    fn a_producer_does_not_touch_the_fields_the_consumer_owns() {
        let mut region = region();
        let ring =
            ToDriverRing::new(AtomicBuffer::from_slice_mut(&mut region.0).expect("aligned region"))
                .expect("a valid ring");

        // Give the consumer's fields values a producer must leave alone.
        let head_slot = ring
            .descriptor_offset(layout::MPSC_HEAD_POSITION_OFFSET)
            .expect("in range");
        let heartbeat_slot = ring
            .descriptor_offset(layout::MPSC_CONSUMER_HEARTBEAT_OFFSET)
            .expect("in range");
        ring.buffer
            .store_i64_release(head_slot, 7)
            .expect("writable");
        ring.buffer
            .store_i64_release(heartbeat_slot, 1234)
            .expect("writable");

        ring.write(0x0E, b"something").expect("fits");
        let _ = ring.next_correlation_id();

        assert_eq!(
            Some(7),
            ring.consumer_position(),
            "head_position is the consumer's"
        );
        assert_eq!(
            Some(1234),
            ring.consumer_heartbeat(),
            "consumer_heartbeat is the consumer's"
        );
    }
}
