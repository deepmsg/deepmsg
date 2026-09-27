//! The to-clients broadcast ring: driver→client events.
//!
//! This is the only ring in the system that is genuinely broadcast. The
//! to-driver ring is MPSC — many clients, one driver — but every client reads
//! the *whole* to-clients stream, including events addressed to other clients.
//! A reader therefore filters by correlation id and must tolerate all thirteen
//! response types plus anything a newer driver invents.
//!
//! Both ends are here because they are one contract: [`ToClientsTransmitter`]
//! mirrors `aeron-client/src/main/c/concurrent/aeron_broadcast_transmitter.{c,h}`
//! and [`ToClientsReceiver`] mirrors `…_receiver.{c,h}`, and the property that
//! makes them a pair — a reader can tell a complete record from one being
//! overwritten *while it reads* — is a statement about the two together, not
//! about either one.
//!
//! # The transmitter writes a record once
//!
//! The C transmitter is **not** the Java one, and the difference is a trap for
//! anyone porting from the Java side. There is no second length-and-type-id
//! write after the body, and no per-record commit header: the header is
//! written once, *before* the payload, and the publication is the single
//! release store of `tail_counter`
//! (`aeron-client/src/main/c/concurrent/aeron_broadcast_transmitter.c:96-102`).
//! A reader that waits for a "record is complete" marker in the record itself
//! — which is how the Java reader works — waits forever here; ours decides by
//! `tail_counter > next_record`, as the C reader does.
//!
//! The one ordering that *is* explicit is the tail-intent: it is raised (with a
//! release **and** a full fence, `:45-49`) before anything in the record is
//! written, because it is what a lapped reader measures itself against — see
//! the receiver's third note below.
//!
//! # The three things that are not obvious
//!
//! **The cursor starts at `latest_counter`, not `tail_counter`.** `tail_counter`
//! is one *past* the newest record, so a reader starting there would skip it
//! (`aeron_broadcast_receiver.c:47-53`). Starting at `latest` means a late
//! joiner sees exactly one message and then the live edge — the ring keeps no
//! history and no reader can recover any.
//!
//! **One message per call.** `receive` advances at most one record, as the
//! reference does (`:72-129`, driven once per duty cycle at
//! `aeron_client_conductor.c:2728`). Java's Agrona drains in a loop; copying
//! that would change the caller's latency characteristics, and the caller is
//! also where timeouts and command processing happen.
//!
//! **Copy first, validate after.** The check is `cursor + capacity >
//! tail_intent_counter`, and doing it *before* the copy would guard nothing:
//! the window that matters is the copy itself, and the reader holds no lock and
//! publishes no position the writer respects. After the copy the pairing holds,
//! because the writer raises the intent *before* it destroys
//! (`aeron_broadcast_transmitter.c:81,90` precede the payload write at `:99`).
//! A failed validate discards the message; the cursor has already advanced, so
//! there is no retry, and that is deliberate.
//!
//! # A receiver writes nothing
//!
//! Unlike the MPSC ring, whose consumer publishes `head_position` and zeroes
//! what it consumed, this reader owns no descriptor field. So a slow reader
//! cannot block a writer, loss is discovered only after the fact, and
//! [`ToClientsReceiver::lapped`] counts *events*, never messages — there is no
//! way to know how many were missed.

use deepmsg_core::buffer::{AtomicBuffer, ReadOnly, ReadWrite, store_fence};

use crate::layout;

/// What a transmit refused, and why.
///
/// The reference returns `-1` and sets an error string for the first two
/// (`aeron_broadcast_transmitter.c:60-69`); the third cannot happen for a
/// region this type constructed, and exists so that the impossible case has
/// somewhere to go other than a panic — a driver that cannot broadcast must
/// count the failure and keep running
/// (`aeron-driver/src/main/c/aeron_driver_conductor.c:2233-2241`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransmitError {
    /// The payload is longer than `capacity / 8`
    /// (`aeron_broadcast_descriptor.h:44`).
    MessageTooLong {
        /// The payload offered.
        length: usize,
        /// The largest payload this ring accepts.
        max: usize,
    },
    /// The type id is one a writer may not produce: `msg_type_id < 1`
    /// (`aeron_broadcast_descriptor.h:45`), which refuses zero as well as the
    /// `-1` that means "padding".
    InvalidTypeId {
        /// The type id offered.
        type_id: i32,
    },
    /// A record's offsets fell outside the region. Unreachable for a region
    /// this type built — see the variant's own note above.
    OutOfRange,
}

impl std::fmt::Display for TransmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MessageTooLong { length, max } => {
                write!(f, "message of {length} bytes exceeds the {max}-byte limit")
            }
            Self::InvalidTypeId { type_id } => {
                write!(f, "message type id {type_id} is not one a writer may send")
            }
            Self::OutOfRange => f.write_str("the record did not fit the region"),
        }
    }
}

impl std::error::Error for TransmitError {}

/// A writer over the to-clients region.
///
/// The driver is its only user, and the only writer on this ring in the whole
/// system — clients never transmit on it.
pub struct ToClientsTransmitter {
    capacity: usize,
}

impl ToClientsTransmitter {
    /// Wrap a region, or `None` if it cannot be a ring.
    ///
    /// The capacity rule is the reader's (`aeron_broadcast_descriptor.h:43`):
    /// a power of two, and non-zero, because a record's offset is the counter
    /// masked with `capacity - 1`.
    ///
    /// **One writer per ring.** Every `transmit` reads `tail_counter` and
    /// writes back past it, as the reference does
    /// (`aeron_broadcast_transmitter.c:71`), which is what lets a second
    /// transmitter on the same file continue where the first stopped — and
    /// what makes two of them *at once* overwrite each other's records. The
    /// ring has no way to enforce it; the driver owns its CnC file, and
    /// anything else that maps it writable is breaking that.
    pub fn new(region: &AtomicBuffer<ReadWrite>) -> Option<Self> {
        let capacity = region.len().checked_sub(layout::BROADCAST_TRAILER_LENGTH)?;

        if !capacity.is_power_of_two() || 0 == capacity {
            return None;
        }

        Some(Self { capacity })
    }

    /// The capacity of the record area.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// The largest payload a single record can carry, `capacity / 8`
    /// (`aeron_broadcast_descriptor.h:44`).
    pub const fn max_message_length(&self) -> usize {
        self.capacity / 8
    }

    /// Publish one record.
    ///
    /// The steps are the reference's (`aeron_broadcast_transmitter.c:71-102`)
    /// in its order, and the order is the whole of the algorithm:
    ///
    /// 1. the tail intent is raised — release, then a fence — to the end of
    ///    this record (plus the padding, if this one wraps), *before* any byte
    ///    of either record is written;
    /// 2. a record that would straddle the end of the ring is replaced by a
    ///    **padding** record — `msg_type_id = -1` first, then its length — and
    ///    the message restarts at offset 0;
    /// 3. the header is written in place, `length` before `msg_type_id`, both
    ///    plain, both before the payload;
    /// 4. the payload;
    /// 5. `latest_counter` (the start of this record), then `tail_counter`
    ///    (one past its end), each with a release. The second is the
    ///    publication.
    pub fn transmit(
        &mut self,
        region: &AtomicBuffer<ReadWrite>,
        type_id: i32,
        payload: &[u8],
    ) -> Result<(), TransmitError> {
        if type_id < 1 {
            return Err(TransmitError::InvalidTypeId { type_id });
        }
        let max = self.max_message_length();
        if payload.len() > max {
            return Err(TransmitError::MessageTooLong {
                length: payload.len(),
                max,
            });
        }

        // Read plainly, as the C does: this is the writer's own last position,
        // and the publication of *this* record is the release store below.
        let current_tail = region
            .load_i64_relaxed(self.trailer(layout::BROADCAST_TAIL_COUNTER_OFFSET))
            .ok_or(TransmitError::OutOfRange)?;

        let aligned = layout::align_up(
            payload.len() + layout::RECORD_HEADER_LENGTH,
            layout::RECORD_ALIGNMENT,
        );
        let new_tail = current_tail + aligned as i64;
        let to_end_of_buffer = self.capacity - self.index_of(current_tail);
        let mut tail = current_tail;
        let mut offset = self.index_of(current_tail);

        if to_end_of_buffer < aligned {
            self.signal_tail_intent(region, new_tail + to_end_of_buffer as i64)?;

            // The padding's own length is its distance to the end of the ring:
            // `capacity - offset`, where the capacity is a power of two and
            // every record starts on an 8-byte boundary — so it is always a
            // multiple of the alignment, and a reader that aligns it is doing
            // an identity. Both are kept: the reader's `align_up` is an
            // invariant guard, not dead code, and the record after the padding
            // starts at offset 0.
            self.store_i32(
                region,
                offset + layout::RECORD_MSG_TYPE_ID_OFFSET,
                layout::PADDING_MSG_TYPE_ID,
            )?;
            self.store_i32(
                region,
                offset + layout::RECORD_LENGTH_OFFSET,
                i32::try_from(to_end_of_buffer).map_err(|_| TransmitError::OutOfRange)?,
            )?;

            tail += to_end_of_buffer as i64;
            offset = 0;
        } else {
            self.signal_tail_intent(region, new_tail)?;
        }

        let record_length = payload.len() + layout::RECORD_HEADER_LENGTH;
        self.store_i32(
            region,
            offset + layout::RECORD_LENGTH_OFFSET,
            i32::try_from(record_length).map_err(|_| TransmitError::OutOfRange)?,
        )?;
        self.store_i32(region, offset + layout::RECORD_MSG_TYPE_ID_OFFSET, type_id)?;
        region
            .copy_in(offset + layout::RECORD_HEADER_LENGTH, payload)
            .ok_or(TransmitError::OutOfRange)?;

        // `latest_counter` lags `tail_counter` by exactly one record; both are
        // releases, and the second is what publishes the record body.
        self.store_i64(
            region,
            self.trailer(layout::BROADCAST_LATEST_COUNTER_OFFSET),
            tail,
        )?;
        self.store_i64(
            region,
            self.trailer(layout::BROADCAST_TAIL_COUNTER_OFFSET),
            tail + aligned as i64,
        )?;

        Ok(())
    }

    /// Raise the intent to `new_tail`, and make it stick before the bytes that
    /// follow (`aeron_broadcast_transmitter.c:45-49`).
    ///
    /// The fence is not decoration: a release store orders what came *before*
    /// it, and the guarantee the receiver needs is about what comes *after*.
    fn signal_tail_intent(
        &self,
        region: &AtomicBuffer<ReadWrite>,
        new_tail: i64,
    ) -> Result<(), TransmitError> {
        self.store_i64(
            region,
            self.trailer(layout::BROADCAST_TAIL_INTENT_COUNTER_OFFSET),
            new_tail,
        )?;
        store_fence();
        Ok(())
    }

    /// The ring offset a counter value addresses.
    ///
    /// The reference narrows to `uint32_t` before masking
    /// (`aeron_broadcast_transmitter.c:72`), so a counter past 2^32 wraps the
    /// same way it does in C.
    fn index_of(&self, cursor: i64) -> usize {
        (cursor as u32 as usize) & (self.capacity - 1)
    }

    fn trailer(&self, field: usize) -> usize {
        self.capacity + field
    }

    fn store_i32(
        &self,
        region: &AtomicBuffer<ReadWrite>,
        offset: usize,
        value: i32,
    ) -> Result<(), TransmitError> {
        region
            .store_i32_relaxed(offset, value)
            .ok_or(TransmitError::OutOfRange)
    }

    fn store_i64(
        &self,
        region: &AtomicBuffer<ReadWrite>,
        offset: usize,
        value: i64,
    ) -> Result<(), TransmitError> {
        region
            .store_i64_release(offset, value)
            .ok_or(TransmitError::OutOfRange)
    }
}

impl std::fmt::Debug for ToClientsTransmitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToClientsTransmitter")
            .field("capacity", &self.capacity)
            .finish()
    }
}

/// What one call to [`ToClientsReceiver::receive`] produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Received {
    /// Nothing new on the ring.
    Empty,
    /// A message arrived; its payload is in [`ToClientsReceiver::message`].
    Message {
        /// The record's `msg_type_id`.
        type_id: i32,
    },
    /// A message arrived but the writer overwrote it while it was being
    /// copied, so it was discarded rather than delivered torn. The cursor has
    /// already moved past it — this is not retryable.
    Discarded,
}

/// A reader over the to-clients region.
///
/// Constructed from the **whole** region — the record area *and* its trailer —
/// because the descriptor lives at the end of it.
pub struct ToClientsReceiver {
    /// The record area: everything before the trailer.
    capacity: usize,
    mask: usize,
    /// The offset of the trailer within the region.
    trailer: usize,
    /// Offset of the next record to read, in the ring's unbounded counter
    /// space.
    next_record: i64,
    /// The counter value of the record most recently read.
    ///
    /// Kept separately from `next_record` because the post-copy validation
    /// asks whether *the record we just read* is still live, and by then
    /// `next_record` has already moved past it. The reference calls this
    /// `cursor` and it is the same distinction.
    cursor: i64,
    /// Where the message currently in `scratch` came from.
    record_offset: usize,
    /// The payload of the most recent message, copied out of the ring. A copy,
    /// not a borrow: the ring belongs to the driver, which is still writing it.
    scratch: Vec<u8>,
    /// How much of `scratch` the last message actually used. The buffer is
    /// reused and only ever grows, so its length is not the message's.
    message_length: usize,
    lapped: u64,
    discarded: u64,
    malformed: u64,
}

impl ToClientsReceiver {
    /// The reference's initial scratch size
    /// (`aeron-client/src/main/c/concurrent/aeron_broadcast_receiver.h:24`).
    /// It grows on demand; the cap the writer enforces is a better bound.
    const INITIAL_SCRATCH: usize = 4096;

    /// Wrap a region, or `None` if it cannot be a ring.
    pub fn new(region: &AtomicBuffer<ReadOnly>) -> Option<Self> {
        let capacity = region.len().checked_sub(layout::BROADCAST_TRAILER_LENGTH)?;

        // The reference requires a power of two (`aeron_broadcast_descriptor.h:43`)
        // and masks with `capacity - 1`, so anything else computes nonsense.
        if !capacity.is_power_of_two() || 0 == capacity {
            return None;
        }

        let trailer = capacity;
        let next_record =
            region.load_i64_acquire(trailer + layout::BROADCAST_LATEST_COUNTER_OFFSET)?;

        Some(Self {
            capacity,
            mask: capacity - 1,
            trailer,
            next_record,
            cursor: next_record,
            record_offset: 0,
            scratch: vec![0; Self::INITIAL_SCRATCH],
            message_length: 0,
            lapped: 0,
            discarded: 0,
            malformed: 0,
        })
    }

    /// The payload of the message most recently delivered by
    /// [`ToClientsReceiver::receive`].
    ///
    /// A borrow of the scratch buffer, which the next `receive` overwrites and
    /// which a growth reallocates — so it is valid only until the next call,
    /// and anything kept must be copied out. This is why it is a copy at all:
    /// handing out a slice of the ring would be handing out memory the driver
    /// is still writing.
    pub fn message(&self) -> &[u8] {
        &self.scratch[..self.message_length]
    }

    /// How many times this reader was overtaken. Counts events, not messages.
    pub const fn lapped(&self) -> u64 {
        self.lapped
    }

    /// How many messages were overwritten mid-copy and thrown away.
    pub const fn discarded(&self) -> u64 {
        self.discarded
    }

    /// How many records carried a length the writer could not have produced.
    ///
    /// The reference has no equivalent: it trusts the record's own length when
    /// computing where the next one starts, and that length is a *signed*
    /// field, so garbage there can move its cursor backwards. Bounding it
    /// first is a deliberate divergence, recorded rather than silent.
    pub const fn malformed(&self) -> u64 {
        self.malformed
    }

    /// The capacity of the record area.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// The largest payload the writer will emit, `capacity / 8`
    /// (`aeron_broadcast_descriptor.h:44`).
    fn max_message_length(&self) -> usize {
        self.capacity / 8
    }

    /// Advance at most one record and copy its payload out.
    pub fn receive(&mut self, region: &AtomicBuffer<ReadOnly>) -> Received {
        let Some(tail) = self.load_counter(region, layout::BROADCAST_TAIL_COUNTER_OFFSET) else {
            return Received::Empty;
        };

        let mut cursor = self.next_record;
        if tail <= cursor {
            return Received::Empty;
        }

        let mut offset = self.index_of(cursor);

        if !self.slot_is_live(region, cursor) {
            // Overtaken. Resync to the newest record — forwards only, never
            // backwards, which is why a cursor ahead of a restarted driver's
            // counters has no recovery path.
            self.lapped += 1;
            let Some(latest) = self.load_counter(region, layout::BROADCAST_LATEST_COUNTER_OFFSET)
            else {
                return Received::Empty;
            };
            cursor = latest;
            offset = self.index_of(cursor);
        }

        let Some(length) = self.record_length(region, offset) else {
            self.malformed += 1;
            // Nowhere safe to go: this header is not a length the writer could
            // have produced, so where the next record starts is unknowable.
            return Received::Empty;
        };
        let Some(type_id) = self.load_i32(region, offset + layout::RECORD_MSG_TYPE_ID_OFFSET)
        else {
            self.malformed += 1;
            return Received::Empty;
        };

        self.cursor = cursor;
        self.next_record = cursor + layout::align_up(length, layout::RECORD_ALIGNMENT) as i64;

        if layout::PADDING_MSG_TYPE_ID == type_id {
            // The writer filled the tail of the ring to reach a boundary and
            // restarted at index 0. Our cursor is already past the padding, and
            // the message is the record at zero — whose **own** length moves
            // the cursor past it. Reading the padding's length instead would
            // land in the middle of the message.
            let Some(first_length) = self.record_length(region, 0) else {
                self.malformed += 1;
                return Received::Empty;
            };
            let Some(first_type) = self.load_i32(region, layout::RECORD_MSG_TYPE_ID_OFFSET) else {
                self.malformed += 1;
                return Received::Empty;
            };

            self.cursor = self.next_record;
            self.next_record += layout::align_up(first_length, layout::RECORD_ALIGNMENT) as i64;
            self.record_offset = 0;
            return self.finish(region, 0, first_type);
        }

        self.record_offset = offset;
        self.finish(region, offset, type_id)
    }

    /// Copy the record at `offset` into the scratch buffer, then validate.
    fn finish(&mut self, region: &AtomicBuffer<ReadOnly>, offset: usize, type_id: i32) -> Received {
        let Some(length) = self.record_length(region, offset) else {
            self.malformed += 1;
            return Received::Empty;
        };
        let payload = length - layout::RECORD_HEADER_LENGTH;

        if self.scratch.len() < payload {
            self.scratch.resize(payload, 0);
        }

        if region
            .copy_out(
                offset + layout::RECORD_HEADER_LENGTH,
                &mut self.scratch[..payload],
            )
            .is_none()
        {
            self.malformed += 1;
            return Received::Empty;
        }

        self.message_length = payload;

        // After the copy, not before — see the module docs. `self.cursor` is
        // the record just read; `next_record` has already moved past it, so
        // there is no retry and the message is simply dropped.
        if !self.slot_is_live(region, self.cursor) {
            self.discarded += 1;
            return Received::Discarded;
        }

        Received::Message { type_id }
    }

    /// A record's total length in bytes, if it is one the writer could have
    /// produced.
    fn record_length(&self, region: &AtomicBuffer<ReadOnly>, offset: usize) -> Option<usize> {
        let length = self.load_i32(region, offset + layout::RECORD_LENGTH_OFFSET)?;

        // The writer caps a payload at `capacity / 8` and stores the length
        // *including* the 8-byte header. Anything outside that is not a record,
        // and computing the next offset from it would be inventing a position.
        let max = self.max_message_length() + layout::RECORD_HEADER_LENGTH;
        if length < layout::RECORD_HEADER_LENGTH as i32 || length as usize > max {
            return None;
        }

        Some(length as usize)
    }

    /// Whether the slot at `cursor` has not yet been (or is not about to be)
    /// overwritten.
    fn slot_is_live(&self, region: &AtomicBuffer<ReadOnly>, cursor: i64) -> bool {
        self.load_counter(region, layout::BROADCAST_TAIL_INTENT_COUNTER_OFFSET)
            .is_none_or(|intent| cursor + self.capacity as i64 > intent)
    }

    /// The ring offset a counter value addresses.
    ///
    /// The reference narrows to `uint32_t` before masking, so a counter past
    /// 2^32 wraps the same way it does in C. Capacities here are far below
    /// that, which is why the mask alone would give the same answer — but the
    /// narrowing is what the reference does and costs nothing to keep.
    fn index_of(&self, cursor: i64) -> usize {
        (cursor as u32 as usize) & self.mask
    }

    fn load_counter(&self, region: &AtomicBuffer<ReadOnly>, field: usize) -> Option<i64> {
        region.load_i64_acquire(self.trailer + field)
    }

    fn load_i32(&self, region: &AtomicBuffer<ReadOnly>, offset: usize) -> Option<i32> {
        region.load_i32_acquire(offset)
    }
}

impl std::fmt::Debug for ToClientsReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToClientsReceiver")
            .field("capacity", &self.capacity)
            .field("next_record", &self.next_record)
            .field("lapped", &self.lapped)
            .field("discarded", &self.discarded)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(align(64))]
    struct Bytes<const N: usize>([u8; N]);

    const CAPACITY: usize = 1024;
    const REGION: usize = CAPACITY + layout::BROADCAST_TRAILER_LENGTH;

    /// A ring with a writer standing in for a driver.
    ///
    /// In production the writer is another process, so no one process holds
    /// both ends. Here it must — which is what `as_read_only` exists for.
    struct Fixture {
        bytes: Bytes<REGION>,
    }

    /// The real transmitter, plus the two pieces of surgery a test needs and a
    /// driver never does: forging a header no writer would produce, and
    /// overtaking a reader.
    struct Writer<'a> {
        buffer: AtomicBuffer<'a, ReadWrite>,
        transmitter: ToClientsTransmitter,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                bytes: Bytes([0u8; REGION]),
            }
        }

        /// Run `f` with a writer and a receiver over one ring.
        fn with<T>(&mut self, f: impl FnOnce(&mut Writer<'_>, &mut ToClientsReceiver) -> T) -> T {
            let mut writer = Writer::new(&mut self.bytes.0);
            let mut receiver =
                ToClientsReceiver::new(&writer.as_read_only()).expect("a valid ring");

            f(&mut writer, &mut receiver)
        }

        /// Publish first, *then* attach — the late-joiner case, which is about
        /// what a receiver constructed after traffic sees.
        fn attach_late<T>(
            &mut self,
            publish: impl FnOnce(&mut Writer<'_>),
            f: impl FnOnce(&mut Writer<'_>, &mut ToClientsReceiver) -> T,
        ) -> T {
            let mut writer = Writer::new(&mut self.bytes.0);
            publish(&mut writer);

            // Attached only now, so its cursor is read from a ring that has
            // already carried traffic.
            let mut receiver =
                ToClientsReceiver::new(&writer.as_read_only()).expect("a valid ring");
            f(&mut writer, &mut receiver)
        }
    }

    impl<'a> Writer<'a> {
        fn new(bytes: &'a mut [u8]) -> Self {
            let buffer = AtomicBuffer::from_slice_mut(bytes).expect("aligned region");
            let transmitter = ToClientsTransmitter::new(&buffer).expect("a valid ring");

            Self {
                buffer,
                transmitter,
            }
        }

        /// A read-only window on the same bytes, for a receiver.
        fn as_read_only(&self) -> AtomicBuffer<'a, ReadOnly> {
            self.buffer.as_read_only()
        }

        /// Where the next record starts, in the ring's counter space — read
        /// from the ring, because that is where the transmitter keeps it.
        fn next(&self) -> i64 {
            self.counter(layout::BROADCAST_TAIL_COUNTER_OFFSET)
        }

        fn trailer(&self, field: usize) -> usize {
            CAPACITY + field
        }

        /// One of the ring's three counters, read the way a reader reads it.
        fn counter(&self, field: usize) -> i64 {
            self.buffer
                .load_i64_acquire(self.trailer(field))
                .expect("in range")
        }

        fn record_header(&self, offset: usize) -> (i32, i32) {
            let length = self
                .buffer
                .load_i32_acquire(offset + layout::RECORD_LENGTH_OFFSET)
                .expect("in range");
            let type_id = self
                .buffer
                .load_i32_acquire(offset + layout::RECORD_MSG_TYPE_ID_OFFSET)
                .expect("in range");
            (length, type_id)
        }

        fn store_i64(&self, field: usize, value: i64) {
            self.transmitter
                .store_i64(&self.buffer, self.trailer(field), value)
                .expect("in range");
        }

        fn set_header(&self, offset: usize, type_id: i32, length: i32) {
            self.transmitter
                .store_i32(&self.buffer, offset + layout::RECORD_LENGTH_OFFSET, length)
                .expect("in range");
            self.transmitter
                .store_i32(
                    &self.buffer,
                    offset + layout::RECORD_MSG_TYPE_ID_OFFSET,
                    type_id,
                )
                .expect("in range");
        }

        /// Publish one record — the real transmitter, not a stand-in.
        fn publish(&mut self, type_id: i32, payload: &[u8]) {
            self.transmitter
                .transmit(&self.buffer, type_id, payload)
                .expect("the payload fits and the type id is real");
        }

        /// Make the ring look like it holds one record at `offset`, without
        /// publishing anything — for the hostile-length cases, where a real
        /// publish would refuse the value before writing it.
        fn forge_header(&self, offset: usize, type_id: i32, length: i32) {
            self.set_header(offset, type_id, length);
            self.store_i64(layout::BROADCAST_LATEST_COUNTER_OFFSET, offset as i64);
            self.store_i64(layout::BROADCAST_TAIL_COUNTER_OFFSET, offset as i64 + 64);
        }

        /// Drive the intent past a position, simulating a writer overtaking a
        /// reader.
        fn overtake(&self, past: i64) {
            self.store_i64(layout::BROADCAST_TAIL_INTENT_COUNTER_OFFSET, past);
        }

        /// Move the published tail, standing in for another writer — or for
        /// this one after a restart.
        fn set_tail(&self, past: i64) {
            self.store_i64(layout::BROADCAST_TAIL_COUNTER_OFFSET, past);
        }
    }

    #[test]
    fn a_fresh_ring_yields_nothing() {
        let mut fixture = Fixture::new();

        fixture.with(|writer, receiver| {
            assert_eq!(Received::Empty, receiver.receive(&writer.as_read_only()));
            assert_eq!(0, receiver.lapped());
        });
    }

    #[test]
    fn delivers_one_message_per_call_and_then_stops() {
        let mut fixture = Fixture::new();

        fixture.with(|writer, receiver| {
            writer.publish(0x0F07, b"first");
            writer.publish(0x0F08, b"second");

            assert_eq!(
                Received::Message { type_id: 0x0F07 },
                receiver.receive(&writer.as_read_only())
            );
            assert_eq!(b"first", receiver.message());

            assert_eq!(
                Received::Message { type_id: 0x0F08 },
                receiver.receive(&writer.as_read_only())
            );
            assert_eq!(b"second", receiver.message());

            assert_eq!(
                Received::Empty,
                receiver.receive(&writer.as_read_only()),
                "one per call, and nothing once caught up"
            );
        });
    }

    #[test]
    fn a_late_joiner_gets_the_newest_record_and_not_the_one_past_it() {
        // The cursor starts at `latest_counter`. Starting at `tail_counter` --
        // one past the newest record -- would report Empty here and silently
        // skip the only message a late joiner could have seen.
        //
        // Attaching *after* the traffic is what makes this the late-joiner
        // case, so it cannot use `with`, which builds both ends up front.
        let mut fixture = Fixture::new();

        fixture.attach_late(
            |writer| {
                writer.publish(0x0F07, b"old");
            },
            |writer, receiver| {
                assert_eq!(
                    Received::Message { type_id: 0x0F07 },
                    receiver.receive(&writer.as_read_only())
                );
                assert_eq!(b"old", receiver.message());
            },
        );
    }

    #[test]
    fn a_lap_resyncs_forwards_and_is_counted() {
        let mut fixture = Fixture::new();

        fixture.with(|writer, receiver| {
            writer.publish(0x0F07, b"first");
            assert_eq!(
                Received::Message { type_id: 0x0F07 },
                receiver.receive(&writer.as_read_only())
            );

            // The threshold at which the writer has overwritten the slot this
            // reader would read next: where the reader stands, plus one ring.
            //
            // The window is narrow and worth naming. Below it there is no lap;
            // *far* above it -- more than a ring past the newest record -- even
            // resyncing would not help, because the slot the reader lands on is
            // itself inside the announced intent, and the message comes back
            // Discarded. That is the honest answer for a reader more than a lap
            // behind, not a bug.
            let lap_at = writer.next() + CAPACITY as i64;

            writer.publish(0x0F08, b"second");
            writer.publish(0x0F09, b"third");

            // Last, because every publish rewrites the intent to the end of
            // its own record and would undo this.
            writer.overtake(lap_at);

            // Resyncing lands on the newest record, so "second" is skipped --
            // which is what a lap means and why the count is of events, not of
            // messages.
            assert_eq!(
                Received::Message { type_id: 0x0F09 },
                receiver.receive(&writer.as_read_only())
            );
            assert_eq!(b"third", receiver.message());
            assert_eq!(1, receiver.lapped(), "the lap is counted");
        });
    }

    #[test]
    fn refuses_a_length_the_writer_could_not_have_produced() {
        // The reference computes the next record's position from this signed
        // length, so a garbage one can move its cursor backwards. Refusing is
        // the divergence this test pins.
        for hostile in [-8, -1, 0, 4, i32::MAX, i32::MIN] {
            let mut fixture = Fixture::new();

            fixture.with(|writer, receiver| {
                writer.forge_header(0, 0x0F07, hostile);

                assert_eq!(
                    Received::Empty,
                    receiver.receive(&writer.as_read_only()),
                    "length {hostile} is not a record"
                );
                assert!(receiver.malformed() > 0, "and it is counted");
            });
        }
    }

    #[test]
    fn refuses_a_region_that_cannot_be_a_ring() {
        // A capacity that is not a power of two cannot be masked.
        const ODD: usize = 1000 + layout::BROADCAST_TRAILER_LENGTH;
        let mut odd = Bytes::<ODD>([0u8; ODD]);
        let buffer = AtomicBuffer::from_slice_mut(&mut odd.0).expect("aligned region");
        assert!(ToClientsReceiver::new(&buffer.as_read_only()).is_none());
        assert!(ToClientsTransmitter::new(&buffer).is_none());

        // And a region too short to hold a trailer at all.
        const TINY: usize = layout::BROADCAST_TRAILER_LENGTH;
        let mut tiny = Bytes::<TINY>([0u8; TINY]);
        let buffer = AtomicBuffer::from_slice_mut(&mut tiny.0).expect("aligned region");
        assert!(ToClientsReceiver::new(&buffer.as_read_only()).is_none());
        assert!(ToClientsTransmitter::new(&buffer).is_none());
    }

    #[test]
    fn a_publish_moves_the_three_counters_the_way_the_protocol_says() {
        let mut fixture = Fixture::new();

        fixture.with(|writer, _receiver| {
            writer.publish(0x0F08, &[0u8; 12]);
            let aligned =
                layout::align_up(12 + layout::RECORD_HEADER_LENGTH, layout::RECORD_ALIGNMENT);

            assert_eq!(0, writer.counter(layout::BROADCAST_LATEST_COUNTER_OFFSET));
            assert_eq!(
                aligned as i64,
                writer.counter(layout::BROADCAST_TAIL_COUNTER_OFFSET)
            );
            assert_eq!(
                aligned as i64,
                writer.counter(layout::BROADCAST_TAIL_INTENT_COUNTER_OFFSET),
                "the intent was raised for this record and no further"
            );

            writer.publish(0x0F08, &[0u8; 4]);
            let second =
                layout::align_up(4 + layout::RECORD_HEADER_LENGTH, layout::RECORD_ALIGNMENT);
            assert_eq!(
                aligned as i64,
                writer.counter(layout::BROADCAST_LATEST_COUNTER_OFFSET),
                "latest lags tail by exactly one record"
            );
            assert_eq!(
                (aligned + second) as i64,
                writer.counter(layout::BROADCAST_TAIL_COUNTER_OFFSET)
            );
        });
    }

    #[test]
    fn a_publish_continues_from_the_tail_it_finds() {
        // The transmitter reads `tail_counter` for every message, as the C does
        // (`aeron_broadcast_transmitter.c:71`) — it does not carry its own idea
        // of where it was. A version that cached the position would overwrite
        // whatever another writer had published in between, and the loss would
        // be invisible: the record would look well-formed and the ring's
        // counters would keep moving.
        let mut fixture = Fixture::new();

        fixture.with(|writer, _receiver| {
            writer.publish(0x0F07, b"first");

            // Someone else moved the tail on — a second transmitter, or this
            // one after a restart.
            const AHEAD: i64 = 512;
            writer.set_tail(AHEAD);

            writer.publish(0x0F08, b"second");

            assert_eq!(
                (8 + 6, 0x0F08),
                writer.record_header(AHEAD as usize),
                "the record landed where the tail said, not where this writer left off"
            );
            assert_eq!(
                AHEAD + 16,
                writer.next(),
                "and the tail moved on from there: 14 bytes of record, aligned to 16"
            );
        });
    }

    #[test]
    fn a_record_that_would_straddle_the_end_is_replaced_by_a_padding_record() {
        // Nine 100-byte payloads take 9 * 112 = 1008 of the 1024-byte ring.
        // The tenth needs 112 and only 16 are left, so the writer fills the
        // tail with a padding record and restarts at offset 0 — the one path
        // a reader cannot discover from `tail_counter` alone.
        //
        // Each message is read as it is published, as a live reader would:
        // filling the whole ring before reading the first message would make
        // that message *unreadable* — it is exactly what a lap means — and the
        // test would be measuring the resync instead of the wrap.
        let mut fixture = Fixture::new();
        let payload = [0x5Au8; 100];
        const ALIGNED: usize = 112;

        fixture.with(|writer, receiver| {
            for n in 0..9 {
                writer.publish(0x0F00 + n, &payload);
                assert_eq!(
                    Received::Message {
                        type_id: 0x0F00 + n
                    },
                    receiver.receive(&writer.as_read_only())
                );
                assert_eq!(&payload[..], receiver.message());
            }
            assert_eq!(9 * ALIGNED as i64, writer.next());

            writer.publish(0x0F09, &payload);

            const PADDING: usize = 16;
            assert_eq!(
                (PADDING as i32, layout::PADDING_MSG_TYPE_ID),
                writer.record_header(9 * ALIGNED),
                "the padding names itself -1 and measures the gap"
            );
            assert_eq!(
                (9 * ALIGNED + PADDING + ALIGNED) as i64,
                writer.next(),
                "and the message after it starts at zero"
            );
            assert_eq!(
                (9 * ALIGNED + PADDING) as i64,
                writer.counter(layout::BROADCAST_LATEST_COUNTER_OFFSET),
                "latest points at the message, past the padding"
            );

            // The reader stepped over the padding and took the record at zero.
            assert_eq!(
                Received::Message { type_id: 0x0F09 },
                receiver.receive(&writer.as_read_only())
            );
            assert_eq!(&payload[..], receiver.message());
        });
    }

    #[test]
    fn transmit_refuses_a_type_id_a_writer_may_not_send() {
        let mut fixture = Fixture::new();

        fixture.with(|writer, _receiver| {
            for hostile in [0, -1, i32::MIN] {
                assert_eq!(
                    Err(TransmitError::InvalidTypeId { type_id: hostile }),
                    writer.transmitter.transmit(&writer.buffer, hostile, b"x"),
                    "type id {hostile} is not a message"
                );
            }

            assert_eq!(0, writer.next(), "and a refused publish moved nothing");
            assert_eq!(0, writer.counter(layout::BROADCAST_TAIL_COUNTER_OFFSET));
        });
    }

    #[test]
    fn transmit_refuses_a_payload_past_the_ring_limit() {
        let mut fixture = Fixture::new();

        fixture.with(|writer, _receiver| {
            let max = writer.transmitter.max_message_length();
            assert_eq!(CAPACITY / 8, max, "capacity / 8, as the descriptor says");

            let too_long = vec![0u8; max + 1];
            assert_eq!(
                Err(TransmitError::MessageTooLong {
                    length: max + 1,
                    max,
                }),
                writer
                    .transmitter
                    .transmit(&writer.buffer, 0x0F08, &too_long)
            );

            assert_eq!(
                Ok(()),
                writer
                    .transmitter
                    .transmit(&writer.buffer, 0x0F08, &too_long[..max]),
                "the limit itself is accepted"
            );
        });
    }
}
