//! The consumer half of the to-clients broadcast ring: driver→client events.
//!
//! This is the only ring in the system that is genuinely broadcast. The
//! to-driver ring is MPSC — many clients, one driver — but every client reads
//! the *whole* to-clients stream, including events addressed to other clients.
//! A reader therefore filters by correlation id and must tolerate all thirteen
//! response types plus anything a newer driver invents.
//!
//! Mirrors `aeron-client/src/main/c/concurrent/aeron_broadcast_receiver.{c,h}`.
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

use deepmsg_core::buffer::{AtomicBuffer, ReadOnly};

use crate::layout;

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
pub struct ToClientsReceiver<'a> {
    buffer: AtomicBuffer<'a, ReadOnly>,
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

impl<'a> ToClientsReceiver<'a> {
    /// The reference's initial scratch size
    /// (`aeron-client/src/main/c/concurrent/aeron_broadcast_receiver.h:24`).
    /// It grows on demand; the cap the writer enforces is a better bound.
    const INITIAL_SCRATCH: usize = 4096;

    /// Wrap a region, or `None` if it cannot be a ring.
    pub fn new(region: AtomicBuffer<'a, ReadOnly>) -> Option<Self> {
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
            buffer: region,
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
    pub fn receive(&mut self) -> Received {
        let Some(tail) = self.load_counter(layout::BROADCAST_TAIL_COUNTER_OFFSET) else {
            return Received::Empty;
        };

        let mut cursor = self.next_record;
        if tail <= cursor {
            return Received::Empty;
        }

        let mut offset = self.index_of(cursor);

        if !self.slot_is_live(cursor) {
            // Overtaken. Resync to the newest record — forwards only, never
            // backwards, which is why a cursor ahead of a restarted driver's
            // counters has no recovery path.
            self.lapped += 1;
            let Some(latest) = self.load_counter(layout::BROADCAST_LATEST_COUNTER_OFFSET) else {
                return Received::Empty;
            };
            cursor = latest;
            offset = self.index_of(cursor);
        }

        let Some(length) = self.record_length(offset) else {
            self.malformed += 1;
            // Nowhere safe to go: this header is not a length the writer could
            // have produced, so where the next record starts is unknowable.
            return Received::Empty;
        };
        let Some(type_id) = self.load_i32(offset + layout::RECORD_MSG_TYPE_ID_OFFSET) else {
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
            let Some(first_length) = self.record_length(0) else {
                self.malformed += 1;
                return Received::Empty;
            };
            let Some(first_type) = self.load_i32(layout::RECORD_MSG_TYPE_ID_OFFSET) else {
                self.malformed += 1;
                return Received::Empty;
            };

            self.cursor = self.next_record;
            self.next_record += layout::align_up(first_length, layout::RECORD_ALIGNMENT) as i64;
            self.record_offset = 0;
            return self.finish(0, first_type);
        }

        self.record_offset = offset;
        self.finish(offset, type_id)
    }

    /// Copy the record at `offset` into the scratch buffer, then validate.
    fn finish(&mut self, offset: usize, type_id: i32) -> Received {
        let Some(length) = self.record_length(offset) else {
            self.malformed += 1;
            return Received::Empty;
        };
        let payload = length - layout::RECORD_HEADER_LENGTH;

        if self.scratch.len() < payload {
            self.scratch.resize(payload, 0);
        }

        if self
            .buffer
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
        if !self.slot_is_live(self.cursor) {
            self.discarded += 1;
            return Received::Discarded;
        }

        Received::Message { type_id }
    }

    /// A record's total length in bytes, if it is one the writer could have
    /// produced.
    fn record_length(&self, offset: usize) -> Option<usize> {
        let length = self.load_i32(offset + layout::RECORD_LENGTH_OFFSET)?;

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
    fn slot_is_live(&self, cursor: i64) -> bool {
        self.load_counter(layout::BROADCAST_TAIL_INTENT_COUNTER_OFFSET)
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

    fn load_counter(&self, field: usize) -> Option<i64> {
        self.buffer.load_i64_acquire(self.trailer + field)
    }

    fn load_i32(&self, offset: usize) -> Option<i32> {
        self.buffer.load_i32_acquire(offset)
    }
}

impl std::fmt::Debug for ToClientsReceiver<'_> {
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
    use deepmsg_core::buffer::ReadWrite;

    #[repr(align(64))]
    struct Bytes<const N: usize>([u8; N]);

    const CAPACITY: usize = 1024;
    const REGION: usize = CAPACITY + layout::BROADCAST_TRAILER_LENGTH;
    const MASK: usize = CAPACITY - 1;

    /// A ring with a writer standing in for a driver.
    ///
    /// In production the writer is another process, so no one process holds
    /// both ends. Here it must — which is what `as_read_only` exists for.
    struct Fixture {
        bytes: Bytes<REGION>,
    }

    /// The writer half, mirroring `aeron_broadcast_transmitter.c`: announce the
    /// intent, write the record, then move the counters.
    struct Writer<'a> {
        buffer: AtomicBuffer<'a, ReadWrite>,
        /// Where the next record starts, in the ring's counter space.
        next: i64,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                bytes: Bytes([0u8; REGION]),
            }
        }

        /// Run `f` with a writer and a receiver over one ring.
        fn with<T>(
            &mut self,
            f: impl FnOnce(&mut Writer<'_>, &mut ToClientsReceiver<'_>) -> T,
        ) -> T {
            let mut writer = Writer {
                buffer: AtomicBuffer::from_slice_mut(&mut self.bytes.0).expect("aligned region"),
                next: 0,
            };
            let mut receiver =
                ToClientsReceiver::new(writer.buffer.as_read_only()).expect("a valid ring");

            f(&mut writer, &mut receiver)
        }

        /// Publish first, *then* attach — the late-joiner case, which is about
        /// what a receiver constructed after traffic sees.
        fn attach_late<T>(
            &mut self,
            publish: impl FnOnce(&mut Writer<'_>),
            f: impl FnOnce(&mut ToClientsReceiver<'_>) -> T,
        ) -> T {
            let mut writer = Writer {
                buffer: AtomicBuffer::from_slice_mut(&mut self.bytes.0).expect("aligned region"),
                next: 0,
            };
            publish(&mut writer);

            let mut receiver =
                ToClientsReceiver::new(writer.buffer.as_read_only()).expect("a valid ring");
            f(&mut receiver)
        }
    }

    impl Writer<'_> {
        fn trailer(&self, field: usize) -> usize {
            CAPACITY + field
        }

        fn store_i64(&self, field: usize, value: i64) {
            self.buffer
                .store_i64_release(self.trailer(field), value)
                .expect("in range");
        }

        fn set_header(&self, offset: usize, type_id: i32, length: i32) {
            self.buffer
                .store_i32_relaxed(offset + layout::RECORD_LENGTH_OFFSET, length)
                .expect("in range");
            self.buffer
                .store_i32_relaxed(offset + layout::RECORD_MSG_TYPE_ID_OFFSET, type_id)
                .expect("in range");
        }

        /// Publish one record, wrapping through a padding record if needed.
        fn publish(&mut self, type_id: i32, payload: &[u8]) -> usize {
            let length = layout::RECORD_HEADER_LENGTH + payload.len();
            let aligned = layout::align_up(length, layout::RECORD_ALIGNMENT);
            let mut offset = (self.next as u32 as usize) & MASK;

            if CAPACITY - offset < aligned {
                let padding = CAPACITY - offset;
                self.store_i64(
                    layout::BROADCAST_TAIL_INTENT_COUNTER_OFFSET,
                    self.next + padding as i64,
                );
                self.set_header(offset, layout::PADDING_MSG_TYPE_ID, padding as i32);
                self.store_i64(
                    layout::BROADCAST_TAIL_COUNTER_OFFSET,
                    self.next + padding as i64,
                );
                self.next += padding as i64;
                offset = 0;
            }

            self.store_i64(
                layout::BROADCAST_TAIL_INTENT_COUNTER_OFFSET,
                self.next + aligned as i64,
            );
            self.set_header(offset, type_id, length as i32);
            self.buffer
                .copy_in(offset + layout::RECORD_HEADER_LENGTH, payload)
                .expect("in range");
            self.store_i64(layout::BROADCAST_LATEST_COUNTER_OFFSET, self.next);
            self.store_i64(
                layout::BROADCAST_TAIL_COUNTER_OFFSET,
                self.next + aligned as i64,
            );
            self.next += aligned as i64;

            offset
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
    }

    #[test]
    fn a_fresh_ring_yields_nothing() {
        let mut fixture = Fixture::new();

        fixture.with(|_writer, receiver| {
            assert_eq!(Received::Empty, receiver.receive());
            assert_eq!(0, receiver.lapped());
        });
    }

    #[test]
    fn delivers_one_message_per_call_and_then_stops() {
        let mut fixture = Fixture::new();

        fixture.with(|writer, receiver| {
            writer.publish(0x0F07, b"first");
            writer.publish(0x0F08, b"second");

            assert_eq!(Received::Message { type_id: 0x0F07 }, receiver.receive());
            assert_eq!(b"first", receiver.message());

            assert_eq!(Received::Message { type_id: 0x0F08 }, receiver.receive());
            assert_eq!(b"second", receiver.message());

            assert_eq!(
                Received::Empty,
                receiver.receive(),
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
            |receiver| {
                assert_eq!(Received::Message { type_id: 0x0F07 }, receiver.receive());
                assert_eq!(b"old", receiver.message());
            },
        );
    }

    #[test]
    fn a_lap_resyncs_forwards_and_is_counted() {
        let mut fixture = Fixture::new();

        fixture.with(|writer, receiver| {
            writer.publish(0x0F07, b"first");
            assert_eq!(Received::Message { type_id: 0x0F07 }, receiver.receive());

            // The threshold at which the writer has overwritten the slot this
            // reader would read next: where the reader stands, plus one ring.
            //
            // The window is narrow and worth naming. Below it there is no lap;
            // *far* above it -- more than a ring past the newest record -- even
            // resyncing would not help, because the slot the reader lands on is
            // itself inside the announced intent, and the message comes back
            // Discarded. That is the honest answer for a reader more than a lap
            // behind, not a bug.
            let lap_at = writer.next + CAPACITY as i64;

            writer.publish(0x0F08, b"second");
            writer.publish(0x0F09, b"third");

            // Last, because every publish rewrites the intent to the end of
            // its own record and would undo this.
            writer.overtake(lap_at);

            // Resyncing lands on the newest record, so "second" is skipped --
            // which is what a lap means and why the count is of events, not of
            // messages.
            assert_eq!(Received::Message { type_id: 0x0F09 }, receiver.receive());
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
                    receiver.receive(),
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
        assert!(ToClientsReceiver::new(buffer.as_read_only()).is_none());

        // And a region too short to hold a trailer at all.
        const TINY: usize = layout::BROADCAST_TRAILER_LENGTH;
        let mut tiny = Bytes::<TINY>([0u8; TINY]);
        let buffer = AtomicBuffer::from_slice_mut(&mut tiny.0).expect("aligned region");
        assert!(ToClientsReceiver::new(buffer.as_read_only()).is_none());
    }
}
