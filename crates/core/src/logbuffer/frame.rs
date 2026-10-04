//! The frames a term buffer carries.
//!
//! A frame is a header and a payload. The header comes in two lengths: the
//! *frame header* is 8 bytes and is what every protocol message starts with;
//! the *data header* is 32 and adds the fields a stream needs to place the
//! frame — term offset, session, stream, term id, reserved value. The data
//! header embeds the frame header as its first member, so a pointer to one is
//! a pointer to the other.
//!
//! Mirrors `aeron-client/src/main/c/protocol/aeron_udp_protocol.h:27-65`.
//!
//! # The length field is the whole protocol
//!
//! `frame_length` is a signed 32-bit field whose **sign is the publication
//! state**:
//!
//! - `0` — nothing here. Either never written, or cleaned after consumption.
//! - negative — a producer has claimed this frame and is filling it.
//! - positive — complete, and its value is the frame's total length *including*
//!   the 32-byte header.
//!
//! A reader stops at anything `<= 0`, which is why it never has to compare
//! indices, and why a producer must negate the length *before* it writes
//! anything else.

use crate::buffer::{AtomicBuffer, ReadOnly, ReadWrite};

/// The frame header's length.
pub const FRAME_HEADER_LENGTH: usize = 8;

/// The data header's length — the frame header plus the stream fields.
pub const DATA_HEADER_LENGTH: usize = 32;

/// `frame_length`, `i32`, volatile.
pub const FRAME_LENGTH_OFFSET: usize = 0;
/// `version`, `i8`.
pub const VERSION_OFFSET: usize = 4;
/// `flags`, `u8`.
pub const FLAGS_OFFSET: usize = 5;
/// `type`, `i16`.
pub const TYPE_OFFSET: usize = 6;
/// `term_offset`, `i32` — where in its term the frame starts.
pub const TERM_OFFSET_FIELD_OFFSET: usize = 8;
/// `session_id`, `i32`.
pub const SESSION_ID_FIELD_OFFSET: usize = 12;
/// `stream_id`, `i32`.
pub const STREAM_ID_FIELD_OFFSET: usize = 16;
/// `term_id`, `i32` — the **absolute** term id, not a count.
pub const TERM_ID_FIELD_OFFSET: usize = 20;
/// `reserved_value`, `i64`.
pub const RESERVED_VALUE_OFFSET: usize = 24;

/// The version a frame header carries. Zero, and the only value in use.
pub const VERSION: i8 = 0;

/// A padding frame: written to consume the tail of a term, or to fill a gap on
/// the receive side.
pub const TYPE_PAD: i16 = 0x00;
/// A data frame.
pub const TYPE_DATA: i16 = 0x01;
/// Data with an ATS timestamp.
///
/// A **producer** never writes this, but a receive-side term buffer contains
/// whatever arrived on the wire, so a reader must not assume `TYPE_DATA`.
pub const TYPE_ATS_DATA: i16 = 0x08;

/// The first fragment of a message.
pub const FLAG_BEGIN: u8 = 0x80;
/// The last fragment.
pub const FLAG_END: u8 = 0x40;
/// End of stream.
pub const FLAG_EOS: u8 = 0x20;
/// The publication was revoked.
pub const FLAG_REVOKED: u8 = 0x10;
/// Both fragment bits: a whole message in one frame.
pub const FLAG_UNFRAGMENTED: u8 = FLAG_BEGIN | FLAG_END;

/// One frame in a term buffer.
///
/// Holds a window onto the term and an offset into it; every accessor is
/// bounds-checked against the window, so a frame that runs past the end of the
/// term reads as `None` rather than as a neighbouring allocation.
pub struct Frame<'a, Access = ReadOnly> {
    buffer: &'a AtomicBuffer<'a, Access>,
    offset: usize,
}

impl<'a, Access> Frame<'a, Access> {
    /// A frame at `offset` within the term.
    pub const fn new(buffer: &'a AtomicBuffer<'a, Access>, offset: usize) -> Self {
        Self { buffer, offset }
    }

    /// Where this frame starts.
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// The frame's total length, header included.
    ///
    /// **Acquire**, because it is the field that publishes everything else in
    /// the frame: a reader that observes a positive length here has been
    /// promised that the payload and the type are complete.
    ///
    /// A caller must treat `<= 0` as "stop", never as a length.
    pub fn frame_length(&self) -> Option<i32> {
        self.buffer
            .load_i32_acquire(self.offset + FRAME_LENGTH_OFFSET)
    }

    /// The frame header's version.
    pub fn version(&self) -> Option<i8> {
        self.buffer
            .load_u8(self.offset + VERSION_OFFSET)
            .map(|v| v as i8)
    }

    /// The flags byte.
    pub fn flags(&self) -> Option<u8> {
        self.buffer.load_u8(self.offset + FLAGS_OFFSET)
    }

    /// The frame type.
    pub fn type_id(&self) -> Option<i16> {
        let offset = self.offset + TYPE_OFFSET;
        let low = self.buffer.load_u8(offset)? as i16;
        let high = self.buffer.load_u8(offset + 1)? as i16;
        Some((high << 8) | (low & 0xFF))
    }

    /// Whether this frame is padding.
    pub fn is_padding(&self) -> bool {
        Some(TYPE_PAD) == self.type_id()
    }

    /// Whether this frame carries a whole message.
    pub fn is_unfragmented(&self) -> bool {
        self.flags()
            .is_some_and(|flags| flags & FLAG_UNFRAGMENTED == FLAG_UNFRAGMENTED)
    }

    /// The term offset recorded in the header.
    ///
    /// Redundant with the frame's location — a writer sets it to match — and
    /// useful as a consistency check.
    pub fn term_offset(&self) -> Option<i32> {
        self.buffer.load_i32(self.offset + TERM_OFFSET_FIELD_OFFSET)
    }

    /// The session id.
    pub fn session_id(&self) -> Option<i32> {
        self.buffer.load_i32(self.offset + SESSION_ID_FIELD_OFFSET)
    }

    /// The stream id.
    pub fn stream_id(&self) -> Option<i32> {
        self.buffer.load_i32(self.offset + STREAM_ID_FIELD_OFFSET)
    }

    /// The **absolute** term id.
    pub fn term_id(&self) -> Option<i32> {
        self.buffer.load_i32(self.offset + TERM_ID_FIELD_OFFSET)
    }

    /// The reserved value, written by whichever reserved-value supplier the
    /// publisher was given.
    pub fn reserved_value(&self) -> Option<i64> {
        self.buffer.load_i64(self.offset + RESERVED_VALUE_OFFSET)
    }

    /// How many payload bytes a complete frame holds.
    ///
    /// `None` for a frame that is not complete, because a negative length
    /// subtracted from the header is not a payload.
    pub fn payload_length(&self) -> Option<usize> {
        let length = self.frame_length()?;
        if length < DATA_HEADER_LENGTH as i32 {
            return None;
        }

        Some(length as usize - DATA_HEADER_LENGTH)
    }

    /// How many bytes this frame occupies in the term, once complete.
    ///
    /// The header stores the unaligned length; a reader steps by the aligned
    /// one. `None` if the frame is not complete.
    pub fn aligned_length(&self) -> Option<usize> {
        let length = self.frame_length()?;
        if length <= 0 {
            return None;
        }

        Some(super::position::align_up(length, super::descriptor::FRAME_ALIGNMENT) as usize)
    }

    /// Copy the payload out, or `None` if the frame is not complete or the
    /// destination does not match its payload length.
    pub fn copy_payload(&self, dst: &mut [u8]) -> Option<()> {
        if dst.len() != self.payload_length()? {
            return None;
        }

        self.buffer.copy_out(self.offset + DATA_HEADER_LENGTH, dst)
    }
}

/// The write side, available only on a writable term.
impl Frame<'_, ReadWrite> {
    /// Start a frame: write the header with a **negative** length so a
    /// concurrent reader stops here, and fence.
    ///
    /// This is the first half of the publish sequence and it must happen before
    /// any other field is touched. The fence is not optional: without it a
    /// weakly-ordered CPU can let a reader observe the new `term_offset` and
    /// `term_id` alongside a **stale positive length** left from the previous
    /// revolution of this partition, and deliver whatever those bytes happen to
    /// be as a valid frame.
    ///
    /// The reference does exactly this — `AERON_SET_RELEASE(…, -length)`
    /// followed by `aeron_release()` (`aeron_publication.c:132-133`), and Java's
    /// `putLongRelease` followed by `VarHandle.storeStoreFence()`
    /// (`HeaderWriter.java:84-85`).
    ///
    /// The remaining header fields are written **plainly**, because the final
    /// positive store of the length is what publishes them.
    #[allow(clippy::too_many_arguments)]
    pub fn begin(
        &self,
        frame_length: i32,
        flags: u8,
        type_id: i16,
        term_offset: i32,
        session_id: i32,
        stream_id: i32,
        term_id: i32,
    ) -> Option<()> {
        self.write_i32_release(FRAME_LENGTH_OFFSET, -frame_length)?;
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);

        self.write_u8(VERSION_OFFSET, VERSION as u8)?;
        self.write_u8(FLAGS_OFFSET, flags)?;
        self.write_i16(TYPE_OFFSET, type_id)?;
        self.write_i32(TERM_OFFSET_FIELD_OFFSET, term_offset)?;
        self.write_i32(SESSION_ID_FIELD_OFFSET, session_id)?;
        self.write_i32(STREAM_ID_FIELD_OFFSET, stream_id)?;
        self.write_i32(TERM_ID_FIELD_OFFSET, term_id)?;

        Some(())
    }

    /// Change the type without disturbing anything else.
    ///
    /// How a padding frame is made: the header is written as a data frame, then
    /// only the type is replaced. The flags therefore stay `BEGIN|END`
    /// (`aeron_publication.c:136` then `:158`).
    pub fn set_type(&self, type_id: i16) -> Option<()> {
        self.write_i16(TYPE_OFFSET, type_id)
    }

    /// Write the reserved value. Must happen before [`Frame::publish`], or it
    /// lands outside the published frame.
    pub fn set_reserved_value(&self, value: i64) -> Option<()> {
        self.write_i64(RESERVED_VALUE_OFFSET, value)
    }

    /// Copy payload bytes in.
    pub fn write_payload(&self, payload: &[u8]) -> Option<()> {
        self.buffer
            .copy_in(self.offset + DATA_HEADER_LENGTH, payload)
    }

    /// Store an `int64` at `offset` bytes into the payload.
    ///
    /// The other half of [`Frame::write_payload`], and the one a **claimed**
    /// frame wants: a producer that has claimed writes its message where the
    /// message will lie instead of assembling it elsewhere and copying it in.
    /// That is what a claim is for — the reference's `BufferClaim` hands over
    /// the log buffer and an offset into it for exactly this
    /// (`aeron_exclusive_publication.h:36-41`), and its senders write a
    /// timestamp, a receiver index and a checksum straight through those and
    /// leave the rest of the message as whatever the term already held.
    ///
    /// Offsets are from the start of the payload, which is where
    /// `BufferClaim.offset()` points.
    ///
    /// Nothing reads a claimed frame before [`Frame::publish`] — its length is
    /// negative until then — so an offset that is only four-byte aligned is
    /// written as two halves rather than refused, the way the reference's
    /// little-endian `putLong` writes wherever it is told to.
    pub fn store_i64_in_payload(&self, offset: usize, value: i64) -> Option<()> {
        let at = self.payload_offset(offset)?;

        if self.buffer.store_i64_relaxed(at, value).is_some() {
            return Some(());
        }

        self.buffer.store_i64_relaxed_unaligned(at, value)
    }

    /// Store an `int32` at `offset` bytes into the payload.
    ///
    /// See [`Frame::store_i64_in_payload`].
    pub fn store_i32_in_payload(&self, offset: usize, value: i32) -> Option<()> {
        self.buffer
            .store_i32_relaxed(self.payload_offset(offset)?, value)
    }

    /// Where `offset` bytes into the payload lies in the term.
    fn payload_offset(&self, offset: usize) -> Option<usize> {
        self.offset
            .checked_add(DATA_HEADER_LENGTH)?
            .checked_add(offset)
    }

    /// Publish the frame: a positive length, release.
    ///
    /// This single 32-bit store is what makes the frame visible, and it is what
    /// the reader's acquire load of the same field pairs with. Everything
    /// written before it — the flags, the type, the payload, the reserved value
    /// — is published by it.
    ///
    /// The value is the **unaligned** frame length, matching what the header
    /// will report to a reader.
    pub fn publish(&self, frame_length: i32) -> Option<()> {
        self.write_i32_release(FRAME_LENGTH_OFFSET, frame_length)
    }

    fn write_u8(&self, offset: usize, value: u8) -> Option<()> {
        // The window's accessors are typed by width; a byte is written through
        // the 32-bit accessor's window check and a narrower store would need a
        // new accessor. The reference's fields are all naturally aligned, so
        // widening to the containing 4-byte slot is not possible here — instead
        // the buffer exposes per-byte access for exactly this case.
        self.buffer.store_u8_relaxed(self.offset + offset, value)
    }

    fn write_i16(&self, offset: usize, value: i16) -> Option<()> {
        self.buffer.store_i16_relaxed(self.offset + offset, value)
    }

    fn write_i32(&self, offset: usize, value: i32) -> Option<()> {
        self.buffer.store_i32_relaxed(self.offset + offset, value)
    }

    fn write_i32_release(&self, offset: usize, value: i32) -> Option<()> {
        self.buffer.store_i32_release(self.offset + offset, value)
    }

    fn write_i64(&self, offset: usize, value: i64) -> Option<()> {
        self.buffer.store_i64_relaxed(self.offset + offset, value)
    }
}

/// The remaining setters a repair needs, which a producer never does.
///
/// A producer writes the placement fields once, as part of [`Frame::begin`].
/// A repair is overwriting a frame someone else left, so it corrects them
/// afterwards instead.
impl Frame<'_, ReadWrite> {
    /// Rewrite the term offset recorded in the header.
    pub fn set_term_offset(&self, term_offset: i32) -> Option<()> {
        self.write_i32(TERM_OFFSET_FIELD_OFFSET, term_offset)
    }

    /// Rewrite the term id recorded in the header.
    pub fn set_term_id(&self, term_id: i32) -> Option<()> {
        self.write_i32(TERM_ID_FIELD_OFFSET, term_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::AtomicBuffer;

    const MESSAGE_LENGTH: usize = 24;

    /// A term with one claimed frame at its start, and the frame's header
    /// written the way `Appender` writes one.
    ///
    /// The buffer cannot be made by a helper and returned: a `Frame` borrows the
    /// `AtomicBuffer` it reads, and the buffer borrows the term. So each test
    /// holds all three.
    macro_rules! claimed {
        ($term:ident, $buffer:ident, $frame:ident, $length:expr) => {
            let mut $term = vec![0_u8; 256];
            let $buffer =
                AtomicBuffer::from_slice_mut(&mut $term).expect("the term is a valid region");
            let $frame = Frame::new(&$buffer, 0);
            let frame_length = i32::try_from($length + DATA_HEADER_LENGTH).expect("small");

            $frame
                .begin(frame_length, FLAG_UNFRAGMENTED, TYPE_DATA, 0, 1, 2, 3)
                .expect("the header is inside the term");
        };
    }

    fn payload_of(frame: &Frame<'_, ReadWrite>, length: usize) -> Vec<u8> {
        let mut payload = vec![0_u8; length];
        assert_eq!(frame.copy_payload(&mut payload), Some(()));

        payload
    }

    #[test]
    fn a_claimed_frame_can_be_written_into_where_it_lies() {
        claimed!(term, buffer, frame, MESSAGE_LENGTH);
        let frame_length = i32::try_from(MESSAGE_LENGTH + DATA_HEADER_LENGTH).expect("small");

        assert_eq!(
            frame.store_i64_in_payload(0, 0x0102_0304_0506_0708),
            Some(())
        );
        assert_eq!(frame.store_i32_in_payload(8, 7), Some(()));
        assert_eq!(
            frame.store_i64_in_payload(MESSAGE_LENGTH - 8, i64::MIN),
            Some(())
        );
        assert_eq!(frame.publish(frame_length), Some(()));

        let mut expected = vec![0_u8; MESSAGE_LENGTH];
        expected[0..8].copy_from_slice(&0x0102_0304_0506_0708_i64.to_le_bytes());
        expected[8..12].copy_from_slice(&7_i32.to_le_bytes());
        expected[MESSAGE_LENGTH - 8..].copy_from_slice(&i64::MIN.to_le_bytes());

        assert_eq!(payload_of(&frame, MESSAGE_LENGTH), expected);
    }

    /// The checksum sits `message_length - 8` bytes in, so a message length that
    /// is not a multiple of eight leaves it only four-byte aligned — where the
    /// reference writes anyway, and where this writes as two halves.
    #[test]
    fn a_checksum_off_an_eight_byte_boundary_is_still_written() {
        const ODD_LENGTH: usize = 28;

        claimed!(term, buffer, frame, ODD_LENGTH);
        let frame_length = i32::try_from(ODD_LENGTH + DATA_HEADER_LENGTH).expect("small");

        assert_eq!(
            frame.store_i64_in_payload(ODD_LENGTH - 8, 0x0A0B_0C0D_0E0F_1011),
            Some(())
        );
        assert_eq!(frame.publish(frame_length), Some(()));

        let payload = payload_of(&frame, ODD_LENGTH);

        assert_eq!(
            payload[ODD_LENGTH - 8..],
            0x0A0B_0C0D_0E0F_1011_i64.to_le_bytes()
        );
    }

    /// A write that would leave the term is refused rather than wrapping into
    /// the header before it or past the mapping's end.
    #[test]
    fn a_write_past_the_end_of_the_term_is_refused() {
        claimed!(term, buffer, frame, 16);
        let past_the_end = 256 - DATA_HEADER_LENGTH;

        assert_eq!(frame.store_i64_in_payload(past_the_end, 1), None);
        assert_eq!(frame.store_i32_in_payload(past_the_end, 1), None);
        assert_eq!(frame.store_i64_in_payload(usize::MAX, 1), None);
    }
}
