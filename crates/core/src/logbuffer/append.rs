//! Claiming space in a term, writing a frame, and rotating.
//!
//! Mirrors `aeron-client/src/main/c/aeron_publication.c:109-249` — the appender
//! the reference inlines into its publications rather than naming. The
//! reference's `aeron_term_appender.{c,h}` do not exist at this commit.
//!
//! # The whole protocol in one paragraph
//!
//! A producer claims space by advancing one of the three **per-term tail
//! counters** with a fetch-and-add, not a compare-and-exchange: two producers
//! each get a disjoint span and neither retries. The claim carries a
//! *header-length* cost the caller did not ask for, because a frame is 32
//! bytes longer than its payload. It writes a **negative** length first so a
//! reader stops at the frame rather than reading a stale one, then the rest of
//! the header, then the payload, and finally publishes by storing the positive
//! length. Where it lands is decided by the tail it got, not by the caller, so
//! the caller must check afterwards whether it fits in the term — if it does
//! not, the span is abandoned as a padding frame and the log rotates.
//!
//! # What is deliberately not here
//!
//! The flow-control **policy**. A producer reads a limit from a counter the
//! driver maintains and appends only below it; how that limit is computed is
//! the driver's business (P1), and the counter is not part of the log buffer.
//! The limit is a parameter, so this module stays drivable from a test.

use crate::buffer::{AtomicBuffer, ReadWrite};

use super::descriptor;
use super::frame::{
    DATA_HEADER_LENGTH, FLAG_UNFRAGMENTED, Frame, SESSION_ID_FIELD_OFFSET, STREAM_ID_FIELD_OFFSET,
    TYPE_DATA, TYPE_PAD,
};
use super::position::{self, Position, RawTail};

/// What an attempt to append produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Appended {
    /// A frame is published, ending at `position`.
    Ok {
        /// The position just past the frame — where the next one starts.
        position: Position,
        /// Where the frame began within its term.
        term_offset: i32,
    },
    /// The frame did not fit the remainder of the term. A padding frame now
    /// covers that remainder and the log has rotated; the caller should try
    /// again, and it will land in the new term.
    EndOfLog,
    /// The position reached this stream's maximum, and the term id cannot grow
    /// further.
    MaxPositionExceeded,
    /// The producer's window is used up. The caller waits for the driver to
    /// raise the limit.
    BackPressured,
    /// The metadata does not describe a usable log.
    Malformed,
}

/// A producer over one log buffer.
///
/// Holds the metadata block and one term — the term the producer believes is
/// current. The reference's concurrent publication does not cache that belief
/// and re-reads it per offer; keeping it here would mean re-deriving it after
/// every rotation, so this does the same.
pub struct Appender<'a> {
    metadata: AtomicBuffer<'a, ReadWrite>,
    term: AtomicBuffer<'a, ReadWrite>,
    term_length: i32,
    initial_term_id: i32,
    bits_to_shift: u32,
}

impl<'a> Appender<'a> {
    /// Wrap a metadata block and the term buffer it describes.
    ///
    /// Returns `None` if the term length is not one of the reference's legal
    /// values, or if the term buffer is not as long as the metadata says — a
    /// reader that trusted a shorter term buffer would write past it.
    pub fn new(
        metadata: AtomicBuffer<'a, ReadWrite>,
        term: AtomicBuffer<'a, ReadWrite>,
    ) -> Option<Self> {
        let term_length = metadata.load_i32(descriptor::TERM_LENGTH_OFFSET)?;
        let bits_to_shift = position::bits_to_shift(term_length)?;

        if term.len() < term_length as usize {
            return None;
        }

        let initial_term_id = metadata.load_i32(descriptor::INITIAL_TERM_ID_OFFSET)?;

        Some(Self {
            metadata,
            term,
            term_length,
            initial_term_id,
            bits_to_shift,
        })
    }

    /// The term length every offset here is relative to.
    pub const fn term_length(&self) -> i32 {
        self.term_length
    }

    /// The term id this log started at.
    pub const fn initial_term_id(&self) -> i32 {
        self.initial_term_id
    }

    /// How many terms have been written since the log began.
    pub fn active_term_count(&self) -> Option<i32> {
        self.metadata.load_i32(descriptor::ACTIVE_TERM_COUNT_OFFSET)
    }

    /// The tail of the term the producer is currently in.
    ///
    /// Reads `active_term_count` **first**, then uses it to pick the counter —
    /// the order matters, because the count is what tells you which counter is
    /// current (`aeron_logbuffer_descriptor.h:94-102`).
    pub fn current_tail(&self) -> Option<RawTail> {
        let term_count = self.active_term_count()?;
        let index = position::index_by_term_count(term_count);
        self.tail_at(index)
    }

    /// The tail of one partition.
    pub fn tail_at(&self, partition: usize) -> Option<RawTail> {
        if partition >= descriptor::PARTITION_COUNT {
            return None;
        }

        let offset = descriptor::TERM_TAIL_COUNTERS_OFFSET
            + partition * descriptor::TERM_TAIL_COUNTER_STRIDE;
        self.metadata
            .load_i64_acquire(offset)
            .map(RawTail::from_raw)
    }

    /// Append a payload as one frame.
    ///
    /// `position_limit` is where the driver says this producer may write up to;
    /// it is read from a counter the driver owns and never written here.
    ///
    /// # Errors
    ///
    /// See [`Appended`]. `EndOfLog` is not a failure — the caller retries and
    /// lands in the rotated term.
    pub fn append(
        &self,
        session_id: i32,
        stream_id: i32,
        position_limit: i64,
        payload: &[u8],
    ) -> Appended {
        let Some(frame_length) = i32::try_from(payload.len() + DATA_HEADER_LENGTH).ok() else {
            return Appended::Malformed;
        };
        let aligned_length = position::align_up(frame_length, descriptor::FRAME_ALIGNMENT);

        let Some(tail) = self.current_tail() else {
            return Appended::Malformed;
        };
        let term_id = tail.term_id();
        let term_offset = tail.term_offset(self.term_length);

        // The frame this append will produce starts where the tail points, and
        // the tail is what it will be advanced to.
        let position = Position::new(
            term_id,
            term_offset,
            self.bits_to_shift,
            self.initial_term_id,
        );
        let end_position = Position::from_raw(position.raw() + aligned_length as i64);

        if end_position.raw() >= position::max_possible_position(self.term_length) {
            return Appended::MaxPositionExceeded;
        }
        if position.raw() >= position_limit {
            return Appended::BackPressured;
        }

        // Claim. The offset this returns is where *this* producer writes; the
        // alignment applies to the tail so the next one starts on a boundary.
        let Some(claimed) = self.claim(aligned_length as i64) else {
            return Appended::Malformed;
        };
        let claimed_offset = claimed.term_offset(self.term_length);

        // The span may run past the term even though the frame started inside
        // it. That is the normal way a term ends.
        if claimed_offset + aligned_length > self.term_length {
            return self.handle_end_of_log(term_offset, term_id, position);
        }

        let Some(end_position) = self.write_frame(
            session_id,
            stream_id,
            claimed_offset,
            term_id,
            frame_length,
            payload,
        ) else {
            return Appended::Malformed;
        };

        Appended::Ok {
            position: end_position,
            term_offset: claimed_offset,
        }
    }

    /// Write the frame and publish it.
    #[allow(clippy::too_many_arguments)]
    fn write_frame(
        &self,
        session_id: i32,
        stream_id: i32,
        term_offset: i32,
        term_id: i32,
        frame_length: i32,
        payload: &[u8],
    ) -> Option<Position> {
        let frame = Frame::new(&self.term, term_offset as usize);

        frame.begin(
            frame_length,
            FLAG_UNFRAGMENTED,
            TYPE_DATA,
            term_offset,
            session_id,
            stream_id,
            term_id,
        )?;
        frame.write_payload(payload)?;
        frame.publish(frame_length)?;

        Some(Position::new(
            term_id,
            term_offset + position::align_up(frame_length, descriptor::FRAME_ALIGNMENT),
            self.bits_to_shift,
            self.initial_term_id,
        ))
    }

    /// Claim `length` bytes of the current term's tail.
    ///
    /// A fetch-and-add, not a compare-and-exchange: every producer gets a
    /// distinct span and nobody retries. The reference does the same
    /// (`aeron_publication.c:109-114`).
    fn claim(&self, length: i64) -> Option<RawTail> {
        let term_count = self.active_term_count()?;
        let index = position::index_by_term_count(term_count);
        let offset =
            descriptor::TERM_TAIL_COUNTERS_OFFSET + index * descriptor::TERM_TAIL_COUNTER_STRIDE;

        self.metadata
            .fetch_add_i64(offset, length)
            .map(RawTail::from_raw)
    }

    /// The span ran past the end of the term.
    ///
    /// Cover the remainder with a padding frame — unless the previous frame
    /// ended *exactly* on the boundary, in which case there is nothing to cover
    /// and a padding frame must not be invented: a reader stepping by the
    /// padding's length from a zero-length position would be misled.
    ///
    /// Then rotate. Rotation happens **inline**, on the offering thread, and
    /// the caller still sees `EndOfLog` as its result — the reference returns
    /// `ADMIN_ACTION` for exactly the same reason.
    fn handle_end_of_log(&self, term_offset: i32, term_id: i32, position: Position) -> Appended {
        if term_offset < self.term_length {
            let padding_length = self.term_length - term_offset;
            let frame = Frame::new(&self.term, term_offset as usize);

            // The header is written as a data frame and only its type is
            // replaced, so a padding frame keeps BEGIN|END flags. The reference
            // does the same (`aeron_publication.c:136` then `:158`).
            if frame
                .begin(
                    padding_length,
                    FLAG_UNFRAGMENTED,
                    TYPE_DATA,
                    term_offset,
                    self.default_header_i32(SESSION_ID_FIELD_OFFSET),
                    self.default_header_i32(STREAM_ID_FIELD_OFFSET),
                    term_id,
                )
                .is_none()
                || frame.set_type(TYPE_PAD).is_none()
                || frame.publish(padding_length).is_none()
            {
                return Appended::Malformed;
            }
        }

        if position.raw() >= position::max_possible_position(self.term_length) {
            return Appended::MaxPositionExceeded;
        }

        let Some(term_count) = self.active_term_count() else {
            return Appended::Malformed;
        };

        if self.rotate(term_count, term_id) {
            Appended::EndOfLog
        } else {
            Appended::Malformed
        }
    }

    /// Rotate the log to the next term.
    ///
    /// Two steps, in this order and for a reason:
    ///
    /// 1. Reset the **next** partition's tail to the new term id at offset
    ///    zero — but only if it still holds the term id from three terms ago.
    ///    If it does not, another producer has already moved past us and this
    ///    rotation is not ours to perform. `next_term_id - PARTITION_COUNT` is
    ///    where a three-partition ring keeps that value.
    /// 2. Advance `active_term_count`. **This is the linearization point**: the
    ///    tail is written first and the count second, so a producer that sees
    ///    the new count is guaranteed to find a zeroed tail.
    ///
    /// The return value is the second step's, not the first's. A rotation whose
    /// tail reset succeeded but whose count update did not has not happened.
    pub fn rotate(&self, current_term_count: i32, current_term_id: i32) -> bool {
        let next_term_id = current_term_id.wrapping_add(1);
        let next_term_count = current_term_count.wrapping_add(1);
        let next_index = position::index_by_term_count(next_term_count);
        let expected_term_id = next_term_id - descriptor::PARTITION_COUNT as i32;

        let offset = descriptor::TERM_TAIL_COUNTERS_OFFSET
            + next_index * descriptor::TERM_TAIL_COUNTER_STRIDE;

        let Some(raw) = self.metadata.load_i64_acquire(offset) else {
            return false;
        };

        if expected_term_id == RawTail::from_raw(raw).term_id() {
            let reset = RawTail::new(next_term_id, 0).raw();
            if !self
                .metadata
                .compare_exchange_i64(offset, raw, reset)
                .unwrap_or(false)
            {
                // Another producer rotated first. Whether that is this rotation
                // is decided by the count below, so carry on rather than
                // failing outright.
                return false;
            }
        }

        self.metadata
            .compare_exchange_i64(
                descriptor::ACTIVE_TERM_COUNT_OFFSET,
                i64::from(current_term_count),
                i64::from(next_term_count),
            )
            .unwrap_or(false)
    }

    /// The term buffer this appender writes into.
    pub fn term_buffer(&self) -> &AtomicBuffer<'a, ReadWrite> {
        &self.term
    }

    /// A field of the log's default header template, which is where the
    /// driver records the publication's session and stream.
    ///
    /// A padding frame is written with no publication context of its own, so
    /// this is what gives it an identity — the same thing the reference's
    /// `header_write` takes from the publication.
    fn default_header_i32(&self, field: usize) -> i32 {
        self.metadata
            .load_i32(descriptor::DEFAULT_FRAME_HEADER_OFFSET + field)
            .unwrap_or(0)
    }

    /// Initialise the three tails for a fresh log.
    ///
    /// Separate from any "apply defaults" because neither reference function
    /// does it: `aeron_logbuffer_metadata_init` documents that it leaves the
    /// tails alone (`aeron_logbuffer_descriptor.h:235-239`), and a writer that
    /// publishes without this produces three zeroed tails — which read as term
    /// id 0 / offset 0 in every partition and are silently wrong for any other
    /// `initial_term_id`.
    pub fn initialise_tails(&self, initial_term_id: i32) -> bool {
        for index in 0..descriptor::PARTITION_COUNT {
            // Partition `k` holds the term id of `(initial + k) - 3`: the term
            // three rotations ago, which is what `rotate` expects to find.
            let term_id = initial_term_id
                .wrapping_add(index as i32)
                .wrapping_sub(descriptor::PARTITION_COUNT as i32);

            let offset = descriptor::TERM_TAIL_COUNTERS_OFFSET
                + index * descriptor::TERM_TAIL_COUNTER_STRIDE;
            if self
                .metadata
                .store_i64_release(offset, RawTail::new(term_id, 0).raw())
                .is_none()
            {
                return false;
            }
        }

        // Partition 0 is the initial term itself, not one three back.
        let first = RawTail::new(initial_term_id, 0).raw();
        self.metadata
            .store_i64_release(descriptor::TERM_TAIL_COUNTERS_OFFSET, first)
            .is_some()
            && self
                .metadata
                .store_i32_relaxed(descriptor::ACTIVE_TERM_COUNT_OFFSET, 0)
                .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::AtomicBuffer;
    use crate::logbuffer::frame;
    use crate::logbuffer::scan::{Scanner, Step};

    /// The smallest legal term length, so the fixtures stay small.
    const TERM_LENGTH: i32 = 64 * 1024;
    const INITIAL_TERM_ID: i32 = 17;

    #[repr(align(64))]
    struct Buffer<const N: usize>([u8; N]);

    /// A metadata block and the term it describes.
    struct Log {
        metadata: Buffer<{ descriptor::METADATA_STRUCT_LENGTH }>,
        term: Box<Buffer<{ TERM_LENGTH as usize }>>,
    }

    impl Log {
        fn new() -> Self {
            let mut log = Self {
                metadata: Buffer([0u8; descriptor::METADATA_STRUCT_LENGTH]),
                // Boxed: 64 KiB is a legal term length and a large stack frame.
                term: Box::new(Buffer([0u8; TERM_LENGTH as usize])),
            };

            {
                let meta = AtomicBuffer::from_slice_mut(&mut log.metadata.0).expect("aligned");
                meta.store_i32_relaxed(descriptor::TERM_LENGTH_OFFSET, TERM_LENGTH)
                    .expect("in range");
                meta.store_i32_relaxed(descriptor::INITIAL_TERM_ID_OFFSET, INITIAL_TERM_ID)
                    .expect("in range");
            }

            log
        }

        fn appender(&mut self) -> Appender<'_> {
            Appender::new(
                AtomicBuffer::from_slice_mut(&mut self.metadata.0).expect("aligned"),
                AtomicBuffer::from_slice_mut(&mut self.term.0).expect("aligned"),
            )
            .expect("a usable log")
        }

        /// Force the current partition's tail, standing in for a term that has
        /// already carried traffic.
        fn set_tail(&mut self, term_id: i32, term_offset: i32, term_count: i32) {
            let meta = AtomicBuffer::from_slice_mut(&mut self.metadata.0).expect("aligned");
            let index = position::index_by_term_count(term_count);
            let offset = descriptor::TERM_TAIL_COUNTERS_OFFSET
                + index * descriptor::TERM_TAIL_COUNTER_STRIDE;

            meta.store_i64_release(offset, RawTail::new(term_id, term_offset).raw())
                .expect("in range");
            meta.store_i32_relaxed(descriptor::ACTIVE_TERM_COUNT_OFFSET, term_count)
                .expect("in range");
        }

        /// Read one frame back out, the way a consumer would.
        fn read_frame(&self, offset: usize) -> (i32, i16, Vec<u8>) {
            let view = AtomicBuffer::from_slice(&self.term.0).expect("aligned");
            let frame = frame::Frame::new(&view, offset);
            let length = frame.frame_length().expect("in range");
            let type_id = frame.type_id().expect("in range");

            let mut payload = vec![0u8; frame.payload_length().unwrap_or(0)];
            if !payload.is_empty() {
                frame.copy_payload(&mut payload).expect("copied");
            }

            (length, type_id, payload)
        }

        /// What a scanner sees first.
        fn first_step(&self) -> Step {
            let view = AtomicBuffer::from_slice(&self.term.0).expect("aligned");
            let mut scanner = Scanner::new(&view, TERM_LENGTH as usize);
            scanner.advance()
        }
    }

    #[test]
    fn appends_a_frame_a_reader_can_follow() {
        let mut log = Log::new();
        let appender = log.appender();
        appender
            .initialise_tails(initial_term_id())
            .then_some(())
            .expect("tails initialised");

        let outcome = appender.append(11, 22, i64::MAX, b"hello");

        let Appended::Ok { term_offset, .. } = outcome else {
            panic!("expected a frame, got {outcome:?}");
        };
        assert_eq!(0, term_offset);

        let (length, type_id, payload) = log.read_frame(0);
        assert_eq!(5 + frame::DATA_HEADER_LENGTH as i32, length);
        assert_eq!(frame::TYPE_DATA, type_id);
        assert_eq!(b"hello".to_vec(), payload);

        assert!(
            matches!(log.first_step(), Step::Data { offset: 0, .. }),
            "and a reader agrees it is a frame"
        );
    }

    #[test]
    fn a_frame_that_does_not_fit_pads_the_remainder_and_rotates() {
        let mut log = Log::new();

        {
            let appender = log.appender();
            appender
                .initialise_tails(initial_term_id())
                .then_some(())
                .expect("tails initialised");
        }

        // A tail one alignment unit from the end. It cannot be anything else:
        // every tail advance is 32-aligned and a term length is a multiple of
        // 32, so the remainder is either zero or at least one whole header.
        log.set_tail(INITIAL_TERM_ID, TERM_LENGTH - 32, 0);

        {
            let appender = log.appender();
            let outcome = appender.append(11, 22, i64::MAX, b"hello");
            assert!(matches!(outcome, Appended::EndOfLog), "got {outcome:?}");
        }

        // The remainder is covered by a padding frame, so a reader can step
        // past it.
        let (length, type_id, _) = log.read_frame((TERM_LENGTH - 32) as usize);
        assert_eq!(32, length);
        assert_eq!(frame::TYPE_PAD, type_id);

        // And the log has moved to the next term, whose tail starts at zero.
        let appender = log.appender();
        let tail = appender.current_tail().expect("readable");
        assert_eq!(INITIAL_TERM_ID + 1, tail.term_id());
        assert_eq!(0, tail.term_offset(TERM_LENGTH));
    }

    #[test]
    fn a_frame_landing_exactly_on_the_boundary_rotates_with_no_padding() {
        // A term can end with no padding frame at all. A reader that expected
        // one as a boundary marker would be wrong here.
        let mut log = Log::new();
        let payload = b"exactly";
        let frame_length = payload.len() as i32 + frame::DATA_HEADER_LENGTH as i32;
        let aligned = position::align_up(frame_length, descriptor::FRAME_ALIGNMENT);

        {
            let appender = log.appender();
            appender
                .initialise_tails(initial_term_id())
                .then_some(())
                .expect("tails initialised");
        }

        log.set_tail(INITIAL_TERM_ID, TERM_LENGTH - aligned, 0);

        {
            let appender = log.appender();
            let outcome = appender.append(11, 22, i64::MAX, payload);
            assert!(
                matches!(outcome, Appended::Ok { term_offset, .. } if term_offset == TERM_LENGTH - aligned),
                "got {outcome:?}"
            );

            // One more, and the term is exactly full.
            let outcome = appender.append(11, 22, i64::MAX, payload);
            assert!(matches!(outcome, Appended::EndOfLog), "got {outcome:?}");
        }

        // The frame that ended the term is still a data frame, not a padding
        // frame: there was nothing left to cover.
        let (_, type_id, _) = log.read_frame((TERM_LENGTH - aligned) as usize);
        assert_eq!(frame::TYPE_DATA, type_id);
    }

    #[test]
    fn back_pressure_is_reported_rather_than_written_through() {
        let mut log = Log::new();
        let appender = log.appender();
        appender
            .initialise_tails(initial_term_id())
            .then_some(())
            .expect("tails initialised");

        // A limit at the current position: the producer may not write at all.
        let outcome = appender.append(11, 22, 0, b"hello");
        assert!(
            matches!(outcome, Appended::BackPressured),
            "got {outcome:?}"
        );

        assert_eq!(
            Step::NotReady { offset: 0 },
            log.first_step(),
            "and nothing was written"
        );
    }

    #[test]
    fn initialise_tails_puts_each_partition_a_term_behind() {
        let mut log = Log::new();
        let appender = log.appender();

        assert!(appender.initialise_tails(INITIAL_TERM_ID));
        assert_eq!(Some(0), appender.active_term_count());

        // Partition `k` holds the term id of `(initial + k) - 3`: the term three
        // rotations ago, which is what `rotate` checks for before reusing it.
        for index in 0..descriptor::PARTITION_COUNT {
            let tail = appender.tail_at(index).expect("readable");
            let expected = if 0 == index {
                INITIAL_TERM_ID
            } else {
                INITIAL_TERM_ID + index as i32 - descriptor::PARTITION_COUNT as i32
            };

            assert_eq!(expected, tail.term_id(), "partition {index}");
            assert_eq!(0, tail.term_offset(TERM_LENGTH));
        }
    }

    #[test]
    fn a_log_whose_term_length_is_illegal_is_refused() {
        let mut log = Log::new();

        {
            let meta = AtomicBuffer::from_slice_mut(&mut log.metadata.0).expect("aligned");
            meta.store_i32_relaxed(descriptor::TERM_LENGTH_OFFSET, 1000)
                .expect("in range");
        }

        let term = AtomicBuffer::from_slice_mut(&mut log.term.0).expect("aligned");
        let meta = AtomicBuffer::from_slice_mut(&mut log.metadata.0).expect("aligned");

        assert!(
            Appender::new(meta, term).is_none(),
            "a term length that is not a power of two is not a log"
        );
    }

    const fn initial_term_id() -> i32 {
        INITIAL_TERM_ID
    }
}
