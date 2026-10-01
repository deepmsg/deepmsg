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
    DATA_HEADER_LENGTH, FLAG_BEGIN, FLAG_END, FLAG_UNFRAGMENTED, Frame, SESSION_ID_FIELD_OFFSET,
    STREAM_ID_FIELD_OFFSET, TERM_ID_FIELD_OFFSET, TERM_OFFSET_FIELD_OFFSET, TYPE_DATA, TYPE_OFFSET,
    TYPE_PAD,
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
    /// Nobody is subscribed, so there is no window to write into.
    ///
    /// Distinct from [`Appended::BackPressured`] and produced the same way the
    /// reference does: both mean "position is at or past the limit", and which
    /// one it is comes from the log's `is_connected` byte
    /// (`aeron-client/src/main/c/aeron_publication.h:76-95`). A caller that
    /// conflates them reports "slow subscriber" for "no subscriber".
    NotConnected,
    /// The producer caught the log mid-rotation: the term count names one
    /// partition while that partition's tail still carries the previous term's
    /// id, so there is no coherent position to write at. Like
    /// [`Appended::EndOfLog`] this means "try again" — the reference returns
    /// its `AERON_PUBLICATION_ADMIN_ACTION` for exactly this state
    /// (`aeron_publication.c:491-494`).
    MidRotation,
    /// The payload is beyond `max_message_length`, which the reference refuses
    /// too (`aeron_publication.c:515-524`).
    MessageTooLarge,
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
    /// `mtu_length - DATA_HEADER_LENGTH`: the largest payload that fits one
    /// frame. Beyond it the reference fragments.
    max_payload_length: usize,
    /// `min(term_length / 8, 16 MiB)`: the largest message of any shape.
    max_message_length: usize,
    /// A test-only seam: bytes this appender "lends" to another producer
    /// between its entry read and its claim.
    ///
    /// The race the claim's value source exists for lives in that window, and
    /// nothing outside [`Appender::append`] can be in it. A test sets this and
    /// gets a deterministic reproduction instead of a thread race; no other
    /// build has the field.
    #[cfg(test)]
    produce_race: Option<(i64, bool)>,
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

        // The MTU is what decides whether a payload fits one frame. A log with
        // no MTU is not one the driver would have created — every publication
        // gets one — so treat zero as refusing everything rather than as "no
        // bound", which would silently write frames the reference fragments.
        let mtu_length = metadata.load_i32(descriptor::MTU_LENGTH_OFFSET)?;
        let max_payload_length = mtu_length.saturating_sub(DATA_HEADER_LENGTH as i32);
        if max_payload_length <= 0 {
            return None;
        }

        Some(Self {
            metadata,
            term,
            term_length,
            initial_term_id,
            bits_to_shift,
            #[allow(clippy::cast_sign_loss)]
            max_payload_length: max_payload_length as usize,
            #[allow(clippy::cast_sign_loss)]
            max_message_length: position::max_message_length(term_length) as usize,
            #[cfg(test)]
            produce_race: None,
        })
    }

    /// Whether the driver considers this log connected to a subscriber.
    ///
    /// Read with **acquire**: the driver sets it with a release when a
    /// subscription links (`aeron_ipc_publication.h:130-138`), and this is what
    /// separates "nobody is listening" from "somebody is slow".
    pub fn is_connected(&self) -> Option<bool> {
        self.metadata
            .load_i32_acquire(descriptor::IS_CONNECTED_OFFSET)
            .map(|value| 1 == value)
    }

    /// The largest payload that fits a single frame.
    pub const fn max_payload_length(&self) -> usize {
        self.max_payload_length
    }

    /// The largest message of any shape
    /// (`min(term_length / 8, 16 MiB)`), beyond which a payload is refused
    /// rather than fragmented.
    pub const fn max_message_length(&self) -> usize {
        self.max_message_length
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
        // Acquire, as `aeron_logbuffer_active_term_count` reads it
        // (`aeron_logbuffer_descriptor.h:179-184`).
        self.metadata
            .load_i32_acquire(descriptor::ACTIVE_TERM_COUNT_OFFSET)
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
    /// See [`Appended`]. Two of its outcomes mean "try again" rather than
    /// "no": [`Appended::EndOfLog`] and [`Appended::MidRotation`].
    pub fn append(
        &self,
        session_id: i32,
        stream_id: i32,
        position_limit: i64,
        payload: &[u8],
    ) -> Appended {
        // Entry, in the reference's order (`aeron_publication.c:483-494`): the
        // limit, then the term count, then the tail that count names, and only
        // then a position. The pair is checked against itself because another
        // producer may be rotating right now — the count advanced while the
        // tail still carries the old term id — and every position computed
        // from a pair like that is nonsense. The reference returns
        // `ADMIN_ACTION` and the caller retries.
        let Some(term_count) = self.active_term_count() else {
            return Appended::Malformed;
        };
        let Some(tail) = self.tail_at(position::index_by_term_count(term_count)) else {
            return Appended::Malformed;
        };
        let term_id = tail.term_id();

        if term_count != position::term_count(term_id, self.initial_term_id) {
            return Appended::MidRotation;
        }

        let term_offset = tail.term_offset(self.term_length);
        let position = Position::new(
            term_id,
            term_offset,
            self.bits_to_shift,
            self.initial_term_id,
        );

        // At or past the limit means the offer does not happen at all — nothing
        // is written and the tail does not move (`aeron_publication.h:76-95`).
        // The classification is the reference's, in its order: an exhausted
        // stream outranks a closed window, and whether anyone is listening
        // decides between the other two.
        if position.raw() >= position_limit {
            return self.back_pressure(position, payload.len());
        }

        // Inside the window, and only now, the payload's *shape* matters
        // (`aeron_publication.c:499-524`): the size bounds are checked here
        // rather than before the window, so a payload too large for this
        // publication still reports back-pressure while nobody can take it.
        if payload.len() > self.max_message_length {
            return Appended::MessageTooLarge;
        }

        let partition = position::index_by_term_count(term_count);

        if payload.len() > self.max_payload_length {
            // Fragmented: the reservation is the whole message, computed with
            // the reference's own formula so the frames below land exactly
            // where it says (`aeron_logbuffer_descriptor.h:326-334`).
            let framed_length =
                descriptor::compute_fragmented_length(payload.len(), self.max_payload_length);

            return self.claim_and_place(
                session_id,
                stream_id,
                partition,
                term_count,
                term_id,
                payload,
                framed_length as i64,
                true,
            );
        }

        let Some(frame_length) = i32::try_from(payload.len() + DATA_HEADER_LENGTH).ok() else {
            return Appended::Malformed;
        };
        let aligned_length = position::align_up(frame_length, descriptor::FRAME_ALIGNMENT);

        self.claim_and_place(
            session_id,
            stream_id,
            partition,
            term_count,
            term_id,
            payload,
            i64::from(aligned_length),
            false,
        )
    }

    /// Claim the space, and write the frame or frames into it.
    ///
    /// The reference splits the same way: `offer` reads the world and decides,
    /// and `append_unfragmented_message` / `append_fragmented_message` each
    /// start with the fetch-and-add and then place what they were given
    /// (`aeron_publication.c:210-317`).
    ///
    /// `framed_length` is what to reserve — one aligned frame, or a whole
    /// fragmented message — and one fetch-and-add reserves all of it, which is
    /// what makes a fragmented message all-or-nothing within its term.
    #[allow(clippy::too_many_arguments)]
    fn claim_and_place(
        &self,
        session_id: i32,
        stream_id: i32,
        partition: usize,
        term_count: i32,
        term_id: i32,
        payload: &[u8],
        framed_length: i64,
        fragmented: bool,
    ) -> Appended {
        // The entry's count and term id are read only by the test seam below;
        // a real build takes everything from the claim.
        #[cfg(not(test))]
        let _ = (term_count, term_id);

        // The seam, if a test set one: another producer takes these bytes
        // before this one claims anything.
        #[cfg(test)]
        if let Some((stolen, rotates)) = self.produce_race {
            let offset = descriptor::TERM_TAIL_COUNTERS_OFFSET
                + partition * descriptor::TERM_TAIL_COUNTER_STRIDE;
            self.metadata.fetch_add_i64(offset, stolen);

            if rotates {
                // The competing producer finishes its own offer: it takes the
                // rest of the term and rotates.
                self.rotate(term_count, term_id);
            }
        }

        // The claim is the **only** source of where this message lands: the
        // tail it returns carries both the term id and the offset, and using
        // the entry read instead is how a concurrent producer's padding ends
        // up on top of a frame somebody already published
        // (`aeron_publication.c:182-186`).
        let Some(claimed) = self.claim(partition, framed_length) else {
            return Appended::Malformed;
        };
        let claimed_offset = claimed.term_offset(self.term_length);
        let claimed_term_id = claimed.term_id();

        #[allow(clippy::cast_possible_truncation)] // bounded by the term length
        let resulting_offset = claimed_offset.saturating_add(framed_length as i32);

        // Where this append would end, computed from the claimed values alone.
        let end_position = Position::new(
            claimed_term_id,
            resulting_offset,
            self.bits_to_shift,
            self.initial_term_id,
        );

        // The span may run past the term even though it started inside it. That
        // is the normal way a term ends — and for a fragmented message it is
        // also the rule that no message straddles two terms.
        if resulting_offset > self.term_length {
            return self.handle_end_of_log(claimed_offset, claimed_term_id, end_position);
        }

        let written = if fragmented {
            self.write_fragmented(
                session_id,
                stream_id,
                claimed_offset,
                claimed_term_id,
                payload,
            )
        } else {
            let Some(frame_length) = i32::try_from(payload.len() + DATA_HEADER_LENGTH).ok() else {
                return Appended::Malformed;
            };

            self.write_frame(
                session_id,
                stream_id,
                claimed_offset,
                claimed_term_id,
                frame_length,
                payload,
            )
        };

        if written.is_none() {
            return Appended::Malformed;
        }

        Appended::Ok {
            position: end_position,
            term_offset: claimed_offset,
        }
    }

    /// Write a message as several frames, each committed as it is written.
    ///
    /// The layout is the reference's (`aeron_publication.c:265-317`): every
    /// frame carries `min(remaining, max_payload_length)` bytes, the first is
    /// flagged `BEGIN`, the last `END`, and the cursor advances by the frame's
    /// **aligned** length while the length it records stays unaligned. The
    /// reservation covered the whole message, so the frames fill the span that
    /// was claimed exactly.
    fn write_fragmented(
        &self,
        session_id: i32,
        stream_id: i32,
        term_offset: i32,
        term_id: i32,
        payload: &[u8],
    ) -> Option<()> {
        let mut flags = FLAG_BEGIN;
        let mut offset = term_offset;
        let mut written = 0;

        while written < payload.len() {
            let remaining = payload.len() - written;
            let bytes = remaining.min(self.max_payload_length);
            let frame_length = i32::try_from(bytes + DATA_HEADER_LENGTH).ok()?;

            // The END flag goes on the frame that ends the message, before it
            // is written (`aeron_publication.c:287-290`).
            if remaining <= self.max_payload_length {
                flags |= FLAG_END;
            }

            let frame = Frame::new(&self.term, offset as usize);
            frame.begin(
                frame_length,
                flags,
                TYPE_DATA,
                offset,
                session_id,
                stream_id,
                term_id,
            )?;
            frame.write_payload(&payload[written..written + bytes])?;
            frame.publish(frame_length)?;

            flags = 0;
            offset += position::align_up(frame_length, descriptor::FRAME_ALIGNMENT);
            written += bytes;
        }

        Some(())
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
    ) -> Option<()> {
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

        Some(())
    }

    /// Which of the three "not now" answers a stalled offer gets
    /// (`aeron_publication.h:76-95`, and the same body again as
    /// `aeron_exclusive_publication_back_pressure_status`,
    /// `aeron_exclusive_publication.h:116-131` — the reference writes it twice,
    /// once per publication kind).
    ///
    /// The order is the reference's: an exhausted stream outranks a closed
    /// window, and whether anyone is listening decides between the other two.
    fn back_pressure(&self, position: Position, payload_length: usize) -> Appended {
        let frame_length = payload_length.saturating_add(DATA_HEADER_LENGTH);
        let aligned_length = frame_length.saturating_add(descriptor::FRAME_ALIGNMENT as usize - 1)
            & !(descriptor::FRAME_ALIGNMENT as usize - 1);

        if position.raw().saturating_add(aligned_length as i64)
            >= position::max_possible_position(self.term_length)
        {
            return Appended::MaxPositionExceeded;
        }

        match self.is_connected() {
            Some(true) => Appended::BackPressured,
            Some(false) => Appended::NotConnected,
            None => Appended::Malformed,
        }
    }

    /// Whether a caller's cached term is still the one this appender is over.
    ///
    /// The exclusive path takes its **offset** from the caller, and the
    /// reference takes the term buffer from the caller's cache too
    /// (`publication->active_partition_index`) — so a stale cache writes into
    /// one term and advances another's tail. Here the term buffer comes from
    /// `active_term_count` while the term id comes from the caller, and those
    /// two can disagree: that is [`Appended::MidRotation`], a retry, exactly as
    /// it is for a concurrent producer whose count and tail disagree.
    fn term_is_current(&self, term_id: i32) -> bool {
        self.active_term_count()
            .is_some_and(|count| count == position::term_count(term_id, self.initial_term_id))
    }

    /// Advance one partition's tail by a **plain store**
    /// (`aeron_put_raw_tail_release`, `aeron_exclusive_publication.c:25-28`).
    ///
    /// Release, not relaxed: the frame is written before the tail that
    /// publishes it, and a reader that saw the tail must see the bytes.
    ///
    /// # Errors
    ///
    /// [`None`] for a partition outside the ring, or a metadata block too short
    /// to hold the counter.
    fn put_raw_tail(&self, partition: usize, term_id: i32, term_offset: i32) -> Option<()> {
        if partition >= descriptor::PARTITION_COUNT {
            return None;
        }

        let offset = descriptor::TERM_TAIL_COUNTERS_OFFSET
            + partition * descriptor::TERM_TAIL_COUNTER_STRIDE;

        self.metadata
            .store_i64_release(offset, RawTail::new(term_id, term_offset).raw())
    }

    /// Append at a position the caller already holds, advancing the tail with a
    /// plain store instead of a claim
    /// (`aeron_exclusive_publication.c:73-115` and its three siblings).
    ///
    /// One line is the whole difference from [`Appender::append`]: the tail
    /// goes forward with `aeron_put_raw_tail_release` rather than a
    /// fetch-and-add, and the offset comes from the caller's record of where
    /// the last offer ended — the reference keeps that on the publication
    /// object (`publication->term_offset`, `:558`) and so does this.
    ///
    /// A claim is a promise that nobody else is writing this term. An exclusive
    /// producer *is* that promise, which is why dropping the fetch-and-add
    /// costs nothing — and also why two of them on one log buffer produce a
    /// corrupt log rather than a slow one: there is nothing left to keep them
    /// apart.
    ///
    /// # Errors
    ///
    /// See [`Appended`]. [`Appended::EndOfLog`] is the one a caller must act
    /// on: the term rotated, so the `term_offset` it passed is stale.
    pub fn append_exclusive(
        &self,
        session_id: i32,
        stream_id: i32,
        position_limit: i64,
        term_id: i32,
        term_offset: i32,
        payload: &[u8],
    ) -> Appended {
        if !self.term_is_current(term_id) {
            return Appended::MidRotation;
        }

        // The position is **derived** from the caller's two numbers, not
        // claimed from a tail: that pair is the whole of what this producer
        // knows about where it is (`aeron_exclusive_publication.c:557-559`).
        let position = Position::new(
            term_id,
            term_offset,
            self.bits_to_shift,
            self.initial_term_id,
        );

        if position.raw() >= position_limit {
            return self.back_pressure(position, payload.len());
        }

        // The bound, then the one-frame-or-several choice (`:565-604`). The
        // reference nests the bound inside the fragmented arm, because a
        // payload small enough for one frame cannot exceed a message maximum
        // that is never below one frame; checking it for both is the same
        // answer with one line fewer, and is what [`Appender::append`] does.
        if payload.len() > self.max_message_length {
            return Appended::MessageTooLarge;
        }

        let Some(frame_length) = i32::try_from(payload.len() + DATA_HEADER_LENGTH).ok() else {
            return Appended::Malformed;
        };
        let fragmented = payload.len() > self.max_payload_length;

        #[allow(clippy::cast_possible_wrap)] // bounded by the message maximum
        let framed_length = if fragmented {
            descriptor::compute_fragmented_length(payload.len(), self.max_payload_length) as i32
        } else {
            position::align_up(frame_length, descriptor::FRAME_ALIGNMENT)
        };

        let resulting_offset = term_offset.saturating_add(framed_length);
        let term_count = position::term_count(term_id, self.initial_term_id);

        // The tail moves **before** the bounds test (`:87-88`), and it moves
        // even for the append that overruns: the next producer — or the
        // rotation — has to see where this one got to.
        if self
            .put_raw_tail(
                position::index_by_term_count(term_count),
                term_id,
                resulting_offset,
            )
            .is_none()
        {
            return Appended::Malformed;
        }

        let end_position = Position::new(
            term_id,
            resulting_offset,
            self.bits_to_shift,
            self.initial_term_id,
        );

        if resulting_offset > self.term_length {
            return self.handle_end_of_log(term_offset, term_id, end_position);
        }

        let written = if fragmented {
            self.write_fragmented(session_id, stream_id, term_offset, term_id, payload)
        } else {
            self.write_frame(
                session_id,
                stream_id,
                term_offset,
                term_id,
                frame_length,
                payload,
            )
        };

        if written.is_none() {
            return Appended::Malformed;
        }

        Appended::Ok {
            position: end_position,
            term_offset,
        }
    }

    /// Claim `length` **payload** bytes at the caller's offset, without a
    /// fetch-and-add (`aeron_claim`, `aeron_exclusive_publication.c:323-355`).
    ///
    /// The answer is **where the claim ends**, not a window onto it: the caller
    /// knows the offset it asked at, builds a [`Frame`] over the term there,
    /// writes into it, and commits with [`Frame::publish`]. That is the
    /// reference's split exactly — `aeron_claim` returns `resulting_offset` and
    /// fills its caller's `buffer_claim` with pointers into the same term
    /// (`:347-354`), and `aeron_header_write` stores the length **negated**
    /// (`:41`) so a reader stepping the term meets a frame it may not read yet.
    ///
    /// The end is what a caller needs to **move**: an exclusive producer keeps
    /// its own offset, and this is the only thing that tells it where the log
    /// now is (`aeron_exclusive_publication_new_position`, `….h:99-108`).
    ///
    /// # Errors
    ///
    /// See [`Appended`]. A claim is one frame, so the bound is
    /// `max_payload_length` and not the message maximum: the reference refuses
    /// a longer claim outright rather than fragmenting it (`:717-726`), which
    /// is what makes a claimed frame something a caller can straddle.
    pub fn try_claim_exclusive(
        &self,
        session_id: i32,
        stream_id: i32,
        position_limit: i64,
        term_id: i32,
        term_offset: i32,
        length: usize,
    ) -> Result<Position, Appended> {
        if !self.term_is_current(term_id) {
            return Err(Appended::MidRotation);
        }

        let position = Position::new(
            term_id,
            term_offset,
            self.bits_to_shift,
            self.initial_term_id,
        );

        if position.raw() >= position_limit {
            return Err(self.back_pressure(position, length));
        }

        if length > self.max_payload_length {
            return Err(Appended::MessageTooLarge);
        }

        let Some(frame_length) = i32::try_from(length + DATA_HEADER_LENGTH).ok() else {
            return Err(Appended::Malformed);
        };

        let aligned_length = position::align_up(frame_length, descriptor::FRAME_ALIGNMENT);
        let resulting_offset = term_offset.saturating_add(aligned_length);
        let term_count = position::term_count(term_id, self.initial_term_id);

        if self
            .put_raw_tail(
                position::index_by_term_count(term_count),
                term_id,
                resulting_offset,
            )
            .is_none()
        {
            return Err(Appended::Malformed);
        }

        if resulting_offset > self.term_length {
            let end_position = Position::new(
                term_id,
                resulting_offset,
                self.bits_to_shift,
                self.initial_term_id,
            );

            return Err(self.handle_end_of_log(term_offset, term_id, end_position));
        }

        let frame = Frame::new(&self.term, term_offset as usize);
        if frame
            .begin(
                frame_length,
                FLAG_UNFRAGMENTED,
                TYPE_DATA,
                term_offset,
                session_id,
                stream_id,
                term_id,
            )
            .is_none()
        {
            return Err(Appended::Malformed);
        }

        Ok(Position::new(
            term_id,
            resulting_offset,
            self.bits_to_shift,
            self.initial_term_id,
        ))
    }

    /// Append a padding frame of `length` **payload** bytes at the caller's
    /// offset (`aeron_append_padding`, `aeron_exclusive_publication.c:356-388`).
    ///
    /// # Errors
    ///
    /// See [`Appended`]; `length` above `max_message_length` is refused, which
    /// is the bound the reference names here and **not** the one-frame bound
    /// [`Appender::try_claim_exclusive`] uses (`:761-770`).
    pub fn append_padding_exclusive(
        &self,
        session_id: i32,
        stream_id: i32,
        position_limit: i64,
        term_id: i32,
        term_offset: i32,
        length: usize,
    ) -> Appended {
        if !self.term_is_current(term_id) {
            return Appended::MidRotation;
        }

        let position = Position::new(
            term_id,
            term_offset,
            self.bits_to_shift,
            self.initial_term_id,
        );

        if position.raw() >= position_limit {
            return self.back_pressure(position, length);
        }

        if length > self.max_message_length {
            return Appended::MessageTooLarge;
        }

        let Some(frame_length) = i32::try_from(length + DATA_HEADER_LENGTH).ok() else {
            return Appended::Malformed;
        };

        let aligned_length = position::align_up(frame_length, descriptor::FRAME_ALIGNMENT);
        let resulting_offset = term_offset.saturating_add(aligned_length);
        let term_count = position::term_count(term_id, self.initial_term_id);

        if self
            .put_raw_tail(
                position::index_by_term_count(term_count),
                term_id,
                resulting_offset,
            )
            .is_none()
        {
            return Appended::Malformed;
        }

        let end_position = Position::new(
            term_id,
            resulting_offset,
            self.bits_to_shift,
            self.initial_term_id,
        );

        if resulting_offset > self.term_length {
            return self.handle_end_of_log(term_offset, term_id, end_position);
        }

        // Written as a data frame and then retyped, as the end-of-log padding
        // is: a padding frame keeps `BEGIN|END`, and only its type says it is
        // not for reading.
        let frame = Frame::new(&self.term, term_offset as usize);
        if frame
            .begin(
                frame_length,
                FLAG_UNFRAGMENTED,
                TYPE_DATA,
                term_offset,
                session_id,
                stream_id,
                term_id,
            )
            .is_none()
            || frame.set_type(TYPE_PAD).is_none()
            || frame.publish(frame_length).is_none()
        {
            return Appended::Malformed;
        }

        Appended::Ok {
            position: end_position,
            term_offset,
        }
    }

    /// Append a frame the caller has already built
    /// (`aeron_append_block`, `aeron_exclusive_publication.c:389-416`, through
    /// `aeron_exclusive_publication_offer_block`, `:810-895`).
    ///
    /// `block` is a **whole aligned frame** — header and payload — and it is
    /// copied in rather than written field by field. That is what makes the
    /// ordering below the point of the function: the header's last eight bytes
    /// carry the frame length, they are stored **release** and last, so a
    /// reader that sees the length sees everything the length covers.
    ///
    /// Three of `offer_block`'s rules live here rather than in a caller,
    /// because they are about the block's shape and nothing else:
    ///
    /// * a block may not straddle a term, so a caller sitting exactly at the
    ///   end is rotated first and told to come back (`:836-839`) — **not**
    ///   padded, which is why this cannot go through `handle_end_of_log`'s
    ///   usual path;
    /// * a block longer than what is left of the term is refused rather than
    ///   padded around (`:847-855`);
    /// * the block is checked against **five** fields — offset, session,
    ///   stream, term id and type — and any disagreement is a refusal
    ///   (`:857-877`). A block whose header says it belongs somewhere else
    ///   would publish a frame that lies about where it is.
    ///
    /// # Errors
    ///
    /// See [`Appended`].
    pub fn append_block_exclusive(
        &self,
        session_id: i32,
        stream_id: i32,
        position_limit: i64,
        term_id: i32,
        term_offset: i32,
        block: &[u8],
    ) -> Appended {
        if !self.term_is_current(term_id) {
            return Appended::MidRotation;
        }

        let position = Position::new(
            term_id,
            term_offset,
            self.bits_to_shift,
            self.initial_term_id,
        );

        // A caller whose offset has reached the end of the term is not asking
        // about a block that fits: it is asking about the next term. The
        // reference rotates here and carries on; here the rotation is done and
        // the caller is told, because the new offset lives on the caller.
        if term_offset >= self.term_length {
            return self.handle_end_of_log(term_offset, term_id, position);
        }

        if position.raw() >= position_limit {
            return self.back_pressure(position, block.len());
        }

        if block.len() > (self.term_length - term_offset) as usize {
            return Appended::MessageTooLarge;
        }

        if !block_belongs_at(block, session_id, stream_id, term_id, term_offset) {
            return Appended::Malformed;
        }

        let resulting_offset =
            term_offset.saturating_add(i32::try_from(block.len()).unwrap_or(i32::MAX));
        let term_count = position::term_count(term_id, self.initial_term_id);

        if self
            .put_raw_tail(
                position::index_by_term_count(term_count),
                term_id,
                resulting_offset,
            )
            .is_none()
        {
            return Appended::Malformed;
        }

        if self.copy_block_in(term_offset, block).is_none() {
            return Appended::Malformed;
        }

        Appended::Ok {
            position: Position::new(
                term_id,
                resulting_offset,
                self.bits_to_shift,
                self.initial_term_id,
            ),
            term_offset,
        }
    }

    /// Copy a whole frame in, publishing it with the header's last word
    /// (`aeron_append_block`, `aeron_exclusive_publication.c:389-416`).
    ///
    /// The order is the reference's and it is the whole reason this is not a
    /// `copy_in`: the three words **after** the first are stored first, so that
    /// the release store of the first — which holds the frame length — is what
    /// makes the frame readable. A reader that sees the length has, by the
    /// release, seen everything the length covers.
    fn copy_block_in(&self, term_offset: i32, block: &[u8]) -> Option<()> {
        let base = term_offset as usize;
        self.term
            .copy_in(base + DATA_HEADER_LENGTH, &block[DATA_HEADER_LENGTH..])?;

        for offset in [24usize, 16, 8] {
            let value = read_i64(block, offset)?;
            self.term.store_i64_relaxed(base + offset, value)?;
        }

        self.term.store_i64_release(base, read_i64(block, 0)?)
    }

    /// Claim `length` bytes of one partition's tail.
    ///
    /// A fetch-and-add, not a compare-and-exchange: every producer gets a
    /// distinct span and nobody retries. The reference does the same
    /// (`aeron_publication.c:109-114`), against the counter the entry read
    /// picked — re-reading `active_term_count` here would be a second read that
    /// can disagree with the first, which is the whole reason the guard at the
    /// top of [`Appender::append`] exists.
    fn claim(&self, partition: usize, length: i64) -> Option<RawTail> {
        let offset = descriptor::TERM_TAIL_COUNTERS_OFFSET
            + partition * descriptor::TERM_TAIL_COUNTER_STRIDE;

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

        // The count comes from the **claimed** term id, not from a read taken
        // before the claim. And the rotation is attempted, not required:
        // another producer may have done it already, and the reference ignores
        // the answer entirely (`aeron_publication.c:144-171`) — the caller
        // retries either way, and treating a lost race as a metadata error is
        // how a normal competition gets reported as a corrupt log.
        let term_count = position::term_count(term_id, self.initial_term_id);
        self.rotate(term_count, term_id);

        Appended::EndOfLog
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
        let expected_term_id = next_term_id.wrapping_sub(descriptor::PARTITION_COUNT as i32);

        let offset = descriptor::TERM_TAIL_COUNTERS_OFFSET
            + next_index * descriptor::TERM_TAIL_COUNTER_STRIDE;

        // The reset retries while it is still ours to perform. Losing the
        // compare-exchange to another producer rotating the same term is not a
        // failure — it means the reset is done — and the reference loops for
        // exactly that reason (`aeron_logbuffer_descriptor.h:197-211`).
        loop {
            let Some(raw) = self.metadata.load_i64_acquire(offset) else {
                return false;
            };

            if expected_term_id != RawTail::from_raw(raw).term_id() {
                break;
            }

            if self
                .metadata
                .compare_exchange_i64(offset, raw, RawTail::new(next_term_id, 0).raw())
                .unwrap_or(false)
            {
                break;
            }
        }

        // The count is the linearization point, and it is **four bytes wide**:
        // the four bytes after it are structure padding that nothing promises
        // to keep zero, and an 8-byte compare-exchange would take them into
        // both the comparison and the write
        // (`aeron_logbuffer_descriptor.h:186-191`).
        self.metadata
            .compare_exchange_i32(
                descriptor::ACTIVE_TERM_COUNT_OFFSET,
                current_term_count,
                next_term_count,
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

/// Whether a block's own header says it belongs at this offset of this stream
/// (`aeron_exclusive_publication.c:857-877`).
///
/// All **five** fields the reference compares, and every one of them is a way
/// a block can be wrong: a frame whose header disagrees with where it is being
/// written would be published claiming a term, a stream or a session it is not
/// on, and a reader would take it.
fn block_belongs_at(
    block: &[u8],
    session_id: i32,
    stream_id: i32,
    term_id: i32,
    term_offset: i32,
) -> bool {
    read_i32(block, TERM_OFFSET_FIELD_OFFSET) == Some(term_offset)
        && read_i32(block, SESSION_ID_FIELD_OFFSET) == Some(session_id)
        && read_i32(block, STREAM_ID_FIELD_OFFSET) == Some(stream_id)
        && read_i32(block, TERM_ID_FIELD_OFFSET) == Some(term_id)
        && read_i16(block, TYPE_OFFSET) == Some(TYPE_DATA)
}

/// A little-endian field out of a frame a caller built. A block too short to
/// hold the field is not one that belongs anywhere.
fn read_i32(block: &[u8], offset: usize) -> Option<i32> {
    Some(i32::from_le_bytes(
        block.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_i16(block: &[u8], offset: usize) -> Option<i16> {
    Some(i16::from_le_bytes(
        block.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_i64(block: &[u8], offset: usize) -> Option<i64> {
    Some(i64::from_le_bytes(
        block.get(offset..offset + 8)?.try_into().ok()?,
    ))
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
                // A real log always has one: the driver writes the publication's
                // MTU at creation, and without it no payload fits a frame.
                meta.store_i32_relaxed(
                    descriptor::MTU_LENGTH_OFFSET,
                    descriptor::MTU_LENGTH_DEFAULT,
                )
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

        /// Set the byte the reference uses to tell "slow subscriber" from "no
        /// subscriber". In a real log the driver writes it.
        fn set_connected(&mut self, connected: bool) {
            let meta = AtomicBuffer::from_slice_mut(&mut self.metadata.0).expect("aligned");
            let value = i32::from(connected);
            meta.store_i32_relaxed(descriptor::IS_CONNECTED_OFFSET, value)
                .expect("in range");
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

        /// The flags byte of the frame at `offset`.
        fn frame_flags(&self, offset: usize) -> Option<u8> {
            let view = AtomicBuffer::from_slice(&self.term.0).expect("aligned");
            frame::Frame::new(&view, offset).flags()
        }

        /// How far the current term's tail has advanced.
        fn tail_offset(&self) -> Option<i32> {
            let view = AtomicBuffer::from_slice(&self.metadata.0).expect("aligned");
            let index = position::index_by_term_count(0);
            let offset = descriptor::TERM_TAIL_COUNTERS_OFFSET
                + index * descriptor::TERM_TAIL_COUNTER_STRIDE;

            Some(RawTail::from_raw(view.load_i64_acquire(offset)?).term_offset(TERM_LENGTH))
        }

        /// The current partition's tail **without** the term-length
        /// saturation, so a tail that has run past the end still reads as
        /// itself — which is the whole point of testing the overrun.
        fn tail_value(&self) -> i32 {
            self.tail_value_opt().expect("a tail")
        }

        fn tail_value_opt(&self) -> Option<i32> {
            let view = AtomicBuffer::from_slice(&self.metadata.0).expect("aligned");
            let raw = view.load_i64_acquire(descriptor::TERM_TAIL_COUNTERS_OFFSET)?;

            #[allow(clippy::cast_possible_truncation)] // a term offset
            Some(RawTail::from_raw(raw).raw_term_offset() as i32)
        }

        /// What a scanner sees first.
        fn first_step(&self) -> Step {
            let view = AtomicBuffer::from_slice(&self.term.0).expect("aligned");
            let mut scanner = Scanner::new(&view, TERM_LENGTH as usize);
            scanner.advance()
        }
    }

    /// The frame a "concurrent producer" left behind at `offset`, written by
    /// hand so a test can tell whether anything covered it later.
    fn another_producers_frame(log: &mut Log, offset: i32) {
        let view = AtomicBuffer::from_slice_mut(&mut log.term.0).expect("aligned");
        let frame = frame::Frame::new(&view, offset as usize);
        frame
            .begin(
                64,
                FLAG_UNFRAGMENTED,
                TYPE_DATA,
                offset,
                11,
                22,
                initial_term_id(),
            )
            .expect("fits");
        frame.write_payload(&[0x5A; 32]).expect("fits");
        frame.publish(64).expect("fits");
    }

    /// Run `initialise_tails` through an appender that is then dropped, so the
    /// caller keeps `&mut log` for the rest of its setup.
    fn initialised(log: &mut Log) {
        let appender = log.appender();
        appender
            .initialise_tails(initial_term_id())
            .then_some(())
            .expect("tails initialised");
    }

    /// A whole aligned frame, as a caller would have built it for
    /// `append_block_exclusive`: header and payload, ready to be copied in.
    fn a_block(
        term_id: i32,
        term_offset: i32,
        session_id: i32,
        stream_id: i32,
        payload: &[u8],
    ) -> Buffer<256> {
        let mut block = Buffer([0u8; 256]);
        let frame_length = i32::try_from(payload.len() + DATA_HEADER_LENGTH).expect("small");

        let view = AtomicBuffer::from_slice_mut(&mut block.0).expect("aligned");
        let frame = frame::Frame::new(&view, 0);
        frame
            .begin(
                frame_length,
                FLAG_UNFRAGMENTED,
                TYPE_DATA,
                term_offset,
                session_id,
                stream_id,
                term_id,
            )
            .expect("fits");
        frame.write_payload(payload).expect("fits");
        frame.publish(frame_length).expect("fits");

        block
    }

    #[test]
    fn a_claim_hands_back_a_frame_its_caller_publishes() {
        // The split the reference makes with `aeron_header_write` (`:30-49`):
        // the header is written with the length **negated**, so a reader
        // stepping the term meets a frame it may not read, and the positive
        // length is what makes it readable.
        let mut log = Log::new();
        initialised(&mut log);

        let payload = b"written into a claim";
        let frame_length = i32::try_from(payload.len() + DATA_HEADER_LENGTH).expect("small");
        let aligned = position::align_up(frame_length, descriptor::FRAME_ALIGNMENT);

        let end = {
            let appender = log.appender();
            appender
                .try_claim_exclusive(11, 22, i64::MAX, initial_term_id(), 0, payload.len())
                .expect("a claim")
        };

        // The claim answers with where the frame **ends**, because that is what
        // its caller has to move to; where it starts is what the caller asked.
        assert_eq!(
            aligned,
            end.term_offset(position::bits_to_shift(TERM_LENGTH).expect("a term")),
            "the claim answers with the end of the frame"
        );

        {
            let view = AtomicBuffer::from_slice_mut(&mut log.term.0).expect("aligned");
            let claim = frame::Frame::new(&view, 0);

            assert_eq!(
                Some(-frame_length),
                claim.frame_length(),
                "nothing may read it yet: the length is negative"
            );

            claim.write_payload(payload).expect("in range");
            claim.publish(frame_length).expect("in range");
        }

        let (length, type_id, read_back) = log.read_frame(0);
        assert_eq!(frame_length, length);
        assert_eq!(TYPE_DATA, type_id);
        assert_eq!(payload.to_vec(), read_back);
    }

    #[test]
    fn a_claim_is_one_frame_and_is_refused_beyond_it() {
        // A claim cannot be fragmented — that is what makes it something a
        // caller can hold open — so the bound is the frame's, not the
        // message's (`aeron_exclusive_publication.c:717-726`).
        let mut log = Log::new();
        initialised(&mut log);

        let appender = log.appender();
        let max_payload = appender.max_payload_length();
        assert!(
            max_payload < appender.max_message_length(),
            "the two differ"
        );

        assert!(
            appender
                .try_claim_exclusive(11, 22, i64::MAX, initial_term_id(), 0, max_payload)
                .is_ok(),
            "exactly one frame's worth is a claim"
        );

        assert_eq!(
            Err(Appended::MessageTooLarge),
            log.appender().try_claim_exclusive(
                11,
                22,
                i64::MAX,
                initial_term_id(),
                max_payload as i32,
                max_payload + 1
            )
        );
    }

    #[test]
    fn a_padding_frame_carries_the_length_it_was_asked_for() {
        let mut log = Log::new();
        initialised(&mut log);

        let length = 64usize;
        let outcome =
            log.appender()
                .append_padding_exclusive(11, 22, i64::MAX, initial_term_id(), 0, length);

        assert_eq!(
            Appended::Ok {
                position: Position::new(
                    initial_term_id(),
                    i32::try_from(length + DATA_HEADER_LENGTH).expect("small"),
                    position::bits_to_shift(TERM_LENGTH).expect("a term"),
                    initial_term_id(),
                ),
                term_offset: 0,
            },
            outcome
        );

        let (frame_length, type_id, read_back) = log.read_frame(0);
        assert_eq!(TYPE_PAD, type_id, "a padding frame, not a data frame");
        assert_eq!(
            i32::try_from(length + DATA_HEADER_LENGTH).expect("small"),
            frame_length
        );
        assert_eq!(
            length,
            read_back.len(),
            "it carries a payload's worth of bytes"
        );
        assert!(
            read_back.iter().all(|byte| *byte == 0),
            "and the reference writes nothing into them: a padding frame is \n             skipped by its type, not by having no payload"
        );
    }

    /// **What this does not cover**: `copy_block_in`'s *ordering*
    /// (`hdr[3]`, `hdr[2]`, `hdr[1]`, then the release on `hdr[0]`) is a claim
    /// about what a reader can see while the copy is happening, and a reader in
    /// this process cannot be made to look mid-copy. A single-threaded test can
    /// only check that the bytes came out right — which a plain `copy_in`
    /// would also satisfy. The order is pinned by the comment on
    /// [`Appender::copy_block_in`] and by `aeron_exclusive_publication.c:389-416`,
    /// not by anything below.
    #[test]
    fn a_block_is_copied_in_whole_only_when_its_header_belongs_here() {
        // Five fields, and each one is a way a caller can be wrong
        // (`aeron_exclusive_publication.c:857-877`). A block that disagrees
        // with any of them would publish a frame claiming a place it is not.
        let payload = b"a block";

        for (term_id, term_offset, session_id, stream_id) in [
            (initial_term_id(), 0, 11, 22),
            (initial_term_id() + 1, 0, 11, 22),
            (initial_term_id(), 64, 11, 22),
            (initial_term_id(), 0, 99, 22),
            (initial_term_id(), 0, 11, 99),
        ] {
            let mut log = Log::new();
            initialised(&mut log);

            let block = a_block(term_id, term_offset, session_id, stream_id, payload);
            let outcome = log.appender().append_block_exclusive(
                11,
                22,
                i64::MAX,
                initial_term_id(),
                0,
                &block.0[..payload.len() + DATA_HEADER_LENGTH],
            );

            let agrees = term_id == initial_term_id()
                && term_offset == 0
                && session_id == 11
                && stream_id == 22;

            if agrees {
                let Appended::Ok { term_offset, .. } = outcome else {
                    panic!("{outcome:?}");
                };
                assert_eq!(0, term_offset);

                let (_, type_id, read_back) = log.read_frame(0);
                assert_eq!(TYPE_DATA, type_id);
                assert_eq!(payload.to_vec(), read_back);
            } else {
                assert_eq!(
                    Appended::Malformed,
                    outcome,
                    "term_id={term_id} offset={term_offset} session={session_id} stream={stream_id}"
                );
                assert_eq!(0, log.read_frame(0).0, "and nothing was copied in");
            }
        }
    }

    #[test]
    fn a_block_longer_than_the_term_has_left_is_refused_rather_than_padded_around() {
        // A block may not straddle a term, and unlike an offer it is not
        // padded around: the caller built a frame that does not fit, which is a
        // different mistake from a payload that happens to land at the end
        // (`aeron_exclusive_publication.c:847-855`).
        let mut log = Log::new();
        initialised(&mut log);

        let payload = b"a block";
        let frame_length = i32::try_from(payload.len() + DATA_HEADER_LENGTH).expect("small");
        let offset = TERM_LENGTH - frame_length / 2;
        log.set_tail(initial_term_id(), offset, 0);

        let block = a_block(initial_term_id(), offset, 11, 22, payload);
        let outcome = log.appender().append_block_exclusive(
            11,
            22,
            i64::MAX,
            initial_term_id(),
            offset,
            &block.0[..payload.len() + DATA_HEADER_LENGTH],
        );

        assert_eq!(Appended::MessageTooLarge, outcome);
        assert_eq!(
            offset,
            log.tail_value(),
            "and the refusal comes before the store"
        );
    }

    #[test]
    fn a_block_asked_for_at_the_end_of_a_term_rotates_and_says_so() {
        // The one place `offer_block` differs in *shape* from the other three:
        // a caller sitting exactly at the end of a term is not being offered a
        // block that fits, it is being offered the next term
        // (`aeron_exclusive_publication.c:836-839`).
        let mut log = Log::new();
        initialised(&mut log);
        log.set_tail(initial_term_id(), TERM_LENGTH, 0);

        let block = a_block(initial_term_id() + 1, 0, 11, 22, b"a block");
        let outcome = log.appender().append_block_exclusive(
            11,
            22,
            i64::MAX,
            initial_term_id(),
            TERM_LENGTH,
            &block.0[..],
        );

        assert_eq!(Appended::EndOfLog, outcome, "rotate, and come back");
        assert_eq!(
            TERM_LENGTH,
            log.tail_value(),
            "the tail of the term it was asked about is where it was: a full              term is rotated, not padded and not written into"
        );
        assert_eq!(
            0,
            log.read_frame(TERM_LENGTH as usize - 128).0,
            "and nothing was written into it: a term that ends exactly on the \
             boundary is rotated with no padding frame, because there is no \
             remainder for one to cover"
        );
    }

    #[test]
    fn an_exclusive_append_with_a_stale_term_is_a_retry_not_a_write() {
        // The exclusive path takes its term id from the caller and its term
        // buffer from `active_term_count`, so a stale cache would advance one
        // term's tail while writing into another's pages. The reference cannot
        // be stale here because it takes both from the same cache; this can, so
        // it refuses instead.
        let mut log = Log::new();
        initialised(&mut log);

        // The log has rotated once; the caller still thinks it is in the first
        // term.
        log.set_tail(initial_term_id() + 1, 0, 1);

        let outcome =
            log.appender()
                .append_exclusive(11, 22, i64::MAX, initial_term_id(), 0, b"a payload");

        assert_eq!(Appended::MidRotation, outcome);
        assert_eq!(
            0,
            log.tail_value(),
            "and nothing was written into either term"
        );
        assert_eq!(0, log.read_frame(0).0);
    }

    #[test]
    fn an_exclusive_append_writes_the_same_bytes_as_a_claimed_one() {
        // The two paths differ in how the tail moves and in nothing else, so
        // the strongest thing to say about the new one is that a reader cannot
        // tell them apart.
        let payload = b"a payload both paths have to agree on";
        let (session_id, stream_id) = (11, 22);

        let mut claimed = Log::new();
        claimed.set_tail(initial_term_id(), 0, 0);
        let claimed_outcome = claimed
            .appender()
            .append(session_id, stream_id, i64::MAX, payload);

        let mut exclusive = Log::new();
        exclusive.set_tail(initial_term_id(), 0, 0);
        let exclusive_outcome = exclusive.appender().append_exclusive(
            session_id,
            stream_id,
            i64::MAX,
            initial_term_id(),
            0,
            payload,
        );

        assert_eq!(claimed_outcome, exclusive_outcome, "same answer");
        assert_eq!(
            claimed.term.0[..],
            exclusive.term.0[..],
            "and byte for byte the same term"
        );

        let (length, type_id, read_back) = exclusive.read_frame(0);
        assert_eq!(payload.len() + DATA_HEADER_LENGTH, length as usize);
        assert_eq!(TYPE_DATA, type_id);
        assert_eq!(payload.to_vec(), read_back);
    }

    #[test]
    fn an_exclusive_append_writes_where_the_caller_says_not_where_the_tail_is() {
        // The one observable difference from a claim, and the reason the
        // caller owns the offset: an exclusive producer's position is its own
        // record, not something it asks the log for
        // (`aeron_exclusive_publication.c:558`). The tail is deliberately set
        // somewhere else, so a claim-shaped implementation would write at 4096
        // and this has to write at 0.
        let mut log = Log::new();
        initialised(&mut log);
        log.set_tail(initial_term_id(), 4096, 0);

        let payload = b"where the caller says";
        let outcome =
            log.appender()
                .append_exclusive(11, 22, i64::MAX, initial_term_id(), 0, payload);

        let Appended::Ok { term_offset, .. } = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(0, term_offset, "the caller's offset, not the tail's");

        let (_, type_id, read_back) = log.read_frame(0);
        assert_eq!(TYPE_DATA, type_id, "the frame is at 0");
        assert_eq!(payload.to_vec(), read_back);

        let (_, _, at_the_tail) = log.read_frame(4096);
        assert_eq!(0, at_the_tail.len(), "and nothing was written at 4096");
    }

    #[test]
    fn an_exclusive_append_that_overruns_the_term_still_moves_the_tail() {
        // The store comes **before** the bounds test
        // (`aeron_exclusive_publication.c:87-88`), which reads like an
        // accident and is not: the append that overruns is the one that ends
        // the term, and a next producer — or the rotation — that saw the old
        // tail would write a frame where this one already padded.
        let mut log = Log::new();
        initialised(&mut log);

        let offset = TERM_LENGTH - 64;
        log.set_tail(initial_term_id(), offset, 0);

        let outcome = log.appender().append_exclusive(
            11,
            22,
            i64::MAX,
            initial_term_id(),
            offset,
            &[0xAB; 128],
        );

        assert_eq!(Appended::EndOfLog, outcome);

        // 128 bytes plus a header, aligned, from an offset 64 short of the end.
        let frame_length = i32::try_from(128 + DATA_HEADER_LENGTH).expect("small");
        let expected = offset + position::align_up(frame_length, descriptor::FRAME_ALIGNMENT);
        assert!(
            expected > TERM_LENGTH,
            "the span has to leave the term for this test to say anything"
        );
        assert_eq!(
            expected,
            log.tail_value(),
            "the tail carries the overrun, not the last offset that fitted"
        );
    }

    #[test]
    fn an_exclusive_append_past_the_window_is_classified_like_a_claimed_one() {
        // The three "not now" answers are one body in the reference, written
        // twice (`aeron_exclusive_publication.h:116-131`), and the two paths
        // here share one — so the interesting part is that a caller that has
        // run out of window is told *which* way it has run out.
        let mut log = Log::new();
        initialised(&mut log);
        log.set_connected(false);

        let outcome = log
            .appender()
            .append_exclusive(11, 22, 0, initial_term_id(), 0, b"payload");

        assert_eq!(
            Appended::NotConnected,
            outcome,
            "no window, and nobody listening"
        );

        log.set_connected(true);
        let outcome = log
            .appender()
            .append_exclusive(11, 22, 0, initial_term_id(), 0, b"payload");

        assert_eq!(
            Appended::BackPressured,
            outcome,
            "no window, but somebody is"
        );
    }

    #[test]
    fn an_exclusive_append_beyond_the_message_maximum_is_refused() {
        let mut log = Log::new();
        initialised(&mut log);

        // A tail that is somewhere, so "unchanged" is a claim worth making.
        log.set_tail(initial_term_id(), 4096, 0);

        let too_large = log.appender().max_message_length() + 1;
        let outcome = log.appender().append_exclusive(
            11,
            22,
            i64::MAX,
            initial_term_id(),
            0,
            &vec![0u8; too_large],
        );

        assert_eq!(Appended::MessageTooLarge, outcome);
        assert_eq!(
            4096,
            log.tail_value(),
            "and the refusal comes before the store, so the tail has not moved"
        );
    }

    #[test]
    fn padding_covers_the_span_this_producer_claimed_not_the_one_it_entered_at() {
        // A1's gate. Another producer takes the tail *between this one's entry
        // read and its claim* — the window the seam creates — and publishes a
        // frame there. This producer's payload no longer fits, so it pads: from
        // the offset it **claimed**, which leaves the other frame alone. A
        // padding written from the entry offset would start 8128 bytes earlier
        // and cover it.
        let mut log = Log::new();
        initialised(&mut log);

        let entry_offset = TERM_LENGTH - 8192;
        another_producers_frame(&mut log, entry_offset);
        log.set_tail(initial_term_id(), entry_offset + 64, 0);

        let mut appender = log.appender();
        // The competing producer takes everything but the last 32 bytes.
        appender.produce_race = Some((i64::from(TERM_LENGTH - 32 - (entry_offset + 64)), false));

        let outcome = { appender.append(11, 22, i64::MAX, &[0xAB; 128]) };

        assert_eq!(
            Appended::EndOfLog,
            outcome,
            "the tail moved to the end of the term, so it is padded and rotated"
        );

        let (padding_length, type_id, _) = log.read_frame((TERM_LENGTH - 32) as usize);
        assert_eq!(
            TYPE_PAD, type_id,
            "the padding starts where the claim landed"
        );
        assert_eq!(32, padding_length, "and covers exactly what was left");

        let (length, type_id, payload) = log.read_frame(entry_offset as usize);
        assert_eq!(64, length, "the other producer's frame is untouched");
        assert_eq!(TYPE_DATA, type_id);
        assert_eq!(vec![0x5A; 32], payload);
    }

    #[test]
    fn a_rotation_another_producer_already_did_is_not_a_metadata_error() {
        // A2's gate. Both producers reach the end of the term; the other one
        // rotates first, so this one's tail compare-exchange finds the next
        // partition already reset and its count compare-exchange finds the
        // count already advanced. That is a race won by somebody else, not a
        // corrupt log — the reference ignores the rotation's answer entirely
        // (`aeron_publication.c:144-171`) and so does this now.
        let mut log = Log::new();
        initialised(&mut log);
        log.set_tail(initial_term_id(), TERM_LENGTH - 128, 0);

        let outcome = {
            let mut appender = log.appender();
            appender.produce_race = Some((128, true));
            appender.append(11, 22, i64::MAX, &[0xAB; 128])
        };

        assert_eq!(
            Appended::EndOfLog,
            outcome,
            "the caller retries; it is not told the log is broken"
        );

        let meta = AtomicBuffer::from_slice(&log.metadata.0).expect("aligned");
        assert_eq!(
            Some(1),
            meta.load_i32_acquire(descriptor::ACTIVE_TERM_COUNT_OFFSET),
            "and the rotation really happened — by the other producer"
        );
    }

    #[test]
    fn rotating_leaves_the_four_bytes_after_the_term_count_alone() {
        // A3's gate. `active_term_count` is four bytes wide and the four bytes
        // after it are structure padding that nothing promises to keep zero. An
        // eight-byte compare-exchange fails to rotate at all when they are not
        // — and would zero them if they were.
        const SENTINEL: i32 = 0x0BAD_F00D_i32;

        let mut log = Log::new();
        initialised(&mut log);

        {
            let meta = AtomicBuffer::from_slice_mut(&mut log.metadata.0).expect("aligned");
            meta.store_i32_relaxed(descriptor::ACTIVE_TERM_COUNT_OFFSET + 4, SENTINEL)
                .expect("in range");
        }

        {
            let appender = log.appender();
            assert!(
                appender.rotate(0, initial_term_id()),
                "the rotation took, whatever the padding holds"
            );
            assert_eq!(Some(1), appender.active_term_count(), "the count moved");
        }

        let meta = AtomicBuffer::from_slice(&log.metadata.0).expect("aligned");
        assert_eq!(
            Some(SENTINEL),
            meta.load_i32(descriptor::ACTIVE_TERM_COUNT_OFFSET + 4),
            "and the bytes it must not touch are where they were"
        );
    }

    #[test]
    fn a_count_that_disagrees_with_its_tail_is_a_retry_not_a_write() {
        // B1's gate. The count names partition 1 while partition 1's tail still
        // carries term 0 — the instant between another producer's tail reset
        // and its count store. Every position computed from that pair is
        // nonsense, so the offer does not happen and nothing is written.
        let mut log = Log::new();
        initialised(&mut log);
        log.set_tail(initial_term_id(), 0, 1);

        let outcome = {
            let appender = log.appender();
            appender.append(11, 22, i64::MAX, b"hello")
        };

        assert_eq!(
            Appended::MidRotation,
            outcome,
            "the caller retries once the rotation has settled"
        );
        assert_eq!(
            (0, 0, Vec::new()),
            log.read_frame(0),
            "and nothing was written"
        );
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
    fn an_exhausted_window_is_reported_rather_than_written_through() {
        let mut log = Log::new();
        log.set_connected(true);

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
    fn a_closed_window_is_distinguished_from_a_slow_subscriber() {
        // Both are "position is at the limit". The reference tells them apart
        // with the log's `is_connected` byte and so does this, because a caller
        // that conflates them waits for a subscriber that does not exist
        // (`aeron-client/src/main/c/aeron_publication.h:76-95`).
        let mut log = Log::new();
        log.set_connected(false);

        let appender = log.appender();
        appender
            .initialise_tails(initial_term_id())
            .then_some(())
            .expect("tails initialised");

        assert_eq!(Some(false), appender.is_connected());
        let outcome = appender.append(11, 22, 0, b"hello");
        assert!(matches!(outcome, Appended::NotConnected), "got {outcome:?}");

        // The same append with the window open goes through, so the difference
        // above is the classification and not the limit.
        let outcome = appender.append(11, 22, i64::MAX, b"hello");
        assert!(matches!(outcome, Appended::Ok { .. }), "got {outcome:?}");
    }

    #[test]
    fn a_payload_of_exactly_one_frame_is_one_whole_frame() {
        // The boundary the fragmentation split sits on: at `max_payload_length`
        // the reference writes one frame flagged BEGIN|END, and one byte more
        // becomes two.
        let mut log = Log::new();
        log.set_connected(true);

        let max_payload = {
            let appender = log.appender();
            appender
                .initialise_tails(initial_term_id())
                .then_some(())
                .expect("tails initialised");

            let max_payload = appender.max_payload_length();
            assert_eq!(
                (descriptor::MTU_LENGTH_DEFAULT - DATA_HEADER_LENGTH as i32) as usize,
                max_payload,
                "one MTU less a data header"
            );

            let outcome = appender.append(11, 22, i64::MAX, &vec![0x11; max_payload]);
            assert!(matches!(outcome, Appended::Ok { .. }), "got {outcome:?}");

            max_payload
        };

        let (length, type_id, payload) = log.read_frame(0);
        assert_eq!(
            max_payload as i32 + DATA_HEADER_LENGTH as i32,
            length,
            "one frame, its own length"
        );
        assert_eq!(TYPE_DATA, type_id);
        assert_eq!(max_payload, payload.len());
    }

    #[test]
    fn a_payload_one_byte_past_one_frame_becomes_two() {
        // The layout the reference produces: payload per frame, BEGIN on the
        // first, END on the last, the cursor advanced by each frame's aligned
        // length while the length recorded stays unaligned.
        let mut log = Log::new();
        log.set_connected(true);

        let max_payload = {
            let appender = log.appender();
            appender
                .initialise_tails(initial_term_id())
                .then_some(())
                .expect("tails initialised");

            let max_payload = appender.max_payload_length();
            let outcome = appender.append(11, 22, i64::MAX, &vec![0x22; max_payload + 1]);
            assert!(matches!(outcome, Appended::Ok { .. }), "got {outcome:?}");

            max_payload
        };

        // First frame: the whole MTU of payload, BEGIN only.
        let (first_length, first_type, first_payload) = log.read_frame(0);
        assert_eq!((max_payload + DATA_HEADER_LENGTH) as i32, first_length);
        assert_eq!(TYPE_DATA, first_type);
        assert_eq!(max_payload, first_payload.len());
        assert_eq!(
            Some(FLAG_BEGIN),
            log.frame_flags(0),
            "the first frame begins a message and does not end one"
        );

        // Second frame: one byte, END only, at the first frame's aligned end.
        let second = position::align_up(first_length, descriptor::FRAME_ALIGNMENT) as usize;
        let (second_length, _, second_payload) = log.read_frame(second);
        assert_eq!(DATA_HEADER_LENGTH as i32 + 1, second_length);
        assert_eq!(vec![0x22], second_payload);
        assert_eq!(
            Some(FLAG_END),
            log.frame_flags(second),
            "and the last frame ends it"
        );

        assert_eq!(
            Some(descriptor::compute_fragmented_length(max_payload + 1, max_payload) as i32),
            log.tail_offset(),
            "the term advanced by exactly what the reservation said"
        );
    }

    #[test]
    fn a_multi_frame_message_lands_where_the_reservation_said() {
        // Three frames, so the middle one is the continuation case: no flags at
        // all. A producer that only got the two-frame case right would still
        // write a layout no reader could follow.
        let mut log = Log::new();
        log.set_connected(true);

        let (max_payload, payload_length) = {
            let appender = log.appender();
            appender
                .initialise_tails(initial_term_id())
                .then_some(())
                .expect("tails initialised");

            let max_payload = appender.max_payload_length();
            let payload_length = max_payload * 2 + 17;
            let payload = vec![0x33; payload_length];
            let outcome = appender.append(11, 22, i64::MAX, &payload);
            assert!(matches!(outcome, Appended::Ok { .. }), "got {outcome:?}");

            (max_payload, payload_length)
        };

        assert_eq!(Some(FLAG_BEGIN), log.frame_flags(0), "first: begins");

        let second = position::align_up(
            (max_payload + DATA_HEADER_LENGTH) as i32,
            descriptor::FRAME_ALIGNMENT,
        ) as usize;
        assert_eq!(
            Some(0),
            log.frame_flags(second),
            "middle: continuation, no flags"
        );

        let third = second
            + position::align_up(
                (max_payload + DATA_HEADER_LENGTH) as i32,
                descriptor::FRAME_ALIGNMENT,
            ) as usize;
        assert_eq!(Some(FLAG_END), log.frame_flags(third), "last: ends");

        let (third_length, _, third_payload) = log.read_frame(third);
        assert_eq!(17, third_payload.len(), "and carries the remainder");
        assert_eq!(DATA_HEADER_LENGTH as i32 + 17, third_length);

        assert_eq!(
            Some(descriptor::compute_fragmented_length(payload_length, max_payload) as i32),
            log.tail_offset()
        );
    }

    #[test]
    fn a_fragmented_message_that_does_not_fit_the_term_is_not_split_across_it() {
        // The all-or-nothing rule: the reservation is the whole message, so a
        // message whose frames would straddle a term boundary is not written at
        // all — the term is padded and the caller retries in the next one
        // (`aeron_publication.c:251-262`).
        let mut log = Log::new();
        log.set_connected(true);
        initialised(&mut log);

        // Room for one frame and a bit, which is not room for a two-frame
        // message.
        let max_payload = {
            let appender = log.appender();
            appender.max_payload_length()
        };
        log.set_tail(
            initial_term_id(),
            TERM_LENGTH - (max_payload as i32 + 64),
            0,
        );

        let outcome = {
            let appender = log.appender();
            appender.append(11, 22, i64::MAX, &vec![0x44; max_payload + 1])
        };

        assert_eq!(Appended::EndOfLog, outcome);

        let (padding_length, type_id, _) =
            log.read_frame((TERM_LENGTH - (max_payload as i32 + 64)) as usize);
        assert_eq!(TYPE_PAD, type_id);
        assert_eq!(max_payload as i32 + 64, padding_length);
    }

    #[test]
    fn a_payload_beyond_max_message_length_is_refused_as_the_reference_does() {
        let mut log = Log::new();
        log.set_connected(true);

        let appender = log.appender();
        appender
            .initialise_tails(initial_term_id())
            .then_some(())
            .expect("tails initialised");

        // Beyond `term_length / 8`, which for a 64 KiB term is 8 KiB — and
        // therefore also beyond what one frame holds. That both bounds are
        // exceeded is the point: the message cap is checked first, before any
        // question of how the payload would be split
        // (`aeron_publication.c:515-524`, before the append).
        assert_eq!(8192, position::max_message_length(TERM_LENGTH));
        assert!(8192 > appender.max_payload_length());

        let oversized = vec![0u8; position::max_message_length(TERM_LENGTH) as usize + 1];
        let outcome = appender.append(11, 22, i64::MAX, &oversized);
        assert!(
            matches!(outcome, Appended::MessageTooLarge),
            "got {outcome:?}"
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
