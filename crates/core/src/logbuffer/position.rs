//! The three ways a place in a stream is named.
//!
//! All three are 64-bit, and two of them are *not* interchangeable:
//!
//! - A **raw tail** — [`RawTail`] — is what one of the three tail counters
//!   holds: `(term_id << 32) | term_offset`, an **absolute** term id.
//! - A **stream position** — [`Position`] — is a linear byte offset:
//!   `(term_count << shift) + term_offset`, a **count** of terms since the
//!   log's `initial_term_id`.
//! - The low 32 bits of either, which is an offset within a term.
//!
//! They are **never numerically equal**, whatever the initial term id: a raw
//! tail splits its bits 32/32 around a term *id*, and a position shifts by
//! `log2(term_length)` around a term *count*. Two layouts, two meanings, one
//! underlying type — which is exactly why the reference's comments have to
//! keep reminding the reader which one a value is
//! (`aeron-client/src/main/c/concurrent/aeron_logbuffer_descriptor.h:112-168`),
//! and why this module has two types instead.
//!
//! The `initial_term_id` matters for a second reason: it is randomised at
//! creation so a stream is not reused by accident
//! (`LogBufferDescriptor.java:510-511`), so a term id and a term count are
//! unrelated numbers in general.
//!
//! Everything here is arithmetic on values. Nothing touches memory.

use super::descriptor;

/// What a term's tail counter holds.
///
/// A wrapper over the `i64` the counter stores, not a position. Convert with
/// [`RawTail::term_offset`] and [`Position::new`], never by casting.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RawTail(i64);

/// A linear byte offset from the start of a stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Position(i64);

impl RawTail {
    /// Pack a term id and an offset, as the reference's `packTail` does
    /// (`LogBufferDescriptor.java:960-963`).
    ///
    /// The offset occupies the low 32 bits *unmasked*, exactly as the reference
    /// leaves it. That is safe only because an offset never reaches 2^31 — a
    /// term is at most 1 GiB — and the assertion below says so.
    pub const fn new(term_id: i32, term_offset: i32) -> Self {
        debug_assert!(
            term_offset >= 0 && term_offset <= descriptor::TERM_MAX_LENGTH,
            "an offset outside [0, term_length] would spill into the term id"
        );

        Self(((term_id as i64) << 32) | (term_offset as u32 as i64))
    }

    /// The stored value, for handing to an atomic.
    pub const fn raw(self) -> i64 {
        self.0
    }

    /// Rebuild from a value an atomic returned.
    ///
    /// No validation: the low 32 bits may be anything a producer left there,
    /// which is why [`RawTail::term_offset`] saturates.
    pub const fn from_raw(raw: i64) -> Self {
        Self(raw)
    }

    /// The absolute term id.
    ///
    /// An arithmetic shift, so a negative term id comes back negative — the
    /// reference does the same on both sides (`:120`, `LogBufferDescriptor.java:925`).
    pub const fn term_id(self) -> i32 {
        (self.0 >> 32) as i32
    }

    /// The low 32 bits, without saturation.
    ///
    /// What `termOffset(rawTail)` returns in the Java reference
    /// (`:948-951`), and what the exclusive publication uses directly.
    pub const fn raw_term_offset(self) -> i64 {
        self.0 & 0xFFFF_FFFF
    }

    /// The offset within the term, **saturating at `term_length`**.
    ///
    /// The saturation value is the sentinel meaning "this term is full, rotate";
    /// it is `term_length` and not `term_length - 1`, and that is load-bearing
    /// (`:112-116`, `LogBufferDescriptor.java:935-940`).
    pub const fn term_offset(self, term_length: i32) -> i32 {
        let offset = self.raw_term_offset();
        if offset < term_length as i64 {
            offset as i32
        } else {
            term_length
        }
    }

    /// The same tail with a new offset, keeping the term id.
    pub const fn with_term_offset(self, term_offset: i32) -> Self {
        Self::new(self.term_id(), term_offset)
    }
}

impl Position {
    /// A position from a term count and an offset.
    pub const fn from_term_count(term_count: i32, term_offset: i32, bits_to_shift: u32) -> Self {
        Self(((term_count as i64) << bits_to_shift) + term_offset as i64)
    }

    /// A position from a **term id** and an offset.
    ///
    /// The term id is absolute; the position counts from `initial_term_id`.
    /// Confusing the two is the bug this module exists to prevent.
    pub const fn new(
        term_id: i32,
        term_offset: i32,
        bits_to_shift: u32,
        initial_term_id: i32,
    ) -> Self {
        Self::from_term_count(
            term_count(term_id, initial_term_id),
            term_offset,
            bits_to_shift,
        )
    }

    /// The stored value.
    pub const fn raw(self) -> i64 {
        self.0
    }

    /// Rebuild from a stored value.
    pub const fn from_raw(value: i64) -> Self {
        Self(value)
    }

    /// The term count, from the log's initial term id.
    pub const fn term_count(self, bits_to_shift: u32) -> i32 {
        (self.0 >> bits_to_shift) as i32
    }

    /// The absolute term id this position falls in.
    ///
    /// Wrapping addition, matching the reference's `computeTermIdFromPosition`
    /// (`:157-161`): a term id is an `i32` that rolls over.
    pub const fn term_id(self, bits_to_shift: u32, initial_term_id: i32) -> i32 {
        initial_term_id.wrapping_add(self.term_count(bits_to_shift))
    }

    /// The offset within its term.
    ///
    /// The mask is computed in 64 bits. The reference writes `1u << bits` — a
    /// *32-bit* literal (`:165`) — which is undefined behaviour at a shift of
    /// 32 or more and happens to be safe only because a term caps at 1 GiB.
    pub const fn term_offset(self, bits_to_shift: u32) -> i32 {
        let mask = (1u64 << bits_to_shift) - 1;
        (self.0 & mask as i64) as i32
    }

    /// Where this position's term begins.
    ///
    /// Copes with a negative term count on rollover, as the reference notes
    /// (`:151-155`).
    pub const fn term_begin(self, bits_to_shift: u32) -> Self {
        Self::from_term_count(self.term_count(bits_to_shift), 0, bits_to_shift)
    }

    /// Which of the three partitions this position falls in.
    ///
    /// The shift is **unsigned**, following the Java reference
    /// (`LogBufferDescriptor.java:766`). The C one shifts arithmetically and
    /// then casts a possibly-negative remainder to `size_t`, which for a
    /// negative position yields an enormous index — a latent out-of-bounds
    /// read rather than a wrong answer (`:130`). Follow Java.
    pub const fn index(self, bits_to_shift: u32) -> usize {
        (((self.0 as u64) >> bits_to_shift) % descriptor::PARTITION_COUNT as u64) as usize
    }
}

/// The term count implied by a term id.
///
/// Wrapping, because a term id rolls over as an `i32` and the difference is
/// meaningful across the rollover (`util/aeron_math.h:22-38`).
pub const fn term_count(term_id: i32, initial_term_id: i32) -> i32 {
    term_id.wrapping_sub(initial_term_id)
}

/// Which partition a term count falls in.
///
/// The reference truncates, so a negative term count yields a negative index
/// and its `size_t` cast makes it enormous. A negative count is a caller bug
/// either way; this returns a usable index and asserts in debug builds rather
/// than turning the bug into an out-of-bounds read.
pub const fn index_by_term_count(term_count: i32) -> usize {
    debug_assert!(term_count >= 0, "a term count is not negative");
    term_count.rem_euclid(descriptor::PARTITION_COUNT as i32) as usize
}

/// Which partition a term id falls in.
pub const fn index_by_term(initial_term_id: i32, term_id: i32) -> usize {
    index_by_term_count(term_count(term_id, initial_term_id))
}

/// `log2(term_length)`, or `None` if the length is not usable.
///
/// A term length must be a power of two in `[64 KiB, 1 GiB]`
/// (`aeron_logbuffer_descriptor.c:185-215`). The bounds are not decoration:
/// the maximum is what keeps a term id inside an `i32` once positions are
/// shifted, and the minimum is what keeps the metadata's frame header from
/// dominating a term.
pub const fn bits_to_shift(term_length: i32) -> Option<u32> {
    if term_length < descriptor::TERM_MIN_LENGTH
        || term_length > descriptor::TERM_MAX_LENGTH
        || !(term_length as u32).is_power_of_two()
    {
        return None;
    }

    Some(term_length.trailing_zeros())
}

/// Whether a term length is one the reference would accept.
pub const fn is_term_length_valid(term_length: i32) -> bool {
    bits_to_shift(term_length).is_some()
}

/// The largest a single message may be for a given term length.
///
/// `min(term_length / 8, 16 MiB)` (`FrameDescriptor.java:124-127`). The cap
/// binds at 128 MiB and above, where `term_length / 8` would be larger.
pub const fn max_message_length(term_length: i32) -> i32 {
    let eighth = term_length >> 3;
    if eighth < descriptor::MAX_MESSAGE_LENGTH {
        eighth
    } else {
        descriptor::MAX_MESSAGE_LENGTH
    }
}

/// The largest position a stream may reach.
///
/// `term_length << 31` (`aeron_publication.c:68`). It exists to keep the term
/// count inside an `i32`; a reader that ignores it will see positions that
/// cannot be decoded.
pub const fn max_possible_position(term_length: i32) -> i64 {
    (term_length as i64) << 31
}

/// Round `value` up to the next multiple of `alignment`, a power of two.
///
/// The same computation as `deepmsg_cnc::layout::align_up`. Deliberately not
/// shared: `cnc` depends on `core`, not the other way round, and four lines do
/// not justify a third crate.
pub const fn align_up(value: i32, alignment: i32) -> i32 {
    (value + (alignment - 1)) & !(alignment - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A term length and its shift, the pair the arithmetic needs.
    const TERM_LENGTH: i32 = 64 * 1024;
    const BITS: u32 = 16;

    #[test]
    fn the_two_encodings_differ_where_it_matters() {
        // The whole reason for two types. With a non-zero initial term id --
        // which the reference randomises -- a raw tail and a position for the
        // same place are different numbers, and only one of them is what a
        // tail counter holds.
        let initial_term_id = 7;
        let term_id = 9;
        let term_offset = 128;

        let tail = RawTail::new(term_id, term_offset);
        let position = Position::new(term_id, term_offset, BITS, initial_term_id);

        assert_eq!(9, tail.term_id());
        assert_eq!(2, term_count(term_id, initial_term_id));
        assert_eq!(2 << BITS, position.raw() - i64::from(term_offset));
        assert_ne!(
            tail.raw(),
            position.raw(),
            "a raw tail and a position coincide only when initial_term_id is 0"
        );
    }

    #[test]
    fn the_two_encodings_use_different_layouts_and_are_never_the_same_number() {
        // A raw tail splits 32/32; a position shifts by log2(term_length). This
        // was wrong in an earlier draft of this module -- an inherited claim
        // that the two coincide when the initial term id is zero. They do not
        // coincide for *any* value, because the layouts differ.
        let tail = RawTail::new(3, 64);
        let position = Position::new(3, 64, BITS, 0);

        assert_eq!((3i64 << 32) | 64, tail.raw());
        assert_eq!((3i64 << BITS) + 64, position.raw());
        assert_ne!(tail.raw(), position.raw());

        // What agrees is what they *mean*: the same place in the same term.
        assert_eq!(tail.term_id(), position.term_id(BITS, 0));
        assert_eq!(tail.term_offset(TERM_LENGTH), position.term_offset(BITS));
    }

    #[test]
    fn the_offset_saturates_at_the_term_length_not_one_before_it() {
        // `term_length` is the rotation sentinel. Saturating at
        // `term_length - 1` would either never rotate or rotate a byte early.
        assert_eq!(
            TERM_LENGTH,
            RawTail::new(1, TERM_LENGTH).term_offset(TERM_LENGTH)
        );
        assert_eq!(
            TERM_LENGTH,
            RawTail::new(1, TERM_LENGTH + 5000).term_offset(TERM_LENGTH),
            "an over-shot tail still reads as full, not as a wrapped offset"
        );
        assert_eq!(
            TERM_LENGTH - 1,
            RawTail::new(1, TERM_LENGTH - 1).term_offset(TERM_LENGTH)
        );
    }

    #[test]
    fn a_negative_term_id_survives_the_round_trip() {
        // Term ids are `i32` and roll over, so negative ones are ordinary.
        let tail = RawTail::new(-1, 32);
        assert_eq!(-1, tail.term_id());
        assert_eq!(32, tail.term_offset(TERM_LENGTH));

        // And the shift that unpacks it is arithmetic, not logical: a logical
        // shift here would turn -1 into a large positive term id.
        assert_eq!(-1i64, tail.raw() >> 32);
    }

    #[test]
    fn a_position_round_trips_through_its_parts() {
        let initial_term_id = 42;
        let term_id = 45;
        let offset = 9 * descriptor::FRAME_ALIGNMENT;

        let position = Position::new(term_id, offset, BITS, initial_term_id);

        assert_eq!(3, position.term_count(BITS));
        assert_eq!(term_id, position.term_id(BITS, initial_term_id));
        assert_eq!(offset, position.term_offset(BITS));
        assert_eq!(
            Position::new(term_id, 0, BITS, initial_term_id),
            position.term_begin(BITS)
        );
    }

    #[test]
    fn a_term_begin_position_is_where_the_term_starts() {
        let initial = 100;
        let position = Position::new(103, 4096, BITS, initial);

        assert_eq!(3 << BITS, position.term_begin(BITS).raw());
    }

    #[test]
    fn the_index_is_unsigned_shifted_so_a_negative_position_stays_in_range() {
        // The C reference shifts arithmetically and casts a negative remainder
        // to size_t, producing an enormous index. Java shifts unsigned and
        // stays in range. Only one of those is a usable answer.
        for position in [-1i64, -4096, 0, 4096, i64::MAX] {
            let index = Position::from_raw(position).index(BITS);
            assert!(
                index < descriptor::PARTITION_COUNT,
                "position {position} produced index {index}"
            );
        }

        assert_eq!(0, Position::from_raw(0).index(BITS));
        assert_eq!(1, Position::from_raw(1i64 << BITS).index(BITS));
        assert_eq!(2, Position::from_raw(2i64 << BITS).index(BITS));
        assert_eq!(0, Position::from_raw(3i64 << BITS).index(BITS), "it wraps");
    }

    #[test]
    fn term_counts_and_ids_index_the_same_partition() {
        let initial = 5;
        for term_id in [5, 6, 7, 8, 9, 10] {
            assert_eq!(
                index_by_term(initial, term_id),
                index_by_term_count(term_count(term_id, initial)),
                "term id {term_id} disagrees with its count"
            );
        }

        assert_eq!(0, index_by_term(0, 0));
        assert_eq!(1, index_by_term(0, 1));
        assert_eq!(2, index_by_term(0, 2));
        assert_eq!(0, index_by_term(0, 3));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "a term count is not negative")]
    fn a_negative_term_count_is_a_caller_bug() {
        // Both references produce nonsense here: C casts a negative remainder
        // to `size_t` and indexes far out of bounds, Java keeps the negative
        // and crashes. Ours is loud in debug and, in release, still returns a
        // valid index rather than an out-of-bounds one.
        let _ = index_by_term_count(-1);
    }

    #[test]
    fn term_lengths_are_powers_of_two_in_range() {
        assert!(is_term_length_valid(64 * 1024));
        assert!(is_term_length_valid(16 * 1024 * 1024));
        assert!(is_term_length_valid(1024 * 1024 * 1024));

        assert!(!is_term_length_valid(64 * 1024 - 1), "below the minimum");
        assert!(!is_term_length_valid(i32::MAX), "above the maximum");
        assert!(!is_term_length_valid(100_000), "not a power of two");
        assert!(!is_term_length_valid(0));
        assert!(!is_term_length_valid(-65536), "negative");

        assert_eq!(Some(16), bits_to_shift(64 * 1024));
        assert_eq!(Some(30), bits_to_shift(1024 * 1024 * 1024));
        assert_eq!(None, bits_to_shift(0));
    }

    #[test]
    fn the_message_cap_binds_above_128_mib() {
        // `min(term_length / 8, 16 MiB)`: the division wins below the crossover
        // and the cap wins above it. A port that drops the cap is right for
        // every term length anyone uses until it is not.
        assert_eq!(8 * 1024, max_message_length(64 * 1024));
        assert_eq!(
            descriptor::MAX_MESSAGE_LENGTH,
            max_message_length(128 * 1024 * 1024),
            "at the crossover the two are equal"
        );
        assert_eq!(
            descriptor::MAX_MESSAGE_LENGTH,
            max_message_length(1024 * 1024 * 1024),
            "and above it the cap binds"
        );
    }

    #[test]
    fn alignment_rounds_up_to_the_frame_boundary() {
        // Frames are 32-byte aligned: a data header is exactly one unit, so no
        // frame is smaller than the alignment.
        assert_eq!(32, align_up(1, 32));
        assert_eq!(32, align_up(32, 32));
        assert_eq!(64, align_up(33, 32));
        assert_eq!(64, align_up(64, 32));
        assert_eq!(0, align_up(0, 32));
    }

    #[test]
    fn a_frame_length_is_never_split_by_aligning_the_tail() {
        // The tail advances by the *aligned* length while the header records
        // the unaligned one; the two must agree on where the next frame starts.
        for payload in [0, 1, 31, 32, 33, 100] {
            let frame_length = payload + 32; // a data header
            let aligned = align_up(frame_length, descriptor::FRAME_ALIGNMENT);
            assert!(aligned >= frame_length);
            assert_eq!(0, aligned % descriptor::FRAME_ALIGNMENT);
            assert!(aligned - frame_length < descriptor::FRAME_ALIGNMENT);
        }
    }
}
