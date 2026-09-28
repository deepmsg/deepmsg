//! Walking frames as a reader does.
//!
//! Mirrors the read loop of `aeron-client/src/main/c/aeron_image.c:246-325`,
//! which is short because the frame lengths do the work: a reader steps from
//! frame to frame by each frame's own length and never keeps an index of its
//! own.
//!
//! Two facts make that work, and both are easy to get wrong:
//!
//! - **A length of `0` or less ends the scan.** Zero means nothing is here —
//!   never written, or cleaned after consumption. Negative means a producer has
//!   claimed the frame and is filling it. A reader cannot tell those apart and
//!   does not need to; either way it stops.
//! - **The header stores the unaligned length; the reader steps by the aligned
//!   one.** A 100-byte payload is a 132-byte frame occupying 160 bytes.

use crate::buffer::{AtomicBuffer, ReadOnly};

use super::descriptor;
use super::frame::{DATA_HEADER_LENGTH, Frame};
use super::position::align_up;

/// What a scanner found at its cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// A data frame, complete and published.
    Data {
        /// Where it starts in the term.
        offset: usize,
        /// Its total length, header included.
        frame_length: i32,
    },
    /// A padding frame: the term's tail, or a repaired hole. A reader skips it.
    Padding {
        /// Where it starts.
        offset: usize,
        /// How much it covers.
        frame_length: i32,
    },
    /// A frame that is claimed but not published, or not written at all. The
    /// scan stops here; the caller waits and resumes from the same offset.
    NotReady {
        /// Where the unfinished frame is.
        offset: usize,
    },
    /// A length the writer could not have produced. The next frame's position
    /// is unknowable, so the scan stops.
    Malformed {
        /// Where it happened.
        offset: usize,
        /// What the header claimed.
        frame_length: i32,
    },
    /// The scanner reached the end of its window — the term's end, or a limit
    /// the caller set.
    End,
}

/// A forward walk over one term.
pub struct Scanner<'a, Access = ReadOnly> {
    term: &'a AtomicBuffer<'a, Access>,
    term_length: usize,
    /// How far this scanner may read. The reference bounds a reader by the term
    /// end, and clamps further when the caller supplies a flow-control limit.
    limit: usize,
    offset: usize,
}

impl<'a, Access> Scanner<'a, Access> {
    /// A scanner starting at the beginning of a term.
    pub fn new(term: &'a AtomicBuffer<'a, Access>, term_length: usize) -> Self {
        Self {
            term,
            term_length,
            limit: term_length,
            offset: 0,
        }
    }

    /// A scanner starting at `offset`.
    pub fn at(term: &'a AtomicBuffer<'a, Access>, term_length: usize, offset: usize) -> Self {
        Self {
            term,
            term_length,
            limit: term_length,
            offset,
        }
    }

    /// Move the cursor without reading.
    pub fn seek(&mut self, offset: usize) {
        self.offset = offset;
    }

    /// Stop reading at `limit`, inclusive of the term end.
    pub fn set_limit(&mut self, limit: usize) {
        self.limit = limit.min(self.term_length);
    }

    /// Where the cursor is.
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// How far this scanner may read.
    pub const fn limit(&self) -> usize {
        self.limit
    }

    /// Advance one frame.
    pub fn advance(&mut self) -> Step {
        let offset = self.offset;
        if offset >= self.limit {
            return Step::End;
        }

        let frame = Frame::new(self.term, offset);
        let Some(frame_length) = frame.frame_length() else {
            return Step::Malformed {
                offset,
                frame_length: 0,
            };
        };

        // `0` is "nothing here" and a negative is "a claim in flight". Both stop
        // a scan, and neither is an error.
        if frame_length <= 0 {
            return Step::NotReady { offset };
        }

        // A length the writer could not have produced. Computing the next offset
        // from it would be inventing a position, so the scan stops instead —
        // a deliberate divergence: the reference propagates it and aligns a
        // value that may be negative.
        let largest = descriptor::MAX_MESSAGE_LENGTH + DATA_HEADER_LENGTH as i32;
        if (frame_length as usize) < DATA_HEADER_LENGTH
            || frame_length > largest.min(self.term_length as i32)
        {
            return Step::Malformed {
                offset,
                frame_length,
            };
        }

        let aligned = frame
            .aligned_length()
            .expect("a positive length has an aligned length");

        // A frame never crosses a term boundary: a producer that could not fit
        // one padded instead. Reaching past the end means the header is wrong.
        if offset + aligned > self.term_length {
            return Step::Malformed {
                offset,
                frame_length,
            };
        }

        self.offset = offset + aligned;

        if frame.is_padding() {
            Step::Padding {
                offset,
                frame_length,
            }
        } else {
            Step::Data {
                offset,
                frame_length,
            }
        }
    }
}

/// The sender's question: how much of the term, from here, is *published* and
/// sendable?
///
/// Mirrors `aeron_term_scanner_scan_for_availability`
/// (`aeron-client/src/main/c/concurrent/aeron_term_scanner.h:26-63`), which the
/// sender calls once per datagram it wants to put on the wire
/// (`aeron-driver/src/main/c/aeron_network_publication.c:525`).
///
/// # Why the sender needs its own scan
///
/// A reader stops at the first unpublished frame and waits. A *sender* has to
/// do something more delicate: it may send several frames in one datagram, it
/// has to know when the datagram budget cuts a frame in half (and stop before
/// it), and it has to carry a padding frame's **header** into the next datagram
/// so the receiver can see the term's tail rather than a hole. That is the
/// whole of the difference from [`Scanner`], and it is why the answer is three
/// cases rather than a walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Availability {
    /// `available` bytes from the cursor are published and sendable, and
    /// `padding` of them belong to a padding frame past its own header — which
    /// the sender includes, because a receiver that sees no padding frame sees
    /// a gap.
    Ready {
        /// How many bytes may go into this datagram.
        available: i32,
        /// How much of `available` is a padding frame's tail.
        padding: i32,
    },
    /// The budget ran out in the middle of a frame: the sender stops here and
    /// comes back with the rest of the window. The reference reports this as a
    /// negative count.
    Limited {
        /// The frame that did not fit, as a negative length — the reference's
        /// own answer, kept so that a caller can tell a full frame from a
        /// partial one.
        frame_length: i32,
    },
    /// Nothing published at the cursor. The sender waits.
    Empty,
}

/// Scan from `offset` for what a sender may put in one datagram, bounded by
/// `max_length` (the datagram budget) and `term_length_left` (the term's end).
///
/// The lengths in `term_length_left` and `max_length` are the reference's
/// `int32_t`s and the answer is in the same units, so a caller comparing them
/// against frame lengths is comparing like with like.
pub fn scan_for_availability(
    term: &AtomicBuffer<'_, ReadOnly>,
    offset: usize,
    term_length_left: i32,
    max_length: i32,
) -> Availability {
    let limit = max_length.min(term_length_left);
    let mut available: i32 = 0;
    let mut padding: i32 = 0;

    loop {
        // SAFETY-adjacent: `Frame::new` reads within the term, and the cursor
        // never passes `limit`, which is at most `term_length_left`.
        let frame = Frame::new(term, offset + available.unsigned_abs() as usize);

        let Some(frame_length) = frame.frame_length() else {
            break;
        };

        // `0` is "not written yet", negative is "claimed and being filled";
        // either way there is nothing to send from here.
        if frame_length <= 0 {
            break;
        }

        let mut aligned = align_up(frame_length, descriptor::FRAME_ALIGNMENT);

        // A padding frame is sent as its header alone: the tail is not data,
        // and the receiver has to see *something* at that offset.
        if frame.is_padding() {
            padding = aligned - DATA_HEADER_LENGTH as i32;
            aligned = DATA_HEADER_LENGTH as i32;
        }

        available += aligned;

        if available > limit {
            // A frame that does not fit the datagram: unless it is the only
            // one, the frames before it are still sendable.
            available = if aligned == available {
                -available
            } else {
                available - aligned
            };
            padding = 0;
            break;
        }

        if padding != 0 || available >= limit {
            break;
        }
    }

    match available {
        available if available > 0 => Availability::Ready { available, padding },
        available if available < 0 => Availability::Limited {
            frame_length: available,
        },
        _ => Availability::Empty,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::ReadWrite;
    use crate::logbuffer::frame::{FLAG_UNFRAGMENTED, FRAME_LENGTH_OFFSET, TYPE_DATA, TYPE_PAD};

    #[repr(align(64))]
    struct Term<const N: usize>([u8; N]);

    const TERM_LENGTH: usize = 1024;

    fn term() -> Term<TERM_LENGTH> {
        Term([0u8; TERM_LENGTH])
    }

    /// Write a complete frame the way a producer would, and return its aligned
    /// length.
    fn write_frame(
        term: &AtomicBuffer<'_, ReadWrite>,
        offset: usize,
        type_id: i16,
        payload: &[u8],
    ) -> usize {
        let frame = Frame::new(term, offset);
        let length = payload.len() as i32 + DATA_HEADER_LENGTH as i32;

        frame
            .begin(length, FLAG_UNFRAGMENTED, type_id, offset as i32, 1, 2, 3)
            .expect("in range");
        frame.write_payload(payload).expect("in range");
        frame.publish(length).expect("in range");

        crate::logbuffer::position::align_up(length, descriptor::FRAME_ALIGNMENT) as usize
    }

    #[test]
    fn an_empty_term_stops_immediately() {
        let bytes = term();
        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned");
        let mut scanner = Scanner::new(&view, TERM_LENGTH);

        assert_eq!(Step::NotReady { offset: 0 }, scanner.advance());
    }

    #[test]
    fn walks_two_frames_and_then_stops() {
        let mut bytes = term();

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
            let first = write_frame(&writer, 0, TYPE_DATA, b"hello");
            let _ = write_frame(&writer, first, TYPE_DATA, b"world!");
        }

        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned");
        let mut scanner = Scanner::new(&view, TERM_LENGTH);

        let first = scanner.advance();
        assert!(matches!(first, Step::Data { offset: 0, .. }));
        let second = scanner.advance();
        assert!(matches!(second, Step::Data { .. }));
        assert!(scanner.offset() > 0);
        assert_eq!(
            Step::NotReady {
                offset: scanner.offset()
            },
            scanner.advance(),
            "the unwritten remainder ends the scan"
        );
    }

    #[test]
    fn a_padding_frame_is_reported_as_one() {
        let mut bytes = term();

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
            let frame = Frame::new(&writer, 0);
            frame
                .begin(64, FLAG_UNFRAGMENTED, TYPE_DATA, 0, 1, 2, 3)
                .expect("in range");
            frame.set_type(TYPE_PAD).expect("in range");
            frame.publish(64).expect("in range");
        }

        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned");
        let mut scanner = Scanner::new(&view, TERM_LENGTH);

        assert_eq!(
            Step::Padding {
                offset: 0,
                frame_length: 64
            },
            scanner.advance()
        );
    }

    #[test]
    fn a_frame_claimed_but_not_published_stops_the_scan() {
        // The negative-length protocol, seen from a reader: it cannot tell this
        // from an unwritten frame, and it does not need to.
        let mut bytes = term();

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
            let frame = Frame::new(&writer, 0);
            frame
                .begin(64, FLAG_UNFRAGMENTED, TYPE_DATA, 0, 1, 2, 3)
                .expect("in range");
            // Deliberately no publish: the header is still in flight.
        }

        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned");
        let mut scanner = Scanner::new(&view, TERM_LENGTH);

        assert_eq!(Step::NotReady { offset: 0 }, scanner.advance());
    }

    #[test]
    fn a_length_the_writer_could_not_have_produced_is_refused() {
        for hostile in [1i32, 8, 31, i32::MAX] {
            let mut bytes = term();

            {
                let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
                // Under the minimum frame length, or past anything a term can
                // hold — written directly, because `begin` would not.
                writer
                    .store_i32_release(FRAME_LENGTH_OFFSET, hostile)
                    .expect("in range");
            }

            let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned");
            let mut scanner = Scanner::new(&view, TERM_LENGTH);

            assert!(
                matches!(scanner.advance(), Step::Malformed { .. }),
                "length {hostile} should be refused"
            );
        }
    }

    #[test]
    fn a_frame_that_would_cross_the_term_end_is_refused() {
        // A producer pads rather than let a frame cross, so a header claiming
        // one is wrong and the next position is unknowable.
        let mut bytes = term();

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
            let offset = TERM_LENGTH - 32;
            let length = 256i32;
            let frame = Frame::new(&writer, offset);
            frame
                .begin(length, FLAG_UNFRAGMENTED, TYPE_DATA, offset as i32, 1, 2, 3)
                .expect("in range");
            frame.publish(length).expect("in range");
        }

        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned");
        let mut scanner = Scanner::at(&view, TERM_LENGTH, TERM_LENGTH - 32);

        assert!(matches!(scanner.advance(), Step::Malformed { .. }));
    }

    #[test]
    fn the_limit_stops_the_scan_before_the_term_does() {
        let mut bytes = term();

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
            write_frame(&writer, 0, TYPE_DATA, b"one");
            write_frame(&writer, 64, TYPE_DATA, b"two");
        }

        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned");
        let mut scanner = Scanner::new(&view, TERM_LENGTH);
        scanner.set_limit(64);

        assert!(matches!(scanner.advance(), Step::Data { offset: 0, .. }));
        assert_eq!(
            Step::End,
            scanner.advance(),
            "the limit is where a reader stops"
        );
    }

    /// A term whose first `count` frames are `payloads`, with a padding frame
    /// closing the tail at `pad_at` (offset) when one is asked for.
    ///
    /// The vectors are the reference's own
    /// (`aeron-driver/src/test/c/aeron_term_scanner_test.cpp:34-200`), which is
    /// the only place its scanner's edge cases are written down.
    #[test]
    fn availability_scans_the_frames_the_reference_scans() {
        let mut bytes = term();

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
            // One frame of a 33-byte payload: 33 + 32 = 65, aligned to 96.
            assert_eq!(96, write_frame(&writer, 0, TYPE_DATA, &[0u8; 33]));
        }

        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned");

        // The whole frame fits the datagram budget.
        assert_eq!(
            Availability::Ready {
                available: 96,
                padding: 0
            },
            scan_for_availability(&view, 0, TERM_LENGTH as i32, 1408)
        );

        // One byte less than the frame, and nothing is sendable: the reference
        // answers with the negated *aligned* length.
        assert_eq!(
            Availability::Limited { frame_length: -96 },
            scan_for_availability(&view, 0, TERM_LENGTH as i32, 95)
        );

        // Nothing written is nothing to send.
        assert_eq!(
            Availability::Empty,
            scan_for_availability(&view, 96, TERM_LENGTH as i32 - 96, 1408)
        );
    }

    #[test]
    fn availability_takes_two_frames_that_fit_and_stops_at_one_that_does_not() {
        let mut bytes = term();

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
            // Two 100-byte payloads: 132 → 160 aligned each.
            assert_eq!(160, write_frame(&writer, 0, TYPE_DATA, &[0u8; 100]));
            assert_eq!(160, write_frame(&writer, 160, TYPE_DATA, &[0u8; 100]));
        }

        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned");

        assert_eq!(
            Availability::Ready {
                available: 320,
                padding: 0
            },
            scan_for_availability(&view, 0, TERM_LENGTH as i32, 1408),
            "both fit"
        );

        // A budget that cuts the second frame in half sends only the first.
        assert_eq!(
            Availability::Ready {
                available: 160,
                padding: 0
            },
            scan_for_availability(&view, 0, TERM_LENGTH as i32, 200)
        );

        // And a budget smaller than the first frame sends nothing.
        assert_eq!(
            Availability::Limited { frame_length: -160 },
            scan_for_availability(&view, 0, TERM_LENGTH as i32, 159)
        );
    }

    #[test]
    fn availability_carries_a_padding_frame_as_its_header_alone() {
        let mut bytes = term();

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
            // A 64-byte payload (96 aligned) and then the term's tail as a
            // padding frame of 128 aligned bytes.
            assert_eq!(96, write_frame(&writer, 0, TYPE_DATA, &[0u8; 64]));
            assert_eq!(128, write_frame(&writer, 96, TYPE_PAD, &[0u8; 96]));
        }

        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned");

        assert_eq!(
            Availability::Ready {
                available: 96 + DATA_HEADER_LENGTH as i32,
                padding: 128 - DATA_HEADER_LENGTH as i32,
            },
            scan_for_availability(&view, 0, TERM_LENGTH as i32, 1408),
            "the padding frame's header is sent, its tail is not"
        );
    }

    #[test]
    fn availability_stops_at_the_terms_end() {
        let mut bytes = term();

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
            // A frame right at the end of the term, and the same frame one
            // term's end earlier with less room than it needs.
            let last = TERM_LENGTH - 96;
            assert_eq!(96, write_frame(&writer, last, TYPE_DATA, &[0u8; 64]));
        }

        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned");

        assert_eq!(
            Availability::Ready {
                available: 96,
                padding: 0
            },
            scan_for_availability(&view, TERM_LENGTH - 96, 96, 1408),
            "a frame that ends exactly at the term's end is sendable"
        );

        assert_eq!(
            Availability::Limited { frame_length: -96 },
            scan_for_availability(&view, TERM_LENGTH - 96, 95, 1408),
            "one that would run past it is not"
        );
    }
}
