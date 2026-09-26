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
}
