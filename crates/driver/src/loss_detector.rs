//! Finding the holes in a term, and asking for them back.
//!
//! Mirrors `aeron-driver/src/main/c/aeron_loss_detector.c` and the gap scanner
//! it is built on (`aeron-client/src/main/c/concurrent/aeron_term_gap_scanner.h`).
//! Two questions, and they are different questions:
//!
//! * **How far can a reader go?** Up to the start of the first hole — a reader
//!   that is moved past a hole has silently skipped data, which for a trading
//!   system is worse than being slow.
//! * **What should be asked for?** The hole itself, once it has stopped
//!   growing, so that asking does not fire for every packet of a burst that is
//!   merely out of order.
//!
//! # The delay is the second question's answer
//!
//! A NAK cannot be sent the instant a hole appears: packets arrive out of order
//! all the time, and a receiver that asked immediately would ask for data that
//! was already in flight — and then, when it arrived, the retransmission would
//! arrive too, doubling the traffic for every hiccup. So a gap that has just
//! appeared is *activated* with an expiry, and only asked for when the expiry
//! passes with the gap still there. A second pass over the same gap re-arms the
//! timer rather than asking again (`gaps_match`,
//! `aeron_loss_detector.h:96-101`).
//!
//! Unicast's delays are a microsecond and then a hundred
//! (`AERON_NAK_UNICAST_DELAY_NS_MIN` and the retry ratio,
//! `aeron-driver/src/main/c/aeron_driver_context.c:222-224`) — loopback-speed
//! numbers, because a unicast receiver has one peer and asking again is cheap.

use deepmsg_core::buffer::{AtomicBuffer, ReadOnly};
use deepmsg_core::logbuffer::{descriptor, position};

use crate::protocol::{DataFrame, FrameHeader};

/// The first NAK's delay for a unicast channel
/// (`AERON_NAK_UNICAST_DELAY_NS_MIN`, `aeron_driver_context.c:222`).
pub const NAK_UNICAST_DELAY_NS: i64 = 1_000;

/// How many times the first delay a retry waits
/// (`AERON_NAK_UNICAST_RETRY_DELAY_RATIO_DEFAULT`,
/// `aeron_driver_context.c:224`: a hundred).
pub const NAK_UNICAST_RETRY_RATIO: i64 = 100;

/// A hole: where it starts, and how long it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Gap {
    /// The term it is in.
    pub term_id: i32,
    /// Where it starts.
    pub term_offset: i32,
    /// How many bytes are missing.
    pub length: usize,
}

/// What a scan found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanResult {
    /// Where a reader may now be moved to — the start of the first hole.
    pub rebuild_offset: i32,
    /// Whether a hole was found that was not the one being tracked.
    pub loss_found: bool,
    /// The NAK to send, when the timer for the tracked hole has expired.
    pub nak: Option<Gap>,
}

/// The holes an image is tracking, and the timer on the one it is asking for.
#[derive(Debug)]
pub struct LossDetector {
    /// The hole the last scan saw (`scanned_gap`).
    scanned: Option<Gap>,
    /// The hole being asked for (`active_gap`).
    active: Option<Gap>,
    /// When the active hole's timer expires; [`TIMER_INACTIVE`] when none is
    /// running.
    expiry_ns: i64,
    /// The image this detector belongs to, for a caller that keeps several.
    pub registration_id: i64,
    /// How long the first NAK waits.
    delay_ns: i64,
    /// How long a repeated NAK waits.
    retry_ns: i64,
}

/// No timer is running (`AERON_LOSS_DETECTOR_TIMER_INACTIVE`).
pub const TIMER_INACTIVE: i64 = -1;

impl LossDetector {
    /// A detector for one image, with unicast's delays.
    pub const fn new(registration_id: i64) -> Self {
        Self {
            scanned: None,
            active: None,
            expiry_ns: TIMER_INACTIVE,
            registration_id,
            delay_ns: NAK_UNICAST_DELAY_NS,
            retry_ns: NAK_UNICAST_DELAY_NS * NAK_UNICAST_RETRY_RATIO,
        }
    }

    /// A detector with the delays a `nak-delay=` parameter named
    /// (`aeron_publication_image_create_static_delay_generator_state`): the
    /// retry is the delay times the driver's ratio.
    pub const fn with_delays(registration_id: i64, delay_ns: i64, retry_ns: i64) -> Self {
        Self {
            scanned: None,
            active: None,
            expiry_ns: TIMER_INACTIVE,
            registration_id,
            delay_ns,
            retry_ns,
        }
    }

    /// The hole being asked for, if any.
    pub const fn active_gap(&self) -> Option<Gap> {
        self.active
    }

    /// Scan a term for a hole (`aeron_loss_detector_scan`, `:46-93`).
    ///
    /// `rebuild_position` is where a reader has got to and `hwm_position` how
    /// far packets have been seen; the scan covers only what lies between them,
    /// because beyond the high-water mark nothing is *missing* — it simply has
    /// not arrived yet, and a hole there would be invented.
    #[allow(clippy::too_many_arguments)]
    pub fn scan(
        &mut self,
        term: &AtomicBuffer<'_, ReadOnly>,
        rebuild_position: i64,
        hwm_position: i64,
        term_length: i32,
        bits_to_shift: u32,
        initial_term_id: i32,
        now_ns: i64,
    ) -> ScanResult {
        let mask = i64::from(term_length) - 1;
        let mut rebuild_offset = (rebuild_position & mask) as i32;

        if rebuild_position >= hwm_position {
            return ScanResult {
                rebuild_offset,
                loss_found: false,
                nak: None,
            };
        }

        let rebuild_term_count = (rebuild_position >> bits_to_shift) as i32;
        let hwm_term_count = (hwm_position >> bits_to_shift) as i32;
        let rebuild_term_id = initial_term_id.wrapping_add(rebuild_term_count);
        let hwm_term_offset = (hwm_position & mask) as i32;

        // The scan stops at the high-water mark when both are in one term, and
        // at the term's end when the hole spans the boundary.
        let limit_offset = if rebuild_term_count == hwm_term_count {
            hwm_term_offset
        } else {
            term_length
        };

        rebuild_offset = scan_for_gap(term, rebuild_term_id, rebuild_offset, limit_offset, |gap| {
            self.scanned = Some(gap);
        });

        if rebuild_offset >= limit_offset {
            return ScanResult {
                rebuild_offset,
                loss_found: false,
                nak: None,
            };
        }

        // A hole is here: ask for it, unless it is the one already being asked
        // for (`gaps_match`, `:96-101`).
        let mut loss_found = false;

        if self.scanned != self.active {
            self.active = self.scanned;
            self.expiry_ns = now_ns + self.delay_ns;
            loss_found = true;
        }

        let mut nak = None;

        if now_ns >= self.expiry_ns {
            nak = self.active;
            self.expiry_ns = now_ns + self.retry_ns;
        }

        ScanResult {
            rebuild_offset,
            loss_found,
            nak,
        }
    }
}

/// Walk a term from `term_offset` to the first hole, and describe the hole
/// (`aeron_term_gap_scanner_scan_for_gap`,
/// `aeron-client/src/main/c/concurrent/aeron_term_gap_scanner.h:31-73`).
///
/// Returns where the gap begins, which is what a reader may be moved to. The
/// hole's *length* is found by stepping forward a data header at a time while
/// the frames there are still unwritten — zeros — because a gap is a run of
/// empty slots, not one.
fn scan_for_gap(
    term: &AtomicBuffer<'_, ReadOnly>,
    term_id: i32,
    term_offset: i32,
    limit_offset: i32,
    mut on_gap: impl FnMut(Gap),
) -> i32 {
    let mut offset = term_offset;

    while let Some(length) = frame_length_at(term, offset) {
        if length <= 0 {
            break;
        }

        offset += align(length);

        if offset >= limit_offset {
            break;
        }
    }

    let gap_begin = offset;

    if offset >= limit_offset {
        return gap_begin;
    }

    // The hole runs while the slots are empty; a slot with a frame in it is
    // where the next piece of contiguous data starts.
    offset += 32;

    while offset < limit_offset {
        match frame_length_at(term, offset) {
            Some(0) => offset += 32,
            _ => break,
        }
    }

    on_gap(Gap {
        term_id,
        term_offset: gap_begin,
        length: usize::try_from(offset - gap_begin).unwrap_or(0),
    });

    gap_begin
}

/// The frame length at an offset, or `None` past the term.
fn frame_length_at(term: &AtomicBuffer<'_, ReadOnly>, offset: i32) -> Option<i32> {
    let offset = usize::try_from(offset).ok()?;
    FrameHeader::read(&term_window(term, offset)?).map(|header| header.frame_length)
}

/// A view of the term from `offset` on, for the readers above.
///
/// The frame readers take a slice; the term is a region of shared memory, so
/// this copies out the header-sized window it needs rather than minting a
/// reference into memory another process is writing — the same rule
/// `deepmsg_core::buffer` states.
fn term_window(term: &AtomicBuffer<'_, ReadOnly>, offset: usize) -> Option<[u8; 32]> {
    if offset + 32 > term.len() {
        return None;
    }

    let mut window = [0u8; 32];
    term.copy_out(offset, &mut window)?;

    Some(window)
}

/// The aligned length of a frame, which is how far the next one is
/// (`AERON_ALIGN(..., AERON_LOGBUFFER_FRAME_ALIGNMENT)`).
fn align(length: i32) -> i32 {
    position::align_up(length, descriptor::FRAME_ALIGNMENT)
}

/// Whether a packet is a data frame this detector would have seen, for a caller
/// that wants to check a term's frames against it.
pub fn is_data_frame(packet: &[u8]) -> bool {
    FrameHeader::read(packet)
        .map(|header| {
            header.frame_type == crate::protocol::frame_type::DATA
                && DataFrame::read(packet).is_some()
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    use deepmsg_core::buffer::ReadWrite;
    use deepmsg_core::logbuffer::frame::{FLAG_UNFRAGMENTED, Frame, TYPE_DATA};

    const TERM_LENGTH: i32 = 1024;
    const BITS: u32 = 10;
    const INITIAL_TERM_ID: i32 = 1_000;

    #[repr(align(64))]
    struct Term(Vec<u8>);

    impl Term {
        fn new() -> Self {
            Self(vec![0u8; TERM_LENGTH as usize])
        }

        fn writable(&mut self) -> AtomicBuffer<'_, ReadWrite> {
            AtomicBuffer::from_slice_mut(&mut self.0).expect("aligned")
        }

        fn readable(&self) -> AtomicBuffer<'_, ReadOnly> {
            AtomicBuffer::from_slice(&self.0).expect("aligned")
        }
    }

    /// Write a published frame at `offset`, returning its aligned length.
    fn write(term: &mut Term, offset: usize, payload: &[u8]) -> usize {
        let writer = term.writable();
        let frame = Frame::new(&writer, offset);
        let length = payload.len() as i32 + 32;

        frame
            .begin(
                length,
                FLAG_UNFRAGMENTED,
                TYPE_DATA,
                offset as i32,
                42,
                1001,
                INITIAL_TERM_ID,
            )
            .expect("in range");
        frame.write_payload(payload).expect("in range");
        frame.publish(length).expect("in range");

        align(length) as usize
    }

    #[test]
    fn a_contiguous_term_has_no_gap() {
        let mut term = Term::new();
        let first = write(&mut term, 0, &[1u8; 100]);
        let second = write(&mut term, first, &[2u8; 100]);

        let mut detector = LossDetector::new(7);
        let result = detector.scan(
            &term.readable(),
            0,
            (first + second) as i64,
            TERM_LENGTH,
            BITS,
            INITIAL_TERM_ID,
            1_000,
        );

        assert_eq!((first + second) as i32, result.rebuild_offset);
        assert!(!result.loss_found);
        assert_eq!(None, result.nak);
        assert_eq!(None, detector.active_gap());
    }

    #[test]
    fn a_hole_stops_the_reader_and_is_asked_for_after_the_delay() {
        let mut term = Term::new();
        let first = write(&mut term, 0, &[1u8; 100]);
        // Nothing at `first`: a hole of two frames' worth, then data again.
        let third = write(&mut term, first + 128, &[3u8; 100]);
        let hwm = (first + 128 + third) as i64;

        let mut detector = LossDetector::new(7);
        let result = detector.scan(
            &term.readable(),
            0,
            hwm,
            TERM_LENGTH,
            BITS,
            INITIAL_TERM_ID,
            1_000,
        );

        assert_eq!(
            first as i32, result.rebuild_offset,
            "a reader stops at the hole"
        );
        assert!(result.loss_found, "and the hole is new");
        assert_eq!(None, result.nak, "but not asked for yet");
        assert_eq!(
            Some(Gap {
                term_id: INITIAL_TERM_ID,
                term_offset: first as i32,
                length: 128
            }),
            detector.active_gap()
        );

        // The delay passes with the hole still there: ask.
        let result = detector.scan(
            &term.readable(),
            0,
            hwm,
            TERM_LENGTH,
            BITS,
            INITIAL_TERM_ID,
            1_000 + NAK_UNICAST_DELAY_NS,
        );
        assert!(!result.loss_found, "the same hole is not new again");
        assert_eq!(detector.active_gap(), result.nak, "the NAK names it");

        // And the retry is the retry delay, not the first delay.
        let result = detector.scan(
            &term.readable(),
            0,
            hwm,
            TERM_LENGTH,
            BITS,
            INITIAL_TERM_ID,
            1_000 + NAK_UNICAST_DELAY_NS + NAK_UNICAST_DELAY_NS,
        );
        assert_eq!(None, result.nak, "not yet");
    }

    #[test]
    fn nothing_is_asked_for_beyond_the_high_water_mark() {
        let mut term = Term::new();
        let first = write(&mut term, 0, &[1u8; 100]);

        let mut detector = LossDetector::new(7);

        // The high-water mark is at the end of what arrived: there is no hole,
        // only data that has not come yet.
        let result = detector.scan(
            &term.readable(),
            0,
            first as i64,
            TERM_LENGTH,
            BITS,
            INITIAL_TERM_ID,
            1_000,
        );
        assert!(!result.loss_found);
        assert_eq!(None, result.nak);

        // And a position at the high-water mark asks for nothing at all.
        let result = detector.scan(
            &term.readable(),
            first as i64,
            first as i64,
            TERM_LENGTH,
            BITS,
            INITIAL_TERM_ID,
            1_000,
        );
        assert_eq!(first as i32, result.rebuild_offset);
        assert_eq!(None, result.nak);
    }

    #[test]
    fn a_hole_that_never_grows_is_asked_for_once_per_retry() {
        let mut term = Term::new();
        let first = write(&mut term, 0, &[1u8; 100]);
        // The next frame starts 128 bytes past the first: a hole of one
        // frame's worth that nothing fills.
        let second = write(&mut term, first + 128, &[2u8; 100]);

        let mut detector = LossDetector::new(7);
        let hwm = (first + 128 + second) as i64;

        let first_pass = detector.scan(
            &term.readable(),
            0,
            hwm,
            TERM_LENGTH,
            BITS,
            INITIAL_TERM_ID,
            0,
        );
        assert!(first_pass.loss_found);

        let asked = detector.scan(
            &term.readable(),
            0,
            hwm,
            TERM_LENGTH,
            BITS,
            INITIAL_TERM_ID,
            NAK_UNICAST_DELAY_NS,
        );
        assert!(asked.nak.is_some());

        let again = detector.scan(
            &term.readable(),
            0,
            hwm,
            TERM_LENGTH,
            BITS,
            INITIAL_TERM_ID,
            NAK_UNICAST_DELAY_NS + NAK_UNICAST_DELAY_NS * NAK_UNICAST_RETRY_RATIO,
        );
        assert!(again.nak.is_some(), "the retry delay has passed");
        assert!(!again.loss_found, "and it is still the same hole");
    }

    #[test]
    fn a_detector_can_be_given_the_channels_own_delays() {
        let detector = LossDetector::with_delays(7, 5_000, 50_000);

        assert_eq!(5_000, detector.delay_ns);
        assert_eq!(50_000, detector.retry_ns);
    }
}
