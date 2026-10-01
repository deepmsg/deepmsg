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

/// The log-normal a **multicast** image's NAK delays are drawn from
/// (`feedback_delay_state_t.optimal_delay`,
/// `aeron-driver/src/main/c/aeron_driver_common.h:139-157`, built by
/// `aeron_feedback_delay_state_init`, `aeron_loss_detector.c:91-119`).
///
/// Why a group needs one at all: a receiver that has lost a packet is one of
/// several, all of which will notice the same hole, and if they all asked at
/// once the publisher would send the same retransmission once per member.
/// Giving each a *random* delay — drawn from a distribution whose mean is the
/// driver's maximum backoff — spreads the asks out, and the backoff is what
/// they wait to let somebody else be the one that asked.
///
/// The four constants are the reference's, in its order, and they are what the
/// inverse-transform sample below needs: `rand_max` and `base_x` place a
/// uniform draw on the log-normal's support, and `constant_t` and `factor_t`
/// turn it into a time.
#[derive(Clone, Copy, Debug)]
pub struct MulticastBackoff {
    /// `max_backoff_T`, kept because the state it is built into holds it:
    /// `static_delay.delay_ns` and `static_delay.retry_ns` are both set to it
    /// (`aeron_driver.c:960-966`).
    max_backoff_ns: i64,
    rand_max: f64,
    base_x: f64,
    constant_t: f64,
    factor_t: f64,
}

impl MulticastBackoff {
    /// Build one for a group of `group_size` and a maximum backoff
    /// (`aeron_feedback_delay_state_init`, `:91-119`, which the driver calls
    /// once with the settings it was configured with — `aeron_driver.c:960-966`).
    ///
    /// `lambda = log(group_size) + 1` is the reference's shape parameter, and
    /// it is why the group size is the one input that changes the *spread*
    /// rather than the scale: one receiver gives `lambda = 1`, ten gives
    /// `3.30`, and a bigger group draws a wider range of delays.
    pub fn new(group_size: usize, max_backoff_ns: i64) -> Self {
        #[allow(clippy::cast_precision_loss)] // a group size, and a duration
        let lambda = (group_size as f64).ln() + 1.0;
        #[allow(clippy::cast_precision_loss)]
        let max_backoff_t = max_backoff_ns as f64;

        Self {
            max_backoff_ns,
            rand_max: lambda / max_backoff_t,
            base_x: lambda / (max_backoff_t * (lambda.exp() - 1.0)),
            constant_t: max_backoff_t / lambda,
            factor_t: (lambda.exp() - 1.0) * (max_backoff_t / lambda),
        }
    }

    /// The scale the driver built this with, which is also the mean.
    pub const fn max_backoff_ns(self) -> i64 {
        self.max_backoff_ns
    }

    /// One delay, from one uniform draw in `[0, 1)`
    /// (`aeron_loss_detector_nak_multicast_delay_generator`, `:120-125`).
    ///
    /// It ignores the retry flag, which is a fact about this generator rather
    /// than an oversight: a repeated ask draws a **fresh** delay where the
    /// unicast one returns the same number every time
    /// (`aeron_loss_detector.h:63-73` against `:85`).
    pub fn delay_ns(self, sample: f64) -> i64 {
        let x = sample * self.rand_max + self.base_x;

        #[allow(clippy::cast_possible_truncation)] // a delay in nanoseconds
        let delay = (self.constant_t * (x * self.factor_t).ln()) as i64;

        delay
    }
}

/// Uniform draws for [`MulticastBackoff`], in place of the reference's
/// `aeron_drand48`.
///
/// The reference seeds drand48 **once per process** from the clock
/// (`aeron_feedback_delay_state_init`, `:110-114`) and every image draws from
/// that one sequence, which is what this does too — one seed, lazily taken.
/// The numbers are not drand48's and are not meant to be: a delay is random by
/// design, and the only thing a group's members need of it is that two of them
/// do not draw the same one.
mod random {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Zero until the first draw, which is when the seed is taken.
    static STATE: AtomicU64 = AtomicU64::new(0);

    /// SplitMix64's increment.
    const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

    /// A uniform draw in `[0, 1)`, as `aeron_drand48` answers.
    ///
    /// One `compare_exchange` per draw, and none of them takes a lock: two
    /// images asking at once get two different numbers, which is the whole
    /// requirement.
    pub fn unit() -> f64 {
        let mut state = STATE.load(Ordering::Relaxed);

        loop {
            // Zero is "not seeded yet", so the first draw replaces it with the
            // clock rather than stepping from it — one seed, as the reference
            // takes one (`aeron_feedback_delay_state_init`, `:110-114`).
            let next = if state == 0 {
                let seed = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(1, |since| since.as_nanos() as u64);

                seed | 1
            } else {
                state.wrapping_add(GOLDEN)
            };

            match STATE.compare_exchange_weak(state, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => {
                    let mut z = next;
                    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);

                    // The top 53 bits are a double's mantissa, so this lands in
                    // `[0, 1)` without a rounding step.
                    #[allow(clippy::cast_precision_loss)]
                    return ((z >> 11) as f64) * (1.0 / 9_007_199_254_740_992.0);
                }

                Err(current) => state = current,
            }
        }
    }
}

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
    /// The random generator a **multicast** image draws both of those from
    /// instead, or [`None`] for the fixed pair above.
    ///
    /// One struct holds both in the reference too
    /// (`feedback_delay_state_t`, `aeron_driver_common.h:139-157`): the state
    /// carries a `static_delay` and an `optimal_delay`, and the generator it
    /// was initialised with decides which of them is read.
    multicast: Option<MulticastBackoff>,
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
            multicast: None,
        }
    }

    /// A detector for the delays a channel named, or the driver's own.
    ///
    /// `nak-delay=` is the one thing a subscription can say about how its gaps
    /// are asked for, and what it buys is exactly this: a **static** delay in
    /// place of the adaptive one. The retry is the delay times the driver's
    /// ratio (`aeron_publication_image_create_static_delay_generator_state`,
    /// `aeron_publication_image.c:112-117`, which multiplies by
    /// `context->nak_unicast_retry_delay_ratio`), and the multiplication
    /// saturates rather than wrapping — a delay near `i64::MAX` is a nonsense a
    /// client typed, not one that should become a *retry before the first
    /// ask*.
    ///
    /// **`reliable=false` wins over all of it**, and is the first thing the
    /// reference checks: an unreliable channel gets a static generator at
    /// **zero** for both delays (`aeron_publication_image_acquire_delay_generator_state`,
    /// `:92-95`, which returns before it looks at `nak-delay=` or at group
    /// semantics). Zero is not a detail — it is what makes an unreliable image
    /// fill its holes at once instead of waiting out a NAK that will never be
    /// sent (see [`crate::publication_image::PublicationImage::fill_gap`]).
    pub fn for_channel(
        registration_id: i64,
        is_reliable: bool,
        treat_as_multicast: bool,
        nak_delay_ns: Option<i64>,
        multicast_backoff: MulticastBackoff,
    ) -> Self {
        if !is_reliable {
            return Self::with_delays(registration_id, 0, 0);
        }

        // The group's generator comes **before** `nak-delay=`, which is what
        // makes a `nak-delay` on a group's channel a parameter the reference
        // reads and then does nothing with (`:97-100` returns in the arm
        // above the `nak-delay` lookup). Both delays are the driver's maximum
        // backoff there, and both are only ever read through the generator.
        if treat_as_multicast {
            return Self::with_multicast_backoff(
                registration_id,
                multicast_backoff,
                multicast_backoff.max_backoff_ns(),
                multicast_backoff.max_backoff_ns(),
            );
        }

        match nak_delay_ns {
            Some(delay_ns) => Self::with_delays(
                registration_id,
                delay_ns,
                delay_ns.saturating_mul(NAK_UNICAST_RETRY_RATIO),
            ),
            None => Self::new(registration_id),
        }
    }

    /// A detector with the delays given outright
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
            multicast: None,
        }
    }

    /// The same, drawing from a multicast group's generator
    /// (`aeron_publication_image_acquire_delay_generator_state`,
    /// `:97-100`, which returns the context's multicast state before it looks
    /// at `nak-delay=` — a `nak-delay` on a group's channel is read and then
    /// **ignored**).
    #[must_use]
    pub const fn with_multicast_backoff(
        registration_id: i64,
        backoff: MulticastBackoff,
        delay_ns: i64,
        retry_ns: i64,
    ) -> Self {
        Self {
            scanned: None,
            active: None,
            expiry_ns: TIMER_INACTIVE,
            registration_id,
            delay_ns,
            retry_ns,
            multicast: Some(backoff),
        }
    }

    /// How long until the next ask — the reference's `delay_generator(state,
    /// retry)`, which is a fixed lookup for a unicast image
    /// (`aeron_loss_detector.h:63-73`) and a **fresh draw** for a multicast
    /// one (`:85`, `:120-125`).
    fn next_delay_ns(&self, retry: bool) -> i64 {
        match self.multicast {
            Some(backoff) => backoff.delay_ns(random::unit()),
            None if retry => self.retry_ns,
            None => self.delay_ns,
        }
    }

    /// The hole being asked for, if any.
    /// The two delays this detector asks at — the first ask, and the retry.
    ///
    /// Test-only, like [`crate::image`]'s `Fragment::new`: the pair is worth
    /// reading only to check that a channel's own `nak-delay` reached the image,
    /// which is the one step between a parameter being parsed and a parameter
    /// doing anything.
    #[cfg(test)]
    pub(crate) const fn delays(&self) -> (i64, i64) {
        (self.delay_ns, self.retry_ns)
    }

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
            self.expiry_ns = now_ns + self.next_delay_ns(false);
            loss_found = true;
        }

        let mut nak = None;

        if now_ns >= self.expiry_ns {
            nak = self.active;
            self.expiry_ns = now_ns + self.next_delay_ns(true);
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

    use crate::config::{NAK_MULTICAST_GROUP_SIZE_DEFAULT, NAK_MULTICAST_MAX_BACKOFF_NS_DEFAULT};
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

    /// The driver's generator, built the way `create_image` builds it.
    fn backoff() -> MulticastBackoff {
        MulticastBackoff::new(
            NAK_MULTICAST_GROUP_SIZE_DEFAULT,
            NAK_MULTICAST_MAX_BACKOFF_NS_DEFAULT,
        )
    }

    #[test]
    fn a_groups_delays_are_drawn_and_not_fixed() {
        // The range is the log-normal's support: `constant_t * log(x *
        // factor_t)` over `x` in `[base_x, base_x + rand_max]`, which is
        // `[0, 1] * lambda` scaled — that is, the draws land inside the
        // driver's maximum backoff, and near it on average.
        let backoff = backoff();
        let max = backoff.max_backoff_ns();

        let draws: Vec<i64> = (0..512).map(|_| backoff.delay_ns(random::unit())).collect();

        assert!(
            draws.iter().all(|delay| *delay >= 0 && *delay <= max),
            "every draw is inside [0, {max}]"
        );
        assert!(
            draws.iter().any(|delay| *delay != draws[0]),
            "and they are not one number repeated"
        );

        // The endpoints, to pin the transform rather than its sample: the
        // smallest `x` gives zero and the largest gives the scale.
        assert_eq!(0, backoff.delay_ns(0.0));
        assert_eq!(max, backoff.delay_ns(1.0));
    }

    #[test]
    fn a_groups_two_delays_are_two_draws() {
        // A retry draws afresh rather than reusing the first delay, which is
        // the difference between this generator and the unicast one
        // (`aeron_loss_detector.h:63-73` against `:85`). One draw is used
        // twice here only because the state is a field: the same detector at
        // two expiries re-draws on each.
        let mut term = Term::new();
        let first = write(&mut term, 0, &[1u8; 100]);
        let third = write(&mut term, first + 128, &[3u8; 100]);
        let hwm = (first + 128 + third) as i64;

        // A static pair far from the draws, so that arming with it would be
        // visible: the generator's scale is ten milliseconds and this is a
        // millisecond.
        let mut detector = LossDetector::with_multicast_backoff(7, backoff(), 1_000, 1_000);

        let result = detector.scan(
            &term.readable(),
            0,
            hwm,
            TERM_LENGTH,
            BITS,
            INITIAL_TERM_ID,
            1_000,
        );
        assert!(result.loss_found, "the hole is activated");

        let armed = detector.expiry_ns;
        assert!(
            armed > 1_000 + 1_000 && armed <= 1_000 + backoff().max_backoff_ns(),
            "armed by a draw rather than by the static pair: {armed}"
        );

        // And the ask re-draws rather than reusing the first delay, which is
        // what makes the expiry move by a *different* amount each time.
        let result = detector.scan(
            &term.readable(),
            0,
            hwm,
            TERM_LENGTH,
            BITS,
            INITIAL_TERM_ID,
            armed,
        );
        assert!(
            result.nak.is_some(),
            "the ask goes out when the draw passes"
        );
        assert_ne!(
            armed - 1_000,
            detector.expiry_ns - armed,
            "the retry is a draw of its own"
        );
    }

    #[test]
    fn the_draws_of_two_groups_are_not_the_same_number() {
        // What the generator is for: two members of a group noticing one hole
        // must not ask at the same instant. The draws are compared as a set
        // because the pair a process makes is a sequence, not a repeat.
        let backoff = backoff();
        let draws: Vec<i64> = (0..64).map(|_| backoff.delay_ns(random::unit())).collect();

        let distinct: std::collections::HashSet<i64> = draws.iter().copied().collect();
        assert!(
            distinct.len() > 8,
            "{} distinct delays out of 64",
            distinct.len()
        );

        let units: Vec<f64> = (0..64).map(|_| random::unit()).collect();
        assert!(units.iter().all(|unit| *unit >= 0.0 && *unit < 1.0));
        let distinct: std::collections::HashSet<u64> =
            units.iter().map(|unit| unit.to_bits()).collect();
        assert_eq!(64, distinct.len(), "and the source never repeats itself");
    }

    #[test]
    fn a_detector_can_be_given_the_channels_own_delays() {
        let detector = LossDetector::with_delays(7, 5_000, 50_000);

        assert_eq!(5_000, detector.delay_ns);
        assert_eq!(50_000, detector.retry_ns);
    }

    #[test]
    fn a_channels_nak_delay_becomes_a_static_detector_at_the_ratio() {
        // The parameter's whole effect, and the argument order that is easy to
        // get backwards: the *first* delay is what the channel named, and the
        // retry is that times the driver's ratio.
        let named = LossDetector::for_channel(7, true, false, Some(5_000), backoff());

        assert_eq!(5_000, named.delay_ns, "the delay the channel named");
        assert_eq!(
            5_000 * NAK_UNICAST_RETRY_RATIO,
            named.retry_ns,
            "and the retry is that times the ratio"
        );

        // A channel that named nothing keeps the driver's own.
        let default = LossDetector::for_channel(7, true, false, None, backoff());
        assert_eq!(NAK_UNICAST_DELAY_NS, default.delay_ns);
        assert_eq!(
            NAK_UNICAST_DELAY_NS * NAK_UNICAST_RETRY_RATIO,
            default.retry_ns
        );

        // And a delay big enough to overflow the multiplication saturates
        // instead: a retry *before* the first ask is not a thing.
        let absurd = LossDetector::for_channel(7, true, false, Some(i64::MAX), backoff());
        assert_eq!(i64::MAX, absurd.delay_ns);
        assert_eq!(i64::MAX, absurd.retry_ns);
    }

    #[test]
    fn an_unreliable_channel_waits_for_nothing_and_ignores_nak_delay() {
        // What `reliable=false` buys, in the one place a delay is visible: the
        // hole is fillable the moment it is seen. The reference returns the
        // zero generator *before* it reads `nak-delay=`
        // (`aeron_publication_image.c:92-95`), so a channel that names both
        // gets zero — the parameter it typed is not read at all.
        let unreliable = LossDetector::for_channel(7, false, false, Some(5_000), backoff());

        assert_eq!(0, unreliable.delay_ns);
        assert_eq!(0, unreliable.retry_ns, "and no backoff before the next try");

        // The contrast, so that the test above cannot pass by the delays being
        // zero for every channel.
        let reliable = LossDetector::for_channel(7, true, false, Some(5_000), backoff());
        assert_eq!(5_000, reliable.delay_ns);
    }
}
