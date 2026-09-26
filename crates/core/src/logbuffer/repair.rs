//! Repairing a term buffer that someone else left in a bad state.
//!
//! Three situations, all on the receive side and all about a term that is not
//! simply a sequence of published frames:
//!
//! - a **hole**, where frames were lost and later arrive by retransmission;
//! - a **stall**, where a producer claimed space and then died, leaving a
//!   negative length no reader can pass;
//! - a **gap**, the region beyond the last valid frame that has not been
//!   written yet and may or may not be on its way.
//!
//! These live here rather than in the driver because the reference keeps them
//! under `aeron-client/src/main/c/concurrent/`, shared by both sides. See the
//! module docs.

use crate::buffer::{AtomicBuffer, ReadOnly, ReadWrite};

use super::descriptor;
use super::frame::{DATA_HEADER_LENGTH, FRAME_LENGTH_OFFSET, Frame, TYPE_PAD};

/// Copy a frame from one term into a hole in another.
///
/// The reference's `aeron_term_rebuilder_insert`
/// (`concurrent/aeron_term_rebuilder.h:24-40`) states the publish order more
/// clearly than any producer does, because it has to write a whole frame
/// *including* its length last:
///
/// 1. the payload,
/// 2. the header's last three 64-bit words, plainly,
/// 3. the first word — length, version, flags, type — with a release.
///
/// Returns `None` if the destination already holds a frame, which is how the
/// caller avoids writing a hole twice, or if either frame is malformed.
pub fn rebuild(
    dest: &AtomicBuffer<'_, ReadWrite>,
    dest_offset: usize,
    source: &AtomicBuffer<'_, ReadOnly>,
    source_offset: usize,
) -> Option<()> {
    let dest_frame = Frame::new(dest, dest_offset);
    if dest_frame.frame_length()? != 0 {
        return None;
    }

    let source_frame = Frame::new(source, source_offset);
    let source_length = source_frame.frame_length()?;
    if source_length < DATA_HEADER_LENGTH as i32 {
        return None;
    }

    let mut payload = vec![0u8; source_length as usize - DATA_HEADER_LENGTH];
    source_frame.copy_payload(&mut payload)?;
    dest_frame.write_payload(&payload)?;

    // Words 3, 2 and 1 — reserved value, stream and term id, session and term
    // offset — then word 0, which publishes the lot.
    for word in [24usize, 16, 8] {
        let value = source.load_i64(source_offset + word)?;
        dest.store_i64_relaxed(dest_offset + word, value)?;
    }

    dest.store_i64_release(dest_offset, source.load_i64(source_offset)?)
}

/// Walk a term from `offset`, reporting each gap, and return where the
/// contiguous run of present frames ends.
///
/// Mirrors `aeron_term_gap_scanner_scan_for_gap`
/// (`concurrent/aeron_term_gap_scanner.h:26-73`): frames are walked while they
/// are complete, and past the last one the scanner probes in 32-byte strides
/// for a non-zero length — because anything already present beyond a hole is
/// data that arrived out of order.
///
/// A **gap** is a range that holds no frame while something beyond it does.
/// The unwritten tail of a term is not a gap: nothing is expected there yet.
/// `on_gap(gap_start, gap_end)` is called once per gap, with `gap_start` where
/// the contiguous run stopped and `gap_end` where the next present frame begins.
pub fn scan_for_gap(
    term: &AtomicBuffer<'_, ReadOnly>,
    term_length: usize,
    offset: usize,
    limit: usize,
    mut on_gap: impl FnMut(usize, usize),
) -> usize {
    let mut offset = offset;
    let limit = limit.min(term_length);

    // Over the frames that are present, contiguously.
    while offset < limit {
        let frame = Frame::new(term, offset);
        let Some(length) = frame.frame_length() else {
            break;
        };
        if length <= 0 {
            break;
        }

        let Some(aligned) = frame.aligned_length() else {
            break;
        };
        if offset + aligned > term_length {
            break;
        }

        offset += aligned;
    }

    let end_of_valid = offset;

    // Then past them, at frame stride, for anything already there.
    let mut probing = end_of_valid;
    while probing + FRAME_LENGTH_OFFSET + 4 <= limit {
        let Some(length) = term.load_i32_acquire(probing + FRAME_LENGTH_OFFSET) else {
            break;
        };

        if 0 != length {
            if probing > end_of_valid {
                on_gap(end_of_valid, probing);
            }
            break;
        }

        probing += DATA_HEADER_LENGTH;
    }

    end_of_valid
}

/// Cover a range with a padding frame.
///
/// Used to fill a hole the retransmit never filled, so that readers can move
/// past it. The header is built from the metadata's default-header template —
/// which is what gives the padding frame a session, stream and term id — and
/// only the placement and the length are written here.
pub fn fill_gap(
    term: &AtomicBuffer<'_, ReadWrite>,
    metadata: &AtomicBuffer<'_, ReadOnly>,
    offset: usize,
    length: usize,
    term_id: i32,
) -> Option<()> {
    reset_as_padding(term, metadata, offset, length as i32, term_id)
}

/// Turn a stalled claim into padding.
///
/// A producer that claims space and dies leaves a negative length there, and no
/// reader can pass it: a scan stops at anything `<= 0` and would wait forever.
/// The driver calls this once a client has been quiet for its unblock timeout.
///
/// `length` is the length the stalled claim was making — **negative**, since
/// that is what the frame holds. The reference's caller passes `-frame_length`
/// for exactly that reason (`concurrent/aeron_term_unblocker.c:73-77`) and the
/// negation here is what turns "a claim in progress" into "padding".
pub fn unblock(
    term: &AtomicBuffer<'_, ReadWrite>,
    metadata: &AtomicBuffer<'_, ReadOnly>,
    offset: usize,
    length: i32,
    term_id: i32,
) -> Option<()> {
    reset_as_padding(term, metadata, offset, -length, term_id)
}

/// Overwrite a frame with the default header, mark it padding, and publish it.
///
/// `length` is positive here: whatever the caller had, this function's job is
/// to leave a published padding frame behind.
fn reset_as_padding(
    term: &AtomicBuffer<'_, ReadWrite>,
    metadata: &AtomicBuffer<'_, ReadOnly>,
    offset: usize,
    length: i32,
    term_id: i32,
) -> Option<()> {
    // How much of the template is valid is a field, not a constant: the C
    // reference copies exactly this many bytes
    // (`aeron_logbuffer_descriptor.h:323`), while the Java one hardcodes 32.
    let declared = metadata.load_i32(descriptor::DEFAULT_FRAME_HEADER_LENGTH_OFFSET)?;
    let template_length = declared.clamp(0, descriptor::DEFAULT_FRAME_HEADER_MAX_LENGTH as i32);
    if template_length < DATA_HEADER_LENGTH as i32 {
        return None;
    }

    let mut template = vec![0u8; template_length as usize];
    metadata.copy_out(descriptor::DEFAULT_FRAME_HEADER_OFFSET, &mut template)?;
    term.copy_in(offset, &template)?;

    // The template carries the *initial* term's identity and offset zero; both
    // have to be corrected for wherever this frame actually is.
    let frame = Frame::new(term, offset);
    frame.set_term_offset(offset as i32)?;
    frame.set_term_id(term_id)?;
    frame.set_type(TYPE_PAD)?;
    frame.publish(length)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logbuffer::frame::{FLAG_BEGIN, FLAG_END, FLAG_UNFRAGMENTED, TYPE_DATA};

    #[repr(align(64))]
    struct Buffer<const N: usize>([u8; N]);

    const TERM_LENGTH: usize = 1024;

    fn term() -> Buffer<TERM_LENGTH> {
        Buffer([0u8; TERM_LENGTH])
    }

    fn metadata() -> Buffer<{ descriptor::METADATA_STRUCT_LENGTH }> {
        let mut bytes = Buffer([0u8; descriptor::METADATA_STRUCT_LENGTH]);

        // A default header: a data frame header for session 11, stream 22,
        // term 33, offset 0.
        let view = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
        let header = DATA_HEADER_LENGTH;
        view.store_i32_relaxed(
            descriptor::DEFAULT_FRAME_HEADER_LENGTH_OFFSET,
            header as i32,
        )
        .expect("in range");

        let frame = Frame::new(&view, descriptor::DEFAULT_FRAME_HEADER_OFFSET);
        frame
            .begin(0, FLAG_UNFRAGMENTED, TYPE_DATA, 0, 11, 22, 33)
            .expect("in range");

        bytes
    }

    #[test]
    fn rebuilds_a_frame_into_a_hole() {
        let mut source_bytes = term();
        let mut dest_bytes = term();

        {
            let source = AtomicBuffer::from_slice_mut(&mut source_bytes.0).expect("aligned");
            let frame = Frame::new(&source, 64);
            let length = 5 + DATA_HEADER_LENGTH as i32;
            frame
                .begin(length, FLAG_BEGIN | FLAG_END, TYPE_DATA, 64, 7, 8, 9)
                .expect("in range");
            frame.write_payload(b"hello").expect("in range");
            frame.publish(length).expect("in range");
        }

        {
            let source = AtomicBuffer::from_slice(&source_bytes.0).expect("aligned");
            let dest = AtomicBuffer::from_slice_mut(&mut dest_bytes.0).expect("aligned");

            assert_eq!(
                Some(()),
                rebuild(&dest, 64, &source, 64),
                "the hole was empty, so the frame goes in"
            );
            assert_eq!(
                None,
                rebuild(&dest, 64, &source, 64),
                "and a second attempt must not double-write it"
            );
        }

        let dest = AtomicBuffer::from_slice(&dest_bytes.0).expect("aligned");
        let frame = Frame::new(&dest, 64);

        assert_eq!(Some(5 + DATA_HEADER_LENGTH as i32), frame.frame_length());
        assert_eq!(Some(7), frame.session_id());
        assert_eq!(Some(8), frame.stream_id());
        assert_eq!(Some(9), frame.term_id());

        let mut payload = [0u8; 5];
        assert_eq!(Some(()), frame.copy_payload(&mut payload));
        assert_eq!(b"hello", &payload);
    }

    #[test]
    fn a_gap_scanner_reports_a_hole_and_returns_where_data_ends() {
        let mut bytes = term();

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");

            // One frame, then a hole, then a frame that arrived out of order.
            let first = Frame::new(&writer, 0);
            let length = 32 + DATA_HEADER_LENGTH as i32;
            first
                .begin(length, FLAG_UNFRAGMENTED, TYPE_DATA, 0, 1, 2, 3)
                .expect("in range");
            first.publish(length).expect("in range");

            let later = Frame::new(&writer, 192);
            later
                .begin(length, FLAG_UNFRAGMENTED, TYPE_DATA, 192, 1, 2, 3)
                .expect("in range");
            later.publish(length).expect("in range");
        }

        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned");
        let mut gaps = Vec::new();
        let end = scan_for_gap(&view, TERM_LENGTH, 0, TERM_LENGTH, |start, stop| {
            gaps.push((start, stop))
        });

        assert_eq!(64, end, "valid data ends where the first frame does");
        assert_eq!(
            vec![(64, 192)],
            gaps,
            "the hole runs from where the run stopped to where the next frame is"
        );
    }

    #[test]
    fn a_gap_scanner_reports_no_gap_when_nothing_follows() {
        let mut bytes = term();

        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
            let first = Frame::new(&writer, 0);
            let length = 32 + DATA_HEADER_LENGTH as i32;
            first
                .begin(length, FLAG_UNFRAGMENTED, TYPE_DATA, 0, 1, 2, 3)
                .expect("in range");
            first.publish(length).expect("in range");
        }

        let view = AtomicBuffer::from_slice(&bytes.0).expect("aligned");
        let mut gaps = Vec::new();
        let _ = scan_for_gap(&view, TERM_LENGTH, 0, TERM_LENGTH, |start, stop| {
            gaps.push((start, stop))
        });

        assert!(
            gaps.is_empty(),
            "an empty tail is not a gap -- nothing is expected there yet"
        );
    }

    #[test]
    fn filling_a_gap_leaves_a_published_padding_frame() {
        let metadata_bytes = metadata();
        let mut bytes = term();

        let metadata = AtomicBuffer::from_slice(&metadata_bytes.0).expect("aligned");
        let term_buffer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");

        assert_eq!(Some(()), fill_gap(&term_buffer, &metadata, 128, 256, 33));

        let frame = Frame::new(&term_buffer, 128);
        assert_eq!(Some(256), frame.frame_length());
        assert!(frame.is_padding());
        assert_eq!(
            Some(11),
            frame.session_id(),
            "identity comes from the template"
        );
        assert_eq!(Some(22), frame.stream_id());
        assert_eq!(Some(33), frame.term_id());
        assert_eq!(
            Some(128),
            frame.term_offset(),
            "but the placement is corrected to where the frame actually is"
        );
    }

    #[test]
    fn unblocking_turns_a_stalled_claim_into_padding() {
        let metadata_bytes = metadata();
        let mut bytes = term();

        // A claim in flight: a negative length, as a dead producer leaves it.
        let stalled_length = 100i32;
        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
            writer
                .store_i32_release(FRAME_LENGTH_OFFSET, -stalled_length)
                .expect("in range");
        }

        let metadata = AtomicBuffer::from_slice(&metadata_bytes.0).expect("aligned");
        let term_buffer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");

        // The caller passes the length the claim was making -- negative, as the
        // frame holds it -- and what lands is positive.
        assert_eq!(
            Some(()),
            unblock(&term_buffer, &metadata, 0, -stalled_length, 33)
        );

        let frame = Frame::new(&term_buffer, 0);
        assert_eq!(Some(stalled_length), frame.frame_length());
        assert!(frame.is_padding());

        // And the scan can now pass it, which is the whole point.
        let view = term_buffer.as_read_only();
        let mut scanner = super::super::scan::Scanner::new(&view, TERM_LENGTH);
        assert!(matches!(
            scanner.advance(),
            super::super::scan::Step::Padding { .. }
        ));
    }
}
