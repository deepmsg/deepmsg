//! Repairing a term buffer that someone else left in a bad state.
//!
//! Three situations, all on the receive side and all about a term that is not
//! simply a sequence of published frames:
//!
//! - a **hole**, where frames were lost and later arrive by retransmission;
//! - a **stall**, where a producer claimed space and then died, leaving a
//!   negative length no reader can pass — or a run that reads **zero**, which
//!   a reader cannot pass either, because zero is not a frame;
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

/// Cover a range with a padding frame, if it is still empty.
///
/// Used to fill a hole the retransmit never filled, so that readers can move
/// past it. The header is built from the metadata's default-header template —
/// which is what gives the padding frame a session, stream and term id — and
/// only the placement and the length are written here.
///
/// **A gap is only a gap while nothing else has landed in it.** The reference
/// checks exactly that first — every frame-aligned slot from the end of the
/// range back to its start must still read zero
/// (`concurrent/aeron_term_gap_filler.c:26-35`) — and writes nothing when one
/// does not. What it is avoiding is the one outcome worse than a hole: a
/// retransmission that arrived between the scan that found the gap and this
/// call would be overwritten by padding, and the reader would be walked past
/// data it never saw, with nothing raised and no counter moved.
///
/// `None` therefore means either "something is in the way" or "the template is
/// not a frame header" — the same conflated `false` the reference returns.
pub fn fill_gap(
    term: &AtomicBuffer<'_, ReadWrite>,
    metadata: &AtomicBuffer<'_, ReadOnly>,
    offset: usize,
    length: usize,
    term_id: i32,
) -> Option<()> {
    let length = i32::try_from(length).ok()?;
    let offset = i32::try_from(offset).ok()?;

    if !scan_back_to_confirm_zeroed(
        |slot| Frame::new(term, slot).frame_length(),
        offset + length,
        offset,
    ) {
        return None;
    }

    reset_as_padding(
        term,
        metadata,
        usize::try_from(offset).ok()?,
        length,
        term_id,
    )
}

/// Confirm that a run of frame-aligned slots still reads zero, walking
/// backwards from `from` (exclusive) to `limit` (inclusive).
///
/// The reference names this once — `aeron_term_unblocker_scan_back_to_confirm_zeroed`
/// (`concurrent/aeron_term_unblocker.c:34-59`) — and then **inlines its own copy**
/// in the gap filler (`concurrent/aeron_term_gap_filler.c:26-35`, and Java's
/// `TermGapFiller.java:54`). Sharing one here is a structural choice, not a
/// behavioural one: the loop is the same loop, and both callers want the same
/// answer.
///
/// Zero is the only accepted reading, and a slot that cannot be read at all
/// counts as non-zero: the point of the walk is to prove the whole run is
/// still empty, and a run that cannot be proved empty is not one to write over.
fn scan_back_to_confirm_zeroed(read: impl Fn(usize) -> Option<i32>, from: i32, limit: i32) -> bool {
    let alignment = descriptor::FRAME_ALIGNMENT;
    let mut slot = from - alignment;

    while slot >= limit {
        let Some(offset) = usize::try_from(slot).ok() else {
            return false;
        };

        if read(offset) != Some(0) {
            return false;
        }

        slot -= alignment;
    }

    true
}

/// What an attempt to unblock a term produced.
///
/// The reference's three, verbatim: `aeron_term_unblocker_status_t`
/// (`concurrent/aeron_term_unblocker.h:23-29`) and Java's
/// `TermUnblocker.Status` (`:38-48`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnblockStatus {
    /// Nothing at the blocked offset was unblockable.
    NoAction,
    /// A run was covered with padding. The log has moved on; there is nothing
    /// else for the caller to do.
    Unblocked,
    /// A run was covered with padding that reaches the **end of the term**, so
    /// the caller must rotate to the next one
    /// (`concurrent/aeron_logbuffer_unblocker.c:54-56`,
    /// `LogBufferUnblocker.java:69-71`).
    UnblockedToEnd,
}

/// Turn whatever is stopped at an offset into padding.
///
/// A producer that claims space and dies leaves a **negative** length there, and
/// no reader can pass it: a scan stops at anything `<= 0` and would wait
/// forever. The other shape is a run that reads **zero** — a claim that was
/// never committed, or space that was zeroed — which a reader also cannot pass,
/// because zero is not a frame.
///
/// The driver runs this once a client has been quiet past its unblock timeout,
/// and again while a publication drains, where the client is known to be gone.
///
/// This is the reference's `aeron_term_unblocker_unblock`
/// (`concurrent/aeron_term_unblocker.c:61-116`) and Java's `TermUnblocker.unblock`
/// (`:77-127`), all three of whose states are reachable:
///
/// * negative length → one frame rewritten as padding, [`UnblockStatus::Unblocked`];
/// * zero → scan forward for the first frame that is *not* zero, confirm by
///   re-reading backwards that the whole run is still zero, and cover the run;
///   [`Unblocked`](UnblockStatus::Unblocked), or
///   [`UnblockedToEnd`](UnblockStatus::UnblockedToEnd) when the run reaches the
///   end of the term;
/// * anything else → nothing is wrong, [`NoAction`](UnblockStatus::NoAction).
pub struct Unblocker<'a> {
    term: &'a AtomicBuffer<'a, ReadWrite>,
    metadata: &'a AtomicBuffer<'a, ReadOnly>,
    /// A test-only seam: the frame-length reads, supplied by the test instead of
    /// taken from the term.
    ///
    /// The backward confirm can only **fail** when a slot that read zero during
    /// the forward scan reads non-zero at the confirm, and nothing outside the
    /// scan/confirm pair can be in that window — no thread, no caller. The
    /// reference's own tests script the term buffer for exactly that reason
    /// (`TermUnblockerTest.java:143-227` reads a Mockito `UnsafeBuffer` whose
    /// `thenReturn(0).thenReturn(x)` answers differently on successive reads),
    /// and a test here sets this and gets the same interleaving deterministically.
    /// No other build has the field.
    #[cfg(test)]
    reads: Option<Box<dyn Fn(usize) -> Option<i32> + 'a>>,
}

impl<'a> Unblocker<'a> {
    /// Read and write `term`, taking the padding template from `metadata`.
    pub fn new(
        term: &'a AtomicBuffer<'a, ReadWrite>,
        metadata: &'a AtomicBuffer<'a, ReadOnly>,
    ) -> Self {
        Self {
            term,
            metadata,
            #[cfg(test)]
            reads: None,
        }
    }

    /// [`Self::new`], with the frame-length reads supplied by a test.
    #[cfg(test)]
    fn with_reads(
        term: &'a AtomicBuffer<'a, ReadWrite>,
        metadata: &'a AtomicBuffer<'a, ReadOnly>,
        reads: Box<dyn Fn(usize) -> Option<i32> + 'a>,
    ) -> Self {
        Self {
            term,
            metadata,
            reads: Some(reads),
        }
    }

    /// Unblock whatever is at `blocked_offset`.
    ///
    /// `tail_offset` is the end of what the producer has written into this term
    /// — the forward scan stops there, because past it nothing is expected yet.
    /// `term_id` and `blocked_offset` are what the padding header is stamped
    /// with, and the reference's caller passes them from the term the blocked
    /// position falls in, not from the active one
    /// (`concurrent/aeron_logbuffer_unblocker.c:58-62`).
    pub fn unblock(
        &self,
        term_length: i32,
        blocked_offset: i32,
        tail_offset: i32,
        term_id: i32,
    ) -> UnblockStatus {
        let alignment = descriptor::FRAME_ALIGNMENT;

        // The one read that decides which of the three shapes this is. A claim
        // in progress is negative — the producer stores `-length` before it
        // knows the frame is complete — so the negation below is what turns "a
        // claim in progress" into "padding"
        // (`concurrent/aeron_term_unblocker.c:73-77`).
        let Some(frame_length) = self.frame_length(blocked_offset) else {
            return UnblockStatus::NoAction;
        };

        if frame_length < 0 {
            return if self
                .reset_as_padding_at(blocked_offset, -frame_length, term_id)
                .is_some()
            {
                UnblockStatus::Unblocked
            } else {
                UnblockStatus::NoAction
            };
        }

        if frame_length != 0 {
            return UnblockStatus::NoAction;
        }

        let mut status = UnblockStatus::NoAction;
        let mut current_offset = blocked_offset + alignment;

        while current_offset < tail_offset {
            let Some(length) = self.frame_length(current_offset) else {
                break;
            };

            if length != 0 {
                // The run ends here. It is only ours to cover if it is still
                // zero all the way back to where it started: a retransmission
                // that landed between the scan above and this call would
                // otherwise be overwritten by padding, and the reader would be
                // walked past data it never saw.
                if self.scan_back_to_confirm_zeroed(current_offset, blocked_offset) {
                    let run = current_offset - blocked_offset;
                    if self
                        .reset_as_padding_at(blocked_offset, run, term_id)
                        .is_some()
                    {
                        status = UnblockStatus::Unblocked;
                    }
                }
                break;
            }

            current_offset += alignment;
        }

        // Nothing was in the way up to the tail. When the tail is the end of
        // the term itself, the run is the whole remainder — but the first frame
        // is read **again** rather than trusted, for the same reason the run
        // above is confirmed: something may have been written since.
        if current_offset == term_length && self.frame_length(blocked_offset) == Some(0) {
            let run = current_offset - blocked_offset;
            if self
                .reset_as_padding_at(blocked_offset, run, term_id)
                .is_some()
            {
                status = UnblockStatus::UnblockedToEnd;
            }
        }

        status
    }

    /// The frame length at `offset`, through the scripted reads when a test has
    /// put them in place. `None` is a slot that cannot be read at all, which
    /// every caller treats as "not zero".
    fn frame_length(&self, offset: i32) -> Option<i32> {
        let offset = usize::try_from(offset).ok()?;

        #[cfg(test)]
        if let Some(reads) = &self.reads {
            return reads(offset);
        }

        Frame::new(self.term, offset).frame_length()
    }

    /// [`scan_back_to_confirm_zeroed`] over this term's own reads.
    fn scan_back_to_confirm_zeroed(&self, from: i32, limit: i32) -> bool {
        scan_back_to_confirm_zeroed(
            |slot| self.frame_length(i32::try_from(slot).ok()?),
            from,
            limit,
        )
    }

    /// [`reset_as_padding`] at an offset the caller computed.
    fn reset_as_padding_at(&self, offset: i32, length: i32, term_id: i32) -> Option<()> {
        let offset = usize::try_from(offset).ok()?;

        reset_as_padding(self.term, self.metadata, offset, length, term_id)
    }
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

/// Copy a whole received *packet* into a hole in a term.
///
/// The reference's `aeron_term_rebuilder_insert`
/// (`concurrent/aeron_term_rebuilder.h:24-40`) takes the packet as it came off
/// the wire — not a frame from another term — and writes it whole: one
/// datagram may carry several frames, and they are contiguous from
/// `dest_offset` because the receiver validated exactly that before calling
/// (`aeron_publication_image_validate_packet`,
/// `aeron-driver/src/main/c/aeron_publication_image.c:645-735`).
///
/// The order is the reference's and it is the publish protocol itself: the
/// payload and the header's last three words go first, plainly, and the first
/// word — length, version, flags, type — goes last with a release, because
/// that word is what tells a reader the frame is there.
///
/// Returns `None` when the destination already holds a frame (a duplicate, or
/// a hole filled by an earlier retransmission) or the packet is too short to
/// be one.
pub fn insert_packet(
    dest: &AtomicBuffer<'_, ReadWrite>,
    dest_offset: usize,
    packet: &[u8],
) -> Option<()> {
    if packet.len() < DATA_HEADER_LENGTH {
        return None;
    }

    let dest_frame = Frame::new(dest, dest_offset);
    if dest_frame.frame_length()? != 0 {
        return None;
    }

    dest.copy_in(
        dest_offset + DATA_HEADER_LENGTH,
        &packet[DATA_HEADER_LENGTH..],
    )?;

    for word in [24usize, 16, 8] {
        let value = i64::from_le_bytes(packet[word..word + 8].try_into().ok()?);
        dest.store_i64_relaxed(dest_offset + word, value)?;
    }

    let header = i64::from_le_bytes(packet[..8].try_into().ok()?);
    dest.store_i64_release(dest_offset, header)?;

    Some(())
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
    fn a_gap_something_has_landed_in_is_left_alone() {
        let metadata_bytes = metadata();
        let mut bytes = term();

        // A retransmission that arrived after the gap was scanned: a real frame
        // in the middle of what the caller still believes is empty.
        {
            let writer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");
            let frame = Frame::new(&writer, 192);
            let length = 32 + DATA_HEADER_LENGTH as i32;
            frame
                .begin(length, FLAG_UNFRAGMENTED, TYPE_DATA, 0, 5, 6, 7)
                .expect("in range");
            frame.write_payload(&[9u8; 32]).expect("in range");
            frame.publish(length).expect("in range");
        }

        let metadata = AtomicBuffer::from_slice(&metadata_bytes.0).expect("aligned");
        let term_buffer = AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned");

        assert_eq!(
            None,
            fill_gap(&term_buffer, &metadata, 128, 256, 33),
            "covering a frame that has already arrived would throw the frame \
             away, and a reader moved past it never saw it"
        );

        // And it is intact rather than half-overwritten.
        let frame = Frame::new(&term_buffer, 192);
        assert_eq!(Some(32 + DATA_HEADER_LENGTH as i32), frame.frame_length());
        assert!(!frame.is_padding());
    }

    // The unblocker's twelve cases are the reference's own, ported: the six
    // static ones are `aeron_logbuffer_unblocker_test.cpp`'s `TermUnblockerTest`
    // (which are also `TermUnblockerTest.java`'s first six), and the last six
    // are the Java cases whose term buffers answer differently on successive
    // reads — the backward confirm's whole reason for existing.

    /// Write a frame length at an offset, leaving the rest of the header zero.
    fn set_length(term: &AtomicBuffer<'_, ReadWrite>, offset: usize, length: i32) {
        term.store_i32_release(offset + FRAME_LENGTH_OFFSET, length)
            .expect("in range");
    }

    /// Scripted frame-length reads: a listed offset reads `0` the first time and
    /// the given value every time after; every other offset reads the term.
    ///
    /// This is `TermUnblockerTest.java`'s Mockito stub
    /// (`thenReturn(0).thenReturn(x)`) in Rust — the interleaving a real writer
    /// would produce, with no thread to schedule.
    fn scripted<'t>(
        term: &'t AtomicBuffer<'t, ReadWrite>,
        script: Vec<(usize, i32)>,
    ) -> Box<dyn Fn(usize) -> Option<i32> + 't> {
        let spent = std::cell::RefCell::new(vec![false; script.len()]);

        Box::new(move |offset: usize| {
            for (index, (at, value)) in script.iter().enumerate() {
                if *at == offset {
                    if std::mem::replace(&mut spent.borrow_mut()[index], true) {
                        return Some(*value);
                    }

                    return Some(0);
                }
            }

            Frame::new(term, offset).frame_length()
        })
    }

    /// Build an unblocker over a fresh term and metadata block.
    fn unblocker<'t>(
        metadata_bytes: &'t Buffer<{ descriptor::METADATA_STRUCT_LENGTH }>,
        bytes: &'t mut Buffer<TERM_LENGTH>,
    ) -> (AtomicBuffer<'t, ReadOnly>, AtomicBuffer<'t, ReadWrite>) {
        (
            AtomicBuffer::from_slice(&metadata_bytes.0).expect("aligned"),
            AtomicBuffer::from_slice_mut(&mut bytes.0).expect("aligned"),
        )
    }

    #[test]
    fn a_complete_message_is_left_alone() {
        // `shouldTakeNoActionWhenMessageIsComplete`.
        let metadata_bytes = metadata();
        let mut bytes = term();
        let (metadata, term_buffer) = unblocker(&metadata_bytes, &mut bytes);
        set_length(&term_buffer, 0, DATA_HEADER_LENGTH as i32);

        assert_eq!(
            UnblockStatus::NoAction,
            Unblocker::new(&term_buffer, &metadata).unblock(
                TERM_LENGTH as i32,
                0,
                TERM_LENGTH as i32,
                33
            ),
            "a frame is already there and readable, so nothing is blocked"
        );
    }

    #[test]
    fn a_term_with_nothing_in_it_is_left_alone() {
        // `shouldTakeNoActionWhenNoUnblockedMessage`: zero all the way to a tail
        // that is not the end of the term. Nothing said the tail will not move,
        // so there is no run to cover.
        let metadata_bytes = metadata();
        let mut bytes = term();
        let (metadata, term_buffer) = unblocker(&metadata_bytes, &mut bytes);

        assert_eq!(
            UnblockStatus::NoAction,
            Unblocker::new(&term_buffer, &metadata).unblock(
                TERM_LENGTH as i32,
                0,
                (TERM_LENGTH / 2) as i32,
                33
            )
        );
    }

    #[test]
    fn a_stalled_claim_becomes_padding() {
        // `shouldPatchNonCommittedMessage`.
        let metadata_bytes = metadata();
        let mut bytes = term();
        let (metadata, term_buffer) = unblocker(&metadata_bytes, &mut bytes);

        // A claim in flight: a negative length, as a dead producer leaves it.
        set_length(&term_buffer, 0, -128);

        assert_eq!(
            UnblockStatus::Unblocked,
            Unblocker::new(&term_buffer, &metadata).unblock(TERM_LENGTH as i32, 0, 128, 33)
        );

        let frame = Frame::new(&term_buffer, 0);
        assert_eq!(
            Some(128),
            frame.frame_length(),
            "the negative turns positive"
        );
        assert!(frame.is_padding());

        // And the scan can now pass it, which is the whole point.
        let view = term_buffer.as_read_only();
        let mut scanner = super::super::scan::Scanner::new(&view, TERM_LENGTH);
        assert!(matches!(
            scanner.advance(),
            super::super::scan::Step::Padding { .. }
        ));
    }

    #[test]
    fn a_zeroed_run_to_the_end_of_the_term_is_padded_and_rotates() {
        // `shouldPatchToEndOfPartition`.
        let metadata_bytes = metadata();
        let mut bytes = term();
        let (metadata, term_buffer) = unblocker(&metadata_bytes, &mut bytes);
        let offset = TERM_LENGTH - 128;

        assert_eq!(
            UnblockStatus::UnblockedToEnd,
            Unblocker::new(&term_buffer, &metadata).unblock(
                TERM_LENGTH as i32,
                offset as i32,
                TERM_LENGTH as i32,
                33,
            ),
            "the run reaches the end of the term, so the caller must rotate"
        );

        let frame = Frame::new(&term_buffer, offset);
        assert_eq!(Some(128), frame.frame_length());
        assert!(frame.is_padding());
    }

    #[test]
    fn the_scan_stops_at_the_next_complete_message() {
        // `shouldScanForwardForNextCompleteMessage`: the run is covered up to
        // the frame that ends it, and that frame is left alone.
        let metadata_bytes = metadata();
        let mut bytes = term();
        let (metadata, term_buffer) = unblocker(&metadata_bytes, &mut bytes);
        set_length(&term_buffer, 128, 128);

        assert_eq!(
            UnblockStatus::Unblocked,
            Unblocker::new(&term_buffer, &metadata).unblock(TERM_LENGTH as i32, 0, 256, 33)
        );

        let covered = Frame::new(&term_buffer, 0);
        assert_eq!(Some(128), covered.frame_length());
        assert!(covered.is_padding());

        // Only the length is asserted on the frame that ended the run: the
        // reference's own test stops at the covered frame, and a header whose
        // type field was never written reads as padding anyway — `PAD` is
        // `0x00` (`protocol/aeron_udp_protocol.h:172`).
        let untouched = Frame::new(&term_buffer, 128);
        assert_eq!(Some(128), untouched.frame_length(), "the run stopped here");
    }

    #[test]
    fn the_scan_stops_at_the_next_stalled_claim() {
        // `shouldScanForwardForNextNonCommittedMessage`: a *negative* length
        // ends the run just as a positive one does — the run is what gets
        // covered, not the claim that ends it.
        let metadata_bytes = metadata();
        let mut bytes = term();
        let (metadata, term_buffer) = unblocker(&metadata_bytes, &mut bytes);
        set_length(&term_buffer, 128, -128);

        assert_eq!(
            UnblockStatus::Unblocked,
            Unblocker::new(&term_buffer, &metadata).unblock(TERM_LENGTH as i32, 0, 256, 33)
        );

        let covered = Frame::new(&term_buffer, 0);
        assert_eq!(Some(128), covered.frame_length());
        assert!(covered.is_padding());

        let untouched = Frame::new(&term_buffer, 128);
        assert_eq!(
            Some(-128),
            untouched.frame_length(),
            "still a claim in flight"
        );
    }

    #[test]
    fn a_message_that_lands_before_the_confirm_is_not_covered() {
        // `shouldTakeNoActionIfMessageCompleteAfterScan`: the blocked offset
        // read zero while the scan passed it and reads a frame when the confirm
        // comes back to it.
        let metadata_bytes = metadata();
        let mut bytes = term();
        let (metadata, term_buffer) = unblocker(&metadata_bytes, &mut bytes);
        set_length(&term_buffer, 128, 128);

        let reads = scripted(&term_buffer, vec![(0, 128)]);
        let unblocker = Unblocker::with_reads(&term_buffer, &metadata, reads);

        assert_eq!(
            UnblockStatus::NoAction,
            unblocker.unblock(TERM_LENGTH as i32, 0, 256, 33),
            "a frame landed in the run between the scan and the confirm"
        );
    }

    #[test]
    fn a_claim_that_lands_before_the_confirm_is_not_covered() {
        // `shouldTakeNoActionIfMessageNonCommittedAfterScan`: the same race with
        // a negative length, which is what a *live* producer would be writing.
        let metadata_bytes = metadata();
        let mut bytes = term();
        let (metadata, term_buffer) = unblocker(&metadata_bytes, &mut bytes);
        set_length(&term_buffer, 128, 128);

        let reads = scripted(&term_buffer, vec![(0, -128)]);
        let unblocker = Unblocker::with_reads(&term_buffer, &metadata, reads);

        assert_eq!(
            UnblockStatus::NoAction,
            unblocker.unblock(TERM_LENGTH as i32, 0, 256, 33)
        );
    }

    #[test]
    fn a_message_at_the_end_of_the_term_is_not_covered_after_the_scan() {
        // `shouldTakeNoActionToEndOfPartitionIfMessageCompleteAfterScan`: the
        // second read of the first frame is the one the end-of-term branch
        // makes, and it is not skipped in favour of the first.
        let metadata_bytes = metadata();
        let mut bytes = term();
        let (metadata, term_buffer) = unblocker(&metadata_bytes, &mut bytes);
        let offset = TERM_LENGTH - 128;

        let reads = scripted(&term_buffer, vec![(offset, 128)]);
        let unblocker = Unblocker::with_reads(&term_buffer, &metadata, reads);

        assert_eq!(
            UnblockStatus::NoAction,
            unblocker.unblock(TERM_LENGTH as i32, offset as i32, TERM_LENGTH as i32, 33),
            "the run reaches the tail, but its first frame is a frame now"
        );
    }

    #[test]
    fn a_claim_at_the_end_of_the_term_is_not_covered_after_the_scan() {
        // `shouldTakeNoActionToEndOfPartitionIfMessageNonCommittedAfterScan`.
        let metadata_bytes = metadata();
        let mut bytes = term();
        let (metadata, term_buffer) = unblocker(&metadata_bytes, &mut bytes);
        let offset = TERM_LENGTH - 128;

        let reads = scripted(&term_buffer, vec![(offset, -128)]);
        let unblocker = Unblocker::with_reads(&term_buffer, &metadata, reads);

        assert_eq!(
            UnblockStatus::NoAction,
            unblocker.unblock(TERM_LENGTH as i32, offset as i32, TERM_LENGTH as i32, 33)
        );
    }

    #[test]
    fn a_second_message_racing_the_scan_stops_it() {
        // `shouldNotUnblockGapWithMessageRaceOnSecondMessageIncreasingTailThenInterrupting`.
        let metadata_bytes = metadata();
        let mut bytes = term();
        let (metadata, term_buffer) = unblocker(&metadata_bytes, &mut bytes);
        set_length(&term_buffer, 256, 128);

        let reads = scripted(&term_buffer, vec![(128, 128)]);
        let unblocker = Unblocker::with_reads(&term_buffer, &metadata, reads);

        assert_eq!(
            UnblockStatus::NoAction,
            unblocker.unblock(TERM_LENGTH as i32, 0, 384, 33)
        );
    }

    #[test]
    fn a_write_racing_the_scan_forward_stops_it() {
        // `shouldNotUnblockGapWithMessageRaceWhenScanForwardTakesAnInterrupt`.
        let metadata_bytes = metadata();
        let mut bytes = term();
        let (metadata, term_buffer) = unblocker(&metadata_bytes, &mut bytes);
        set_length(&term_buffer, 160, 7);

        let reads = scripted(&term_buffer, vec![(128, 128)]);
        let unblocker = Unblocker::with_reads(&term_buffer, &metadata, reads);

        assert_eq!(
            UnblockStatus::NoAction,
            unblocker.unblock(TERM_LENGTH as i32, 0, 384, 33)
        );
    }

    #[test]
    fn a_packet_is_inserted_payload_first_and_published_last() {
        let mut source = term();
        let mut destination = term();

        {
            let source_buffer = AtomicBuffer::from_slice_mut(&mut source.0).expect("aligned");
            let frame = Frame::new(&source_buffer, 0);
            let length = 13 + DATA_HEADER_LENGTH as i32;
            frame
                .begin(length, FLAG_BEGIN | FLAG_END, TYPE_DATA, 0, 7, 8, 9)
                .expect("in range");
            frame.write_payload(b"retransmitted").expect("in range");
            frame.publish(length).expect("in range");
        }

        let packet = source.0[..96].to_vec();

        {
            let writer = AtomicBuffer::from_slice_mut(&mut destination.0).expect("aligned");
            insert_packet(&writer, 32, &packet).expect("a hole");
        }

        let view = AtomicBuffer::from_slice(&destination.0).expect("aligned");
        let frame = Frame::new(&view, 32);

        assert_eq!(Some(64), frame.aligned_length(), "45 bytes aligned to 64");
        let mut payload = [0u8; 13];
        frame.copy_payload(&mut payload).expect("in range");
        assert_eq!(b"retransmitted", &payload);
    }

    #[test]
    fn a_frame_that_is_already_there_is_not_overwritten() {
        let mut source = term();
        let mut destination = term();

        {
            let source_buffer = AtomicBuffer::from_slice_mut(&mut source.0).expect("aligned");
            let frame = Frame::new(&source_buffer, 0);
            let length = 3 + DATA_HEADER_LENGTH as i32;
            frame
                .begin(length, FLAG_BEGIN | FLAG_END, TYPE_DATA, 0, 7, 8, 9)
                .expect("in range");
            frame.write_payload(b"one").expect("in range");
            frame.publish(length).expect("in range");
        }

        {
            let dest_buffer = AtomicBuffer::from_slice_mut(&mut destination.0).expect("aligned");
            let frame = Frame::new(&dest_buffer, 0);
            let length = 12 + DATA_HEADER_LENGTH as i32;
            frame
                .begin(length, FLAG_BEGIN | FLAG_END, TYPE_DATA, 0, 7, 8, 9)
                .expect("in range");
            frame.write_payload(b"already here").expect("in range");
            frame.publish(length).expect("in range");
        }

        let packet = source.0[..96].to_vec();
        let writer = AtomicBuffer::from_slice_mut(&mut destination.0).expect("aligned");

        assert!(
            insert_packet(&writer, 0, &packet).is_none(),
            "a duplicate is refused rather than written twice"
        );
    }
}
